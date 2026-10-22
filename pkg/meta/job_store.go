package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
)

type DesiredState string

const (
	DesiredStateRunning DesiredState = "RUNNING"
	DesiredStateStopped DesiredState = "STOPPED"
)

type JobExecutionRole string

const (
	JobExecutionRoleAll       JobExecutionRole = "ALL"
	JobExecutionRoleSnapshot  JobExecutionRole = "SNAPSHOT"
	JobExecutionRoleStreaming JobExecutionRole = "STREAMING"
)

type PersistedJob struct {
	ID            string
	SubmissionID  string
	Name          string
	MetaKey       string
	Config        *config.JobConfig
	DesiredState  DesiredState
	ExecutionRole JobExecutionRole
	// ResumeRequested distinguishes a resubmit from a new queued job.
	ResumeRequested bool
	LeaseOwner      string
	LeaseUntil      time.Time
	LastStatus      string
	Errors          []PersistedJobError
	ProgressJSON    []byte
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type PersistedJobError struct {
	Component string    `json:"component"`
	Message   string    `json:"message"`
	Time      time.Time `json:"time"`
}

type FailureNotificationState string

const (
	FailureNotificationPending FailureNotificationState = "PENDING"
	FailureNotificationSent    FailureNotificationState = "SENT"
	FailureNotificationFailed  FailureNotificationState = "FAILED"
)

type FailureNotificationPayload struct {
	JobID           string `json:"job_id"`
	JobName         string `json:"job_name,omitempty"`
	SinkType        string `json:"sink_type,omitempty"`
	ErrorComponent  string `json:"error_component,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	ProgressSummary string `json:"progress_summary,omitempty"`
	ProgressDetail  string `json:"progress_detail,omitempty"`
	DashboardURL    string `json:"dashboard_url,omitempty"`
}

type FailureNotification struct {
	IncidentID    string
	JobID         string
	Payload       FailureNotificationPayload
	State         FailureNotificationState
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	SentAt        time.Time
}

type JobStore interface {
	Init(ctx context.Context) error
	SaveJob(ctx context.Context, job PersistedJob) error
	LoadJobs(ctx context.Context) ([]PersistedJob, error)
	DeleteJob(ctx context.Context, jobID string) error
}

// JobWorkerStore is the optional durable coordination capability used by
// split snapshot and streaming workers. The lease makes a job single-owner
// even when more than one replica of a worker role is running.
type JobWorkerStore interface {
	ClaimJobs(ctx context.Context, role JobExecutionRole, owner string, limit int, leaseDuration time.Duration) ([]PersistedJob, error)
	RenewJobLease(ctx context.Context, jobID, submissionID, owner string, leaseDuration time.Duration) (bool, error)
	ReleaseJobLease(ctx context.Context, jobID, submissionID, owner string) error
}

// ClaimedJobStore protects split-worker writes. A snapshot or streaming
// worker may update only a row it still owns, so a late callback cannot
// recreate a job that another worker/API process has deleted.
type ClaimedJobStore interface {
	SaveClaimedJob(ctx context.Context, job PersistedJob, owner string) (bool, error)
}

// FailureNotificationStore is an optional durable outbox capability. Job
// managers continue to work with JobStore implementations that do not provide
// it, but notification retries cannot survive a process restart without it.
type FailureNotificationStore interface {
	SaveFailureNotification(ctx context.Context, notification FailureNotification) error
	LoadPendingFailureNotifications(ctx context.Context) ([]FailureNotification, error)
}

type MySQLJobStore struct {
	db *sql.DB
	// Reserve connections for heartbeats so progress persistence and job
	// reconciliation cannot exhaust the pool needed to retain ownership.
	leaseDB *sql.DB
	target  string
}

func normalizeJobStoreDSN(dsn string) (string, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse mysql job store dsn: %w", err)
	}
	// SaveClaimedJob uses RowsAffected to decide whether the guarded lease row
	// still matched. MySQL otherwise reports only rows whose values changed,
	// so an idempotent update can look exactly like a lost lease.
	cfg.ClientFoundRows = true
	return cfg.FormatDSN(), nil
}

func NewMySQLJobStore(dsn string) (*MySQLJobStore, error) {
	target, err := mysqlStoreTarget(dsn)
	if err != nil {
		return nil, err
	}
	normalizedDSN, err := normalizeJobStoreDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", normalizedDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)

	leaseConfig, err := drivermysql.ParseDSN(normalizedDSN)
	if err != nil {
		db.Close()
		return nil, err
	}
	if leaseConfig.Params == nil {
		leaseConfig.Params = make(map[string]string)
	}
	// Client cancellation closes a socket; it does not immediately end a
	// blocked server statement. Keep abandoned heartbeat lock waits short.
	leaseConfig.Params["innodb_lock_wait_timeout"] = "1"
	leaseDB, err := sql.Open("mysql", leaseConfig.FormatDSN())
	if err != nil {
		db.Close()
		return nil, err
	}
	leaseDB.SetMaxOpenConns(4)
	leaseDB.SetMaxIdleConns(4)
	leaseDB.SetConnMaxIdleTime(time.Minute)
	leaseDB.SetConnMaxLifetime(30 * time.Minute)
	return &MySQLJobStore{db: db, leaseDB: leaseDB, target: target}, nil
}

func (s *MySQLJobStore) Close() error {
	if s == nil {
		return nil
	}
	var errs []error
	if s.db != nil {
		errs = append(errs, s.db.Close())
	}
	if s.leaseDB != nil {
		errs = append(errs, s.leaseDB.Close())
	}
	return errors.Join(errs...)
}

func (s *MySQLJobStore) Init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, jobRecoveryArchiveDDL); err != nil {
		return err
	}
	const ddl = `
	CREATE TABLE IF NOT EXISTS job_registry (
	  job_id        VARCHAR(255) NOT NULL PRIMARY KEY,
	  submission_id VARCHAR(64) NOT NULL DEFAULT '',
	  job_name      VARCHAR(255) NOT NULL,
	  config_json   LONGTEXT NOT NULL,
	  meta_key      VARCHAR(255) NULL,
	  desired_state VARCHAR(32) NOT NULL,
	  execution_role VARCHAR(32) NOT NULL DEFAULT 'ALL',
	  resume_requested BOOLEAN NOT NULL DEFAULT FALSE,
	  lease_owner    VARCHAR(255) NULL,
	  lease_until    DATETIME(6) NULL,
	  last_status   VARCHAR(32) NOT NULL,
	  errors_json   LONGTEXT NULL,
	  progress_json LONGTEXT NULL,
	  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	  updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE job_registry ADD COLUMN errors_json LONGTEXT NULL AFTER last_status`); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE job_registry ADD COLUMN progress_json LONGTEXT NULL AFTER errors_json`); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE job_registry ADD COLUMN meta_key VARCHAR(255) NULL AFTER config_json`); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE job_registry ADD COLUMN submission_id VARCHAR(64) NOT NULL DEFAULT '' AFTER job_id`); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	// Existing rows predate submission fencing. Give each one a stable token so
	// workers restored during a rolling deployment can immediately use guarded
	// writes without requiring a separate migration step.
	if _, err := s.db.ExecContext(ctx, `UPDATE job_registry SET submission_id=LOWER(REPLACE(UUID(), '-', '')) WHERE submission_id=''`); err != nil {
		return err
	}
	for _, migration := range []string{
		`ALTER TABLE job_registry ADD COLUMN execution_role VARCHAR(32) NOT NULL DEFAULT 'ALL' AFTER desired_state`,
		`ALTER TABLE job_registry ADD COLUMN resume_requested BOOLEAN NOT NULL DEFAULT FALSE AFTER execution_role`,
		`ALTER TABLE job_registry ADD COLUMN lease_owner VARCHAR(255) NULL AFTER resume_requested`,
		`ALTER TABLE job_registry ADD COLUMN lease_until DATETIME(6) NULL AFTER lease_owner`,
	} {
		if _, err := s.db.ExecContext(ctx, migration); err != nil && !isDuplicateColumnError(err) {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX idx_job_registry_worker ON job_registry (execution_role, desired_state, lease_until)`); err != nil && !isDuplicateKeyNameError(err) {
		return err
	}
	const notificationDDL = `
	CREATE TABLE IF NOT EXISTS job_failure_notifications (
	  incident_id     VARCHAR(255) NOT NULL PRIMARY KEY,
	  job_id          VARCHAR(255) NOT NULL,
	  payload_json    LONGTEXT NOT NULL,
	  state           VARCHAR(32) NOT NULL,
	  attempts        INT NOT NULL DEFAULT 0,
	  next_attempt_at DATETIME(6) NULL,
	  last_error      LONGTEXT NULL,
	  created_at      DATETIME(6) NOT NULL,
	  updated_at      DATETIME(6) NOT NULL,
	  sent_at         DATETIME(6) NULL,
	  INDEX idx_failure_notifications_pending (state, next_attempt_at),
	  INDEX idx_failure_notifications_job (job_id)
	);`
	if _, err := s.db.ExecContext(ctx, notificationDDL); err != nil {
		return err
	}
	return nil
}

func (s *MySQLJobStore) SaveJob(ctx context.Context, job PersistedJob) error {
	if job.Config == nil {
		return fmt.Errorf("persisted job config is nil for job_id=%s", job.ID)
	}
	if strings.TrimSpace(job.SubmissionID) == "" {
		return fmt.Errorf("persisted job submission id is empty for job_id=%s", job.ID)
	}

	cfg := *job.Config
	config.ApplyDefaults(&cfg)

	payload, err := json.Marshal(&cfg)
	if err != nil {
		return err
	}
	errorsJSON, err := json.Marshal(job.Errors)
	if err != nil {
		return err
	}

	id := job.ID
	if id == "" {
		id = cfg.ID
	}
	if id == "" {
		return fmt.Errorf("persisted job id is empty")
	}

	name := job.Name
	if name == "" {
		name = cfg.Name
	}

	desired := job.DesiredState
	if desired == "" {
		desired = DesiredStateStopped
	}
	executionRole := job.ExecutionRole
	if executionRole == "" {
		executionRole = JobExecutionRoleAll
	}
	status := job.LastStatus
	if status == "" {
		status = "CREATED"
	}

	var progressJSON any
	if len(job.ProgressJSON) > 0 {
		progressJSON = string(job.ProgressJSON)
	}

	const stmt = `
	INSERT INTO job_registry (job_id, submission_id, job_name, config_json, meta_key, desired_state, execution_role, resume_requested, last_status, errors_json, progress_json, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
	ON DUPLICATE KEY UPDATE
	  submission_id = VALUES(submission_id),
	  config_json = JSON_SET(VALUES(config_json),'$.name',job_name),
	  meta_key = VALUES(meta_key),
	  desired_state = VALUES(desired_state),
	  execution_role = VALUES(execution_role),
	  resume_requested = VALUES(resume_requested),
	  last_status = VALUES(last_status),
	  errors_json = VALUES(errors_json),
	  progress_json = VALUES(progress_json),
	  updated_at = NOW();`
	_, err = s.db.ExecContext(ctx, stmt, id, job.SubmissionID, name, string(payload), nullableString(job.MetaKey), string(desired), string(executionRole), job.ResumeRequested, status, string(errorsJSON), progressJSON)
	return err
}

// SaveClaimedJob updates a job only while the caller still owns its worker
// lease. Unlike SaveJob, it never inserts: a deleted row stays deleted even
// if a snapshot completion callback arrives slightly later. A control-plane
// pause request is represented by last_status=PAUSING; routine worker progress
// must not overwrite that request before the worker observes it.
func (s *MySQLJobStore) SaveClaimedJob(ctx context.Context, job PersistedJob, owner string) (bool, error) {
	if job.Config == nil {
		return false, fmt.Errorf("persisted job config is nil for job_id=%s", job.ID)
	}
	if strings.TrimSpace(job.SubmissionID) == "" {
		return false, fmt.Errorf("persisted job submission id is empty for job_id=%s", job.ID)
	}
	if owner == "" {
		return false, fmt.Errorf("claimed job owner is empty")
	}

	cfg := *job.Config
	errorsJSON, err := json.Marshal(job.Errors)
	if err != nil {
		return false, err
	}

	id := job.ID
	if id == "" {
		id = cfg.ID
	}
	if id == "" {
		return false, fmt.Errorf("persisted job id is empty")
	}
	desired := job.DesiredState
	if desired == "" {
		desired = DesiredStateStopped
	}
	executionRole := job.ExecutionRole
	if executionRole == "" {
		executionRole = JobExecutionRoleAll
	}
	status := job.LastStatus
	if status == "" {
		status = "CREATED"
	}
	var progressJSON any
	if len(job.ProgressJSON) > 0 {
		progressJSON = string(job.ProgressJSON)
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE job_registry
		SET meta_key=?, desired_state=?, execution_role=?, resume_requested=FALSE,
		    last_status=CASE
		      WHEN last_status='PAUSING' AND ? IN ('CREATED','QUEUED','PENDING','RUNNING') THEN last_status
		      ELSE ?
		    END,
		    errors_json=?, progress_json=?, updated_at=NOW()
		WHERE job_id=? AND submission_id=? AND lease_owner=? AND lease_until >= UTC_TIMESTAMP(6)`,
		nullableString(job.MetaKey), string(desired), string(executionRole),
		status, status, string(errorsJSON), progressJSON, id, job.SubmissionID, owner)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

func (s *MySQLJobStore) LoadJobs(ctx context.Context) ([]PersistedJob, error) {
	const q = `
	SELECT job_id, submission_id, job_name, config_json, meta_key, desired_state, execution_role, resume_requested, lease_owner, lease_until, last_status, errors_json, progress_json, created_at, updated_at
	FROM job_registry
	ORDER BY created_at ASC`
	return s.loadJobs(ctx, q)
}

func (s *MySQLJobStore) loadJobs(ctx context.Context, query string, args ...any) ([]PersistedJob, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]PersistedJob, 0)
	for rows.Next() {
		var (
			jobID, submissionID, name, configJSON, desiredState, executionRole, lastStatus string
			resumeRequested                                                                bool
			metaKey                                                                        sql.NullString
			leaseOwner                                                                     sql.NullString
			leaseUntil                                                                     sql.NullTime
			errorsJSON, progressJSON                                                       sql.NullString
			createdAt, updatedAt                                                           time.Time
		)
		if err := rows.Scan(&jobID, &submissionID, &name, &configJSON, &metaKey, &desiredState, &executionRole, &resumeRequested, &leaseOwner, &leaseUntil, &lastStatus, &errorsJSON, &progressJSON, &createdAt, &updatedAt); err != nil {
			return nil, err
		}

		var cfg config.JobConfig
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return nil, fmt.Errorf("decode persisted job %s: %w", jobID, err)
		}
		config.ApplyDefaults(&cfg)
		if cfg.ID == "" {
			cfg.ID = jobID
		}
		cfg.Name = name

		var errorHistory []PersistedJobError
		if errorsJSON.Valid && errorsJSON.String != "" && errorsJSON.String != "null" {
			if err := json.Unmarshal([]byte(errorsJSON.String), &errorHistory); err != nil {
				return nil, fmt.Errorf("decode persisted job errors %s: %w", jobID, err)
			}
		}

		out = append(out, PersistedJob{
			ID:              jobID,
			SubmissionID:    submissionID,
			Name:            name,
			MetaKey:         metaKey.String,
			Config:          &cfg,
			DesiredState:    DesiredState(desiredState),
			ExecutionRole:   JobExecutionRole(executionRole),
			ResumeRequested: resumeRequested,
			LeaseOwner:      leaseOwner.String,
			LeaseUntil:      leaseUntil.Time,
			LastStatus:      lastStatus,
			Errors:          errorHistory,
			ProgressJSON:    []byte(progressJSON.String),
			CreatedAt:       createdAt,
			UpdatedAt:       updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *MySQLJobStore) ClaimJobs(ctx context.Context, role JobExecutionRole, owner string, limit int, leaseDuration time.Duration) ([]PersistedJob, error) {
	if role != JobExecutionRoleSnapshot && role != JobExecutionRoleStreaming {
		return nil, fmt.Errorf("unsupported worker role %q", role)
	}
	if owner == "" {
		return nil, fmt.Errorf("worker owner is empty")
	}
	if limit <= 0 {
		limit = 100
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT job_id
		FROM job_registry
		WHERE execution_role=? AND desired_state=?
		  AND (lease_until IS NULL OR lease_until < UTC_TIMESTAMP(6))
		ORDER BY updated_at ASC
		LIMIT ?
		FOR UPDATE`, string(role), string(DesiredStateRunning), limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	for _, id := range ids {
		// A legacy control plane can insert an empty submission after Init.
		// Backfill only this locked candidate, atomically with its lease. A
		// global UPDATE on every poll locks unrelated running jobs even when
		// none match the predicate and can block renewal beyond lease expiry.
		if _, err := tx.ExecContext(ctx, `
			UPDATE job_registry
			SET lease_owner=?, lease_until=TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
			    submission_id=CASE WHEN submission_id='' THEN LOWER(REPLACE(UUID(), '-', '')) ELSE submission_id END
			WHERE job_id=? AND execution_role=? AND desired_state=?
			  AND (lease_until IS NULL OR lease_until < UTC_TIMESTAMP(6))`,
			owner, leaseDuration.Microseconds(), id, string(role), string(DesiredStateRunning)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	const claimedQuery = `
	SELECT job_id, submission_id, job_name, config_json, meta_key, desired_state, execution_role, resume_requested, lease_owner, lease_until, last_status, errors_json, progress_json, created_at, updated_at
	FROM job_registry
	WHERE lease_owner=? AND execution_role=? AND desired_state=? AND lease_until >= UTC_TIMESTAMP(6)
	ORDER BY updated_at ASC`
	return s.loadJobs(ctx, claimedQuery, owner, string(role), string(DesiredStateRunning))
}

func (s *MySQLJobStore) RenewJobLease(ctx context.Context, jobID, submissionID, owner string, leaseDuration time.Duration) (bool, error) {
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	db := s.leaseDB
	if db == nil {
		db = s.db
	}
	result, err := db.ExecContext(ctx, `
		UPDATE job_registry
		SET lease_until=TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6))
		WHERE job_id=? AND submission_id=? AND lease_owner=?
		  AND (desired_state=? OR last_status IN ('PAUSING','STOPPING'))`,
		leaseDuration.Microseconds(), jobID, submissionID, owner, string(DesiredStateRunning))
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

func (s *MySQLJobStore) ReleaseJobLease(ctx context.Context, jobID, submissionID, owner string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE job_registry
		SET lease_owner=NULL, lease_until=NULL
		WHERE job_id=? AND submission_id=? AND lease_owner=?`, jobID, submissionID, owner)
	return err
}

func (s *MySQLJobStore) DeleteJob(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_failure_notifications WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_registry WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLJobStore) SaveFailureNotification(ctx context.Context, notification FailureNotification) error {
	if notification.IncidentID == "" {
		return fmt.Errorf("failure notification incident id is empty")
	}
	if notification.JobID == "" {
		return fmt.Errorf("failure notification job id is empty")
	}
	payload, err := json.Marshal(notification.Payload)
	if err != nil {
		return err
	}
	state := notification.State
	if state == "" {
		state = FailureNotificationPending
	}
	createdAt := notification.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	updatedAt := notification.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	var nextAttemptAt any
	if !notification.NextAttemptAt.IsZero() {
		nextAttemptAt = notification.NextAttemptAt.UTC()
	}
	var sentAt any
	if !notification.SentAt.IsZero() {
		sentAt = notification.SentAt.UTC()
	}

	const stmt = `
	INSERT INTO job_failure_notifications (
	  incident_id, job_id, payload_json, state, attempts, next_attempt_at,
	  last_error, created_at, updated_at, sent_at
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
	  payload_json = IF(state IN ('SENT', 'FAILED'), payload_json, VALUES(payload_json)),
	  attempts = IF(state IN ('SENT', 'FAILED'), attempts, VALUES(attempts)),
	  next_attempt_at = IF(state IN ('SENT', 'FAILED'), next_attempt_at, VALUES(next_attempt_at)),
	  last_error = IF(state IN ('SENT', 'FAILED'), last_error, VALUES(last_error)),
	  updated_at = IF(state IN ('SENT', 'FAILED'), updated_at, VALUES(updated_at)),
	  sent_at = IF(state = 'SENT', sent_at, VALUES(sent_at)),
	  state = IF(state IN ('SENT', 'FAILED'), state, VALUES(state));`
	_, err = s.db.ExecContext(
		ctx,
		stmt,
		notification.IncidentID,
		notification.JobID,
		string(payload),
		string(state),
		notification.Attempts,
		nextAttemptAt,
		notification.LastError,
		createdAt.UTC(),
		updatedAt.UTC(),
		sentAt,
	)
	return err
}

func (s *MySQLJobStore) LoadPendingFailureNotifications(ctx context.Context) ([]FailureNotification, error) {
	const query = `
	SELECT incident_id, job_id, payload_json, state, attempts, next_attempt_at,
	       last_error, created_at, updated_at, sent_at
	FROM job_failure_notifications
	WHERE state = 'PENDING'
	ORDER BY created_at ASC;`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]FailureNotification, 0)
	for rows.Next() {
		var (
			notification          FailureNotification
			payloadJSON, state    string
			nextAttemptAt, sentAt sql.NullTime
			lastError             sql.NullString
		)
		if err := rows.Scan(
			&notification.IncidentID,
			&notification.JobID,
			&payloadJSON,
			&state,
			&notification.Attempts,
			&nextAttemptAt,
			&lastError,
			&notification.CreatedAt,
			&notification.UpdatedAt,
			&sentAt,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(payloadJSON), &notification.Payload); err != nil {
			return nil, fmt.Errorf("decode failure notification %s: %w", notification.IncidentID, err)
		}
		notification.State = FailureNotificationState(state)
		if nextAttemptAt.Valid {
			notification.NextAttemptAt = nextAttemptAt.Time
		}
		if sentAt.Valid {
			notification.SentAt = sentAt.Time
		}
		if lastError.Valid {
			notification.LastError = lastError.String
		}
		out = append(out, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
