package meta

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	MaintenanceTaskQueued    = "queued"
	MaintenanceTaskLeased    = "leased"
	MaintenanceTaskSucceeded = "succeeded"
	MaintenanceTaskSkipped   = "skipped"
	MaintenanceTaskFailed    = "failed"
	MaintenanceTaskRetry     = "retry"
	MaintenanceTaskCancelled = "cancelled"

	// MaintenanceInventoryMaxAge bounds cached inventory freshness.
	MaintenanceInventoryMaxAge = time.Hour
)

type IcebergMaintenanceState struct {
	TableKey                      string     `json:"table_key"`
	Catalog                       string     `json:"catalog"`
	Namespace                     string     `json:"namespace"`
	Table                         string     `json:"table"`
	OwnerType                     string     `json:"owner_type"`
	OwnerJobID                    string     `json:"owner_job_id"`
	SnapshotComplete              bool       `json:"snapshot_complete"`
	LastSnapshotID                int64      `json:"last_snapshot_id"`
	InventorySnapshotID           int64      `json:"inventory_snapshot_id"`
	LastInventoryAt               *time.Time `json:"last_inventory_at,omitempty"`
	NextInventoryCheckAt          *time.Time `json:"next_inventory_check_at,omitempty"`
	InventoryLeaseOwner           string     `json:"inventory_lease_owner,omitempty"`
	InventoryLeaseUntil           *time.Time `json:"inventory_lease_until,omitempty"`
	LastWriteAt                   *time.Time `json:"last_write_at,omitempty"`
	NewDataFiles                  int        `json:"new_data_files"`
	NewEqualityDeleteFiles        int        `json:"new_equality_delete_files"`
	ActiveDataFiles               int        `json:"active_data_files"`
	ActiveSmallFiles              int        `json:"active_small_files"`
	ActiveSmallBytes              int64      `json:"active_small_bytes"`
	ActiveEqualityDeleteFiles     int        `json:"active_equality_delete_files"`
	ActivePositionDeleteFiles     int        `json:"active_position_delete_files"`
	ApplicableEqualityDeleteFiles *int       `json:"applicable_equality_delete_files,omitempty"`
	ApplicablePositionDeleteFiles *int       `json:"applicable_position_delete_files,omitempty"`
	ActiveCompactableFiles        int        `json:"active_compactable_files"`
	ActiveCompactableBytes        int64      `json:"active_compactable_bytes"`
	ActiveCompactionGroups        int        `json:"active_compaction_groups"`
	PartitionFields               []string   `json:"partition_fields"`
	NextCompactionCheckAt         *time.Time `json:"next_compaction_check_at,omitempty"`
	NextExpireCheckAt             *time.Time `json:"next_expire_check_at,omitempty"`
	NextOrphanCheckAt             *time.Time `json:"next_orphan_check_at,omitempty"`
	LastCompactionAt              *time.Time `json:"last_compaction_at,omitempty"`
	LastExpireAt                  *time.Time `json:"last_expire_at,omitempty"`
	LastOrphanAt                  *time.Time `json:"last_orphan_at,omitempty"`
	LeaseOwner                    string     `json:"lease_owner,omitempty"`
	LeaseUntil                    *time.Time `json:"lease_until,omitempty"`
	AttemptCount                  int        `json:"attempt_count"`
	LastError                     string     `json:"last_error,omitempty"`
	CreatedAt                     time.Time  `json:"created_at"`
	UpdatedAt                     time.Time  `json:"updated_at"`
}

func (s IcebergMaintenanceState) EffectiveEqualityDeleteFiles() int {
	if s.ApplicableEqualityDeleteFiles != nil {
		return *s.ApplicableEqualityDeleteFiles
	}
	return s.ActiveEqualityDeleteFiles
}

func (s IcebergMaintenanceState) EffectivePositionDeleteFiles() int {
	if s.ApplicablePositionDeleteFiles != nil {
		return *s.ApplicablePositionDeleteFiles
	}
	return s.ActivePositionDeleteFiles
}

type IcebergMaintenanceTask struct {
	ID             int64          `json:"id"`
	IdempotencyKey string         `json:"idempotency_key"`
	TableKey       string         `json:"table_key"`
	OwnerJobID     string         `json:"owner_job_id"`
	Operation      string         `json:"operation"`
	Priority       int            `json:"priority"`
	Status         string         `json:"status"`
	LeaseOwner     string         `json:"lease_owner,omitempty"`
	LeaseUntil     *time.Time     `json:"lease_until,omitempty"`
	AttemptCount   int            `json:"attempt_count"`
	NotBefore      time.Time      `json:"not_before"`
	ScheduleWindow string         `json:"schedule_window"`
	Payload        map[string]any `json:"payload,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type IcebergMaintenanceRun struct {
	ID           int64      `json:"id"`
	WorkerID     string     `json:"worker_id"`
	Status       string     `json:"status"`
	TaskCount    int        `json:"task_count"`
	SuccessCount int        `json:"success_count"`
	SkippedCount int        `json:"skipped_count"`
	FailedCount  int        `json:"failed_count"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

type IcebergMaintenanceRunFilter struct {
	OwnerJobID string
	Status     string
	Search     string
	Operation  string
	Engine     string
}

type IcebergMaintenanceResult struct {
	ID               int64          `json:"id"`
	RunID            int64          `json:"run_id"`
	TaskID           int64          `json:"task_id"`
	TableKey         string         `json:"table_key"`
	Operation        string         `json:"operation"`
	Engine           string         `json:"engine"`
	RoutingReason    string         `json:"routing_reason,omitempty"`
	Status           string         `json:"status"`
	InputFiles       int            `json:"input_files"`
	InputBytes       int64          `json:"input_bytes"`
	OutputFiles      int            `json:"output_files"`
	OutputBytes      int64          `json:"output_bytes"`
	DeleteFiles      int            `json:"delete_files"`
	ExpiredSnapshots int            `json:"expired_snapshots"`
	OrphanCandidates int            `json:"orphan_candidates"`
	DeletedFiles     int            `json:"deleted_files"`
	DeletedBytes     int64          `json:"deleted_bytes"`
	DurationMillis   int64          `json:"duration_ms"`
	Attempt          int            `json:"attempt"`
	SubmissionID     string         `json:"submission_id,omitempty"`
	Details          map[string]any `json:"details,omitempty"`
	Error            string         `json:"error,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type IcebergMaintenanceSummary struct {
	Tables             int        `json:"tables"`
	SnapshotBlocked    int        `json:"snapshot_blocked"`
	QueuedTasks        int        `json:"queued_tasks"`
	RetryTasks         int        `json:"retry_tasks"`
	ActiveLeases       int        `json:"active_leases"`
	FailedTasks        int        `json:"failed_tasks"`
	FailedTables       int        `json:"failed_tables"`
	OldestQueuedAt     *time.Time `json:"oldest_queued_at,omitempty"`
	OldestQueuedAgeSec int64      `json:"oldest_queued_age_seconds"`
}

// IcebergMaintenanceFailure is the latest failed task for one table and
// operation. A failure stops being current as soon as a newer task is queued,
// retried, or completed for the same table and operation.
type IcebergMaintenanceFailure struct {
	TaskID           int64          `json:"task_id"`
	TableKey         string         `json:"table_key"`
	Catalog          string         `json:"catalog,omitempty"`
	Namespace        string         `json:"namespace,omitempty"`
	Table            string         `json:"table,omitempty"`
	OwnerType        string         `json:"owner_type,omitempty"`
	OwnerJobID       string         `json:"owner_job_id"`
	Operation        string         `json:"operation"`
	AttemptCount     int            `json:"attempt_count"`
	LastError        string         `json:"last_error"`
	UpdatedAt        time.Time      `json:"updated_at"`
	CanRetry         bool           `json:"can_retry"`
	Priority         int            `json:"-"`
	SnapshotComplete bool           `json:"-"`
	Payload          map[string]any `json:"-"`
}

type IcebergMaintenanceStore struct {
	db *sql.DB
}

func NewIcebergMaintenanceStore(dsn string) (*IcebergMaintenanceStore, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("maintenance MySQL DSN is empty")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &IcebergMaintenanceStore{db: db}, nil
}

func (s *IcebergMaintenanceStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *IcebergMaintenanceStore) Init(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("maintenance store is nil")
	}
	ddls := []string{
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_catalog_guards (
		  catalog VARCHAR(255) NOT NULL PRIMARY KEY,
		  updated_at DATETIME(6) NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_reservations (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  reservation_key CHAR(64) NOT NULL,
		  owner_job_id VARCHAR(255) NOT NULL,
		  submission_id VARCHAR(64) NOT NULL,
		  reservation_kind VARCHAR(32) NOT NULL,
		  catalog VARCHAR(255) NOT NULL,
		  namespace_pattern VARCHAR(512) NOT NULL,
		  table_pattern VARCHAR(255) NOT NULL,
		  active TINYINT(1) NOT NULL DEFAULT 1,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  UNIQUE KEY uq_maintenance_reservation (reservation_key),
		  INDEX idx_maintenance_reservation_catalog (catalog, active)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_monitors (
		  monitor_id VARCHAR(255) NOT NULL PRIMARY KEY,
		  monitor_name VARCHAR(255) NOT NULL,
		  status VARCHAR(32) NOT NULL,
		  config_json LONGTEXT NOT NULL,
		  excluded_scope_count INT NOT NULL DEFAULT 0,
		  last_discovery_at DATETIME(6) NULL,
		  last_discovery_attempt_at DATETIME(6) NULL,
		  last_discovery_error LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  INDEX idx_maintenance_monitors_status (status, updated_at)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_monitor_targets (
		  monitor_id VARCHAR(255) NOT NULL,
		  table_key VARCHAR(512) NOT NULL,
		  catalog VARCHAR(255) NOT NULL,
		  namespace_name VARCHAR(512) NOT NULL,
		  table_name VARCHAR(255) NOT NULL,
		  specificity INT NOT NULL,
		  claim_status VARCHAR(32) NOT NULL,
		  last_error LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  PRIMARY KEY (monitor_id, table_key),
		  INDEX idx_monitor_target_table (table_key, specificity, claim_status)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_state (
		  table_key VARCHAR(512) NOT NULL PRIMARY KEY,
		  catalog VARCHAR(255) NOT NULL,
		  namespace_name VARCHAR(512) NOT NULL,
		  table_name VARCHAR(255) NOT NULL,
		  owner_type VARCHAR(32) NOT NULL DEFAULT 'job',
		  owner_job_id VARCHAR(255) NOT NULL,
		  snapshot_complete TINYINT(1) NOT NULL DEFAULT 0,
		  last_snapshot_id BIGINT NOT NULL DEFAULT 0,
		  inventory_snapshot_id BIGINT NOT NULL DEFAULT 0,
		  last_inventory_at DATETIME(6) NULL,
		  next_inventory_check_at DATETIME(6) NULL,
		  inventory_priority INT NOT NULL DEFAULT 0,
		  inventory_lease_owner VARCHAR(255) NULL,
		  inventory_lease_until DATETIME(6) NULL,
		  last_write_at DATETIME(6) NULL,
		  new_data_files INT NOT NULL DEFAULT 0,
		  new_equality_delete_files INT NOT NULL DEFAULT 0,
		  active_data_files INT NOT NULL DEFAULT 0,
		  active_small_files INT NOT NULL DEFAULT 0,
		  active_small_bytes BIGINT NOT NULL DEFAULT 0,
		  active_equality_delete_files INT NOT NULL DEFAULT 0,
		  active_position_delete_files INT NOT NULL DEFAULT 0,
		  applicable_equality_delete_files INT NULL,
		  applicable_position_delete_files INT NULL,
		  active_compactable_files INT NOT NULL DEFAULT 0,
		  active_compactable_bytes BIGINT NOT NULL DEFAULT 0,
		  active_compaction_groups INT NOT NULL DEFAULT 0,
		  partition_fields_json LONGTEXT NULL,
		  next_compaction_check_at DATETIME(6) NULL,
		  next_expire_check_at DATETIME(6) NULL,
		  next_orphan_check_at DATETIME(6) NULL,
		  last_compaction_at DATETIME(6) NULL,
		  last_expire_at DATETIME(6) NULL,
		  last_orphan_at DATETIME(6) NULL,
		  lease_owner VARCHAR(255) NULL,
		  lease_until DATETIME(6) NULL,
		  attempt_count INT NOT NULL DEFAULT 0,
		  last_error LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  INDEX idx_maintenance_state_compaction (snapshot_complete, next_compaction_check_at),
		  INDEX idx_maintenance_state_expire (snapshot_complete, next_expire_check_at),
		  INDEX idx_maintenance_state_orphan (snapshot_complete, next_orphan_check_at),
		  INDEX idx_maintenance_state_inventory (snapshot_complete, last_inventory_at, next_inventory_check_at, inventory_lease_until),
		  INDEX idx_maintenance_state_inventory_priority (snapshot_complete, inventory_priority, next_inventory_check_at, inventory_lease_until),
		  INDEX idx_maintenance_state_owner (owner_job_id),
		  INDEX idx_maintenance_state_lease (lease_until)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_tasks (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  idempotency_key VARCHAR(64) NOT NULL,
		  table_key VARCHAR(512) NOT NULL,
		  owner_job_id VARCHAR(255) NOT NULL,
		  operation VARCHAR(64) NOT NULL,
		  priority INT NOT NULL DEFAULT 100,
		  status VARCHAR(32) NOT NULL,
		  lease_owner VARCHAR(255) NULL,
		  lease_until DATETIME(6) NULL,
		  attempt_count INT NOT NULL DEFAULT 0,
		  not_before DATETIME(6) NOT NULL,
		  schedule_window VARCHAR(64) NOT NULL,
		  payload_json LONGTEXT NULL,
		  last_error LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  updated_at DATETIME(6) NOT NULL,
		  UNIQUE KEY uq_maintenance_task_idempotency (idempotency_key),
		  INDEX idx_maintenance_task_due (status, not_before, priority, id),
		  INDEX idx_maintenance_task_lease (status, lease_until),
		  INDEX idx_maintenance_task_table (table_key, operation, status),
		  INDEX idx_maintenance_task_owner_status (owner_job_id, status, created_at, id)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_runs (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  worker_id VARCHAR(255) NOT NULL,
		  status VARCHAR(32) NOT NULL,
		  task_count INT NOT NULL DEFAULT 0,
		  success_count INT NOT NULL DEFAULT 0,
		  skipped_count INT NOT NULL DEFAULT 0,
		  failed_count INT NOT NULL DEFAULT 0,
		  started_at DATETIME(6) NOT NULL,
		  finished_at DATETIME(6) NULL,
		  created_at DATETIME(6) NOT NULL,
		  INDEX idx_maintenance_runs_started (started_at),
		  INDEX idx_maintenance_runs_status (status)
		)`,
		`CREATE TABLE IF NOT EXISTS iceberg_maintenance_results (
		  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		  run_id BIGINT NOT NULL,
		  task_id BIGINT NOT NULL,
		  table_key VARCHAR(512) NOT NULL,
		  operation VARCHAR(64) NOT NULL,
		  engine VARCHAR(32) NOT NULL,
		  routing_reason VARCHAR(255) NULL,
		  status VARCHAR(32) NOT NULL,
		  input_files INT NOT NULL DEFAULT 0,
		  input_bytes BIGINT NOT NULL DEFAULT 0,
		  output_files INT NOT NULL DEFAULT 0,
		  output_bytes BIGINT NOT NULL DEFAULT 0,
		  delete_files INT NOT NULL DEFAULT 0,
		  expired_snapshots INT NOT NULL DEFAULT 0,
		  orphan_candidates INT NOT NULL DEFAULT 0,
		  deleted_files INT NOT NULL DEFAULT 0,
		  deleted_bytes BIGINT NOT NULL DEFAULT 0,
		  duration_ms BIGINT NOT NULL DEFAULT 0,
		  attempt INT NOT NULL DEFAULT 0,
		  submission_id VARCHAR(255) NULL,
		  details_json LONGTEXT NULL,
		  error_text LONGTEXT NULL,
		  created_at DATETIME(6) NOT NULL,
		  UNIQUE KEY uq_maintenance_result_task_attempt (task_id, attempt),
		  INDEX idx_maintenance_results_run (run_id, id),
		  INDEX idx_maintenance_results_table (table_key, created_at),
		  INDEX idx_maintenance_results_status (status, created_at)
		)`,
	}
	for _, ddl := range ddls {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "last_write_at", "DATETIME(6) NULL AFTER last_snapshot_id"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_monitors", "last_discovery_at", "DATETIME(6) NULL AFTER config_json"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_monitors", "last_discovery_attempt_at", "DATETIME(6) NULL AFTER last_discovery_at"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_monitors", "excluded_scope_count", "INT NOT NULL DEFAULT 0 AFTER config_json"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_monitors", "last_discovery_error", "LONGTEXT NULL AFTER last_discovery_at"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "inventory_snapshot_id", "BIGINT NOT NULL DEFAULT 0 AFTER last_snapshot_id"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "last_inventory_at", "DATETIME(6) NULL AFTER inventory_snapshot_id"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "next_inventory_check_at", "DATETIME(6) NULL AFTER last_inventory_at"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "inventory_priority", "INT NOT NULL DEFAULT 0 AFTER next_inventory_check_at"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "inventory_lease_owner", "VARCHAR(255) NULL AFTER next_inventory_check_at"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "inventory_lease_until", "DATETIME(6) NULL AFTER inventory_lease_owner"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "active_compactable_files", "INT NOT NULL DEFAULT 0 AFTER active_position_delete_files"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "applicable_equality_delete_files", "INT NULL AFTER active_position_delete_files"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "applicable_position_delete_files", "INT NULL AFTER applicable_equality_delete_files"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "active_compactable_bytes", "BIGINT NOT NULL DEFAULT 0 AFTER active_compactable_files"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "active_compaction_groups", "INT NOT NULL DEFAULT 0 AFTER active_compactable_bytes"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "iceberg_maintenance_state", "partition_fields_json", "LONGTEXT NULL AFTER active_compaction_groups"); err != nil {
		return err
	}
	if err := s.ensureIndex(ctx, "iceberg_maintenance_tasks", "idx_maintenance_task_owner_status", "(owner_job_id, status, created_at, id)"); err != nil {
		return err
	}
	if err := s.ensureIndex(ctx, "iceberg_maintenance_state", "idx_maintenance_state_inventory", "(snapshot_complete, last_inventory_at, next_inventory_check_at, inventory_lease_until)"); err != nil {
		return err
	}
	if err := s.ensureIndex(ctx, "iceberg_maintenance_state", "idx_maintenance_state_inventory_priority", "(snapshot_complete, inventory_priority, next_inventory_check_at, inventory_lease_until)"); err != nil {
		return err
	}
	return nil
}

// ensureColumn keeps the durable state schema forward-compatible for existing
// Rivus deployments, where CREATE TABLE IF NOT EXISTS does not add new fields.
func (s *IcebergMaintenanceStore) ensureColumn(ctx context.Context, table, column, definition string) error {
	var found int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, table, column).Scan(&found)
	if err != nil {
		return err
	}
	if found != 0 {
		return nil
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition))
	if err == nil {
		return nil
	}
	// API and worker processes can run this migration concurrently. Treat a
	// duplicate-column race as success after verifying the resulting schema.
	if checkErr := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`, table, column).Scan(&found); checkErr == nil && found != 0 {
		return nil
	}
	return err
}

// ensureIndex adds indexes introduced after the durable maintenance tables were
// first deployed. CREATE TABLE IF NOT EXISTS does not update existing tables.
func (s *IcebergMaintenanceStore) ensureIndex(ctx context.Context, table, index, definition string) error {
	var found int
	check := `SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`
	if err := s.db.QueryRowContext(ctx, check, table, index).Scan(&found); err != nil {
		return err
	}
	if found != 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD INDEX %s %s", table, index, definition))
	if err == nil {
		return nil
	}
	// Rivus API and the maintenance worker may initialize at the same time. If
	// the other process won the ALTER race, accept the now-existing index.
	if checkErr := s.db.QueryRowContext(ctx, check, table, index).Scan(&found); checkErr == nil && found != 0 {
		return nil
	}
	return err
}

func (s *IcebergMaintenanceStore) SnapshotDone(ctx context.Context, metaKey string) (bool, bool, error) {
	var done int
	err := s.db.QueryRowContext(ctx, `SELECT CASE WHEN done = 1 AND completion_version = 1 THEN 1 ELSE 0 END FROM job_snapshots WHERE job_id = ?`, metaKey).Scan(&done)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return done != 0, true, nil
}

func (s *IcebergMaintenanceStore) UpsertState(ctx context.Context, state IcebergMaintenanceState, initialCompaction, initialExpire, initialOrphan time.Time) error {
	if strings.TrimSpace(state.TableKey) == "" {
		return fmt.Errorf("maintenance state table key is empty")
	}
	const stmt = `INSERT INTO iceberg_maintenance_state (
	  table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	  snapshot_complete, last_snapshot_id, last_write_at,
	  next_inventory_check_at, next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	  created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))
	ON DUPLICATE KEY UPDATE
	  catalog = VALUES(catalog), namespace_name = VALUES(namespace_name), table_name = VALUES(table_name),
	  owner_type = VALUES(owner_type), owner_job_id = VALUES(owner_job_id),
	  snapshot_complete = GREATEST(snapshot_complete, VALUES(snapshot_complete)),
	  last_write_at = COALESCE(last_write_at, VALUES(last_write_at)),
	  next_inventory_check_at = CASE
	    WHEN last_inventory_at IS NULL THEN COALESCE(next_inventory_check_at, VALUES(next_inventory_check_at))
	    ELSE next_inventory_check_at END,
	  next_compaction_check_at = COALESCE(next_compaction_check_at, VALUES(next_compaction_check_at)),
	  next_expire_check_at = COALESCE(next_expire_check_at, VALUES(next_expire_check_at)),
	  next_orphan_check_at = COALESCE(next_orphan_check_at, VALUES(next_orphan_check_at)),
	  updated_at = UTC_TIMESTAMP(6)`
	_, err := s.db.ExecContext(ctx, stmt,
		state.TableKey, state.Catalog, state.Namespace, state.Table,
		firstNonEmptyMeta(state.OwnerType, "job"), state.OwnerJobID,
		boolInt(state.SnapshotComplete), state.LastSnapshotID, state.LastWriteAt,
		state.NextInventoryCheckAt, initialCompaction.UTC(), initialExpire.UTC(), initialOrphan.UTC(),
	)
	return err
}

func (s *IcebergMaintenanceStore) CoalesceSignal(
	ctx context.Context,
	tableKey string,
	snapshotID int64,
	newDataFiles, newEqDeleteFiles int,
	snapshotComplete bool,
	equalityDeleteFilesThreshold int,
	nextCompaction time.Time,
) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var previousSnapshotID int64
	if err := tx.QueryRowContext(ctx, `SELECT last_snapshot_id FROM iceberg_maintenance_state WHERE table_key=? FOR UPDATE`, tableKey).Scan(&previousSnapshotID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	snapshotChanged := snapshotIDsDiffer(previousSnapshotID, snapshotID)
	addedDataFiles, addedEqDeleteFiles := 0, 0
	if snapshotChanged {
		addedDataFiles = max(newDataFiles, 0)
		addedEqDeleteFiles = max(newEqDeleteFiles, 0)
	}
	const stmt = `UPDATE iceberg_maintenance_state
	SET next_compaction_check_at = CASE
	      WHEN GREATEST(snapshot_complete, ?) = 1 AND (
	        new_equality_delete_files + ? >= ?
	      ) THEN CASE
	        WHEN next_compaction_check_at IS NULL OR next_compaction_check_at > ? THEN ?
	        ELSE next_compaction_check_at END
	      ELSE next_compaction_check_at END,
	    new_data_files = new_data_files + ?,
	    new_equality_delete_files = new_equality_delete_files + ?,
	    last_write_at = CASE
	      WHEN ? = 1
	      THEN UTC_TIMESTAMP(6) ELSE last_write_at END,
	    next_inventory_check_at = CASE
	      WHEN ? = 1 AND GREATEST(snapshot_complete, ?) = 1
	      THEN UTC_TIMESTAMP(6) ELSE next_inventory_check_at END,
	    inventory_priority = CASE
	      WHEN ? = 1 AND GREATEST(snapshot_complete, ?) = 1
	      THEN GREATEST(inventory_priority, 50) ELSE inventory_priority END,
	    snapshot_complete = GREATEST(snapshot_complete, ?),
	    last_snapshot_id = CASE WHEN ? = 1 THEN ? ELSE last_snapshot_id END,
	    updated_at = UTC_TIMESTAMP(6)
	WHERE table_key = ?`
	if equalityDeleteFilesThreshold <= 0 {
		equalityDeleteFilesThreshold = int(^uint(0) >> 1)
	}
	due := nextCompaction.UTC()
	res, err := tx.ExecContext(ctx, stmt,
		boolInt(snapshotComplete),
		addedEqDeleteFiles, equalityDeleteFilesThreshold,
		due, due,
		addedDataFiles, addedEqDeleteFiles,
		boolInt(snapshotChanged),
		boolInt(snapshotChanged), boolInt(snapshotComplete),
		boolInt(snapshotChanged), boolInt(snapshotComplete),
		boolInt(snapshotComplete), boolInt(snapshotChanged), snapshotID,
		tableKey,
	)
	if err != nil {
		return false, err
	}
	if _, err := res.RowsAffected(); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Iceberg snapshot IDs are opaque identifiers, not ordered sequence numbers.
// A later snapshot may have a numerically smaller ID than the previous one.
func snapshotIDsDiffer(previous, current int64) bool {
	return current != 0 && current != previous
}

func (s *IcebergMaintenanceStore) SetSnapshotCompleteForOwner(
	ctx context.Context,
	ownerJobID string,
	complete bool,
	equalityDeleteFilesThreshold int,
	nextCompaction time.Time,
) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET snapshot_complete=?,
	    next_compaction_check_at=CASE
	      WHEN ?=1 AND new_equality_delete_files >= ? THEN CASE
	        WHEN next_compaction_check_at IS NULL OR next_compaction_check_at > ? THEN ?
	        ELSE next_compaction_check_at END
	      ELSE next_compaction_check_at END,
	    updated_at=UTC_TIMESTAMP(6)
	WHERE owner_job_id=?`,
		boolInt(complete), boolInt(complete), equalityDeleteFilesThreshold,
		nextCompaction.UTC(), nextCompaction.UTC(), ownerJobID,
	)
	return err
}

// DueStates filters to the coordinator's eligible owners before applying the
// page limit. Deleted or disabled owners must not occupy every page forever.
func (s *IcebergMaintenanceStore) DueStates(ctx context.Context, operation string, now time.Time, limit int, ownerIDs []string) ([]IcebergMaintenanceState, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	column, err := dueColumn(operation)
	if err != nil {
		return nil, err
	}
	if len(ownerIDs) == 0 {
		return []IcebergMaintenanceState{}, nil
	}
	ownerPlaceholders := strings.TrimSuffix(strings.Repeat("?,", len(ownerIDs)), ",")
	args := make([]any, 0, len(ownerIDs)+2)
	args = append(args, now.UTC())
	for _, ownerID := range ownerIDs {
		args = append(args, ownerID)
	}
	args = append(args, limit)
	query := fmt.Sprintf(`SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, inventory_snapshot_id, last_inventory_at, last_write_at, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, applicable_equality_delete_files, applicable_position_delete_files,
	 active_compactable_files, active_compactable_bytes, active_compaction_groups, partition_fields_json,
	 next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, inventory_lease_owner, inventory_lease_until, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at
	FROM iceberg_maintenance_state AS state
	WHERE snapshot_complete = 1 AND %s IS NOT NULL AND %s <= ?
	  AND owner_type IN ('job', 'monitor')
	  AND owner_job_id IN (%s)
	  AND (owner_type <> 'monitor' OR EXISTS (
	    SELECT 1 FROM iceberg_maintenance_monitors AS monitor
	    WHERE monitor.monitor_id=SUBSTRING(state.owner_job_id, 9) AND monitor.status='ACTIVE'
	  ))
	ORDER BY %s ASC, table_key ASC LIMIT ?`, column, column, ownerPlaceholders, column)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]IcebergMaintenanceState, 0, limit)
	for rows.Next() {
		state, err := scanMaintenanceState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	return out, rows.Err()
}

// ClaimPendingInventoryState claims one due inventory scan. The small, leased
// claim prevents a first inventory sweep from becoming an unbounded startup
// burst for large deployments. Explicit job-detail refresh requests receive a
// higher priority than the slow first-scan sweep. Within a priority, retry
// failed scans before an equally large bulk refresh keeps their errors stale.
// The background pass also revisits expired inventories without an explicit
// request: after a successful scan FinishInventoryClaim clears the request,
// and external writers do not emit Rivus commit signals to schedule another.
func (s *IcebergMaintenanceStore) ClaimPendingInventoryState(ctx context.Context, workerID string, now time.Time, lease time.Duration, minimumPriority int) (*IcebergMaintenanceState, error) {
	return s.claimPendingInventoryState(ctx, workerID, now, lease, minimumPriority, false)
}

func (s *IcebergMaintenanceStore) claimPendingInventoryState(ctx context.Context, workerID string, now time.Time, lease time.Duration, minimumPriority int, requireIdleTable bool) (*IcebergMaintenanceState, error) {
	if lease <= 0 {
		lease = 15 * time.Minute
	}
	if minimumPriority < 0 {
		minimumPriority = 0
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, inventory_snapshot_id, last_inventory_at, last_write_at, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, applicable_equality_delete_files, applicable_position_delete_files,
	 active_compactable_files, active_compactable_bytes, active_compaction_groups, partition_fields_json,
	 next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, inventory_lease_owner, inventory_lease_until, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at
	FROM iceberg_maintenance_state
	WHERE snapshot_complete=1
	  AND ((next_inventory_check_at IS NOT NULL AND next_inventory_check_at <= ?)
	    OR (?=0 AND next_inventory_check_at IS NULL
	      AND owner_type IN ('job','monitor')
	      AND (last_inventory_at IS NULL OR last_inventory_at < ?)))
	  AND inventory_priority >= ?
	  AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)
	  AND (?=0 OR lease_until IS NULL OR lease_until < ?)
	  AND (owner_type <> 'monitor' OR EXISTS (
	    SELECT 1 FROM iceberg_maintenance_monitors AS monitor
	    WHERE monitor.monitor_id=SUBSTRING(iceberg_maintenance_state.owner_job_id, 9) AND monitor.status='ACTIVE'
	  ))
	ORDER BY inventory_priority DESC, (last_error IS NOT NULL AND last_error<>'') DESC,
	 COALESCE(next_inventory_check_at, last_inventory_at, created_at) ASC, table_key
	LIMIT 1 FOR UPDATE SKIP LOCKED`, now.UTC(), minimumPriority, now.UTC().Add(-MaintenanceInventoryMaxAge), minimumPriority, now.UTC(), requireIdleTable, now.UTC())
	state, err := scanMaintenanceState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	until := now.Add(lease).UTC()
	res, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET inventory_lease_owner=?, inventory_lease_until=?, next_inventory_check_at=?, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND (next_inventory_check_at IS NULL OR next_inventory_check_at <= ?)
	  AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)
	  AND (?=0 OR lease_until IS NULL OR lease_until < ?)`, workerID, until, until, state.TableKey, now.UTC(), now.UTC(), requireIdleTable, now.UTC())
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("inventory state %s was lost while claiming", state.TableKey)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &state, nil
}

// FinishInventoryClaim clears a completed inventory scan unless Rivus CDC
// committed another snapshot while that scan was running. The claimed CDC
// snapshot is deliberately used instead of inventory_snapshot_id: a native or
// Spark maintenance commit legitimately creates a newer Iceberg snapshot, but
// it must not cause an endless inventory-refresh loop.
func (s *IcebergMaintenanceStore) FinishInventoryClaim(ctx context.Context, tableKey, workerID string, claimedLastSnapshotID int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET inventory_lease_owner=NULL, inventory_lease_until=NULL,
	    next_inventory_check_at=CASE
	      WHEN last_snapshot_id <> ? THEN UTC_TIMESTAMP(6)
	      ELSE NULL END,
	    inventory_priority=CASE
	      WHEN last_snapshot_id <> ? THEN GREATEST(inventory_priority, 50)
	      ELSE 0 END,
	    updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND inventory_lease_owner=?`, claimedLastSnapshotID, claimedLastSnapshotID, tableKey, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("inventory state %s lease is no longer owned by %s", tableKey, workerID)
	}
	return nil
}

// RequestInventoryRefresh schedules stale or changed tables for inventory.
func (s *IcebergMaintenanceStore) RequestInventoryRefresh(ctx context.Context, ownerJobID string, now time.Time, force bool) (int64, error) {
	ownerJobID = strings.TrimSpace(ownerJobID)
	if ownerJobID == "" {
		return 0, fmt.Errorf("maintenance inventory owner job id is required")
	}
	now = now.UTC()
	where := "owner_job_id=? AND snapshot_complete=1 AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)"
	args := []any{now, ownerJobID, now}
	if !force {
		where += " AND (last_inventory_at IS NULL OR last_inventory_at < ? OR (last_write_at IS NOT NULL AND last_write_at > last_inventory_at))"
		args = append(args, now.Add(-MaintenanceInventoryMaxAge))
	}
	query := `UPDATE iceberg_maintenance_state
		SET next_inventory_check_at=?, inventory_priority=100, updated_at=UTC_TIMESTAMP(6)
		WHERE ` + where
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RequestTableInventoryRefresh schedules one owner-qualified table. The
// maintenance monitor worker uses this when it first observes an ACTIVE
// monitor so a deleted/recreated monitor cannot accidentally reactivate stale
// tables that are no longer present in its explicit target list.
func (s *IcebergMaintenanceStore) RequestTableInventoryRefresh(ctx context.Context, tableKey, ownerJobID string, now time.Time) (int64, error) {
	tableKey = strings.TrimSpace(tableKey)
	ownerJobID = strings.TrimSpace(ownerJobID)
	if tableKey == "" || ownerJobID == "" {
		return 0, fmt.Errorf("maintenance inventory table key and owner id are required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET next_inventory_check_at=?, inventory_priority=100, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND owner_job_id=? AND snapshot_complete=1
	  AND (inventory_lease_until IS NULL OR inventory_lease_until < ?)`,
		now.UTC(), tableKey, ownerJobID, now.UTC())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *IcebergMaintenanceStore) RetryInventoryClaim(ctx context.Context, tableKey, workerID string, retryAt time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET inventory_lease_owner=NULL, inventory_lease_until=NULL, next_inventory_check_at=?, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND inventory_lease_owner=?`, retryAt.UTC(), tableKey, workerID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("inventory state %s lease is no longer owned by %s", tableKey, workerID)
	}
	return nil
}

func (s *IcebergMaintenanceStore) EnqueueTask(ctx context.Context, state IcebergMaintenanceState, operation string, priority int, scheduleWindow string, notBefore time.Time, payload map[string]any) (bool, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	rawIdempotency := fmt.Sprintf("%s|%s|%s", state.TableKey, operation, scheduleWindow)
	sum := sha256.Sum256([]byte(rawIdempotency))
	idempotency := hex.EncodeToString(sum[:])
	const stmt = `INSERT IGNORE INTO iceberg_maintenance_tasks (
	 idempotency_key, table_key, owner_job_id, operation, priority, status, attempt_count,
	 not_before, schedule_window, payload_json, created_at, updated_at
) SELECT ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)
WHERE EXISTS (
	SELECT 1 FROM iceberg_maintenance_state AS state
	WHERE state.table_key=? AND state.owner_job_id=?
	  AND (state.owner_type <> 'monitor' OR EXISTS (
	    SELECT 1 FROM iceberg_maintenance_monitors AS monitor
	    WHERE monitor.monitor_id=SUBSTRING(state.owner_job_id, 9) AND monitor.status='ACTIVE'
	  ))
)
AND NOT EXISTS (
	SELECT 1 FROM iceberg_maintenance_tasks AS active
	WHERE active.table_key=? AND active.operation=?
	  AND active.status IN ('queued','retry','leased')
)`
	res, err := s.db.ExecContext(ctx, stmt,
		idempotency, state.TableKey, state.OwnerJobID, operation, priority, notBefore.UTC(), scheduleWindow, string(payloadJSON),
		state.TableKey, state.OwnerJobID,
		state.TableKey, operation,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *IcebergMaintenanceStore) AdvanceSchedule(ctx context.Context, tableKey, operation string, next time.Time) error {
	column, err := dueColumn(operation)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`UPDATE iceberg_maintenance_state SET %s = ?, updated_at = UTC_TIMESTAMP(6) WHERE table_key = ?`, column)
	_, err = s.db.ExecContext(ctx, query, next.UTC(), tableKey)
	return err
}

func (s *IcebergMaintenanceStore) ClaimTasks(ctx context.Context, workerID string, now time.Time, lease time.Duration, limit int) ([]IcebergMaintenanceTask, error) {
	if err := s.RecoverExpiredMaintenanceLeases(ctx, now); err != nil {
		return nil, err
	}
	return s.claimTasksOwnershipSafe(ctx, workerID, now, lease, "", limit)
}

func (s *IcebergMaintenanceStore) RenewLease(ctx context.Context, taskID int64, workerID string, until time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks SET lease_until=?, updated_at=UTC_TIMESTAMP(6)
	WHERE id=? AND status='leased' AND lease_owner=?`, until.UTC(), taskID, workerID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("maintenance task %d lease is no longer owned by %s", taskID, workerID)
	}
	return nil
}

func (s *IcebergMaintenanceStore) FinishTask(ctx context.Context, taskID int64, workerID, status, lastError string, retryAt *time.Time) error {
	var res sql.Result
	var err error
	if status == MaintenanceTaskRetry {
		if retryAt == nil {
			return fmt.Errorf("retry task %d requires retry time", taskID)
		}
		res, err = s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
		SET status='retry', lease_owner=NULL, lease_until=NULL, not_before=?, last_error=?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status='leased' AND lease_owner=?`, retryAt.UTC(), nullString(lastError), taskID, workerID)
	} else {
		res, err = s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
		SET status=?, lease_owner=NULL, lease_until=NULL, last_error=?, updated_at=UTC_TIMESTAMP(6)
		WHERE id=? AND status='leased' AND lease_owner=?`, status, nullString(lastError), taskID, workerID)
	}
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("maintenance task %d lease is no longer owned by %s", taskID, workerID)
	}
	return nil
}

// RecordTableWrite records data-changing Iceberg snapshot activity, including
// external writers. Preserve newer CDC signals and never turn a repeated scan
// into a new write. This deliberately does not change last_snapshot_id, which
// is the CDC signal deduplication cursor.
func (s *IcebergMaintenanceStore) RecordTableWrite(ctx context.Context, tableKey string, written *time.Time) error {
	if written == nil || written.IsZero() {
		return nil
	}
	at := written.UTC()
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state
	SET last_write_at=?, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND (last_write_at IS NULL OR last_write_at < ?)
	  AND ? <= UTC_TIMESTAMP(6)`, at, tableKey, at, at)
	return err
}

// ClampOrphanSchedules applies shorter intervals to existing durable schedules.
// A recent write selects the active deadline; idle tables still get their own
// periodic cleanup. Existing earlier deadlines (including overdue work) win.
func (s *IcebergMaintenanceStore) ClampOrphanSchedules(ctx context.Context, ownerID string, now, activeSince, activeDeadline, idleDeadline time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state AS state
	SET next_orphan_check_at=LEAST(COALESCE(next_orphan_check_at, ?),
	    CASE WHEN last_write_at > ? AND last_write_at <= ? THEN ? ELSE ? END)
	WHERE owner_job_id=? AND snapshot_complete=1 AND owner_type IN ('job', 'monitor')
	  AND (owner_type <> 'monitor' OR EXISTS (
	    SELECT 1 FROM iceberg_maintenance_monitors AS monitor
	    WHERE monitor.monitor_id=SUBSTRING(state.owner_job_id, 9) AND monitor.status='ACTIVE'
	  ))`, idleDeadline.UTC(), activeSince.UTC(), now.UTC(), activeDeadline.UTC(), idleDeadline.UTC(), ownerID)
	return err
}

func (s *IcebergMaintenanceStore) UpdateInventory(
	ctx context.Context,
	tableKey string,
	snapshotID int64,
	dataFiles, smallFiles int,
	smallBytes int64,
	equalityDeletes, positionDeletes, applicableEqualityDeletes, applicablePositionDeletes, compactableFiles int,
	compactableBytes int64,
	compactionGroups int,
	partitionFields ...[]string,
) error {
	var partitionJSON any
	if len(partitionFields) > 0 && partitionFields[0] != nil {
		encoded, err := json.Marshal(partitionFields[0])
		if err != nil {
			return err
		}
		partitionJSON = string(encoded)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET
	 inventory_snapshot_id=?, last_inventory_at=UTC_TIMESTAMP(6), active_data_files=?, active_small_files=?, active_small_bytes=?,
	 active_equality_delete_files=?, active_position_delete_files=?,
	 applicable_equality_delete_files=?, applicable_position_delete_files=?, active_compactable_files=?,
	 active_compactable_bytes=?, active_compaction_groups=?, partition_fields_json=?, last_error=NULL, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=?`, snapshotID, dataFiles, smallFiles, smallBytes, equalityDeletes, positionDeletes,
		applicableEqualityDeletes, applicablePositionDeletes,
		compactableFiles, compactableBytes, compactionGroups, partitionJSON, tableKey)
	return err
}

// ScheduleCompactionCheck schedules work or promotes an existing queued task.
func (s *IcebergMaintenanceStore) ScheduleCompactionCheck(ctx context.Context, tableKey string, due time.Time, priority int) error {
	if priority <= 0 {
		priority = 10
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks
	SET priority=LEAST(priority, ?), updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=? AND operation='compact' AND status IN ('queued','retry')`, priority, tableKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state AS state
	SET next_compaction_check_at = CASE
	      WHEN snapshot_complete = 1 THEN CASE
	        WHEN next_compaction_check_at IS NULL OR next_compaction_check_at > ? THEN ?
	        ELSE next_compaction_check_at END
	      ELSE next_compaction_check_at END,
	    updated_at=UTC_TIMESTAMP(6)
	WHERE state.table_key=?
	  AND NOT EXISTS (
	    SELECT 1 FROM iceberg_maintenance_tasks AS active
	    WHERE active.table_key=state.table_key AND active.operation='compact'
	      AND active.status IN ('queued','retry','leased')
	  )`, due.UTC(), due.UTC(), tableKey); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *IcebergMaintenanceStore) MarkInventoryMissing(ctx context.Context, tableKey string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET
	 inventory_snapshot_id=0, last_inventory_at=UTC_TIMESTAMP(6), active_data_files=0,
	 active_small_files=0, active_small_bytes=0, active_equality_delete_files=0,
	 active_position_delete_files=0, applicable_equality_delete_files=0, applicable_position_delete_files=0,
	 active_compactable_files=0, active_compactable_bytes=0,
	 active_compaction_groups=0, partition_fields_json=NULL, last_error=NULL, updated_at=UTC_TIMESTAMP(6)
	WHERE table_key=?`, tableKey)
	return err
}

func (s *IcebergMaintenanceStore) RecordStateSuccess(ctx context.Context, tableKey, operation string, at time.Time, resetCompactionCounters bool) error {
	column := ""
	switch operation {
	case "compact":
		column = "last_compaction_at"
	case "expire_snapshots":
		column = "last_expire_at"
	case "remove_orphan_files":
		column = "last_orphan_at"
	default:
		return fmt.Errorf("unsupported maintenance operation %q", operation)
	}
	extra := ""
	if operation == "compact" && resetCompactionCounters {
		extra = ", new_data_files=0, new_equality_delete_files=0"
	}
	query := fmt.Sprintf(`UPDATE iceberg_maintenance_state SET %s=?, attempt_count=0, last_error=NULL%s, updated_at=UTC_TIMESTAMP(6) WHERE table_key=?`, column, extra)
	_, err := s.db.ExecContext(ctx, query, at.UTC(), tableKey)
	return err
}

func (s *IcebergMaintenanceStore) RecordStateError(ctx context.Context, tableKey, message string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET attempt_count=attempt_count+1, last_error=?, updated_at=UTC_TIMESTAMP(6) WHERE table_key=?`, nullString(message), tableKey)
	return err
}

func (s *IcebergMaintenanceStore) CreateRun(ctx context.Context, workerID string, taskCount int, at time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `INSERT INTO iceberg_maintenance_runs
	(worker_id,status,task_count,success_count,skipped_count,failed_count,started_at,created_at)
	VALUES (?, 'running', ?, 0, 0, 0, ?, ?)`, workerID, taskCount, at.UTC(), at.UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// CreateRunForTasks creates the run and its initial task results atomically.
// Owner-scoped history can therefore display active work before each operation
// finishes, and a run never appears with only a partial set of claimed tasks.
func (s *IcebergMaintenanceStore) CreateRunForTasks(ctx context.Context, workerID string, tasks []IcebergMaintenanceTask, at time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT INTO iceberg_maintenance_runs
	(worker_id,status,task_count,success_count,skipped_count,failed_count,started_at,created_at)
	VALUES (?, 'running', ?, 0, 0, 0, ?, ?)`, workerID, len(tasks), at.UTC(), at.UTC())
	if err != nil {
		return 0, err
	}
	runID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	const insert = `INSERT INTO iceberg_maintenance_results (
	 run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,
	 deleted_bytes,duration_ms,attempt,details_json,created_at
	) VALUES (?,?,?,?,?,'Awaiting worker execution','running',0,0,0,0,0,0,0,0,0,0,?,'{"phase":"waiting"}',?)`
	for _, task := range tasks {
		if task.ID <= 0 || task.AttemptCount <= 0 {
			return 0, fmt.Errorf("invalid claimed maintenance task id=%d attempt=%d", task.ID, task.AttemptCount)
		}
		if _, err := tx.ExecContext(ctx, insert, runID, task.ID, task.TableKey, task.Operation, "pending", task.AttemptCount, at.UTC()); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return runID, nil
}

func (s *IcebergMaintenanceStore) FinishRun(ctx context.Context, runID int64, success, skipped, failed int, at time.Time) error {
	status := "finished"
	if failed > 0 {
		status = "finished_with_errors"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_runs SET status=?, success_count=?, skipped_count=?, failed_count=?, finished_at=? WHERE id=?`,
		status, success, skipped, failed, at.UTC(), runID)
	return err
}

func (s *IcebergMaintenanceStore) InsertResult(ctx context.Context, result IcebergMaintenanceResult) error {
	detailsJSON, err := json.Marshal(result.Details)
	if err != nil {
		return err
	}
	if result.CreatedAt.IsZero() {
		result.CreatedAt = time.Now().UTC()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO iceberg_maintenance_results (
	 run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,
	 deleted_bytes,duration_ms,attempt,submission_id,details_json,error_text,created_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON DUPLICATE KEY UPDATE status=VALUES(status), engine=VALUES(engine), routing_reason=VALUES(routing_reason),
	 input_files=VALUES(input_files), input_bytes=VALUES(input_bytes), output_files=VALUES(output_files),
	 output_bytes=VALUES(output_bytes), delete_files=VALUES(delete_files), expired_snapshots=VALUES(expired_snapshots),
	 orphan_candidates=VALUES(orphan_candidates), deleted_files=VALUES(deleted_files), deleted_bytes=VALUES(deleted_bytes),
	 duration_ms=VALUES(duration_ms), submission_id=VALUES(submission_id), details_json=VALUES(details_json), error_text=VALUES(error_text)`,
		result.RunID, result.TaskID, result.TableKey, result.Operation, result.Engine, nullString(result.RoutingReason), result.Status,
		result.InputFiles, result.InputBytes, result.OutputFiles, result.OutputBytes, result.DeleteFiles, result.ExpiredSnapshots,
		result.OrphanCandidates, result.DeletedFiles, result.DeletedBytes, result.DurationMillis, result.Attempt,
		nullString(result.SubmissionID), string(detailsJSON), nullString(result.Error), result.CreatedAt.UTC())
	return err
}

func (s *IcebergMaintenanceStore) Summary(ctx context.Context, now time.Time) (IcebergMaintenanceSummary, error) {
	var out IcebergMaintenanceSummary
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(snapshot_complete=0),0) FROM iceberg_maintenance_state`).Scan(&out.Tables, &out.SnapshotBlocked); err != nil {
		return out, err
	}
	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT
		 COALESCE(SUM(status='queued'),0), COALESCE(SUM(status='retry'),0),
		 COALESCE(SUM(status='leased' AND lease_until > UTC_TIMESTAMP(6)),0), COALESCE(SUM(status='failed'),0),
	 MIN(CASE WHEN status IN ('queued','retry') THEN created_at END)
	FROM iceberg_maintenance_tasks`).Scan(&out.QueuedTasks, &out.RetryTasks, &out.ActiveLeases, &out.FailedTasks, &oldest); err != nil {
		return out, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT task.table_key)
	FROM iceberg_maintenance_tasks task
	JOIN (
		SELECT table_key, operation, MAX(id) AS id
		FROM iceberg_maintenance_tasks
		GROUP BY table_key, operation
	) latest ON latest.id=task.id
	WHERE task.status='failed'`).Scan(&out.FailedTables); err != nil {
		return out, err
	}
	if oldest.Valid {
		t := oldest.Time.UTC()
		out.OldestQueuedAt = &t
		out.OldestQueuedAgeSec = int64(now.UTC().Sub(t).Seconds())
		if out.OldestQueuedAgeSec < 0 {
			out.OldestQueuedAgeSec = 0
		}
	}
	return out, nil
}

func (s *IcebergMaintenanceStore) ListLatestFailures(ctx context.Context, limit int) ([]IcebergMaintenanceFailure, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, latestMaintenanceFailuresQuery+` ORDER BY task.updated_at DESC, task.id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IcebergMaintenanceFailure, 0, limit)
	for rows.Next() {
		failure, err := scanMaintenanceFailure(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, failure)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) GetLatestFailure(ctx context.Context, taskID int64) (*IcebergMaintenanceFailure, error) {
	failure, err := scanMaintenanceFailure(s.db.QueryRowContext(ctx, latestMaintenanceFailuresQuery+` AND task.id=?`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &failure, nil
}

const latestMaintenanceFailuresQuery = `SELECT
	task.id, task.table_key, state.catalog, state.namespace_name, state.table_name,
	state.owner_type, task.owner_job_id, task.operation, task.attempt_count,
	task.last_error, task.updated_at, task.priority, state.snapshot_complete,
	task.payload_json,
	CASE
		WHEN state.table_key IS NULL OR state.snapshot_complete=0 THEN 0
		WHEN state.owner_type='monitor' THEN EXISTS (
			SELECT 1 FROM iceberg_maintenance_monitors AS monitor
			WHERE monitor.monitor_id=SUBSTRING(state.owner_job_id, 9) AND monitor.status='ACTIVE'
		)
		ELSE 1
	END AS can_retry
FROM iceberg_maintenance_tasks AS task
JOIN (
	SELECT table_key, operation, MAX(id) AS id
	FROM iceberg_maintenance_tasks
	GROUP BY table_key, operation
) AS latest ON latest.id=task.id
LEFT JOIN iceberg_maintenance_state AS state
	ON state.table_key=task.table_key AND state.owner_job_id=task.owner_job_id
WHERE task.status='failed'`

func scanMaintenanceFailure(row rowScanner) (IcebergMaintenanceFailure, error) {
	var failure IcebergMaintenanceFailure
	var catalog, namespace, table, ownerType, lastError, payloadJSON sql.NullString
	var snapshotComplete sql.NullInt64
	var canRetry int
	err := row.Scan(
		&failure.TaskID,
		&failure.TableKey,
		&catalog,
		&namespace,
		&table,
		&ownerType,
		&failure.OwnerJobID,
		&failure.Operation,
		&failure.AttemptCount,
		&lastError,
		&failure.UpdatedAt,
		&failure.Priority,
		&snapshotComplete,
		&payloadJSON,
		&canRetry,
	)
	if err != nil {
		return failure, err
	}
	failure.Catalog = catalog.String
	failure.Namespace = namespace.String
	failure.Table = table.String
	failure.OwnerType = ownerType.String
	failure.LastError = lastError.String
	failure.SnapshotComplete = snapshotComplete.Valid && snapshotComplete.Int64 != 0
	failure.CanRetry = canRetry != 0
	failure.Payload = map[string]any{}
	if payloadJSON.Valid && strings.TrimSpace(payloadJSON.String) != "" {
		if err := json.Unmarshal([]byte(payloadJSON.String), &failure.Payload); err != nil {
			return failure, fmt.Errorf("decode maintenance task %d payload: %w", failure.TaskID, err)
		}
	}
	return failure, nil
}

func (s *IcebergMaintenanceStore) ListRuns(ctx context.Context, limit, offset int) ([]IcebergMaintenanceRun, error) {
	return s.ListRunsFiltered(ctx, IcebergMaintenanceRunFilter{}, limit, offset)
}

func (s *IcebergMaintenanceStore) ListRunsFiltered(ctx context.Context, filter IcebergMaintenanceRunFilter, limit, offset int) ([]IcebergMaintenanceRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	where, args := maintenanceRunFilterWhere(filter, false)
	query := `SELECT r.id,r.worker_id,r.status,r.task_count,r.success_count,r.skipped_count,r.failed_count,r.started_at,r.finished_at,r.created_at
	FROM iceberg_maintenance_runs r` + where + ` ORDER BY r.id DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IcebergMaintenanceRun
	for rows.Next() {
		var run IcebergMaintenanceRun
		var finished sql.NullTime
		if err := rows.Scan(&run.ID, &run.WorkerID, &run.Status, &run.TaskCount, &run.SuccessCount, &run.SkippedCount, &run.FailedCount, &run.StartedAt, &finished, &run.CreatedAt); err != nil {
			return nil, err
		}
		if finished.Valid {
			t := finished.Time.UTC()
			run.FinishedAt = &t
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) CountRunsFiltered(ctx context.Context, filter IcebergMaintenanceRunFilter) (int, error) {
	where, args := maintenanceRunFilterWhere(filter, false)
	var total int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_maintenance_runs r`+where, args...).Scan(&total)
	return total, err
}

func maintenanceRunFilterWhere(filter IcebergMaintenanceRunFilter, ownerScoped bool) (string, []any) {
	filter.OwnerJobID = strings.TrimSpace(filter.OwnerJobID)
	filter.Status = strings.TrimSpace(filter.Status)
	filter.Search = strings.TrimSpace(filter.Search)
	filter.Operation = strings.TrimSpace(filter.Operation)
	filter.Engine = strings.TrimSpace(filter.Engine)

	clauses := make([]string, 0, 5)
	args := make([]any, 0, 12)
	if filter.Status != "" {
		clauses = append(clauses, "r.status=?")
		args = append(args, filter.Status)
	}
	if filter.Search != "" {
		pattern := "%" + filter.Search + "%"
		if ownerScoped {
			clauses = append(clauses, `(r.worker_id LIKE ? OR CAST(r.id AS CHAR) LIKE ? OR EXISTS (
				SELECT 1 FROM iceberg_maintenance_results search_res
				JOIN iceberg_maintenance_tasks search_task ON search_task.id=search_res.task_id
				WHERE search_res.run_id=r.id AND search_task.owner_job_id=? AND search_res.table_key LIKE ?))`)
			args = append(args, pattern, pattern, filter.OwnerJobID, pattern)
		} else {
			clauses = append(clauses, `(r.worker_id LIKE ? OR CAST(r.id AS CHAR) LIKE ? OR EXISTS (
				SELECT 1 FROM iceberg_maintenance_results search_res
				WHERE search_res.run_id=r.id AND search_res.table_key LIKE ?))`)
			args = append(args, pattern, pattern, pattern)
		}
	}
	for _, item := range []struct {
		value  string
		column string
	}{
		{value: filter.Operation, column: "operation"},
		{value: filter.Engine, column: "engine"},
	} {
		if item.value == "" {
			continue
		}
		if ownerScoped {
			clauses = append(clauses, fmt.Sprintf(`EXISTS (
				SELECT 1 FROM iceberg_maintenance_results filter_res
				JOIN iceberg_maintenance_tasks filter_task ON filter_task.id=filter_res.task_id
				WHERE filter_res.run_id=r.id AND filter_task.owner_job_id=? AND filter_res.%s=?)`, item.column))
			args = append(args, filter.OwnerJobID, item.value)
		} else {
			clauses = append(clauses, fmt.Sprintf(`EXISTS (
				SELECT 1 FROM iceberg_maintenance_results filter_res
				WHERE filter_res.run_id=r.id AND filter_res.%s=?)`, item.column))
			args = append(args, item.value)
		}
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func (s *IcebergMaintenanceStore) ListResultsForRun(ctx context.Context, runID int64, limit int) ([]IcebergMaintenanceResult, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,deleted_bytes,duration_ms,attempt,
	 submission_id,details_json,error_text,created_at FROM iceberg_maintenance_results WHERE run_id=? ORDER BY id ASC LIMIT ?`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IcebergMaintenanceResult
	for rows.Next() {
		result, err := scanMaintenanceResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) GetState(ctx context.Context, tableKey string) (*IcebergMaintenanceState, error) {
	row := s.db.QueryRowContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, inventory_snapshot_id, last_inventory_at, last_write_at, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, applicable_equality_delete_files, applicable_position_delete_files,
	 active_compactable_files, active_compactable_bytes, active_compaction_groups, partition_fields_json,
	 next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, inventory_lease_owner, inventory_lease_until, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at FROM iceberg_maintenance_state WHERE table_key=?`, tableKey)
	state, err := scanMaintenanceState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanMaintenanceState(row rowScanner) (IcebergMaintenanceState, error) {
	var state IcebergMaintenanceState
	var snapshotComplete int
	var applicableEqualityDeletes, applicablePositionDeletes sql.NullInt64
	var lastInventory, lastWrite, nextCompaction, nextExpire, nextOrphan, lastCompaction, lastExpire, lastOrphan, inventoryLeaseUntil, leaseUntil sql.NullTime
	var inventoryLeaseOwner, leaseOwner, lastError, partitionJSON sql.NullString
	err := row.Scan(&state.TableKey, &state.Catalog, &state.Namespace, &state.Table, &state.OwnerType, &state.OwnerJobID,
		&snapshotComplete, &state.LastSnapshotID, &state.InventorySnapshotID, &lastInventory, &lastWrite, &state.NewDataFiles, &state.NewEqualityDeleteFiles,
		&state.ActiveDataFiles, &state.ActiveSmallFiles, &state.ActiveSmallBytes, &state.ActiveEqualityDeleteFiles,
		&state.ActivePositionDeleteFiles, &applicableEqualityDeletes, &applicablePositionDeletes,
		&state.ActiveCompactableFiles, &state.ActiveCompactableBytes, &state.ActiveCompactionGroups, &partitionJSON,
		&nextCompaction, &nextExpire, &nextOrphan,
		&lastCompaction, &lastExpire, &lastOrphan, &inventoryLeaseOwner, &inventoryLeaseUntil, &leaseOwner, &leaseUntil,
		&state.AttemptCount, &lastError, &state.CreatedAt, &state.UpdatedAt)
	if err != nil {
		return state, err
	}
	if partitionJSON.Valid {
		if err := json.Unmarshal([]byte(partitionJSON.String), &state.PartitionFields); err != nil {
			return state, fmt.Errorf("decode partition fields for %s: %w", state.TableKey, err)
		}
	}
	state.SnapshotComplete = snapshotComplete != 0
	if applicableEqualityDeletes.Valid {
		count := int(applicableEqualityDeletes.Int64)
		state.ApplicableEqualityDeleteFiles = &count
	}
	if applicablePositionDeletes.Valid {
		count := int(applicablePositionDeletes.Int64)
		state.ApplicablePositionDeleteFiles = &count
	}
	state.LastInventoryAt = nullTimePtr(lastInventory)
	state.LastWriteAt = nullTimePtr(lastWrite)
	state.NextCompactionCheckAt = nullTimePtr(nextCompaction)
	state.NextExpireCheckAt = nullTimePtr(nextExpire)
	state.NextOrphanCheckAt = nullTimePtr(nextOrphan)
	state.LastCompactionAt = nullTimePtr(lastCompaction)
	state.LastExpireAt = nullTimePtr(lastExpire)
	state.LastOrphanAt = nullTimePtr(lastOrphan)
	state.InventoryLeaseUntil = nullTimePtr(inventoryLeaseUntil)
	if inventoryLeaseOwner.Valid {
		state.InventoryLeaseOwner = inventoryLeaseOwner.String
	}
	state.LeaseUntil = nullTimePtr(leaseUntil)
	if leaseOwner.Valid {
		state.LeaseOwner = leaseOwner.String
	}
	if lastError.Valid {
		state.LastError = lastError.String
	}
	return state, nil
}

func scanMaintenanceResult(row rowScanner) (IcebergMaintenanceResult, error) {
	var result IcebergMaintenanceResult
	var routingReason, submissionID, detailsJSON, errorText sql.NullString
	err := row.Scan(&result.ID, &result.RunID, &result.TaskID, &result.TableKey, &result.Operation, &result.Engine,
		&routingReason, &result.Status, &result.InputFiles, &result.InputBytes, &result.OutputFiles, &result.OutputBytes,
		&result.DeleteFiles, &result.ExpiredSnapshots, &result.OrphanCandidates, &result.DeletedFiles, &result.DeletedBytes,
		&result.DurationMillis, &result.Attempt, &submissionID, &detailsJSON, &errorText, &result.CreatedAt)
	if err != nil {
		return result, err
	}
	if routingReason.Valid {
		result.RoutingReason = routingReason.String
	}
	if submissionID.Valid {
		result.SubmissionID = submissionID.String
	}
	if detailsJSON.Valid && strings.TrimSpace(detailsJSON.String) != "" {
		_ = json.Unmarshal([]byte(detailsJSON.String), &result.Details)
	}
	if errorText.Valid {
		result.Error = errorText.String
	}
	return result, nil
}

func dueColumn(operation string) (string, error) {
	switch operation {
	case "compact":
		return "next_compaction_check_at", nil
	case "expire_snapshots":
		return "next_expire_check_at", nil
	case "remove_orphan_files":
		return "next_orphan_check_at", nil
	default:
		return "", fmt.Errorf("unsupported maintenance operation %q", operation)
	}
}

func nullTimePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time.UTC()
	return &t
}

func nullString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func firstNonEmptyMeta(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
