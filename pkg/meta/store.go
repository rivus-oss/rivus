package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Offset struct {
	BinlogFile string
	BinlogPos  uint32
	UpdatedAt  time.Time
}

type SnapshotState struct {
	JobID     string
	StartFile string
	StartPos  uint32
	Done      bool
	UpdatedAt time.Time
	// Version 1 proves the sink saved completion after all required writes.
	// Legacy done=1 rows have version 0 and cannot safely authorize CDC.
	CompletionVersion int
}

func (s *SnapshotState) CompletionConfirmed() bool {
	return s != nil && s.Done && s.CompletionVersion == 1
}

type SnapshotProgress struct {
	JobID      string
	TableName  string
	NextOffset int64
	CursorJSON string
	UpdatedAt  time.Time
}

type OffsetStore interface {
	GetOffset(ctx context.Context, jobID string) (*Offset, error)
	SaveOffset(ctx context.Context, jobID string, offset Offset) error

	GetSnapshotState(ctx context.Context, jobID string) (*SnapshotState, error)
	SaveSnapshotStart(ctx context.Context, jobID string, start Offset) error
	// Sink-only: call after all required snapshot writes are committed, before
	// completion ACK. Missing/legacy state must fail; confirmed calls repeat safely.
	MarkSnapshotDone(ctx context.Context, jobID string) error
	GetSnapshotProgress(ctx context.Context, jobID string) (*SnapshotProgress, error)
	SaveSnapshotProgress(ctx context.Context, jobID string, tableName string, nextOffset int64, cursorJSON string) error
	ClearSnapshotProgress(ctx context.Context, jobID string) error

	// NEW: buat bersihin meta saat delete job
	DeleteJobState(ctx context.Context, jobID string) error
}

type MySQLOffsetStore struct {
	db     *sql.DB
	runner sqlRunner
	target string
}

func NewMySQLOffsetStore(dsn string) (*MySQLOffsetStore, error) {
	target, err := mysqlStoreTarget(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)

	return &MySQLOffsetStore{db: db, target: target}, nil
}

func (s *MySQLOffsetStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *MySQLOffsetStore) Init(ctx context.Context) error {
	ddl := `
CREATE TABLE IF NOT EXISTS job_offsets (
  job_id      VARCHAR(255) NOT NULL PRIMARY KEY,
  binlog_file VARCHAR(255) NOT NULL,
  binlog_pos  BIGINT NOT NULL,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	if _, err := s.sqlRunner().ExecContext(ctx, ddl); err != nil {
		return err
	}

	ddl2 := `
CREATE TABLE IF NOT EXISTS job_snapshots (
  job_id      VARCHAR(255) NOT NULL PRIMARY KEY,
  start_file  VARCHAR(255) NOT NULL,
  start_pos   BIGINT NOT NULL,
  done        TINYINT(1) NOT NULL DEFAULT 0,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	if _, err := s.sqlRunner().ExecContext(ctx, ddl2); err != nil {
		return err
	}
	if _, err := s.sqlRunner().ExecContext(ctx, `ALTER TABLE job_snapshots ADD COLUMN completion_version TINYINT NOT NULL DEFAULT 0 AFTER done`); err != nil && !isDuplicateColumnError(err) {
		return err
	}

	ddl3 := `
CREATE TABLE IF NOT EXISTS job_snapshot_progress (
  job_id       VARCHAR(255) NOT NULL PRIMARY KEY,
  table_name   VARCHAR(255) NOT NULL,
  next_offset  BIGINT NOT NULL,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);`
	if _, err := s.sqlRunner().ExecContext(ctx, ddl3); err != nil {
		return err
	}

	if _, err := s.sqlRunner().ExecContext(ctx, `ALTER TABLE job_snapshot_progress ADD COLUMN cursor_json LONGTEXT NULL AFTER next_offset`); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	if _, err := s.sqlRunner().ExecContext(ctx, snapshotScopeDDL); err != nil {
		return err
	}
	if _, err := s.sqlRunner().ExecContext(ctx, `CREATE TABLE IF NOT EXISTS iceberg_commit_intents (
		operation_id CHAR(36) NOT NULL PRIMARY KEY,
		job_id VARCHAR(255) NOT NULL,
		state_key VARCHAR(255) NOT NULL,
		batch_id VARCHAR(512) NOT NULL,
		kind VARCHAR(16) NOT NULL,
		source_start_file VARCHAR(255) NOT NULL DEFAULT '',
		source_start_pos BIGINT NOT NULL DEFAULT 0,
		source_end_file VARCHAR(255) NOT NULL DEFAULT '',
		source_end_pos BIGINT NOT NULL DEFAULT 0,
		snapshot_table VARCHAR(255) NOT NULL DEFAULT '',
		snapshot_start_offset BIGINT NOT NULL DEFAULT 0,
		snapshot_end_offset BIGINT NOT NULL DEFAULT 0,
		status VARCHAR(16) NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_iceberg_commit_state (state_key(191), status)
	)`); err != nil {
		return err
	}

	// Defaults preserve old intents; absent identity must continue to fail closed.
	for _, column := range []string{"source_table", "target_namespace", "target_table", "operation", "target_uuid", "trino_query_id"} {
		if _, err := s.sqlRunner().ExecContext(ctx, "ALTER TABLE iceberg_commit_intents ADD COLUMN "+column+" VARCHAR(255) NOT NULL DEFAULT ''"); err != nil && !isDuplicateColumnError(err) {
			return err
		}
	}

	if _, err := s.sqlRunner().ExecContext(ctx, "ALTER TABLE iceberg_commit_intents ADD COLUMN replay_keys LONGTEXT NULL"); err != nil && !isDuplicateColumnError(err) {
		return err
	}
	if _, err := s.sqlRunner().ExecContext(ctx, "CREATE INDEX idx_iceberg_commit_job ON iceberg_commit_intents (job_id)"); err != nil && !isDuplicateKeyNameError(err) {
		return err
	}

	return nil
}

func (s *MySQLOffsetStore) GetOffset(ctx context.Context, jobID string) (*Offset, error) {
	const q = `SELECT binlog_file, binlog_pos, updated_at FROM job_offsets WHERE job_id = ?`
	row := s.sqlRunner().QueryRowContext(ctx, q, jobID)

	var off Offset
	if err := row.Scan(&off.BinlogFile, &off.BinlogPos, &off.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &off, nil
}

func (s *MySQLOffsetStore) SaveOffset(ctx context.Context, jobID string, off Offset) error {
	const stmt = `
INSERT INTO job_offsets (job_id, binlog_file, binlog_pos, updated_at)
VALUES (?, ?, ?, NOW())
ON DUPLICATE KEY UPDATE
  binlog_file = VALUES(binlog_file),
  binlog_pos = VALUES(binlog_pos),
  updated_at = NOW();
`
	_, err := s.sqlRunner().ExecContext(ctx, stmt, jobID, off.BinlogFile, off.BinlogPos)
	return err
}

func (s *MySQLOffsetStore) GetSnapshotState(ctx context.Context, jobID string) (*SnapshotState, error) {
	const q = `SELECT job_id, start_file, start_pos, done, updated_at, completion_version FROM job_snapshots WHERE job_id = ?`
	row := s.sqlRunner().QueryRowContext(ctx, q, jobID)

	var st SnapshotState
	var doneInt int
	if err := row.Scan(&st.JobID, &st.StartFile, &st.StartPos, &doneInt, &st.UpdatedAt, &st.CompletionVersion); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	st.Done = doneInt != 0
	return &st, nil
}

func (s *MySQLOffsetStore) SaveSnapshotStart(ctx context.Context, jobID string, start Offset) error {
	const stmt = `
INSERT INTO job_snapshots (job_id, start_file, start_pos, done, updated_at)
VALUES (?, ?, ?, 0, NOW())
ON DUPLICATE KEY UPDATE
  start_file = VALUES(start_file),
  start_pos = VALUES(start_pos),
  done = 0,
  completion_version = 0,
  updated_at = NOW();
`
	_, err := s.sqlRunner().ExecContext(ctx, stmt, jobID, start.BinlogFile, start.BinlogPos)
	return err
}

func (s *MySQLOffsetStore) MarkSnapshotDone(ctx context.Context, jobID string) error {
	// Called by the sink only after its completion barrier succeeds. Keep
	// progress intact until the source receives the ACK. Do not upgrade an
	// unsafe legacy done=1 row by merely flushing an empty restarted sink.
	const stmt = `
UPDATE job_snapshots
SET done = 1, completion_version = 1, updated_at = NOW()
WHERE job_id = ? AND done = 0
  AND NOT EXISTS (SELECT 1 FROM iceberg_commit_intents WHERE state_key = ? AND status <> 'COMMITTED');
`
	result, err := s.sqlRunner().ExecContext(ctx, stmt, jobID, jobID)
	if err != nil {
		return fmt.Errorf("mark snapshot done: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("snapshot completion affected rows: %w", err)
	}
	if count == 1 {
		return nil
	}
	st, err := s.GetSnapshotState(ctx, jobID)
	if err != nil {
		return fmt.Errorf("verify snapshot completion: %w", err)
	}
	if st.CompletionConfirmed() {
		return nil
	}
	return fmt.Errorf("snapshot completion BLOCKED for state=%s: missing start, legacy completion, or unresolved commit intent", jobID)
}

func (s *MySQLOffsetStore) GetSnapshotProgress(ctx context.Context, jobID string) (*SnapshotProgress, error) {
	const q = `SELECT job_id, table_name, next_offset, cursor_json, updated_at FROM job_snapshot_progress WHERE job_id = ?`
	row := s.sqlRunner().QueryRowContext(ctx, q, jobID)

	var st SnapshotProgress
	var cursorJSON sql.NullString
	if err := row.Scan(&st.JobID, &st.TableName, &st.NextOffset, &cursorJSON, &st.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if cursorJSON.Valid {
		st.CursorJSON = cursorJSON.String
	}
	return &st, nil
}

func (s *MySQLOffsetStore) SaveSnapshotProgress(ctx context.Context, jobID string, tableName string, nextOffset int64, cursorJSON string) error {
	const stmt = `
INSERT INTO job_snapshot_progress (job_id, table_name, next_offset, cursor_json, updated_at)
VALUES (?, ?, ?, ?, NOW())
ON DUPLICATE KEY UPDATE
  table_name = VALUES(table_name),
  next_offset = VALUES(next_offset),
  cursor_json = VALUES(cursor_json),
  updated_at = NOW();
`
	_, err := s.sqlRunner().ExecContext(ctx, stmt, jobID, tableName, nextOffset, nullableString(cursorJSON))
	if err != nil {
		return err
	}
	// An ACKed Iceberg batch is safe to forget only after this durable
	// snapshot progress covers its full row range. If cleanup fails, startup
	// can still compare the retained COMMITTED intent with saved progress.
	_, err = s.sqlRunner().ExecContext(ctx, `DELETE FROM iceberg_commit_intents WHERE state_key = ? AND kind = 'snapshot' AND status = 'COMMITTED' AND snapshot_table = ? AND snapshot_end_offset <= ?`, jobID, tableName, nextOffset)
	return err
}

func (s *MySQLOffsetStore) ClearSnapshotProgress(ctx context.Context, jobID string) error {
	_, err := s.sqlRunner().ExecContext(ctx, `DELETE FROM job_snapshot_progress WHERE job_id = ?`, jobID)
	return err
}

func (s *MySQLOffsetStore) DeleteJobState(ctx context.Context, jobID string) error {
	if s.runner != nil {
		return deleteJobState(ctx, s.runner, jobID)
	}
	// Serializable + locking read prevents an intent INSERT from racing the reset.
	// All retained intents protect their checkpoints, including COMMITTED intents
	// whose source acknowledgement has not yet become durable.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin job state cleanup %s: %w", jobID, err)
	}
	defer tx.Rollback()
	if err := deleteJobState(ctx, tx, jobID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job state cleanup %s: %w", jobID, err)
	}
	return nil
}

func deleteJobState(ctx context.Context, tx sqlRunner, jobID string) error {
	if err := checkJobStateCleanup(ctx, tx, jobID, true); err != nil {
		return err
	}
	for _, table := range []string{"job_snapshots", "job_snapshot_progress", "job_offsets"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE job_id = ?", jobID); err != nil {
			return fmt.Errorf("delete %s key=%s: %w", table, jobID, err)
		}
	}
	return nil
}

// JobStateCleanupChecker supports a non-mutating admission check. Cleanup
// must still repeat the check under its transaction lock to close the race.
type JobStateCleanupChecker interface {
	CheckJobStateCleanup(context.Context, string) error
}

var ErrJobStateCleanupBlocked = errors.New("job state cleanup BLOCKED")

func (s *MySQLOffsetStore) CheckJobStateCleanup(ctx context.Context, key string) error {
	return checkJobStateCleanup(ctx, s.sqlRunner(), key, false)
}

func checkJobStateCleanup(ctx context.Context, runner sqlRunner, jobID string, lock bool) error {
	query := `SELECT operation_id FROM iceberg_commit_intents WHERE state_key = ? OR job_id = ? LIMIT 1`
	if lock {
		query += " FOR UPDATE"
	}
	var operationID string
	err := runner.QueryRowContext(ctx, query, jobID, jobID).Scan(&operationID)
	if err == nil {
		return fmt.Errorf("%w key=%s: retained Iceberg commit intent operation=%s requires recovery", ErrJobStateCleanupBlocked, jobID, operationID)
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("check Iceberg intents before cleanup %s: %w", jobID, err)
	}
	return nil
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func isDuplicateColumnError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exists")
}

func isDuplicateKeyNameError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists")
}
