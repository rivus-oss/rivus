package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// Snapshot scopes survive checkpoint resets. A new submission gets a new plan;
// a restart, retry or snapshot-to-CDC handoff keeps the original plan.
type SnapshotScopePlanStore interface {
	GetSnapshotScopePlan(context.Context, string, string) (string, error)
	SaveSnapshotScopePlan(context.Context, string, string, string) error
}

const snapshotScopeDDL = `CREATE TABLE IF NOT EXISTS job_snapshot_scopes (
	state_key VARCHAR(255) NOT NULL,
	submission_id VARCHAR(64) NOT NULL,
	plan_json LONGTEXT NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (state_key, submission_id)
)`

func (s *MySQLOffsetStore) GetSnapshotScopePlan(ctx context.Context, key, submission string) (string, error) {
	var payload string
	err := s.sqlRunner().QueryRowContext(ctx, `SELECT plan_json FROM job_snapshot_scopes WHERE state_key = ? AND submission_id = ?`, key, submission).Scan(&payload)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return payload, err
}

func (s *MySQLOffsetStore) SaveSnapshotScopePlan(ctx context.Context, key, submission, payload string) error {
	if key == "" || submission == "" || !json.Valid([]byte(payload)) {
		return fmt.Errorf("snapshot scope requires state key, submission ID and valid JSON")
	}
	// First writer wins. Callers re-read the authoritative plan before applying it.
	_, err := s.sqlRunner().ExecContext(ctx, `INSERT INTO job_snapshot_scopes (state_key, submission_id, plan_json) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE state_key = VALUES(state_key)`, key, submission, payload)
	return err
}

func (s *FencedOffsetStore) SaveSnapshotScopePlan(ctx context.Context, key, submission, payload string) error {
	if submission != s.ownership.SubmissionID {
		return ErrWorkerOwnershipLost
	}
	return s.write(ctx, key, func(store *MySQLOffsetStore) error {
		return store.SaveSnapshotScopePlan(ctx, key, submission, payload)
	})
}
