package meta

import (
	"context"
	"database/sql"
	"time"
)

// CleanupResult deliberately excludes task payloads and executor details, which
// can contain connection properties. Nil deletion counts mean unmeasured, not zero.
type CleanupResult struct {
	ID               int64     `json:"id"`
	RunID            int64     `json:"run_id"`
	Status           string    `json:"status"`
	Engine           string    `json:"engine"`
	Reason           string    `json:"reason,omitempty"`
	Error            string    `json:"error,omitempty"`
	DeletedFiles     *int      `json:"deleted_files"`
	DeletedBytes     *int64    `json:"deleted_bytes"`
	ExpiredSnapshots int       `json:"expired_snapshots"`
	DurationMillis   int64     `json:"duration_ms"`
	DryRun           bool      `json:"dry_run"`
	CreatedAt        time.Time `json:"created_at"`
}

type CleanupTaskView struct {
	TableKey      string         `json:"table_key"`
	Operation     string         `json:"operation"`
	TaskID        int64          `json:"task_id"`
	Status        string         `json:"status"`
	NotBefore     time.Time      `json:"not_before"`
	LeaseUntil    *time.Time     `json:"lease_until,omitempty"`
	LastError     string         `json:"last_error,omitempty"`
	LastSuccessAt *time.Time     `json:"last_success_at"`
	LastResult    *CleanupResult `json:"last_result"`
}

// ListCleanupTasksForOwner loads both operations in one bounded query. Indexed
// latest-task lookups avoid transferring the owner's entire task history on
// every dashboard refresh. Ownership is checked for both tasks and table state.
func (s *IcebergMaintenanceStore) ListCleanupTasksForOwner(ctx context.Context, owner string, limit int) ([]CleanupTaskView, error) {
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT st.table_key, ops.operation,
	 COALESCE(t.id,0), COALESCE(t.status,''), t.not_before, t.lease_until, t.last_error,
	 (SELECT success.updated_at FROM iceberg_maintenance_tasks success
	  JOIN iceberg_maintenance_results sr ON sr.task_id=success.id AND sr.attempt=success.attempt_count
	  WHERE success.owner_job_id=st.owner_job_id AND success.table_key=st.table_key
	   AND success.operation=ops.operation AND success.status='succeeded'
	   AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(sr.details_json,'$.dry_run')),'false') <> 'true'
	  ORDER BY success.id DESC LIMIT 1),
	 r.id, r.run_id, r.status, r.engine, r.routing_reason, r.error_text,
	 r.deleted_files, r.deleted_bytes, r.expired_snapshots, r.duration_ms, r.created_at,
	 COALESCE(JSON_UNQUOTE(JSON_EXTRACT(r.details_json,'$.dry_run')),'false')='true'
	FROM (SELECT table_key, owner_job_id FROM iceberg_maintenance_state
	 WHERE owner_job_id=? ORDER BY (last_error IS NOT NULL AND last_error<>'') DESC,
	 namespace_name, table_name, catalog, table_key LIMIT ?) st
	CROSS JOIN (SELECT 'remove_orphan_files' AS operation UNION ALL SELECT 'expire_snapshots') ops
	LEFT JOIN iceberg_maintenance_tasks t ON t.id=(
	 SELECT latest.id FROM iceberg_maintenance_tasks latest
	 WHERE latest.owner_job_id=st.owner_job_id AND latest.table_key=st.table_key AND latest.operation=ops.operation
	 ORDER BY latest.id DESC LIMIT 1)
	LEFT JOIN iceberg_maintenance_results r ON r.task_id=t.id AND r.attempt=t.attempt_count`, owner, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	views := make([]CleanupTaskView, 0)
	for rows.Next() {
		var v CleanupTaskView
		var notBefore, lease, success, created sql.NullTime
		var taskError, status, engine, reason, resultError sql.NullString
		var id, runID, files, bytes, expired, duration sql.NullInt64
		var dryRun bool
		if err := rows.Scan(&v.TableKey, &v.Operation, &v.TaskID, &v.Status, &notBefore, &lease, &taskError, &success,
			&id, &runID, &status, &engine, &reason, &resultError, &files, &bytes, &expired, &duration, &created, &dryRun); err != nil {
			return nil, err
		}
		v.NotBefore = notBefore.Time
		v.LeaseUntil = nullTimePtr(lease)
		v.LastSuccessAt = nullTimePtr(success)
		v.LastError = taskError.String
		if id.Valid {
			v.LastResult = cleanupResult(IcebergMaintenanceResult{ID: id.Int64, RunID: runID.Int64,
				Operation: v.Operation, Status: status.String, Engine: engine.String, RoutingReason: reason.String,
				Error: resultError.String, DeletedFiles: int(files.Int64), DeletedBytes: bytes.Int64,
				ExpiredSnapshots: int(expired.Int64), DurationMillis: duration.Int64, CreatedAt: created.Time})
			v.LastResult.DryRun = dryRun
		}
		views = append(views, v)
	}
	return views, rows.Err()
}

func cleanupResult(r IcebergMaintenanceResult) *CleanupResult {
	dryRun, _ := r.Details["dry_run"].(bool)
	v := &CleanupResult{ID: r.ID, RunID: r.RunID, Status: r.Status, Engine: r.Engine,
		Reason: r.RoutingReason, Error: r.Error, ExpiredSnapshots: r.ExpiredSnapshots,
		DurationMillis: r.DurationMillis, CreatedAt: r.CreatedAt, DryRun: dryRun}
	// Only native orphan cleanup currently measures physical deletions. Snapshot
	// expiration can reclaim storage but its zero-valued counters are unmeasured.
	if r.Operation == "remove_orphan_files" && r.Engine == "native" && r.Status == "succeeded" {
		v.DeletedFiles, v.DeletedBytes = &r.DeletedFiles, &r.DeletedBytes
	}
	return v
}

func (s *IcebergMaintenanceStore) ListCleanupHistory(ctx context.Context, owner, tableKey, operation string, limit int) ([]CleanupResult, error) {
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.run_id,r.task_id,r.table_key,r.operation,r.engine,r.routing_reason,r.status,
	 r.input_files,r.input_bytes,r.output_files,r.output_bytes,r.delete_files,r.expired_snapshots,r.orphan_candidates,
	 r.deleted_files,r.deleted_bytes,r.duration_ms,r.attempt,r.submission_id,r.details_json,r.error_text,r.created_at
	FROM iceberg_maintenance_results r
	JOIN iceberg_maintenance_tasks t ON t.id=r.task_id AND t.owner_job_id=?
	JOIN iceberg_maintenance_state st ON st.table_key=r.table_key AND st.owner_job_id=t.owner_job_id
	WHERE r.table_key=? AND r.operation=? ORDER BY r.created_at DESC, r.id DESC LIMIT ?`, owner, tableKey, operation, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]CleanupResult, 0)
	for rows.Next() {
		r, err := scanMaintenanceResult(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, *cleanupResult(r))
	}
	return results, rows.Err()
}
