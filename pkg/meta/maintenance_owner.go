package meta

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// IcebergMaintenanceOwnerSummary is the small, owner-scoped queue snapshot
// used by the job-details UI. It is intentionally computed from durable MySQL
// state rather than worker process memory.
type IcebergMaintenanceOwnerSummary struct {
	Tables                    int        `json:"tables"`
	InventoriedTables         int        `json:"inventoried_tables"`
	InventoryErrors           int        `json:"inventory_errors"`
	LatestInventoryAt         *time.Time `json:"latest_inventory_at,omitempty"`
	ActiveDataFiles           int        `json:"active_data_files"`
	ActiveSmallFiles          int        `json:"active_small_files"`
	ActiveSmallBytes          int64      `json:"active_small_bytes"`
	EqualityDeletes           int        `json:"active_equality_delete_files"`
	PositionDeletes           int        `json:"active_position_delete_files"`
	ApplicableEqualityDeletes int        `json:"applicable_equality_delete_files"`
	ApplicablePositionDeletes int        `json:"applicable_position_delete_files"`
	ApplicableCountsPending   int        `json:"applicable_counts_pending"`
	Blocked                   int        `json:"snapshot_blocked"`
	QueuedTasks               int        `json:"queued_tasks"`
	RetryTasks                int        `json:"retry_tasks"`
	ActiveLeases              int        `json:"active_leases"`
	FailedTasks               int        `json:"failed_tasks"`
	FailedTables              int        `json:"failed_tables"`
	OldestQueuedAt            *time.Time `json:"oldest_queued_at,omitempty"`
	OldestQueuedAgeSec        int64      `json:"oldest_queued_age_seconds"`
}

func (s *IcebergMaintenanceStore) ListStatesForOwner(ctx context.Context, ownerJobID string, limit int) ([]IcebergMaintenanceState, error) {
	ownerJobID = strings.TrimSpace(ownerJobID)
	if ownerJobID == "" {
		return []IcebergMaintenanceState{}, nil
	}
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	// Prioritize current errors when the owner has more tables than the detail
	// response limit. Aggregate counts below still cover all tables.
	rows, err := s.db.QueryContext(ctx, `SELECT table_key, catalog, namespace_name, table_name, owner_type, owner_job_id,
	 snapshot_complete, last_snapshot_id, inventory_snapshot_id, last_inventory_at, last_write_at, new_data_files, new_equality_delete_files,
	 active_data_files, active_small_files, active_small_bytes, active_equality_delete_files,
	 active_position_delete_files, applicable_equality_delete_files, applicable_position_delete_files,
	 active_compactable_files, active_compactable_bytes, active_compaction_groups, partition_fields_json,
	 next_compaction_check_at, next_expire_check_at, next_orphan_check_at,
	 last_compaction_at, last_expire_at, last_orphan_at, inventory_lease_owner, inventory_lease_until, lease_owner, lease_until,
	 attempt_count, last_error, created_at, updated_at
	FROM iceberg_maintenance_state
	WHERE owner_job_id=?
	ORDER BY (last_error IS NOT NULL AND last_error<>'') DESC, namespace_name, table_name, catalog, table_key
	LIMIT ?`, ownerJobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IcebergMaintenanceState, 0)
	for rows.Next() {
		state, err := scanMaintenanceState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, state)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) SummaryForOwner(ctx context.Context, ownerJobID string) (IcebergMaintenanceOwnerSummary, error) {
	var out IcebergMaintenanceOwnerSummary
	ownerJobID = strings.TrimSpace(ownerJobID)
	if ownerJobID == "" {
		return out, nil
	}

	var latestInventory sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
	 COALESCE(SUM(last_inventory_at IS NOT NULL),0),
	 COALESCE(SUM(last_error IS NOT NULL AND last_error<>''),0),
	 COALESCE(SUM(snapshot_complete=0),0),
	 MAX(last_inventory_at),
	 COALESCE(SUM(active_data_files),0),
	 COALESCE(SUM(active_small_files),0),
	 COALESCE(SUM(active_small_bytes),0),
	 COALESCE(SUM(active_equality_delete_files),0),
	 COALESCE(SUM(active_position_delete_files),0),
	 COALESCE(SUM(applicable_equality_delete_files),0),
	 COALESCE(SUM(applicable_position_delete_files),0),
	 COALESCE(SUM(last_inventory_at IS NOT NULL AND (applicable_equality_delete_files IS NULL OR applicable_position_delete_files IS NULL)),0)
	FROM iceberg_maintenance_state WHERE owner_job_id=?`, ownerJobID).Scan(
		&out.Tables, &out.InventoriedTables, &out.InventoryErrors, &out.Blocked, &latestInventory,
		&out.ActiveDataFiles, &out.ActiveSmallFiles, &out.ActiveSmallBytes,
		&out.EqualityDeletes, &out.PositionDeletes,
		&out.ApplicableEqualityDeletes, &out.ApplicablePositionDeletes, &out.ApplicableCountsPending,
	); err != nil {
		return out, err
	}
	if latestInventory.Valid {
		out.LatestInventoryAt = &latestInventory.Time
	}

	var oldest sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT
	 COALESCE(SUM(status='queued'),0),
	 COALESCE(SUM(status='retry'),0),
	 COALESCE(SUM(status='leased' AND lease_until > UTC_TIMESTAMP(6)),0),
	 COALESCE(SUM(status='failed'),0),
	 MIN(CASE WHEN status IN ('queued','retry') THEN created_at END)
	FROM iceberg_maintenance_tasks WHERE owner_job_id=?`, ownerJobID).Scan(
		&out.QueuedTasks, &out.RetryTasks, &out.ActiveLeases, &out.FailedTasks, &oldest,
	); err != nil && err != sql.ErrNoRows {
		return out, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT task.table_key)
	FROM iceberg_maintenance_tasks task
	JOIN (
		SELECT table_key, operation, MAX(id) AS id
		FROM iceberg_maintenance_tasks
		WHERE owner_job_id=?
		GROUP BY table_key, operation
	) latest ON latest.id=task.id
	WHERE task.status='failed'`, ownerJobID).Scan(&out.FailedTables); err != nil {
		return out, err
	}
	if oldest.Valid {
		t := oldest.Time.UTC()
		out.OldestQueuedAt = &t
		out.OldestQueuedAgeSec = int64(time.Since(t).Seconds())
		if out.OldestQueuedAgeSec < 0 {
			out.OldestQueuedAgeSec = 0
		}
	}
	return out, nil
}

// ListRunsForOwner returns only the portion of each durable worker run that
// belongs to one Rivus job. A worker run may contain tasks from multiple jobs,
// so the per-status counts are recomputed from that job's results.
func (s *IcebergMaintenanceStore) ListRunsForOwner(ctx context.Context, ownerJobID string, limit, offset int) ([]IcebergMaintenanceRun, error) {
	return s.ListRunsForOwnerFiltered(ctx, IcebergMaintenanceRunFilter{OwnerJobID: ownerJobID}, limit, offset)
}

func (s *IcebergMaintenanceStore) ListRunsForOwnerFiltered(ctx context.Context, filter IcebergMaintenanceRunFilter, limit, offset int) ([]IcebergMaintenanceRun, error) {
	ownerJobID := strings.TrimSpace(filter.OwnerJobID)
	if ownerJobID == "" {
		return []IcebergMaintenanceRun{}, nil
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	filter.OwnerJobID = ownerJobID
	filterWhere, filterArgs := maintenanceRunFilterWhere(filter, true)
	where := " WHERE t.owner_job_id=?"
	if filterWhere != "" {
		where += " AND " + strings.TrimPrefix(filterWhere, " WHERE ")
	}
	args := append([]any{ownerJobID}, filterArgs...)
	query := `SELECT
	 r.id, r.worker_id, r.status,
	 COUNT(res.id),
	 COALESCE(SUM(res.status='succeeded'),0),
	 COALESCE(SUM(res.status='skipped'),0),
	 COALESCE(SUM(res.status='failed'),0),
	 r.started_at, r.finished_at, r.created_at
	FROM iceberg_maintenance_runs r
	JOIN iceberg_maintenance_results res ON res.run_id=r.id
	JOIN iceberg_maintenance_tasks t ON t.id=res.task_id
	` + where + `
	GROUP BY r.id, r.worker_id, r.status, r.started_at, r.finished_at, r.created_at
	ORDER BY r.id DESC
	LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IcebergMaintenanceRun, 0)
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

func (s *IcebergMaintenanceStore) CountRunsForOwnerFiltered(ctx context.Context, filter IcebergMaintenanceRunFilter) (int, error) {
	filter.OwnerJobID = strings.TrimSpace(filter.OwnerJobID)
	if filter.OwnerJobID == "" {
		return 0, nil
	}
	filterWhere, filterArgs := maintenanceRunFilterWhere(filter, true)
	where := " WHERE t.owner_job_id=?"
	if filterWhere != "" {
		where += " AND " + strings.TrimPrefix(filterWhere, " WHERE ")
	}
	args := append([]any{filter.OwnerJobID}, filterArgs...)
	var total int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT r.id)
	FROM iceberg_maintenance_runs r
	JOIN iceberg_maintenance_results res ON res.run_id=r.id
	JOIN iceberg_maintenance_tasks t ON t.id=res.task_id`+where, args...).Scan(&total)
	return total, err
}

func (s *IcebergMaintenanceStore) ListResultsForRunOwner(ctx context.Context, runID int64, ownerJobID string, limit int) ([]IcebergMaintenanceResult, error) {
	ownerJobID = strings.TrimSpace(ownerJobID)
	if ownerJobID == "" {
		return []IcebergMaintenanceResult{}, nil
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `SELECT res.id,res.run_id,res.task_id,res.table_key,res.operation,res.engine,res.routing_reason,res.status,res.input_files,res.input_bytes,
	 res.output_files,res.output_bytes,res.delete_files,res.expired_snapshots,res.orphan_candidates,res.deleted_files,res.deleted_bytes,res.duration_ms,res.attempt,
	 res.submission_id,res.details_json,res.error_text,res.created_at
	FROM iceberg_maintenance_results res
	JOIN iceberg_maintenance_tasks t ON t.id=res.task_id
	WHERE res.run_id=? AND t.owner_job_id=?
	ORDER BY res.id ASC LIMIT ?`, runID, ownerJobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]IcebergMaintenanceResult, 0)
	for rows.Next() {
		result, err := scanMaintenanceResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, rows.Err()
}

func (s *IcebergMaintenanceStore) LatestResultForTable(ctx context.Context, tableKey string) (*IcebergMaintenanceResult, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,run_id,task_id,table_key,operation,engine,routing_reason,status,input_files,input_bytes,
	 output_files,output_bytes,delete_files,expired_snapshots,orphan_candidates,deleted_files,deleted_bytes,duration_ms,attempt,
	 submission_id,details_json,error_text,created_at
	FROM iceberg_maintenance_results WHERE table_key=? ORDER BY id DESC LIMIT 1`, tableKey)
	result, err := scanMaintenanceResult(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &result, nil
}
