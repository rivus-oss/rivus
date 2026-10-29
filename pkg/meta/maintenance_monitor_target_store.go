package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type MaintenanceMonitorClaimStatus string

const (
	MaintenanceMonitorTargetOwned      MaintenanceMonitorClaimStatus = "owned"
	MaintenanceMonitorTargetReserved   MaintenanceMonitorClaimStatus = "reserved"
	MaintenanceMonitorTargetConflicted MaintenanceMonitorClaimStatus = "conflicted"
	MaintenanceMonitorTargetRetired    MaintenanceMonitorClaimStatus = "retired"
)

type IcebergMaintenanceMonitorTarget struct {
	MonitorID   string                        `json:"monitor_id"`
	TableKey    string                        `json:"table_key"`
	Catalog     string                        `json:"catalog"`
	Namespace   string                        `json:"namespace"`
	Table       string                        `json:"table"`
	Specificity int                           `json:"specificity"`
	ClaimStatus MaintenanceMonitorClaimStatus `json:"claim_status"`
	LastError   string                        `json:"last_error,omitempty"`
	CreatedAt   time.Time                     `json:"created_at"`
	UpdatedAt   time.Time                     `json:"updated_at"`

	NextInventoryAt  *time.Time `json:"-"`
	NextCompactionAt time.Time  `json:"-"`
	NextExpireAt     time.Time  `json:"-"`
	NextOrphanAt     time.Time  `json:"-"`
}

type IcebergMaintenanceMonitorDelta struct {
	Added   int `json:"added"`
	Removed int `json:"removed"`
}

func (s *IcebergMaintenanceStore) ListMonitorTargets(ctx context.Context, monitorID string) ([]IcebergMaintenanceMonitorTarget, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT monitor_id, table_key, catalog, namespace_name, table_name,
		specificity, claim_status, COALESCE(last_error, ''), created_at, updated_at
		FROM iceberg_maintenance_monitor_targets WHERE monitor_id=?
		ORDER BY catalog, namespace_name, table_name`, strings.TrimSpace(monitorID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []IcebergMaintenanceMonitorTarget
	for rows.Next() {
		var target IcebergMaintenanceMonitorTarget
		if err := rows.Scan(&target.MonitorID, &target.TableKey, &target.Catalog, &target.Namespace,
			&target.Table, &target.Specificity, &target.ClaimStatus, &target.LastError,
			&target.CreatedAt, &target.UpdatedAt); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, rows.Err()
}

func (s *IcebergMaintenanceStore) RecordMonitorDiscoveryFailure(ctx context.Context, monitorID, message string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_maintenance_monitors
		SET last_discovery_attempt_at=?, last_discovery_error=?, updated_at=? WHERE monitor_id=?`, now.UTC(), message, now.UTC(), strings.TrimSpace(monitorID))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrMaintenanceMonitorNotFound
	}
	return nil
}

// A discovery deliberately commits between batches. The attempt token prevents
// a superseded discovery, or one interrupted by pause/delete, from continuing
// with its old target set after another coordinator changes the monitor.
var ErrMaintenanceDiscoverySuperseded = errors.New("maintenance discovery superseded")

const (
	maintenanceDiscoveryBatchSize  = 50
	maintenanceDiscoveryBatchYield = 25 * time.Millisecond
)

type maintenanceDiscoveryAttempt struct {
	startedAt time.Time
	createdAt time.Time
}

func (s *IcebergMaintenanceStore) ApplyMonitorDiscovery(
	ctx context.Context,
	monitor IcebergMaintenanceMonitor,
	targets []IcebergMaintenanceMonitorTarget,
	now time.Time,
) (IcebergMaintenanceMonitorDelta, error) {
	return s.applyMonitorDiscoveryBatches(ctx, monitor, targets, now, maintenanceDiscoveryBatchSize, waitMaintenanceDiscoveryBatch)
}

func waitMaintenanceDiscoveryBatch(ctx context.Context) error {
	timer := time.NewTimer(maintenanceDiscoveryBatchYield)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *IcebergMaintenanceStore) applyMonitorDiscoveryBatches(
	ctx context.Context, monitor IcebergMaintenanceMonitor,
	targets []IcebergMaintenanceMonitorTarget, now time.Time,
	batchSize int, yield func(context.Context) error,
) (IcebergMaintenanceMonitorDelta, error) {
	var delta IcebergMaintenanceMonitorDelta
	monitor.ID = strings.TrimSpace(monitor.ID)
	if monitor.ID == "" || batchSize <= 0 {
		return delta, fmt.Errorf("maintenance monitor id and positive discovery batch size are required")
	}
	normalized := make(map[string]IcebergMaintenanceMonitorTarget, len(targets))
	for _, target := range targets {
		target.MonitorID = monitor.ID
		target.Catalog = strings.TrimSpace(target.Catalog)
		target.Namespace = strings.TrimSpace(target.Namespace)
		target.Table = strings.TrimSpace(target.Table)
		if target.Catalog == "" || target.Namespace == "" || target.Table == "" || target.Specificity <= 0 {
			return delta, fmt.Errorf("monitor target catalog, namespace, table and positive specificity are required")
		}
		target.TableKey = strings.TrimSpace(target.TableKey)
		if target.TableKey == "" {
			target.TableKey = target.Catalog + "." + target.Namespace + "." + target.Table
		}
		normalized[target.TableKey] = target
	}
	attempt, err := s.beginMonitorDiscovery(ctx, monitor.ID)
	if err != nil {
		return delta, err
	}
	// Read the work list without catalog or target locks. Each batch reloads its
	// current rows under the catalog guard and revalidates the attempt token.
	existing, err := s.ListMonitorTargets(ctx, monitor.ID)
	if err != nil {
		return delta, err
	}
	affected := make(map[string]IcebergMaintenanceMonitorTarget, len(normalized)+len(existing))
	for key, target := range normalized {
		affected[key] = target
	}
	for _, target := range existing {
		if _, keep := normalized[target.TableKey]; keep || target.ClaimStatus != MaintenanceMonitorTargetRetired {
			affected[target.TableKey] = target
		}
	}
	keys := make([]string, 0, len(affected))
	for key := range affected {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if a, b := affected[keys[i]].Catalog, affected[keys[j]].Catalog; a != b {
			return a < b
		}
		return keys[i] < keys[j]
	})
	for start := 0; start < len(keys); {
		end := start + 1
		// Group by the existing catalog to avoid blocking unrelated catalogs.
		// A target moved to another catalog also locks its destination below.
		for end < len(keys) && end-start < batchSize && affected[keys[end]].Catalog == affected[keys[start]].Catalog {
			end++
		}
		var batchDelta IcebergMaintenanceMonitorDelta
		batchCtx, cancel := context.WithTimeout(ctx, maintenanceOwnershipTimeout)
		err = retryMaintenanceTransaction(batchCtx, func() error {
			var batchErr error
			batchDelta, batchErr = s.applyMonitorDiscoveryBatch(batchCtx, monitor.ID, attempt, keys[start:end], affected, normalized)
			return batchErr
		})
		cancel()
		if err != nil {
			return delta, fmt.Errorf("reconcile monitor %s targets %d-%d of %d: %w", monitor.ID, start+1, end, len(keys), err)
		}
		delta.Added += batchDelta.Added
		delta.Removed += batchDelta.Removed
		start = end
		if start < len(keys) {
			// No transaction/connection is held while yielding to startup and
			// heartbeat writers. Cancellation preserves the committed batches.
			if err := yield(ctx); err != nil {
				return delta, err
			}
		}
	}
	// Mark success only after every batch commits. Interrupted discoveries
	// remain due and converge from durable rows on the next attempt.
	err = retryMaintenanceTransaction(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := validateMonitorDiscovery(ctx, tx, monitor.ID, attempt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitors
			SET last_discovery_at=?, last_discovery_error=NULL, updated_at=? WHERE monitor_id=?`, now.UTC(), now.UTC(), monitor.ID); err != nil {
			return err
		}
		return tx.Commit()
	})
	return delta, err
}

func (s *IcebergMaintenanceStore) beginMonitorDiscovery(ctx context.Context, monitorID string) (maintenanceDiscoveryAttempt, error) {
	var attempt maintenanceDiscoveryAttempt
	err := retryMaintenanceTransaction(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var status string
		var previous sql.NullTime
		if err := tx.QueryRowContext(ctx, `SELECT status, last_discovery_attempt_at, created_at
			FROM iceberg_maintenance_monitors WHERE monitor_id=? FOR UPDATE`, monitorID).Scan(&status, &previous, &attempt.createdAt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrMaintenanceMonitorNotFound
			}
			return err
		}
		if MaintenanceMonitorStatus(status) != MaintenanceMonitorActive {
			return fmt.Errorf("%w: monitor %s is not active", ErrMaintenanceDiscoverySuperseded, monitorID)
		}
		// Reuse the durable attempt timestamp as a monotonically increasing
		// token; no schema migration or in-process-only mutex is needed.
		if err := tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&attempt.startedAt); err != nil {
			return err
		}
		if previous.Valid && !attempt.startedAt.After(previous.Time) {
			attempt.startedAt = previous.Time.Add(time.Microsecond)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitors
			SET last_discovery_attempt_at=?, last_discovery_error=NULL WHERE monitor_id=?`, attempt.startedAt, monitorID); err != nil {
			return err
		}
		return tx.Commit()
	})
	return attempt, err
}

func validateMonitorDiscovery(ctx context.Context, tx *sql.Tx, monitorID string, attempt maintenanceDiscoveryAttempt) error {
	var status string
	var current sql.NullTime
	var created time.Time
	err := tx.QueryRowContext(ctx, `SELECT status, last_discovery_attempt_at, created_at
		FROM iceberg_maintenance_monitors WHERE monitor_id=? FOR UPDATE`, monitorID).Scan(&status, &current, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %w", ErrMaintenanceDiscoverySuperseded, ErrMaintenanceMonitorNotFound)
	}
	if err != nil {
		return err
	}
	if MaintenanceMonitorStatus(status) != MaintenanceMonitorActive || !current.Valid ||
		!current.Time.Equal(attempt.startedAt) || !created.Equal(attempt.createdAt) {
		return fmt.Errorf("%w: monitor %s changed between batches", ErrMaintenanceDiscoverySuperseded, monitorID)
	}
	return nil
}

func (s *IcebergMaintenanceStore) applyMonitorDiscoveryBatch(
	ctx context.Context, monitorID string, attempt maintenanceDiscoveryAttempt, keys []string,
	affected, normalized map[string]IcebergMaintenanceMonitorTarget,
) (IcebergMaintenanceMonitorDelta, error) {
	var delta IcebergMaintenanceMonitorDelta
	now := time.Now().UTC()
	tx, cleanup, err := s.beginMaintenanceOwnershipTransaction(ctx)
	if err != nil {
		return delta, err
	}
	defer cleanup()
	catalogs := make(map[string]struct{})
	for _, key := range keys {
		catalogs[affected[key].Catalog] = struct{}{}
		if target, keep := normalized[key]; keep {
			catalogs[target.Catalog] = struct{}{}
		}
	}
	if err := lockMaintenanceCatalogs(ctx, tx, sortedStringSet(catalogs), now); err != nil {
		return delta, err
	}
	if err := validateMonitorDiscovery(ctx, tx, monitorID, attempt); err != nil {
		return delta, err
	}
	args := []any{monitorID}
	for _, key := range keys {
		args = append(args, key)
	}
	rows, err := tx.QueryContext(ctx, `SELECT monitor_id, table_key, catalog, namespace_name, table_name,
		specificity, claim_status, COALESCE(last_error, ''), created_at, updated_at
		FROM iceberg_maintenance_monitor_targets WHERE monitor_id=? AND table_key IN (`+strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",")+`) FOR UPDATE`, args...)
	if err != nil {
		return delta, err
	}
	existing := make(map[string]IcebergMaintenanceMonitorTarget, len(keys))
	for rows.Next() {
		var target IcebergMaintenanceMonitorTarget
		if err := rows.Scan(&target.MonitorID, &target.TableKey, &target.Catalog, &target.Namespace,
			&target.Table, &target.Specificity, &target.ClaimStatus, &target.LastError, &target.CreatedAt, &target.UpdatedAt); err != nil {
			rows.Close()
			return delta, err
		}
		if _, locked := catalogs[target.Catalog]; !locked {
			rows.Close()
			return delta, fmt.Errorf("%w: target catalog changed", ErrMaintenanceDiscoverySuperseded)
		}
		existing[target.TableKey] = target
	}
	if err := rows.Close(); err != nil {
		return delta, err
	}
	if err := rows.Err(); err != nil {
		return delta, err
	}
	for _, key := range keys {
		target, keep := normalized[key]
		old, exists := existing[key]
		switch {
		case !keep && exists && old.ClaimStatus != MaintenanceMonitorTargetRetired:
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets
				SET claim_status=?, last_error=NULL, updated_at=? WHERE monitor_id=? AND table_key=?`, MaintenanceMonitorTargetRetired, now, monitorID, key)
			delta.Removed++
		case keep && !exists:
			_, err = tx.ExecContext(ctx, `INSERT INTO iceberg_maintenance_monitor_targets
				(monitor_id, table_key, catalog, namespace_name, table_name, specificity, claim_status, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, monitorID, key, target.Catalog, target.Namespace,
				target.Table, target.Specificity, MaintenanceMonitorTargetConflicted, now, now)
			delta.Added++
		case keep && old.ClaimStatus == MaintenanceMonitorTargetRetired:
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets
				SET catalog=?, namespace_name=?, table_name=?, specificity=?, claim_status=?, last_error=NULL, updated_at=?
				WHERE monitor_id=? AND table_key=?`, target.Catalog, target.Namespace, target.Table, target.Specificity,
				MaintenanceMonitorTargetConflicted, now, monitorID, key)
			delta.Added++
		case keep && old.Specificity != target.Specificity:
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets SET specificity=?, updated_at=?
				WHERE monitor_id=? AND table_key=?`, target.Specificity, now, monitorID, key)
		}
		if err != nil {
			return delta, err
		}
	}
	// A streaming reservation may have committed during the yield. Never carry
	// the old reservation cache across transactions.
	reservationCache := make(map[string][]IcebergMaintenanceReservation)
	for _, key := range keys {
		if err := s.reconcileMonitorTargetOwnership(ctx, tx, key, normalized, reservationCache, now); err != nil {
			return delta, err
		}
	}
	return delta, tx.Commit()
}

func (s *IcebergMaintenanceStore) reconcileMonitorTargetOwnership(
	ctx context.Context,
	tx *sql.Tx,
	tableKey string,
	newTargets map[string]IcebergMaintenanceMonitorTarget,
	reservationCache map[string][]IcebergMaintenanceReservation,
	now time.Time,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT target.monitor_id, target.catalog, target.namespace_name, target.table_name,
		target.specificity, target.claim_status, target.created_at
		FROM iceberg_maintenance_monitor_targets AS target
		JOIN iceberg_maintenance_monitors AS monitor ON monitor.monitor_id=target.monitor_id
		WHERE target.table_key=? AND target.claim_status<>? AND monitor.status='ACTIVE'
		ORDER BY target.specificity DESC, target.created_at ASC, target.monitor_id ASC FOR UPDATE`,
		tableKey, MaintenanceMonitorTargetRetired)
	if err != nil {
		return err
	}
	var candidates []IcebergMaintenanceMonitorTarget
	for rows.Next() {
		var target IcebergMaintenanceMonitorTarget
		target.TableKey = tableKey
		if err := rows.Scan(&target.MonitorID, &target.Catalog, &target.Namespace, &target.Table,
			&target.Specificity, &target.ClaimStatus, &target.CreatedAt); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, target)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var state maintenanceReservationState
	stateErr := tx.QueryRowContext(ctx, `SELECT table_key, catalog, namespace_name, table_name,
		owner_type, owner_job_id, lease_until FROM iceberg_maintenance_state WHERE table_key=? FOR UPDATE`, tableKey).Scan(
		&state.TableKey, &state.Catalog, &state.Namespace, &state.Table, &state.OwnerType, &state.OwnerJobID, &state.LeaseUntil,
	)
	if stateErr != nil && !errors.Is(stateErr, sql.ErrNoRows) {
		return stateErr
	}
	if len(candidates) == 0 {
		if stateErr == nil && state.LeaseUntil.Valid && state.LeaseUntil.Time.After(now) && strings.HasPrefix(state.OwnerJobID, "monitor:") {
			return nil
		}
		if stateErr == nil && strings.HasPrefix(state.OwnerJobID, "monitor:") {
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type='unmanaged', owner_job_id='unmanaged',
				next_inventory_check_at=NULL, next_compaction_check_at=NULL, next_expire_check_at=NULL,
				next_orphan_check_at=NULL, updated_at=? WHERE table_key=?`, now, tableKey); err != nil {
				return err
			}
			return cancelQueuedMaintenanceTasksForOwner(ctx, tx, tableKey, state.OwnerJobID, now)
		}
		return nil
	}
	if stateErr == nil && state.LeaseUntil.Valid && state.LeaseUntil.Time.After(now) && strings.HasPrefix(state.OwnerJobID, "monitor:") {
		for _, candidate := range candidates {
			desired := MaintenanceMonitorTargetConflicted
			if MaintenanceMonitorOwnerID(candidate.MonitorID) == state.OwnerJobID {
				desired = MaintenanceMonitorTargetOwned
			}
			if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, desired, now); err != nil {
				return err
			}
		}
		return nil
	}

	sample := candidates[0]
	if stateErr != nil {
		state = maintenanceReservationState{TableKey: tableKey, Catalog: sample.Catalog, Namespace: sample.Namespace, Table: sample.Table}
	}
	reservations, ok := reservationCache[sample.Catalog]
	if !ok {
		reservations, err = loadActiveMaintenanceReservations(ctx, tx, sample.Catalog)
		if err != nil {
			return err
		}
		reservationCache[sample.Catalog] = reservations
	}
	reservation := matchingMaintenanceReservation(reservations, state)
	if reservation.OwnerJobID != "" {
		for _, candidate := range candidates {
			if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, MaintenanceMonitorTargetReserved, now); err != nil {
				return err
			}
		}
		if stateErr == nil && strings.HasPrefix(state.OwnerJobID, "monitor:") {
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type=?, owner_job_id=?,
				next_inventory_check_at=NULL, next_compaction_check_at=NULL, next_expire_check_at=NULL,
				next_orphan_check_at=NULL, updated_at=? WHERE table_key=?`, reservation.Kind, reservation.OwnerJobID, now, tableKey); err != nil {
				return err
			}
			return cancelQueuedMaintenanceTasksForOwner(ctx, tx, tableKey, state.OwnerJobID, now)
		}
		return nil
	}
	if stateErr == nil && !strings.HasPrefix(state.OwnerJobID, "monitor:") && state.OwnerType != "unmanaged" && state.OwnerType != "stream-excluded" && state.OwnerType != "deleted-monitor" {
		for _, candidate := range candidates {
			if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, MaintenanceMonitorTargetReserved, now); err != nil {
				return err
			}
		}
		return nil
	}

	topSpecificity := candidates[0].Specificity
	winner := candidates[0]
	if stateErr == nil && strings.HasPrefix(state.OwnerJobID, "monitor:") {
		for _, candidate := range candidates {
			if candidate.Specificity == topSpecificity && MaintenanceMonitorOwnerID(candidate.MonitorID) == state.OwnerJobID {
				winner = candidate
				break
			}
		}
	}
	for _, candidate := range candidates {
		desired := MaintenanceMonitorTargetConflicted
		if candidate.MonitorID == winner.MonitorID {
			desired = MaintenanceMonitorTargetOwned
		}
		if err := updateMonitorTargetClaimStatus(ctx, tx, candidate, desired, now); err != nil {
			return err
		}
	}

	ownerID := MaintenanceMonitorOwnerID(winner.MonitorID)
	if stateErr == nil && state.OwnerJobID == ownerID {
		return nil
	}
	var nextInventory any = now
	compaction, expire, orphan := now.Add(time.Hour), now.Add(24*time.Hour), now.Add(7*24*time.Hour)
	if target, ok := newTargets[tableKey]; ok && target.MonitorID == winner.MonitorID {
		nextInventory = target.NextInventoryAt
		if target.NextCompactionAt.IsZero() == false {
			compaction = target.NextCompactionAt
		}
		if target.NextExpireAt.IsZero() == false {
			expire = target.NextExpireAt
		}
		if target.NextOrphanAt.IsZero() == false {
			orphan = target.NextOrphanAt
		}
	}
	if stateErr != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO iceberg_maintenance_state
			(table_key, catalog, namespace_name, table_name, owner_type, owner_job_id, snapshot_complete,
			 next_inventory_check_at, inventory_priority, next_compaction_check_at, next_expire_check_at,
			 next_orphan_check_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, 'monitor', ?, 1, ?, 0, ?, ?, ?, ?, ?)`, tableKey, winner.Catalog,
			winner.Namespace, winner.Table, ownerID, nextInventory, compaction.UTC(), expire.UTC(), orphan.UTC(), now, now)
		return err
	}
	oldOwner := state.OwnerJobID
	if _, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_state SET owner_type='monitor', owner_job_id=?, snapshot_complete=1,
		next_inventory_check_at=COALESCE(next_inventory_check_at, ?), inventory_priority=0,
		next_compaction_check_at=COALESCE(next_compaction_check_at, ?),
		next_expire_check_at=COALESCE(next_expire_check_at, ?), next_orphan_check_at=COALESCE(next_orphan_check_at, ?),
		updated_at=? WHERE table_key=?`, ownerID, nextInventory, compaction.UTC(), expire.UTC(), orphan.UTC(), now, tableKey); err != nil {
		return err
	}
	if oldOwner != ownerID {
		return cancelQueuedMaintenanceTasksForOwner(ctx, tx, tableKey, oldOwner, now)
	}
	return nil
}

func updateMonitorTargetClaimStatus(ctx context.Context, tx *sql.Tx, target IcebergMaintenanceMonitorTarget, status MaintenanceMonitorClaimStatus, now time.Time) error {
	if target.ClaimStatus == status {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_monitor_targets SET claim_status=?, last_error=NULL, updated_at=?
		WHERE monitor_id=? AND table_key=? AND claim_status<>?`, status, now, target.MonitorID, target.TableKey, status)
	return err
}

func cancelQueuedMaintenanceTasksForOwner(ctx context.Context, tx *sql.Tx, tableKey, ownerID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE iceberg_maintenance_tasks SET status=?, lease_owner=NULL, lease_until=NULL, updated_at=?
		WHERE table_key=? AND owner_job_id=? AND status IN (?, ?)`, MaintenanceTaskCancelled, now, tableKey, ownerID,
		MaintenanceTaskQueued, MaintenanceTaskRetry)
	return err
}
