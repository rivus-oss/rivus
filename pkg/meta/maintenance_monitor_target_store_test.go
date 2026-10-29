package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
)

func TestApplyMonitorDiscoveryDoesNotRewriteUnchangedTargetOrState(t *testing.T) {
	store, catalog, tableKey := newMaintenanceOwnershipIntegrationStore(t)
	monitor := IcebergMaintenanceMonitor{
		ID:     "delta-" + catalog,
		Name:   "Delta monitor",
		Status: MaintenanceMonitorActive,
		Config: &config.JobConfig{},
	}
	if err := store.CreateMonitor(context.Background(), monitor); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_monitor_targets WHERE monitor_id=?`, monitor.ID)
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_monitors WHERE monitor_id=?`, monitor.ID)
	})

	now := time.Now().UTC().Truncate(time.Microsecond)
	inventory := now.Add(time.Hour)
	target := IcebergMaintenanceMonitorTarget{
		MonitorID: monitor.ID, TableKey: tableKey, Catalog: catalog, Namespace: "sales", Table: "orders",
		Specificity: 1, NextInventoryAt: &inventory,
		NextCompactionAt: now.Add(2 * time.Hour), NextExpireAt: now.Add(3 * time.Hour), NextOrphanAt: now.Add(4 * time.Hour),
	}
	if _, err := store.ApplyMonitorDiscovery(context.Background(), monitor, []IcebergMaintenanceMonitorTarget{target}, now); err != nil {
		t.Fatal(err)
	}
	var firstTargetUpdate, firstStateUpdate time.Time
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND table_key=?`, monitor.ID, tableKey).Scan(&firstTargetUpdate); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_state
		WHERE table_key=?`, tableKey).Scan(&firstStateUpdate); err != nil {
		t.Fatal(err)
	}

	if delta, err := store.ApplyMonitorDiscovery(context.Background(), monitor, []IcebergMaintenanceMonitorTarget{target}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	} else if delta.Added != 0 || delta.Removed != 0 {
		t.Fatalf("unchanged discovery delta = %#v", delta)
	}
	var secondTargetUpdate, secondStateUpdate time.Time
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND table_key=?`, monitor.ID, tableKey).Scan(&secondTargetUpdate); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(context.Background(), `SELECT updated_at FROM iceberg_maintenance_state
		WHERE table_key=?`, tableKey).Scan(&secondStateUpdate); err != nil {
		t.Fatal(err)
	}
	if !secondTargetUpdate.Equal(firstTargetUpdate) || !secondStateUpdate.Equal(firstStateUpdate) {
		t.Fatalf("unchanged discovery rewrote rows: target %s -> %s, state %s -> %s",
			firstTargetUpdate, secondTargetUpdate, firstStateUpdate, secondStateUpdate)
	}

	if _, err := store.db.ExecContext(context.Background(), `UPDATE iceberg_maintenance_state
		SET owner_type='unmanaged', owner_job_id='unmanaged', snapshot_complete=0 WHERE table_key=?`, tableKey); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyMonitorDiscovery(context.Background(), monitor, []IcebergMaintenanceMonitorTarget{target}, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	state, err := store.GetState(context.Background(), tableKey)
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.OwnerJobID != MaintenanceMonitorOwnerID(monitor.ID) || !state.SnapshotComplete {
		t.Fatalf("reclaimed state = %#v, want monitor ownership with snapshot inventory required", state)
	}
}

// This uses a real MySQL catalog with the production-sized target count. The
// callback runs only between committed batches, so reservation and heartbeat
// writes would time out if a batch retained its catalog/registry locks.
func TestMonitorDiscoveryBatchesAllowStreamingBetween9025Targets(t *testing.T) {
	store, monitor, targets := newBatchDiscoveryFixture(t, 9025)
	jobID := "stream-" + monitor.ID
	seedCurrentOwnershipJob(t, store, jobID, "submission-1")
	if _, err := store.db.ExecContext(context.Background(), `UPDATE job_registry SET lease_owner='batch-worker',
		lease_until=TIMESTAMPADD(SECOND,30,UTC_TIMESTAMP(6)) WHERE job_id=?`, jobID); err != nil {
		t.Fatal(err)
	}
	jobStore, err := NewMySQLJobStore(os.Getenv("RIVUS_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer jobStore.Close()
	protected := targets[len(targets)-1]
	yields := 0
	started := time.Now()
	batchStarted := started
	var longestBatch time.Duration
	delta, err := store.applyMonitorDiscoveryBatches(context.Background(), monitor, targets, started,
		maintenanceDiscoveryBatchSize, func(ctx context.Context) error {
			longestBatch = max(longestBatch, time.Since(batchStarted))
			defer func() { batchStarted = time.Now() }()
			yields++
			if yields != 1 {
				return nil
			}
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var committed int
			if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_maintenance_monitor_targets
				WHERE monitor_id=?`, monitor.ID).Scan(&committed); err != nil || committed != maintenanceDiscoveryBatchSize {
				return fmt.Errorf("committed targets before first yield=%d, err=%v", committed, err)
			}
			if err := store.SyncMaintenanceReservations(ctx, jobID, "submission-1", MaintenanceReservationStreaming,
				[]IcebergMaintenanceReservationSelector{{Catalog: protected.Catalog, NamespacePattern: protected.Namespace, TablePattern: protected.Table}}, time.Now()); err != nil {
				return fmt.Errorf("streaming could not reserve between batches: %w", err)
			}
			ok, err := jobStore.RenewJobLease(ctx, jobID, "submission-1", "batch-worker", 30*time.Second)
			if err != nil || !ok {
				return fmt.Errorf("heartbeat between batches renewed=%t: %v", ok, err)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	longestBatch = max(longestBatch, time.Since(batchStarted))
	batchCount := (len(targets) + maintenanceDiscoveryBatchSize - 1) / maintenanceDiscoveryBatchSize
	if delta.Added != len(targets) || delta.Removed != 0 || yields != batchCount-1 {
		t.Fatalf("delta=%+v yields=%d, want 9025 added and %d yields", delta, yields, batchCount-1)
	}
	state, err := store.GetState(context.Background(), protected.TableKey)
	if err != nil || state == nil || state.OwnerJobID != jobID {
		t.Fatalf("new streaming ownership overwritten: state=%+v err=%v", state, err)
	}
	var status string
	if err := store.db.QueryRowContext(context.Background(), `SELECT claim_status FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND table_key=?`, monitor.ID, protected.TableKey).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(MaintenanceMonitorTargetReserved) {
		t.Fatalf("streaming target claim=%s, want reserved", status)
	}
	t.Logf("9025 targets, %d committed batches, streaming admission+heartbeat succeeded between batches; elapsed %s; longest batch interval %s", batchCount, time.Since(started), longestBatch)
}

func TestMonitorDiscoveryCancelledBatchCanResumeAndRetireTargets(t *testing.T) {
	store, monitor, targets := newBatchDiscoveryFixture(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	delta, err := store.applyMonitorDiscoveryBatches(ctx, monitor, targets, time.Now(), 1, func(context.Context) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || delta.Added != 1 {
		t.Fatalf("cancel delta=%+v err=%v", delta, err)
	}
	assertDiscoveryComplete(t, store, monitor.ID, false)
	delta, err = store.ApplyMonitorDiscovery(context.Background(), monitor, targets, time.Now())
	if err != nil || delta.Added != 2 {
		t.Fatalf("resume delta=%+v err=%v", delta, err)
	}
	assertDiscoveryComplete(t, store, monitor.ID, true)
	delta, err = store.applyMonitorDiscoveryBatches(context.Background(), monitor, nil, time.Now(), 1,
		func(context.Context) error { return nil })
	if err != nil || delta.Removed != 3 {
		t.Fatalf("retirement delta=%+v err=%v", delta, err)
	}
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM iceberg_maintenance_state
		WHERE catalog=? AND owner_job_id=?`, targets[0].Catalog, MaintenanceMonitorOwnerID(monitor.ID)).Scan(&count); err != nil || count != 0 {
		t.Fatalf("retired monitor still owns %d states: %v", count, err)
	}
}

func TestMonitorDiscoveryNewAttemptFencesOlderBatches(t *testing.T) {
	store, monitor, targets := newBatchDiscoveryFixture(t, 3)
	newerTime := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	_, err := store.applyMonitorDiscoveryBatches(context.Background(), monitor, targets, time.Now(), 1, func(ctx context.Context) error {
		_, err := store.ApplyMonitorDiscovery(ctx, monitor, targets[2:], newerTime)
		return err
	})
	if !errors.Is(err, ErrMaintenanceDiscoverySuperseded) || !IsTransientMetadataError(err) {
		t.Fatalf("old discovery error=%v, want retryable superseded", err)
	}
	current, err := store.GetMonitor(context.Background(), monitor.ID)
	if err != nil || current == nil || current.LastDiscoveryAt == nil || !current.LastDiscoveryAt.Equal(newerTime) {
		t.Fatalf("old discovery overwrote success marker: %+v err=%v", current, err)
	}
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=? AND claim_status<>'retired'`, monitor.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("old membership resurrected: active=%d err=%v", count, err)
	}
}

func TestMonitorDiscoveryPauseResumeFencesOldBatches(t *testing.T) {
	store, monitor, targets := newBatchDiscoveryFixture(t, 3)
	_, err := store.applyMonitorDiscoveryBatches(context.Background(), monitor, targets, time.Now(), 1, func(ctx context.Context) error {
		if err := store.SetMonitorStatus(ctx, monitor.ID, MaintenanceMonitorPaused, time.Now()); err != nil {
			return err
		}
		return store.SetMonitorStatus(ctx, monitor.ID, MaintenanceMonitorActive, time.Now())
	})
	if !errors.Is(err, ErrMaintenanceDiscoverySuperseded) {
		t.Fatalf("old discovery survived pause/resume: %v", err)
	}
	assertDiscoveryComplete(t, store, monitor.ID, false)
	if _, err := store.ApplyMonitorDiscovery(context.Background(), monitor, targets, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertDiscoveryComplete(t, store, monitor.ID, true)
}

func TestMonitorDiscoveryDeleteRecreateFencesOldBatches(t *testing.T) {
	store, monitor, targets := newBatchDiscoveryFixture(t, 3)
	_, err := store.applyMonitorDiscoveryBatches(context.Background(), monitor, targets, time.Now(), 1, func(ctx context.Context) error {
		if err := store.DeleteMonitor(ctx, monitor.ID, time.Now()); err != nil {
			return err
		}
		return store.CreateMonitor(ctx, monitor)
	})
	if !errors.Is(err, ErrMaintenanceDiscoverySuperseded) {
		t.Fatalf("old discovery survived recreation: %v", err)
	}
	var count int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM iceberg_maintenance_monitor_targets
		WHERE monitor_id=?`, monitor.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("old membership resurrected after recreate: count=%d err=%v", count, err)
	}
}

func newBatchDiscoveryFixture(t *testing.T, count int) (*IcebergMaintenanceStore, IcebergMaintenanceMonitor, []IcebergMaintenanceMonitorTarget) {
	t.Helper()
	store, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	monitor := IcebergMaintenanceMonitor{ID: "batch-" + catalog, Name: "Batch monitor", Status: MaintenanceMonitorActive, Config: &config.JobConfig{}}
	if err := store.CreateMonitor(context.Background(), monitor); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_monitor_targets WHERE monitor_id=?`, monitor.ID)
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_monitors WHERE monitor_id=?`, monitor.ID)
		_, _ = store.db.ExecContext(context.Background(), `DELETE FROM iceberg_maintenance_state WHERE catalog=?`, catalog)
	})
	targets := make([]IcebergMaintenanceMonitorTarget, count)
	for i := range targets {
		table := fmt.Sprintf("table_%05d", i)
		targets[i] = IcebergMaintenanceMonitorTarget{MonitorID: monitor.ID, TableKey: catalog + ".batch." + table,
			Catalog: catalog, Namespace: "batch", Table: table, Specificity: 1}
	}
	for start := 0; start < count; start += 500 {
		end := min(start+500, count)
		args := make([]any, 0, (end-start)*4)
		for _, target := range targets[start:end] {
			args = append(args, target.TableKey, target.Catalog, target.Namespace, target.Table)
		}
		_, err := store.db.ExecContext(context.Background(), `INSERT INTO iceberg_maintenance_state
			(table_key,catalog,namespace_name,table_name,owner_type,owner_job_id,snapshot_complete,created_at,updated_at) VALUES `+
			strings.TrimSuffix(strings.Repeat("(?,?,?,?,'unmanaged','unmanaged',1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)),", end-start), ","), args...)
		if err != nil {
			t.Fatal(err)
		}
	}
	return store, monitor, targets
}

func assertDiscoveryComplete(t *testing.T, store *IcebergMaintenanceStore, id string, want bool) {
	t.Helper()
	var completed sql.NullTime
	if err := store.db.QueryRowContext(context.Background(), `SELECT last_discovery_at FROM iceberg_maintenance_monitors
		WHERE monitor_id=?`, id).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed.Valid != want {
		t.Fatalf("last_discovery_at valid=%t, want %t", completed.Valid, want)
	}
}
