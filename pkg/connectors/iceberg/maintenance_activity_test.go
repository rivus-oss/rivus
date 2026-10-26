package iceberg

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/meta"
)

type activityTestMetadata struct {
	icetable.Metadata
	snapshots []icetable.Snapshot
	currentID int64
}

func (m activityTestMetadata) CurrentSnapshot() *icetable.Snapshot {
	return m.SnapshotByID(m.currentID)
}
func (m activityTestMetadata) SnapshotByID(id int64) *icetable.Snapshot {
	for i := range m.snapshots {
		if m.snapshots[i].SnapshotID == id {
			return &m.snapshots[i]
		}
	}
	return nil
}

func TestLatestTableWriteRecognizesExternalWritersAndIgnoresMaintenance(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	old := now.Add(-10 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	parentID := int64(900) // Snapshot IDs are opaque, not increasing counters.
	for _, tc := range []struct {
		name       string
		op         icetable.Operation
		properties iceberglib.Properties
		written    time.Time
		want       time.Time
	}{
		{"spark append", icetable.OpAppend, nil, recent, recent},
		{"trino update", icetable.OpOverwrite, nil, recent, recent},
		{"trino delete", icetable.OpDelete, nil, recent, recent},
		{"external compaction", icetable.OpReplace, nil, recent, old},
		{"rivus compaction", icetable.OpOverwrite, iceberglib.Properties{"rivus.maintenance": "native-compaction"}, recent, old},
		{"unknown operation", icetable.Operation("unknown"), nil, recent, old},
		{"future timestamp", icetable.OpAppend, nil, now.Add(time.Hour), old},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := activityTestMetadata{currentID: 100, snapshots: []icetable.Snapshot{
				{SnapshotID: parentID, TimestampMs: old.UnixMilli(), Summary: &icetable.Summary{Operation: icetable.OpAppend}},
				{SnapshotID: 100, ParentSnapshotID: &parentID, TimestampMs: tc.written.UnixMilli(), Summary: &icetable.Summary{Operation: tc.op, Properties: tc.properties}},
				// An unpublished branch must not make the main table look active.
				{SnapshotID: 999, TimestampMs: now.UnixMilli(), Summary: &icetable.Summary{Operation: icetable.OpAppend}},
			}}
			got := latestTableWriteAt(metadata, now)
			if got == nil || !got.Equal(tc.want) {
				t.Fatalf("last write=%v, want %v", got, tc.want)
			}
			// A later scan must not refresh the timestamp of an unchanged table.
			if again := latestTableWriteAt(metadata, now.Add(time.Minute)); again == nil || !again.Equal(*got) {
				t.Fatalf("unchanged snapshot refreshed activity: %v -> %v", got, again)
			}
		})
	}
	for _, metadata := range []activityTestMetadata{
		{},
		{currentID: 1, snapshots: []icetable.Snapshot{{SnapshotID: 1, TimestampMs: recent.UnixMilli()}}},
		{currentID: 1, snapshots: []icetable.Snapshot{{SnapshotID: 1, TimestampMs: recent.UnixMilli(), Summary: &icetable.Summary{Operation: icetable.OpReplace}}}},
	} {
		if got := latestTableWriteAt(metadata, now); got != nil {
			t.Fatalf("table without a known data write has activity: %v", got)
		}
	}
}

func TestInventoryCollectsSnapshotWriteTimestamp(t *testing.T) {
	tbl, _ := newEqualityDeltaTestTable(t)
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(1), "status": "new"}})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	tbl, err = tbl.Append(context.Background(), reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := scanActiveFileInventoryOnce(context.Background(), tbl, defaultNativeMaintenanceSettings())
	if err != nil {
		t.Fatal(err)
	}
	if inventory.LastWriteAt == nil || inventory.LastWriteAt.UnixMilli() != tbl.CurrentSnapshot().TimestampMs {
		t.Fatalf("inventory did not collect write timestamp: %+v", inventory)
	}
}

func TestOrphanActivityExpiresIndependentlyOfCleanupInterval(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		age  time.Duration
		want bool
	}{
		{-time.Second, false},
		{0, true},
		{48 * time.Hour, true},
		{orphanActivityWindow - time.Second, true},
		{orphanActivityWindow, false},
		{8 * 24 * time.Hour, false},
	} {
		written := now.Add(-tc.age)
		if got := orphanCleanupActive(meta.IcebergMaintenanceState{LastWriteAt: &written}, now); got != tc.want {
			t.Fatalf("activity age=%s: active=%t, want %t", tc.age, got, tc.want)
		}
	}
}

func TestOrphanDefaultsMatchDecodedConfig(t *testing.T) {
	cfg, err := decodeIcebergConfig(map[string]any{"table_maintenance": map[string]any{"enabled": true}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TableMaintenance.NativeOrphanIntervalSeconds != 24*60*60 || cfg.TableMaintenance.NativeOrphanInactiveIntervalSeconds != 7*24*60*60 {
		t.Fatalf("inconsistent orphan defaults: %+v", cfg.TableMaintenance)
	}
}

func TestOrphanIntervalsRespectExplicitOverrides(t *testing.T) {
	for _, tc := range []struct {
		name    string
		active  int
		idle    int
		wantErr bool
	}{
		{"longer active interval with implicit idle interval", 30 * 86400, 0, false},
		{"explicit legacy intervals", 30 * 86400, 90 * 86400, false},
		{"explicit shorter intervals", 3600, 86400, false},
		{"reject idle interval shorter than active", 86400, 3600, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			maintenance := map[string]any{"enabled": true, "native_orphan_interval_seconds": tc.active}
			if tc.idle != 0 {
				maintenance["native_orphan_inactive_interval_seconds"] = tc.idle
			}
			settings, err := nativeMaintenanceSettingsFromRaw(map[string]any{"table_maintenance": maintenance})
			if tc.wantErr {
				if err == nil {
					t.Fatal("unsafe interval ordering was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantIdle := tc.idle
			if wantIdle == 0 {
				wantIdle = max(7*86400, tc.active)
			}
			if settings.OrphanInterval != time.Duration(tc.active)*time.Second || settings.OrphanInactiveInterval != time.Duration(wantIdle)*time.Second {
				t.Fatalf("interval overrides were lost: %+v", settings)
			}
		})
	}
}

func TestOrphanEnvironmentIntervalsMatchDecodedConfig(t *testing.T) {
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INTERVAL_SECONDS", "2592000")
	t.Setenv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INACTIVE_INTERVAL_SECONDS", "7776000")
	raw := map[string]any{"table_maintenance": map[string]any{"enabled": true}}
	cfg, err := decodeIcebergConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := nativeMaintenanceSettingsFromRaw(raw)
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(cfg.TableMaintenance.NativeOrphanIntervalSeconds)*time.Second != settings.OrphanInterval || settings.OrphanInterval != 30*24*time.Hour || time.Duration(cfg.TableMaintenance.NativeOrphanInactiveIntervalSeconds)*time.Second != settings.OrphanInactiveInterval {
		t.Fatalf("environment intervals differ between decoded config and scheduler: cfg=%+v settings=%+v", cfg.TableMaintenance, settings)
	}
}

func TestEnqueueOrphanCleanupIncludesInactiveTables(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("RIVUS_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("RIVUS_TEST_MYSQL_DSN is not set")
	}
	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	owner := "test-orphan-idle-" + now.Format("20060102150405.000000000")
	state := meta.IcebergMaintenanceState{
		TableKey: owner + ".ns.idle", Catalog: owner, Namespace: "ns", Table: "idle",
		OwnerType: "job", OwnerJobID: owner, SnapshotComplete: true,
	}
	if err := store.UpsertState(ctx, state, now.Add(time.Hour), now.Add(time.Hour), now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_tasks WHERE owner_job_id=?", owner)
		_, _ = db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_state WHERE table_key=?", state.TableKey)
		_ = db.Close()
	})
	if err := enqueueDueMaintenance(ctx, store, map[string]maintenanceWorkerJob{owner: {Settings: defaultNativeMaintenanceSettings()}}, now, 100); err != nil {
		t.Fatal(err)
	}
	var operation, status string
	err = db.QueryRowContext(ctx, `SELECT operation, status FROM iceberg_maintenance_tasks WHERE owner_job_id=?`, owner).Scan(&operation, &status)
	if err != nil || operation != maintenanceOperationOrphan || status != meta.MaintenanceTaskQueued {
		t.Fatalf("inactive orphan cleanup was not enqueued: operation=%s status=%s err=%v", operation, status, err)
	}
	saved, err := store.GetState(ctx, state.TableKey)
	if err != nil || saved == nil || saved.NextOrphanCheckAt == nil || saved.NextOrphanCheckAt.Before(now.Add(7*24*time.Hour)) || saved.NextOrphanCheckAt.After(now.Add(7*24*time.Hour+time.Hour)) {
		t.Fatalf("idle cleanup was not rescheduled weekly: state=%+v err=%v", saved, err)
	}
}

func TestEnqueueMaintenanceDoesNotStarveBehindDeletedOwners(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("RIVUS_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("RIVUS_TEST_MYSQL_DSN is not set")
	}
	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	owner := "test-due-owner-" + now.Format("20060102150405.000000000")
	deletedOwner := owner + "-deleted"
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_tasks WHERE owner_job_id IN (?,?)", owner, deletedOwner)
		_, _ = db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_state WHERE owner_job_id IN (?,?)", owner, deletedOwner)
		_ = db.Close()
	})

	// More than an entire page of older recovery states whose owner no longer
	// exists must not prevent a current owner's later schedule from being read.
	old := now.Add(-24 * time.Hour)
	for i := 0; i < 101; i++ {
		state := meta.IcebergMaintenanceState{
			TableKey: fmt.Sprintf("%s.ns.old_%03d", owner, i), Catalog: owner,
			Namespace: "ns", Table: fmt.Sprintf("old_%03d", i), OwnerType: "job",
			OwnerJobID: deletedOwner, SnapshotComplete: true,
		}
		if err := store.UpsertState(ctx, state, old, old, old); err != nil {
			t.Fatal(err)
		}
	}
	// Obsolete ownership types are not schedulable even when their old owner
	// ID happens to be present in the coordinator's map.
	unmanaged := meta.IcebergMaintenanceState{
		TableKey: owner + ".ns.unmanaged", Catalog: owner, Namespace: "ns",
		Table: "unmanaged", OwnerType: "unmanaged", OwnerJobID: owner, SnapshotComplete: true,
	}
	if err := store.UpsertState(ctx, unmanaged, old, old, old); err != nil {
		t.Fatal(err)
	}
	live := meta.IcebergMaintenanceState{
		TableKey: owner + ".ns.current", Catalog: owner, Namespace: "ns",
		Table: "current", OwnerType: "job", OwnerJobID: owner, SnapshotComplete: true,
	}
	due := now.Add(-time.Minute)
	if err := store.UpsertState(ctx, live, due, due, due); err != nil {
		t.Fatal(err)
	}
	jobs := map[string]maintenanceWorkerJob{owner: {Settings: defaultNativeMaintenanceSettings()}}
	if err := enqueueDueMaintenance(ctx, store, jobs, now, 100); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"compact", "expire_snapshots", "remove_orphan_files"} {
		var count int
		err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_maintenance_tasks
			WHERE owner_job_id=? AND table_key=? AND operation=? AND status='queued'`, owner, live.TableKey, operation).Scan(&count)
		if err != nil || count != 1 {
			t.Fatalf("current owner's %s tasks=%d error=%v; want one queued task", operation, count, err)
		}
	}
	var obsoleteTasks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_maintenance_tasks
		WHERE owner_job_id=? OR table_key=?`, deletedOwner, unmanaged.TableKey).Scan(&obsoleteTasks); err != nil || obsoleteTasks != 0 {
		t.Fatalf("obsolete owners queued %d tasks, error=%v; want zero", obsoleteTasks, err)
	}
}

func TestEnqueueMaintenanceWithoutOwnersDoesNotQuery(t *testing.T) {
	if err := enqueueDueMaintenance(context.Background(), nil, nil, time.Now(), 100); err != nil {
		t.Fatal(err)
	}
}
