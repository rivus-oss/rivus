package meta

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCleanupViewOwnerIsolationAndSuccessfulHistory(t *testing.T) {
	dsn := os.Getenv("RIVUS_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RIVUS_TEST_MYSQL_DSN is not set")
	}
	store, err := NewIcebergMaintenanceStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	owner := fmt.Sprintf("cleanup-view-%d", time.Now().UnixNano())
	key := owner + ".ds.orders"
	now := time.Now().UTC().Truncate(time.Microsecond)
	state := IcebergMaintenanceState{TableKey: key, Catalog: owner, Namespace: "ds", Table: "orders", OwnerJobID: owner, OwnerType: "job", SnapshotComplete: true}
	if err := store.UpsertState(ctx, state, now, now, now); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_results WHERE table_key=?", key)
		store.db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_tasks WHERE table_key=?", key)
		store.db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_state WHERE table_key=?", key)
	})
	seed := func(taskOwner, op, status string, at time.Time) int64 {
		t.Helper()
		res, err := store.db.ExecContext(ctx, `INSERT INTO iceberg_maintenance_tasks
		 (idempotency_key,table_key,owner_job_id,operation,status,attempt_count,not_before,schedule_window,created_at,updated_at)
		 VALUES (?,?,?,?,?,1,?,'test',?,?)`, fmt.Sprintf("%d-%s", time.Now().UnixNano(), status), key, taskOwner, op, status, at, at, at)
		if err != nil {
			t.Fatal(err)
		}
		id, _ := res.LastInsertId()
		if err := store.InsertResult(ctx, IcebergMaintenanceResult{TaskID: id, RunID: id, TableKey: key, Operation: op,
			Engine: "native", Status: status, Attempt: 1, DeletedFiles: 4, DeletedBytes: 123456,
			ExpiredSnapshots: 7, RoutingReason: "reason <test>", CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	successAt := now.Add(-2 * time.Hour)
	seed(owner, "remove_orphan_files", "succeeded", successAt)
	dryID := seed(owner, "remove_orphan_files", "succeeded", successAt.Add(30*time.Minute))
	if _, err := store.db.ExecContext(ctx, `UPDATE iceberg_maintenance_results SET details_json='{"dry_run":true}' WHERE task_id=?`, dryID); err != nil {
		t.Fatal(err)
	}
	skippedID := seed(owner, "remove_orphan_files", "skipped", now.Add(-time.Hour))
	seed(owner, "expire_snapshots", "succeeded", now)
	// A newer historical owner's task must not replace this owner's latest row.
	seed("other-"+owner, "remove_orphan_files", "failed", now)
	views, err := store.ListCleanupTasksForOwner(ctx, owner, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 2 {
		t.Fatalf("views=%+v", views)
	}
	for _, v := range views {
		if v.Operation == "remove_orphan_files" {
			if v.TaskID != skippedID || v.Status != "skipped" || v.LastSuccessAt == nil || !v.LastSuccessAt.Equal(successAt) {
				t.Fatalf("latest attempt and successful timestamp conflated: %+v", v)
			}
		} else if v.LastResult == nil || v.LastResult.DeletedBytes != nil || v.LastResult.ExpiredSnapshots != 7 {
			t.Fatalf("expiration deletion bytes must be unknown: %+v", v)
		}
	}
	history, err := store.ListCleanupHistory(ctx, owner, key, "remove_orphan_files", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 || history[0].Status != "skipped" || !history[1].DryRun || history[2].DeletedBytes == nil || *history[2].DeletedBytes != 123456 {
		t.Fatalf("history lost order or owner isolation: %+v", history)
	}
	if _, err := store.db.ExecContext(ctx, "UPDATE iceberg_maintenance_state SET owner_job_id=? WHERE table_key=?", "other-"+owner, key); err != nil {
		t.Fatal(err)
	}
	history, err = store.ListCleanupHistory(ctx, owner, key, "remove_orphan_files", 10)
	if err != nil || len(history) != 0 {
		t.Fatalf("retired owner can read current table history: %+v, %v", history, err)
	}
}

func TestCleanupDeletionCountsDoNotInventMeasurements(t *testing.T) {
	for _, tc := range []struct {
		operation, engine, status string
		measured                  bool
	}{
		{"remove_orphan_files", "native", "succeeded", true},
		{"expire_snapshots", "native", "succeeded", false},
		{"remove_orphan_files", "native", "skipped", false},
		{"remove_orphan_files", "native", "failed", false},
		{"remove_orphan_files", "spark", "succeeded", false},
	} {
		v := cleanupResult(IcebergMaintenanceResult{Operation: tc.operation, Engine: tc.engine, Status: tc.status})
		if (v.DeletedBytes != nil) != tc.measured {
			t.Fatalf("%+v => %+v", tc, v)
		}
	}
}
