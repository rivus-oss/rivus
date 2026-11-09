package api

import (
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestCleanupStatusIndependentOfInventory(t *testing.T) {
	now := time.Now().UTC()
	past, future := now.Add(-time.Hour), now.Add(time.Hour)
	state := meta.IcebergMaintenanceState{SnapshotComplete: true, NextOrphanCheckAt: &past,
		LastInventoryAt: &past, LastError: "inventory scan failed", LeaseUntil: &future}
	for _, tc := range []struct{ taskStatus, want string }{
		{"", "due"}, {"succeeded", "due"}, {"queued", "queued"}, {"retry", "retry"},
		{"failed", "failed"}, {"skipped", "due"}, {"cancelled", "due"}, {"leased", "running"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			task := meta.CleanupTaskView{Operation: "remove_orphan_files", Status: tc.taskStatus, LeaseUntil: &future}
			v := cleanupOperationView(state, task, true, false, now)
			if v.Status != tc.want || strings.Contains(v.Reason, "inventory") {
				t.Fatalf("cleanup inherited compaction/inventory state: %+v", v)
			}
		})
	}
	task := meta.CleanupTaskView{Operation: "remove_orphan_files", Status: "leased", LeaseUntil: &past}
	if v := cleanupOperationView(state, task, true, false, now); v.Status != "lease_expired" {
		t.Fatalf("expired lease shown as running: %+v", v)
	}
}

func TestCleanupScheduleAndBlockingStates(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	state := meta.IcebergMaintenanceState{SnapshotComplete: true, NextExpireCheckAt: &future, LastExpireAt: &now}
	task := meta.CleanupTaskView{Operation: "expire_snapshots", Status: "skipped", LastResult: &meta.CleanupResult{Reason: "No snapshots eligible"}}
	v := cleanupOperationView(state, task, true, false, now)
	if v.LastSuccessAt != nil || v.Status != "skipped" || v.Reason != "No snapshots eligible" || v.NextDueAt != &future {
		t.Fatalf("skipped cleanup is not a successful cleanup: %+v", v)
	}
	task.Status = "succeeded"
	if v := cleanupOperationView(state, task, true, false, now); v.Status != "scheduled" {
		t.Fatal(v)
	}
	if v := cleanupOperationView(state, task, true, true, now); v.Status != "paused" {
		t.Fatal(v)
	}
	if v := cleanupOperationView(state, task, false, false, now); v.Status != "disabled" {
		t.Fatal(v)
	}
	state.SnapshotComplete = false
	if v := cleanupOperationView(state, task, true, false, now); v.Status != "waiting_for_snapshot" {
		t.Fatal(v)
	}
}
