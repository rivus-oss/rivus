package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

type maintenanceCleanupView struct {
	Operation     string              `json:"operation"`
	Status        string              `json:"status"`
	Reason        string              `json:"reason"`
	NextDueAt     *time.Time          `json:"next_due_at"`
	LastSuccessAt *time.Time          `json:"last_success_at"`
	LastResult    *meta.CleanupResult `json:"last_result"`
	NotBefore     *time.Time          `json:"not_before,omitempty"`
}

func cleanupOperationView(state meta.IcebergMaintenanceState, task meta.CleanupTaskView, enabled, paused bool, now time.Time) maintenanceCleanupView {
	v := maintenanceCleanupView{Operation: task.Operation, Status: "unscheduled", LastSuccessAt: task.LastSuccessAt,
		LastResult: task.LastResult, Reason: "No cleanup schedule is recorded yet."}
	if task.Operation == "remove_orphan_files" {
		v.NextDueAt = state.NextOrphanCheckAt
	} else {
		v.NextDueAt = state.NextExpireCheckAt
	}
	if v.NextDueAt != nil {
		v.Status, v.Reason = "scheduled", "Waiting until the next cleanup due time."
		if !v.NextDueAt.After(now) {
			v.Status, v.Reason = "due", "Due for scheduling; start time depends on worker capacity."
		}
	}
	// A task lease is separate from inventory freshness and the table lease.
	if task.Status == meta.MaintenanceTaskLeased {
		if task.LeaseUntil != nil && task.LeaseUntil.After(now) {
			v.Status, v.Reason = "running", "Cleanup is running."
		} else {
			v.Status, v.Reason = "lease_expired", "Worker lease expired; waiting for recovery."
		}
		return v
	}
	switch task.Status {
	case meta.MaintenanceTaskQueued:
		v.Status, v.Reason = "queued", "Queued; waiting for a worker."
	case meta.MaintenanceTaskRetry:
		v.Status, v.Reason = "retry", "Retry pending."
	case meta.MaintenanceTaskFailed:
		v.Status, v.Reason = "failed", "The latest cleanup attempt failed."
	case meta.MaintenanceTaskSkipped:
		if v.Status != "due" {
			v.Status, v.Reason = "skipped", "The latest cleanup attempt was skipped."
		}
	case meta.MaintenanceTaskCancelled:
		if v.Status != "due" {
			v.Status, v.Reason = "cancelled", "The latest cleanup task was cancelled."
		}
	}
	if task.Status == meta.MaintenanceTaskQueued || task.Status == meta.MaintenanceTaskRetry {
		v.NotBefore = &task.NotBefore
	}
	if task.LastResult != nil && task.LastResult.Reason != "" && (v.Status == "skipped" || v.Status == "failed") {
		v.Reason = task.LastResult.Reason
	}
	if task.LastError != "" {
		v.Reason = task.LastError
	} else if task.LastResult != nil && task.LastResult.Error != "" {
		v.Reason = task.LastResult.Error
	}
	if !state.SnapshotComplete {
		v.Status, v.Reason = "waiting_for_snapshot", "Waiting for the initial snapshot to complete."
	}
	if !enabled {
		v.Status, v.Reason = "disabled", "Automatic maintenance is disabled."
	} else if paused {
		v.Status, v.Reason = "paused", "Maintenance is paused."
	}
	return v
}

func (s *Server) handleMaintenanceCleanupHistory(w http.ResponseWriter, r *http.Request) {
	owner, key := maintenanceJobID(r), strings.TrimSpace(r.URL.Query().Get("table_key"))
	op := strings.TrimSpace(r.URL.Query().Get("operation"))
	if owner == "" || key == "" || (op != "remove_orphan_files" && op != "expire_snapshots") {
		maintenanceAPIError(w, fmt.Errorf("job_id, table_key and a cleanup operation are required"), http.StatusBadRequest)
		return
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	results, err := store.ListCleanupHistory(r.Context(), owner, key, op, maintenanceQueryInt(r, "limit", 10, 1, 100))
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{"results": results})
}
