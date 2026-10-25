package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func (s *Server) handleMaintenanceSummary(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	jobID := maintenanceJobID(r)
	if jobID != "" {
		summary, err := store.SummaryForOwner(r.Context(), jobID)
		if err != nil {
			maintenanceAPIError(w, err, http.StatusInternalServerError)
			return
		}
		maintenanceAPIJSON(w, http.StatusOK, summary)
		return
	}

	summary, err := store.Summary(r.Context(), time.Now().UTC())
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, summary)
}

func (s *Server) handleMaintenanceFailures(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	limit := maintenanceQueryInt(r, "limit", 50, 1, 200)
	failures, err := store.ListLatestFailures(r.Context(), limit)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"failures": failures,
		"limit":    limit,
	})
}

func (s *Server) handleMaintenanceFailureRetry(w http.ResponseWriter, r *http.Request) {
	taskID, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || taskID <= 0 {
		maintenanceAPIError(w, fmt.Errorf("invalid maintenance task id"), http.StatusBadRequest)
		return
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	failure, err := store.GetLatestFailure(r.Context(), taskID)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if failure == nil {
		maintenanceAPIError(w, fmt.Errorf("maintenance failure is no longer current"), http.StatusNotFound)
		return
	}
	if !failure.CanRetry {
		maintenanceAPIError(w, fmt.Errorf("maintenance failure cannot be retried because its table state or owner is inactive"), http.StatusConflict)
		return
	}

	payload := make(map[string]any, len(failure.Payload)+2)
	for key, value := range failure.Payload {
		payload[key] = value
	}
	payload["manual"] = true
	payload["retried_task_id"] = failure.TaskID
	now := time.Now().UTC()
	state := meta.IcebergMaintenanceState{
		TableKey:         failure.TableKey,
		Catalog:          failure.Catalog,
		Namespace:        failure.Namespace,
		Table:            failure.Table,
		OwnerType:        failure.OwnerType,
		OwnerJobID:       failure.OwnerJobID,
		SnapshotComplete: failure.SnapshotComplete,
	}
	queued, err := store.EnqueueTask(
		r.Context(),
		state,
		failure.Operation,
		failure.Priority,
		fmt.Sprintf("manual-retry-%s-%d", failure.Operation, now.UnixNano()),
		now,
		payload,
	)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if !queued {
		maintenanceAPIError(w, fmt.Errorf("a task for this table and operation is already queued or running"), http.StatusConflict)
		return
	}
	maintenanceAPIJSON(w, http.StatusAccepted, map[string]any{
		"task_id":   failure.TaskID,
		"table_key": failure.TableKey,
		"operation": failure.Operation,
		"queued":    true,
		"message":   "maintenance retry queued",
	})
}

func (s *Server) handleMaintenanceRuns(w http.ResponseWriter, r *http.Request) {
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	limit := maintenanceQueryInt(r, "limit", 50, 1, 200)
	offset := maintenanceQueryInt(r, "offset", 0, 0, 1_000_000)
	jobID := maintenanceJobID(r)
	filter := meta.IcebergMaintenanceRunFilter{
		OwnerJobID: jobID,
		Status:     maintenanceQueryText(r, "status", 64),
		Search:     maintenanceQueryText(r, "q", 200),
		Operation:  maintenanceQueryText(r, "operation", 64),
		Engine:     maintenanceQueryText(r, "engine", 32),
	}

	var (
		runs  []meta.IcebergMaintenanceRun
		total int
		err   error
	)
	if jobID != "" {
		runs, err = store.ListRunsForOwnerFiltered(r.Context(), filter, limit, offset)
		if err == nil {
			total, err = store.CountRunsForOwnerFiltered(r.Context(), filter)
		}
	} else {
		runs, err = store.ListRunsFiltered(r.Context(), filter, limit, offset)
		if err == nil {
			total, err = store.CountRunsFiltered(r.Context(), filter)
		}
	}
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if r.URL.Query().Get("include_results") == "1" {
		type runWithResults struct {
			meta.IcebergMaintenanceRun
			Results []meta.IcebergMaintenanceResult `json:"results"`
		}
		withResults := make([]runWithResults, 0, len(runs))
		for _, run := range runs {
			var results []meta.IcebergMaintenanceResult
			if jobID != "" {
				results, err = store.ListResultsForRunOwner(r.Context(), run.ID, jobID, 10)
			} else {
				results, err = store.ListResultsForRun(r.Context(), run.ID, 10)
			}
			if err != nil {
				maintenanceAPIError(w, err, http.StatusInternalServerError)
				return
			}
			withResults = append(withResults, runWithResults{IcebergMaintenanceRun: run, Results: results})
		}
		maintenanceAPIJSON(w, http.StatusOK, map[string]any{
			"runs":   withResults,
			"limit":  limit,
			"offset": offset,
			"total":  total,
			"job_id": jobID,
		})
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"runs":   runs,
		"limit":  limit,
		"offset": offset,
		"total":  total,
		"job_id": jobID,
	})
}

func (s *Server) handleMaintenanceRun(w http.ResponseWriter, r *http.Request) {
	runID, err := strconv.ParseInt(strings.TrimSpace(r.PathValue("id")), 10, 64)
	if err != nil || runID <= 0 {
		http.Error(w, "invalid maintenance run id", http.StatusBadRequest)
		return
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	limit := maintenanceQueryInt(r, "limit", 100, 1, 500)
	jobID := maintenanceJobID(r)

	var results []meta.IcebergMaintenanceResult
	if jobID != "" {
		results, err = store.ListResultsForRunOwner(r.Context(), runID, jobID, limit)
	} else {
		results, err = store.ListResultsForRun(r.Context(), runID, limit)
	}
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, map[string]any{
		"run_id":  runID,
		"results": results,
		"job_id":  jobID,
	})
}

func (s *Server) handleMaintenanceTableState(w http.ResponseWriter, r *http.Request) {
	tableKey := strings.TrimSpace(r.PathValue("key"))
	if tableKey == "" {
		http.Error(w, "maintenance table key is required", http.StatusBadRequest)
		return
	}
	store, ok := s.maintenanceAPIStore(w)
	if !ok {
		return
	}
	state, err := store.GetState(r.Context(), tableKey)
	if err != nil {
		maintenanceAPIError(w, err, http.StatusInternalServerError)
		return
	}
	if state == nil {
		http.Error(w, "maintenance table state not found", http.StatusNotFound)
		return
	}
	maintenanceAPIJSON(w, http.StatusOK, state)
}

func (s *Server) maintenanceAPIStore(w http.ResponseWriter) (*meta.IcebergMaintenanceStore, bool) {
	store, err := s.maintenanceStoreForView(nil)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "not configured") {
			status = http.StatusServiceUnavailable
		}
		maintenanceAPIError(w, err, status)
		return nil, false
	}
	return store, true
}

func maintenanceJobID(r *http.Request) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.URL.Query().Get("job_id"))
}

func maintenanceQueryInt(r *http.Request, key string, fallback, minValue, maxValue int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minValue || value > maxValue {
		return fallback
	}
	return value
}

func maintenanceQueryText(r *http.Request, key string, maxLength int) string {
	if r == nil {
		return ""
	}
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if maxLength > 0 && len(value) > maxLength {
		value = value[:maxLength]
	}
	return value
}

func maintenanceAPIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func maintenanceAPIError(w http.ResponseWriter, err error, status int) {
	maintenanceAPIJSON(w, status, map[string]string{"error": err.Error()})
}
