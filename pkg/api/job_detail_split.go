package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	connectoriceberg "github.com/gerinsp/rivus/pkg/connectors/iceberg"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

// handleJobDetail is the split-runtime aware job detail endpoint. The master
// owns no execution pipeline, so runtime progress comes from job_registry and
// Iceberg maintenance state comes directly from the durable maintenance store.
func (s *Server) handleJobDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	if err := s.jobManager.RefreshPersistedViews(r.Context()); err != nil {
		fmt.Printf("[api] refresh persisted job view failed job=%s: %v\n", id, err)
	}
	job, err := s.jobManager.Get(id)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	maintenance := any(job.TableMaintenanceStatus())
	if durable, durableErr := s.durableIcebergMaintenanceView(r.Context(), job); durableErr != nil {
		fmt.Printf("[api] durable maintenance view failed job=%s: %v\n", id, durableErr)
	} else if durable != nil {
		maintenance = durable
	}

	health := job.Health()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":                  job.Config.ID,
		"name":                job.Name(),
		"status":              job.GetStatus(),
		"health_status":       health.Status,
		"health_detail":       health.Detail,
		"created":             job.CreatedTime(),
		"updated":             job.UpdatedTime(),
		"meta_key":            job.MetaKey(),
		"checkpoint":          job.Checkpoint(),
		"progress":            job.Progress(),
		"iceberg_maintenance": maintenance,
		"last_error":          job.GetLastError(),
		"errors":              job.GetErrors(),
		"config":              job.DisplayConfig(),
	})
}

func (s *Server) durableIcebergMaintenanceView(ctx context.Context, job *core.Job) (map[string]any, error) {
	if job == nil || job.Config == nil || job.Config.Sink == nil || !strings.EqualFold(strings.TrimSpace(job.Config.Sink.Type), "iceberg_native") {
		return nil, nil
	}
	return s.durableIcebergMaintenanceConfigView(ctx, job.Config, job.Config.ID, false)
}

func (s *Server) durableIcebergMaintenanceConfigView(ctx context.Context, jobCfg *config.JobConfig, ownerID string, paused bool) (map[string]any, error) {
	if jobCfg == nil || jobCfg.Sink == nil || !strings.EqualFold(strings.TrimSpace(jobCfg.Sink.Type), "iceberg_native") {
		return nil, nil
	}

	var icebergCfg config.IcebergConfig
	if raw, err := yaml.Marshal(jobCfg.Sink.Config); err == nil {
		_ = yaml.Unmarshal(raw, &icebergCfg)
	}
	tm := icebergCfg.TableMaintenance

	store, err := s.maintenanceStoreForView(ctx)
	if err != nil {
		return nil, err
	}
	states, err := store.ListStatesForOwner(ctx, ownerID, 5000)
	if err != nil {
		return nil, err
	}
	summary, err := store.SummaryForOwner(ctx, ownerID)
	if err != nil {
		return nil, err
	}

	enabled := tm.Enabled || tm.NativeEnabled || len(states) > 0 || summary.Tables > 0
	if !enabled {
		return map[string]any{
			"enabled": false,
			"state":   "disabled",
		}, nil
	}

	dataThreshold := tm.DataFilesThreshold
	if dataThreshold <= 0 {
		dataThreshold = 200
	}
	deleteThreshold := tm.EqualityDeleteFilesThreshold
	if deleteThreshold <= 0 {
		deleteThreshold = 50
	}
	positionDeleteThreshold := tm.PositionDeleteFilesThreshold
	if positionDeleteThreshold <= 0 {
		positionDeleteThreshold = 25
	}
	smallFileSize := tm.SmallFileSizeBytes
	if smallFileSize <= 0 {
		smallFileSize = 64 * 1024 * 1024
	}
	smallFilesMinCount := tm.SmallFilesMinCount
	if smallFilesMinCount <= 0 {
		smallFilesMinCount = 10
	}
	smallFilesMinBytes := tm.SmallFilesMinTotalBytes
	if smallFilesMinBytes <= 0 {
		smallFilesMinBytes = 256 * 1024 * 1024
	}
	maxConcurrent := tm.MaxConcurrentJobs
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	resourceProfile := strings.TrimSpace(tm.RunnerResourceProfile)
	if resourceProfile == "" {
		resourceProfile = "small"
	}
	catalogName := strings.TrimSpace(tm.CatalogName)
	if catalogName == "" {
		catalogName = strings.TrimSpace(icebergCfg.CatalogName)
	}

	now := time.Now().UTC()
	tables := make([]map[string]any, 0, len(states))
	tablesReady := 0
	inventoryScanning := false
	inventoryPending := false
	inventoryStale := false

	for _, state := range states {
		tableState := connectoriceberg.DurableMaintenanceTableStateAt(state, tm, now)
		if tableState == "scanning" {
			inventoryScanning = true
		}
		if tableState == "inventory_pending" {
			inventoryPending = true
		}
		if tableState == "stale" {
			inventoryStale = true
		}
		if tableState == "ready" || tableState == "running" {
			tablesReady++
		}
		checkedAt := ""
		if state.LastInventoryAt != nil {
			checkedAt = state.LastInventoryAt.UTC().Format(time.RFC3339)
		}

		tables = append(tables, map[string]any{
			"namespace":                        state.Namespace,
			"table":                            state.Table,
			"identifier":                       state.Namespace + "." + state.Table,
			"state":                            tableState,
			"active_data_files":                state.ActiveDataFiles,
			"active_equality_delete_files":     state.ActiveEqualityDeleteFiles,
			"active_position_delete_files":     state.ActivePositionDeleteFiles,
			"applicable_equality_delete_files": state.ApplicableEqualityDeleteFiles,
			"applicable_position_delete_files": state.ApplicablePositionDeleteFiles,
			"eligible_small_files":             state.ActiveSmallFiles,
			"eligible_small_bytes":             state.ActiveSmallBytes,
			"compactable_files":                state.ActiveCompactableFiles,
			"compactable_bytes":                state.ActiveCompactableBytes,
			"compaction_groups":                state.ActiveCompactionGroups,
			"partition_fields":                 state.PartitionFields,
			"new_data_files":                   state.NewDataFiles,
			"new_equality_delete_files":        state.NewEqualityDeleteFiles,
			"checked_at":                       checkedAt,
			"error":                            state.LastError,
			"operations":                       []string{},
		})
	}

	sort.Slice(tables, func(i, j int) bool {
		return fmt.Sprint(tables[i]["identifier"]) < fmt.Sprint(tables[j]["identifier"])
	})

	state := durableMaintenanceOverallState(summary, summary.InventoriedTables, tablesReady, summary.InventoryErrors,
		inventoryScanning, inventoryPending, inventoryStale)
	if paused {
		state = "paused"
	}

	tablesTotal := summary.Tables
	if len(tm.Tables) > tablesTotal {
		tablesTotal = len(tm.Tables)
	}

	checkedAt := ""
	if summary.LatestInventoryAt != nil {
		checkedAt = summary.LatestInventoryAt.UTC().Format(time.RFC3339)
	}

	return map[string]any{
		"enabled":                          true,
		"state":                            state,
		"catalog_name":                     catalogName,
		"runner_resource_profile":          resourceProfile,
		"max_concurrent_jobs":              maxConcurrent,
		"data_files_threshold":             dataThreshold,
		"equality_delete_files_threshold":  deleteThreshold,
		"position_delete_files_threshold":  positionDeleteThreshold,
		"small_file_size_bytes":            smallFileSize,
		"small_files_min_count":            smallFilesMinCount,
		"small_files_min_total_bytes":      smallFilesMinBytes,
		"active_data_files":                summary.ActiveDataFiles,
		"active_equality_delete_files":     summary.EqualityDeletes,
		"active_position_delete_files":     summary.PositionDeletes,
		"applicable_equality_delete_files": summary.ApplicableEqualityDeletes,
		"applicable_position_delete_files": summary.ApplicablePositionDeletes,
		"applicable_counts_pending":        summary.ApplicableCountsPending,
		"eligible_small_files":             summary.ActiveSmallFiles,
		"eligible_small_bytes":             summary.ActiveSmallBytes,
		"tables_total":                     tablesTotal,
		"tables_scanned":                   summary.InventoriedTables,
		"tables_returned":                  len(tables),
		"tables_ready":                     tablesReady,
		"active_runs":                      summary.ActiveLeases,
		"inventory_errors":                 summary.InventoryErrors,
		"paused":                           paused,
		"checked_at":                       checkedAt,
		"tables":                           tables,
	}, nil
}

func durableMaintenanceOverallState(
	summary meta.IcebergMaintenanceOwnerSummary,
	tablesScanned int,
	tablesReady int,
	inventoryErrors int,
	inventoryScanning bool,
	inventoryPending bool,
	inventoryStale bool,
) string {
	switch {
	case summary.ActiveLeases > 0:
		return "running"
	case inventoryScanning:
		return "scanning"
	case inventoryErrors > 0:
		return "error"
	case inventoryPending:
		return "inventory_pending"
	case inventoryStale:
		return "stale"
	case summary.Blocked > 0 && summary.Blocked == summary.Tables:
		return "waiting_for_snapshot"
	case summary.Tables > 0 && tablesScanned == 0:
		return "inventory_pending"
	case summary.QueuedTasks > 0 || summary.RetryTasks > 0 || tablesReady > 0:
		return "ready"
	default:
		return "healthy"
	}
}
