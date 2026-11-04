package iceberg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

func TestBuildMaintenanceSQLUsesTypedAllowlistedOptions(t *testing.T) {
	sql, err := buildMaintenanceSQL("rivus", config.IcebergTarget{Namespace: "analytics", Table: "orders"}, "rewrite_data_files", map[string]any{
		"strategy": "binpack",
		"options": map[string]any{
			"min-input-files": "2",
		},
	})
	if err != nil {
		t.Fatalf("buildMaintenanceSQL returned error: %v", err)
	}
	want := "CALL `rivus`.system.`rewrite_data_files`(table => 'analytics.orders', options => map('min-input-files', '2'), strategy => 'binpack')"
	if sql != want {
		t.Fatalf("SQL = %q, want %q", sql, want)
	}
}

func TestBuildMaintenanceSQLRejectsUnknownOption(t *testing.T) {
	_, err := buildMaintenanceSQL("rivus", config.IcebergTarget{Namespace: "analytics", Table: "orders"}, "expire_snapshots", map[string]any{
		"sql": "DROP TABLE important",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported expire_snapshots option") {
		t.Fatalf("error = %v, want unsupported option", err)
	}
}

func TestValidateStreamingMaintenanceSafety(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	err := validateStreamingMaintenanceSafety("remove_orphan_files", map[string]any{
		"older_than": now.Add(-time.Hour).Format(time.RFC3339),
	}, now)
	if err == nil {
		t.Fatal("expected recent orphan cutoff to be rejected")
	}
	if err := validateStreamingMaintenanceSafety("remove_orphan_files", map[string]any{
		"older_than": now.Add(-time.Hour).Format(time.RFC3339),
		"dry_run":    true,
	}, now); err != nil {
		t.Fatalf("dry run returned error: %v", err)
	}
}

func TestAutomaticOperationsDueCombinesFileAndTimeTriggers(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	state := &tableMaintenanceWatchState{
		newDataFiles:        100,
		newEqualityDeletes:  50,
		activeSmallFiles:    10,
		activeSmallBytes:    256 * 1024 * 1024,
		activeEqDeletes:     50,
		eligibilityReady:    true,
		lastExpireSnapshots: now.Add(-6 * time.Hour),
		lastOrphanCleanup:   now.Add(-24 * time.Hour),
	}
	cfg := config.IcebergTableMaintenanceConfig{
		DataFilesThreshold:             100,
		EqualityDeleteFilesThreshold:   50,
		SmallFileSizeBytes:             64 * 1024 * 1024,
		SmallFilesMinCount:             10,
		SmallFilesMinTotalBytes:        256 * 1024 * 1024,
		ExpireSnapshotsIntervalSeconds: int((6 * time.Hour).Seconds()),
		ExpireSnapshotsOlderThanHours:  168,
		ExpireSnapshotsRetainLast:      10,
		OrphanCleanupIntervalSeconds:   int((24 * time.Hour).Seconds()),
		OrphanCleanupOlderThanHours:    72,
	}
	operations := automaticOperationsDue(state, cfg, now)
	if len(operations) != 3 {
		t.Fatalf("operations = %#v, want compaction, expiration, and orphan cleanup", operations)
	}
	if operations[0].Type != "rewrite_data_files" || operations[1].Type != "expire_snapshots" || operations[2].Type != "remove_orphan_files" {
		t.Fatalf("operation order = %#v", operations)
	}
	rewriteOptions := operations[0].Options["options"].(map[string]any)
	if rewriteOptions["delete-file-threshold"] != "1" {
		t.Fatalf("delete-file-threshold = %#v, want 1", rewriteOptions["delete-file-threshold"])
	}
}

func TestAutomaticMaintenanceDefersSubmissionAcrossInitialSnapshot(t *testing.T) {
	now := time.Now()
	monitor := &tableMaintenanceMonitor{
		cfg: config.IcebergConfig{TableMaintenance: config.IcebergTableMaintenanceConfig{
			DataFilesThreshold:      100,
			SmallFilesMinCount:      10,
			SmallFilesMinTotalBytes: 256 * 1024 * 1024,
		}},
		states: map[string]*tableMaintenanceWatchState{
			"analytics.orders": {
				target:           config.IcebergTarget{Namespace: "analytics", Table: "orders"},
				initialized:      true,
				activeSmallFiles: 100,
				activeSmallBytes: 256 * 1024 * 1024,
				eligibilityReady: true,
			},
		},
		wake: make(chan struct{}, 1),
	}

	monitor.pauseSubmissions()
	monitor.mu.Lock()
	key, _, operations := monitor.nextDueLocked(now)
	monitor.mu.Unlock()
	if key != "" || len(operations) != 0 {
		t.Fatalf("paused maintenance returned key=%q operations=%v", key, operations)
	}

	monitor.resumeSubmissions()
	monitor.mu.Lock()
	key, _, operations = monitor.nextDueLocked(now)
	monitor.mu.Unlock()
	if key != "analytics.orders" || len(operations) != 1 || operations[0].Type != "rewrite_data_files" {
		t.Fatalf("resumed maintenance returned key=%q operations=%v", key, operations)
	}
}

func TestAutomaticCompactionUsesSmallFileCountWithoutByteFloor(t *testing.T) {
	cfg := config.IcebergTableMaintenanceConfig{
		DataFilesThreshold:      200,
		SmallFilesMinCount:      10,
		SmallFilesMinTotalBytes: 256 * 1024 * 1024,
	}
	state := &tableMaintenanceWatchState{
		eligibilityReady: true,
		activeSmallFiles: 199,
		activeSmallBytes: 14 * 1024 * 1024,
	}
	if operations := automaticOperationsDue(state, cfg, time.Now()); len(operations) != 0 {
		t.Fatalf("operations = %#v, want no compaction below the small-file count threshold", operations)
	}
	state.activeSmallFiles = 200
	if operations := automaticOperationsDue(state, cfg, time.Now()); len(operations) != 1 || operations[0].Type != "rewrite_data_files" {
		t.Fatalf("operations = %#v, want compaction at the small-file count threshold", operations)
	}
}

func TestAutomaticCompactionUsesActiveCountsAfterMonitorRestart(t *testing.T) {
	cfg := config.IcebergTableMaintenanceConfig{
		DataFilesThreshold:           200,
		EqualityDeleteFilesThreshold: 50,
		SmallFilesMinCount:           10,
		SmallFilesMinTotalBytes:      256 * 1024 * 1024,
	}

	dataState := &tableMaintenanceWatchState{
		eligibilityReady: true,
		activeSmallFiles: 200,
		activeSmallBytes: 14 * 1024 * 1024,
	}
	if operations := automaticOperationsDue(dataState, cfg, time.Now()); len(operations) != 1 || operations[0].Type != "rewrite_data_files" {
		t.Fatalf("data operations after restart = %#v", operations)
	}

	deleteState := &tableMaintenanceWatchState{
		eligibilityReady: true,
		activeEqDeletes:  50,
	}
	if operations := automaticOperationsDue(deleteState, cfg, time.Now()); len(operations) != 1 || operations[0].Type != "rewrite_data_files" {
		t.Fatalf("delete operations after restart = %#v", operations)
	}
}

func TestTableMaintenanceStatusSummarizesCurrentFileInventory(t *testing.T) {
	now := time.Date(2026, 8, 14, 10, 0, 0, 0, time.UTC)
	monitor := &tableMaintenanceMonitor{
		cfg: config.IcebergConfig{
			Warehouse: "asmat",
			TableMaintenance: config.IcebergTableMaintenanceConfig{
				Enabled:                      true,
				CatalogName:                  "asmat",
				RunnerResourceProfile:        "small",
				MaxConcurrentJobs:            1,
				DataFilesThreshold:           200,
				EqualityDeleteFilesThreshold: 50,
				SmallFileSizeBytes:           64 * 1024 * 1024,
				SmallFilesMinCount:           10,
				SmallFilesMinTotalBytes:      256 * 1024 * 1024,
			},
		},
		states: map[string]*tableMaintenanceWatchState{
			"analytics.orders": {
				target:              config.IcebergTarget{Namespace: "analytics", Table: "orders"},
				initialized:         true,
				newDataFiles:        200,
				newEqualityDeletes:  35,
				activeDataFiles:     220,
				activeSmallFiles:    220,
				activeSmallBytes:    512 * 1024 * 1024,
				activeEqDeletes:     35,
				eligibilityReady:    true,
				lastCheckedAt:       now.Add(-time.Minute),
				lastExpireSnapshots: now,
				lastOrphanCleanup:   now,
			},
			"analytics.customers": {
				target:              config.IcebergTarget{Namespace: "analytics", Table: "customers"},
				initialized:         true,
				newDataFiles:        3,
				activeDataFiles:     90,
				activeSmallFiles:    25,
				activeSmallBytes:    64 * 1024 * 1024,
				activeEqDeletes:     7,
				eligibilityReady:    true,
				lastCheckedAt:       now.Add(-2 * time.Minute),
				lastExpireSnapshots: now,
				lastOrphanCleanup:   now,
			},
		},
	}

	status := monitor.statusLocked(now)
	if status.State != "ready" || status.TablesReady != 1 || status.TablesScanned != 2 || status.TablesTotal != 2 {
		t.Fatalf("status summary = %#v", status)
	}
	if status.ActiveDataFiles != 310 || status.ActiveEqualityDeleteFiles != 42 {
		t.Fatalf("active file totals = data:%d equality:%d", status.ActiveDataFiles, status.ActiveEqualityDeleteFiles)
	}
	if status.EligibleSmallFiles != 245 || status.EligibleSmallBytes != 576*1024*1024 {
		t.Fatalf("eligible totals = files:%d bytes:%d", status.EligibleSmallFiles, status.EligibleSmallBytes)
	}
	if len(status.Tables) != 2 || status.Tables[0].Catalog != "asmat" || status.Tables[0].Identifier != "asmat.analytics.customers" || status.Tables[1].State != "ready" {
		t.Fatalf("table status = %#v", status.Tables)
	}
	if status.CheckedAt != now.Add(-time.Minute).Format(time.RFC3339) {
		t.Fatalf("checked_at = %q", status.CheckedAt)
	}
}

func TestNormalizeIcebergConfigDefaultsAutomaticMaintenancePolicy(t *testing.T) {
	cfg := normalizeIcebergConfig(config.IcebergConfig{
		TableMaintenance: config.IcebergTableMaintenanceConfig{Enabled: true},
	})
	maintenance := cfg.TableMaintenance
	if maintenance.DataFilesThreshold != 200 || maintenance.EqualityDeleteFilesThreshold != 50 || maintenance.PositionDeleteFilesThreshold != 25 {
		t.Fatalf("file thresholds = %d/%d/%d, want 200/50/25", maintenance.DataFilesThreshold, maintenance.EqualityDeleteFilesThreshold, maintenance.PositionDeleteFilesThreshold)
	}
	if maintenance.ExpireSnapshotsIntervalSeconds != 6*60*60 || maintenance.OrphanCleanupIntervalSeconds != 24*60*60 {
		t.Fatalf("time intervals = %d/%d", maintenance.ExpireSnapshotsIntervalSeconds, maintenance.OrphanCleanupIntervalSeconds)
	}
	if maintenance.OrphanCleanupOlderThanHours != 72 {
		t.Fatalf("orphan cutoff = %v hours, want 72", maintenance.OrphanCleanupOlderThanHours)
	}
	if maintenance.RunnerResourceProfile != "small" {
		t.Fatalf("runner resource profile = %q, want small", maintenance.RunnerResourceProfile)
	}
	if maintenance.SmallFileSizeBytes != 64*1024*1024 || maintenance.SmallFilesMinCount != 10 || maintenance.SmallFilesMinTotalBytes != 256*1024*1024 {
		t.Fatalf("small-file eligibility = %d/%d/%d", maintenance.SmallFileSizeBytes, maintenance.SmallFilesMinCount, maintenance.SmallFilesMinTotalBytes)
	}
	if maintenance.CompactOptions["strategy"] != "binpack" {
		t.Fatalf("compact strategy = %#v, want binpack", maintenance.CompactOptions["strategy"])
	}
	rewriteOptions := stringAnyMap(maintenance.CompactOptions["options"])
	if rewriteOptions["target-file-size-bytes"] != "134217728" ||
		rewriteOptions["max-file-group-size-bytes"] != "268435456" ||
		rewriteOptions["max-concurrent-file-group-rewrites"] != "1" {
		t.Fatalf("compact options = %#v", rewriteOptions)
	}
}

func TestSubmitTableMaintenanceBuildsSparkRequest(t *testing.T) {
	t.Setenv("RIVUS_RUNNER_URI", "http://runner-from-environment.invalid")
	t.Setenv("RUNNER_API_TOKEN", "environment-token")

	var got sparkCreateSubmissionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/submissions/create" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode Spark request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"action":"CreateSubmissionResponse","message":"submitted","submissionId":"driver-1","success":true}`))
	}))
	defer server.Close()

	jobCfg := maintenanceTestJobConfig(server.URL)
	result, err := SubmitTableMaintenanceForJobConfig(context.Background(), "job-1", jobCfg, TableMaintenanceRequest{
		Tables:     []string{"analytics.orders"},
		Operations: []TableMaintenanceOperation{{Type: "rewrite_data_files"}},
	}, true)
	if err != nil {
		t.Fatalf("SubmitTableMaintenanceForJobConfig returned error: %v", err)
	}
	if result.SubmissionID != "driver-1" {
		t.Fatalf("submission ID = %q", result.SubmissionID)
	}
	if got.MainClass != defaultSparkMaintenanceMainClass || got.AppResource != "" || len(got.AppArgs) != 3 || got.AppArgs[0] != "local:///jobs/maintenance.py" {
		t.Fatalf("Spark request = %#v", got)
	}
	if got.SparkProperties["spark.sql.catalog.rivus.type"] != "rest" {
		t.Fatalf("Spark catalog properties = %#v", got.SparkProperties)
	}
	var payload maintenancePayload
	if err := json.Unmarshal([]byte(got.AppArgs[2]), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(payload.Statements) != 1 || !strings.Contains(payload.Statements[0].SQL, "rewrite_data_files") {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestRunnerAppMaintenanceSubmissionStatusAndCancel(t *testing.T) {
	var submitted runnerMaintenanceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Runner-Token"); got != "runner-secret" {
			t.Fatalf("X-Runner-Token = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/internal/system/jobs/sql":
			if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
				t.Fatalf("decode runner request: %v", err)
			}
			_, _ = w.Write([]byte(`{"job_id":"runner-job-1","job_name":"maintenance","status":"starting"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/internal/system/jobs/runner-job-1":
			_, _ = w.Write([]byte(`{"job_id":"runner-job-1","job_name":"maintenance","status":"finished"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/system/jobs/runner-job-1/cancel":
			_, _ = w.Write([]byte(`{"job_id":"runner-job-1","job_name":"maintenance","status":"cancelled"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	jobCfg := runnerMaintenanceTestJobConfig(server.URL)
	result, err := SubmitTableMaintenanceForJobConfig(context.Background(), "job-1", jobCfg, TableMaintenanceRequest{
		Tables:          []string{"analytics.orders"},
		Operations:      []TableMaintenanceOperation{{Type: "rewrite_data_files"}},
		ExternalRunKey:  "stable-maintenance-task-42",
		ResourceProfile: "xlarge",
	}, true)
	if err != nil {
		t.Fatalf("submit through runner-app: %v", err)
	}
	if result.SubmissionID != "runner-job-1" || result.Action != "RunnerJobSubmission" {
		t.Fatalf("submission = %#v", result)
	}
	if !strings.Contains(submitted.Content, "rewrite_data_files") || submitted.ResourceProfile != "xlarge" {
		t.Fatalf("runner request = %#v", submitted)
	}
	if submitted.ExternalRunKey != "stable-maintenance-task-42" {
		t.Fatalf("external run key = %q", submitted.ExternalRunKey)
	}
	maintenance := submitted.JobContext["iceberg_maintenance"].(map[string]any)
	if submitted.JobContext["maintenance_mode"] != "compaction" || maintenance["mode"] != "compaction" {
		t.Fatalf("maintenance display mode context = %#v", submitted.JobContext)
	}
	if maintenance["pause_rivus_writers"] != true {
		t.Fatalf("pause_rivus_writers = %#v", maintenance["pause_rivus_writers"])
	}

	status, err := GetTableMaintenanceStatusForJobConfig(context.Background(), jobCfg, result.SubmissionID)
	if err != nil || status.DriverState != "FINISHED" {
		t.Fatalf("runner status = %#v, err=%v", status, err)
	}
	cancelled, err := CancelTableMaintenanceForJobConfig(context.Background(), jobCfg, result.SubmissionID)
	if err != nil || cancelled.DriverState != "KILLED" {
		t.Fatalf("runner cancel = %#v, err=%v", cancelled, err)
	}
}

func TestRunnerAppCleanupSubmissionKeepsRivusWritersOnline(t *testing.T) {
	var submitted runnerMaintenanceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
			t.Fatalf("decode runner request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"runner-cleanup-1","job_name":"maintenance","status":"starting"}`))
	}))
	defer server.Close()

	jobCfg := runnerMaintenanceTestJobConfig(server.URL)
	_, err := SubmitTableMaintenanceForJobConfig(context.Background(), "job-1", jobCfg, TableMaintenanceRequest{
		Tables:     []string{"analytics.orders"},
		Operations: []TableMaintenanceOperation{{Type: "expire_snapshots"}},
	}, true)
	if err != nil {
		t.Fatalf("submit cleanup through runner-app: %v", err)
	}
	maintenance := submitted.JobContext["iceberg_maintenance"].(map[string]any)
	if submitted.JobContext["maintenance_mode"] != "cleanup" || maintenance["pause_rivus_writers"] != false {
		t.Fatalf("cleanup coordination context = %#v", submitted.JobContext)
	}
}

func TestAutomaticSparkCompactionCoordinatesOnlyAfterRepeatedCommitConflict(t *testing.T) {
	for _, tt := range []struct {
		name      string
		attempt   int
		lastError string
		want      bool
	}{
		{name: "first conflict", attempt: 1, lastError: "CommitFailedException: branch main has changed", want: false},
		{name: "second conflict", attempt: 2, lastError: "CommitFailedException: branch main has changed", want: false},
		{name: "third conflict", attempt: 3, lastError: "CommitFailedException: branch main has changed", want: true},
		{name: "fifth branch conflict", attempt: 5, lastError: "Requirement failed: branch main has changed", want: true},
		{name: "third resource failure", attempt: 3, lastError: "Spark executor out of memory", want: false},
		{name: "third timeout", attempt: 3, lastError: "context deadline exceeded", want: false},
		{name: "third attempt without prior failure", attempt: 3, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := meta.IcebergMaintenanceTask{AttemptCount: tt.attempt, LastError: tt.lastError}
			if got := shouldCoordinateSparkCompaction(task); got != tt.want {
				t.Fatalf("coordination = %t, want %t", got, tt.want)
			}
		})
	}
}

func maintenanceTestJobConfig(sparkRESTURI string) *config.JobConfig {
	return &config.JobConfig{
		Name: "maintenance test",
		Sink: &config.ConnectorSpec{
			Type: "iceberg_native",
			Config: map[string]any{
				"rest_uri":     "http://iceberg-rest:8181",
				"warehouse":    "warehouse",
				"catalog_name": "rivus",
				"table_maintenance": map[string]any{
					"spark_rest_uri": sparkRESTURI,
					"spark_master":   "spark://spark-master:7077",
					"app_resource":   "local:///jobs/maintenance.py",
				},
			},
		},
	}
}

func runnerMaintenanceTestJobConfig(runnerURI string) *config.JobConfig {
	return &config.JobConfig{
		Name: "maintenance test",
		Sink: &config.ConnectorSpec{
			Type: "iceberg_native",
			Config: map[string]any{
				"rest_uri":     "http://iceberg-rest:8181",
				"warehouse":    "warehouse",
				"catalog_name": "rivus",
				"table_maintenance": map[string]any{
					"runner_uri":              runnerURI,
					"runner_api_token":        "runner-secret",
					"runner_resource_profile": "small",
				},
			},
		},
	}
}
