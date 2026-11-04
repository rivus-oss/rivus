package iceberg

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

// tableMaintenanceMonitor is retained as a compatibility name because Sink and
// the job runtime already expose a maintenance status reporter. It is no longer
// an automatic scheduler. In worker mode it only bridges durable MySQL worker
// state into the existing job-details status shape.
type tableMaintenanceMonitor struct {
	jobID   string
	jobName string
	cfg     config.IcebergConfig
	sink    *Sink

	mu             sync.Mutex
	states         map[string]*tableMaintenanceWatchState // legacy test compatibility only
	wake           chan struct{}
	paused         bool
	statusReporter connector.TableMaintenanceStatusReporter
}

// These legacy state shapes are kept only so old unit tests and helper logic can
// continue validating compaction thresholds. Runtime scheduling no longer uses
// them.
type tableMaintenanceWatchState struct {
	target              config.IcebergTarget
	initialized         bool
	lastSequenceNumber  int64
	newDataFiles        int
	newEqualityDeletes  int
	activeSmallFiles    int
	activeSmallBytes    int64
	activeDataFiles     int
	activeEqDeletes     int
	eligibilityReady    bool
	eligibilityDirty    bool
	lastCheckedAt       time.Time
	lastInventoryError  string
	lastExpireSnapshots time.Time
	lastOrphanCleanup   time.Time
	active              *activeTableMaintenance
}

type activeTableMaintenance struct {
	submissionID       string
	operations         []string
	submittedDataFiles int
	submittedEqDeletes int
	previousExpireAt   time.Time
	previousOrphanAt   time.Time
}

func newTableMaintenanceMonitor(jobID, jobName string, cfg config.IcebergConfig, sink *Sink) *tableMaintenanceMonitor {
	// NativeEnabled is the normalized worker gate. The old Enabled-based
	// in-process scheduler is intentionally never started.
	if !cfg.TableMaintenance.NativeEnabled {
		return nil
	}
	return &tableMaintenanceMonitor{
		jobID:   jobID,
		jobName: jobName,
		cfg:     cfg,
		sink:    sink,
		states:  make(map[string]*tableMaintenanceWatchState),
		wake:    make(chan struct{}, 1),
	}
}

func validateAutomaticTableMaintenanceConfig(cfg config.IcebergConfig) error {
	// Worker-mode validation happens in nativeMaintenanceSettingsFromRaw. The
	// legacy Spark scheduler/backend validation is intentionally retired.
	return nil
}

func (m *tableMaintenanceMonitor) pauseSubmissions() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.paused = true
	m.mu.Unlock()
	m.publishStatus()
}

func (m *tableMaintenanceMonitor) resumeSubmissions() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.paused = false
	m.mu.Unlock()
	m.publishStatus()
}

func (m *tableMaintenanceMonitor) setStatusReporter(reporter connector.TableMaintenanceStatusReporter) {
	if m == nil || reporter == nil {
		return
	}
	m.mu.Lock()
	m.statusReporter = reporter
	m.mu.Unlock()
	reporter(&connector.TableMaintenanceStatus{
		Enabled:                      true,
		State:                        "watching",
		CatalogName:                  maintenanceCatalogName(m.cfg),
		RunnerResourceProfile:        m.cfg.TableMaintenance.RunnerResourceProfile,
		MaxConcurrentJobs:            1,
		DataFilesThreshold:           m.cfg.TableMaintenance.DataFilesThreshold,
		EqualityDeleteFilesThreshold: m.cfg.TableMaintenance.EqualityDeleteFilesThreshold,
		PositionDeleteFilesThreshold: m.cfg.TableMaintenance.PositionDeleteFilesThreshold,
		SmallFileSizeBytes:           m.cfg.TableMaintenance.SmallFileSizeBytes,
		SmallFilesMinCount:           m.cfg.TableMaintenance.SmallFilesMinCount,
		SmallFilesMinTotalBytes:      m.cfg.TableMaintenance.SmallFilesMinTotalBytes,
	})
}

func (m *tableMaintenanceMonitor) registerTable(config.IcebergTarget, *icetable.Table, time.Time) {}
func (m *tableMaintenanceMonitor) observeTable(config.IcebergTarget, *icetable.Table, time.Time)  {}

func (m *tableMaintenanceMonitor) run(ctx context.Context) {
	if m == nil {
		return
	}
	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	if dsn == "" {
		m.publishBridgeError("maintenance metadata store is not configured")
		<-ctx.Done()
		return
	}
	store, err := sharedNativeMaintenanceSignalStore(dsn)
	if err != nil {
		m.publishBridgeError(err.Error())
		<-ctx.Done()
		return
	}

	m.publishDurableStatus(ctx, store)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.publishDurableStatus(ctx, store)
		case <-m.wake:
			m.publishDurableStatus(ctx, store)
		}
	}
}

func (m *tableMaintenanceMonitor) publishBridgeError(message string) {
	m.mu.Lock()
	reporter := m.statusReporter
	m.mu.Unlock()
	if reporter == nil {
		return
	}
	reporter(&connector.TableMaintenanceStatus{
		Enabled:         true,
		State:           "error",
		CatalogName:     maintenanceCatalogName(m.cfg),
		InventoryErrors: 1,
		Tables: []connector.TableMaintenanceTableStatus{{
			Identifier: "maintenance-worker",
			State:      "error",
			Error:      message,
		}},
	})
}

func (m *tableMaintenanceMonitor) publishDurableStatus(parent context.Context, store *meta.IcebergMaintenanceStore) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	states, err := store.ListStatesForOwner(ctx, m.jobID, 5000)
	if err != nil {
		m.publishBridgeError(err.Error())
		return
	}
	summary, err := store.SummaryForOwner(ctx, m.jobID)
	if err != nil {
		m.publishBridgeError(err.Error())
		return
	}

	status := &connector.TableMaintenanceStatus{
		Enabled:                      true,
		State:                        "healthy",
		CatalogName:                  maintenanceCatalogName(m.cfg),
		RunnerResourceProfile:        m.cfg.TableMaintenance.RunnerResourceProfile,
		MaxConcurrentJobs:            1,
		DataFilesThreshold:           m.cfg.TableMaintenance.DataFilesThreshold,
		EqualityDeleteFilesThreshold: m.cfg.TableMaintenance.EqualityDeleteFilesThreshold,
		PositionDeleteFilesThreshold: m.cfg.TableMaintenance.PositionDeleteFilesThreshold,
		SmallFileSizeBytes:           m.cfg.TableMaintenance.SmallFileSizeBytes,
		SmallFilesMinCount:           m.cfg.TableMaintenance.SmallFilesMinCount,
		SmallFilesMinTotalBytes:      m.cfg.TableMaintenance.SmallFilesMinTotalBytes,
		TablesTotal:                  summary.Tables,
		ActiveRuns:                   summary.ActiveLeases,
	}

	var latest time.Time
	inventoryScanning := false
	inventoryPending := false
	inventoryStale := false
	for _, state := range states {
		tableState := durableMaintenanceTableState(state, m.cfg.TableMaintenance)
		if tableState == "scanning" {
			inventoryScanning = true
		}
		if tableState == "inventory_pending" {
			inventoryPending = true
		}
		if tableState == "stale" {
			inventoryStale = true
		}
		operations := []string{}
		if result, resultErr := store.LatestResultForTable(ctx, state.TableKey); resultErr == nil && result != nil {
			operations = append(operations, result.Operation)
			if result.Engine != "" {
				operations = append(operations, "engine:"+result.Engine)
			}
			if result.RoutingReason != "" {
				operations = append(operations, "route:"+result.RoutingReason)
			}
		}
		checkedAt := ""
		if state.LastInventoryAt != nil {
			checkedAt = state.LastInventoryAt.UTC().Format(time.RFC3339)
			status.TablesScanned++
		}
		status.Tables = append(status.Tables, connector.TableMaintenanceTableStatus{
			Catalog:                       state.Catalog,
			Namespace:                     state.Namespace,
			Table:                         state.Table,
			Identifier:                    state.Catalog + "." + state.Namespace + "." + state.Table,
			State:                         tableState,
			ActiveDataFiles:               state.ActiveDataFiles,
			ActiveEqualityDeleteFiles:     state.ActiveEqualityDeleteFiles,
			ActivePositionDeleteFiles:     state.ActivePositionDeleteFiles,
			ApplicableEqualityDeleteFiles: state.EffectiveEqualityDeleteFiles(),
			ApplicablePositionDeleteFiles: state.EffectivePositionDeleteFiles(),
			EligibleSmallFiles:            state.ActiveSmallFiles,
			EligibleSmallBytes:            state.ActiveSmallBytes,
			CompactableFiles:              state.ActiveCompactableFiles,
			CompactableBytes:              state.ActiveCompactableBytes,
			CompactionGroups:              state.ActiveCompactionGroups,
			PartitionFields:               state.PartitionFields,
			NewDataFiles:                  state.NewDataFiles,
			NewEqualityDeleteFiles:        state.NewEqualityDeleteFiles,
			CheckedAt:                     checkedAt,
			Error:                         state.LastError,
			Operations:                    operations,
		})
		status.ActiveDataFiles += state.ActiveDataFiles
		status.ActiveEqualityDeleteFiles += state.ActiveEqualityDeleteFiles
		status.ActivePositionDeleteFiles += state.ActivePositionDeleteFiles
		status.ApplicableEqualityDeleteFiles += state.EffectiveEqualityDeleteFiles()
		status.ApplicablePositionDeleteFiles += state.EffectivePositionDeleteFiles()
		status.EligibleSmallFiles += state.ActiveSmallFiles
		status.EligibleSmallBytes += state.ActiveSmallBytes
		if tableState == "ready" || tableState == "running" {
			status.TablesReady++
		}
		if state.LastError != "" {
			status.InventoryErrors++
		}
		if state.LastInventoryAt != nil && state.LastInventoryAt.After(latest) {
			latest = *state.LastInventoryAt
		}
	}

	switch {
	case summary.ActiveLeases > 0:
		status.State = "running"
	case inventoryScanning:
		status.State = "scanning"
	case status.InventoryErrors > 0:
		status.State = "error"
	case inventoryPending:
		status.State = "inventory_pending"
	case inventoryStale:
		status.State = "stale"
	case summary.Blocked > 0 && summary.Blocked == summary.Tables:
		status.State = "waiting_for_snapshot"
	case status.TablesTotal > 0 && status.TablesScanned == 0:
		status.State = "inventory_pending"
	case summary.QueuedTasks > 0 || summary.RetryTasks > 0 || status.TablesReady > 0:
		status.State = "ready"
	}
	if !latest.IsZero() {
		status.CheckedAt = latest.UTC().Format(time.RFC3339)
	}
	sort.Slice(status.Tables, func(i, j int) bool { return status.Tables[i].Identifier < status.Tables[j].Identifier })

	m.mu.Lock()
	reporter := m.statusReporter
	m.mu.Unlock()
	if reporter != nil {
		reporter(status)
	}
}

func durableMaintenanceTableState(state meta.IcebergMaintenanceState, cfg config.IcebergTableMaintenanceConfig) string {
	return DurableMaintenanceTableStateAt(state, cfg, time.Now().UTC())
}

// DurableMaintenanceTableStateAt returns a table's durable maintenance state.
func DurableMaintenanceTableStateAt(state meta.IcebergMaintenanceState, cfg config.IcebergTableMaintenanceConfig, now time.Time) string {
	if !state.SnapshotComplete {
		return "waiting_for_snapshot"
	}
	if state.InventoryLeaseUntil != nil && state.InventoryLeaseUntil.After(now) {
		return "scanning"
	}
	if state.LeaseUntil != nil && state.LeaseUntil.After(now) {
		return "running"
	}
	if state.LastError != "" {
		return "error"
	}
	if state.NextInventoryCheckAt != nil {
		return "inventory_pending"
	}
	if state.LastInventoryAt == nil {
		return "inventory_pending"
	}
	if state.LastInventoryAt.Before(now.Add(-meta.MaintenanceInventoryMaxAge)) {
		return "stale"
	}
	dataThreshold := cfg.DataFilesThreshold
	if dataThreshold <= 0 {
		dataThreshold = defaultDataFilesThreshold
	}
	deleteThreshold := cfg.EqualityDeleteFilesThreshold
	if deleteThreshold <= 0 {
		deleteThreshold = defaultEqualityDeleteFilesThreshold
	}
	positionDeleteThreshold := cfg.PositionDeleteFilesThreshold
	if positionDeleteThreshold <= 0 {
		positionDeleteThreshold = defaultPositionDeleteFilesThreshold
	}
	smallMinCount := cfg.SmallFilesMinCount
	if smallMinCount <= 0 {
		smallMinCount = defaultNativeMinSmallFiles
	}
	smallMinBytes := cfg.SmallFilesMinTotalBytes
	if smallMinBytes <= 0 {
		smallMinBytes = defaultNativeMinSmallBytes
	}

	dataPressure := state.ActiveSmallFiles >= dataThreshold
	dataReady := dataPressure && state.ActiveCompactionGroups > 0
	deleteReady := state.EffectiveEqualityDeleteFiles() >= deleteThreshold
	positionDeleteReady := state.EffectivePositionDeleteFiles() >= positionDeleteThreshold
	smallBytesReady := state.ActiveCompactableFiles >= smallMinCount && state.ActiveCompactableBytes >= smallMinBytes
	if dataReady || deleteReady || positionDeleteReady || smallBytesReady {
		return "ready"
	}
	if dataPressure && state.ActiveCompactionGroups == 0 {
		return "partitioned"
	}
	return "healthy"
}

func snapshotSummaryCount(properties map[string]string, key string) int {
	value, err := strconv.Atoi(strings.TrimSpace(properties[key]))
	if err != nil || value < 0 {
		return 0
	}
	return value
}

// Legacy threshold helpers remain for compatibility tests only. Runtime
// scheduling is performed by maintenance_worker.go against durable state.
func automaticOperationsDue(state *tableMaintenanceWatchState, cfg config.IcebergTableMaintenanceConfig, now time.Time) []TableMaintenanceOperation {
	if state == nil || state.active != nil || !state.eligibilityReady {
		return nil
	}
	operations := make([]TableMaintenanceOperation, 0, 3)
	dataTrigger := cfg.DataFilesThreshold > 0 && state.activeSmallFiles >= cfg.DataFilesThreshold
	deleteTrigger := cfg.EqualityDeleteFilesThreshold > 0 && state.activeEqDeletes >= cfg.EqualityDeleteFilesThreshold
	smallEnough := cfg.SmallFilesMinCount > 0 && cfg.SmallFilesMinTotalBytes > 0 && state.activeSmallFiles >= cfg.SmallFilesMinCount && state.activeSmallBytes >= cfg.SmallFilesMinTotalBytes
	if deleteTrigger || dataTrigger || smallEnough {
		options := cloneStringAnyMap(cfg.CompactOptions)
		if options == nil {
			options = map[string]any{}
		}
		rewrite := stringAnyMap(options["options"])
		if rewrite == nil {
			rewrite = map[string]any{}
		}
		if deleteTrigger {
			rewrite["delete-file-threshold"] = "1"
		}
		options["options"] = rewrite
		operations = append(operations, TableMaintenanceOperation{Type: "rewrite_data_files", Options: options})
	}
	if cfg.ExpireSnapshotsIntervalSeconds > 0 && !state.lastExpireSnapshots.IsZero() && now.Sub(state.lastExpireSnapshots) >= time.Duration(cfg.ExpireSnapshotsIntervalSeconds)*time.Second {
		operations = append(operations, TableMaintenanceOperation{Type: "expire_snapshots", Options: map[string]any{
			"older_than":  now.Add(-time.Duration(cfg.ExpireSnapshotsOlderThanHours * float64(time.Hour))).UTC().Format(time.RFC3339),
			"retain_last": cfg.ExpireSnapshotsRetainLast,
		}})
	}
	if cfg.OrphanCleanupIntervalSeconds > 0 && !state.lastOrphanCleanup.IsZero() && now.Sub(state.lastOrphanCleanup) >= time.Duration(cfg.OrphanCleanupIntervalSeconds)*time.Second {
		operations = append(operations, TableMaintenanceOperation{Type: "remove_orphan_files", Options: map[string]any{
			"older_than": now.Add(-time.Duration(cfg.OrphanCleanupOlderThanHours * float64(time.Hour))).UTC().Format(time.RFC3339),
		}})
	}
	return operations
}

func (m *tableMaintenanceMonitor) nextDueLocked(now time.Time) (string, config.IcebergTarget, []TableMaintenanceOperation) {
	if m == nil || m.paused {
		return "", config.IcebergTarget{}, nil
	}
	keys := make([]string, 0, len(m.states))
	for key := range m.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		state := m.states[key]
		if state == nil || !state.initialized {
			continue
		}
		operations := automaticOperationsDue(state, m.cfg.TableMaintenance, now)
		if len(operations) > 0 {
			return key, state.target, operations
		}
	}
	return "", config.IcebergTarget{}, nil
}

func (m *tableMaintenanceMonitor) statusLocked(now time.Time) *connector.TableMaintenanceStatus {
	status := &connector.TableMaintenanceStatus{
		Enabled:                      true,
		State:                        "healthy",
		CatalogName:                  maintenanceCatalogName(m.cfg),
		RunnerResourceProfile:        m.cfg.TableMaintenance.RunnerResourceProfile,
		MaxConcurrentJobs:            m.cfg.TableMaintenance.MaxConcurrentJobs,
		DataFilesThreshold:           m.cfg.TableMaintenance.DataFilesThreshold,
		EqualityDeleteFilesThreshold: m.cfg.TableMaintenance.EqualityDeleteFilesThreshold,
		PositionDeleteFilesThreshold: m.cfg.TableMaintenance.PositionDeleteFilesThreshold,
		SmallFileSizeBytes:           m.cfg.TableMaintenance.SmallFileSizeBytes,
		SmallFilesMinCount:           m.cfg.TableMaintenance.SmallFilesMinCount,
		SmallFilesMinTotalBytes:      m.cfg.TableMaintenance.SmallFilesMinTotalBytes,
		TablesTotal:                  len(m.states),
		Paused:                       m.paused,
	}
	var latest time.Time
	keys := make([]string, 0, len(m.states))
	for key := range m.states {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		state := m.states[key]
		if state == nil {
			continue
		}
		tableState := "watching"
		if state.lastInventoryError != "" {
			tableState = "error"
			status.InventoryErrors++
		} else if state.active != nil {
			tableState = "running"
			status.ActiveRuns++
			status.TablesReady++
		} else if len(automaticOperationsDue(state, m.cfg.TableMaintenance, now)) > 0 {
			tableState = "ready"
			status.TablesReady++
		} else if state.eligibilityReady {
			tableState = "healthy"
		}
		if state.eligibilityReady {
			status.TablesScanned++
		}
		status.ActiveDataFiles += state.activeDataFiles
		status.ActiveEqualityDeleteFiles += state.activeEqDeletes
		status.ApplicableEqualityDeleteFiles += state.activeEqDeletes
		status.EligibleSmallFiles += state.activeSmallFiles
		status.EligibleSmallBytes += state.activeSmallBytes
		if state.lastCheckedAt.After(latest) {
			latest = state.lastCheckedAt
		}
		status.Tables = append(status.Tables, connector.TableMaintenanceTableStatus{
			Catalog:                       status.CatalogName,
			Namespace:                     state.target.Namespace,
			Table:                         state.target.Table,
			Identifier:                    canonicalMaintenanceTableKey(status.CatalogName, state.target.Namespace, state.target.Table),
			State:                         tableState,
			ActiveDataFiles:               state.activeDataFiles,
			ActiveEqualityDeleteFiles:     state.activeEqDeletes,
			ApplicableEqualityDeleteFiles: state.activeEqDeletes,
			EligibleSmallFiles:            state.activeSmallFiles,
			EligibleSmallBytes:            state.activeSmallBytes,
			NewDataFiles:                  state.newDataFiles,
			NewEqualityDeleteFiles:        state.newEqualityDeletes,
			CheckedAt:                     formatMaintenanceTime(state.lastCheckedAt),
			Error:                         state.lastInventoryError,
		})
	}
	switch {
	case status.ActiveRuns > 0:
		status.State = "running"
	case status.InventoryErrors > 0:
		status.State = "error"
	case status.TablesReady > 0:
		status.State = "ready"
	}
	if !latest.IsZero() {
		status.CheckedAt = latest.UTC().Format(time.RFC3339)
	}
	return status
}

func (m *tableMaintenanceMonitor) publishStatus() {
	if m == nil {
		return
	}
	m.mu.Lock()
	reporter := m.statusReporter
	status := m.statusLocked(time.Now().UTC())
	m.mu.Unlock()
	if reporter != nil {
		reporter(status)
	}
}

func formatMaintenanceTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}
