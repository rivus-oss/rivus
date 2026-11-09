package iceberg

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/util"
)

const (
	defaultNativeMaxInputBytes          = int64(512 * 1024 * 1024)
	defaultNativeMaxInputFiles          = 250
	defaultNativeMaxEqualityDeleteFiles = 100
	defaultNativeTargetBytes            = int64(128 * 1024 * 1024)
	defaultNativeSmallBytes             = int64(64 * 1024 * 1024)
	defaultNativeMinSmallFiles          = 10
	defaultNativeMinSmallBytes          = int64(256 * 1024 * 1024)
	defaultDataFilesThreshold           = 200
	defaultEqualityDeleteFilesThreshold = 50
	defaultPositionDeleteFilesThreshold = 25
	defaultNativeTimeout                = 10 * time.Minute
	defaultNativeOrphanAge              = 7 * 24 * time.Hour
	defaultNativeOrphanInterval         = 24 * time.Hour
	defaultNativeOrphanInactiveInterval = 7 * 24 * time.Hour
	orphanActivityWindow                = 7 * 24 * time.Hour
	sparkMediumInputBytes               = int64(512 * 1024 * 1024)
	sparkLargeInputBytes                = int64(2 * 1024 * 1024 * 1024)
	sparkXLargeInputBytes               = int64(8 * 1024 * 1024 * 1024)
	sparkMediumInputFiles               = 250
	sparkLargeInputFiles                = 1000
	sparkXLargeInputFiles               = 2000
	sparkMediumDeleteFiles              = 100
	sparkLargeDeleteFiles               = 400
	sparkXLargeDeleteFiles              = 800
	orphanHashBuckets                   = 256
)

type nativeMaintenanceSettings struct {
	Enabled                     bool
	Executor                    string
	DataFilesThreshold          int
	EqualityDeleteThreshold     int
	PositionDeleteThreshold     int
	MaxSelectedInputBytes       int64
	MaxSelectedFiles            int
	MaxEqualityDeleteFiles      int
	TargetFileSizeBytes         int64
	SmallFileSizeBytes          int64
	MinSmallFiles               int
	MinSmallBytes               int64
	ScanConcurrency             int
	Timeout                     time.Duration
	TempDirectory               string
	ExpireInterval              time.Duration
	SnapshotMaxAge              time.Duration
	SnapshotRetainLast          int
	OrphanInterval              time.Duration
	OrphanInactiveInterval      time.Duration
	OrphanMinAge                time.Duration
	OrphanDryRun                bool
	MetadataCleanupEnabled      bool
	MetadataPreviousVersionsMax int
	SparkPollInterval           time.Duration
	SparkTimeout                time.Duration
	IdleCompactionInterval      time.Duration
}

type nativeTaskOutcome struct {
	Result                     meta.IcebergMaintenanceResult
	Retryable                  bool
	FollowUpCompaction         bool
	FollowUpCompactionPriority int
}

type activeFileInventory struct {
	SnapshotID                int64
	LastWriteAt               *time.Time
	PartitionFields           []string
	DataFiles                 int
	SmallFiles                int
	SmallBytes                int64
	EqualityDeletes           int
	PositionDeletes           int
	ApplicableEqualityDeletes int
	ApplicablePositionDeletes int
	CompactableFiles          int
	CompactableBytes          int64
	CompactionGroups          int
}

type compactionWorkload struct {
	Groups              []icetable.CompactionTaskGroup
	SelectedDataFiles   int
	SelectedDeleteFiles int
	SelectedFiles       int
	SelectedBytes       int64
	EqualityDeletes     int
	PositionDeletes     int
	GroupCount          int
}

// compactionTriggers describes why a table is eligible for a rewrite. The
// counters come from the active files in its current Iceberg snapshot, not
// from historical commit counts. That distinction prevents an initial
// snapshot of healthy 128 MiB files from looking like a small-file problem.
type compactionTriggers struct {
	SmallFileCount bool
	SmallFileBytes bool
	EqualityDelete bool
	PositionDelete bool
}

func (t compactionTriggers) Any() bool {
	return t.SmallFileCount || t.SmallFileBytes || t.EqualityDelete || t.PositionDelete
}

func defaultNativeMaintenanceSettings() nativeMaintenanceSettings {
	orphanInterval := durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INTERVAL_SECONDS", defaultNativeOrphanInterval)
	return nativeMaintenanceSettings{
		Executor:                    maintenanceExecutorHybrid,
		DataFilesThreshold:          intEnv("RIVUS_MAINTENANCE_DATA_FILES_THRESHOLD", defaultDataFilesThreshold),
		EqualityDeleteThreshold:     intEnv("RIVUS_MAINTENANCE_EQUALITY_DELETE_FILES_THRESHOLD", defaultEqualityDeleteFilesThreshold),
		PositionDeleteThreshold:     intEnv("RIVUS_MAINTENANCE_POSITION_DELETE_FILES_THRESHOLD", defaultPositionDeleteFilesThreshold),
		MaxSelectedInputBytes:       int64Env("RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_INPUT_BYTES", defaultNativeMaxInputBytes),
		MaxSelectedFiles:            intEnv("RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_FILES", defaultNativeMaxInputFiles),
		MaxEqualityDeleteFiles:      intEnv("RIVUS_MAINTENANCE_NATIVE_MAX_EQUALITY_DELETE_FILES", defaultNativeMaxEqualityDeleteFiles),
		TargetFileSizeBytes:         int64Env("RIVUS_MAINTENANCE_NATIVE_TARGET_FILE_SIZE_BYTES", defaultNativeTargetBytes),
		SmallFileSizeBytes:          int64Env("RIVUS_MAINTENANCE_SMALL_FILE_SIZE_BYTES", defaultNativeSmallBytes),
		MinSmallFiles:               intEnv("RIVUS_MAINTENANCE_SMALL_FILES_MIN_COUNT", defaultNativeMinSmallFiles),
		MinSmallBytes:               int64Env("RIVUS_MAINTENANCE_SMALL_FILES_MIN_TOTAL_BYTES", defaultNativeMinSmallBytes),
		ScanConcurrency:             intEnv("RIVUS_MAINTENANCE_NATIVE_SCAN_CONCURRENCY", 1),
		Timeout:                     durationEnv("RIVUS_MAINTENANCE_NATIVE_TIMEOUT_SECONDS", defaultNativeTimeout),
		TempDirectory:               firstNonEmpty(strings.TrimSpace(os.Getenv("RIVUS_MAINTENANCE_WORKER_TEMP_DIRECTORY")), "/tmp/rivus-maintenance"),
		ExpireInterval:              durationEnv("RIVUS_MAINTENANCE_NATIVE_EXPIRE_INTERVAL_SECONDS", 24*time.Hour),
		SnapshotMaxAge:              time.Duration(float64Env("RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_MAX_AGE_HOURS", 168.0) * float64(time.Hour)),
		SnapshotRetainLast:          intEnv("RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_RETAIN_LAST", 10),
		OrphanInterval:              orphanInterval,
		OrphanInactiveInterval:      durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INACTIVE_INTERVAL_SECONDS", max(defaultNativeOrphanInactiveInterval, orphanInterval)),
		OrphanMinAge:                time.Duration(float64Env("RIVUS_MAINTENANCE_NATIVE_ORPHAN_MIN_AGE_HOURS", defaultNativeOrphanAge.Hours()) * float64(time.Hour)),
		OrphanDryRun:                boolEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_DRY_RUN", false),
		MetadataCleanupEnabled:      boolEnv("RIVUS_MAINTENANCE_METADATA_CLEANUP_ENABLED", false),
		MetadataPreviousVersionsMax: intEnv("RIVUS_MAINTENANCE_METADATA_PREVIOUS_VERSIONS_MAX", 10),
		SparkPollInterval:           durationEnv("RIVUS_MAINTENANCE_SPARK_POLL_INTERVAL_SECONDS", 5*time.Second),
		SparkTimeout:                durationEnv("RIVUS_MAINTENANCE_SPARK_TIMEOUT_SECONDS", 2*time.Hour),
		IdleCompactionInterval:      durationEnv("RIVUS_MAINTENANCE_NATIVE_IDLE_CHECK_INTERVAL_SECONDS", 7*24*time.Hour),
	}
}

func executeNativeMaintenanceTask(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobID string,
	jobCfg *config.JobConfig,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	settings nativeMaintenanceSettings,
) nativeTaskOutcome {
	started := time.Now()
	result := meta.IcebergMaintenanceResult{
		TaskID:    task.ID,
		TableKey:  state.TableKey,
		Operation: task.Operation,
		Engine:    "native",
		Status:    "failed",
		Attempt:   task.AttemptCount,
		CreatedAt: started.UTC(),
	}
	finish := func(out nativeTaskOutcome) nativeTaskOutcome {
		out.Result.DurationMillis = time.Since(started).Milliseconds()
		return out
	}

	if jobCfg == nil {
		result.Error = "owner job configuration is unavailable"
		return finish(nativeTaskOutcome{Result: result})
	}
	setupCtx, setupCancel := context.WithTimeout(ctx, settings.Timeout)
	iceCfg, err := icebergConfigForNativeWorker(jobCfg)
	if err != nil {
		setupCancel()
		result.Error = err.Error()
		return finish(nativeTaskOutcome{Result: result})
	}
	cat, err := newCatalogWithRetry(setupCtx, iceCfg)
	if err != nil {
		setupCancel()
		result.Error = err.Error()
		return finish(nativeTaskOutcome{Result: result, Retryable: true})
	}
	tbl, err := cat.LoadTable(setupCtx, namespaceIdentifier(state.Namespace, state.Table))
	if err != nil {
		if errorsIsNoSuchIcebergTable(err) {
			if storeErr := store.MarkInventoryMissing(setupCtx, state.TableKey); storeErr != nil {
				setupCancel()
				result.Error = fmt.Sprintf("mark missing table inventory: %v", storeErr)
				return finish(nativeTaskOutcome{Result: result, Retryable: true})
			}
			setupCancel()
			result.Status = "skipped"
			result.RoutingReason = "table does not exist yet"
			return finish(nativeTaskOutcome{Result: result})
		}
		setupCancel()
		result.Error = fmt.Sprintf("load table: %v", err)
		return finish(nativeTaskOutcome{Result: result, Retryable: true})
	}
	if task.Operation == "compact" {
		inventory, inventoryErr := scanActiveFileInventory(setupCtx, tbl, settings)
		if inventoryErr != nil {
			setupCancel()
			result.Error = fmt.Sprintf("scan active file inventory: %v", inventoryErr)
			return finish(nativeTaskOutcome{Result: result, Retryable: true})
		}
		if err := saveActiveFileInventory(setupCtx, store, state.TableKey, inventory); err != nil {
			setupCancel()
			result.Error = fmt.Sprintf("save active file inventory: %v", err)
			return finish(nativeTaskOutcome{Result: result, Retryable: true})
		}
		// The planning decision below must use the inventory just read, rather
		// than the state value that was loaded before this task was leased.
		state = stateWithActiveInventory(state, inventory)
	}
	outcome := executeAfterMaintenanceSetup(setupCancel, func() nativeTaskOutcome {
		switch task.Operation {
		case maintenanceOperationCompact:
			return executeHybridCompaction(ctx, jobID, jobCfg, iceCfg, state, task, settings)
		case maintenanceOperationExpire:
			taskCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
			defer cancel()
			return executeNativeSnapshotExpiration(taskCtx, tbl, result, settings)
		case maintenanceOperationOrphan:
			taskCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
			defer cancel()
			return executeBoundedOrphanCleanup(taskCtx, tbl, result, settings)
		default:
			result.Error = fmt.Sprintf("unsupported maintenance operation %q", task.Operation)
			return nativeTaskOutcome{Result: result}
		}
	})

	if task.Operation == "compact" && outcome.Result.Status == "succeeded" {
		refreshCtx, refreshCancel := context.WithTimeout(ctx, settings.Timeout)
		if refreshedTable, err := loadNativeMaintenanceTable(refreshCtx, iceCfg, state); err == nil {
			if inventory, inventoryErr := scanActiveFileInventory(refreshCtx, refreshedTable, settings); inventoryErr == nil {
				if saveErr := saveActiveFileInventory(refreshCtx, store, state.TableKey, inventory); saveErr != nil {
					addInventoryWarning(&outcome.Result, saveErr)
				} else {
					if followUp, priority := followUpCompactionForInventory(inventory, state, settings, time.Now().UTC()); followUp {
						outcome.FollowUpCompaction = true
						outcome.FollowUpCompactionPriority = priority
						if outcome.Result.Details == nil {
							outcome.Result.Details = map[string]any{}
						}
						outcome.Result.Details["follow_up_compaction"] = true
						outcome.Result.Details["follow_up_priority"] = outcome.FollowUpCompactionPriority
					}
				}
			} else {
				addInventoryWarning(&outcome.Result, inventoryErr)
			}
		} else {
			addInventoryWarning(&outcome.Result, err)
		}
		refreshCancel()
	}
	outcome.Result.DurationMillis = time.Since(started).Milliseconds()
	return outcome
}

// Compaction reloads the table after this setup context is released.
func executeAfterMaintenanceSetup(setupCancel context.CancelFunc, execute func() nativeTaskOutcome) nativeTaskOutcome {
	setupCancel()
	return execute()
}

func loadNativeMaintenanceTable(ctx context.Context, iceCfg config.IcebergConfig, state meta.IcebergMaintenanceState) (*icetable.Table, error) {
	cat, err := newCatalogWithRetry(ctx, iceCfg)
	if err != nil {
		return nil, fmt.Errorf("create catalog: %w", err)
	}
	tbl, err := cat.LoadTable(ctx, namespaceIdentifier(state.Namespace, state.Table))
	if err != nil {
		return nil, fmt.Errorf("load table: %w", err)
	}
	return tbl, nil
}

func refreshPendingInventory(ctx context.Context, store *meta.IcebergMaintenanceStore, jobCfg *config.JobConfig, state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) error {
	iceCfg, err := icebergConfigForNativeWorker(jobCfg)
	if err != nil {
		return err
	}
	cat, err := newCatalogWithRetry(ctx, iceCfg)
	if err != nil {
		return err
	}
	tbl, err := cat.LoadTable(ctx, namespaceIdentifier(state.Namespace, state.Table))
	if err != nil {
		if errorsIsNoSuchIcebergTable(err) {
			return store.MarkInventoryMissing(ctx, state.TableKey)
		}
		return fmt.Errorf("load table: %w", err)
	}
	tbl, err = applyMetadataCleanupPolicy(ctx, tbl, settings)
	if err != nil {
		return err
	}
	inventory, err := scanActiveFileInventory(ctx, tbl, settings)
	if err != nil {
		return err
	}
	if err := saveActiveFileInventory(ctx, store, state.TableKey, inventory); err != nil {
		return err
	}
	now := time.Now().UTC()
	if !state.SnapshotComplete || !proactiveCompactionDue(inventory, state, settings, now) {
		return nil
	}
	observed := stateWithActiveInventory(state, inventory)
	return store.ScheduleCompactionCheck(ctx, state.TableKey, now, compactionTaskPriority(observed, settings, now))
}

func scanActiveFileInventory(ctx context.Context, tbl *icetable.Table, settings nativeMaintenanceSettings) (activeFileInventory, error) {
	return retryInventoryDNS(ctx, func() (activeFileInventory, error) {
		return scanActiveFileInventoryOnce(ctx, tbl, settings)
	})
}

// A transient Docker DNS miss can happen while opening an S3 manifest. Retry
// the entire scan so no partially accumulated file counts are persisted.
func retryInventoryDNS(ctx context.Context, scan func() (activeFileInventory, error)) (activeFileInventory, error) {
	var inventory activeFileInventory
	err := util.RetryWithBackoff(ctx, config.RetryPolicy{
		MaxAttempts: 2,
		BaseBackoff: 250 * time.Millisecond,
		MaxBackoff:  250 * time.Millisecond,
	}, func() error {
		candidate, scanErr := scan()
		if scanErr == nil {
			inventory = candidate
			return nil
		}
		if !isDNSLookupFailure(scanErr) {
			return util.Permanent(scanErr)
		}
		return scanErr
	})
	return inventory, err
}

func isDNSLookupFailure(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "no such host") || strings.Contains(message, "temporary failure in name resolution")
}

func scanActiveFileInventoryOnce(ctx context.Context, tbl *icetable.Table, settings nativeMaintenanceSettings) (activeFileInventory, error) {
	var inventory activeFileInventory
	if tbl == nil {
		return inventory, nil
	}
	inventory.PartitionFields = maintenancePartitionFields(tbl)
	if tbl.CurrentSnapshot() == nil {
		return inventory, nil
	}
	inventory.SnapshotID = tbl.CurrentSnapshot().SnapshotID
	inventory.LastWriteAt = latestTableWriteAt(tbl.Metadata(), time.Now().UTC())
	fsys, err := tbl.FS(ctx)
	if err != nil {
		return inventory, fmt.Errorf("open table filesystem: %w", err)
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fsys)
	if err != nil {
		return inventory, fmt.Errorf("read current manifest list: %w", err)
	}
	dataTasks := make([]icetable.FileScanTask, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return inventory, err
		}
		for entry, entryErr := range manifest.Entries(fsys, true) {
			if entryErr != nil {
				return inventory, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), entryErr)
			}
			if entry == nil || entry.DataFile() == nil {
				continue
			}
			file := entry.DataFile()
			accumulateActiveFile(&inventory, file, settings.SmallFileSizeBytes)
			if file.ContentType() == iceberglib.EntryContentData {
				dataTasks = append(dataTasks, icetable.FileScanTask{File: file, Length: file.FileSizeBytes()})
			}
		}
	}
	if inventory.EqualityDeletes > 0 || inventory.PositionDeletes > 0 {
		// Manifest entries count every delete file retained by the snapshot.
		// Only the scan planner knows which unique deletes apply to live data.
		plannedTasks, planErr := tbl.Scan().PlanFiles(ctx)
		if planErr != nil {
			return inventory, fmt.Errorf("plan applicable delete files: %w", planErr)
		}
		inventoryApplicableDeletes(&inventory, plannedTasks)
		dataTasks = plannedTasks
	}
	if err := addCompactableInventory(&inventory, dataTasks, settings); err != nil {
		return inventory, err
	}
	return inventory, nil
}

// A non-nil empty list means the table is known to be unpartitioned. Nil is
// reserved for inventory that has not read the table metadata yet.
func maintenancePartitionFields(tbl *icetable.Table) []string {
	fields := make([]string, 0)
	spec := tbl.Spec()
	for _, partition := range spec.Fields() {
		if _, void := partition.Transform.(iceberglib.VoidTransform); void {
			continue
		}
		names := make([]string, 0, len(partition.SourceIDs))
		for _, id := range partition.SourceIDs {
			name, ok := tbl.Schema().FindColumnName(id)
			if !ok {
				names = nil
				break
			}
			names = append(names, name)
		}
		label := partition.Name
		if len(names) > 0 {
			label = strings.Join(names, ", ")
			if _, identity := partition.Transform.(iceberglib.IdentityTransform); !identity {
				label = fmt.Sprintf("%s(%s)", partition.Transform, label)
			}
		}
		fields = append(fields, label)
	}
	return fields
}

func inventoryApplicableDeletes(inventory *activeFileInventory, tasks []icetable.FileScanTask) {
	seenEquality := make(map[string]struct{})
	seenPosition := make(map[string]struct{})
	for _, task := range tasks {
		for _, file := range task.EqualityDeleteFiles {
			if file != nil {
				seenEquality[file.FilePath()] = struct{}{}
			}
		}
		for _, file := range task.DeleteFiles {
			if file != nil {
				seenPosition[file.FilePath()] = struct{}{}
			}
		}
	}
	inventory.ApplicableEqualityDeletes = len(seenEquality)
	inventory.ApplicablePositionDeletes = len(seenPosition)
}

// addCompactableInventory records only files that the real Iceberg planner can
// rewrite together. Global small-file counts are not sufficient for
// partitioned tables: ten files in ten different partitions form zero legal
// compaction groups.
func addCompactableInventory(inventory *activeFileInventory, tasks []icetable.FileScanTask, settings nativeMaintenanceSettings) error {
	if inventory == nil || len(tasks) == 0 {
		return nil
	}
	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = settings.TargetFileSizeBytes
	cfg.MinFileSizeBytes = settings.SmallFileSizeBytes
	cfg.MaxFileSizeBytes = maxInt64(settings.TargetFileSizeBytes*9/5, settings.SmallFileSizeBytes+1)
	cfg.MinInputFiles = uint(settings.MinSmallFiles)
	cfg.DeleteFileThreshold = 1
	tasks, err := wholeFileCompactionTasks(tasks)
	if err != nil {
		return err
	}
	plan, err := cfg.PlanCompaction(tasks)
	if err != nil {
		return fmt.Errorf("plan compactable inventory: %w", err)
	}
	for _, group := range plan.Groups {
		inventory.CompactionGroups++
		inventory.CompactableFiles += len(group.Tasks)
		inventory.CompactableBytes += group.TotalSizeBytes
	}
	return nil
}

func accumulateActiveFile(inventory *activeFileInventory, file iceberglib.DataFile, smallFileSizeBytes int64) {
	if inventory == nil || file == nil {
		return
	}
	switch file.ContentType() {
	case iceberglib.EntryContentData:
		inventory.DataFiles++
		if smallFileSizeBytes > 0 && file.FileSizeBytes() < smallFileSizeBytes {
			inventory.SmallFiles++
			inventory.SmallBytes += file.FileSizeBytes()
		}
	case iceberglib.EntryContentEqDeletes:
		inventory.EqualityDeletes++
	case iceberglib.EntryContentPosDeletes:
		inventory.PositionDeletes++
	}
}

// latestTableWriteAt follows the current snapshot's ancestry, so writes to an
// unpublished branch do not mark the main table active. Use commit timestamps,
// not scan time: repeatedly observing the same snapshot must not keep an idle
// table active. Replace snapshots rewrite files without changing table data.
func latestTableWriteAt(metadata icetable.Metadata, now time.Time) *time.Time {
	seen := make(map[int64]struct{})
	snapshot := metadata.CurrentSnapshot()
	for snapshot != nil {
		if _, visited := seen[snapshot.SnapshotID]; visited {
			break
		}
		seen[snapshot.SnapshotID] = struct{}{}
		if snapshot.Summary != nil && snapshot.Summary.Properties["rivus.maintenance"] == "" {
			switch snapshot.Summary.Operation {
			case icetable.OpAppend, icetable.OpOverwrite, icetable.OpDelete:
				written := time.UnixMilli(snapshot.TimestampMs).UTC()
				if snapshot.TimestampMs > 0 && !written.After(now) {
					return &written
				}
			}
		}
		if snapshot.ParentSnapshotID == nil {
			break
		}
		snapshot = metadata.SnapshotByID(*snapshot.ParentSnapshotID)
	}
	return nil
}

func saveActiveFileInventory(ctx context.Context, store *meta.IcebergMaintenanceStore, tableKey string, inventory activeFileInventory) error {
	if err := store.UpdateInventory(ctx, tableKey, inventory.SnapshotID, inventory.DataFiles, inventory.SmallFiles,
		inventory.SmallBytes, inventory.EqualityDeletes, inventory.PositionDeletes,
		inventory.ApplicableEqualityDeletes, inventory.ApplicablePositionDeletes,
		inventory.CompactableFiles, inventory.CompactableBytes, inventory.CompactionGroups, inventory.PartitionFields); err != nil {
		return err
	}
	return store.RecordTableWrite(ctx, tableKey, inventory.LastWriteAt)
}

func inventoryTriggersCompaction(inventory activeFileInventory, settings nativeMaintenanceSettings) bool {
	return compactionTriggersFor(meta.IcebergMaintenanceState{
		ActiveSmallFiles:              inventory.SmallFiles,
		ActiveSmallBytes:              inventory.SmallBytes,
		ActiveEqualityDeleteFiles:     inventory.EqualityDeletes,
		ActivePositionDeleteFiles:     inventory.PositionDeletes,
		ApplicableEqualityDeleteFiles: &inventory.ApplicableEqualityDeletes,
		ApplicablePositionDeleteFiles: &inventory.ApplicablePositionDeletes,
		ActiveCompactableFiles:        inventory.CompactableFiles,
		ActiveCompactableBytes:        inventory.CompactableBytes,
		ActiveCompactionGroups:        inventory.CompactionGroups,
	}, settings).Any()
}

func compactionTriggersFor(state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) compactionTriggers {
	return compactionTriggers{
		SmallFileCount: settings.DataFilesThreshold > 0 && state.ActiveSmallFiles >= settings.DataFilesThreshold && state.ActiveCompactionGroups > 0,
		SmallFileBytes: settings.MinSmallFiles > 0 && settings.MinSmallBytes > 0 &&
			state.ActiveCompactableFiles >= settings.MinSmallFiles && state.ActiveCompactableBytes >= settings.MinSmallBytes,
		EqualityDelete: settings.EqualityDeleteThreshold > 0 && state.EffectiveEqualityDeleteFiles() >= settings.EqualityDeleteThreshold,
		// Trino snapshot replacement intentionally leaves a small number of
		// position-delete files while it replaces the recent history window.
		// Make the table eligible only after the per-table threshold is reached;
		// engine routing is decided separately from the selected workload size.
		PositionDelete: settings.PositionDeleteThreshold > 0 && state.EffectivePositionDeleteFiles() >= settings.PositionDeleteThreshold,
	}
}

func addInventoryWarning(result *meta.IcebergMaintenanceResult, err error) {
	if result == nil || err == nil {
		return
	}
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	result.Details["inventory_refresh_warning"] = err.Error()
}

func icebergConfigForNativeWorker(jobCfg *config.JobConfig) (config.IcebergConfig, error) {
	if jobCfg == nil {
		return config.IcebergConfig{}, fmt.Errorf("job config is nil")
	}
	sinkType, sinkCfg := jobSinkSpec(jobCfg)
	if !strings.EqualFold(sinkType, "iceberg_native") {
		return config.IcebergConfig{}, fmt.Errorf("job sink is %q, not iceberg_native", sinkType)
	}
	return decodeIcebergConfig(sinkCfg)
}

func executeHybridCompaction(
	ctx context.Context,
	jobID string,
	jobCfg *config.JobConfig,
	iceCfg config.IcebergConfig,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	settings nativeMaintenanceSettings,
) nativeTaskOutcome {
	nativeCtx, nativeCancel := context.WithTimeout(ctx, settings.Timeout)
	defer nativeCancel()
	result := meta.IcebergMaintenanceResult{
		TaskID:    task.ID,
		TableKey:  state.TableKey,
		Operation: task.Operation,
		Engine:    "native",
		Status:    "failed",
		Attempt:   task.AttemptCount,
		CreatedAt: time.Now().UTC(),
	}
	tbl, err := loadNativeMaintenanceTable(nativeCtx, iceCfg, state)
	if err != nil {
		if errorsIsNoSuchIcebergTable(err) {
			result.Status = "skipped"
			result.RoutingReason = "table does not exist yet"
			return nativeTaskOutcome{Result: result}
		}
		result.Error = fmt.Sprintf("load table for compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if tbl.CurrentSnapshot() == nil {
		result.Status = "skipped"
		result.RoutingReason = "table has no current snapshot"
		return nativeTaskOutcome{Result: result}
	}
	startSnapshotID := tbl.CurrentSnapshot().SnapshotID

	triggers := compactionTriggersFor(state, settings)
	projectedGrowthTrigger := !triggers.Any() && proactiveCompactionStateDue(state, settings, time.Now().UTC())
	cfg := compaction.DefaultConfig()
	cfg.TargetFileSizeBytes = settings.TargetFileSizeBytes
	cfg.MinFileSizeBytes = settings.SmallFileSizeBytes
	cfg.MaxFileSizeBytes = maxInt64(settings.TargetFileSizeBytes*9/5, settings.SmallFileSizeBytes+1)
	cfg.MinInputFiles = uint(settings.MinSmallFiles)
	// Delete-driven maintenance must be able to rewrite a single affected data
	// file. Ordinary small-file compaction keeps the higher minimum so one
	// small file is never rewritten in a loop.
	if triggers.EqualityDelete || triggers.PositionDelete {
		cfg.MinInputFiles = 1
	}
	cfg.DeleteFileThreshold = 1
	cfg.PreserveDeadEqualityDeletes = false

	plan, err := analyzeWholeFileCompaction(nativeCtx, tbl, cfg)
	if err != nil {
		result.Error = fmt.Sprintf("analyze compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	work := buildCompactionWorkload(plan)
	result.InputFiles = work.SelectedFiles
	result.InputBytes = work.SelectedBytes
	result.DeleteFiles = work.EqualityDeletes + work.PositionDeletes
	result.Details = map[string]any{
		"executor":                         settings.Executor,
		"groups":                           work.GroupCount,
		"selected_data_files":              work.SelectedDataFiles,
		"selected_delete_files":            work.SelectedDeleteFiles,
		"equality_delete_files":            work.EqualityDeletes,
		"position_delete_files":            work.PositionDeletes,
		"active_equality_delete_files":     state.ActiveEqualityDeleteFiles,
		"active_position_delete_files":     state.ActivePositionDeleteFiles,
		"applicable_equality_delete_files": state.EffectiveEqualityDeleteFiles(),
		"applicable_position_delete_files": state.EffectivePositionDeleteFiles(),
		"trigger_small_file_count":         triggers.SmallFileCount,
		"trigger_small_file_bytes":         triggers.SmallFileBytes,
		"trigger_equality_delete_files":    triggers.EqualityDelete,
		"trigger_position_delete_files":    triggers.PositionDelete,
		"trigger_projected_growth":         projectedGrowthTrigger,
		"queue_priority":                   task.Priority,
		"maintenance_pressure":             compactionPressure(state, settings),
		"starting_snapshot_id":             startSnapshotID,
		"estimated_output_files":           plan.EstOutputFiles,
		"estimated_output_bytes":           plan.EstOutputBytes,
		"max_native_input_bytes":           settings.MaxSelectedInputBytes,
		"max_native_input_files":           settings.MaxSelectedFiles,
		"max_native_equality_delete_files": settings.MaxEqualityDeleteFiles,
	}
	if deleteOnlyCompactionHasNoApplicableDeletes(triggers, work) {
		result.Status = "skipped"
		result.RoutingReason = "delete threshold reached, but planner found no applicable delete files"
		return nativeTaskOutcome{Result: result}
	}

	// The safety route is deliberately evaluated before the normal byte-based
	// eligibility check. A table with 5,000 delete files must go to Spark even
	// when those files are small in total.
	routeSpark, reason := shouldRouteCompactionToSpark(work, state, settings)
	eligible := triggers.Any() || projectedGrowthTrigger
	coordinateAfterConflict := shouldCoordinateSparkCompaction(task)
	if eligible && settings.Executor != maintenanceExecutorNative && nativeParquetLimitError(task.LastError) {
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = "previous native rewrite exceeded Parquet reader page-size limit"
		out := executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
		if out.Result.Error != "" {
			out.Result.Error = "native Parquet reader page-size limit; " + out.Result.Error
		}
		return out
	}
	if eligible && coordinateAfterConflict && settings.Executor != maintenanceExecutorNative {
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = "repeated Iceberg commit conflict; escalating to coordinated Spark compaction"
		return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
	}
	if work.SelectedDataFiles == 0 {
		// The native planner can have no usable group even when a table has a
		// dangerous delete-file count. Let Spark make its own plan in hybrid
		// mode for that case; native mode remains bounded and reports a skip.
		if eligible && routeSpark {
			if settings.Executor == maintenanceExecutorNative {
				result.Status = "skipped"
				result.RoutingReason = "native safety limit reached; enable hybrid executor for Spark: " + reason
				return nativeTaskOutcome{Result: result}
			}
			nativeCancel()
			result.Engine = "spark"
			if settings.Executor == maintenanceExecutorSpark {
				result.RoutingReason = "executor=spark selected Spark compaction"
			} else {
				result.RoutingReason = reason
			}
			return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
		}
		result.Status = "skipped"
		result.RoutingReason = "compaction planner found no eligible data-file group"
		return nativeTaskOutcome{Result: result}
	}
	if !eligible {
		result.Status = "skipped"
		result.RoutingReason = "active inventory has not reached a compaction trigger"
		return nativeTaskOutcome{Result: result}
	}

	if settings.Executor == maintenanceExecutorSpark {
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = "executor=spark selected Spark compaction"
		return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
	}
	if routeSpark {
		if settings.Executor == maintenanceExecutorNative {
			result.Status = "skipped"
			result.RoutingReason = "native safety limit reached; enable hybrid executor for Spark: " + reason
			return nativeTaskOutcome{Result: result}
		}
		nativeCancel()
		result.Engine = "spark"
		result.RoutingReason = reason
		return executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
	}

	if err := tbl.Refresh(nativeCtx); err != nil {
		result.Error = fmt.Sprintf("refresh before compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if tbl.CurrentSnapshot() == nil || tbl.CurrentSnapshot().SnapshotID != startSnapshotID {
		result.Error = "current snapshot changed after compaction analysis; CDC wins and maintenance will retry"
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	rewrittenPaths := make(map[string]struct{}, work.SelectedDataFiles)
	for _, group := range work.Groups {
		for _, scanTask := range group.Tasks {
			if scanTask.File != nil {
				rewrittenPaths[scanTask.File.FilePath()] = struct{}{}
			}
		}
	}
	fs, err := tbl.FS(nativeCtx)
	if err != nil {
		result.Error = fmt.Sprintf("get table filesystem: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	deadEqDeletes, err := compaction.CollectDeadEqualityDeletes(nativeCtx, fs, tbl.CurrentSnapshot(), rewrittenPaths)
	if err != nil {
		result.Error = fmt.Sprintf("collect dead equality deletes: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	tx := tbl.NewTransaction()
	rewriteResult, err := tx.RewriteDataFiles(nativeCtx, work.Groups, icetable.RewriteDataFilesOptions{
		PartialProgress:          false,
		ExtraDeleteFilesToRemove: deadEqDeletes,
		SnapshotProps: iceberglib.Properties{
			"rivus.maintenance":         "native-compaction",
			"rivus.maintenance.job_id":  jobID,
			"rivus.maintenance.task_id": fmt.Sprintf("%d", task.ID),
			"rivus.maintenance.worker":  "maintenance-worker",
		},
		GroupOptions: []icetable.CompactionGroupOption{
			icetable.WithCompactionTargetFileSize(settings.TargetFileSizeBytes),
			icetable.WithCompactionScanConcurrency(settings.ScanConcurrency),
		},
	})
	if err != nil {
		if settings.Executor == maintenanceExecutorHybrid && nativeParquetLimitError(err.Error()) {
			nativeCancel()
			result.Engine = "spark"
			result.RoutingReason = "native Parquet reader page-size limit; delegating to Spark"
			result.Details["native_fallback_error"] = err.Error()
			out := executeSparkCompactionFallback(ctx, jobID, jobCfg, iceCfg, state, task, result, settings, work, hasDeleteWork(work, state))
			if out.Result.Error != "" {
				out.Result.Error = fmt.Sprintf("native rewrite: %v; %s", err, out.Result.Error)
			}
			return out
		}
		result.Error = fmt.Sprintf("rewrite data files: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if rewriteResult.RewrittenGroups == 0 {
		result.Status = "skipped"
		result.RoutingReason = "compaction planner produced no rewrite output"
		return nativeTaskOutcome{Result: result}
	}
	committed, err := tx.Commit(nativeCtx)
	if err != nil {
		result.Error = fmt.Sprintf("commit native compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: errors.Is(err, icetable.ErrCommitFailed) || nativeCtx.Err() != nil}
	}
	result.Status = "succeeded"
	if result.RoutingReason == "" {
		if settings.Executor == maintenanceExecutorNative {
			result.RoutingReason = "executor=native selected native compaction"
		} else {
			result.RoutingReason = "hybrid selected native: workload is within native file and byte limits"
		}
	}
	result.OutputFiles = rewriteResult.AddedDataFiles
	result.OutputBytes = rewriteResult.BytesAfter
	result.Details["rewritten_groups"] = rewriteResult.RewrittenGroups
	result.Details["removed_data_files"] = rewriteResult.RemovedDataFiles
	result.Details["removed_equality_delete_files"] = rewriteResult.RemovedEqualityDeleteFiles
	result.Details["removed_position_delete_files"] = rewriteResult.RemovedPositionDeleteFiles
	if committed != nil && committed.CurrentSnapshot() != nil {
		result.Details["committed_snapshot_id"] = committed.CurrentSnapshot().SnapshotID
	}
	return nativeTaskOutcome{Result: result}
}

func nativeParquetLimitError(message string) bool {
	message = strings.ToLower(message)
	if strings.Contains(message, "native parquet reader page-size limit") {
		return true
	}
	return strings.Contains(message, "parquet:") && strings.Contains(message, "exceeds configured limit") &&
		(strings.Contains(message, "compressed page size") || strings.Contains(message, "uncompressed page size"))
}

func analyzeWholeFileCompaction(ctx context.Context, tbl *icetable.Table, cfg compaction.Config) (compaction.Plan, error) {
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return compaction.Plan{}, err
	}
	tasks, err = wholeFileCompactionTasks(tasks)
	if err != nil {
		return compaction.Plan{}, err
	}
	return cfg.PlanCompaction(tasks)
}

// Scan planning can return multiple row-group ranges for one file. A rewrite
// replaces whole data files: passing ranges separately can duplicate removals,
// inflate workload counts, or split a file's output across different commits.
// This helper is only for unfiltered, whole-table maintenance planning.
func wholeFileCompactionTasks(tasks []icetable.FileScanTask) ([]icetable.FileScanTask, error) {
	result := make([]icetable.FileScanTask, 0, len(tasks))
	seen := make(map[string]int, len(tasks))
	for _, task := range tasks {
		if task.File == nil || task.File.FilePath() == "" {
			return nil, fmt.Errorf("compaction task has no data file path")
		}
		if task.Residual != nil && !task.Residual.Equals(iceberglib.AlwaysTrue{}) {
			return nil, fmt.Errorf("whole-file compaction requires unfiltered scan tasks")
		}
		task.Start, task.Length = 0, task.File.FileSizeBytes()
		path := task.File.FilePath()
		if index, ok := seen[path]; ok {
			if !reflect.DeepEqual(result[index], task) {
				return nil, fmt.Errorf("inconsistent compaction scan tasks for data file %q", path)
			}
			continue
		}
		seen[path] = len(result)
		result = append(result, task)
	}
	return result, nil
}

// A table-wide delete count can remain high even when none of those delete
// files apply to the data files selected by the planner. Rewriting a single
// unrelated data file would make no progress and immediately trigger another
// compaction from the unchanged inventory.
func deleteOnlyCompactionHasNoApplicableDeletes(triggers compactionTriggers, work compactionWorkload) bool {
	return (triggers.EqualityDelete || triggers.PositionDelete) &&
		!triggers.SmallFileCount && !triggers.SmallFileBytes &&
		work.SelectedDeleteFiles == 0
}

func buildCompactionWorkload(plan compaction.Plan) compactionWorkload {
	work := compactionWorkload{GroupCount: len(plan.Groups)}
	work.Groups = make([]icetable.CompactionTaskGroup, 0, len(plan.Groups))
	seenEq := make(map[string]int64)
	seenPos := make(map[string]int64)
	for _, group := range plan.Groups {
		converted := icetable.CompactionTaskGroup{
			PartitionKey:   group.PartitionKey,
			Tasks:          group.Tasks,
			TotalSizeBytes: group.TotalSizeBytes,
		}
		work.Groups = append(work.Groups, converted)
		work.SelectedDataFiles += len(group.Tasks)
		work.SelectedBytes += group.TotalSizeBytes
		for _, task := range group.Tasks {
			for _, deleteFile := range task.EqualityDeleteFiles {
				if deleteFile != nil {
					if _, seen := seenEq[deleteFile.FilePath()]; !seen {
						seenEq[deleteFile.FilePath()] = deleteFile.FileSizeBytes()
					}
				}
			}
			for _, deleteFile := range task.DeleteFiles {
				if deleteFile != nil {
					if _, seen := seenPos[deleteFile.FilePath()]; !seen {
						seenPos[deleteFile.FilePath()] = deleteFile.FileSizeBytes()
					}
				}
			}
		}
	}
	work.EqualityDeletes = len(seenEq)
	work.PositionDeletes = len(seenPos)
	work.SelectedDeleteFiles = work.EqualityDeletes + work.PositionDeletes
	work.SelectedFiles = work.SelectedDataFiles + work.SelectedDeleteFiles
	for _, size := range seenEq {
		work.SelectedBytes += size
	}
	for _, size := range seenPos {
		work.SelectedBytes += size
	}
	return work
}

func shouldRouteCompactionToSpark(work compactionWorkload, state meta.IcebergMaintenanceState, settings nativeMaintenanceSettings) (bool, string) {
	equalityDeletes := maxInt(work.EqualityDeletes, state.EffectiveEqualityDeleteFiles())
	switch {
	case equalityDeletes > settings.MaxEqualityDeleteFiles:
		return true, fmt.Sprintf("equality-delete files %d exceed native limit %d", equalityDeletes, settings.MaxEqualityDeleteFiles)
	case work.SelectedBytes > settings.MaxSelectedInputBytes:
		return true, fmt.Sprintf("selected input %d bytes exceeds native limit %d", work.SelectedBytes, settings.MaxSelectedInputBytes)
	case work.SelectedFiles > settings.MaxSelectedFiles:
		return true, fmt.Sprintf("selected input %d files exceeds native limit %d", work.SelectedFiles, settings.MaxSelectedFiles)
	default:
		return false, ""
	}
}

func hasDeleteWork(work compactionWorkload, state meta.IcebergMaintenanceState) bool {
	return work.EqualityDeletes > 0 || work.PositionDeletes > 0 ||
		state.EffectiveEqualityDeleteFiles() > 0 || state.EffectivePositionDeleteFiles() > 0
}

func executeSparkCompactionFallback(
	ctx context.Context,
	jobID string,
	jobCfg *config.JobConfig,
	iceCfg config.IcebergConfig,
	state meta.IcebergMaintenanceState,
	task meta.IcebergMaintenanceTask,
	result meta.IcebergMaintenanceResult,
	settings nativeMaintenanceSettings,
	work compactionWorkload,
	deleteTriggered bool,
) nativeTaskOutcome {
	pauseRivusWriters := shouldCoordinateSparkCompaction(task)
	minInputFiles := settings.MinSmallFiles
	if deleteTriggered {
		minInputFiles = 1
	}
	compactOptions := normalizeAutomaticCompactOptions(iceCfg.TableMaintenance.CompactOptions)
	options := stringAnyMap(compactOptions["options"])
	options["target-file-size-bytes"] = fmt.Sprintf("%d", settings.TargetFileSizeBytes)
	options["min-input-files"] = fmt.Sprintf("%d", minInputFiles)
	if deleteTriggered {
		options["delete-file-threshold"] = "1"
	}
	compactOptions["options"] = options
	resourceProfile, sizingReason := dynamicSparkResourceProfile(iceCfg.TableMaintenance.RunnerResourceProfile, work, task)
	if result.Details == nil {
		result.Details = map[string]any{}
	}
	result.Details["spark_resource_profile"] = resourceProfile
	result.Details["spark_resource_sizing"] = sizingReason
	result.Details["spark_writer_coordination"] = pauseRivusWriters
	request := TableMaintenanceRequest{
		Tables: []string{tableKey(state.Namespace, state.Table)},
		// Keep retries idempotent within one durable attempt while allowing a
		// later, right-sized attempt to create a new runner job.
		ExternalRunKey:    fmt.Sprintf("rivus-maintenance:%d:%d", task.ID, task.AttemptCount),
		ResourceProfile:   resourceProfile,
		PauseRivusWriters: &pauseRivusWriters,
		Operations: []TableMaintenanceOperation{{
			Type:    "rewrite_data_files",
			Options: compactOptions,
		}},
	}
	submission, err := SubmitTableMaintenanceForJobConfig(ctx, jobID, jobCfg, request, false)
	if err != nil {
		result.Error = fmt.Sprintf("submit Spark compaction: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	result.SubmissionID = submission.SubmissionID
	pollEvery := settings.SparkPollInterval
	if pollEvery <= 0 {
		pollEvery = 5 * time.Second
	}
	deadline := time.NewTimer(settings.SparkTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			result.Error = ctx.Err().Error()
			return nativeTaskOutcome{Result: result, Retryable: true}
		case <-deadline.C:
			result.Error = "Spark compaction exceeded maintenance timeout"
			return nativeTaskOutcome{Result: result, Retryable: true}
		case <-ticker.C:
			status, err := GetTableMaintenanceStatusForJobConfig(ctx, jobCfg, submission.SubmissionID)
			if err != nil {
				result.Error = fmt.Sprintf("poll Spark compaction: %v", err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			switch strings.ToUpper(strings.TrimSpace(status.DriverState)) {
			case "FINISHED", "SUCCESS", "SUCCEEDED":
				result.Status = "succeeded"
				return nativeTaskOutcome{Result: result}
			case "FAILED", "ERROR", "KILLED", "CANCELLED", "CANCELED":
				message := firstNonEmpty(strings.TrimSpace(status.Message), "Spark compaction failed")
				result.Error = fmt.Sprintf("Spark compaction failed with resource profile %s: %s", resourceProfile, message)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
		}
	}
}

func shouldCoordinateSparkCompaction(task meta.IcebergMaintenanceTask) bool {
	if task.AttemptCount < 3 {
		return false
	}
	errorText := strings.ToLower(strings.TrimSpace(task.LastError))
	return strings.Contains(errorText, "commitfailedexception") ||
		strings.Contains(errorText, "branch main has changed") ||
		(strings.Contains(errorText, "requirement failed") && strings.Contains(errorText, "branch"))
}

func dynamicSparkResourceProfile(configured string, work compactionWorkload, task meta.IcebergMaintenanceTask) (string, string) {
	profile := normalizeSparkResourceProfile(configured)
	reason := "configured minimum"
	deleteFiles := work.EqualityDeletes + work.PositionDeletes

	needed := "tiny"
	workloadReason := ""
	switch {
	case work.SelectedBytes > sparkXLargeInputBytes || work.SelectedFiles > sparkXLargeInputFiles || deleteFiles > sparkXLargeDeleteFiles:
		needed = "xlarge"
		workloadReason = "xlarge selected rewrite workload"
	case work.SelectedBytes > sparkLargeInputBytes || work.SelectedFiles > sparkLargeInputFiles || deleteFiles > sparkLargeDeleteFiles:
		needed = "large"
		workloadReason = "large selected rewrite workload"
	case work.SelectedBytes > sparkMediumInputBytes || work.SelectedFiles > sparkMediumInputFiles || deleteFiles > sparkMediumDeleteFiles:
		needed = "medium"
		workloadReason = "medium selected rewrite workload"
	}
	if sparkResourceProfileRank(needed) > sparkResourceProfileRank(profile) {
		profile = needed
		reason = workloadReason
	}

	if isSparkHeapError(task.LastError) && profile != "xlarge" {
		steps := task.AttemptCount - 1
		if steps < 1 {
			steps = 1
		}
		for i := 0; i < steps && profile != "xlarge"; i++ {
			profile = nextSparkResourceProfile(profile)
		}
		reason = "previous Spark attempt exhausted executor heap"
	}
	return profile, reason
}

func normalizeSparkResourceProfile(profile string) string {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "tiny", "small", "medium", "large", "xlarge":
		return strings.ToLower(strings.TrimSpace(profile))
	default:
		return "small"
	}
}

func sparkResourceProfileRank(profile string) int {
	switch normalizeSparkResourceProfile(profile) {
	case "tiny":
		return 0
	case "medium":
		return 2
	case "large":
		return 3
	case "xlarge":
		return 4
	default:
		return 1
	}
}

func nextSparkResourceProfile(profile string) string {
	switch normalizeSparkResourceProfile(profile) {
	case "tiny":
		return "small"
	case "small":
		return "medium"
	case "medium":
		return "large"
	default:
		return "xlarge"
	}
}

func isSparkHeapError(message string) bool {
	message = strings.ToLower(message)
	return strings.Contains(message, "outofmemoryerror") ||
		strings.Contains(message, "java heap space") ||
		strings.Contains(message, "gc overhead limit exceeded")
}

func executeNativeSnapshotExpiration(ctx context.Context, tbl *icetable.Table, result meta.IcebergMaintenanceResult, settings nativeMaintenanceSettings) nativeTaskOutcome {
	before := append([]icetable.Snapshot(nil), tbl.Metadata().Snapshots()...)
	if len(before) == 0 {
		result.Status = "skipped"
		result.RoutingReason = "table has no snapshots"
		return nativeTaskOutcome{Result: result}
	}
	staging := tbl.NewTransaction()
	if err := staging.ExpireSnapshots(
		icetable.WithOlderThan(settings.SnapshotMaxAge),
		icetable.WithRetainLast(settings.SnapshotRetainLast),
		icetable.WithPostCommit(false),
	); err != nil {
		result.Error = fmt.Sprintf("stage snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	staged, err := staging.StagedTable()
	if err != nil {
		result.Error = fmt.Sprintf("inspect staged snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	remaining := make(map[int64]struct{}, len(staged.Metadata().Snapshots()))
	for _, snapshot := range staged.Metadata().Snapshots() {
		remaining[snapshot.SnapshotID] = struct{}{}
	}
	expired := 0
	for _, snapshot := range before {
		if _, ok := remaining[snapshot.SnapshotID]; !ok {
			expired++
		}
	}
	result.ExpiredSnapshots = expired
	if expired == 0 {
		result.Status = "skipped"
		result.RoutingReason = "no snapshots are eligible after retain-last and reference protection"
		return nativeTaskOutcome{Result: result}
	}

	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(
		icetable.WithOlderThan(settings.SnapshotMaxAge),
		icetable.WithRetainLast(settings.SnapshotRetainLast),
	); err != nil {
		result.Error = fmt.Sprintf("prepare snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if _, err := tx.Commit(ctx); err != nil {
		result.Error = fmt.Sprintf("commit snapshot expiration: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: errors.Is(err, icetable.ErrCommitFailed) || ctx.Err() != nil}
	}
	result.Status = "succeeded"
	result.RoutingReason = "native snapshot expiration"
	return nativeTaskOutcome{Result: result}
}

type orphanDiskRecord struct {
	Path string `json:"path"`
	Size int64  `json:"size,omitempty"`
}

type orphanReferenceIndexStats struct {
	Snapshots          int
	ManifestLists      int
	UniqueManifests    int
	DuplicateManifests int
	DataFileReferences int64
}

// orphanBucketWriter amortizes filesystem work across the full scan. The old
// path opened, appended to, and closed a bucket file for every referenced data
// file, turning a large table into millions of local open/close operations.
// Writers are opened lazily, so small tables do not consume all 256 descriptors.
type orphanBucketWriter struct {
	tempDir string
	kind    string
	files   [orphanHashBuckets]*os.File
	writers [orphanHashBuckets]*bufio.Writer
	closed  bool
}

func newOrphanBucketWriter(tempDir, kind string) *orphanBucketWriter {
	return &orphanBucketWriter{tempDir: tempDir, kind: kind}
}

func (w *orphanBucketWriter) Append(record orphanDiskRecord) error {
	if w == nil || w.closed {
		return fmt.Errorf("orphan bucket writer is closed")
	}
	bucket := int(sha256.Sum256([]byte(record.Path))[0])
	if w.writers[bucket] == nil {
		f, err := os.OpenFile(bucketFile(w.tempDir, w.kind, bucket), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		w.files[bucket] = f
		w.writers[bucket] = bufio.NewWriterSize(f, 32*1024)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := w.writers[bucket].Write(encoded); err != nil {
		return err
	}
	return w.writers[bucket].WriteByte('\n')
}

func (w *orphanBucketWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	var out error
	for bucket := 0; bucket < orphanHashBuckets; bucket++ {
		if w.writers[bucket] != nil {
			out = errors.Join(out, w.writers[bucket].Flush())
		}
		if w.files[bucket] != nil {
			out = errors.Join(out, w.files[bucket].Close())
		}
	}
	return out
}

func executeBoundedOrphanCleanup(ctx context.Context, tbl *icetable.Table, result meta.IcebergMaintenanceResult, settings nativeMaintenanceSettings) nativeTaskOutcome {
	if settings.OrphanMinAge < defaultNativeOrphanAge {
		result.Error = fmt.Sprintf("orphan minimum age %s is below mandatory safety floor %s", settings.OrphanMinAge, defaultNativeOrphanAge)
		return nativeTaskOutcome{Result: result}
	}
	fsys, err := tbl.FS(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("get table filesystem: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	listable, ok := fsys.(iceio.ListableIO)
	if !ok {
		result.Error = "table filesystem does not implement streaming ListableIO; refusing unbounded orphan scan"
		return nativeTaskOutcome{Result: result}
	}

	baseTemp := settings.TempDirectory
	if strings.TrimSpace(baseTemp) == "" {
		baseTemp = "/tmp/rivus-maintenance"
	}
	if err := os.MkdirAll(baseTemp, 0o700); err != nil {
		result.Error = fmt.Sprintf("create maintenance temp directory: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	tempDir, err := os.MkdirTemp(baseTemp, "orphan-")
	if err != nil {
		result.Error = fmt.Sprintf("create orphan temp directory: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	defer os.RemoveAll(tempDir)

	indexStarted := time.Now()
	indexStats, err := writeReferencedFileBuckets(ctx, tbl, fsys, tempDir)
	result.Details = map[string]any{
		"dry_run":                      settings.OrphanDryRun,
		"minimum_age_hours":            settings.OrphanMinAge.Hours(),
		"disk_buckets":                 orphanHashBuckets,
		"reference_index_duration_ms":  time.Since(indexStarted).Milliseconds(),
		"snapshots_indexed":            indexStats.Snapshots,
		"manifest_lists_indexed":       indexStats.ManifestLists,
		"unique_manifests_indexed":     indexStats.UniqueManifests,
		"duplicate_manifests_skipped":  indexStats.DuplicateManifests,
		"data_file_references_indexed": indexStats.DataFileReferences,
	}
	if err != nil {
		result.Error = fmt.Sprintf("index referenced files: %v", err)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	cutoff := time.Now().Add(-settings.OrphanMinAge)
	var scannedBytes int64
	candidateWriter := newOrphanBucketWriter(tempDir, "candidate")
	listStarted := time.Now()
	walkErr := listable.WalkDir(tbl.Location(), func(path string, entry stdfs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().Before(cutoff) {
			return nil
		}
		if err := ensureSameStoragePrefix(tbl.Location(), path); err != nil {
			return err
		}
		scannedBytes += info.Size()
		return candidateWriter.Append(orphanDiskRecord{Path: path, Size: info.Size()})
	})
	closeErr := candidateWriter.Close()
	result.Details["object_listing_duration_ms"] = time.Since(listStarted).Milliseconds()
	result.Details["scan_bytes"] = scannedBytes
	if walkErr != nil {
		result.Error = fmt.Sprintf("stream object listing: %v", walkErr)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}
	if closeErr != nil {
		result.Error = fmt.Sprintf("flush candidate index: %v", closeErr)
		return nativeTaskOutcome{Result: result, Retryable: true}
	}

	var candidates, deleted int
	var deletedBytes int64
	for bucket := 0; bucket < orphanHashBuckets; bucket++ {
		if err := ctx.Err(); err != nil {
			result.Error = err.Error()
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
		refs, err := loadReferenceBucket(tempDir, bucket)
		if err != nil {
			result.Error = fmt.Sprintf("load reference bucket %d: %v", bucket, err)
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
		candidateFile := bucketFile(tempDir, "candidate", bucket)
		f, err := os.Open(candidateFile)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result.Error = fmt.Sprintf("open candidate bucket %d: %v", bucket, err)
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			var record orphanDiskRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				f.Close()
				result.Error = fmt.Sprintf("decode candidate bucket %d: %v", bucket, err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			if _, referenced := refs[record.Path]; referenced {
				continue
			}
			candidates++
			if settings.OrphanDryRun {
				continue
			}
			if err := fsys.Remove(record.Path); err != nil && !errors.Is(err, stdfs.ErrNotExist) {
				f.Close()
				result.Error = fmt.Sprintf("delete orphan %s: %v", record.Path, err)
				return nativeTaskOutcome{Result: result, Retryable: true}
			}
			deleted++
			deletedBytes += record.Size
		}
		scanErr := scanner.Err()
		f.Close()
		if scanErr != nil {
			result.Error = fmt.Sprintf("scan candidate bucket %d: %v", bucket, scanErr)
			return nativeTaskOutcome{Result: result, Retryable: true}
		}
	}
	result.OrphanCandidates = candidates
	result.DeletedFiles = deleted
	result.DeletedBytes = deletedBytes
	result.Status = "succeeded"
	result.RoutingReason = "bounded-memory disk-bucket orphan cleanup"
	return nativeTaskOutcome{Result: result}
}

func writeReferencedFileBuckets(ctx context.Context, tbl *icetable.Table, fsys iceio.IO, tempDir string) (stats orphanReferenceIndexStats, err error) {
	writer := newOrphanBucketWriter(tempDir, "reference")
	defer func() {
		err = errors.Join(err, writer.Close())
	}()
	add := func(path string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		path = strings.TrimSpace(path)
		if path == "" {
			return nil
		}
		if err := ensureSameStoragePrefix(tbl.Location(), path); err != nil {
			return err
		}
		return writer.Append(orphanDiskRecord{Path: path})
	}
	if err := add(tbl.MetadataLocation()); err != nil {
		return stats, err
	}
	for entry := range tbl.Metadata().PreviousFiles() {
		if err := add(entry.MetadataFile); err != nil {
			return stats, err
		}
	}
	if err := add(strings.TrimRight(tbl.Location(), "/") + "/metadata/version-hint.text"); err != nil {
		return stats, err
	}
	for stat := range tbl.Metadata().Statistics() {
		if err := add(stat.StatisticsPath); err != nil {
			return stats, err
		}
	}
	for stat := range tbl.Metadata().PartitionStatistics() {
		if err := add(stat.StatisticsPath); err != nil {
			return stats, err
		}
	}
	seenManifests := make(map[string]struct{})
	for _, snapshot := range tbl.Metadata().Snapshots() {
		stats.Snapshots++
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if err := add(snapshot.ManifestList); err != nil {
			return stats, err
		}
		stats.ManifestLists++
		manifests, err := snapshot.Manifests(fsys)
		if err != nil {
			return stats, fmt.Errorf("snapshot %d manifests: %w", snapshot.SnapshotID, err)
		}
		for _, manifest := range manifests {
			manifestPath := strings.TrimSpace(manifest.FilePath())
			if _, exists := seenManifests[manifestPath]; exists {
				stats.DuplicateManifests++
				continue
			}
			seenManifests[manifestPath] = struct{}{}
			stats.UniqueManifests++
			if err := add(manifestPath); err != nil {
				return stats, err
			}
			for entry, err := range manifest.Entries(fsys, true) {
				if err != nil {
					return stats, fmt.Errorf("manifest %s entries: %w", manifestPath, err)
				}
				if err := add(entry.DataFile().FilePath()); err != nil {
					return stats, err
				}
				stats.DataFileReferences++
			}
		}
	}
	return stats, nil
}

func bucketFile(tempDir, kind string, bucket int) string {
	return filepath.Join(tempDir, fmt.Sprintf("%s-%03d.jsonl", kind, bucket))
}

func loadReferenceBucket(tempDir string, bucket int) (map[string]struct{}, error) {
	refs := make(map[string]struct{})
	f, err := os.Open(bucketFile(tempDir, "reference", bucket))
	if errors.Is(err, os.ErrNotExist) {
		return refs, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var record orphanDiskRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, err
		}
		refs[record.Path] = struct{}{}
	}
	return refs, scanner.Err()
}

func ensureSameStoragePrefix(tableLocation, path string) error {
	base, err := url.Parse(tableLocation)
	if err != nil {
		return fmt.Errorf("invalid table location %q: %w", tableLocation, err)
	}
	candidate, err := url.Parse(path)
	if err != nil {
		return fmt.Errorf("invalid referenced path %q: %w", path, err)
	}
	if !strings.EqualFold(base.Scheme, candidate.Scheme) || !strings.EqualFold(base.Host, candidate.Host) {
		return fmt.Errorf("orphan cleanup prefix mismatch: table=%s candidate=%s", tableLocation, path)
	}
	return nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var _ io.Reader = (*os.File)(nil)
