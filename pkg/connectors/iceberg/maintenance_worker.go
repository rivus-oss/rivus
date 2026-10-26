package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	defaultMaintenanceWorkerPoll    = 30 * time.Second
	defaultMaintenanceLeaseDuration = 15 * time.Minute
	defaultMaintenanceRetryLimit    = 5
	defaultMaintenanceRetryBackoff  = time.Minute
	defaultInventoryRetryBackoff    = 5 * time.Minute
	defaultMaintenanceTaskPageSize  = 1
	defaultMaintenanceDuePageSize   = 100
	defaultCompactionCheckInterval  = 7 * 24 * time.Hour
	interactiveInventoryBatchSize   = 3
	maintenanceJobStateSyncInterval = 5 * time.Minute
)

type MaintenanceWorkerOptions struct {
	Queue         bool
	PollInterval  time.Duration
	LeaseDuration time.Duration
	TaskPageSize  int
	DuePageSize   int
	WorkerID      string
}

type maintenanceWorkerJob struct {
	Job                meta.PersistedJob
	Settings           nativeMaintenanceSettings
	OwnerType          string
	MonitorTargets     []maintenanceMonitorTarget
	MonitorDiscoveryAt time.Time
}

func RunMaintenanceWorker(ctx context.Context, dsn string, opts MaintenanceWorkerOptions) error {
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("RIVUS_META_MYSQL_DSN is required for maintenance-worker")
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = durationEnv("RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS", defaultMaintenanceWorkerPoll)
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = durationEnv("RIVUS_MAINTENANCE_LEASE_SECONDS", defaultMaintenanceLeaseDuration)
	}
	if opts.TaskPageSize != 1 {
		opts.TaskPageSize = intEnv("RIVUS_MAINTENANCE_TASK_PAGE_SIZE", defaultMaintenanceTaskPageSize)
		if opts.TaskPageSize != 1 {
			opts.TaskPageSize = 1
		}
	}
	if opts.DuePageSize <= 0 || opts.DuePageSize > 500 {
		opts.DuePageSize = intEnv("RIVUS_MAINTENANCE_DUE_PAGE_SIZE", defaultMaintenanceDuePageSize)
	}
	if strings.TrimSpace(opts.WorkerID) == "" {
		opts.WorkerID = maintenanceWorkerID()
	}

	if raw := strings.TrimSpace(os.Getenv("RIVUS_MAINTENANCE_GOMAXPROCS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			runtime.GOMAXPROCS(n)
		}
	} else {
		runtime.GOMAXPROCS(1)
	}
	if strings.TrimSpace(os.Getenv("GOMEMLIMIT")) == "" {
		debug.SetMemoryLimit(256 * 1024 * 1024)
	}

	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Init(ctx); err != nil {
		return fmt.Errorf("initialize maintenance store: %w", err)
	}

	jobStore, err := meta.NewMySQLJobStore(dsn)
	if err != nil {
		return err
	}
	defer jobStore.Close()
	if err := jobStore.Init(ctx); err != nil {
		return fmt.Errorf("initialize job store: %w", err)
	}

	log.Printf("[maintenance-worker %s] started queue=%t poll=%s lease=%s task_page=%d due_page=%d",
		opts.WorkerID, opts.Queue, opts.PollInterval, opts.LeaseDuration, opts.TaskPageSize, opts.DuePageSize)

	var jobs map[string]maintenanceWorkerJob
	var streamExclusions []maintenanceMonitorTarget
	var lastStateSync time.Time
	var lastMonitorSync time.Time
	for {
		now := time.Now().UTC()
		if jobs == nil || now.Sub(lastStateSync) >= maintenanceJobStateSyncInterval {
			jobs, streamExclusions, err = syncMaintenanceStates(ctx, store, jobStore, now)
			if err != nil {
				return err
			}
			lastStateSync = now
			lastMonitorSync = now
		}
		if lastMonitorSync.IsZero() || now.Sub(lastMonitorSync) >= opts.PollInterval {
			jobs, err = syncMaintenanceMonitorStates(ctx, store, jobs, streamExclusions, now)
			if err != nil {
				return err
			}
			lastMonitorSync = now
		}
		for {
			claimed, err := scanPriorityInventoryBatch(ctx, store, jobStore, jobs, opts, now, 100, interactiveInventoryBatchSize)
			if err != nil {
				log.Printf("[maintenance-worker %s] interactive inventory scan error: %v", opts.WorkerID, err)
				break
			}
			if claimed == 0 {
				break
			}
		}
		for {
			claimed, err := scanPriorityInventoryBatch(ctx, store, jobStore, jobs, opts, now, 1, interactiveInventoryBatchSize)
			if err != nil {
				log.Printf("[maintenance-worker %s] commit inventory scan error: %v", opts.WorkerID, err)
				break
			}
			if claimed == 0 {
				break
			}
		}
		if _, err := scanOnePendingInventory(ctx, store, jobStore, jobs, opts, now, 0); err != nil {
			log.Printf("[maintenance-worker %s] pending inventory scan error: %v", opts.WorkerID, err)
		}
		if err := enqueueDueMaintenance(ctx, store, jobs, now, opts.DuePageSize); err != nil {
			return err
		}
		processed, err := processMaintenancePage(ctx, store, jobStore, jobs, opts, now)
		if err != nil {
			return err
		}
		if !opts.Queue {
			log.Printf("[maintenance-worker %s] one-shot complete processed=%d", opts.WorkerID, processed)
			return nil
		}
		select {
		case <-ctx.Done():
			log.Printf("[maintenance-worker %s] shutdown requested", opts.WorkerID)
			return nil
		case <-time.After(opts.PollInterval):
		}
	}
}

func syncMaintenanceStates(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, now time.Time) (map[string]maintenanceWorkerJob, []maintenanceMonitorTarget, error) {
	persisted, err := jobStore.LoadJobs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("load persisted jobs for maintenance: %w", err)
	}
	if err := reconcilePersistedMaintenanceReservations(ctx, store, persisted, now); err != nil {
		return nil, nil, fmt.Errorf("reconcile maintenance reservations: %w", err)
	}
	streamExclusions := streamingMaintenanceExclusions(persisted)
	jobs := make(map[string]maintenanceWorkerJob)
	claimedTables := make(map[string]string)
	for _, job := range persisted {
		if job.Config == nil || isStandaloneMaintenanceMonitorConfig(job.Config) {
			continue
		}
		sinkType, sinkCfg := jobSinkSpec(job.Config)
		if !strings.EqualFold(sinkType, "iceberg_native") || !nativeMaintenanceEnabledFromRaw(sinkCfg) {
			continue
		}
		settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s invalid maintenance config: %v", job.ID, err)
			continue
		}
		iceCfg, err := decodeIcebergConfig(sinkCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s decode Iceberg config: %v", job.ID, err)
			continue
		}
		lightSink := &Sink{cfg: iceCfg}
		targets, err := orphanCleanupTargets(job.Config, lightSink, nil)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s resolve targets: %v", job.ID, err)
			continue
		}
		jobID := firstNonEmpty(strings.TrimSpace(job.ID), strings.TrimSpace(job.Config.ID))
		if jobID == "" {
			continue
		}
		snapshotComplete := maintenanceSnapshotComplete(ctx, store, job)
		if !reservationMustRemain(job, snapshotComplete) {
			continue
		}
		jobs[jobID] = maintenanceWorkerJob{Job: job, Settings: settings, OwnerType: "job"}
		catalogName, err := maintenanceIdentityCatalogName(iceCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip job=%s unresolved physical catalog: %v", job.ID, err)
			continue
		}
		for _, target := range targets {
			tableIdentity := canonicalMaintenanceTableKey(catalogName, target.Namespace, target.Table)
			if owner, exists := claimedTables[tableIdentity]; exists && owner != jobID {
				log.Printf("[maintenance-worker] skip duplicate table owner table=%s owner=%s conflicting_owner=%s", tableIdentity, owner, jobID)
				continue
			}
			claimedTables[tableIdentity] = jobID
			inventoryDue := now
			compactionDue := now.Add(deterministicJitter(tableIdentity+"|compact", settings.IdleCompactionInterval))
			expireDue := now.Add(deterministicJitter(tableIdentity+"|expire", settings.ExpireInterval))
			orphanDue := now.Add(deterministicJitter(tableIdentity+"|orphan", settings.OrphanInactiveInterval))
			if err := store.UpsertState(ctx, meta.IcebergMaintenanceState{
				TableKey:             tableIdentity,
				Catalog:              catalogName,
				Namespace:            target.Namespace,
				Table:                target.Table,
				OwnerType:            "job",
				OwnerJobID:           jobID,
				SnapshotComplete:     snapshotComplete,
				NextInventoryCheckAt: &inventoryDue,
			}, compactionDue, expireDue, orphanDue); err != nil {
				return nil, nil, fmt.Errorf("upsert maintenance state %s: %w", tableIdentity, err)
			}
		}
	}

	jobs, err = syncMaintenanceMonitorStates(ctx, store, jobs, streamExclusions, now)
	if err != nil {
		return nil, nil, err
	}
	// Bring persisted schedules within the configured intervals after an upgrade
	// or config change. Only pull deadlines forward; never postpone due work.
	for ownerID, job := range jobs {
		active := meta.IcebergMaintenanceState{TableKey: ownerID, LastWriteAt: &now}
		idle := meta.IcebergMaintenanceState{TableKey: ownerID}
		if err := store.ClampOrphanSchedules(ctx, ownerID, now, now.Add(-orphanActivityWindow),
			nextMaintenanceSchedule(active, maintenanceOperationOrphan, now, job.Settings),
			nextMaintenanceSchedule(idle, maintenanceOperationOrphan, now, job.Settings)); err != nil {
			return nil, nil, fmt.Errorf("reconcile orphan schedules for %s: %w", ownerID, err)
		}
	}
	return jobs, streamExclusions, err
}

func reservationMustRemain(job meta.PersistedJob, snapshotDone bool) bool {
	if job.Config == nil {
		return false
	}
	if normalizeMaintenanceMode(job.Config.Mode) == config.JobModeSnapshotOnly {
		return !snapshotDone
	}
	if strings.EqualFold(strings.TrimSpace(job.LastStatus), "PAUSED") {
		return true
	}
	return job.DesiredState == meta.DesiredStateRunning
}

func reconcilePersistedMaintenanceReservations(
	ctx context.Context,
	store *meta.IcebergMaintenanceStore,
	jobs []meta.PersistedJob,
	now time.Time,
) error {
	keep := make(map[string]string)
	for _, job := range jobs {
		if job.Config == nil || isStandaloneMaintenanceMonitorConfig(job.Config) {
			continue
		}
		sinkType, sinkCfg := jobSinkSpec(job.Config)
		if !strings.EqualFold(sinkType, "iceberg_native") {
			continue
		}
		snapshotDone := false
		if normalizeMaintenanceMode(job.Config.Mode) == config.JobModeSnapshotOnly {
			snapshotDone = maintenanceSnapshotComplete(ctx, store, job)
		}
		if !reservationMustRemain(job, snapshotDone) {
			continue
		}
		iceCfg, err := decodeIcebergConfig(sinkCfg)
		if err != nil {
			return fmt.Errorf("decode reservation job %s: %w", job.ID, err)
		}
		selectors, err := maintenanceReservationSelectors(job.Config, iceCfg)
		if err != nil {
			return fmt.Errorf("project reservation job %s: %w", job.ID, err)
		}
		ownerID := firstNonEmpty(strings.TrimSpace(job.ID), strings.TrimSpace(job.Config.ID))
		submissionID := strings.TrimSpace(job.SubmissionID)
		if submissionID == "" {
			submissionID = ownerID + ":legacy"
		}
		kind := meta.MaintenanceReservationStreaming
		if normalizeMaintenanceMode(job.Config.Mode) == config.JobModeSnapshotOnly {
			kind = meta.MaintenanceReservationSnapshot
		}
		if err := store.SyncMaintenanceReservations(ctx, ownerID, submissionID, kind, selectors, now); err != nil {
			return err
		}
		keep[ownerID] = submissionID
	}
	return store.ReleaseMaintenanceReservationsExcept(ctx, keep, now)
}

// streamingMaintenanceExclusions reserves every Iceberg target selected by a
// streaming job, whether or not that job enables the maintenance worker. The
// catalog monitor is a fallback for non-streaming tables only.
func streamingMaintenanceExclusions(persisted []meta.PersistedJob) []maintenanceMonitorTarget {
	seen := make(map[string]struct{})
	var exclusions []maintenanceMonitorTarget
	for _, job := range persisted {
		if job.Config == nil || !isStreamingJobConfig(job.Config) {
			continue
		}
		sinkType, sinkCfg := jobSinkSpec(job.Config)
		if !strings.EqualFold(sinkType, "iceberg_native") {
			continue
		}
		iceCfg, err := decodeIcebergConfig(sinkCfg)
		if err != nil {
			continue
		}
		catalogName, catalogErr := maintenanceIdentityCatalogName(iceCfg)
		if catalogErr != nil {
			log.Printf("[maintenance-worker] skip streaming reservation job=%s: %v", job.ID, catalogErr)
			continue
		}
		selectors, err := maintenanceReservationSelectors(job.Config, iceCfg)
		if err != nil || len(selectors) == 0 {
			// If a streaming selection cannot be projected safely, reserve the
			// whole catalog instead of risking concurrent maintenance ownership.
			selectors = []meta.IcebergMaintenanceReservationSelector{{Catalog: catalogName, NamespacePattern: "*", TablePattern: "*"}}
		}
		for _, selector := range selectors {
			exclusion := maintenanceMonitorTarget{Catalog: catalogName, Namespace: selector.NamespacePattern, Table: selector.TablePattern}
			key := exclusion.Catalog + "\x00" + exclusion.Namespace + "\x00" + exclusion.Table
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			exclusions = append(exclusions, exclusion)
		}
	}
	return exclusions
}

func isStreamingJobConfig(cfg *config.JobConfig) bool {
	if cfg == nil || cfg.Mode == config.JobModeMaintenanceOnly || cfg.Mode == config.JobModeSnapshotOnly {
		return false
	}
	return true
}

func maintenanceTargetExcludedByStream(exclusions []maintenanceMonitorTarget, target maintenanceMonitorTarget) bool {
	return newStreamingExclusionMatcher(exclusions).Excludes(target)
}

type streamingExclusionMatcher struct {
	exact    map[string]struct{}
	patterns []maintenanceMonitorTarget
}

func newStreamingExclusionMatcher(exclusions []maintenanceMonitorTarget) streamingExclusionMatcher {
	matcher := streamingExclusionMatcher{exact: make(map[string]struct{}, len(exclusions))}
	for _, exclusion := range exclusions {
		if hasGlobMeta(exclusion.Namespace) || hasGlobMeta(exclusion.Table) {
			matcher.patterns = append(matcher.patterns, exclusion)
			continue
		}
		matcher.exact[canonicalMaintenanceTableKey(exclusion.Catalog, exclusion.Namespace, exclusion.Table)] = struct{}{}
	}
	return matcher
}

func (m streamingExclusionMatcher) Excludes(target maintenanceMonitorTarget) bool {
	if _, exists := m.exact[canonicalMaintenanceTableKey(target.Catalog, target.Namespace, target.Table)]; exists {
		return true
	}
	for _, exclusion := range m.patterns {
		if exclusion.Catalog != target.Catalog {
			continue
		}
		namespaceMatch, namespaceErr := path.Match(exclusion.Namespace, target.Namespace)
		tableMatch, tableErr := path.Match(exclusion.Table, target.Table)
		if namespaceErr == nil && tableErr == nil && namespaceMatch && tableMatch {
			return true
		}
	}
	return false
}

// syncMaintenanceMonitorStates is intentionally cheap and runs every worker
// poll, independently of the ten-minute ingestion-job rescan. This makes a
// newly created, paused, or resumed monitor take effect promptly without
// repeatedly walking thousands of ingestion jobs.
func syncMaintenanceMonitorStates(ctx context.Context, store *meta.IcebergMaintenanceStore, jobs map[string]maintenanceWorkerJob, _ []maintenanceMonitorTarget, now time.Time) (map[string]maintenanceWorkerJob, error) {
	if jobs == nil {
		jobs = make(map[string]maintenanceWorkerJob)
	}
	for ownerID := range jobs {
		if strings.HasPrefix(ownerID, "monitor:") {
			delete(jobs, ownerID)
		}
	}

	monitors, err := store.ListMonitors(ctx)
	if err != nil {
		return nil, fmt.Errorf("load maintenance monitors: %w", err)
	}
	for _, monitor := range monitors {
		if monitor.Status != meta.MaintenanceMonitorActive || monitor.Config == nil {
			continue
		}
		cfg, explicitTargets, err := PrepareMaintenanceMonitorConfig(monitor.Config)
		if err != nil {
			log.Printf("[maintenance-worker] skip monitor=%s invalid config: %v", monitor.ID, err)
			continue
		}
		_, sinkCfg := jobSinkSpec(cfg)
		settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip monitor=%s invalid maintenance settings: %v", monitor.ID, err)
			continue
		}
		iceCfg, err := decodeIcebergConfig(sinkCfg)
		if err != nil {
			log.Printf("[maintenance-worker] skip monitor=%s decode Iceberg config: %v", monitor.ID, err)
			continue
		}
		ownerID := meta.MaintenanceMonitorOwnerID(monitor.ID)
		persistedTargets, err := store.ListMonitorTargets(ctx, monitor.ID)
		if err != nil {
			return nil, fmt.Errorf("load maintenance monitor %s targets: %w", monitor.ID, err)
		}
		targets := activeMaintenanceMonitorTargets(persistedTargets)
		applyDiscovery := false
		if catalogMonitoringEnabled(iceCfg) {
			if maintenanceMonitorDiscoveryDue(monitor, maintenanceDiscoveryInterval(iceCfg), now) {
				discoveryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				discovered, discoveryErr := discoverCatalogMonitorTargets(discoveryCtx, cfg, iceCfg)
				cancel()
				if discoveryErr != nil {
					if recordErr := store.RecordMonitorDiscoveryFailure(ctx, monitor.ID, discoveryErr.Error(), time.Now().UTC()); recordErr != nil {
						return nil, fmt.Errorf("record maintenance monitor %s discovery failure: %w", monitor.ID, recordErr)
					}
					log.Printf("[maintenance-worker] monitor=%s catalog discovery failed; retaining %d persisted targets: %v", monitor.ID, len(targets), discoveryErr)
				} else {
					targets = discovered
					applyDiscovery = true
					log.Printf("[maintenance-worker] monitor=%s discovered %d matching Iceberg tables", monitor.ID, len(targets))
				}
			}
		} else {
			catalogName, identityErr := maintenanceIdentityCatalogName(iceCfg)
			if identityErr != nil {
				log.Printf("[maintenance-worker] skip monitor=%s unresolved physical catalog: %v", monitor.ID, identityErr)
				continue
			}
			targets = make([]maintenanceMonitorTarget, 0, len(explicitTargets))
			for _, target := range explicitTargets {
				targets = append(targets, maintenanceMonitorTarget{Catalog: catalogName, Namespace: target.Namespace, Table: target.Table})
			}
			applyDiscovery = true
		}
		if applyDiscovery {
			membership := make([]meta.IcebergMaintenanceMonitorTarget, 0, len(targets))
			for _, target := range targets {
				tableIdentity := canonicalMaintenanceTableKey(target.Catalog, target.Namespace, target.Table)
				inventoryDue := now.Add(deterministicJitter(tableIdentity+"|initial-inventory", 24*time.Hour))
				membership = append(membership, meta.IcebergMaintenanceMonitorTarget{
					MonitorID: monitor.ID, TableKey: tableIdentity, Catalog: target.Catalog,
					Namespace: target.Namespace, Table: target.Table, Specificity: monitorSpecificity(iceCfg, target),
					NextInventoryAt:  &inventoryDue,
					NextCompactionAt: now.Add(deterministicJitter(tableIdentity+"|compact", settings.IdleCompactionInterval)),
					NextExpireAt:     now.Add(deterministicJitter(tableIdentity+"|expire", settings.ExpireInterval)),
					NextOrphanAt:     now.Add(deterministicJitter(tableIdentity+"|orphan", settings.OrphanInactiveInterval)),
				})
			}
			if _, err := store.ApplyMonitorDiscovery(ctx, monitor, membership, now); err != nil {
				return nil, fmt.Errorf("apply maintenance monitor %s discovery: %w", monitor.ID, err)
			}
			persistedTargets, err = store.ListMonitorTargets(ctx, monitor.ID)
			if err != nil {
				return nil, fmt.Errorf("reload maintenance monitor %s targets: %w", monitor.ID, err)
			}
			targets = activeMaintenanceMonitorTargets(persistedTargets)
		}
		persisted := meta.PersistedJob{
			ID: ownerID, Name: monitor.Name, Config: cfg,
			DesiredState: meta.DesiredStateRunning, LastStatus: "RUNNING",
		}
		discoveryAt := time.Time{}
		if monitor.LastDiscoveryAt != nil {
			discoveryAt = monitor.LastDiscoveryAt.UTC()
		}
		if applyDiscovery {
			discoveryAt = now
		}
		jobs[ownerID] = maintenanceWorkerJob{
			Job: persisted, Settings: settings, OwnerType: "monitor",
			MonitorTargets: targets, MonitorDiscoveryAt: discoveryAt,
		}
	}
	return jobs, nil
}

const maintenanceDiscoveryFailureRetry = 5 * time.Minute

func maintenanceMonitorDiscoveryDue(monitor meta.IcebergMaintenanceMonitor, interval time.Duration, now time.Time) bool {
	if monitor.LastDiscoveryAttemptAt != nil && monitor.LastDiscoveryError != "" &&
		now.Sub(monitor.LastDiscoveryAttemptAt.UTC()) < maintenanceDiscoveryFailureRetry {
		return false
	}
	return monitor.LastDiscoveryAt == nil || now.Sub(monitor.LastDiscoveryAt.UTC()) >= interval
}

func activeMaintenanceMonitorTargets(targets []meta.IcebergMaintenanceMonitorTarget) []maintenanceMonitorTarget {
	out := make([]maintenanceMonitorTarget, 0, len(targets))
	for _, target := range targets {
		if target.ClaimStatus == meta.MaintenanceMonitorTargetRetired {
			continue
		}
		out = append(out, maintenanceMonitorTarget{Catalog: target.Catalog, Namespace: target.Namespace, Table: target.Table})
	}
	return out
}

// scanOnePendingInventory performs one background metadata scan per poll. It
// intentionally remains slow so a 6,000-table first scan never becomes a
// burst. Detail-page requests use scanPriorityInventoryBatch instead.
func scanOnePendingInventory(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, jobs map[string]maintenanceWorkerJob, opts MaintenanceWorkerOptions, now time.Time, minimumPriority int) (bool, error) {
	state, err := store.ClaimPendingInventoryState(ctx, opts.WorkerID, now, opts.LeaseDuration, minimumPriority)
	if err != nil || state == nil {
		return false, err
	}
	return true, scanClaimedInventory(ctx, store, jobStore, jobs, opts, *state)
}

// scanPriorityInventoryBatch drains manual refreshes and commit-triggered
// inventory updates without the normal poll delay. Each batch reads only a
// few table metadata files concurrently, so a checkpoint that commits many
// tables does not become an unbounded burst.
func scanPriorityInventoryBatch(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, jobs map[string]maintenanceWorkerJob, opts MaintenanceWorkerOptions, now time.Time, minimumPriority, limit int) (int, error) {
	if limit <= 0 {
		limit = interactiveInventoryBatchSize
	}
	type claimedState struct {
		state meta.IcebergMaintenanceState
	}
	claimed := make([]claimedState, 0, limit)
	for len(claimed) < limit {
		state, err := store.ClaimPendingInventoryState(ctx, opts.WorkerID, now, opts.LeaseDuration, minimumPriority)
		if err != nil {
			return len(claimed), err
		}
		if state == nil {
			break
		}
		claimed = append(claimed, claimedState{state: *state})
	}
	if len(claimed) == 0 {
		return 0, nil
	}

	var wg sync.WaitGroup
	for _, item := range claimed {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := scanClaimedInventory(ctx, store, jobStore, jobs, opts, item.state); err != nil {
				log.Printf("[maintenance-worker %s] priority inventory table=%s error: %v", opts.WorkerID, item.state.TableKey, err)
			}
		}()
	}
	wg.Wait()
	return len(claimed), nil
}

func scanClaimedInventory(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, jobs map[string]maintenanceWorkerJob, opts MaintenanceWorkerOptions, state meta.IcebergMaintenanceState) error {
	job, ok, err := resolveMaintenanceWorkerJob(ctx, store, jobStore, jobs, state.OwnerJobID)
	if err != nil {
		message := fmt.Sprintf("load owner job configuration for inventory scan: %v", err)
		_ = store.RecordStateError(ctx, state.TableKey, message)
		if retryErr := store.RetryInventoryClaim(ctx, state.TableKey, opts.WorkerID, time.Now().Add(defaultInventoryRetryBackoff)); retryErr != nil {
			return retryErr
		}
		return fmt.Errorf("%s", message)
	}
	if !ok || job.Job.Config == nil {
		message := "owner job configuration is unavailable for inventory scan"
		_ = store.RecordStateError(ctx, state.TableKey, message)
		if retryErr := store.RetryInventoryClaim(ctx, state.TableKey, opts.WorkerID, time.Now().Add(defaultInventoryRetryBackoff)); retryErr != nil {
			return retryErr
		}
		return fmt.Errorf("%s", message)
	}
	stateConfig, err := maintenanceWorkerConfigForState(job.Job.Config, state)
	if err != nil {
		message := fmt.Sprintf("resolve catalog configuration for inventory scan: %v", err)
		_ = store.RecordStateError(ctx, state.TableKey, message)
		if retryErr := store.RetryInventoryClaim(ctx, state.TableKey, opts.WorkerID, time.Now().Add(defaultInventoryRetryBackoff)); retryErr != nil {
			return retryErr
		}
		return fmt.Errorf("%s", message)
	}

	scanCtx, cancel := context.WithTimeout(ctx, job.Settings.Timeout)
	err = refreshPendingInventory(scanCtx, store, stateConfig, state, job.Settings)
	cancel()
	if err != nil {
		_ = store.RecordStateError(ctx, state.TableKey, err.Error())
		if retryErr := store.RetryInventoryClaim(ctx, state.TableKey, opts.WorkerID, time.Now().Add(defaultInventoryRetryBackoff)); retryErr != nil {
			return retryErr
		}
		return err
	}
	return store.FinishInventoryClaim(ctx, state.TableKey, opts.WorkerID, state.LastSnapshotID)
}

func maintenanceWorkerConfigForState(cfg *config.JobConfig, state meta.IcebergMaintenanceState) (*config.JobConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("maintenance owner config is nil")
	}
	_, sinkCfg := jobSinkSpec(cfg)
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return nil, err
	}
	if !catalogMonitoringEnabled(iceCfg) {
		return cfg, nil
	}
	return maintenanceMonitorConfigForCatalog(cfg, state.Catalog)
}

// resolveMaintenanceWorkerJob uses the periodically refreshed job map during
// normal operation. If a job was deleted and immediately re-submitted, that
// map can briefly be stale. In that case reload only the owning job from the
// durable registry instead of waiting for the next full state sync, which may
// touch thousands of tables.
func resolveMaintenanceWorkerJob(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, jobs map[string]maintenanceWorkerJob, jobID string) (maintenanceWorkerJob, bool, error) {
	if job, ok := jobs[jobID]; ok && job.Job.Config != nil {
		return job, true, nil
	}
	persisted, err := jobStore.LoadJobs(ctx)
	if err != nil {
		return maintenanceWorkerJob{}, false, err
	}
	for _, job := range persisted {
		id := firstNonEmpty(strings.TrimSpace(job.ID), jobConfigID(job.Config))
		if id != jobID || job.Config == nil || isStandaloneMaintenanceMonitorConfig(job.Config) {
			continue
		}
		sinkType, sinkCfg := jobSinkSpec(job.Config)
		if !strings.EqualFold(sinkType, "iceberg_native") || !nativeMaintenanceEnabledFromRaw(sinkCfg) {
			return maintenanceWorkerJob{}, false, nil
		}
		settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
		if err != nil {
			return maintenanceWorkerJob{}, false, err
		}
		return maintenanceWorkerJob{Job: job, Settings: settings, OwnerType: "job"}, true, nil
	}
	if strings.HasPrefix(jobID, "monitor:") {
		monitorID := strings.TrimPrefix(jobID, "monitor:")
		monitor, err := store.GetMonitor(ctx, monitorID)
		if err != nil {
			return maintenanceWorkerJob{}, false, err
		}
		if monitor == nil || monitor.Status != meta.MaintenanceMonitorActive || monitor.Config == nil {
			return maintenanceWorkerJob{}, false, nil
		}
		cfg, _, err := PrepareMaintenanceMonitorConfig(monitor.Config)
		if err != nil {
			return maintenanceWorkerJob{}, false, err
		}
		_, sinkCfg := jobSinkSpec(cfg)
		settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
		if err != nil {
			return maintenanceWorkerJob{}, false, err
		}
		return maintenanceWorkerJob{Job: meta.PersistedJob{
			ID: jobID, Name: monitor.Name, Config: cfg,
			DesiredState: meta.DesiredStateRunning, LastStatus: "RUNNING",
		}, Settings: settings, OwnerType: "monitor"}, true, nil
	}
	return maintenanceWorkerJob{}, false, nil
}

func jobConfigID(cfg *config.JobConfig) string {
	if cfg == nil {
		return ""
	}
	return strings.TrimSpace(cfg.ID)
}

// Standalone maintenance registrations belong to the monitor control plane.
// Older Rivus versions could persist them in job_registry before failing to
// start a source-less ingestion pipeline. Those legacy rows must not reserve
// table ownership ahead of the real monitor with the same targets.
func isStandaloneMaintenanceMonitorConfig(cfg *config.JobConfig) bool {
	if cfg == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(string(cfg.Mode)), string(config.JobModeMaintenanceOnly))
}

func maintenanceSnapshotComplete(ctx context.Context, store *meta.IcebergMaintenanceStore, job meta.PersistedJob) bool {
	if job.Config == nil {
		return false
	}
	mode := normalizeMaintenanceMode(job.Config.Mode)
	if mode == config.JobModeLatest || mode == config.JobModeLatestOffset {
		return true
	}
	metaKey := strings.TrimSpace(job.MetaKey)
	if metaKey == "" {
		// Backward compatibility for registry rows created before meta_key was
		// persisted. New rows use the control plane's authoritative key so the
		// maintenance barrier and streaming checkpoint cannot diverge.
		sourceType, sourceCfg := jobSourceSpec(job.Config)
		sinkType, sinkCfg := jobSinkSpec(job.Config)
		metaKey = maintenanceMetaKey(job.Config.ID, string(mode), sourceType, sourceCfg, sinkType, sinkCfg)
	}
	done, found, err := store.SnapshotDone(ctx, metaKey)
	if err != nil {
		log.Printf("[maintenance-worker] snapshot barrier lookup job=%s error=%v", job.ID, err)
		return false
	}
	if found {
		return done
	}
	return mode == config.JobModeSnapshotOnly && strings.EqualFold(strings.TrimSpace(job.LastStatus), "DONE")
}

func enqueueDueMaintenance(ctx context.Context, store *meta.IcebergMaintenanceStore, jobs map[string]maintenanceWorkerJob, now time.Time, pageSize int) error {
	if len(jobs) == 0 {
		return nil
	}
	ownerIDs := make([]string, 0, len(jobs))
	for ownerID := range jobs {
		ownerIDs = append(ownerIDs, ownerID)
	}
	sort.Strings(ownerIDs)
	operations := []struct {
		name     string
		priority int
	}{
		{name: "compact", priority: 10},
		{name: "expire_snapshots", priority: 30},
		{name: "remove_orphan_files", priority: 40},
	}
	for _, operation := range operations {
		states, err := store.DueStates(ctx, operation.name, now, pageSize, ownerIDs)
		if err != nil {
			return fmt.Errorf("query due %s states: %w", operation.name, err)
		}
		for _, state := range states {
			job, ok := jobs[state.OwnerJobID]
			if !ok {
				continue
			}
			due := stateDueTime(state, operation.name)
			if due == nil {
				continue
			}
			window := due.UTC().Format(time.RFC3339Nano)
			priority := operation.priority
			if operation.name == maintenanceOperationCompact {
				priority = compactionTaskPriority(state, job.Settings, now)
			}
			_, err := store.EnqueueTask(ctx, state, operation.name, priority, window, now, map[string]any{
				"scheduled_at": due.UTC().Format(time.RFC3339Nano),
				"executor":     job.Settings.Executor,
				"priority":     priority,
			})
			if err != nil {
				return fmt.Errorf("enqueue %s for %s: %w", operation.name, state.TableKey, err)
			}
			next := nextMaintenanceSchedule(state, operation.name, now, job.Settings)
			if err := store.AdvanceSchedule(ctx, state.TableKey, operation.name, next); err != nil {
				return fmt.Errorf("advance %s schedule for %s: %w", operation.name, state.TableKey, err)
			}
		}
	}
	return nil
}

func processMaintenancePage(ctx context.Context, store *meta.IcebergMaintenanceStore, jobStore meta.JobStore, jobs map[string]maintenanceWorkerJob, opts MaintenanceWorkerOptions, now time.Time) (int, error) {
	tasks, err := store.ClaimTasks(ctx, opts.WorkerID, now, opts.LeaseDuration, opts.TaskPageSize)
	if err != nil {
		return 0, fmt.Errorf("claim maintenance tasks: %w", err)
	}
	return processClaimedMaintenanceTasks(ctx, store, jobStore, jobs, opts, opts.WorkerID, tasks, now)
}

func maintenancePreflightFailureResult(runID int64, task meta.IcebergMaintenanceTask, message string) meta.IcebergMaintenanceResult {
	return meta.IcebergMaintenanceResult{
		RunID:         runID,
		TaskID:        task.ID,
		TableKey:      task.TableKey,
		Operation:     task.Operation,
		Engine:        "none",
		RoutingReason: "Worker preflight failed",
		Status:        "failed",
		Attempt:       task.AttemptCount,
		Error:         message,
		CreatedAt:     time.Now().UTC(),
	}
}

func nativeMaintenanceEnabledFromRaw(sinkCfg any) bool {
	maintenance := rawMaintenanceMap(sinkCfg)
	return rawBool(maintenance, "enabled", false)
}

func nativeMaintenanceSettingsFromRaw(sinkCfg any) (nativeMaintenanceSettings, error) {
	settings := defaultNativeMaintenanceSettings()
	maintenance := rawMaintenanceMap(sinkCfg)
	settings.Enabled = rawBool(maintenance, "enabled", false)
	settings.Executor = normalizeMaintenanceExecutor(rawString(maintenance, "executor", maintenanceExecutorHybrid))
	settings.DataFilesThreshold = rawInt(maintenance, "data_files_threshold", settings.DataFilesThreshold)
	settings.EqualityDeleteThreshold = rawInt(maintenance, "equality_delete_files_threshold", settings.EqualityDeleteThreshold)
	settings.PositionDeleteThreshold = rawInt(maintenance, "position_delete_files_threshold", settings.PositionDeleteThreshold)
	settings.MaxSelectedInputBytes = rawInt64(maintenance, "native_max_selected_input_bytes", settings.MaxSelectedInputBytes)
	settings.MaxSelectedFiles = rawInt(maintenance, "native_max_selected_files", settings.MaxSelectedFiles)
	settings.MaxEqualityDeleteFiles = rawInt(maintenance, "native_max_equality_delete_files", settings.MaxEqualityDeleteFiles)
	settings.TargetFileSizeBytes = rawInt64(maintenance, "native_target_file_size_bytes", settings.TargetFileSizeBytes)
	settings.SmallFileSizeBytes = rawInt64(maintenance, "small_file_size_bytes", settings.SmallFileSizeBytes)
	settings.MinSmallFiles = rawInt(maintenance, "small_files_min_count", settings.MinSmallFiles)
	settings.MinSmallBytes = rawInt64(maintenance, "small_files_min_total_bytes", settings.MinSmallBytes)
	settings.ScanConcurrency = rawInt(maintenance, "native_scan_concurrency", settings.ScanConcurrency)
	settings.Timeout = time.Duration(rawInt(maintenance, "native_timeout_seconds", int(settings.Timeout/time.Second))) * time.Second
	settings.TempDirectory = rawString(maintenance, "worker_temp_directory", settings.TempDirectory)
	settings.ExpireInterval = time.Duration(rawInt(maintenance, "native_expire_interval_seconds", int(settings.ExpireInterval/time.Second))) * time.Second
	settings.SnapshotMaxAge = time.Duration(rawFloat64(maintenance, "native_snapshot_max_age_hours", settings.SnapshotMaxAge.Hours()) * float64(time.Hour))
	settings.SnapshotRetainLast = rawInt(maintenance, "native_snapshot_retain_last", settings.SnapshotRetainLast)
	settings.OrphanInterval = time.Duration(rawInt(maintenance, "native_orphan_interval_seconds", int(settings.OrphanInterval/time.Second))) * time.Second
	// An explicitly longer active interval must remain valid when no inactive
	// interval was supplied. Explicit inactive overrides still undergo validation.
	inactiveDefault := durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INACTIVE_INTERVAL_SECONDS", max(defaultNativeOrphanInactiveInterval, settings.OrphanInterval))
	settings.OrphanInactiveInterval = time.Duration(rawInt(maintenance, "native_orphan_inactive_interval_seconds", int(inactiveDefault/time.Second))) * time.Second
	settings.OrphanMinAge = time.Duration(rawFloat64(maintenance, "native_orphan_min_age_hours", settings.OrphanMinAge.Hours()) * float64(time.Hour))
	settings.OrphanDryRun = rawBool(maintenance, "native_orphan_dry_run", settings.OrphanDryRun)
	settings.SparkPollInterval = time.Duration(rawInt(maintenance, "spark_poll_interval_seconds", int(settings.SparkPollInterval/time.Second))) * time.Second
	settings.SparkTimeout = time.Duration(rawInt(maintenance, "spark_timeout_seconds", int(settings.SparkTimeout/time.Second))) * time.Second
	settings.IdleCompactionInterval = time.Duration(rawInt(maintenance, "native_idle_check_interval_seconds", int(settings.IdleCompactionInterval/time.Second))) * time.Second

	if err := validateMaintenanceExecutor(settings.Executor); err != nil {
		return settings, err
	}
	switch {
	case settings.DataFilesThreshold <= 0:
		return settings, fmt.Errorf("data_files_threshold must be > 0")
	case settings.EqualityDeleteThreshold <= 0:
		return settings, fmt.Errorf("equality_delete_files_threshold must be > 0")
	case settings.PositionDeleteThreshold <= 0:
		return settings, fmt.Errorf("position_delete_files_threshold must be > 0")
	case settings.MaxSelectedInputBytes <= 0:
		return settings, fmt.Errorf("native_max_selected_input_bytes must be > 0")
	case settings.MaxSelectedFiles <= 0:
		return settings, fmt.Errorf("native_max_selected_files must be > 0")
	case settings.MaxEqualityDeleteFiles <= 0:
		return settings, fmt.Errorf("native_max_equality_delete_files must be > 0")
	case settings.TargetFileSizeBytes <= 0:
		return settings, fmt.Errorf("native_target_file_size_bytes must be > 0")
	case settings.SmallFileSizeBytes <= 0:
		return settings, fmt.Errorf("small_file_size_bytes must be > 0")
	case settings.MinSmallFiles <= 0:
		return settings, fmt.Errorf("small_files_min_count must be > 0")
	case settings.MinSmallBytes <= 0:
		return settings, fmt.Errorf("small_files_min_total_bytes must be > 0")
	case settings.ScanConcurrency <= 0:
		return settings, fmt.Errorf("native_scan_concurrency must be > 0")
	case settings.Timeout <= 0:
		return settings, fmt.Errorf("native_timeout_seconds must be > 0")
	case settings.ExpireInterval <= 0:
		return settings, fmt.Errorf("native_expire_interval_seconds must be > 0")
	case settings.SnapshotMaxAge <= 0:
		return settings, fmt.Errorf("native_snapshot_max_age_hours must be > 0")
	case settings.SnapshotRetainLast < 1:
		return settings, fmt.Errorf("native_snapshot_retain_last must be >= 1")
	case settings.OrphanInterval <= 0:
		return settings, fmt.Errorf("native_orphan_interval_seconds must be > 0")
	case settings.OrphanInactiveInterval < settings.OrphanInterval:
		return settings, fmt.Errorf("native_orphan_inactive_interval_seconds must be >= native_orphan_interval_seconds")
	case settings.OrphanMinAge < defaultNativeOrphanAge:
		return settings, fmt.Errorf("native_orphan_min_age_hours must be at least %.0f", defaultNativeOrphanAge.Hours())
	case settings.IdleCompactionInterval <= 0:
		return settings, fmt.Errorf("native_idle_check_interval_seconds must be > 0")
	}
	return settings, nil
}

func rawMaintenanceMap(sinkCfg any) map[string]any {
	root, ok := sinkCfg.(map[string]any)
	if !ok {
		return nil
	}
	value, ok := root["table_maintenance"]
	if !ok {
		return nil
	}
	out, _ := value.(map[string]any)
	return out
}

func rawBool(m map[string]any, key string, fallback bool) bool {
	if m == nil {
		return fallback
	}
	value, ok := m[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func rawString(m map[string]any, key, fallback string) string {
	if m == nil {
		return fallback
	}
	if value, ok := m[key].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func rawInt(m map[string]any, key string, fallback int) int {
	return int(rawInt64(m, key, int64(fallback)))
}

func rawInt64(m map[string]any, key string, fallback int64) int64 {
	if m == nil {
		return fallback
	}
	value, ok := m[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func rawFloat64(m map[string]any, key string, fallback float64) float64 {
	if m == nil {
		return fallback
	}
	value, ok := m[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		if n, err := v.Float64(); err == nil {
			return n
		}
	case string:
		if n, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return n
		}
	}
	return fallback
}

func canonicalMaintenanceTableKey(catalog, namespace, table string) string {
	return strings.TrimSpace(catalog) + "." + strings.TrimSpace(namespace) + "." + strings.TrimSpace(table)
}

func deterministicJitter(key string, window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}
	sum := sha256.Sum256([]byte(key))
	value := binary.BigEndian.Uint64(sum[:8])
	return time.Duration(value % uint64(window))
}

func nextMaintenanceSchedule(state meta.IcebergMaintenanceState, operation string, now time.Time, settings nativeMaintenanceSettings) time.Time {
	tableKey := state.TableKey
	interval := settings.IdleCompactionInterval
	if interval <= 0 {
		interval = defaultCompactionCheckInterval
	}
	switch operation {
	case "expire_snapshots":
		interval = settings.ExpireInterval
	case "remove_orphan_files":
		interval = settings.OrphanInterval
		if !orphanCleanupActive(state, now) {
			interval = settings.OrphanInactiveInterval
		}
	}
	jitterWindow := interval / 10
	if jitterWindow > time.Hour {
		jitterWindow = time.Hour
	}
	return now.Add(interval).Add(deterministicJitter(tableKey+"|"+operation+"|next", jitterWindow))
}

func orphanCleanupActive(state meta.IcebergMaintenanceState, now time.Time) bool {
	if state.LastWriteAt == nil {
		return false
	}
	lastWrite := state.LastWriteAt.UTC()
	return !lastWrite.After(now) && now.Sub(lastWrite) < orphanActivityWindow
}

func stateDueTime(state meta.IcebergMaintenanceState, operation string) *time.Time {
	switch operation {
	case "compact":
		return state.NextCompactionCheckAt
	case "expire_snapshots":
		return state.NextExpireCheckAt
	case "remove_orphan_files":
		return state.NextOrphanCheckAt
	default:
		return nil
	}
}

func maintenanceRetryBackoff(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		base = defaultMaintenanceRetryBackoff
	}
	if attempt < 1 {
		attempt = 1
	}
	power := math.Pow(2, float64(minInt(attempt-1, 6)))
	return time.Duration(float64(base) * power)
}

func retryLimitFromRaw(jobCfg *config.JobConfig) int {
	if jobCfg == nil {
		return defaultMaintenanceRetryLimit
	}
	_, sinkCfg := jobSinkSpec(jobCfg)
	return rawInt(rawMaintenanceMap(sinkCfg), "retry_limit", defaultMaintenanceRetryLimit)
}

func retryBackoffFromRaw(jobCfg *config.JobConfig) time.Duration {
	if jobCfg == nil {
		return defaultMaintenanceRetryBackoff
	}
	_, sinkCfg := jobSinkSpec(jobCfg)
	seconds := rawInt(rawMaintenanceMap(sinkCfg), "retry_base_backoff_seconds", int(defaultMaintenanceRetryBackoff/time.Second))
	return time.Duration(seconds) * time.Second
}

func normalizeMaintenanceMode(mode config.JobMode) config.JobMode {
	switch mode {
	case config.JobModeInitial, config.JobModeSnapshotOnly, config.JobModeResume, config.JobModeLatestOffset, config.JobModeLatest:
		return mode
	default:
		return config.JobModeInitial
	}
}

type maintenanceMetaKeyPayload struct {
	Version int    `json:"v"`
	JobID   string `json:"job_id"`
	Mode    string `json:"mode"`
	Source  struct {
		Type   string `json:"type"`
		Config any    `json:"config"`
	} `json:"source"`
	Sink struct {
		Type   string `json:"type"`
		Config any    `json:"config"`
	} `json:"sink"`
}

func maintenanceMetaKey(jobID, mode, sourceType string, sourceCfg any, sinkType string, sinkCfg any) string {
	var payload maintenanceMetaKeyPayload
	payload.Version = 1
	payload.JobID = jobID
	payload.Mode = mode
	payload.Source.Type = sourceType
	payload.Source.Config = sourceCfg
	payload.Sink.Type = sinkType
	payload.Sink.Config = stableMaintenanceSinkConfig(sinkCfg)
	encoded, _ := json.Marshal(payload)
	sum := sha256.Sum256(encoded)
	return "rivus/v1/" + hex.EncodeToString(sum[:])
}

func stableMaintenanceSinkConfig(cfg any) any {
	m, ok := cfg.(map[string]any)
	if !ok {
		return cfg
	}
	return copyMaintenanceMapWithoutKey(m, "cdc_delete_executor")
}

func copyMaintenanceMapWithoutKey(in map[string]any, skip string) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		if key == skip {
			continue
		}
		switch nested := value.(type) {
		case map[string]any:
			out[key] = copyMaintenanceMapWithoutKey(nested, skip)
		case []any:
			items := make([]any, len(nested))
			for i, item := range nested {
				if child, ok := item.(map[string]any); ok {
					items[i] = copyMaintenanceMapWithoutKey(child, skip)
				} else {
					items[i] = item
				}
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}

func maintenanceWorkerID() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "rivus"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func intEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func int64Env(key string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func float64Env(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}

func timePtr(t time.Time) *time.Time { return &t }

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
