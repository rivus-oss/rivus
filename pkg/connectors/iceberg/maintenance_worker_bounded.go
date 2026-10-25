package iceberg

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

// runMaintenanceWorkerBoundedOnce keeps scheduling and inventory discovery in one
// lightweight coordinator loop, while queue execution is delegated to bounded
// operation-specific pools. This lets snapshot expiration scale independently
// without allowing compaction to fan out with it.
func runMaintenanceWorkerBoundedOnce(ctx context.Context, dsn string, opts MaintenanceWorkerOptions) error {
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("RIVUS_META_MYSQL_DSN is required for maintenance-worker")
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = durationEnv("RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS", defaultMaintenanceWorkerPoll)
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = durationEnv("RIVUS_MAINTENANCE_LEASE_SECONDS", defaultMaintenanceLeaseDuration)
	}
	// TaskPageSize remains a one-shot compatibility setting. Queue mode claims
	// one task per executor at a time so a worker never leases a long batch that
	// can expire before later tasks in that batch start running.
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
	initCtx, initCancel := context.WithTimeout(ctx, 30*time.Second)
	defer initCancel()
	if err := store.Init(initCtx); err != nil {
		return fmt.Errorf("initialize maintenance store: %w", err)
	}

	jobStore, err := meta.NewMySQLJobStore(dsn)
	if err != nil {
		return err
	}
	defer jobStore.Close()
	if err := jobStore.Init(initCtx); err != nil {
		return fmt.Errorf("initialize job store: %w", err)
	}
	initCancel()

	workerCtx, workerCancel := context.WithCancel(ctx)
	var executorWG sync.WaitGroup
	defer func() {
		workerCancel()
		executorWG.Wait()
	}()

	concurrency := maintenanceConcurrencyFromEnv()
	queueAlerts := newMaintenanceQueueAlertManager(opts.WorkerID)
	registry := &maintenanceWorkerRegistry{}
	executorsStarted := false
	inventoryBatchesPerCycle := intEnv("RIVUS_MAINTENANCE_INVENTORY_BATCHES_PER_CYCLE", 4)
	if inventoryBatchesPerCycle < 1 {
		inventoryBatchesPerCycle = 1
	}
	if inventoryBatchesPerCycle > 32 {
		inventoryBatchesPerCycle = 32
	}

	log.Printf(
		"[maintenance-worker %s] started queue=%t poll=%s lease=%s task_page=%d due_page=%d compact_concurrency=%d expire_concurrency=%d orphan_concurrency=%d",
		opts.WorkerID,
		opts.Queue,
		opts.PollInterval,
		opts.LeaseDuration,
		opts.TaskPageSize,
		opts.DuePageSize,
		concurrency.Compact,
		concurrency.ExpireSnapshots,
		concurrency.OrphanCleanup,
	)

	var jobs map[string]maintenanceWorkerJob
	var streamExclusions []maintenanceMonitorTarget
	var lastStateSync time.Time
	var lastMonitorSync time.Time
	for {
		now := time.Now().UTC()
		if jobs == nil || now.Sub(lastStateSync) >= maintenanceJobStateSyncInterval {
			jobs, streamExclusions, err = syncMaintenanceStates(workerCtx, store, jobStore, now)
			if err != nil {
				return err
			}
			registry.Replace(jobs)
			lastStateSync = now
			lastMonitorSync = now
			if opts.Queue && !executorsStarted {
				startMaintenanceExecutorPools(workerCtx, store, jobStore, registry, opts, concurrency, &executorWG)
				executorsStarted = true
			}
		}
		if lastMonitorSync.IsZero() || now.Sub(lastMonitorSync) >= opts.PollInterval {
			jobs, err = syncMaintenanceMonitorStates(workerCtx, store, jobs, streamExclusions, now)
			if err != nil {
				return err
			}
			registry.Replace(jobs)
			lastMonitorSync = now
		}

		if opts.Queue {
			if err := store.RecoverExpiredMaintenanceLeases(workerCtx, now); err != nil {
				return fmt.Errorf("recover expired maintenance leases: %w", err)
			}
		}

		inventoryScanFailed := false
		for batch := 0; batch < inventoryBatchesPerCycle; batch++ {
			claimed, err := scanPriorityInventoryBatchBounded(workerCtx, store, jobStore, jobs, opts, now, 100, interactiveInventoryBatchSize)
			if err != nil {
				if workerCtx.Err() != nil {
					return nil
				}
				log.Printf("[maintenance-worker %s] interactive inventory scan error: %v", opts.WorkerID, err)
				inventoryScanFailed = true
				break
			}
			if claimed == 0 {
				break
			}
		}
		for batch := 0; !inventoryScanFailed && batch < inventoryBatchesPerCycle; batch++ {
			claimed, err := scanPriorityInventoryBatchBounded(workerCtx, store, jobStore, jobs, opts, now, 1, interactiveInventoryBatchSize)
			if err != nil {
				if workerCtx.Err() != nil {
					return nil
				}
				log.Printf("[maintenance-worker %s] commit inventory scan error: %v", opts.WorkerID, err)
				inventoryScanFailed = true
				break
			}
			if claimed == 0 {
				break
			}
		}
		if !inventoryScanFailed {
			if _, err := scanOnePendingInventoryBounded(workerCtx, store, jobStore, jobs, opts, now, 0); err != nil {
				if workerCtx.Err() != nil {
					return nil
				}
				log.Printf("[maintenance-worker %s] pending inventory scan error: %v", opts.WorkerID, err)
			}
		}

		if err := enqueueDueMaintenance(workerCtx, store, jobs, now, opts.DuePageSize); err != nil {
			return err
		}

		if opts.Queue {
			queueAlerts.MaybeCheck(workerCtx, store, now)
		} else {
			processed, err := processMaintenancePage(workerCtx, store, jobStore, jobs, opts, now)
			if err != nil {
				return err
			}
			log.Printf("[maintenance-worker %s] one-shot complete processed=%d", opts.WorkerID, processed)
			return nil
		}

		select {
		case <-workerCtx.Done():
			log.Printf("[maintenance-worker %s] shutdown requested", opts.WorkerID)
			return nil
		case <-time.After(opts.PollInterval):
		}
	}
}
