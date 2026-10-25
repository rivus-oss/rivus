package iceberg

import (
	"context"
	"log"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

// RunMaintenanceWorkerBounded retries transient metadata outages in queue mode.
// Each attempt fully drains its executor pools and closes its stores before a
// new coordinator starts. Durable task leases and commit reconciliation retain
// responsibility for deciding which work can safely run again.
func RunMaintenanceWorkerBounded(ctx context.Context, dsn string, opts MaintenanceWorkerOptions) error {
	return runMaintenanceWorkerWithRetry(ctx, opts.Queue, func(ctx context.Context) error {
		return runMaintenanceWorkerBoundedOnce(ctx, dsn, opts)
	}, waitMaintenanceWorkerRetry)
}

func runMaintenanceWorkerWithRetry(ctx context.Context, queue bool, run func(context.Context) error, wait func(context.Context, time.Duration) error) error {
	delay := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil || !queue || !meta.IsTransientMetadataError(err) {
			return err
		}
		if time.Since(started) >= time.Minute {
			delay = time.Second
		}
		log.Printf("[maintenance-worker] metadata unavailable; coordinator drained, retrying in %s: %v", delay, err)
		if err := wait(ctx, delay); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		delay = min(2*delay, 30*time.Second)
	}
}

func waitMaintenanceWorkerRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
