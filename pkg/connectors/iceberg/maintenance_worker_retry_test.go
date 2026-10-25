package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMaintenanceQueueRecoversFromMetadataOutage(t *testing.T) {
	calls := 0
	var delays []time.Duration
	err := runMaintenanceWorkerWithRetry(context.Background(), true, func(context.Context) error {
		calls++
		switch calls {
		case 1:
			return sql.ErrTxDone
		case 2:
			return context.DeadlineExceeded
		default:
			return nil
		}
	}, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	})
	if err != nil || calls != 3 || !reflect.DeepEqual(delays, []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("queue did not recover: calls=%d delays=%v error=%v", calls, delays, err)
	}
}

func TestMaintenanceRetryPreservesPermanentAndOneShotErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		queue bool
		err   error
	}{{"one-shot", false, context.DeadlineExceeded}, {"ownership fence", true, errors.New("worker ownership lost")}, {"invalid configuration", true, errors.New("invalid config")}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := runMaintenanceWorkerWithRetry(context.Background(), tc.queue, func(context.Context) error {
				calls++
				return tc.err
			}, func(context.Context, time.Duration) error {
				t.Fatal("permanent or one-shot error must not be retried")
				return nil
			})
			if !errors.Is(err, tc.err) || calls != 1 {
				t.Fatalf("error changed: calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestMaintenanceShutdownInterruptsRetryBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := runMaintenanceWorkerWithRetry(ctx, true, func(context.Context) error {
		calls++
		return context.DeadlineExceeded
	}, func(ctx context.Context, delay time.Duration) error {
		cancel()
		return waitMaintenanceWorkerRetry(ctx, delay)
	})
	if err != nil || calls != 1 {
		t.Fatalf("shutdown did not stop retry cleanly: calls=%d error=%v", calls, err)
	}
}

func TestMaintenanceOutageBackoffIsBounded(t *testing.T) {
	calls := 0
	var delays []time.Duration
	err := runMaintenanceWorkerWithRetry(context.Background(), true, func(context.Context) error {
		calls++
		if calls <= 9 {
			return context.DeadlineExceeded
		}
		return nil
	}, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	})
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second}
	if err != nil || !reflect.DeepEqual(delays, want) {
		t.Fatalf("sustained outage must not hot-loop or stop retrying: delays=%v error=%v", delays, err)
	}
}
