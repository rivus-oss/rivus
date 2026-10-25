package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

func TestMaintenanceStoreRecoversAfterInitialTimeout(t *testing.T) {
	t.Setenv("RIVUS_META_MYSQL_DSN", "test-only")
	calls := 0
	want := &meta.IcebergMaintenanceStore{}
	s := &Server{maintenanceStoreInit: func(context.Context, string) (*meta.IcebergMaintenanceStore, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return want, nil
	}}
	rec := httptest.NewRecorder()
	if _, ok := s.maintenanceAPIStore(rec); ok || rec.Code != http.StatusInternalServerError {
		t.Fatalf("initial outage should be visible: status=%d body=%s", rec.Code, rec.Body)
	}
	if _, err := s.maintenanceStoreForView(context.Background()); !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("requests should share failure cooldown: calls=%d error=%v", calls, err)
	}
	// Simulate the next request after the retry deadline without sleeping.
	s.maintenanceRetryAt = time.Now().Add(-time.Second)
	got, err := s.maintenanceStoreForView(context.Background())
	if err != nil || got != want || calls != 2 {
		t.Fatalf("job-detail lookup did not recover: calls=%d store=%p error=%v", calls, got, err)
	}
	rec = httptest.NewRecorder()
	if got, ok := s.maintenanceAPIStore(rec); !ok || got != want || calls != 2 {
		t.Fatalf("API did not share recovered store: ok=%t calls=%d store=%p", ok, calls, got)
	}
}

func TestMaintenanceStoreConcurrentRequestsInitializeOnce(t *testing.T) {
	t.Setenv("RIVUS_META_MYSQL_DSN", "test-only")
	var calls atomic.Int32
	want := &meta.IcebergMaintenanceStore{}
	s := &Server{maintenanceStoreInit: func(context.Context, string) (*meta.IcebergMaintenanceStore, error) {
		calls.Add(1)
		return want, nil
	}}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := s.maintenanceStoreForView(context.Background())
			if err != nil || got != want {
				t.Errorf("concurrent lookup failed: store=%p error=%v", got, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent initialization count=%d, want 1", calls.Load())
	}
}

func TestMaintenanceStoreInitializationSurvivesRequestCancellation(t *testing.T) {
	t.Setenv("RIVUS_META_MYSQL_DSN", "test-only")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	want := &meta.IcebergMaintenanceStore{}
	s := &Server{maintenanceStoreInit: func(ctx context.Context, _ string) (*meta.IcebergMaintenanceStore, error) {
		if ctx.Err() != nil {
			t.Fatalf("request cancellation poisoned shared initialization: %v", ctx.Err())
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > maintenanceStoreInitTimeout {
			t.Fatal("initialization must have an independent bounded deadline")
		}
		return want, nil
	}}
	if got, err := s.maintenanceStoreForView(ctx); err != nil || got != want {
		t.Fatalf("shared initialization failed: store=%p error=%v", got, err)
	}
}
