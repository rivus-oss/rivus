package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

type isolatedLeaseStore struct {
	blockingClaimStore
	first   func(context.Context) error
	calls   atomic.Int32
	healthy chan string
}

func (s *isolatedLeaseStore) RenewJobLease(ctx context.Context, id, _, _ string, _ time.Duration) (bool, error) {
	if s.calls.Add(1) == 1 {
		return true, s.first(ctx)
	}
	s.healthy <- id
	return true, nil
}

func leaseIsolationManager(store meta.JobStore, count int, lease time.Duration) *JobManager {
	m := NewJobManager(nil, WithJobStore(store), WithWorkerRole(WorkerRoleStreaming),
		WithWorkerID("isolation-owner"), WithWorkerTiming(time.Millisecond, lease))
	for i := 0; i < count; i++ {
		job := NewJob(newTestJobConfig(fmt.Sprintf("isolation-%d", i)), nil)
		job.status = JobStatusRunning
		m.jobs[job.Config.ID] = job
		m.executionRoles[job.Config.ID] = meta.JobExecutionRoleStreaming
		m.workerLeases[job.Config.ID] = job.SubmissionID()
	}
	return m
}

func TestWorkerLeaseRenewalDoesNotSerializeUnrelatedJobs(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	store := &isolatedLeaseStore{healthy: make(chan string, 3), first: func(ctx context.Context) error {
		close(blocked)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	m := leaseIsolationManager(store, 3, 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	finished := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- m.renewWorkerJobLeases(ctx, store); close(finished) }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("lease renewal did not exit after cancellation")
		}
	})
	<-blocked
	select {
	case <-store.healthy:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("an unrelated job could not renew while one renewal was blocked")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := store.calls.Load(); got != 3 {
		t.Fatalf("renewed %d jobs, want 3", got)
	}
}

func TestWorkerLeaseRenewalContinuesAfterIndividualError(t *testing.T) {
	injected := errors.New("one registry row is unavailable")
	store := &isolatedLeaseStore{healthy: make(chan string, 3), first: func(context.Context) error { return injected }}
	err := leaseIsolationManager(store, 3, time.Second).renewWorkerJobLeases(context.Background(), store)
	if !errors.Is(err, injected) {
		t.Fatalf("renewal error = %v, want original failure", err)
	}
	if got := store.calls.Load(); got != 3 {
		t.Fatalf("one failure prevented other renewals: attempted %d jobs, want 3", got)
	}
}

func TestWorkerLeaseRenewalBoundsBlockedAttempt(t *testing.T) {
	store := &isolatedLeaseStore{healthy: make(chan string, 2), first: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := leaseIsolationManager(store, 2, 120*time.Millisecond).renewWorkerJobLeases(ctx, store)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked renewal returned %v, want deadline exceeded", err)
	}
	if ctx.Err() != nil {
		t.Fatal("one renewal consumed the entire parent lifetime")
	}
	if got := store.calls.Load(); got != 2 {
		t.Fatalf("blocked renewal prevented other renewals: attempted %d jobs, want 2", got)
	}
}

type blockedLeaseStore struct {
	blockingClaimStore
	calls atomic.Int32
	lost  bool
}

func (s *blockedLeaseStore) RenewJobLease(ctx context.Context, _, _, _ string, _ time.Duration) (bool, error) {
	s.calls.Add(1)
	if s.lost {
		return false, nil
	}
	<-ctx.Done()
	return false, ctx.Err()
}

func (s *blockedLeaseStore) ReleaseJobLease(ctx context.Context, _, _, _ string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestWorkerLeaseRenewalBoundsEntirePass(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost=%v", lost), func(t *testing.T) {
			store := &blockedLeaseStore{lost: lost}
			const count = 29
			lease := 1200 * time.Millisecond
			m := leaseIsolationManager(store, count, lease)
			ctx, cancel := context.WithTimeout(context.Background(), lease)
			defer cancel()
			start := time.Now()
			err := m.renewWorkerJobLeases(ctx, store)
			if !lost && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("blocked renewal error = %v", err)
			}
			if elapsed := time.Since(start); elapsed >= lease/2 {
				t.Fatalf("renewal pass consumed %s of %s lease", elapsed, lease)
			}
			if got := store.calls.Load(); got != count {
				t.Fatalf("attempted %d renewals, want %d", got, count)
			}
		})
	}
}
