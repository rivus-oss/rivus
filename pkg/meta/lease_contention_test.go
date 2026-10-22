package meta

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

// These tests use only the explicitly configured RIVUS_TEST_MYSQL_DSN fixture.
func waitForLeaseTestLockWaits(t *testing.T, db *sql.DB, minimum int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM performance_schema.data_lock_waits`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count >= minimum {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d blocked MySQL transactions", minimum)
}

func TestMaintenanceCatalogGuardSerializesConcurrentWaiters(t *testing.T) {
	store, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	store.db.SetMaxOpenConns(12)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	blocker, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if err := lockMaintenanceCatalogs(ctx, blocker, []string{catalog}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// An existing guard is the duplicate-key path that used to upgrade S to X.
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	blocker, err = store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if err := lockMaintenanceCatalogs(ctx, blocker, []string{catalog}, time.Now()); err != nil {
		t.Fatal(err)
	}
	const waiters = 8
	results := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err == nil {
				defer tx.Rollback()
				err = lockMaintenanceCatalogs(ctx, tx, []string{catalog}, time.Now())
				if err == nil {
					err = tx.Commit()
				}
			}
			results <- err
		}()
	}
	waitForLeaseTestLockWaits(t, store.db, waiters)
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < waiters; i++ {
		if err := <-results; err != nil {
			t.Fatalf("catalog guard waiter failed: %v", err)
		}
	}
}

func TestMaintenanceCleanupDoesNotLockUnrelatedWorker(t *testing.T) {
	store, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	staleOwner, healthyOwner := catalog+"-a", catalog+"-b"
	seedCurrentOwnershipJob(t, store, staleOwner, "old")
	seedCurrentOwnershipJob(t, store, healthyOwner, "current")
	selectors := []IcebergMaintenanceReservationSelector{{Catalog: catalog, NamespacePattern: "sales", TablePattern: "*"}}
	for _, owner := range []string{staleOwner, healthyOwner} {
		submission := "current"
		if owner == staleOwner {
			submission = "old"
		}
		if err := store.SyncMaintenanceReservations(context.Background(), owner, submission, MaintenanceReservationStreaming, selectors, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE job_registry SET desired_state='STOPPED', last_status='STOPPED' WHERE job_id=?`, staleOwner); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	blocker, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if err := lockMaintenanceCatalogs(ctx, blocker, []string{catalog}, time.Now()); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- store.ReleaseMaintenanceReservationsExcept(ctx, nil, time.Now()) }()
	waitForLeaseTestLockWaits(t, store.db, 1)
	// Cleanup is blocked on the stale owner's catalog. A healthy owner's
	// registry row must remain writable by its heartbeat.
	renewCtx, stopRenewal := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = store.db.ExecContext(renewCtx, `UPDATE job_registry SET lease_until=UTC_TIMESTAMP(6) WHERE job_id=?`, healthyOwner)
	stopRenewal()
	if err != nil {
		t.Fatalf("cleanup blocked unrelated worker heartbeat: %v", err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if got := activeMaintenanceReservationCount(t, store, staleOwner, "old"); got != 0 {
		t.Fatalf("stale reservations retained: %d", got)
	}
	if got := activeMaintenanceReservationCount(t, store, healthyOwner, "current"); got != 1 {
		t.Fatalf("current reservations removed: %d", got)
	}
}

func TestJobLeaseRenewalUsesMetadataServerClock(t *testing.T) {
	maintenance, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	jobID := catalog + "-clock"
	seedCurrentOwnershipJob(t, maintenance, jobID, "current")
	store, err := NewMySQLJobStore(os.Getenv("RIVUS_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.leaseDB.SetMaxOpenConns(1)
	store.db.SetMaxOpenConns(1)
	ctx := context.Background()
	var lockWait int
	if err := store.leaseDB.QueryRow(`SELECT @@SESSION.innodb_lock_wait_timeout`).Scan(&lockWait); err != nil || lockWait != 1 {
		t.Fatalf("heartbeat server lock timeout = %d, err=%v", lockWait, err)
	}
	// Model a metadata clock different from Go's wall clock without changing
	// any host clock. All statements on this test-only connection see it.
	for _, db := range []*sql.DB{store.db, store.leaseDB} {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, `SET timestamp=1700000000.123456`); err != nil {
			conn.Close()
			t.Fatal(err)
		}
		conn.Close()
	}
	claimed, err := store.ClaimJobs(ctx, JobExecutionRoleStreaming, "worker", 100, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, job := range claimed {
		found = found || job.ID == jobID
	}
	if !found {
		t.Fatal("test job not claimed")
	}
	assertLease := func() {
		t.Helper()
		var remaining int64
		if err := store.leaseDB.QueryRow(`SELECT TIMESTAMPDIFF(MICROSECOND, UTC_TIMESTAMP(6), lease_until) FROM job_registry WHERE job_id=?`, jobID).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining != (30 * time.Second).Microseconds() {
			t.Fatalf("lease remaining according to metadata clock = %d microseconds", remaining)
		}
	}
	assertLease()
	ok, err := store.RenewJobLease(ctx, jobID, "current", "worker", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("renewal: %v, %v", ok, err)
	}
	assertLease()
	if ok, err := store.RenewJobLease(ctx, jobID, "stale", "worker", time.Minute); err != nil || ok {
		t.Fatalf("stale submission renewed: %v, %v", ok, err)
	}
}

func TestMaintenanceReservationTimeoutReleasesRegistryLock(t *testing.T) {
	store, catalog, _ := newMaintenanceOwnershipIntegrationStore(t)
	owner := catalog + "-timeout"
	seedCurrentOwnershipJob(t, store, owner, "current")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	blocker, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if err := lockMaintenanceCatalogs(ctx, blocker, []string{catalog}, time.Now()); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() {
		finished <- store.SyncMaintenanceReservations(ctx, owner, "current", MaintenanceReservationStreaming,
			[]IcebergMaintenanceReservationSelector{{Catalog: catalog, NamespacePattern: "sales", TablePattern: "*"}}, time.Now())
	}()
	waitForLeaseTestLockWaits(t, store.db, 1)
	select {
	case err := <-finished:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked reservation returned %v", err)
		}
	case <-time.After(maintenanceOwnershipTimeout + time.Second):
		t.Fatal("reservation did not honor ownership timeout")
	}
	// MySQL's lock timeout timer is coarse; allow the server to finish the
	// final bounded lock wait after the client deadline closes its socket.
	renewCtx, stopRenewal := context.WithTimeout(ctx, 3*time.Second)
	defer stopRenewal()
	if _, err := store.db.ExecContext(renewCtx, `UPDATE job_registry SET lease_until=UTC_TIMESTAMP(6) WHERE job_id=?`, owner); err != nil {
		t.Fatalf("timed-out reservation retained registry lock: %v", err)
	}
	if got := activeMaintenanceReservationCount(t, store, owner, "current"); got != 0 {
		t.Fatalf("timed-out reservation committed partial work: %d", got)
	}
}

func TestMaintenanceOwnershipRestoresConnectionSettings(t *testing.T) {
	store, _, _ := newMaintenanceOwnershipIntegrationStore(t)
	store.db.SetMaxOpenConns(1)
	if _, err := store.db.Exec(`SET SESSION innodb_lock_wait_timeout=9`); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []bool{false, true} {
		tx, cleanup, err := store.beginMaintenanceOwnershipTransaction(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var during int
		err = tx.QueryRow(`SELECT @@SESSION.innodb_lock_wait_timeout`).Scan(&during)
		if commit && err == nil {
			err = tx.Commit()
		}
		cleanup()
		if err != nil || during != 1 {
			t.Fatalf("ownership lock wait = %d, err=%v", during, err)
		}
		var after int
		if err := store.db.QueryRow(`SELECT @@SESSION.innodb_lock_wait_timeout`).Scan(&after); err != nil || after != 9 {
			t.Fatalf("pool session setting not restored: %d, err=%v", after, err)
		}
	}
}
