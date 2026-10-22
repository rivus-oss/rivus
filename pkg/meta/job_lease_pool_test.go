package meta

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestJobLeaseRenewalSurvivesExhaustedRegistryPool(t *testing.T) {
	mainState := &fenceSQLState{}
	leaseState := &fenceSQLState{}
	db := sql.OpenDB(fenceConnector{mainState})
	db.SetMaxOpenConns(1)
	leaseDB := sql.OpenDB(fenceConnector{leaseState})
	store := &MySQLJobStore{db: db, leaseDB: leaseDB}
	t.Cleanup(func() { _ = store.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Hold every general-purpose connection as a blocked progress write would.
	held, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	renewed, err := store.RenewJobLease(ctx, "job", "submission", "worker", 30*time.Second)
	if err != nil || !renewed {
		t.Fatalf("renewal blocked behind registry work: renewed=%v err=%v", renewed, err)
	}
	if mainState.writes != 0 || leaseState.writes != 1 {
		t.Fatalf("renewal used shared pool: general=%d heartbeat=%d", mainState.writes, leaseState.writes)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := leaseDB.PingContext(ctx); err == nil {
		t.Fatal("closing registry left heartbeat connections open")
	}
}

func TestNewMySQLJobStoreReservesIndependentLeaseConnections(t *testing.T) {
	store, err := NewMySQLJobStore("test:test@tcp(localhost:3306)/test?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.leaseDB == nil || store.leaseDB == store.db {
		t.Fatal("lease renewal shares the registry pool")
	}
	if got := store.leaseDB.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("heartbeat connection limit=%d, want 4", got)
	}
}
