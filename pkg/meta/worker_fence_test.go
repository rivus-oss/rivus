package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type fenceSQLState struct {
	owned           bool
	loseDuringWrite bool
	writes          int
	checks          int
	commits         int
	rollbacks       int
	backup          int
	queryErr        error
}
type fenceConnector struct{ state *fenceSQLState }

func (c fenceConnector) Connect(context.Context) (driver.Conn, error) { return fenceConn{c.state}, nil }
func (fenceConnector) Driver() driver.Driver                          { return deleteFailureDriver{} }

type fenceConn struct{ state *fenceSQLState }

func (fenceConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (fenceConn) Close() error                        { return nil }
func (c fenceConn) Begin() (driver.Tx, error) {
	c.state.backup = c.state.writes
	return fenceTx{c.state}, nil
}
func (c fenceConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("fence requires serializable transaction")
	}
	return c.Begin()
}

type fenceTx struct{ state *fenceSQLState }

func (t fenceTx) Commit() error   { t.state.commits++; return nil }
func (t fenceTx) Rollback() error { t.state.rollbacks++; t.state.writes = t.state.backup; return nil }
func (c fenceConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "job_registry") {
		return nil, errors.New("unexpected read: " + query)
	}
	c.state.checks++
	if c.state.queryErr != nil {
		return nil, c.state.queryErr
	}
	if len(args) != 3 || args[0].Value != "job" || args[1].Value != "submission" || args[2].Value != "worker" {
		return nil, errors.New("ownership query omitted job/submission/owner")
	}
	if !strings.Contains(query, "UTC_TIMESTAMP(6)") {
		return nil, errors.New("lease expiration not fenced")
	}
	return &fenceRows{owned: c.state.owned}, nil
}
func (c fenceConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.state.writes++
	if c.state.loseDuringWrite {
		c.state.owned = false
	}
	return driver.RowsAffected(1), nil
}

type fenceRows struct{ owned bool }

func (*fenceRows) Columns() []string { return []string{"owned"} }
func (*fenceRows) Close() error      { return nil }
func (r *fenceRows) Next(dest []driver.Value) error {
	if !r.owned {
		return io.EOF
	}
	r.owned = false
	dest[0] = int64(1)
	return nil
}

func fenceTestStore(t *testing.T, state *fenceSQLState) *FencedOffsetStore {
	t.Helper()
	db := sql.OpenDB(fenceConnector{state})
	t.Cleanup(func() { _ = db.Close() })
	return &FencedOffsetStore{MySQLOffsetStore: &MySQLOffsetStore{db: db}, ownership: WorkerOwnership{JobID: "job", SubmissionID: "submission", Owner: "worker", StateKey: "state"}}
}

func TestWorkerFenceRejectsLostOwnershipAndRollsBackExpiredWrite(t *testing.T) {
	for _, mode := range []string{"owned", "lost", "expired_during_write", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			state := &fenceSQLState{owned: mode != "lost", loseDuringWrite: mode == "expired_during_write"}
			injected := errors.New("lease store unavailable")
			if mode == "unavailable" {
				state.queryErr = injected
			}
			store := fenceTestStore(t, state)
			err := store.SaveOffset(context.Background(), "state", Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42})
			if mode == "owned" {
				if err != nil || state.writes != 1 || state.commits != 1 || state.checks != 2 {
					t.Fatalf("owned result=%v state=%#v", err, state)
				}
				return
			}
			if err == nil || state.writes != 0 || state.commits != 0 {
				t.Fatalf("stale/unavailable write result=%v state=%#v", err, state)
			}
			if mode != "unavailable" && !errors.Is(err, ErrWorkerOwnershipLost) {
				t.Fatalf("lost lease result=%v", err)
			}
		})
	}
}

func TestWorkerFenceProtectsRecoveryAndSnapshotWrites(t *testing.T) {
	for _, operation := range []string{"begin", "mark", "delete", "progress", "done", "start", "clear", "scope"} {
		t.Run(operation, func(t *testing.T) {
			state := &fenceSQLState{owned: false}
			s := fenceTestStore(t, state)
			ctx := context.Background()
			var err error
			switch operation {
			case "begin":
				err = s.BeginIcebergCommit(ctx, IcebergCommitIntent{JobID: "job", StateKey: "state", OperationID: "op"})
			case "mark":
				err = s.MarkIcebergCommitCommitted(ctx, "state", "op")
			case "delete":
				err = s.DeleteIcebergCommit(ctx, "state", "op")
			case "progress":
				err = s.SaveSnapshotProgress(ctx, "state", "app.orders", 2, "")
			case "done":
				err = s.MarkSnapshotDone(ctx, "state")
			case "start":
				err = s.SaveSnapshotStart(ctx, "state", Offset{})
			case "clear":
				err = s.ClearSnapshotProgress(ctx, "state")
			case "scope":
				err = s.SaveSnapshotScopePlan(ctx, "state", "submission", `{}`)
			}
			if !errors.Is(err, ErrWorkerOwnershipLost) || state.writes != 0 {
				t.Fatalf("stale %s result=%v writes=%d", operation, err, state.writes)
			}
		})
	}
}

func TestWorkerFenceRejectsOtherStateKey(t *testing.T) {
	state := &fenceSQLState{owned: true}
	err := fenceTestStore(t, state).SaveOffset(context.Background(), "other-job", Offset{})
	if err == nil || state.writes != 0 {
		t.Fatal("worker wrote another job checkpoint")
	}
}

func TestWorkerFenceRequiresColocatedRegistryAndCompleteToken(t *testing.T) {
	registry := &MySQLJobStore{target: "tcp:localhost:3306/rivus"}
	token := WorkerOwnership{JobID: "job", SubmissionID: "submission", Owner: "worker", StateKey: "state"}
	if _, err := registry.FenceWorkerOffsets(&MySQLOffsetStore{target: registry.target}, token); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.FenceWorkerOffsets(&MySQLOffsetStore{target: "tcp:other:3306/rivus"}, token); err == nil {
		t.Fatal("cross-database fence allowed")
	}
	token.SubmissionID = ""
	if _, err := registry.FenceWorkerOffsets(&MySQLOffsetStore{target: registry.target}, token); err == nil {
		t.Fatal("incomplete generation allowed")
	}
}
