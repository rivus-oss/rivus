package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

type scopeSQLState struct {
	plans map[string]string
	err   error
}
type scopeSQLConnector struct{ state *scopeSQLState }

func (c scopeSQLConnector) Connect(context.Context) (driver.Conn, error) {
	return scopeSQLConn{c.state}, nil
}
func (scopeSQLConnector) Driver() driver.Driver { return deleteFailureDriver{} }

type scopeSQLConn struct{ state *scopeSQLState }

func (scopeSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (scopeSQLConn) Close() error              { return nil }
func (scopeSQLConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected begin") }
func (c scopeSQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.state.err != nil {
		return nil, c.state.err
	}
	if strings.Contains(query, "FROM job_snapshot_scopes WHERE state_key = ? AND submission_id = ?") && len(args) == 2 {
		return &deleteIntentRows{intent: c.state.plans[args[0].Value.(string)+":"+args[1].Value.(string)]}, nil
	}
	if strings.Contains(query, "FROM iceberg_commit_intents") {
		return &deleteIntentRows{}, nil
	}
	return nil, errors.New("unexpected query: " + query)
}
func (c scopeSQLConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.state.err != nil {
		return nil, c.state.err
	}
	if strings.Contains(query, "INSERT INTO job_snapshot_scopes") {
		if !strings.Contains(query, "ON DUPLICATE KEY UPDATE state_key = VALUES(state_key)") || len(args) != 3 {
			return nil, errors.New("scope insert must preserve the first plan")
		}
		key := args[0].Value.(string) + ":" + args[1].Value.(string)
		if c.state.plans[key] == "" {
			c.state.plans[key] = args[2].Value.(string)
		}
		return driver.RowsAffected(1), nil
	}
	if strings.Contains(query, "DELETE FROM job_snapshot_scopes") {
		return nil, errors.New("checkpoint reset must preserve scopes")
	}
	if strings.HasPrefix(query, "DELETE FROM job_") {
		return driver.RowsAffected(1), nil
	}
	return nil, errors.New("unexpected write: " + query)
}

func TestSnapshotScopeStorageIsImmutableAndSubmissionBound(t *testing.T) {
	state := &scopeSQLState{plans: make(map[string]string)}
	db := sql.OpenDB(scopeSQLConnector{state})
	t.Cleanup(func() { db.Close() })
	store := &MySQLOffsetStore{db: db, runner: db}
	ctx := context.Background()
	for _, row := range []struct{ key, submission, payload string }{
		{"state", "one", `{"cutoff":"2026-09-01"}`},
		{"state", "one", `{"cutoff":"2026-10-01"}`},
		{"state", "two", `{"cutoff":"2026-10-01"}`},
		{"other", "one", `{"cutoff":"2026-11-01"}`},
	} {
		if err := store.SaveSnapshotScopePlan(ctx, row.key, row.submission, row.payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteJobState(ctx, "state"); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ key, submission, want string }{
		{"state", "one", `{"cutoff":"2026-09-01"}`},
		{"state", "two", `{"cutoff":"2026-10-01"}`},
		{"other", "one", `{"cutoff":"2026-11-01"}`},
		{"missing", "one", ""},
	} {
		got, err := store.GetSnapshotScopePlan(ctx, row.key, row.submission)
		if err != nil || got != row.want {
			t.Fatalf("key=%s submission=%s got=%s err=%v", row.key, row.submission, got, err)
		}
	}
	state.err = errors.New("metadata unavailable")
	if err := store.SaveSnapshotScopePlan(ctx, "state", "three", `{}`); !errors.Is(err, state.err) {
		t.Fatalf("lost save error: %v", err)
	}
	if _, err := store.GetSnapshotScopePlan(ctx, "state", "one"); !errors.Is(err, state.err) {
		t.Fatalf("lost read error: %v", err)
	}
}

func TestSnapshotScopeFenceRejectsDifferentSubmission(t *testing.T) {
	state := &fenceSQLState{owned: true}
	store := fenceTestStore(t, state)
	if err := store.SaveSnapshotScopePlan(context.Background(), "state", "old-submission", `{}`); !errors.Is(err, ErrWorkerOwnershipLost) || state.writes != 0 {
		t.Fatalf("wrong submission wrote scope: err=%v writes=%d", err, state.writes)
	}
}
