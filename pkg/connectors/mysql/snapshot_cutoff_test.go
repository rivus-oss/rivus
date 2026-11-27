package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

func TestSnapshotCutoffDetectsCountBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, now, want string
		counts          []snapshotMonthCount
		excluded        int64
	}{
		{"ancient and invalid dates", "2026-11-27", "2026-10-01", []snapshotMonthCount{{"1970-01-01", 252}, {"0000-00-01", 3}, {"", 2}, {"2026-10-01", 189901}, {"2026-11-01", 2}, {"2042-08-01", 2000000}}, 257},
		{"observed addon distribution", "2026-11-27", "2026-10-01", []snapshotMonthCount{{"2025-06-01", 1}, {"2025-09-01", 361}, {"2026-01-01", 19}, {"2026-03-01", 2}, {"2026-04-01", 2}, {"2026-06-01", 8}, {"2026-07-01", 60}, {"2026-08-01", 141}, {"2026-09-01", 5268}, {"2026-10-01", 119928}, {"2026-11-01", 95197}}, 5862},
		{"january includes december", "2027-01-02", "2026-12-01", []snapshotMonthCount{{"1990-01-01", 3}, {"2026-11-01", 20}, {"2026-12-01", 25000}, {"2027-01-01", 5}}, 23},
		{"retain several populated months", "2027-01-27", "2026-10-01", []snapshotMonthCount{{"2026-09-01", 147}, {"2026-10-01", 25000}, {"2026-11-01", 30000}, {"2026-12-01", 28000}}, 147},
		{"busy month does not discard populated older month", "2026-11-27", "2026-09-01", []snapshotMonthCount{{"2026-08-01", 10}, {"2026-09-01", 12000}, {"2026-10-01", 240000}}, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now, _ := time.Parse("2006-01-02", tc.now)
			got, err := detectSnapshotCutoff(now, tc.counts, config.MySQLSnapshotCutoffConfig{Column: "TglBerangkat"})
			if err != nil || got.Value != tc.want || got.ExcludedRows != tc.excluded {
				t.Fatalf("got=%+v err=%v want=%s excluded=%d", got, err, tc.want, tc.excluded)
			}
		})
	}
}

func TestSnapshotCutoffStopsOnUnclearCounts(t *testing.T) {
	for _, counts := range [][]snapshotMonthCount{
		nil,
		{{"1970-01-01", 252}, {"2026-11-01", 100000}, {"2042-01-01", 1000000}},
		{{"2026-10-01", 9999}, {"2026-11-01", 100000}},
		{{"2026-09-01", 5000}, {"2026-10-01", 11000}},
		{{"2026-10-01", -1}},
	} {
		if _, err := detectSnapshotCutoff(time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC), counts, config.MySQLSnapshotCutoffConfig{Column: "date", LookbackMonths: 1}); err == nil {
			t.Fatalf("accepted ambiguous/invalid counts: %v", counts)
		}
	}
}

type cutoffTestStore struct {
	*testOffsetStore
	plans   map[string]string
	loadErr error
	saveErr error
	intent  bool
}

func (s *cutoffTestStore) GetSnapshotScopePlan(_ context.Context, key, submission string) (string, error) {
	return s.plans[key+":"+submission], s.loadErr
}
func (s *cutoffTestStore) SaveSnapshotScopePlan(_ context.Context, key, submission, value string) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.plans[key+":"+submission] == "" {
		s.plans[key+":"+submission] = value
	}
	return nil
}
func (s *cutoffTestStore) CheckJobStateCleanup(context.Context, string) error {
	if s.intent {
		return meta.ErrJobStateCleanupBlocked
	}
	return nil
}

type cutoffSQL struct {
	reference string
	counts    map[string][]snapshotMonthCount
	queries   int
	err       error
	dataType  string
}
type cutoffConnector struct{ state *cutoffSQL }

func (c cutoffConnector) Connect(context.Context) (driver.Conn, error) {
	return cutoffConn{c.state}, nil
}
func (c cutoffConnector) Driver() driver.Driver { return cutoffDriver{} }

type cutoffDriver struct{}

func (cutoffDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unexpected Open") }

type cutoffConn struct{ state *cutoffSQL }

func (cutoffConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected Prepare") }
func (cutoffConn) Close() error                        { return nil }
func (cutoffConn) Begin() (driver.Tx, error)           { return nil, errors.New("source must only SELECT") }
func (c cutoffConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.queries++
	if c.state.err != nil {
		return nil, c.state.err
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("cutoff query must have a timeout")
	}
	if strings.Contains(query, "CURRENT_DATE()") {
		return &cutoffRows{columns: []string{"month"}, values: [][]driver.Value{{c.state.reference}}}, nil
	}
	if strings.Contains(query, "information_schema.COLUMNS") {
		dataType := c.state.dataType
		if dataType == "" {
			dataType = "datetime"
		}
		return &cutoffRows{columns: []string{"type"}, values: [][]driver.Value{{dataType}}}, nil
	}
	for table, counts := range c.state.counts {
		if strings.Contains(query, "FROM `app`.`"+table+"`") {
			reference, _ := time.Parse("2006-01-02", c.state.reference)
			if len(args) != 1 || args[0].Value != reference.AddDate(0, 1, 0).Format("2006-01-02") || !strings.Contains(query, "MAX_EXECUTION_TIME(30000)") || !strings.Contains(query, "IS NULL") {
				return nil, fmt.Errorf("missing bounded query or invalid-date handling: %s %v", query, args)
			}
			rows := &cutoffRows{columns: []string{"month", "count"}}
			for _, item := range counts {
				rows.values = append(rows.values, []driver.Value{item.Month, item.Rows})
			}
			return rows, nil
		}
	}
	return nil, fmt.Errorf("unexpected query %s", query)
}

type cutoffRows struct {
	columns []string
	values  [][]driver.Value
}

func (r *cutoffRows) Columns() []string { return r.columns }
func (r *cutoffRows) Close() error      { return nil }
func (r *cutoffRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}

func cutoffFixture(t *testing.T) (*Source, connector.JobContext, *cutoffTestStore, *cutoffSQL) {
	t.Helper()
	state := &cutoffSQL{reference: "2026-11-01", counts: map[string][]snapshotMonthCount{
		"orders":   {{"1970-01-01", 252}, {"2026-09-01", 5268}, {"2026-10-01", 119928}},
		"packages": {{"2026-08-01", 12}, {"2026-09-01", 22000}, {"2026-10-01", 25000}},
	}}
	db := sql.OpenDB(cutoffConnector{state})
	t.Cleanup(func() { db.Close() })
	store := &cutoffTestStore{testOffsetStore: &testOffsetStore{}, plans: make(map[string]string)}
	source := &Source{jobID: "job", stateKey: "state", db: db, offsetSto: store, cfg: config.NormalizeMySQLConfig(config.MySQLConfig{
		Tables:       []string{"app.orders", "app.packages"},
		TableConfigs: map[string]config.MySQLTableConfig{"app.*": {SnapshotCutoff: &config.MySQLSnapshotCutoffConfig{Column: "CreatedAt"}, SnapshotKeyColumns: []string{"id"}}},
	})}
	jctx := connector.JobContext{SubmissionID: "submission", SinkType: "iceberg_native", Mode: config.JobModeInitial, SinkConfig: config.IcebergConfig{SnapshotWriteMode: config.IcebergSnapshotAutoCutoffWriteMode, SnapshotReplaceDeleteExecutor: "trino", TrinoDelete: config.IcebergTrinoDeleteConfig{URI: "http://fixture"}}}
	return source, jctx, store, state
}

func TestSnapshotCutoffSharesPerTableDatesAndFreezesOnRestart(t *testing.T) {
	source, jctx, store, state := cutoffFixture(t)
	originalConfig := source.cfg
	resolved, err := source.resolveSnapshotCutoffs(context.Background(), jctx)
	if err != nil {
		t.Fatal(err)
	}
	sink := resolved.(config.IcebergConfig)
	for key, want := range map[string]string{"app.orders": "2026-10-01", "app.packages": "2026-09-01"} {
		table := strings.TrimPrefix(key, "app.")
		filter := sink.SnapshotReplaceFilters[key]
		if filter.Value != want || filter.Column != "createdat" || filter.Op != ">=" {
			t.Fatalf("incorrect target filter: %+v", filter)
		}
		if err := config.ValidateIcebergRebuildFilter(source.snapshotFilterForTable("app", table), &filter); err != nil {
			t.Fatalf("different source/target scopes: %v", err)
		}
		cfg, _ := source.tableConfigForTable("app", table)
		if !reflect.DeepEqual(cfg.SnapshotKeyColumns, []string{"id"}) {
			t.Fatalf("lost pagination columns: %v", cfg.SnapshotKeyColumns)
		}
	}
	if originalConfig.TableConfigs["app.*"].Filter != "" || len(jctx.SinkConfig.(config.IcebergConfig).SnapshotReplaceFilters) != 0 {
		t.Fatal("mutated the stored job configuration")
	}
	// Initial-mode cleanup must not erase the frozen scope.
	if err := source.resetInitialCheckpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.snapshot = &meta.SnapshotState{StartFile: "mysql-bin.000001"}
	state.err = errors.New("database unavailable after restart")
	beforeQueries := state.queries
	restarted := &Source{jobID: "job", stateKey: "state", cfg: originalConfig, offsetSto: store, db: source.db}
	jctx.Mode = config.JobModeResume
	resolvedAgain, err := restarted.resolveSnapshotCutoffs(context.Background(), jctx)
	if err != nil || state.queries != beforeQueries || !reflect.DeepEqual(resolved, resolvedAgain) {
		t.Fatalf("restart changed scope or queried live dates: err=%v before=%d after=%d", err, beforeQueries, state.queries)
	}
	// A changed rule cannot reuse saved snapshot progress.
	changed := originalConfig.TableConfigs["app.*"]
	cutoff := *changed.SnapshotCutoff
	cutoff.MinMonthRows = 500
	changed.SnapshotCutoff = &cutoff
	restarted.cfg = originalConfig
	restarted.cfg.TableConfigs = map[string]config.MySQLTableConfig{"app.*": changed}
	if _, err := restarted.resolveSnapshotCutoffs(context.Background(), jctx); err == nil {
		t.Fatal("accepted different count rules for a resumed snapshot")
	}
}

func TestSnapshotCutoffFailsBeforeApplyingPartialPlan(t *testing.T) {
	for _, fault := range []string{"ambiguous second table", "query failure", "save failure", "load failure", "checkpoint", "progress", "offset", "intent", "manual source filter", "manual target filter", "non-date column", "wrong sink", "unsafe write mode", "unversioned mode", "native executor", "missing submission", "latest without plan"} {
		t.Run(fault, func(t *testing.T) {
			source, jctx, store, state := cutoffFixture(t)
			sink := jctx.SinkConfig.(config.IcebergConfig)
			switch fault {
			case "ambiguous second table":
				state.counts["packages"] = []snapshotMonthCount{{"2026-10-01", 3}}
			case "query failure":
				state.err = errors.New("query failed")
			case "save failure":
				store.saveErr = errors.New("save failed")
			case "load failure":
				store.loadErr = errors.New("load failed")
			case "checkpoint":
				store.snapshot = &meta.SnapshotState{}
			case "progress":
				store.progress = &meta.SnapshotProgress{}
			case "offset":
				store.offset = &meta.Offset{}
			case "intent":
				store.intent = true
			case "manual source filter":
				cfg := source.cfg.TableConfigs["app.*"]
				cfg.Filter = "id > 0"
				source.cfg.TableConfigs["app.*"] = cfg
			case "manual target filter":
				sink.SnapshotReplaceFilters = map[string]config.IcebergSnapshotReplaceFilterConfig{"*.orders": {Column: "other", Op: ">=", Value: "2026-09-01"}}
			case "non-date column":
				state.dataType = "varchar"
			case "wrong sink":
				jctx.SinkType = "doris"
			case "unsafe write mode":
				sink.SnapshotWriteMode = "truncate-append"
			case "unversioned mode":
				sink.SnapshotWriteMode = "auto"
			case "native executor":
				sink.SnapshotReplaceDeleteExecutor = "native"
			case "missing submission":
				jctx.SubmissionID = ""
			case "latest without plan":
				jctx.Mode = config.JobModeLatest
			}
			jctx.SinkConfig = sink
			before, _ := json.Marshal(source.cfg)
			if _, err := source.resolveSnapshotCutoffs(context.Background(), jctx); err == nil {
				t.Fatal("invalid scope was accepted")
			}
			after, _ := json.Marshal(source.cfg)
			if string(before) != string(after) || len(store.plans) != 0 {
				t.Fatal("failure partially applied a cutoff")
			}
		})
	}
}

func TestSnapshotCutoffNewSubmissionDetectsNewMonth(t *testing.T) {
	source, jctx, store, state := cutoffFixture(t)
	original := source.cfg
	if _, err := source.resolveSnapshotCutoffs(context.Background(), jctx); err != nil {
		t.Fatal(err)
	}
	state.reference = "2026-12-01"
	for table := range state.counts {
		state.counts[table] = []snapshotMonthCount{{"2026-10-01", 10}, {"2026-11-01", 120000}}
	}
	jctx.SubmissionID = "new-submission"
	fresh := &Source{jobID: "job", stateKey: "state", cfg: original, db: source.db, offsetSto: store}
	resolved, err := fresh.resolveSnapshotCutoffs(context.Background(), jctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, filter := range resolved.(config.IcebergConfig).SnapshotReplaceFilters {
		if filter.Value != "2026-11-01" {
			t.Fatalf("new submission did not detect a new boundary: %+v", filter)
		}
	}
	if len(store.plans) != 2 {
		t.Fatalf("new plan overwrote old scope: %v", store.plans)
	}
}

func TestSnapshotCutoffRejectsCorruptSavedPlans(t *testing.T) {
	for _, fault := range []string{"version", "missing table", "future cutoff", "changed column", "invalid counts", "invalid reference", "malformed JSON"} {
		t.Run(fault, func(t *testing.T) {
			source, jctx, store, state := cutoffFixture(t)
			original := source.cfg
			if _, err := source.resolveSnapshotCutoffs(context.Background(), jctx); err != nil {
				t.Fatal(err)
			}
			var plan snapshotCutoffPlan
			if err := json.Unmarshal([]byte(store.plans["state:submission"]), &plan); err != nil {
				t.Fatal(err)
			}
			item := plan.Tables["app.orders"]
			switch fault {
			case "version":
				plan.Version = 2
			case "missing table":
				delete(plan.Tables, "app.packages")
			case "future cutoff":
				item.Value = "2042-01-01"
			case "changed column":
				item.Column = "other_column"
			case "invalid counts":
				item.BoundaryMonthRows = 1
			case "invalid reference":
				plan.ReferenceMonth = "2026-11-05"
			}
			plan.Tables["app.orders"] = item
			payload, _ := json.Marshal(plan)
			if fault == "malformed JSON" {
				payload = []byte("{")
			}
			store.plans["state:submission"] = string(payload)
			beforeQueries := state.queries
			restarted := &Source{jobID: "job", stateKey: "state", cfg: original, db: source.db, offsetSto: store}
			if _, err := restarted.resolveSnapshotCutoffs(context.Background(), jctx); err == nil || state.queries != beforeQueries || restarted.snapshotFilterForTable("app", "orders") != "" {
				t.Fatalf("corrupt plan was used or silently recomputed: err=%v", err)
			}
		})
	}
}

func TestSnapshotCutoffCannotBeIgnoredByDirectSourceCalls(t *testing.T) {
	source, _, _, state := cutoffFixture(t)
	ctx := context.Background()
	if err := source.RunFull(ctx, nil, config.JobModeInitial); err == nil {
		t.Fatal("unresolved source started")
	}
	if err := source.RunInitialSnapshotAll(ctx, nil); err == nil {
		t.Fatal("unresolved source read a snapshot")
	}
	if err := source.ResumeInitialSnapshotAll(ctx, nil); err == nil {
		t.Fatal("unresolved source resumed a snapshot")
	}
	if _, err := source.CountRows(ctx, "app", "orders"); err == nil {
		t.Fatal("unresolved source counted unfiltered rows")
	}
	if state.queries != 0 {
		t.Fatal("direct calls queried source data before resolving scope")
	}
}
