package iceberg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/model"
)

func TestAutoMigrationConvertsSupportedTypes(t *testing.T) {
	day := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		from  iceberglib.Type
		value any
		to    string
		want  any
	}{
		{"text to integer", iceberglib.PrimitiveTypes.String, "0018", "int", int64(18)},
		{"double to text", iceberglib.PrimitiveTypes.Float64, float64(18.5), "varchar(255)", "18.5"},
		{"double to integer", iceberglib.PrimitiveTypes.Float64, float64(18), "int", int64(18)},
		{"double to float", iceberglib.PrimitiveTypes.Float64, float64(1.5), "float", float64(1.5)},
		{"decimal to text", iceberglib.DecimalTypeOf(6, 2), "18.25", "varchar(255)", "18.25"},
		{"text to decimal", iceberglib.PrimitiveTypes.String, "18.25", "decimal(6,2)", "18.25"},
		{"decimal scale", iceberglib.DecimalTypeOf(6, 2), "18.25", "decimal(7,3)", "18.25"},
		{"boolean to text", iceberglib.PrimitiveTypes.Bool, true, "varchar(255)", "1"},
		{"boolean to integer", iceberglib.PrimitiveTypes.Bool, true, "int", int64(1)},
		{"text to boolean", iceberglib.PrimitiveTypes.String, "1", "tinyint(1)", true},
		{"date to text", iceberglib.PrimitiveTypes.Date, day, "varchar(255)", "2026-11-27"},
		{"text to date", iceberglib.PrimitiveTypes.String, "2026-11-27", "date", day},
		{"timestamp to date", iceberglib.PrimitiveTypes.Timestamp, day, "date", day},
		{"binary to text", iceberglib.PrimitiveTypes.Binary, []byte("hello"), "varchar(255)", "hello"},
		{"text to binary", iceberglib.PrimitiveTypes.String, "hello", "varbinary(20)", []byte("hello")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink, state := integerStringTestSink(t, tc.from, []map[string]interface{}{{"id": int64(1), "LayoutPromo": tc.value}})
			err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: "ALTER TABLE promos MODIFY COLUMN LayoutPromo " + tc.to + " NULL"})
			if err != nil {
				t.Fatal(err)
			}
			rows := integerStringTestRows(t, state.table)
			if len(rows) != 1 || !reflect.DeepEqual(rows[1]["LayoutPromo"], tc.want) {
				t.Fatalf("result = %#v, want %#v", rows, tc.want)
			}
		})
	}
}

func TestAutoMigrationRejectsInvalidOrLossyValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		from  iceberglib.Type
		value any
		to    string
	}{
		{"invalid number", iceberglib.PrimitiveTypes.String, "18,19", "int"},
		{"integer overflow", iceberglib.PrimitiveTypes.Int64, int64(2147483648), "int"},
		{"fractional integer", iceberglib.PrimitiveTypes.Float64, float64(18.5), "int"},
		{"float precision", iceberglib.PrimitiveTypes.Float64, float64(0.1), "float"},
		{"nonbinary boolean", iceberglib.PrimitiveTypes.Int32, int32(2), "tinyint(1)"},
		{"decimal truncation", iceberglib.DecimalTypeOf(6, 2), "18.25", "decimal(6,1)"},
		{"timestamp truncation", iceberglib.PrimitiveTypes.Timestamp, time.Date(2026, 11, 27, 12, 30, 0, 0, time.UTC), "date"},
		{"invalid date", iceberglib.PrimitiveTypes.String, "not-a-date", "date"},
		{"invalid UTF8", iceberglib.PrimitiveTypes.Binary, []byte{0xff}, "varchar(255)"},
		{"unsupported units", iceberglib.PrimitiveTypes.Int64, int64(18), "datetime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink, state := integerStringTestSink(t, tc.from, []map[string]interface{}{{"id": int64(1), "LayoutPromo": tc.value}})
			before := state.table.CurrentSnapshot().SnapshotID
			store := newGuardTestStore()
			sink.offsetSto, sink.stateKey = store, "migration-state"
			err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: "ALTER TABLE promos MODIFY COLUMN LayoutPromo " + tc.to + " NULL"})
			if err == nil || strings.Contains(err.Error(), "outcome UNKNOWN") {
				t.Fatalf("invalid conversion error = %v", err)
			}
			loaded, loadErr := sink.catalog.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			field, _ := loaded.Schema().FindFieldByName("LayoutPromo")
			if !field.Type.Equals(tc.from) || loaded.CurrentSnapshot().SnapshotID != before || len(store.intents) != 0 || store.saveCount != 0 {
				t.Fatalf("failed migration changed schema, data or checkpoint: field=%v intents=%v saves=%d", field, store.intents, store.saveCount)
			}
		})
	}
}

func TestAutoMigrationRequiresOptInAndTableScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy config.IcebergAutoMigrationConfig
		unsafe bool
		want   string
	}{
		{"disabled", config.IcebergAutoMigrationConfig{}, false, "automatic migration is disabled"},
		{"unsafe flag is not migration", config.IcebergAutoMigrationConfig{}, true, "automatic migration is disabled"},
		{"wrong table", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app.other"}}, false, "not listed"},
		{"missing table", config.IcebergAutoMigrationConfig{Enabled: true}, false, "explicit source tables"},
		{"size limit", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app.promos"}, MaxTableBytes: 1}, false, "max_table_bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
			sink.cfg.AutoMigration = normalizeAutoMigrationConfig(tc.policy)
			sink.cfg.AllowUnsafeTypeChanges = tc.unsafe
			before := state.table.CurrentSnapshot().SnapshotID
			err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
			loaded, err := sink.catalog.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
			if err != nil {
				t.Fatal(err)
			}
			if loaded.CurrentSnapshot().SnapshotID != before || integerStringTestRows(t, loaded)[1]["LayoutPromo"] != int64(18) {
				t.Fatal("blocked migration changed committed data")
			}
		})
	}
}

func TestAutoMigrationKeepsNativePromotionWithoutOptIn(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
	sink.cfg.AutoMigration.Enabled = false
	before := state.table.CurrentSnapshot().SnapshotID
	field, _ := state.table.Schema().FindFieldByName("LayoutPromo")
	if err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: "ALTER TABLE promos MODIFY COLUMN LayoutPromo bigint NULL"}); err != nil {
		t.Fatal(err)
	}
	after, _ := state.table.Schema().FindFieldByName("LayoutPromo")
	if after.ID != field.ID || !after.Type.Equals(iceberglib.PrimitiveTypes.Int64) || state.table.CurrentSnapshot().SnapshotID != before {
		t.Fatal("native promotion replaced a field or rewrote data")
	}
}

func TestAutoMigrationConfigurationValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cfg       config.IcebergAutoMigrationConfig
		wantError bool
	}{
		{"default disabled", config.IcebergAutoMigrationConfig{}, false},
		{"explicit table", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{" app.promos ", "APP.PROMOS"}}, false},
		{"wildcard", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app.*"}}, true},
		{"short name", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"promos"}}, true},
		{"empty table", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{" "}}, true},
		{"negative size", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app.promos"}, MaxTableBytes: -1}, true},
		{"negative timeout", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app.promos"}, TimeoutSeconds: -1}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := normalizeAutoMigrationConfig(tc.cfg)
			if err := validateAutoMigrationConfig(cfg); (err != nil) != tc.wantError {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestAutoMigrationResolvesSourceTablesWhenOverrideOmitted(t *testing.T) {
	refs := []connector.TableRef{{Schema: "app_b", Table: "orders"}, {Schema: " App_A ", Table: " Orders "}, {Schema: "APP_A", Table: "ORDERS"}}
	for _, tc := range []struct {
		name      string
		cfg       config.IcebergAutoMigrationConfig
		refs      []connector.TableRef
		want      []string
		wantError bool
	}{
		{"source selection", config.IcebergAutoMigrationConfig{Enabled: true}, refs, []string{"app_a.orders", "app_b.orders"}, false},
		{"explicit smaller scope", config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app_b.orders"}}, refs, []string{"app_b.orders"}, false},
		{"disabled without source", config.IcebergAutoMigrationConfig{}, nil, []string{}, false},
		{"no selection", config.IcebergAutoMigrationConfig{Enabled: true}, nil, nil, true},
		{"unexpanded wildcard", config.IcebergAutoMigrationConfig{Enabled: true}, []connector.TableRef{{Schema: "app_*", Table: "orders"}}, nil, true},
		{"missing schema", config.IcebergAutoMigrationConfig{Enabled: true}, []connector.TableRef{{Table: "orders"}}, nil, true},
		{"missing table", config.IcebergAutoMigrationConfig{Enabled: true}, []connector.TableRef{{Schema: "app"}}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := resolveAutoMigrationTables(tc.cfg, tc.refs)
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v cfg=%+v", err, resolved)
			}
			if err == nil && !reflect.DeepEqual(resolved.Tables, tc.want) {
				t.Fatalf("resolved=%v want=%v", resolved.Tables, tc.want)
			}
		})
	}
}

func TestAutoMigrationFactoryInheritsExpandedSourceSelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/config" {
			t.Errorf("unexpected catalog request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
	}))
	defer server.Close()
	reg := connector.NewRegistry()
	Register(reg)
	rolling := false
	cfg := config.IcebergConfig{RestURI: server.URL, Warehouse: "fixture", SnapshotWriteMode: "append", SnapshotRollingEnabled: &rolling, AutoMigration: config.IcebergAutoMigrationConfig{Enabled: true}}
	jctx := connector.JobContext{JobID: "migration-source-scope", SourceType: "mysql", SourceTables: []connector.TableRef{{Schema: "asmat_trikusuma", Table: "tbl_md_promo"}, {Schema: "asmat_wbshuttle", Table: "tbl_md_promo"}}, Retry: config.RetryPolicy{MaxAttempts: 1}}
	created, err := reg.NewSink("iceberg_native", jctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	policy := created.(*Sink).cfg.AutoMigration
	if !reflect.DeepEqual(policy.Tables, []string{"asmat_trikusuma.tbl_md_promo", "asmat_wbshuttle.tbl_md_promo"}) || policy.MaxTableBytes != defaultMigrationMaxBytes || policy.TimeoutSeconds != defaultMigrationTimeout {
		t.Fatalf("factory did not bind source tables and limits: %+v", policy)
	}
	if cfg.AutoMigration.Tables != nil {
		t.Fatal("factory mutated the stored migration configuration")
	}
	if _, err := reg.NewSink("iceberg_native", connector.JobContext{}, cfg); err == nil || !strings.Contains(err.Error(), "resolved source tables") {
		t.Fatalf("missing source selection was accepted: %v", err)
	}
}

func TestAutoMigrationRewritesUsingInferredSourceScope(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
	policy, err := resolveAutoMigrationTables(config.IcebergAutoMigrationConfig{Enabled: true}, []connector.TableRef{{Schema: "app", Table: "promos"}})
	if err != nil {
		t.Fatal(err)
	}
	sink.cfg.AutoMigration = policy
	if err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL}); err != nil {
		t.Fatal(err)
	}
	if got := integerStringTestRows(t, state.table)[1]["LayoutPromo"]; got != "18" {
		t.Fatalf("inferred migration did not preserve data: %v", got)
	}
}

func TestAutoMigrationStartupKeepsCompatibleExistingTypes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		idType          string
		valueType       string
		valueColumnType string
	}{
		{"wider identifier and data columns", "int", "int", "int(11)"},
		{"legacy tinyint identifier and data columns", "tinyint", "tinyint", "tinyint(1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int64, []map[string]interface{}{{"id": int64(3), "LayoutPromo": int64(18)}})
			source := *state.sourceSchema
			source.Columns = append([]model.TableColumn(nil), source.Columns...)
			source.Columns[0].DataType = tc.idType
			source.Columns[0].ColumnType = "int(11)"
			if tc.idType == "tinyint" {
				source.Columns[0].ColumnType = "tinyint(1)"
			}
			source.Columns[1].DataType = tc.valueType
			source.Columns[1].ColumnType = tc.valueColumnType
			beforeSnapshot := state.table.CurrentSnapshot().SnapshotID
			beforeSchema := state.table.Schema()
			// An unnecessary rewrite would exceed this cap. Startup must keep
			// the existing IDs and values without opening or rewriting data files.
			sink.cfg.AutoMigration.MaxTableBytes = 1
			loaded, err := sink.syncTableSchema(context.Background(), state.table, "bronze", "promos", &source, []string{"id"})
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.Schema().Equals(beforeSchema) || loaded.CurrentSnapshot().SnapshotID != beforeSnapshot {
				t.Fatal("startup replaced a compatible field or rewrote the table")
			}
			if got := integerStringTestRows(t, loaded)[3]["LayoutPromo"]; got != int64(18) {
				t.Fatalf("startup changed a legacy numeric value: %v", got)
			}
		})
	}
}

func TestAutoMigrationStartupHonorsPreparationTimeout(t *testing.T) {
	for _, tc := range []struct {
		cfg  config.IcebergAutoMigrationConfig
		want time.Duration
	}{
		{config.IcebergAutoMigrationConfig{}, 45 * time.Second},
		{config.IcebergAutoMigrationConfig{Enabled: true}, 900 * time.Second},
		{config.IcebergAutoMigrationConfig{Enabled: true, TimeoutSeconds: 120}, 120 * time.Second},
		{config.IcebergAutoMigrationConfig{Enabled: true, TimeoutSeconds: 10}, 45 * time.Second},
	} {
		sink := &Sink{cfg: config.IcebergConfig{AutoMigration: tc.cfg}}
		if got := sink.TablePreparationTimeout(); got != tc.want {
			t.Fatalf("timeout=%s want=%s", got, tc.want)
		}
	}
}
