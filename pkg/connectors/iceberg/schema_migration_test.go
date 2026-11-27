package iceberg

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
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
