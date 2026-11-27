package config

import (
	"encoding/json"
	"math"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSnapshotCutoffConfigDefaultsAndValidation(t *testing.T) {
	for _, mutate := range []func(*MySQLTableConfig){
		func(c *MySQLTableConfig) { c.Filter = "id > 0" },
		func(c *MySQLTableConfig) { c.SnapshotCutoff.Column = "created_at` OR 1=1" },
		func(c *MySQLTableConfig) { c.SnapshotCutoff.MinMonthRows = -1 },
		func(c *MySQLTableConfig) { c.SnapshotCutoff.MinGrowthRatio = 1 },
		func(c *MySQLTableConfig) { c.SnapshotCutoff.MinGrowthRatio = math.NaN() },
		func(c *MySQLTableConfig) { c.SnapshotCutoff.LookbackMonths = 121 },
		func(c *MySQLTableConfig) { c.SnapshotCutoff.QueryTimeoutSeconds = 601 },
	} {
		cfg := MySQLTableConfig{SnapshotCutoff: &MySQLSnapshotCutoffConfig{Column: "CreatedAt"}}
		mutate(&cfg)
		if err := ValidateMySQLSnapshotCutoff(cfg); err == nil {
			t.Fatalf("accepted invalid configuration: %+v", cfg)
		}
	}
	var cfg MySQLConfig
	if err := yaml.Unmarshal([]byte("tables: [app.orders]\ntable_configs:\n  'APP.*':\n    snapshot_cutoff:\n      column: ' CreatedAt '\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	cfg = NormalizeMySQLConfig(cfg)
	cutoff := cfg.TableConfigs["app.*"].SnapshotCutoff
	if cutoff == nil || cutoff.Column != "CreatedAt" || cutoff.MinMonthRows != 10000 || cutoff.MinGrowthRatio != 10 || cutoff.LookbackMonths != 24 || cutoff.QueryTimeoutSeconds != 30 {
		t.Fatalf("incorrect defaults: %+v", cutoff)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded MySQLConfig
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.TableConfigs["app.*"].SnapshotCutoff; got == nil || *got != *cutoff {
		t.Fatalf("JSON did not preserve count rule: %+v", got)
	}
}

func TestSnapshotCutoffRebuildAdmissionRequiresGeneratedScope(t *testing.T) {
	for _, fault := range []string{"none", "manual source", "manual sink", "full reset", "invalid rule"} {
		t.Run(fault, func(t *testing.T) {
			table := map[string]any{"snapshot_cutoff": map[string]any{"column": "created_at"}}
			sink := map[string]any{"snapshot_write_mode": IcebergSnapshotAutoCutoffWriteMode, "snapshot_replace_delete_executor": "trino", "trino_delete": map[string]any{"uri": "http://fixture"}, "snapshot_truncate_tables": []string{"app.orders"}}
			switch fault {
			case "manual source":
				table["filter"] = "id > 0"
			case "manual sink":
				sink["snapshot_replace_filters"] = map[string]any{"app.orders": map[string]any{"column": "created_at", "op": ">=", "value": "2026-09-01"}}
			case "full reset":
				sink["snapshot_replace_delete_executor"] = "native"
			case "invalid rule":
				table["snapshot_cutoff"].(map[string]any)["lookback_months"] = -1
			}
			cfg := &JobConfig{Source: &ConnectorSpec{Type: "mysql", Config: map[string]any{"tables": []string{"app.orders"}, "table_configs": map[string]any{"app.*": table}}}, Sink: &ConnectorSpec{Type: "iceberg_native", Config: sink}}
			err := ValidateIcebergSnapshotRebuild(cfg)
			if (err == nil) != (fault == "none") {
				t.Fatalf("admission err=%v", err)
			}
		})
	}
}
