package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
)

func TestDurableMaintenanceViewIncludesPartitionEligibility(t *testing.T) {
	dsn := os.Getenv("RIVUS_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("RIVUS_TEST_MYSQL_DSN is not set")
	}
	ctx := context.Background()
	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	owner := fmt.Sprintf("eligibility-view-%d", time.Now().UnixNano())
	key := owner + ".bronze.orders"
	now := time.Now().UTC()
	if err := store.UpsertState(ctx, meta.IcebergMaintenanceState{
		TableKey: key, Catalog: owner, Namespace: "bronze", Table: "orders",
		OwnerType: "job", OwnerJobID: owner, SnapshotComplete: true,
	}, now, now, now); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_state WHERE table_key=?", key)
	server := &Server{maintenanceStore: store}
	cfg := &config.JobConfig{ID: owner, Sink: &config.ConnectorSpec{Type: "iceberg_native", Config: map[string]any{
		"table_maintenance": map[string]any{"enabled": true, "data_files_threshold": 50},
	}}}
	for _, counts := range []struct {
		files, groups int
		partitions    []string
	}{{0, 0, []string{"date"}}, {648, 1, []string{}}, {20, 2, nil}} {
		if err := store.UpdateInventory(ctx, key, 42, 648, 648, 6000000, 0, 0, 0, 0, counts.files, 12345, counts.groups, counts.partitions); err != nil {
			t.Fatal(err)
		}
		view, err := server.durableIcebergMaintenanceConfigView(ctx, cfg, owner, false)
		if err != nil {
			t.Fatal(err)
		}
		rows := view["tables"].([]map[string]any)
		if len(rows) != 1 || rows[0]["eligible_small_files"] != 648 || rows[0]["compactable_files"] != counts.files || rows[0]["compactable_bytes"] != int64(12345) || rows[0]["compaction_groups"] != counts.groups {
			t.Fatalf("table totals and planner eligibility were not projected separately: %+v", rows)
		}
		if !reflect.DeepEqual(rows[0]["partition_fields"], counts.partitions) {
			t.Fatalf("partitioned, unpartitioned and unknown tables were conflated: %+v", rows[0])
		}
	}
}

func TestDurableMaintenanceOverallStateReportsStaleInventory(t *testing.T) {
	summary := meta.IcebergMaintenanceOwnerSummary{Tables: 1}
	got := durableMaintenanceOverallState(summary, 1, 0, 0, false, false, true)
	if got != "stale" {
		t.Fatalf("durable maintenance state = %q, want stale", got)
	}
}

func TestDurableMaintenanceOverallStatePreservesErrorPrecedence(t *testing.T) {
	summary := meta.IcebergMaintenanceOwnerSummary{Tables: 1}
	got := durableMaintenanceOverallState(summary, 1, 0, 1, false, false, true)
	if got != "error" {
		t.Fatalf("durable maintenance state = %q, want error", got)
	}
}
