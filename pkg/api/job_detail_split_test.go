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

func TestDurableMaintenanceViewIncludesCatalogAndPartitionEligibility(t *testing.T) {
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
		if rows[0]["catalog"] != owner || rows[0]["identifier"] != key {
			t.Fatalf("table catalog identity was lost: %+v", rows[0])
		}
		cleanup := rows[0]["cleanup"].([]maintenanceCleanupView)
		if len(cleanup) != 2 || cleanup[0].LastSuccessAt != nil || cleanup[1].LastResult != nil {
			t.Fatalf("tables without cleanup history must still expose both schedules: %+v", cleanup)
		}
	}

	// A catalog-wide monitor can own identically named tables in different
	// catalogs, as well as tables whose namespaces have multiple levels.
	identities := map[string]meta.IcebergMaintenanceState{
		key: {Catalog: owner, Namespace: "bronze", Table: "orders"},
	}
	for _, namespace := range []string{"bronze", "parent.bronze"} {
		state := meta.IcebergMaintenanceState{
			Catalog: "other-" + owner, Namespace: namespace, Table: "orders",
			OwnerType: "monitor", OwnerJobID: owner, SnapshotComplete: true,
		}
		state.TableKey = state.Catalog + "." + namespace + ".orders"
		if err := store.UpsertState(ctx, state, now, now, now); err != nil {
			t.Fatal(err)
		}
		defer db.ExecContext(ctx, "DELETE FROM iceberg_maintenance_state WHERE table_key=?", state.TableKey)
		identities[state.TableKey] = state
	}
	view, err := server.durableIcebergMaintenanceConfigView(ctx, cfg, owner, false)
	if err != nil {
		t.Fatal(err)
	}
	rows := view["tables"].([]map[string]any)
	if len(rows) != len(identities) {
		t.Fatalf("table count = %d, want %d", len(rows), len(identities))
	}
	for _, row := range rows {
		identifier, _ := row["identifier"].(string)
		identity, ok := identities[identifier]
		if !ok || row["catalog"] != identity.Catalog || row["namespace"] != identity.Namespace || row["table"] != identity.Table {
			t.Fatalf("catalogs or nested namespaces were conflated: %+v", row)
		}
		delete(identities, identifier)
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
