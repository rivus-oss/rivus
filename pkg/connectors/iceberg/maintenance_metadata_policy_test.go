package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
)

func TestMetadataPolicyBoundsCommittedJSONWithoutExpiringData(t *testing.T) {
	ctx := context.Background()
	cat := recoveryCatalog(t)
	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"bronze", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(1), "status": "keep"}})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = tbl.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := tbl.CurrentSnapshot().SnapshotID
	settings := defaultNativeMaintenanceSettings()
	settings.MetadataCleanupEnabled, settings.MetadataPreviousVersionsMax = true, 2
	tbl, err = applyMetadataCleanupPolicy(ctx, tbl, settings)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		tx := tbl.NewTransaction()
		if err := tx.SetProperties(iceberglib.Properties{"test.commit": strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
		tbl, err = tx.Commit(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	files, err := filepath.Glob(filepath.Join(tbl.Location(), "metadata", "*.metadata.json"))
	if err != nil || len(files) == 0 || len(files) > 3 {
		t.Fatalf("metadata JSON not bounded to current plus two previous versions: %d files, %v", len(files), err)
	}
	if _, err := os.Stat(tbl.MetadataLocation()); err != nil {
		t.Fatal("current metadata was removed:", err)
	}
	if tbl.CurrentSnapshot().SnapshotID != snapshot || len(tbl.Metadata().Snapshots()) != 1 {
		t.Fatal("metadata cleanup removed snapshot history")
	}
	plan, err := tbl.Scan().PlanFiles(ctx)
	if err != nil || len(plan) != 1 || plan[0].File.Count() != 1 {
		t.Fatalf("live data no longer readable: %+v, %v", plan, err)
	}
}

func TestMetadataCleanupPolicyIsOptInAndIdempotent(t *testing.T) {
	ctx := context.Background()
	tbl, _ := newEqualityDeltaTestTable(t)
	reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(1), "status": "live"}})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err = tbl.Append(ctx, reader, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	location := tbl.MetadataLocation()
	snapshot := tbl.CurrentSnapshot().SnapshotID
	settings := defaultNativeMaintenanceSettings()
	settings.MetadataCleanupEnabled = false
	unchanged, err := applyMetadataCleanupPolicy(ctx, tbl, settings)
	if err != nil || unchanged != tbl || unchanged.MetadataLocation() != location {
		t.Fatalf("opted-out table was changed: %v", err)
	}
	settings.MetadataCleanupEnabled, settings.MetadataPreviousVersionsMax = true, 10
	updated, err := applyMetadataCleanupPolicy(ctx, tbl, settings)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range metadataCleanupProperties(10) {
		if updated.Properties()[key] != value {
			t.Fatalf("missing policy %s=%s", key, value)
		}
	}
	if updated.CurrentSnapshot().SnapshotID != snapshot || len(updated.Metadata().Snapshots()) != len(tbl.Metadata().Snapshots()) {
		t.Fatal("metadata policy changed data history")
	}
	compliant, err := applyMetadataCleanupPolicy(ctx, updated, settings)
	if err != nil || compliant != updated || compliant.MetadataLocation() != updated.MetadataLocation() {
		t.Fatalf("compliant policy created another metadata commit: %v", err)
	}
	settings.MetadataPreviousVersionsMax = 0
	if _, err := applyMetadataCleanupPolicy(ctx, updated, settings); err == nil {
		t.Fatal("zero retained metadata versions accepted")
	}
}

func TestNewTableMetadataPolicyAndExplicitOptOut(t *testing.T) {
	t.Setenv("RIVUS_MAINTENANCE_METADATA_CLEANUP_ENABLED", "true")
	t.Setenv("RIVUS_MAINTENANCE_METADATA_PREVIOUS_VERSIONS_MAX", "10")
	cfg, err := decodeIcebergConfig(map[string]any{"table_maintenance": map[string]any{"enabled": true}})
	if err != nil {
		t.Fatal(err)
	}
	props := (&Sink{cfg: cfg}).defaultTableProperties()
	if props[icetable.MetadataDeleteAfterCommitEnabledKey] != "true" || props[icetable.MetadataPreviousVersionsMaxKey] != "10" {
		t.Fatalf("new table did not inherit metadata policy: %+v", props)
	}
	no := false
	cfg.TableMaintenance.MetadataCleanupEnabled = &no
	props = (&Sink{cfg: cfg}).defaultTableProperties()
	if _, exists := props[icetable.MetadataDeleteAfterCommitEnabledKey]; exists {
		t.Fatal("explicit opt-out was overwritten by environment")
	}
	settings, err := nativeMaintenanceSettingsFromRaw(map[string]any{"table_maintenance": map[string]any{"enabled": true, "metadata_cleanup_enabled": false}})
	if err != nil || settings.MetadataCleanupEnabled {
		t.Fatalf("worker ignored explicit opt-out: %v", err)
	}
	// Maintenance-disabled sinks preserve manually supplied table properties.
	cfg = config.IcebergConfig{TableProperties: iceberglib.Properties{icetable.MetadataPreviousVersionsMaxKey: "20"}}
	if props := (&Sink{cfg: cfg}).defaultTableProperties(); props[icetable.MetadataPreviousVersionsMaxKey] != "20" {
		t.Fatal("disabled maintenance changed table properties")
	}
}

func TestMetadataPolicyValidationAndRetentionSafety(t *testing.T) {
	settings, err := nativeMaintenanceSettingsFromRaw(map[string]any{"table_maintenance": map[string]any{
		"enabled": true, "metadata_cleanup_enabled": true, "metadata_previous_versions_max": 0,
	}})
	if err == nil {
		t.Fatalf("invalid metadata retention accepted: %+v", settings)
	}
	settings, err = nativeMaintenanceSettingsFromRaw(map[string]any{"table_maintenance": map[string]any{
		"enabled": true, "metadata_cleanup_enabled": true, "metadata_previous_versions_max": 10,
		"native_orphan_interval_seconds": 86400, "native_orphan_inactive_interval_seconds": 604800,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if settings.OrphanMinAge != defaultNativeOrphanAge || settings.SnapshotRetainLast != 10 || settings.SnapshotMaxAge.Hours() != 168 {
		t.Fatalf("faster scheduling changed retention: %+v", settings)
	}
}
