package iceberg

import (
	"testing"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

func TestSnapshotCutoffModeRequiresResolvedSourceScope(t *testing.T) {
	for _, fault := range []string{"none", "unresolved", "no filters", "native executor"} {
		t.Run(fault, func(t *testing.T) {
			cfg := config.IcebergConfig{SnapshotWriteMode: config.IcebergSnapshotAutoCutoffWriteMode, SnapshotCutoffResolved: true, SnapshotReplaceDeleteExecutor: "trino", SnapshotReplaceFilters: map[string]config.IcebergSnapshotReplaceFilterConfig{"app.orders": {Column: "created_at", Op: ">=", Value: "2026-10-01"}}}
			switch fault {
			case "unresolved":
				cfg.SnapshotCutoffResolved = false
			case "no filters":
				cfg.SnapshotReplaceFilters = nil
			case "native executor":
				cfg.SnapshotReplaceDeleteExecutor = "native"
			}
			resolved, err := rebuildSnapshotConfig(connector.JobContext{}, cfg)
			if (err == nil) != (fault == "none") {
				t.Fatalf("err=%v", err)
			}
			if err == nil && resolved.SnapshotWriteMode != snapshotWriteModeAuto {
				t.Fatalf("mode was not translated: %s", resolved.SnapshotWriteMode)
			}
		})
	}
	if isValidSnapshotWriteMode(config.IcebergSnapshotAutoCutoffWriteMode) {
		t.Fatal("unresolved cutoff mode must be rejected by the ordinary sink constructor")
	}
	decoded, err := decodeIcebergConfig(map[string]any{"snapshot_write_mode": config.IcebergSnapshotAutoCutoffWriteMode, "SnapshotCutoffResolved": true, "snapshot_cutoff_resolved": true})
	if err != nil || decoded.SnapshotCutoffResolved {
		t.Fatalf("serialized config supplied runtime-only proof: %+v err=%v", decoded, err)
	}
}
