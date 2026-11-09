package iceberg

import (
	"context"
	"fmt"
	"strconv"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
)

func metadataCleanupPolicyConfig(cfg config.IcebergTableMaintenanceConfig) (bool, int) {
	enabled := boolEnv("RIVUS_MAINTENANCE_METADATA_CLEANUP_ENABLED", false)
	if cfg.MetadataCleanupEnabled != nil {
		enabled = *cfg.MetadataCleanupEnabled
	}
	versions := cfg.MetadataPreviousVersionsMax
	if versions == 0 {
		versions = intEnv("RIVUS_MAINTENANCE_METADATA_PREVIOUS_VERSIONS_MAX", 10)
	}
	return enabled, versions
}

func metadataCleanupProperties(versions int) iceberglib.Properties {
	return iceberglib.Properties{
		icetable.MetadataDeleteAfterCommitEnabledKey: "true",
		icetable.MetadataPreviousVersionsMaxKey:      strconv.Itoa(versions),
	}
}

// Reconcile through the table transaction so external writers retain optimistic
// commit protection. Inventory already owns a table lease and has loaded this
// metadata; no additional discovery/listing pass is needed. Compliant tables do
// not commit again. This does not expire snapshots or change data files.
func applyMetadataCleanupPolicy(ctx context.Context, tbl *icetable.Table, settings nativeMaintenanceSettings) (*icetable.Table, error) {
	if !settings.MetadataCleanupEnabled {
		return tbl, nil
	}
	if settings.MetadataPreviousVersionsMax < 1 {
		return nil, fmt.Errorf("metadata_previous_versions_max must be >= 1")
	}
	changes := iceberglib.Properties{}
	for key, value := range metadataCleanupProperties(settings.MetadataPreviousVersionsMax) {
		if tbl.Properties()[key] != value {
			changes[key] = value
		}
	}
	if len(changes) == 0 {
		return tbl, nil
	}
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(changes); err != nil {
		return nil, fmt.Errorf("set metadata cleanup policy: %w", err)
	}
	updated, err := tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("commit metadata cleanup policy: %w", err)
	}
	return updated, nil
}
