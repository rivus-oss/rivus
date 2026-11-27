package iceberg

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

func Register(reg *connector.Registry) {
	reg.RegisterSink("iceberg_native", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
		icfg, err := decodeIcebergConfig(cfg)
		if err != nil {
			return nil, err
		}
		icfg.AutoMigration, err = resolveAutoMigrationTables(icfg.AutoMigration, jctx.SourceTables)
		if err != nil {
			return nil, err
		}
		native := icfg.SnapshotWriteMode == config.IcebergRebuildScopedNativeWriteMode
		icfg, err = rebuildSnapshotConfig(jctx, icfg)
		if err != nil {
			return nil, err
		}

		scopedToken := ""
		if jctx.SubmissionID != "" && jctx.Metadata[config.IcebergRebuildScopedKey] == jctx.SubmissionID {
			scopedToken = jctx.SubmissionID
		}
		sink, err := newSinkWithRebuildMode(jctx.JobID, jctx.MetaKey, jctx.JobName, icfg, jctx.Retry, jctx.MetaStore, jctx.ReportProgress, scopedToken, native)
		if err != nil {
			return nil, err
		}
		sink.authoritativeRebuild = jctx.SubmissionID != "" && jctx.Metadata[config.IcebergRebuildFullResetKey] == jctx.SubmissionID
		ownership, err := newIcebergMaintenanceOwnership(jctx, icfg)
		if err != nil {
			return nil, err
		}
		sink.maintenanceOwnership = ownership
		if sink.maintenanceSignals != nil {
			sink.maintenanceSignals.setInitialSnapshotComplete(maintenanceStartsComplete(jctx.Mode))
		}
		return sink, nil
	})

}

func rebuildSnapshotConfig(jctx connector.JobContext, cfg config.IcebergConfig) (config.IcebergConfig, error) {
	if cfg.SnapshotWriteMode == config.IcebergSnapshotAutoCutoffWriteMode {
		if !cfg.SnapshotCutoffResolved || len(cfg.SnapshotReplaceFilters) == 0 || cfg.SnapshotReplaceDeleteExecutor != "trino" {
			return cfg, fmt.Errorf("%s requires source-resolved durable snapshot cutoffs", config.IcebergSnapshotAutoCutoffWriteMode)
		}
		cfg.SnapshotWriteMode = snapshotWriteModeAuto
	}
	scoped := cfg.SnapshotWriteMode == config.IcebergRebuildScopedWriteMode || cfg.SnapshotWriteMode == config.IcebergRebuildScopedNativeWriteMode
	scopedBound := jctx.SubmissionID != "" && jctx.Metadata[config.IcebergRebuildScopedKey] == jctx.SubmissionID
	if scoped != scopedBound || (scoped && jctx.Metadata[config.IcebergRebuildFullResetKey] != "") {
		return cfg, fmt.Errorf("scoped rebuild protocol and submission marker must match")
	}
	if scoped {
		if _, ok := jctx.MetaStore.(meta.IcebergTrinoQueryStore); !ok {
			return cfg, fmt.Errorf("scoped rebuild requires durable Trino query identities")
		}
		if _, ok := jctx.MetaStore.(meta.OwnershipChecker); !ok {
			return cfg, fmt.Errorf("scoped rebuild requires worker ownership fencing")
		}
		cfg.SnapshotWriteMode = snapshotWriteModeAuto
		rolling := false
		cfg.SnapshotRollingEnabled = &rolling
	}
	bound := jctx.SubmissionID != "" && jctx.Metadata[config.IcebergRebuildFullResetKey] == jctx.SubmissionID
	protocol := cfg.SnapshotWriteMode == config.IcebergRebuildSnapshotWriteMode
	if protocol != bound {
		return cfg, fmt.Errorf("snapshot rebuild BLOCKED: protocol mode and current submission marker must match")
	}
	if protocol {
		if jctx.MetaStore == nil {
			return cfg, fmt.Errorf("snapshot rebuild BLOCKED: durable metadata required")
		}
		cfg.SnapshotWriteMode = snapshotWriteModeTruncateAppend
	}
	return cfg, nil
}

func maintenanceStartsComplete(mode config.JobMode) bool {
	switch mode {
	case config.JobModeResume, config.JobModeLatest, config.JobModeLatestOffset:
		return true
	default:
		return false
	}
}

func decodeIcebergConfig(v any) (config.IcebergConfig, error) {
	switch t := v.(type) {
	case config.IcebergConfig:
		return normalizeIcebergConfig(t), nil
	case *config.IcebergConfig:
		if t == nil {
			return config.IcebergConfig{}, fmt.Errorf("iceberg_native config is nil")
		}
		return normalizeIcebergConfig(*t), nil
	}

	b, err := yaml.Marshal(v)
	if err != nil {
		return config.IcebergConfig{}, err
	}

	var c config.IcebergConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return config.IcebergConfig{}, err
	}

	return normalizeIcebergConfig(c), nil
}

func normalizeIcebergConfig(c config.IcebergConfig) config.IcebergConfig {
	c.AutoMigration = normalizeAutoMigrationConfig(c.AutoMigration)
	c.CatalogURI = strings.TrimRight(strings.TrimSpace(c.CatalogURI), "/")
	c.Warehouse = strings.TrimRight(strings.TrimSpace(c.Warehouse), "/")
	c.Credential = strings.TrimSpace(c.Credential)
	c.OAuthToken = strings.TrimSpace(c.OAuthToken)
	c.Scope = strings.TrimSpace(c.Scope)
	c.Prefix = strings.Trim(strings.TrimSpace(c.Prefix), "/")
	c.DefaultNamespace = strings.TrimSpace(c.DefaultNamespace)
	c.SnapshotWriteMode = normalizeSnapshotWriteMode(c.SnapshotWriteMode)
	c.SnapshotReplaceDeleteExecutor = normalizeSnapshotReplaceDeleteExecutor(c.SnapshotReplaceDeleteExecutor)
	c.CDCDeleteExecutor = normalizeSnapshotReplaceDeleteExecutor(c.CDCDeleteExecutor)
	c.TableMaintenance.SparkRESTURI = strings.TrimRight(strings.TrimSpace(c.TableMaintenance.SparkRESTURI), "/")
	c.TableMaintenance.RunnerURI = strings.TrimRight(strings.TrimSpace(c.TableMaintenance.RunnerURI), "/")
	c.TableMaintenance.RunnerAPIToken = strings.TrimSpace(c.TableMaintenance.RunnerAPIToken)
	c.TableMaintenance.RunnerResourceProfile = strings.ToLower(strings.TrimSpace(c.TableMaintenance.RunnerResourceProfile))
	c.TableMaintenance.SparkMaster = strings.TrimSpace(c.TableMaintenance.SparkMaster)
	c.TableMaintenance.AppResource = strings.TrimSpace(c.TableMaintenance.AppResource)
	c.TableMaintenance.MainClass = strings.TrimSpace(c.TableMaintenance.MainClass)
	c.TableMaintenance.ClientSparkVersion = strings.TrimSpace(c.TableMaintenance.ClientSparkVersion)
	c.TableMaintenance.CatalogName = strings.TrimSpace(c.TableMaintenance.CatalogName)
	c.TableMaintenance.RESTAuthHeader = strings.TrimSpace(c.TableMaintenance.RESTAuthHeader)
	c.TableMaintenance.Executor = normalizeMaintenanceExecutor(c.TableMaintenance.Executor)
	c.TableMaintenance.WorkerTempDirectory = strings.TrimSpace(c.TableMaintenance.WorkerTempDirectory)
	c.SnapshotSpoolDirectory = strings.TrimSpace(c.SnapshotSpoolDirectory)
	if c.TableMaintenance.Enabled || c.TableMaintenance.NativeEnabled {
		if c.TableMaintenance.RunnerResourceProfile == "" {
			c.TableMaintenance.RunnerResourceProfile = "small"
		}
		if c.TableMaintenance.PollIntervalSeconds <= 0 {
			c.TableMaintenance.PollIntervalSeconds = 60
		}
		if c.TableMaintenance.MaxConcurrentJobs <= 0 {
			c.TableMaintenance.MaxConcurrentJobs = 1
		}
		if c.TableMaintenance.DataFilesThreshold == 0 {
			c.TableMaintenance.DataFilesThreshold = 200
		}
		if c.TableMaintenance.EqualityDeleteFilesThreshold == 0 {
			c.TableMaintenance.EqualityDeleteFilesThreshold = 50
		}
		if c.TableMaintenance.PositionDeleteFilesThreshold == 0 {
			c.TableMaintenance.PositionDeleteFilesThreshold = 25
		}
		if c.TableMaintenance.SmallFileSizeBytes == 0 {
			c.TableMaintenance.SmallFileSizeBytes = 64 * 1024 * 1024
		}
		if c.TableMaintenance.SmallFilesMinCount == 0 {
			c.TableMaintenance.SmallFilesMinCount = 10
		}
		if c.TableMaintenance.SmallFilesMinTotalBytes == 0 {
			c.TableMaintenance.SmallFilesMinTotalBytes = 256 * 1024 * 1024
		}
		if c.TableMaintenance.NativeSignalDelaySeconds <= 0 {
			c.TableMaintenance.NativeSignalDelaySeconds = 5 * 60
		}
		if c.TableMaintenance.NativeIdleCheckIntervalSeconds <= 0 {
			c.TableMaintenance.NativeIdleCheckIntervalSeconds = 7 * 24 * 60 * 60
		}
		if c.TableMaintenance.NativeOrphanIntervalSeconds <= 0 {
			c.TableMaintenance.NativeOrphanIntervalSeconds = int(durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INTERVAL_SECONDS", defaultNativeOrphanInterval).Seconds())
		}
		if c.TableMaintenance.NativeOrphanInactiveIntervalSeconds <= 0 {
			inactiveDefault := max(defaultNativeOrphanInactiveInterval, time.Duration(c.TableMaintenance.NativeOrphanIntervalSeconds)*time.Second)
			c.TableMaintenance.NativeOrphanInactiveIntervalSeconds = int(durationEnv("RIVUS_MAINTENANCE_NATIVE_ORPHAN_INACTIVE_INTERVAL_SECONDS", inactiveDefault).Seconds())
		}
		if c.TableMaintenance.NativeMaxSelectedInputBytes <= 0 {
			c.TableMaintenance.NativeMaxSelectedInputBytes = defaultNativeMaxInputBytes
		}
		if c.TableMaintenance.NativeMaxSelectedFiles <= 0 {
			c.TableMaintenance.NativeMaxSelectedFiles = defaultNativeMaxInputFiles
		}
		if c.TableMaintenance.NativeMaxEqualityDeleteFiles <= 0 {
			c.TableMaintenance.NativeMaxEqualityDeleteFiles = defaultNativeMaxEqualityDeleteFiles
		}
		if c.TableMaintenance.NativeTargetFileSizeBytes <= 0 {
			c.TableMaintenance.NativeTargetFileSizeBytes = defaultNativeTargetBytes
		}
		if c.TableMaintenance.NativeScanConcurrency <= 0 {
			c.TableMaintenance.NativeScanConcurrency = 1
		}
		if c.TableMaintenance.NativeTimeoutSeconds <= 0 {
			c.TableMaintenance.NativeTimeoutSeconds = int(defaultNativeTimeout.Seconds())
		}
		if c.TableMaintenance.WorkerTempDirectory == "" {
			c.TableMaintenance.WorkerTempDirectory = "/tmp/rivus-maintenance"
		}
	}
	if c.TableMaintenance.Enabled {
		c.TableMaintenance.CompactOptions = normalizeAutomaticCompactOptions(c.TableMaintenance.CompactOptions)
		if c.TableMaintenance.ExpireSnapshotsIntervalSeconds == 0 {
			c.TableMaintenance.ExpireSnapshotsIntervalSeconds = 6 * 60 * 60
		}
		if c.TableMaintenance.ExpireSnapshotsOlderThanHours <= 0 {
			c.TableMaintenance.ExpireSnapshotsOlderThanHours = 7 * 24
		}
		if c.TableMaintenance.ExpireSnapshotsRetainLast <= 0 {
			c.TableMaintenance.ExpireSnapshotsRetainLast = 10
		}
		if c.TableMaintenance.OrphanCleanupIntervalSeconds == 0 {
			c.TableMaintenance.OrphanCleanupIntervalSeconds = 24 * 60 * 60
		}
		if c.TableMaintenance.OrphanCleanupOlderThanHours <= 0 {
			c.TableMaintenance.OrphanCleanupOlderThanHours = 72
		}
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.SnapshotBatchSize <= 0 {
		c.SnapshotBatchSize = 10000
	}
	if c.SnapshotTargetFileSizeBytes <= 0 {
		c.SnapshotTargetFileSizeBytes = 128 * 1024 * 1024
	}
	if c.SnapshotParquetRowGroupRows <= 0 {
		c.SnapshotParquetRowGroupRows = 50000
	}
	if c.SnapshotSpoolMaxBytes <= 0 {
		c.SnapshotSpoolMaxBytes = 20 * 1024 * 1024 * 1024
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = config.ByteSize(128 * 1024 * 1024)
	}
	if c.MaxConcurrentCommits <= 0 {
		c.MaxConcurrentCommits = 2
	}
	if c.FlushSeconds <= 0 {
		c.FlushSeconds = 30
	}
	if c.CheckpointFlushSeconds <= 0 {
		c.CheckpointFlushSeconds = 10
	}
	if c.CheckpointSaveIntervalSeconds <= 0 {
		c.CheckpointSaveIntervalSeconds = 5
	}
	if c.DeleteConcurrency <= 0 {
		c.DeleteConcurrency = 2
	}
	if c.IdleTableEvictSeconds < 0 {
		c.IdleTableEvictSeconds = 0
	}

	if c.Overrides != nil {
		normalized := make(map[string]config.IcebergTarget, len(c.Overrides))
		for key, target := range c.Overrides {
			nk := strings.ToLower(strings.TrimSpace(key))
			if nk == "" {
				continue
			}
			target.Namespace = strings.TrimSpace(target.Namespace)
			target.Table = strings.TrimSpace(target.Table)
			normalized[nk] = target
		}
		c.Overrides = normalized
	}

	if c.PrimaryKeys != nil {
		normalized := make(map[string][]string, len(c.PrimaryKeys))
		for key, cols := range c.PrimaryKeys {
			nk := strings.ToLower(strings.TrimSpace(key))
			if nk == "" {
				continue
			}
			out := make([]string, 0, len(cols))
			for _, col := range cols {
				cc := strings.TrimSpace(col)
				if cc == "" {
					continue
				}
				out = append(out, cc)
			}
			normalized[nk] = out
		}
		c.PrimaryKeys = normalized
	}

	if c.SnapshotReplaceFilters != nil {
		normalized := make(map[string]config.IcebergSnapshotReplaceFilterConfig, len(c.SnapshotReplaceFilters))
		for key, filter := range c.SnapshotReplaceFilters {
			nk := strings.ToLower(strings.TrimSpace(key))
			filter.Column = strings.TrimSpace(filter.Column)
			filter.Op = strings.TrimSpace(filter.Op)
			filter.Value = strings.TrimSpace(filter.Value)
			if nk == "" || filter.Column == "" || filter.Op == "" || filter.Value == "" {
				continue
			}
			normalized[nk] = filter
		}
		c.SnapshotReplaceFilters = normalized
	}

	if len(c.SnapshotTruncateTables) > 0 {
		normalized := make([]string, 0, len(c.SnapshotTruncateTables))
		for _, table := range c.SnapshotTruncateTables {
			nt := strings.ToLower(strings.TrimSpace(table))
			if nt == "" {
				continue
			}
			normalized = append(normalized, nt)
		}
		c.SnapshotTruncateTables = normalized
	}

	if len(c.SnapshotTruncateExcludeTables) > 0 {
		normalized := make([]string, 0, len(c.SnapshotTruncateExcludeTables))
		for _, table := range c.SnapshotTruncateExcludeTables {
			nt := strings.ToLower(strings.TrimSpace(table))
			if nt == "" {
				continue
			}
			normalized = append(normalized, nt)
		}
		c.SnapshotTruncateExcludeTables = normalized
	}

	if c.TableProperties != nil {
		normalized := make(map[string]string, len(c.TableProperties))
		for key, value := range c.TableProperties {
			nk := strings.TrimSpace(key)
			if nk == "" {
				continue
			}
			normalized[nk] = strings.TrimSpace(value)
		}
		c.TableProperties = normalized
	}
	c.TrinoDelete.URI = strings.TrimRight(strings.TrimSpace(c.TrinoDelete.URI), "/")
	c.TrinoDelete.User = strings.TrimSpace(c.TrinoDelete.User)
	c.TrinoDelete.Password = strings.TrimSpace(c.TrinoDelete.Password)
	c.TrinoDelete.Source = strings.TrimSpace(c.TrinoDelete.Source)
	c.TrinoDelete.Catalog = strings.TrimSpace(c.TrinoDelete.Catalog)
	c.TrinoDelete.AccessToken = strings.TrimSpace(c.TrinoDelete.AccessToken)

	c.MetadataColumns = normalizeMetadataColumnsConfig(c.MetadataColumns)

	return c
}

func normalizeAutomaticCompactOptions(input map[string]any) map[string]any {
	options := copyAnyMap(input)
	if _, exists := options["strategy"]; !exists {
		options["strategy"] = "binpack"
	}
	rewriteOptions := stringAnyMap(options["options"])
	if rewriteOptions == nil {
		rewriteOptions = make(map[string]any)
	}
	defaults := map[string]any{
		"target-file-size-bytes":             "134217728",
		"min-file-size-bytes":                "67108864",
		"max-file-size-bytes":                "241591910",
		"min-input-files":                    "10",
		"max-file-group-size-bytes":          "268435456",
		"max-concurrent-file-group-rewrites": "1",
		"partial-progress.enabled":           "true",
		"partial-progress.max-commits":       "100",
		"rewrite-job-order":                  "files-desc",
	}
	for key, value := range defaults {
		if _, exists := rewriteOptions[key]; !exists {
			rewriteOptions[key] = value
		}
	}
	options["options"] = rewriteOptions
	return options
}

func normalizeMetadataColumnsConfig(c config.IcebergMetadataColumnsConfig) config.IcebergMetadataColumnsConfig {
	c.CreatedAt.Name = strings.TrimSpace(c.CreatedAt.Name)
	if c.CreatedAt.Name == "" {
		c.CreatedAt.Name = "created_at"
	}
	if c.CreatedAt.SourceColumns != nil {
		normalized := make(map[string]string, len(c.CreatedAt.SourceColumns))
		for key, value := range c.CreatedAt.SourceColumns {
			nk := strings.ToLower(strings.TrimSpace(key))
			nv := strings.TrimSpace(value)
			if nk == "" || nv == "" {
				continue
			}
			normalized[nk] = nv
		}
		c.CreatedAt.SourceColumns = normalized
	}
	if len(c.CreatedAt.SourceColumns) > 0 {
		c.CreatedAt.Enabled = true
	}

	c.UpdatedAt.Name = strings.TrimSpace(c.UpdatedAt.Name)
	if c.UpdatedAt.Name == "" {
		c.UpdatedAt.Name = "updated_at"
	}

	c.ETLLoadedAt.Name = strings.TrimSpace(c.ETLLoadedAt.Name)
	if c.ETLLoadedAt.Name == "" {
		c.ETLLoadedAt.Name = "etl_loaded_at"
	}

	return c
}
