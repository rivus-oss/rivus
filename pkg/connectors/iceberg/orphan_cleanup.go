package iceberg

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"
	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
)

const defaultOrphanCleanupOlderThan = 72 * time.Hour

type OrphanCleanupOptions struct {
	DryRun         bool
	OlderThan      time.Duration
	MaxConcurrency int
	Tables         []string
}

type OrphanCleanupResult struct {
	DryRun           bool                       `json:"dry_run"`
	OlderThanHours   float64                    `json:"older_than_hours"`
	TableCount       int                        `json:"table_count"`
	OrphanFileCount  int                        `json:"orphan_file_count"`
	DeletedFileCount int                        `json:"deleted_file_count"`
	ScannedSizeBytes int64                      `json:"scanned_size_bytes"`
	Tables           []OrphanCleanupTableResult `json:"tables"`
}

type OrphanCleanupTableResult struct {
	Namespace          string   `json:"namespace"`
	Table              string   `json:"table"`
	Target             string   `json:"target"`
	OrphanFileCount    int      `json:"orphan_file_count"`
	DeletedFileCount   int      `json:"deleted_file_count"`
	ScannedSizeBytes   int64    `json:"scanned_size_bytes"`
	SampleOrphanFiles  []string `json:"sample_orphan_files,omitempty"`
	SampleDeletedFiles []string `json:"sample_deleted_files,omitempty"`
	Error              string   `json:"error,omitempty"`
}

func CleanupOrphanFilesForJobConfig(ctx context.Context, jobID string, jobCfg *config.JobConfig, opts OrphanCleanupOptions) (*OrphanCleanupResult, error) {
	if jobCfg == nil {
		return nil, fmt.Errorf("job config is nil")
	}

	sinkType, sinkCfg := jobSinkSpec(jobCfg)
	if !strings.EqualFold(sinkType, "iceberg_native") {
		return nil, fmt.Errorf("job sink is %q, not iceberg_native", sinkType)
	}

	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return nil, err
	}
	// This catalog/file cleanup path never runs a source snapshot or sink Run.
	// It needs no source predicate to inspect orphan files.
	if iceCfg.SnapshotWriteMode == config.IcebergSnapshotAutoCutoffWriteMode {
		iceCfg.SnapshotWriteMode = snapshotWriteModeAuto
	}
	// Orphan cleanup does not evolve schemas and has no source discovery phase.
	iceCfg.AutoMigration.Enabled = false
	sink, err := NewSink(jobID, "", strings.TrimSpace(jobCfg.Name), iceCfg, jobCfg.Retry, nil, nil)
	if err != nil {
		return nil, err
	}

	targets, err := orphanCleanupTargets(jobCfg, sink, opts.Tables)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no iceberg target tables found; pass explicit tables as namespace.table")
	}

	olderThan := opts.OlderThan
	if olderThan <= 0 {
		olderThan = defaultOrphanCleanupOlderThan
	}

	result := &OrphanCleanupResult{
		DryRun:         opts.DryRun,
		OlderThanHours: olderThan.Hours(),
		TableCount:     len(targets),
		Tables:         make([]OrphanCleanupTableResult, 0, len(targets)),
	}

	for _, target := range targets {
		tableResult := cleanupTableOrphans(ctx, sink, target, opts.DryRun, olderThan, opts.MaxConcurrency)
		result.OrphanFileCount += tableResult.OrphanFileCount
		result.DeletedFileCount += tableResult.DeletedFileCount
		result.ScannedSizeBytes += tableResult.ScannedSizeBytes
		result.Tables = append(result.Tables, tableResult)
	}

	return result, nil
}

func cleanupTableOrphans(ctx context.Context, sink *Sink, target config.IcebergTarget, dryRun bool, olderThan time.Duration, maxConcurrency int) OrphanCleanupTableResult {
	out := OrphanCleanupTableResult{
		Namespace: strings.TrimSpace(target.Namespace),
		Table:     strings.TrimSpace(target.Table),
	}
	out.Target = tableKey(out.Namespace, out.Table)

	tbl, err := sink.catalog.LoadTable(ctx, namespaceIdentifier(out.Namespace, out.Table))
	if err != nil {
		if errorsIsNoSuchIcebergTable(err) {
			out.Error = "table not found"
			return out
		}
		out.Error = sink.operationError(fmt.Sprintf("load table target=%q for orphan cleanup", out.Target), err).Error()
		return out
	}

	cleanupOpts := []icetable.OrphanCleanupOption{
		icetable.WithDryRun(dryRun),
		icetable.WithFilesOlderThan(olderThan),
	}
	if maxConcurrency > 0 {
		cleanupOpts = append(cleanupOpts, icetable.WithCleanupMaxConcurrency(maxConcurrency))
	}
	cleanup, err := tbl.DeleteOrphanFiles(ctx, cleanupOpts...)
	if err != nil {
		out.Error = sink.operationError(fmt.Sprintf("delete orphan files target=%q", out.Target), err).Error()
		return out
	}

	out.OrphanFileCount = len(cleanup.OrphanFileLocations)
	out.DeletedFileCount = len(cleanup.DeletedFiles)
	out.ScannedSizeBytes = cleanup.TotalSizeBytes
	out.SampleOrphanFiles = sampleStrings(cleanup.OrphanFileLocations, 50)
	out.SampleDeletedFiles = sampleStrings(cleanup.DeletedFiles, 50)
	return out
}

func errorsIsNoSuchIcebergTable(err error) bool {
	return errors.Is(err, icecatalog.ErrNoSuchTable) || strings.Contains(strings.ToLower(err.Error()), "no such table")
}

func orphanCleanupTargets(jobCfg *config.JobConfig, sink *Sink, explicit []string) ([]config.IcebergTarget, error) {
	targets := make([]config.IcebergTarget, 0)
	for _, raw := range explicit {
		target, err := parseTargetTable(raw)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	if len(targets) > 0 {
		return dedupeTargets(targets), nil
	}

	// Maintenance-only monitors have no source selection. Their explicit
	// targets live with the maintenance settings and are also available to
	// manual operations issued from the monitor UI.
	if sink != nil && len(sink.cfg.TableMaintenance.Tables) > 0 {
		return dedupeTargets(sink.cfg.TableMaintenance.Tables), nil
	}

	sourceType, sourceCfg := jobSourceSpec(jobCfg)
	if sourceType == "" || strings.EqualFold(sourceType, "mysql") {
		mysqlCfg, err := decodeMySQLConfig(sourceCfg)
		if err != nil {
			return nil, err
		}
		mysqlCfg = config.NormalizeMySQLConfig(mysqlCfg)
		for _, sourceTable := range mysqlCfg.Tables {
			srcSchema, srcTable, ok := splitSourceTable(sourceTable)
			if !ok {
				continue
			}
			targetNamespace, targetTable := sink.ResolveTarget(srcSchema, srcTable)
			targets = append(targets, config.IcebergTarget{Namespace: targetNamespace, Table: targetTable})
		}
	}

	for _, override := range sink.cfg.Overrides {
		if strings.TrimSpace(override.Namespace) == "" || strings.TrimSpace(override.Table) == "" {
			continue
		}
		targets = append(targets, override)
	}

	return dedupeTargets(targets), nil
}

func jobSourceSpec(jobCfg *config.JobConfig) (string, any) {
	if jobCfg.Source != nil && strings.TrimSpace(jobCfg.Source.Type) != "" {
		return strings.ToLower(strings.TrimSpace(jobCfg.Source.Type)), jobCfg.Source.Config
	}
	return "mysql", jobCfg.MySQL
}

func jobSinkSpec(jobCfg *config.JobConfig) (string, any) {
	if jobCfg.Sink != nil && strings.TrimSpace(jobCfg.Sink.Type) != "" {
		return strings.ToLower(strings.TrimSpace(jobCfg.Sink.Type)), jobCfg.Sink.Config
	}
	return "doris", jobCfg.Doris
}

func decodeMySQLConfig(v any) (config.MySQLConfig, error) {
	switch t := v.(type) {
	case config.MySQLConfig:
		return t, nil
	case *config.MySQLConfig:
		if t == nil {
			return config.MySQLConfig{}, fmt.Errorf("mysql config is nil")
		}
		return *t, nil
	}

	b, err := yaml.Marshal(v)
	if err != nil {
		return config.MySQLConfig{}, err
	}
	var c config.MySQLConfig
	if err := yaml.Unmarshal(b, &c); err != nil {
		return config.MySQLConfig{}, err
	}
	return c, nil
}

func splitSourceTable(raw string) (string, string, bool) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(raw)), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	schema := strings.TrimSpace(parts[0])
	table := strings.TrimSpace(parts[1])
	if schema == "" || table == "" || strings.Contains(table, "*") {
		return "", "", false
	}
	return schema, table, true
}

func parseTargetTable(raw string) (config.IcebergTarget, error) {
	raw = strings.TrimSpace(raw)
	idx := strings.LastIndex(raw, ".")
	if idx <= 0 || idx >= len(raw)-1 {
		return config.IcebergTarget{}, fmt.Errorf("invalid iceberg target table %q; expected namespace.table", raw)
	}
	target := config.IcebergTarget{
		Namespace: strings.TrimSpace(raw[:idx]),
		Table:     strings.TrimSpace(raw[idx+1:]),
	}
	if target.Namespace == "" || target.Table == "" {
		return config.IcebergTarget{}, fmt.Errorf("invalid iceberg target table %q; expected namespace.table", raw)
	}
	return target, nil
}

func dedupeTargets(targets []config.IcebergTarget) []config.IcebergTarget {
	seen := make(map[string]config.IcebergTarget, len(targets))
	for _, target := range targets {
		target.Namespace = strings.TrimSpace(target.Namespace)
		target.Table = strings.TrimSpace(target.Table)
		if target.Namespace == "" || target.Table == "" {
			continue
		}
		seen[tableKey(target.Namespace, target.Table)] = target
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]config.IcebergTarget, 0, len(keys))
	for _, key := range keys {
		out = append(out, seen[key])
	}
	return out
}

func sampleStrings(values []string, limit int) []string {
	if len(values) == 0 || limit <= 0 {
		return nil
	}
	if len(values) <= limit {
		out := append([]string(nil), values...)
		sort.Strings(out)
		return out
	}
	out := append([]string(nil), values[:limit]...)
	sort.Strings(out)
	return out
}
