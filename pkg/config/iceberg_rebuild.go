package config

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

const IcebergRebuildFullResetKey = "iceberg_rebuild_full_reset"

// An older sink rejects this mode before catalog discovery. The new factory
// translates it only after verifying the submission-bound rebuild marker.
const IcebergRebuildSnapshotWriteMode = "rebuild-truncate-append-v1"

// ValidateIcebergSnapshotRebuild requires an explicit replacement scope for
// every source table: native full reset, or matching source/target Trino filters.
// Purged binlogs alone never authorize appending a snapshot to populated targets.
func ValidateIcebergSnapshotRebuild(cfg *JobConfig) error {
	if cfg == nil || cfg.Source == nil || cfg.Source.Type != "mysql" || cfg.Sink == nil || cfg.Sink.Type != "iceberg_native" {
		return fmt.Errorf("snapshot rebuild requires MySQL and native Iceberg")
	}
	if cfg.Mode == JobModeSnapshotOnly {
		return fmt.Errorf("purged-binlog rebuild requires a snapshot-to-CDC job")
	}
	if IcebergSnapshotRebuildUsesTrino(cfg) {
		return validateScopedIcebergRebuild(cfg)
	}
	decode := func(v any, dst any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, dst)
	}
	var source MySQLConfig
	var sink IcebergConfig
	if err := decode(cfg.Source.Config, &source); err != nil {
		return err
	}
	if err := decode(cfg.Sink.Config, &sink); err != nil {
		return err
	}
	executor := strings.ToLower(strings.TrimSpace(sink.SnapshotReplaceDeleteExecutor))
	if executor != "" && executor != "native" {
		return fmt.Errorf("archived snapshot rebuild requires native target reset with durable catalog commit guard")
	}
	mode := strings.ToLower(strings.TrimSpace(sink.SnapshotWriteMode))
	if mode != "auto" && mode != "truncate-append" && mode != IcebergRebuildSnapshotWriteMode {
		return fmt.Errorf("snapshot rebuild requires configured truncate-append or auto with snapshot_truncate_tables")
	}
	if mode == "auto" && len(sink.SnapshotReplaceFilters) > 0 {
		return fmt.Errorf("snapshot rebuild requires full target reset; auto currently selects snapshot_replace_filters instead of truncate")
	}
	selections := append([]string(nil), source.Tables...)
	if source.Database != "" && source.Table != "" {
		selections = append(selections, source.Database+"."+source.Table)
	}
	for _, database := range source.Databases {
		for _, table := range source.TableNames {
			selections = append(selections, database+"."+table)
		}
	}
	if len(selections) == 0 {
		return fmt.Errorf("snapshot rebuild requires explicit source table selection")
	}
	for _, selection := range selections {
		selection = strings.ToLower(strings.TrimSpace(selection))
		parts := strings.Split(selection, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(selection, "*?[") {
			return fmt.Errorf("snapshot rebuild requires explicit source table names: %s", selection)
		}
		matches := func(pattern string, allow bool) bool {
			pattern = strings.ToLower(strings.TrimSpace(pattern))
			if pattern == selection {
				return true
			}
			if !allow {
				return false
			}
			// Match the sink's glob language (only '*'), including its bare
			// table-name patterns. Do not accept '?' or character classes that
			// the sink would treat literally and therefore never truncate.
			glob := func(pattern, value string) bool {
				if pattern == "" {
					return false
				}
				return regexp.MustCompile("^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*") + "$").MatchString(value)
			}
			patternParts := strings.Split(pattern, ".")
			if len(patternParts) == 2 {
				return glob(patternParts[0], parts[0]) && glob(patternParts[1], parts[1])
			}
			return glob(pattern, parts[1])
		}
		for _, excluded := range sink.SnapshotTruncateExcludeTables {
			if matches(excluded, true) {
				return fmt.Errorf("snapshot rebuild target excluded from truncate: %s", selection)
			}
		}
		for pattern, tableConfig := range source.TableConfigs {
			if (strings.TrimSpace(tableConfig.Filter) != "" || tableConfig.SnapshotCutoff != nil) && matches(pattern, true) {
				return fmt.Errorf("full snapshot rebuild requires removing source snapshot filter for %s", selection)
			}
		}
		covered := mode == "truncate-append" || mode == IcebergRebuildSnapshotWriteMode
		for _, pattern := range sink.SnapshotTruncateTables {
			if matches(pattern, sink.SnapshotTruncateAllowPatterns) {
				covered = true
			}
		}
		if !covered {
			return fmt.Errorf("snapshot rebuild requires configured truncate for source table %s", selection)
		}
	}
	return nil
}
