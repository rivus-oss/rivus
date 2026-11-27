package config

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
)

const IcebergRebuildScopedKey = "iceberg_rebuild_scoped"
const IcebergRebuildScopedWriteMode = "rebuild-scoped-trino-v1"

// Older workers reject this mode before opening connectors. Reset still uses
// durable Trino receipts; snapshot data is written as guarded native batches.
const IcebergRebuildScopedNativeWriteMode = "rebuild-scoped-native-v2"

func IcebergSnapshotRebuildUsesTrino(cfg *JobConfig) bool {
	if cfg == nil || cfg.Sink == nil {
		return false
	}
	executor, _ := cfg.Sink.Config["snapshot_replace_delete_executor"].(string)
	return strings.EqualFold(strings.TrimSpace(executor), "trino")
}

var rebuildFilterExpression = regexp.MustCompile("(?i)^\\s*`?([a-z_][a-z0-9_]*)`?\\s*(>=|<=|<>|!=|=|>|<)\\s*'((?:''|[^'])*)'\\s*$")

// Only the single-column literal predicates used by the supported rebuild
// configuration are admitted. Do not guess equivalence for arbitrary SQL.
func ValidateIcebergRebuildFilter(expression string, filter *IcebergSnapshotReplaceFilterConfig) error {
	if filter == nil {
		if strings.TrimSpace(expression) != "" {
			return fmt.Errorf("source snapshot filter has no matching target replacement filter")
		}
		return nil
	}
	match := rebuildFilterExpression.FindStringSubmatch(expression)
	ops := map[string]string{"gte": ">=", "ge": ">=", "gt": ">", "lte": "<=", "le": "<=", "lt": "<", "eq": "=", "neq": "<>", "ne": "<>"}
	rawOp := strings.ToLower(strings.TrimSpace(filter.Op))
	op := ops[rawOp]
	if op == "" && strings.Contains(" >= <= <> != = > < ", " "+rawOp+" ") {
		op = rawOp
		if op == "!=" {
			op = "<>"
		}
	}
	if len(match) != 4 || !strings.EqualFold(match[1], filter.Column) || op == "" || (match[2] != op && !(op == "<>" && match[2] == "!=")) || strings.ReplaceAll(match[3], "''", "'") != filter.Value {
		return fmt.Errorf("source and target snapshot filter scopes do not match")
	}
	return nil
}

func rebuildPatternMatches(pattern, selection string) bool {
	if strings.ContainsAny(pattern, "?[") {
		return false
	}
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	selection = strings.ToLower(strings.TrimSpace(selection))
	if !strings.Contains(pattern, ".") {
		selection = strings.SplitN(selection, ".", 2)[1]
	}
	matched, err := path.Match(pattern, selection)
	return err == nil && matched
}

func rebuildConfigMatch[T any](entries map[string]T, selection string) *T {
	if item, ok := entries[selection]; ok {
		return &item
	}
	keys := make([]string, 0)
	for key := range entries {
		if rebuildPatternMatches(key, selection) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a := len(strings.ReplaceAll(keys[i], "*", ""))
		b := len(strings.ReplaceAll(keys[j], "*", ""))
		if a != b {
			return a > b
		}
		return keys[i] < keys[j]
	})
	if len(keys) == 0 {
		return nil
	}
	item := entries[keys[0]]
	return &item
}

func rebuildSourceConfigMatch(entries map[string]MySQLTableConfig, selection string) *MySQLTableConfig {
	if item, ok := entries[selection]; ok {
		return &item
	}
	parts := strings.SplitN(selection, ".", 2)
	if len(parts) == 2 {
		if item, ok := entries[parts[1]]; ok {
			return &item
		}
	}
	return rebuildConfigMatch(entries, selection)
}

func validateScopedIcebergRebuild(cfg *JobConfig) error {
	var source MySQLConfig
	var sink IcebergConfig
	decode := func(value any, target any) error {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return json.Unmarshal(encoded, target)
	}
	if err := decode(cfg.Source.Config, &source); err != nil {
		return err
	}
	source = NormalizeMySQLConfig(source)
	if err := decode(cfg.Sink.Config, &sink); err != nil {
		return err
	}
	hasAutomaticCutoff := false
	for _, tableCfg := range source.TableConfigs {
		hasAutomaticCutoff = hasAutomaticCutoff || tableCfg.SnapshotCutoff != nil
	}
	if strings.TrimSpace(sink.TrinoDelete.URI) == "" || (len(sink.SnapshotReplaceFilters) == 0 && !hasAutomaticCutoff) {
		return fmt.Errorf("scoped Trino rebuild requires coordinator configuration and matching replacement filters")
	}
	mode := strings.ToLower(strings.TrimSpace(sink.SnapshotWriteMode))
	if mode != "auto" && mode != IcebergSnapshotAutoCutoffWriteMode && mode != IcebergRebuildScopedWriteMode && mode != IcebergRebuildScopedNativeWriteMode {
		return fmt.Errorf("scoped Trino rebuild requires auto snapshot mode")
	}
	if mode == IcebergSnapshotAutoCutoffWriteMode && !hasAutomaticCutoff {
		return fmt.Errorf("%s requires source snapshot_cutoff settings", IcebergSnapshotAutoCutoffWriteMode)
	}
	selections := append([]string(nil), source.Tables...)
	if source.Database != "" && source.Table != "" {
		selections = append(selections, source.Database+"."+source.Table)
	}
	for _, db := range source.Databases {
		for _, table := range source.TableNames {
			selections = append(selections, db+"."+table)
		}
	}
	if len(selections) == 0 {
		return fmt.Errorf("scoped rebuild requires explicit source selection")
	}
	for _, selection := range selections {
		selection = strings.ToLower(strings.TrimSpace(selection))
		parts := strings.Split(selection, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(selection, "*?[") {
			return fmt.Errorf("scoped rebuild requires explicit source table names")
		}
		filter := rebuildConfigMatch(sink.SnapshotReplaceFilters, selection)
		sourceCfg := rebuildSourceConfigMatch(source.TableConfigs, selection)
		if sourceCfg != nil && sourceCfg.SnapshotCutoff != nil {
			if err := ValidateMySQLSnapshotCutoff(*sourceCfg); err != nil {
				return fmt.Errorf("scoped rebuild %s: %w", selection, err)
			}
			if filter != nil {
				return fmt.Errorf("scoped rebuild %s: snapshot_cutoff generates its replacement filter; remove the manual replacement filter", selection)
			}
			// The runtime resolver freezes and validates both generated predicates
			// before sink construction and before the first destructive reset.
			continue
		}
		expression := ""
		if sourceCfg != nil {
			expression = sourceCfg.Filter
		}
		if err := ValidateIcebergRebuildFilter(expression, filter); err != nil {
			return fmt.Errorf("scoped rebuild %s: %w", selection, err)
		}
		if filter == nil {
			for _, exclude := range sink.SnapshotTruncateExcludeTables {
				if rebuildPatternMatches(exclude, selection) {
					return fmt.Errorf("scoped rebuild cannot fully reset excluded source %s", selection)
				}
			}
			covered := false
			for _, pattern := range sink.SnapshotTruncateTables {
				if pattern == selection || (sink.SnapshotTruncateAllowPatterns && rebuildPatternMatches(pattern, selection)) {
					covered = true
				}
			}
			if !covered {
				return fmt.Errorf("scoped rebuild requires configured reset for unfiltered table %s", selection)
			}
		}
	}
	return nil
}
