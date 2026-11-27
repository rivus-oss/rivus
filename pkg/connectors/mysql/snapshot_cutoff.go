package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

type snapshotMonthCount struct {
	Month string
	Rows  int64
}

type snapshotCutoffResult struct {
	Column            string `json:"column"`
	Value             string `json:"value"`
	ExcludedRows      int64  `json:"excluded_rows"`
	BoundaryMonthRows int64  `json:"boundary_month_rows"`
	PreviousMonthRows int64  `json:"previous_month_rows"`
}

type snapshotCutoffPlan struct {
	Version        int                             `json:"version"`
	ConfigHash     string                          `json:"config_hash"`
	ReferenceMonth string                          `json:"reference_month"`
	Tables         map[string]snapshotCutoffResult `json:"tables"`
}

// A completed, sufficiently populated last month anchors the search. Walk back
// through retained months until both a sparse preceding month and a large jump
// support the boundary. Neither ancient dates nor future schedules anchor it.
func detectSnapshotCutoff(now time.Time, counts []snapshotMonthCount, raw config.MySQLSnapshotCutoffConfig) (snapshotCutoffResult, error) {
	cfg := config.NormalizeMySQLSnapshotCutoff(raw)
	if err := config.ValidateMySQLSnapshotCutoff(config.MySQLTableConfig{SnapshotCutoff: &cfg}); err != nil {
		return snapshotCutoffResult{}, err
	}
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	byMonth := make(map[string]int64)
	var invalidRows int64
	for _, item := range counts {
		if item.Rows < 0 {
			return snapshotCutoffResult{}, fmt.Errorf("invalid negative monthly row count")
		}
		parsed, err := time.Parse("2006-01-02", item.Month)
		if err != nil || parsed.Day() != 1 || parsed.Year() < 1 {
			invalidRows += item.Rows
			continue
		}
		byMonth[item.Month] += item.Rows
	}
	last := month.AddDate(0, -1, 0)
	if byMonth[last.Format("2006-01-02")] < cfg.MinMonthRows {
		return snapshotCutoffResult{}, fmt.Errorf("snapshot cutoff unclear: last completed month %s has %d rows, below min_month_rows=%d; use an explicit filter or adjust the count rule", last.Format("2006-01"), byMonth[last.Format("2006-01-02")], cfg.MinMonthRows)
	}
	for i := 0; i < cfg.LookbackMonths; i++ {
		candidate := last.AddDate(0, -i, 0)
		value := candidate.Format("2006-01-02")
		previous := candidate.AddDate(0, -1, 0).Format("2006-01-02")
		rows, oldRows := byMonth[value], byMonth[previous]
		if rows < cfg.MinMonthRows || oldRows >= cfg.MinMonthRows {
			continue
		}
		if oldRows > 0 && float64(rows)/float64(oldRows) < cfg.MinGrowthRatio {
			continue
		}
		excluded := invalidRows
		for key, count := range byMonth {
			if key < value {
				excluded += count
			}
		}
		return snapshotCutoffResult{Column: cfg.Column, Value: value, ExcludedRows: excluded, BoundaryMonthRows: rows, PreviousMonthRows: oldRows}, nil
	}
	return snapshotCutoffResult{}, fmt.Errorf("snapshot cutoff unclear: no sparse-to-dense boundary in %d completed months; use an explicit filter or adjust the count rule", cfg.LookbackMonths)
}

func (s *Source) cutoffCounts(ctx context.Context, table connector.TableRef, cfg config.MySQLSnapshotCutoffConfig, reference time.Time) ([]snapshotMonthCount, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.QueryTimeoutSeconds)*time.Second)
	defer cancel()
	var dataType string
	if err := s.db.QueryRowContext(ctx, `SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`, table.Schema, table.Table, cfg.Column).Scan(&dataType); err != nil {
		return nil, fmt.Errorf("snapshot cutoff column %s: %w", cfg.Column, err)
	}
	switch strings.ToLower(dataType) {
	case "date", "datetime", "timestamp":
	default:
		return nil, fmt.Errorf("snapshot cutoff column %s must be DATE, DATETIME or TIMESTAMP, got %s", cfg.Column, dataType)
	}
	column := "`" + escapeMySQLIdentifier(cfg.Column) + "`"
	query := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(%d) */ DATE_FORMAT(%s, '%%Y-%%m-01'), COUNT(*) FROM `%s`.`%s` WHERE %s < ? OR %s IS NULL GROUP BY DATE_FORMAT(%s, '%%Y-%%m-01') ORDER BY 1", cfg.QueryTimeoutSeconds*1000, column, escapeMySQLIdentifier(table.Schema), escapeMySQLIdentifier(table.Table), column, column, column)
	rows, err := s.db.QueryContext(ctx, query, reference.AddDate(0, 1, 0).Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var counts []snapshotMonthCount
	for rows.Next() {
		var month sql.NullString
		var n int64
		if err := rows.Scan(&month, &n); err != nil {
			return nil, err
		}
		counts = append(counts, snapshotMonthCount{Month: month.String, Rows: n})
	}
	return counts, rows.Err()
}

func (s *Source) resolveSnapshotCutoffs(ctx context.Context, jctx connector.JobContext) (any, error) {
	selected := make(map[string]config.MySQLSnapshotCutoffConfig)
	refs := make(map[string]connector.TableRef)
	for _, table := range s.tableRefs() {
		cfg, _ := s.tableConfigForTable(table.Schema, table.Table)
		if cfg.SnapshotCutoff == nil {
			continue
		}
		if err := config.ValidateMySQLSnapshotCutoff(cfg); err != nil {
			return nil, err
		}
		key := strings.ToLower(table.Schema + "." + table.Table)
		selected[key] = config.NormalizeMySQLSnapshotCutoff(*cfg.SnapshotCutoff)
		refs[key] = table
	}
	if len(selected) == 0 {
		return jctx.SinkConfig, nil
	}
	if jctx.SinkType != "iceberg_native" {
		return nil, fmt.Errorf("snapshot_cutoff requires an iceberg_native sink")
	}
	var sink config.IcebergConfig
	encoded, err := yaml.Marshal(jctx.SinkConfig)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(encoded, &sink); err != nil {
		return nil, err
	}
	mode := strings.ToLower(strings.TrimSpace(sink.SnapshotWriteMode))
	if mode != config.IcebergSnapshotAutoCutoffWriteMode && mode != config.IcebergRebuildScopedWriteMode && mode != config.IcebergRebuildScopedNativeWriteMode {
		return nil, fmt.Errorf("snapshot_cutoff requires snapshot_write_mode %s so older workers reject unsupported automatic scopes", config.IcebergSnapshotAutoCutoffWriteMode)
	}
	if !strings.EqualFold(strings.TrimSpace(sink.SnapshotReplaceDeleteExecutor), "trino") || strings.TrimSpace(sink.TrinoDelete.URI) == "" {
		return nil, fmt.Errorf("snapshot_cutoff requires the Trino replacement executor and trino_delete.uri")
	}
	for key := range selected {
		for pattern := range sink.SnapshotReplaceFilters {
			if mysqlTableConfigPatternMatches(pattern, key, refs[key].Table) {
				return nil, fmt.Errorf("snapshot_cutoff for %s generates both filters; remove its manual snapshot_replace_filters entry %s", key, pattern)
			}
		}
	}
	store, ok := s.offsetSto.(meta.SnapshotScopePlanStore)
	if !ok || jctx.SubmissionID == "" || s.checkpointKey() == "" {
		return nil, fmt.Errorf("snapshot_cutoff requires durable snapshot scope storage and a submission ID")
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return nil, err
	}
	// Hash inputs, never credentials themselves, so a changed rule cannot reuse
	// a partially copied snapshot with a different predicate.
	inputs, err := json.Marshal(struct {
		Source config.MySQLConfig
		Sink   config.IcebergConfig
	}{s.cfg, sink})
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(inputs)
	hashString := hex.EncodeToString(hash[:])
	key := s.checkpointKey()
	payload, err := store.GetSnapshotScopePlan(ctx, key, jctx.SubmissionID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot cutoff: %w", err)
	}
	if payload == "" {
		if jctx.Mode == config.JobModeLatest || jctx.Mode == config.JobModeLatestOffset {
			return nil, fmt.Errorf("snapshot_cutoff has no saved plan; start a new snapshot before using a CDC-only mode")
		}
		if err := s.checkNewSnapshotScope(ctx); err != nil {
			return nil, err
		}
		plan := snapshotCutoffPlan{Version: 1, ConfigHash: hashString, Tables: make(map[string]snapshotCutoffResult)}
		queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := s.db.QueryRowContext(queryCtx, `SELECT DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01')`).Scan(&plan.ReferenceMonth)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("read MySQL reference month: %w", err)
		}
		reference, err := time.Parse("2006-01-02", plan.ReferenceMonth)
		if err != nil {
			return nil, fmt.Errorf("invalid MySQL reference month: %w", err)
		}
		keys := make([]string, 0, len(selected))
		for key := range selected {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			s.reportProgress(connector.ProgressInfo{Phase: "preflight", Summary: "Detecting snapshot cutoff", Detail: key, CurrentTable: key})
			counts, err := s.cutoffCounts(ctx, refs[key], selected[key], reference)
			if err != nil {
				return nil, fmt.Errorf("snapshot cutoff %s: %w", key, err)
			}
			result, err := detectSnapshotCutoff(reference, counts, selected[key])
			if err != nil {
				return nil, fmt.Errorf("snapshot cutoff %s: %w", key, err)
			}
			plan.Tables[key] = result
		}
		encoded, err := json.Marshal(plan)
		if err != nil {
			return nil, err
		}
		if err := store.SaveSnapshotScopePlan(ctx, key, jctx.SubmissionID, string(encoded)); err != nil {
			return nil, fmt.Errorf("save snapshot cutoff: %w", err)
		}
		// Respect the first durable plan if another start raced this one.
		payload, err = store.GetSnapshotScopePlan(ctx, key, jctx.SubmissionID)
		if err != nil || payload == "" {
			return nil, fmt.Errorf("snapshot cutoff was not durably confirmed: %v", err)
		}
	}
	var plan snapshotCutoffPlan
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return nil, fmt.Errorf("decode saved snapshot cutoff: %w", err)
	}
	if plan.Version != 1 || plan.ConfigHash != hashString || len(plan.Tables) != len(selected) {
		return nil, fmt.Errorf("saved snapshot cutoff does not match this configuration; start a new snapshot submission")
	}
	reference, err := time.Parse("2006-01-02", plan.ReferenceMonth)
	if err != nil || reference.Year() < 1 || reference.Day() != 1 {
		return nil, fmt.Errorf("invalid saved snapshot cutoff reference month")
	}
	// Validate the complete plan before mutating either runtime configuration.
	for key, cfg := range selected {
		result, ok := plan.Tables[key]
		date, err := time.Parse("2006-01-02", result.Value)
		validCounts := result.BoundaryMonthRows >= cfg.MinMonthRows && result.PreviousMonthRows >= 0 && result.PreviousMonthRows < cfg.MinMonthRows && (result.PreviousMonthRows == 0 || float64(result.BoundaryMonthRows)/float64(result.PreviousMonthRows) >= cfg.MinGrowthRatio)
		if !ok || result.Column != cfg.Column || err != nil || date.Year() < 1 || date.Day() != 1 || !date.Before(reference) || date.Before(reference.AddDate(0, -cfg.LookbackMonths, 0)) || result.ExcludedRows < 0 || !validCounts {
			return nil, fmt.Errorf("invalid saved snapshot cutoff for %s", key)
		}
	}
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return nil, err
	}
	filters := make(map[string]config.IcebergSnapshotReplaceFilterConfig, len(sink.SnapshotReplaceFilters)+len(selected))
	for key, filter := range sink.SnapshotReplaceFilters {
		filters[key] = filter
	}
	sourceConfigs := make(map[string]config.MySQLTableConfig, len(s.cfg.TableConfigs)+len(selected))
	for key, cfg := range s.cfg.TableConfigs {
		sourceConfigs[key] = cfg
	}
	for key, result := range plan.Tables {
		ref := refs[key]
		cfg, _ := s.tableConfigForTable(ref.Schema, ref.Table)
		cfg.Filter = fmt.Sprintf("`%s` >= '%s'", result.Column, result.Value)
		cfg.SnapshotCutoff = nil
		sourceConfigs[key] = cfg
		filters[key] = config.IcebergSnapshotReplaceFilterConfig{Column: strings.ToLower(result.Column), Op: ">=", Value: result.Value}
		log.Printf("[mysql][job %s] snapshot_cutoff table=%s column=%s cutoff=%s excluded_rows=%d boundary_month_rows=%d previous_month_rows=%d submission=%s", s.jobID, key, result.Column, result.Value, result.ExcludedRows, result.BoundaryMonthRows, result.PreviousMonthRows, jctx.SubmissionID)
		s.reportProgress(connector.ProgressInfo{Phase: "preflight", Summary: "Snapshot cutoff resolved", Detail: fmt.Sprintf("%s | cutoff %s | %d older/invalid rows excluded", key, result.Value, result.ExcludedRows), CurrentTable: key})
	}
	s.cfg.TableConfigs = sourceConfigs
	sink.SnapshotReplaceFilters = filters
	sink.SnapshotCutoffResolved = true
	return sink, nil
}

func (s *Source) checkNewSnapshotScope(ctx context.Context) error {
	keys := []string{s.checkpointKey()}
	if s.jobID != s.checkpointKey() {
		keys = append(keys, s.jobID)
	}
	for _, key := range keys {
		state, err := s.offsetSto.GetSnapshotState(ctx, key)
		if err != nil {
			return err
		}
		offset, err := s.offsetSto.GetOffset(ctx, key)
		if err != nil {
			return err
		}
		progress, err := s.offsetSto.GetSnapshotProgress(ctx, key)
		if err != nil {
			return err
		}
		if state != nil || offset != nil || progress != nil {
			return fmt.Errorf("snapshot_cutoff cannot change an existing checkpoint without a saved cutoff; use a controlled new snapshot submission")
		}
	}
	checker, ok := s.offsetSto.(meta.JobStateCleanupChecker)
	if !ok {
		return fmt.Errorf("snapshot_cutoff metadata store cannot verify existing Iceberg commit intents")
	}
	for _, key := range keys {
		if err := checker.CheckJobStateCleanup(ctx, key); err != nil {
			return err
		}
	}
	return meta.CheckOwnership(ctx, s.offsetSto)
}

func (s *Source) requireResolvedSnapshotCutoffs() error {
	for _, table := range s.tableRefs() {
		cfg, _ := s.tableConfigForTable(table.Schema, table.Table)
		if cfg.SnapshotCutoff != nil {
			return fmt.Errorf("snapshot_cutoff for %s.%s must be resolved with its target scope before reading rows", table.Schema, table.Table)
		}
	}
	return nil
}
