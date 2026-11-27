package config

import (
	"fmt"
	"math"
	"regexp"
	"strings"
)

var snapshotCutoffColumn = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Older workers reject this write mode instead of ignoring snapshot_cutoff and
// starting an unfiltered snapshot. The factory translates it only after resolution.
const IcebergSnapshotAutoCutoffWriteMode = "auto-cutoff-v1"

func NormalizeMySQLSnapshotCutoff(c MySQLSnapshotCutoffConfig) MySQLSnapshotCutoffConfig {
	c.Column = strings.TrimSpace(c.Column)
	if c.MinMonthRows == 0 {
		c.MinMonthRows = 10000
	}
	if c.MinGrowthRatio == 0 {
		c.MinGrowthRatio = 10
	}
	if c.LookbackMonths == 0 {
		c.LookbackMonths = 24
	}
	if c.QueryTimeoutSeconds == 0 {
		c.QueryTimeoutSeconds = 30
	}
	return c
}

func ValidateMySQLSnapshotCutoff(c MySQLTableConfig) error {
	if c.SnapshotCutoff == nil {
		return nil
	}
	if strings.TrimSpace(c.Filter) != "" {
		return fmt.Errorf("snapshot_cutoff and filter cannot be configured together")
	}
	v := NormalizeMySQLSnapshotCutoff(*c.SnapshotCutoff)
	if !snapshotCutoffColumn.MatchString(v.Column) {
		return fmt.Errorf("snapshot_cutoff.column must be a single column name")
	}
	if v.MinMonthRows < 1 || v.MinGrowthRatio <= 1 || math.IsNaN(v.MinGrowthRatio) || math.IsInf(v.MinGrowthRatio, 0) {
		return fmt.Errorf("snapshot_cutoff requires min_month_rows > 0 and finite min_growth_ratio > 1")
	}
	if v.LookbackMonths < 1 || v.LookbackMonths > 120 || v.QueryTimeoutSeconds < 1 || v.QueryTimeoutSeconds > 600 {
		return fmt.Errorf("snapshot_cutoff requires lookback_months in 1..120 and query_timeout_seconds in 1..600")
	}
	return nil
}
