package iceberg

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberglib "github.com/apache/iceberg-go"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

const (
	defaultMigrationMaxBytes = 1 << 30
	defaultMigrationTimeout  = 15 * 60
)

func normalizeAutoMigrationConfig(cfg config.IcebergAutoMigrationConfig) config.IcebergAutoMigrationConfig {
	if cfg.MaxTableBytes == 0 {
		cfg.MaxTableBytes = defaultMigrationMaxBytes
	}
	if cfg.TimeoutSeconds == 0 {
		cfg.TimeoutSeconds = defaultMigrationTimeout
	}
	tables := make([]string, 0, len(cfg.Tables))
	seen := make(map[string]bool)
	for _, table := range cfg.Tables {
		table = strings.ToLower(strings.TrimSpace(table))
		if !seen[table] {
			tables = append(tables, table)
			seen[table] = true
		}
	}
	cfg.Tables = tables
	return cfg
}

func resolveAutoMigrationTables(cfg config.IcebergAutoMigrationConfig, sourceTables []connector.TableRef) (config.IcebergAutoMigrationConfig, error) {
	cfg = normalizeAutoMigrationConfig(cfg)
	if !cfg.Enabled {
		return cfg, nil
	}
	if len(cfg.Tables) == 0 {
		for _, table := range sourceTables {
			if strings.TrimSpace(table.Schema) == "" || strings.TrimSpace(table.Table) == "" {
				return cfg, fmt.Errorf("auto_migration cannot infer a table without a source schema and table name")
			}
			cfg.Tables = append(cfg.Tables, strings.TrimSpace(table.Schema)+"."+strings.TrimSpace(table.Table))
		}
		cfg = normalizeAutoMigrationConfig(cfg)
		sort.Strings(cfg.Tables)
	}
	return cfg, validateAutoMigrationConfig(cfg)
}

func validateAutoMigrationConfig(cfg config.IcebergAutoMigrationConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if len(cfg.Tables) == 0 {
		return fmt.Errorf("auto_migration.enabled requires resolved source tables or explicit source tables in auto_migration.tables")
	}
	for _, table := range cfg.Tables {
		parts := strings.Split(table, ".")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.ContainsAny(table, "*?[") {
			return fmt.Errorf("auto_migration.tables requires exact source schema.table names, got %q", table)
		}
	}
	if cfg.MaxTableBytes <= 0 || cfg.TimeoutSeconds <= 0 {
		return fmt.Errorf("auto_migration.max_table_bytes and timeout_seconds must be positive")
	}
	return nil
}

func schemaMigrationAllowed(cfg config.IcebergAutoMigrationConfig, sourceKey, column string, current, desired iceberglib.Type) error {
	if !cfg.Enabled {
		return fmt.Errorf("column %q: Iceberg cannot promote %s to %s; automatic migration is disabled; enable auto_migration for selected source %q to authorize a validated table rewrite", column, current, desired, sourceKey)
	}
	if err := validateAutoMigrationConfig(cfg); err != nil {
		return err
	}
	allowed := false
	for _, table := range cfg.Tables {
		if strings.EqualFold(table, sourceKey) {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("column %q: source %q is not listed in auto_migration.tables", column, sourceKey)
	}
	if !schemaMigrationConversionSupported(current, desired) {
		return fmt.Errorf("column %q: automatic migration from %s to %s is unsupported; plan an explicit target rebuild", column, current, desired)
	}
	return nil
}

func schemaRewriteRequired(current, desired iceberglib.Type) bool {
	if current.Equals(desired) {
		return false
	}
	// Keep native evolution limited to Iceberg's widening promotions. Some
	// library casts also accept binary/text changes without validating UTF-8.
	switch from := current.(type) {
	case iceberglib.Int32Type:
		_, ok := desired.(iceberglib.Int64Type)
		return !ok
	case iceberglib.Float32Type:
		_, ok := desired.(iceberglib.Float64Type)
		return !ok
	case iceberglib.DecimalType:
		to, ok := desired.(iceberglib.DecimalType)
		return !ok || from.Scale() != to.Scale() || from.Precision() > to.Precision()
	default:
		return true
	}
}

// Preserve the existing wider writer and legacy MySQL boolean mapping unless
// the operator opts into aligning types through a data migration.
func schemaWriterCanKeepType(current, desired iceberglib.Type) bool {
	if icebergTypeCanRepresent(current, desired) {
		return true
	}
	_, currentBool := current.(iceberglib.BooleanType)
	_, desiredBool := desired.(iceberglib.BooleanType)
	return currentBool && migrationTypeCategory(desired) == "number" || desiredBool && migrationTypeCategory(current) == "number"
}

func migrationTypeCategory(typ iceberglib.Type) string {
	switch typ.(type) {
	case iceberglib.Int32Type, iceberglib.Int64Type, iceberglib.Float32Type, iceberglib.Float64Type, iceberglib.DecimalType:
		return "number"
	case iceberglib.StringType:
		return "text"
	case iceberglib.BooleanType:
		return "boolean"
	case iceberglib.DateType, iceberglib.TimestampType, iceberglib.TimestampTzType, iceberglib.TimestampNsType, iceberglib.TimestampTzNsType:
		return "temporal"
	case iceberglib.BinaryType:
		return "binary"
	default:
		return "unsupported"
	}
}

func schemaMigrationConversionSupported(current, desired iceberglib.Type) bool {
	from, to := migrationTypeCategory(current), migrationTypeCategory(desired)
	if from == "unsupported" || to == "unsupported" {
		return false
	}
	if from == "text" || to == "text" {
		return true
	}
	if (from == "number" || from == "boolean") && (to == "number" || to == "boolean") {
		return true
	}
	return from == "temporal" && to == "temporal"
}

func arrowText(typ arrow.DataType) bool {
	return typ.ID() == arrow.STRING || typ.ID() == arrow.LARGE_STRING
}

// SafeCast rejects parsing errors and overflow. For typed inputs, also compare
// an exact round trip: Arrow's safe casts alone can still round floating-point
// values or discard a timestamp's time of day. Text inputs use strict parsing,
// allowing valid alternate spellings such as "0018" to become integer 18.
func castSchemaMigrationColumn(ctx context.Context, input arrow.Array, desired arrow.DataType) (arrow.Array, error) {
	output, err := rawSchemaMigrationCast(ctx, input, desired)
	if err != nil {
		return nil, err
	}
	if arrowText(input.DataType()) {
		return output, nil
	}
	roundTrip, err := rawSchemaMigrationCast(ctx, output, input.DataType())
	if err != nil {
		output.Release()
		return nil, fmt.Errorf("conversion cannot preserve the original values: %w", err)
	}
	defer roundTrip.Release()
	if !array.ApproxEqual(input, roundTrip, array.WithNaNsEqual(true), array.WithAbsTolerance(0)) {
		output.Release()
		return nil, fmt.Errorf("conversion from %s to %s would lose data", input.DataType(), desired)
	}
	return output, nil
}

func rawSchemaMigrationCast(ctx context.Context, input arrow.Array, desired arrow.DataType) (arrow.Array, error) {
	if !arrowText(input.DataType()) && !arrowText(desired) {
		return compute.CastArray(ctx, input, compute.SafeCastOptions(desired))
	}
	builder := array.NewBuilder(memory.DefaultAllocator, desired)
	defer builder.Release()
	builder.Reserve(input.Len())
	for i := 0; i < input.Len(); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input.IsNull(i) {
			builder.AppendNull()
			continue
		}
		value := input.ValueStr(i)
		switch values := input.(type) {
		case *array.Boolean:
			value = "0"
			if values.Value(i) {
				value = "1"
			}
		case *array.Binary:
			value = string(values.Value(i))
		case *array.LargeBinary:
			value = string(values.Value(i))
		case *array.Timestamp:
			typ := values.DataType().(*arrow.TimestampType)
			value = values.Value(i).ToTime(typ.Unit).UTC().Format("2006-01-02 15:04:05.999999999")
			if typ.TimeZone != "" {
				value += "+00:00"
			}
		}
		if !utf8.ValidString(value) {
			return nil, fmt.Errorf("conversion contains invalid UTF-8")
		}
		switch out := builder.(type) {
		case *array.StringBuilder:
			out.Append(value)
		case *array.LargeStringBuilder:
			out.Append(value)
		case *array.BinaryBuilder:
			out.Append([]byte(value))
		case *array.Date32Builder:
			date, err := time.Parse("2006-01-02", value)
			if err != nil {
				return nil, fmt.Errorf("invalid date value")
			}
			out.Append(arrow.Date32FromTime(date))
		default:
			// Builder parsing is base ten for integers. Do not let the Arrow
			// null sentinel turn a non-null source string into a null result.
			if value == array.NullValueStr {
				return nil, fmt.Errorf("non-null text cannot become NULL")
			}
			if err := builder.AppendValueFromString(value); err != nil {
				return nil, fmt.Errorf("invalid value for destination type %s", desired)
			}
		}
	}
	output := builder.NewArray()
	if desired.ID() == arrow.DECIMAL128 {
		for i := 0; i < input.Len(); i++ {
			if input.IsNull(i) {
				continue
			}
			before, okBefore := new(big.Rat).SetString(input.ValueStr(i))
			after, okAfter := new(big.Rat).SetString(output.ValueStr(i))
			if !okBefore || !okAfter || before.Cmp(after) != 0 {
				output.Release()
				return nil, fmt.Errorf("decimal conversion would lose data")
			}
		}
	}
	return output, nil
}
