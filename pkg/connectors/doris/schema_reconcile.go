package doris

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/model"
)

const dorisSchemaChangeWaitTimeout = 10 * time.Minute

type dorisTargetColumn struct {
	Name       string
	Type       string
	IsNullable bool
	IsKey      bool
}

type dorisDesiredColumn struct {
	SourceName string
	TargetName string
	Type       string
	IsNullable bool
	IsKey      bool
}

type dorisSchemaChangeKind string

const (
	dorisSchemaAddColumn    dorisSchemaChangeKind = "add_column"
	dorisSchemaModifyColumn dorisSchemaChangeKind = "modify_column"
)

type dorisSchemaChange struct {
	Kind    dorisSchemaChangeKind
	Current *dorisTargetColumn
	Desired dorisDesiredColumn
}

func desiredDorisColumns(sourceSchema *model.TableSchema) []dorisDesiredColumn {
	if sourceSchema == nil {
		return nil
	}

	pkIndexes := make(map[int]struct{})
	nonPKIndexes := make([]int, 0, len(sourceSchema.Columns))
	for i, col := range sourceSchema.Columns {
		if col.IsPK {
			pkIndexes[i] = struct{}{}
		} else {
			nonPKIndexes = append(nonPKIndexes, i)
		}
	}
	if len(pkIndexes) == 0 && len(sourceSchema.Columns) > 0 {
		pkIndexes[0] = struct{}{}
		filtered := nonPKIndexes[:0]
		for _, index := range nonPKIndexes {
			if index != 0 {
				filtered = append(filtered, index)
			}
		}
		nonPKIndexes = filtered
	}

	orderedIndexes := make([]int, 0, len(sourceSchema.Columns))
	for i := range sourceSchema.Columns {
		if _, isKey := pkIndexes[i]; isKey {
			orderedIndexes = append(orderedIndexes, i)
		}
	}
	orderedIndexes = append(orderedIndexes, nonPKIndexes...)

	usedTargetNames := make(map[string]int, len(sourceSchema.Columns))
	desired := make([]dorisDesiredColumn, 0, len(sourceSchema.Columns))
	for _, i := range orderedIndexes {
		col := sourceSchema.Columns[i]
		_, isKey := pkIndexes[i]
		desired = append(desired, dorisDesiredColumn{
			SourceName: col.Name,
			TargetName: sanitizeDorisColumnName(col.Name, len(desired), usedTargetNames),
			Type:       mapMySQLColumnToDoris(col, isKey),
			IsNullable: col.IsNullable,
			IsKey:      isKey,
		})
	}
	return desired
}

func planDorisSchemaChanges(current []dorisTargetColumn, desired []dorisDesiredColumn) ([]dorisSchemaChange, error) {
	currentByName := make(map[string]dorisTargetColumn, len(current))
	for _, col := range current {
		currentByName[strings.ToLower(strings.TrimSpace(col.Name))] = col
	}

	changes := make([]dorisSchemaChange, 0)
	for _, desiredCol := range desired {
		currentCol, ok := currentByName[strings.ToLower(strings.TrimSpace(desiredCol.TargetName))]
		if !ok {
			if desiredCol.IsKey {
				return nil, fmt.Errorf(
					"target is missing source key column %q; recreate the Doris table so its UNIQUE KEY can be rebuilt safely",
					desiredCol.TargetName,
				)
			}
			changes = append(changes, dorisSchemaChange{
				Kind:    dorisSchemaAddColumn,
				Desired: desiredCol,
			})
			continue
		}

		if currentCol.IsKey != desiredCol.IsKey {
			return nil, fmt.Errorf(
				"key mismatch for column %q: Doris key=%t but source key=%t; recreate the Doris table so its UNIQUE KEY can be rebuilt safely",
				desiredCol.TargetName,
				currentCol.IsKey,
				desiredCol.IsKey,
			)
		}

		typeCompatible := dorisTypeCanRepresent(currentCol.Type, desiredCol.Type)
		needsNullableRelaxation := desiredCol.IsNullable && !currentCol.IsNullable
		if typeCompatible && !needsNullableRelaxation {
			continue
		}
		if !typeCompatible && !isSafeDorisTypeWidening(currentCol.Type, desiredCol.Type) {
			return nil, fmt.Errorf(
				"unsafe type mismatch for column %q: Doris has %s but source requires %s; alter it manually or recreate the target table",
				desiredCol.TargetName,
				currentCol.Type,
				desiredCol.Type,
			)
		}

		currentCopy := currentCol
		changes = append(changes, dorisSchemaChange{
			Kind:    dorisSchemaModifyColumn,
			Current: &currentCopy,
			Desired: desiredCol,
		})
	}

	return changes, nil
}

func normalizeDorisTypeName(raw string) string {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	raw = strings.Join(strings.Fields(raw), "")
	switch {
	case raw == "TEXT":
		return "STRING"
	case strings.HasPrefix(raw, "DECIMALV2("):
		return "DECIMAL" + raw[len("DECIMALV2"):]
	case strings.HasPrefix(raw, "DECIMALV3("):
		return "DECIMAL" + raw[len("DECIMALV3"):]
	case raw == "INTEGER":
		return "INT"
	default:
		return raw
	}
}

func dorisTypeCanRepresent(currentRaw, desiredRaw string) bool {
	current := normalizeDorisTypeName(currentRaw)
	desired := normalizeDorisTypeName(desiredRaw)
	if current == desired {
		return true
	}
	if current == "STRING" && isDorisStringType(desired) {
		return true
	}

	currentBase, currentParams := splitDorisType(current)
	desiredBase, desiredParams := splitDorisType(desired)

	if isDorisStringType(current) && isDorisStringType(desired) {
		currentLen, currentBounded := dorisStringLength(currentBase, currentParams)
		desiredLen, desiredBounded := dorisStringLength(desiredBase, desiredParams)
		if !currentBounded {
			return true
		}
		return desiredBounded && currentLen >= desiredLen
	}

	if currentRank, ok := dorisIntegerRank(currentBase); ok {
		if desiredRank, desiredOK := dorisIntegerRank(desiredBase); desiredOK {
			return currentRank >= desiredRank
		}
	}

	if currentBase == "DOUBLE" && desiredBase == "FLOAT" {
		return true
	}
	if currentBase == "DATETIME" && desiredBase == "DATE" {
		return true
	}
	if strings.HasPrefix(currentBase, "DATETIMEV2") && (desiredBase == "DATETIME" || desiredBase == "DATE") {
		return true
	}
	if currentBase == "DECIMAL" && desiredBase == "DECIMAL" {
		return decimalCanRepresent(currentParams, desiredParams)
	}

	return false
}

func isSafeDorisTypeWidening(currentRaw, desiredRaw string) bool {
	current := normalizeDorisTypeName(currentRaw)
	desired := normalizeDorisTypeName(desiredRaw)
	currentBase, currentParams := splitDorisType(current)
	desiredBase, desiredParams := splitDorisType(desired)

	if desired == "STRING" && currentBase != "DATE" && currentBase != "DATETIME" && !strings.HasPrefix(currentBase, "DATETIMEV2") {
		return true
	}
	if isDorisStringType(current) && isDorisStringType(desired) {
		currentLen, currentBounded := dorisStringLength(currentBase, currentParams)
		desiredLen, desiredBounded := dorisStringLength(desiredBase, desiredParams)
		return currentBounded && (!desiredBounded || desiredLen > currentLen)
	}
	if currentRank, ok := dorisIntegerRank(currentBase); ok {
		if desiredRank, desiredOK := dorisIntegerRank(desiredBase); desiredOK {
			return desiredRank > currentRank
		}
	}
	if currentBase == "FLOAT" && desiredBase == "DOUBLE" {
		return true
	}
	if currentBase == "DATE" && (desiredBase == "DATETIME" || strings.HasPrefix(desiredBase, "DATETIMEV2")) {
		return true
	}
	if currentBase == "DECIMAL" && desiredBase == "DECIMAL" {
		return decimalCanRepresent(desiredParams, currentParams)
	}
	return false
}

func isDorisStringType(normalized string) bool {
	base, _ := splitDorisType(normalized)
	switch base {
	case "CHAR", "VARCHAR", "STRING":
		return true
	default:
		return false
	}
}

func splitDorisType(normalized string) (string, []int) {
	if idx := strings.IndexByte(normalized, '('); idx >= 0 {
		return normalized[:idx], parseTypeParams(normalized)
	}
	return normalized, nil
}

func dorisStringLength(base string, params []int) (int, bool) {
	if base == "STRING" {
		return 0, false
	}
	if len(params) == 0 {
		return 0, true
	}
	return params[0], true
}

func dorisIntegerRank(base string) (int, bool) {
	switch base {
	case "TINYINT":
		return 1, true
	case "SMALLINT":
		return 2, true
	case "INT":
		return 3, true
	case "BIGINT":
		return 4, true
	case "LARGEINT":
		return 5, true
	default:
		return 0, false
	}
}

func decimalCanRepresent(currentParams, desiredParams []int) bool {
	if len(currentParams) < 2 || len(desiredParams) < 2 {
		return false
	}
	currentPrecision, currentScale := currentParams[0], currentParams[1]
	desiredPrecision, desiredScale := desiredParams[0], desiredParams[1]
	return currentScale >= desiredScale &&
		currentPrecision-currentScale >= desiredPrecision-desiredScale
}

func dorisSchemaChangeSQL(database, table string, change dorisSchemaChange) string {
	switch change.Kind {
	case dorisSchemaAddColumn:
		// Existing rows have no value for a newly discovered source column, so
		// add it nullable even when the source column is required.
		return fmt.Sprintf(
			"ALTER TABLE %s.%s ADD COLUMN %s %s NULL",
			quoteDorisIdentifier(database),
			quoteDorisIdentifier(table),
			quoteDorisIdentifier(change.Desired.TargetName),
			change.Desired.Type,
		)
	case dorisSchemaModifyColumn:
		stmt := fmt.Sprintf(
			"ALTER TABLE %s.%s MODIFY COLUMN %s %s",
			quoteDorisIdentifier(database),
			quoteDorisIdentifier(table),
			quoteDorisIdentifier(change.Desired.TargetName),
			change.Desired.Type,
		)
		if change.Current != nil && change.Current.IsKey {
			stmt += " KEY"
		}
		if change.Desired.IsNullable || (change.Current != nil && change.Current.IsNullable) {
			stmt += " NULL"
		} else {
			stmt += " NOT NULL"
		}
		return stmt
	default:
		return ""
	}
}

func dorisSchemaChangeApplied(current []dorisTargetColumn, change dorisSchemaChange) bool {
	for _, col := range current {
		if !strings.EqualFold(strings.TrimSpace(col.Name), strings.TrimSpace(change.Desired.TargetName)) {
			continue
		}
		if !dorisTypeCanRepresent(col.Type, change.Desired.Type) {
			return false
		}
		return !change.Desired.IsNullable || col.IsNullable
	}
	return false
}

func (s *Sink) reconcileTableSchemaOnce(
	ctx context.Context,
	targetDB, targetTable string,
	desired []dorisDesiredColumn,
) error {
	current, err := s.fetchDorisTableColumns(ctx, targetDB, targetTable)
	if err != nil {
		return fmt.Errorf("inspect Doris schema %s.%s: %w", targetDB, targetTable, err)
	}

	changes, err := planDorisSchemaChanges(current, desired)
	if err != nil {
		return fmt.Errorf("reconcile Doris schema %s.%s: %w", targetDB, targetTable, err)
	}
	for _, change := range changes {
		stmt := dorisSchemaChangeSQL(targetDB, targetTable, change)
		if stmt == "" {
			continue
		}
		currentType := "(missing)"
		if change.Current != nil {
			currentType = change.Current.Type
		}
		log.Printf(
			"[doris][job %s] startup schema reconcile target=%s.%s action=%s column=%s current=%s desired=%s",
			s.jobID,
			targetDB,
			targetTable,
			change.Kind,
			change.Desired.TargetName,
			currentType,
			change.Desired.Type,
		)
		if err := s.execDDL(ctx, stmt); err != nil {
			return fmt.Errorf("reconcile Doris schema %s.%s: %w | stmt=%s", targetDB, targetTable, err, stmt)
		}
		if err := s.waitForDorisSchemaChange(ctx, targetDB, targetTable, change); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) waitForDorisSchemaChange(
	ctx context.Context,
	targetDB, targetTable string,
	change dorisSchemaChange,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, dorisSchemaChangeWaitTimeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		current, err := s.fetchDorisTableColumns(waitCtx, targetDB, targetTable)
		if err == nil && dorisSchemaChangeApplied(current, change) {
			return nil
		}

		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf(
				"timed out waiting for Doris schema reconciliation %s.%s column=%s type=%s",
				targetDB,
				targetTable,
				change.Desired.TargetName,
				change.Desired.Type,
			)
		case <-ticker.C:
		}
	}
}

func (s *Sink) fetchDorisTableColumns(ctx context.Context, db, table string) ([]dorisTargetColumn, error) {
	q := fmt.Sprintf("DESC %s.%s", quoteDorisIdentifier(db), quoteDorisIdentifier(table))
	rows, err := s.sqlDB.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resultColumns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	fieldIdx := findResultColumn(resultColumns, "Field")
	typeIdx := findResultColumn(resultColumns, "Type")
	nullIdx := findResultColumn(resultColumns, "Null")
	keyIdx := findResultColumn(resultColumns, "Key")
	if fieldIdx < 0 || typeIdx < 0 {
		return nil, fmt.Errorf("DESC result missing Field/Type columns: %v", resultColumns)
	}

	out := make([]dorisTargetColumn, 0, 64)
	for rows.Next() {
		values := make([]sql.RawBytes, len(resultColumns))
		dest := make([]any, len(resultColumns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		name := strings.TrimSpace(string(values[fieldIdx]))
		if name == "" {
			continue
		}
		nullText := ""
		if nullIdx >= 0 {
			nullText = strings.TrimSpace(string(values[nullIdx]))
		}
		keyText := ""
		if keyIdx >= 0 {
			keyText = strings.TrimSpace(string(values[keyIdx]))
		}
		out = append(out, dorisTargetColumn{
			Name:       name,
			Type:       strings.TrimSpace(string(values[typeIdx])),
			IsNullable: strings.EqualFold(nullText, "yes") || strings.EqualFold(nullText, "true"),
			IsKey:      strings.EqualFold(keyText, "true") || strings.EqualFold(keyText, "pri"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func findResultColumn(columns []string, name string) int {
	for i, col := range columns {
		if strings.EqualFold(strings.TrimSpace(col), name) {
			return i
		}
	}
	return -1
}
