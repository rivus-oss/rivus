package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

func (s *Sink) commitSchemaTransaction(ctx context.Context, txn *icetable.Transaction, original *icetable.Table, replacements map[int]int, progress commitProgress) (*icetable.Table, error) {
	rewrite := len(replacements) > 0 && original.CurrentSnapshot() != nil
	if rewrite {
		policy := normalizeAutoMigrationConfig(s.cfg.AutoMigration)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(policy.TimeoutSeconds)*time.Second)
		defer cancel()
		progress.operation = "schema-rewrite"
		// Schema reconciliation must not consume the enclosing source row's
		// replay identity: that row has not been written yet.
		ctx = withIcebergBatch(ctx, icebergBatch{kind: "internal"})
		log.Printf("[iceberg][job %s] auto-migration source=%s target=%s columns=%d max_table_bytes=%d timeout_seconds=%d",
			s.jobID, progress.sourceKey, tableKey(progress.targetNamespace, progress.targetTable), len(replacements), policy.MaxTableBytes, policy.TimeoutSeconds)
	}
	progress.targetUUID = original.Metadata().TableUUID().String()
	var updated *icetable.Table
	progress.recoveredTable = &updated
	err := s.withCommitSlot(ctx, progress, func(ctx context.Context) error {
		if rewrite {
			if err := rewriteSchemaData(ctx, txn, original, replacements, int64(normalizeAutoMigrationConfig(s.cfg.AutoMigration).MaxTableBytes)); err != nil {
				return icebergPreparationFailed(err)
			}
		}
		if err := txn.SetProperties(icebergCommitProperties(ctx, nil)); err != nil {
			return icebergPreparationFailed(err)
		}
		var err error
		updated, err = s.commitStagedSchemaTransaction(ctx, txn, original)
		var preparation *icebergPreparationError
		if errors.As(err, &preparation) {
			return err
		}
		return classifyCatalogCommitError(updated, err)
	})
	return updated, err
}

// iceberg-go accumulates a current-schema assertion for each staged schema.
// The catalog must check only the original schema: intermediate schemas are
// introduced by this very commit and cannot exist before it. Submit the staged
// payload once, preserving the UUID, snapshot and default-shape requirements.
// The pinned snapshot on data rewrites prevents replay against newer data.
func (s *Sink) commitStagedSchemaTransaction(ctx context.Context, txn *icetable.Transaction, original *icetable.Table) (*icetable.Table, error) {
	if s.catalog == nil {
		return txn.Commit(ctx)
	}
	payload, err := txn.TableCommit()
	if err != nil {
		return nil, icebergPreparationFailed(err)
	}
	if len(payload.Updates) == 0 {
		return original, nil
	}
	schemaAssertions := 0
	for _, requirement := range payload.Requirements {
		if requirement.GetType() == "assert-current-schema-id" {
			schemaAssertions++
		}
	}
	if schemaAssertions <= 1 {
		return txn.Commit(ctx)
	}
	requirements := make([]icetable.Requirement, 0, len(payload.Requirements))
	schemaAsserted := false
	for _, requirement := range payload.Requirements {
		if requirement.GetType() == "assert-current-schema-id" {
			if !schemaAsserted {
				requirements = append(requirements, icetable.AssertCurrentSchemaID(original.Schema().ID))
				schemaAsserted = true
			}
			continue
		}
		requirements = append(requirements, requirement)
	}
	requirements = append(requirements, icetable.AssertLastAssignedFieldID(original.Metadata().LastColumnID()))
	metadata, location, err := s.catalog.CommitTable(ctx, payload.Identifier, requirements, payload.Updates)
	if err != nil {
		return nil, err
	}
	txn.MarkCommitted()
	updated := icetable.New(payload.Identifier, metadata, location, original.FS, s.catalog)
	for _, update := range payload.Updates {
		err = errors.Join(err, update.PostCommit(ctx, original, updated))
	}
	return updated, err
}

// The map links replacement field IDs to the original scan's field IDs.
// Apply each action to staged metadata so CHANGE COLUMN and multi-action DDL
// resolve names and types against preceding actions, without a catalog commit.
func stageIcebergDDL(txn *icetable.Transaction, plan []ddlAction, cfg config.IcebergConfig, sourceKey string) (map[int]int, error) {
	replacements := make(map[int]int)
	for _, action := range plan {
		staged, err := txn.StagedTable()
		if err != nil {
			return nil, err
		}
		current := staged.Schema()
		if action.Kind == ddlActionUpdateColumn {
			desired, err := icebergTypeForColumn(action.Column)
			if err != nil {
				return nil, err
			}
			field, exists := current.FindFieldByNameCaseInsensitive(action.Column.Name)
			if exists && schemaRewriteRequired(field.Type, desired) {
				if err := schemaMigrationAllowed(normalizeAutoMigrationConfig(cfg.AutoMigration), sourceKey, field.Name, field.Type, desired); err != nil {
					return nil, err
				}
				if err := validateSchemaRewriteField(staged.Table, field); err != nil {
					return nil, err
				}
				if err := txn.AssertRefSnapshotID(""); err != nil {
					return nil, err
				}
				if err := txn.AssertDefaultShape(); err != nil {
					return nil, err
				}
				required := field.Required
				if cfg.AllowUnsafeTypeChanges {
					required = !action.Column.IsNullable
				}
				// Required replacement fields have no initial default: every live
				// row is rewritten before this transaction can become visible.
				update := txn.UpdateSchema(false, true)
				update.DeleteColumn([]string{field.Name})
				update.AddColumn([]string{field.Name}, desired, action.Column.ColumnType, required, nil)
				if err := update.Commit(); err != nil {
					return nil, fmt.Errorf("migrate column %q from %s to %s: %w", field.Name, field.Type, desired, err)
				}
				next, err := txn.StagedTable()
				if err != nil {
					return nil, err
				}
				replacement, _ := next.Schema().FindFieldByNameCaseInsensitive(field.Name)
				originalID := field.ID
				if id, ok := replacements[field.ID]; ok {
					originalID = id
				}
				replacements[replacement.ID] = originalID
				position := action
				position.Column.Name = field.Name
				if position.Position == "" || position.Position == model.ColumnPositionLast {
					for i, old := range current.Fields() {
						if old.ID == field.ID {
							position.Position = model.ColumnPositionFirst
							if i > 0 {
								position.Position = model.ColumnPositionAfter
								position.AfterColumn = current.Fields()[i-1].Name
							}
							break
						}
					}
				}
				move := txn.UpdateSchema(false, false)
				moveIcebergDDLColumn(move, position)
				if err := move.Commit(); err != nil {
					return nil, err
				}
				continue
			}
		}
		updater := txn.UpdateSchema(false, cfg.AllowUnsafeTypeChanges)
		if err := applyDDLAction(updater, action, cfg); err != nil {
			return nil, err
		}
		if err := updater.Commit(); err != nil {
			return nil, fmt.Errorf("%s column %q: %w", action.Kind, action.Column.Name, err)
		}
	}
	return replacements, nil
}

func validateSchemaRewriteField(tbl *icetable.Table, field iceberglib.NestedField) error {
	reason := ""
	if slices.Contains(tbl.Schema().IdentifierFieldIDs, field.ID) {
		reason = "an identifier field"
	}
	spec := tbl.Spec()
	for _, partition := range spec.Fields() {
		if slices.Contains(partition.SourceIDs, field.ID) {
			reason = "a partition source"
		}
	}
	order := tbl.SortOrder()
	for _, sort := range order.Fields() {
		if slices.Contains(sort.SourceIDs, field.ID) {
			reason = "a sort source"
		}
	}
	if reason != "" {
		return fmt.Errorf("column %q: automatic schema migration cannot replace %s; rebuild the target with the new schema", field.Name, reason)
	}
	return nil
}

// Read from the original immutable table, not the transaction's new schema.
// Arrow casts one batch at a time; large tables are never collected in memory.
func schemaMigrationReader(ctx context.Context, original *icetable.Table, desired *iceberglib.Schema, replacements map[int]int) (array.RecordReader, error) {
	outputSchema, err := icetable.SchemaToArrowSchema(desired, nil, false, false)
	if err != nil {
		return nil, err
	}
	inputSchema, records, err := original.Scan(icetable.WithArrowBatchSize(8192)).ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}
	inputIndices := make([]int, len(desired.Fields()))
	for i, field := range desired.Fields() {
		id := field.ID
		if originalID, ok := replacements[id]; ok {
			id = originalID
		}
		inputIndices[i] = -1
		if old, ok := original.Schema().FindFieldByID(id); ok {
			indices := inputSchema.FieldIndices(old.Name)
			if len(indices) != 1 {
				return nil, fmt.Errorf("rewrite source column %q is missing or ambiguous", old.Name)
			}
			inputIndices[i] = indices[0]
		}
	}
	return array.ReaderFromIter(outputSchema, func(yield func(arrow.RecordBatch, error) bool) {
		for record, scanErr := range records {
			if scanErr != nil {
				yield(nil, scanErr)
				return
			}
			converted, err := convertSchemaRewriteBatch(ctx, record, outputSchema, inputIndices)
			record.Release()
			if err != nil {
				yield(nil, err)
				return
			}
			// ReaderFromIter takes ownership of the yielded batch.
			if !yield(converted, nil) {
				return
			}
		}
	}), nil
}

func convertSchemaRewriteBatch(ctx context.Context, input arrow.RecordBatch, output *arrow.Schema, indices []int) (arrow.RecordBatch, error) {
	columns := make([]arrow.Array, output.NumFields())
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	for i, field := range output.Fields() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if indices[i] < 0 {
			columns[i] = array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, int(input.NumRows()))
			continue
		}
		column := input.Column(indices[i])
		if arrow.TypeEqual(column.DataType(), field.Type) {
			column.Retain()
			columns[i] = column
		} else {
			var err error
			columns[i], err = castSchemaMigrationColumn(ctx, column, field.Type)
			if err != nil {
				return nil, fmt.Errorf("rewrite column %q: %w", field.Name, err)
			}
		}
		if !field.Nullable && columns[i].NullN() > 0 {
			return nil, fmt.Errorf("rewrite column %q contains NULL but the target is required", field.Name)
		}
	}
	return array.NewRecordBatch(output, columns, input.NumRows()), nil
}

func rewriteSchemaData(ctx context.Context, txn *icetable.Transaction, original *icetable.Table, replacements map[int]int, maxBytes int64) error {
	staged, err := txn.StagedTable()
	if err != nil {
		return err
	}
	mapping, err := json.Marshal(staged.Schema().NameMapping())
	if err != nil {
		return err
	}
	if err := txn.SetProperties(iceberglib.Properties{icetable.DefaultNameMappingKey: string(mapping)}); err != nil {
		return err
	}
	// Retire every old delete file as well as the data files. Their effects are
	// already reflected by the scan; none may apply to the replacement rows.
	var deletes []iceberglib.DataFile
	var inputBytes int64
	seen := make(map[string]bool)
	if snapshot := original.CurrentSnapshot(); snapshot != nil {
		fs, err := original.FS(ctx)
		if err != nil {
			return err
		}
		manifests, err := snapshot.Manifests(fs)
		if err != nil {
			return err
		}
		for _, manifest := range manifests {
			for entry, err := range manifest.Entries(fs, true) {
				if err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				file := entry.DataFile()
				if !seen[file.FilePath()] {
					if size := file.FileSizeBytes(); size < 0 || size > maxBytes-inputBytes {
						return fmt.Errorf("automatic migration exceeds auto_migration.max_table_bytes=%d or has unknown file size; plan a larger explicit migration", maxBytes)
					} else {
						inputBytes += size
					}
					seen[file.FilePath()] = true
				}
				if file.ContentType() != iceberglib.EntryContentData {
					deletes = append(deletes, file)
				}
			}
		}
	}
	reader, err := schemaMigrationReader(ctx, original, staged.Schema(), replacements)
	if err != nil {
		return err
	}
	defer reader.Release()
	props := icebergCommitProperties(ctx, nil)
	if err := txn.Overwrite(ctx, reader, props); err != nil {
		return err
	}
	if len(deletes) > 0 {
		return txn.ReplaceFiles(ctx, nil, nil, deletes, props)
	}
	return nil
}
