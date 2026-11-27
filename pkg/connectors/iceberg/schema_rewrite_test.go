package iceberg

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

func integerStringTestSink(t *testing.T, typ iceberglib.Type, rows []map[string]interface{}) (*Sink, *tableState) {
	t.Helper()
	ctx := context.Background()
	cat := recoveryCatalog(t)
	schema := iceberglib.NewSchemaWithIdentifiers(0, []int{1},
		iceberglib.NestedField{ID: 1, Name: "id", Type: iceberglib.PrimitiveTypes.Int64, Required: true},
		iceberglib.NestedField{ID: 2, Name: "LayoutPromo", Type: typ},
		iceberglib.NestedField{ID: 3, Name: "status", Type: iceberglib.PrimitiveTypes.String},
	)
	tbl, err := cat.CreateTable(ctx, icetable.Identifier{"bronze", "promos"}, schema,
		icecatalog.WithProperties(iceberglib.Properties{icetable.PropertyFormatVersion: "2"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) > 0 {
		reader, release, err := buildRecordReader(tbl.Schema(), rows)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err = tbl.Append(ctx, reader, nil)
		release()
		if err != nil {
			t.Fatal(err)
		}
	}
	dataType := "int"
	if typ.Equals(iceberglib.PrimitiveTypes.Int64) {
		dataType = "bigint"
	}
	source := &model.TableSchema{SchemaName: "app", TableName: "promos", Columns: []model.TableColumn{
		{Name: "id", DataType: "bigint", IsPK: true},
		{Name: "LayoutPromo", DataType: dataType, IsNullable: true},
		{Name: "status", DataType: "varchar", IsNullable: true},
	}}
	state := &tableState{sourceKey: "app.promos", targetNamespace: "bronze", targetTable: "promos", table: tbl, sourceSchema: source}
	sink := &Sink{
		jobID: "schema-test", catalog: cat,
		cfg: normalizeIcebergConfig(config.IcebergConfig{
			Overrides:     map[string]config.IcebergTarget{"app.promos": {Namespace: "bronze", Table: "promos"}},
			AutoMigration: config.IcebergAutoMigrationConfig{Enabled: true, Tables: []string{"app.promos"}},
		}),
		retry:  config.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond},
		states: map[string]*tableState{state.sourceKey: state}, sourceSchemas: map[string]*model.TableSchema{state.sourceKey: source},
	}
	return sink, state
}

func integerStringTestRows(t *testing.T, tbl *icetable.Table) map[int64]map[string]interface{} {
	t.Helper()
	_, records, err := tbl.Scan(icetable.WithArrowBatchSize(1)).ToArrowRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows := make(map[int64]map[string]interface{})
	for record, err := range records {
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range arrowRecordBatchRows(record) {
			id := row["id"].(int64)
			if _, exists := rows[id]; exists {
				t.Fatalf("duplicate id %d", id)
			}
			rows[id] = row
		}
		record.Release()
	}
	return rows
}

const layoutStringDDL = "ALTER TABLE `tbl_md_promo` CHANGE COLUMN `LayoutPromo` `LayoutPromo` VARCHAR(255) NULL DEFAULT NULL"

func TestIntegerStringDDLRewritesExistingRows(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   iceberglib.Type
		value int64
		want  string
	}{
		{"int", iceberglib.PrimitiveTypes.Int32, -2147483648, "-2147483648"},
		{"long", iceberglib.PrimitiveTypes.Int64, math.MaxInt64, "9223372036854775807"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink, state := integerStringTestSink(t, tc.typ, []map[string]interface{}{
				{"id": int64(1), "LayoutPromo": tc.value, "status": "keep"},
				{"id": int64(2), "LayoutPromo": nil, "status": "null"},
			})
			original := state.table
			oldField, _ := original.Schema().FindFieldByName("LayoutPromo")
			ev := model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL}
			if err := sink.applyDDL(context.Background(), state, ev); err != nil {
				t.Fatal(err)
			}
			field, _ := state.table.Schema().FindFieldByName("LayoutPromo")
			if !field.Type.Equals(iceberglib.PrimitiveTypes.String) || field.ID == oldField.ID {
				t.Fatalf("replacement field = %+v, old ID = %d", field, oldField.ID)
			}
			fields := state.table.Schema().Fields()
			if fields[1].Name != "LayoutPromo" || fields[2].Name != "status" {
				t.Fatalf("column order changed: %v", fields)
			}
			rows := integerStringTestRows(t, state.table)
			if len(rows) != 2 || rows[1]["LayoutPromo"] != tc.want || rows[2]["LayoutPromo"] != nil || rows[1]["status"] != "keep" {
				t.Fatalf("converted rows: %v", rows)
			}
			// An old immutable table handle still reads the original int files.
			if before := integerStringTestRows(t, original); before[1]["LayoutPromo"] != tc.value {
				t.Fatalf("original snapshot was changed: %v", before)
			}
			id := state.table.CurrentSnapshot().SnapshotID
			if err := sink.applyDDL(context.Background(), state, ev); err != nil {
				t.Fatal(err)
			}
			if state.table.CurrentSnapshot().SnapshotID != id {
				t.Fatal("replayed DDL rewrote the table a second time")
			}
			reader, release, err := buildRecordReader(state.table.Schema(), []map[string]interface{}{{"id": int64(3), "LayoutPromo": "18,19", "status": "new"}})
			if err != nil {
				t.Fatal(err)
			}
			updated, err := state.table.Append(context.Background(), reader, nil)
			release()
			if err != nil {
				t.Fatal(err)
			}
			if rows := integerStringTestRows(t, updated); len(rows) != 3 || rows[3]["LayoutPromo"] != "18,19" {
				t.Fatalf("new string value: %v", rows)
			}
		})
	}
}

func TestIntegerStringSchemaSyncRewritesExistingRows(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
	source := copyTableSchema(state.sourceSchema)
	source.Columns[1].DataType, source.Columns[1].ColumnType = "varchar", "varchar(255)"
	updated, err := sink.syncTableSchema(context.Background(), state.table, "bronze", "promos", source, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if rows := integerStringTestRows(t, updated); len(rows) != 1 || rows[1]["LayoutPromo"] != "18" {
		t.Fatalf("startup reconciliation did not convert integers: %v", rows)
	}
}

func TestIntegerStringDDLEmptyTableAndRename(t *testing.T) {
	for _, populated := range []bool{false, true} {
		var seed []map[string]interface{}
		if populated {
			seed = []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18), "status": "keep"}}
		}
		sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, seed)
		sink.cfg.AllowRenameColumn = true
		ev := model.Event{Type: model.EventTypeDDL, DDL: "ALTER TABLE promos CHANGE COLUMN LayoutPromo layouts varchar(255) NULL FIRST, ADD COLUMN note varchar(20)"}
		if err := sink.applyDDL(context.Background(), state, ev); err != nil {
			t.Fatal(err)
		}
		if fields := state.table.Schema().Fields(); fields[0].Name != "layouts" || len(fields) != 4 {
			t.Fatalf("fields: %v", fields)
		}
		if populated {
			rows := integerStringTestRows(t, state.table)
			if rows[1]["layouts"] != "18" || rows[1]["note"] != nil || rows[1]["status"] != "keep" {
				t.Fatalf("renamed data: %v", rows)
			}
		}
	}
}

func TestIntegerStringDDLAppliesExistingEqualityDeletes(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{
		{"id": int64(1), "LayoutPromo": int32(18)}, {"id": int64(2), "LayoutPromo": int32(19)},
	})
	updated, err := sink.commitEqualityDelta(context.Background(), state, &reducedBatch{
		deleteKeys: []map[string]interface{}{{"id": int64(2)}},
		deleteRows: []pendingDelete{{key: map[string]interface{}{"id": int64(2)}, row: map[string]interface{}{"id": int64(2), "LayoutPromo": int32(19)}}},
		pkCols:     []string{"id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	state.table = updated
	if err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL}); err != nil {
		t.Fatal(err)
	}
	if rows := integerStringTestRows(t, state.table); len(rows) != 1 || rows[1]["LayoutPromo"] != "18" {
		t.Fatalf("deleted rows were resurrected: %v", rows)
	}
	inventory, err := scanActiveFileInventory(context.Background(), state.table, nativeMaintenanceSettings{TargetFileSizeBytes: 128 << 20, SmallFileSizeBytes: 32 << 20, MinSmallFiles: 2})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.EqualityDeletes != 0 || inventory.PositionDeletes != 0 {
		t.Fatalf("old delete files retained: %+v", inventory)
	}
}

func TestIntegerStringDDLAppliesExistingPositionDeletes(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{
		{"id": int64(1), "LayoutPromo": int32(18)}, {"id": int64(2), "LayoutPromo": int32(19)},
	})
	txn := state.table.NewTransaction()
	if err := txn.SetProperties(iceberglib.Properties{icetable.WriteDeleteModeKey: icetable.WriteModeMergeOnRead}); err != nil {
		t.Fatal(err)
	}
	var err error
	state.table, err = txn.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.table, err = state.table.Delete(context.Background(), iceberglib.EqualTo(iceberglib.Reference("id"), int64(2)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL}); err != nil {
		t.Fatal(err)
	}
	if rows := integerStringTestRows(t, state.table); len(rows) != 1 || rows[1]["LayoutPromo"] != "18" {
		t.Fatalf("position-deleted row was resurrected: %v", rows)
	}
}

func TestIntegerStringDDLRejectsPartitionAndSortSources(t *testing.T) {
	for _, kind := range []string{"partition", "sort"} {
		t.Run(kind, func(t *testing.T) {
			sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, nil)
			txn := state.table.NewTransaction()
			var err error
			if kind == "partition" {
				err = txn.UpdateSpec(false).AddField("LayoutPromo", iceberglib.IdentityTransform{}, "layout").Commit()
			} else {
				order, orderErr := icetable.NewSortOrder(1, []icetable.SortField{{SourceIDs: []int{2}, Transform: iceberglib.IdentityTransform{}, Direction: icetable.SortASC, NullOrder: icetable.NullsFirst}})
				if orderErr != nil {
					t.Fatal(orderErr)
				}
				err = txn.ReplaceSortOrder(order)
			}
			if err != nil {
				t.Fatal(err)
			}
			state.table, err = txn.Commit(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			err = sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL})
			if err == nil || !strings.Contains(err.Error(), kind+" source") {
				t.Fatalf("error = %v", err)
			}
			field, _ := state.table.Schema().FindFieldByName("LayoutPromo")
			if !field.Type.Equals(iceberglib.PrimitiveTypes.Int32) {
				t.Fatal("rejected rewrite changed the column type")
			}
		})
	}
}

func TestIntegerStringRewritePreparationFailurePreservesDataAndClearsIntent(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": nil}})
	sink.cfg.AllowUnsafeTypeChanges = true
	store := newGuardTestStore()
	sink.offsetSto, sink.stateKey = store, "schema-state"
	err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: "ALTER TABLE promos MODIFY COLUMN LayoutPromo varchar(255) NOT NULL"})
	if err == nil || !strings.Contains(err.Error(), "contains NULL") || strings.Contains(err.Error(), "outcome UNKNOWN") {
		t.Fatalf("error = %v", err)
	}
	if len(store.intents) != 0 || store.saveCount != 0 {
		t.Fatalf("failed preparation changed durable progress: intents=%v saves=%d", store.intents, store.saveCount)
	}
	loaded, err := sink.catalog.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
	if err != nil {
		t.Fatal(err)
	}
	field, _ := loaded.Schema().FindFieldByName("LayoutPromo")
	if !field.Type.Equals(iceberglib.PrimitiveTypes.Int32) || integerStringTestRows(t, loaded)[1]["LayoutPromo"] != nil {
		t.Fatal("failed preparation changed committed data or schema")
	}
}

func TestIntegerStringDDLRecoversLostResponseWithoutConsumingSourceRow(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
	cat := &responseLossCatalog{Catalog: sink.catalog, lose: true}
	sink.catalog = cat
	var err error
	state.table, err = cat.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
	if err != nil {
		t.Fatal(err)
	}
	store := newGuardTestStore()
	sink.offsetSto, sink.stateKey = store, "schema-state"
	sink.recoveryPolicy = &config.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond}
	sink.recoveryWait = func(context.Context, time.Duration) error { return nil }
	ev := recoveryEvent(2, 0)
	ev.Schema, ev.Table = "app", "promos"
	ctx := withIcebergBatch(context.Background(), cdcIcebergBatch(state.sourceKey, []model.Event{ev}))
	if err := sink.applyDDL(ctx, state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL}); err != nil {
		t.Fatal(err)
	}
	if len(store.intents) != 0 || len(sink.recoveryEvents) != 0 {
		t.Fatalf("schema rewrite consumed or retained a source row: intents=%v replay=%v", store.intents, sink.recoveryEvents)
	}
	if rows := integerStringTestRows(t, state.table); len(rows) != 1 || rows[1]["LayoutPromo"] != "18" {
		t.Fatalf("lost-response recovery changed rows: %v", rows)
	}
}

type schemaRewriteConcurrentCatalog struct {
	icecatalog.Catalog
	beforeCommit func()
}

func (c *schemaRewriteConcurrentCatalog) CommitTable(ctx context.Context, id icetable.Identifier, requirements []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	if c.beforeCommit != nil {
		hook := c.beforeCommit
		c.beforeCommit = nil
		hook()
	}
	// Match the REST catalog's typed HTTP 409 for a rejected requirement.
	current, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, "", err
	}
	for _, requirement := range requirements {
		if err := requirement.Validate(current.Metadata()); err != nil {
			return nil, "", fmt.Errorf("%w: %v", icetable.ErrCommitFailed, err)
		}
	}
	return c.Catalog.CommitTable(ctx, id, requirements, updates)
}

func (c *schemaRewriteConcurrentCatalog) LoadTable(ctx context.Context, id icetable.Identifier) (*icetable.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}
	return icetable.New(id, tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func TestIntegerStringDDLIncludesConcurrentAppendOnRetry(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
	base := sink.catalog
	cat := &schemaRewriteConcurrentCatalog{Catalog: base}
	cat.beforeCommit = func() {
		tbl, err := base.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
		if err != nil {
			t.Fatal(err)
		}
		reader, release, err := buildRecordReader(tbl.Schema(), []map[string]interface{}{{"id": int64(2), "LayoutPromo": int32(19)}})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if _, err := tbl.Append(context.Background(), reader, nil); err != nil {
			t.Fatal(err)
		}
	}
	sink.catalog = cat
	var err error
	state.table, err = cat.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL}); err != nil {
		t.Fatal(err)
	}
	if rows := integerStringTestRows(t, state.table); len(rows) != 2 || rows[1]["LayoutPromo"] != "18" || rows[2]["LayoutPromo"] != "19" {
		t.Fatalf("concurrent rows were lost: %v", rows)
	}
}

func TestIntegerStringDDLRejectsIdentifierAndPreservesSchema(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, nil)
	before := state.table.Schema()
	err := sink.applyDDL(context.Background(), state, model.Event{Type: model.EventTypeDDL, DDL: "ALTER TABLE promos MODIFY COLUMN id varchar(255) NOT NULL"})
	if err == nil || !strings.Contains(err.Error(), "identifier field") {
		t.Fatalf("error = %v", err)
	}
	if !reflect.DeepEqual(before.Fields(), state.table.Schema().Fields()) {
		t.Fatal("rejected rewrite changed the schema")
	}
}

func TestIntegerStringRewriteCanceledBeforeCommit(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, []map[string]interface{}{{"id": int64(1), "LayoutPromo": int32(18)}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sink.applyDDL(ctx, state, model.Event{Type: model.EventTypeDDL, DDL: layoutStringDDL})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	loaded, err := sink.catalog.LoadTable(context.Background(), icetable.Identifier{"bronze", "promos"})
	if err != nil {
		t.Fatal(err)
	}
	field, _ := loaded.Schema().FindFieldByName("LayoutPromo")
	if !field.Type.Equals(iceberglib.PrimitiveTypes.Int32) || integerStringTestRows(t, loaded)[1]["LayoutPromo"] != int64(18) {
		t.Fatal("cancellation committed a partial rewrite")
	}
}

// A table property alone cannot prove a data rewrite after snapshot rollback.
func TestIntegerStringRewriteRecoveryRequiresSnapshotEvidence(t *testing.T) {
	sink, state := integerStringTestSink(t, iceberglib.PrimitiveTypes.Int32, nil)
	intent := meta.IcebergCommitIntent{OperationID: "rewrite-1", JobID: sink.jobID, StateKey: "schema-state", Kind: "internal", Operation: "schema-rewrite", SourceTable: state.sourceKey, TargetNamespace: "bronze", TargetTable: "promos", Status: meta.IcebergCommitPrepared}
	txn := state.table.NewTransaction()
	ctx := context.WithValue(context.Background(), icebergIntentKey{}, intent)
	if err := txn.SetProperties(icebergCommitProperties(ctx, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.loadProvenIcebergTable(ctx, intent); err == nil {
		t.Fatal("metadata marker incorrectly proved a missing rewrite snapshot")
	}
}
