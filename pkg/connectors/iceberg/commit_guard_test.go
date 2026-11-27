package iceberg

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type failedEqualityPreparationFS struct{ iceio.LocalFS }

func (failedEqualityPreparationFS) Create(string) (iceio.FileWriter, error) {
	return failedEqualityPreparationWriter{}, nil
}

func (failedEqualityPreparationFS) Remove(string) error { return os.ErrNotExist }

type failedEqualityPreparationWriter struct{}

func (failedEqualityPreparationWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
func (failedEqualityPreparationWriter) Close() error {
	return errors.New("fixture equality-delete upload failed")
}
func (failedEqualityPreparationWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(io.Discard, r)
}

func TestIcebergGuardEqualityPreparationFailureDoesNotBecomeUnknown(t *testing.T) {
	for _, fault := range []string{"delete-file", "data-row", "catalog-response"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			tbl, cat := newEqualityDeltaTestTable(t)
			if fault == "delete-file" {
				tbl = icetable.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), func(context.Context) (iceio.IO, error) {
					return failedEqualityPreparationFS{}, nil
				}, cat)
			}
			store := newGuardTestStore()
			s := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
			state := &tableState{sourceKey: "app.orders", table: tbl}
			batch := &reducedBatch{pkCols: []string{"id"}, deleteKeys: []map[string]interface{}{{"id": int64(10)}}, rows: []map[string]interface{}{{"id": int64(10), "status": "updated"}}}
			if fault == "data-row" {
				batch.rows[0]["id"] = "invalid-integer"
			}
			if fault == "catalog-response" {
				cat.commitErr = errors.New("fixture catalog response lost")
			}
			ctx = withIcebergBatch(ctx, icebergBatch{kind: "cdc", batchID: "fixture", sourceEnd: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}})
			err := s.withCommitSlot(ctx, commitProgress{operation: "delete-append-equality", icebergCommit: true}, func(ctx context.Context) error {
				_, err := s.commitEqualityDelta(ctx, state, batch)
				return err
			})
			if err == nil || store.saveCount != 0 {
				t.Fatalf("error=%v checkpoint saves=%d", err, store.saveCount)
			}
			if fault == "catalog-response" {
				if cat.commitAttempts != 1 || len(store.intents) != 1 || !strings.Contains(err.Error(), "outcome UNKNOWN") {
					t.Fatalf("ambiguous request lost protection: calls=%d intents=%d error=%v", cat.commitAttempts, len(store.intents), err)
				}
			} else if cat.commitAttempts != 0 || len(store.intents) != 0 || strings.Contains(err.Error(), "outcome UNKNOWN") {
				t.Fatalf("preparation became unknown: calls=%d intents=%d error=%v", cat.commitAttempts, len(store.intents), err)
			}
		})
	}
}

type guardTestStore struct {
	*testOffsetStore
	intents          map[string]meta.IcebergCommitIntent
	snapshotProgress *meta.SnapshotProgress
	beginErr         error
	markErr          error
	deleteErr        error
}

func newGuardTestStore() *guardTestStore {
	return &guardTestStore{testOffsetStore: &testOffsetStore{}, intents: make(map[string]meta.IcebergCommitIntent)}
}

func (s *guardTestStore) ListIcebergCommitsForJob(_ context.Context, key, job string) ([]meta.IcebergCommitIntent, error) {
	var rows []meta.IcebergCommitIntent
	for _, intent := range s.intents {
		if intent.StateKey == key || intent.JobID == job {
			rows = append(rows, intent)
		}
	}
	return rows, nil
}

func (s *guardTestStore) BeginIcebergCommit(_ context.Context, intent meta.IcebergCommitIntent) error {
	if s.beginErr != nil {
		return s.beginErr
	}
	s.intents[intent.OperationID] = intent
	return nil
}

func TestIcebergGuardIntentWriteFailurePreventsCatalogCall(t *testing.T) {
	store := newGuardTestStore()
	store.beginErr = errors.New("metadata DB unavailable")
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	called := false
	err := sink.withCommitSlot(context.Background(), commitProgress{operation: "append", icebergCommit: true}, func(ctx context.Context) error {
		called = true
		return nil
	})
	if err == nil || called || len(store.intents) != 0 {
		t.Fatalf("intent write failure=(%v, catalog called=%t, intents=%d), want error/no call/no intent", err, called, len(store.intents))
	}
}

func TestIcebergGuardProvenRejectedCommitRemovesIntent(t *testing.T) {
	store := newGuardTestStore()
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	err := sink.withCommitSlot(context.Background(), commitProgress{operation: "append", icebergCommit: true}, func(ctx context.Context) error {
		return icetable.ErrCommitFailed
	})
	if !errors.Is(err, icetable.ErrCommitFailed) || len(store.intents) != 0 {
		t.Fatalf("rejected commit=(%v, intents=%d), want retryable conflict and no intent", err, len(store.intents))
	}
	if err := sink.checkIcebergRecovery(context.Background()); err != nil {
		t.Fatalf("restart after proven rejection: %v", err)
	}
}

func (s *guardTestStore) MarkIcebergCommitCommitted(_ context.Context, _, operationID string) error {
	if s.markErr != nil {
		return s.markErr
	}
	intent := s.intents[operationID]
	intent.Status = meta.IcebergCommitCommitted
	s.intents[operationID] = intent
	return nil
}

func TestIcebergGuardCommitSucceededButIntentUpdateFailedBlocksRestart(t *testing.T) {
	store := newGuardTestStore()
	store.markErr = errors.New("metadata DB unavailable after catalog success")
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	ctx := withIcebergBatch(context.Background(), icebergBatch{kind: "cdc", batchID: "batch-mark-failure", sourceEnd: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}})
	accepted := false
	err := sink.withCommitSlot(ctx, commitProgress{operation: "append", icebergCommit: true}, func(ctx context.Context) error {
		accepted = true
		return nil
	})
	if !accepted || err == nil || !strings.Contains(err.Error(), "outcome UNKNOWN") {
		t.Fatalf("catalog accepted=%t result=%v, want UNKNOWN", accepted, err)
	}
	assertGuardBlocksRestart(t, store)
}

func (s *guardTestStore) DeleteIcebergCommit(_ context.Context, _, operationID string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.intents, operationID)
	return nil
}

func TestIcebergPreparationCleanupFailureRetainsReceiptAndBothCauses(t *testing.T) {
	store := newGuardTestStore()
	store.deleteErr = meta.ErrWorkerOwnershipLost
	s := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	cause := errors.New("fixture preparation failure")
	err := s.withCommitSlot(context.Background(), commitProgress{operation: "delete-append-equality", icebergCommit: true}, func(context.Context) error {
		return icebergPreparationFailed(cause)
	})
	if !errors.Is(err, cause) || !errors.Is(err, meta.ErrWorkerOwnershipLost) || len(store.intents) != 1 || store.saveCount != 0 {
		t.Fatalf("cleanup lost evidence: error=%v intents=%d checkpoint saves=%d", err, len(store.intents), store.saveCount)
	}
	for _, i := range store.intents {
		if i.Status != meta.IcebergCommitPrepared {
			t.Fatal("cleanup failure marked an unaccepted write committed")
		}
	}
}

type contextCheckedCleanupStore struct{ *guardTestStore }

func (s *contextCheckedCleanupStore) DeleteIcebergCommit(ctx context.Context, key, operation string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.guardTestStore.DeleteIcebergCommit(ctx, key, operation)
}

func TestIcebergCancelledPreparationCleansOnlyKnownUnsentIntent(t *testing.T) {
	for _, preparation := range []bool{true, false} {
		t.Run(map[bool]string{true: "preparation", false: "uncertain catalog outcome"}[preparation], func(t *testing.T) {
			store := &contextCheckedCleanupStore{newGuardTestStore()}
			sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := sink.withCommitSlot(ctx, commitProgress{operation: "schema-rewrite", icebergCommit: true}, func(context.Context) error {
				cancel()
				if preparation {
					return icebergPreparationFailed(context.Canceled)
				}
				return context.Canceled
			})
			if err == nil || store.saveCount != 0 {
				t.Fatalf("err=%v checkpoint saves=%d", err, store.saveCount)
			}
			if preparation {
				if len(store.intents) != 0 || strings.Contains(err.Error(), "outcome UNKNOWN") {
					t.Fatalf("known unsent preparation remained blocked: %v", err)
				}
			} else if len(store.intents) != 1 || !strings.Contains(err.Error(), "outcome UNKNOWN") {
				t.Fatalf("uncertain outcome lost its receipt: %v", err)
			}
		})
	}
}

func (s *guardTestStore) ListIcebergCommits(_ context.Context, _ string) ([]meta.IcebergCommitIntent, error) {
	intents := make([]meta.IcebergCommitIntent, 0, len(s.intents))
	for _, intent := range s.intents {
		intents = append(intents, intent)
	}
	return intents, nil
}

func (s *guardTestStore) SaveSnapshotProgress(_ context.Context, stateKey, table string, nextOffset int64, cursor string) error {
	s.snapshotProgress = &meta.SnapshotProgress{JobID: stateKey, TableName: table, NextOffset: nextOffset, CursorJSON: cursor}
	return nil
}

func (s *guardTestStore) GetSnapshotProgress(context.Context, string) (*meta.SnapshotProgress, error) {
	return s.snapshotProgress, nil
}

func TestIcebergGuardCrashBeforeCommitBlocksRestart(t *testing.T) {
	store := newGuardTestStore()
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	ctx := withIcebergBatch(context.Background(), icebergBatch{kind: "cdc", batchID: "batch-1", sourceStart: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 40}, sourceEnd: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}})
	called := false
	err := sink.withCommitSlot(ctx, commitProgress{operation: "append", icebergCommit: true}, func(ctx context.Context) error {
		called = true
		if len(store.intents) != 1 {
			t.Fatal("intent was not durable before commit callback")
		}
		for id, intent := range store.intents {
			if id == "" || intent.OperationID != id || intent.JobID != "job" || intent.StateKey != "state" || intent.BatchID != "batch-1" || intent.SourceStartPos != 40 || intent.SourceEndPos != 42 || intent.Status != meta.IcebergCommitPrepared {
				t.Fatalf("incomplete durable intent before commit: %+v", intent)
			}
		}
		return errors.New("simulated process crash before catalog request")
	})
	if !called || err == nil {
		t.Fatalf("commit callback called=%t error=%v", called, err)
	}
	assertGuardBlocksRestart(t, store)
}

func TestIcebergGuardCommittedBeforeCheckpointBlocksRestart(t *testing.T) {
	store := newGuardTestStore()
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	ctx := withIcebergBatch(context.Background(), icebergBatch{kind: "cdc", batchID: "batch-2", sourceEnd: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}})
	if err := sink.withCommitSlot(ctx, commitProgress{operation: "append", icebergCommit: true}, func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("confirmed commit: %v", err)
	}
	assertGuardBlocksRestart(t, store)
}

func TestIcebergGuardSnapshotCommitBeforeAckBlocksRestart(t *testing.T) {
	store := newGuardTestStore()
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	ctx := withIcebergBatch(context.Background(), snapshotIcebergBatch("app.orders", 0, 10))
	if err := sink.withCommitSlot(ctx, commitProgress{operation: "snapshot-append", icebergCommit: true}, func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("confirmed snapshot commit: %v", err)
	}
	assertGuardBlocksRestart(t, store) // Source did not receive ACK or save progress.
	if err := store.SaveSnapshotProgress(context.Background(), "state", "app.orders", 10, ""); err != nil {
		t.Fatalf("save snapshot progress: %v", err)
	}
	restarted := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	if err := restarted.checkIcebergRecovery(context.Background()); err != nil {
		t.Fatalf("restart after confirmed snapshot progress: %v", err)
	}
}

func TestIcebergGuardUnknownCommitBlocksReplayAfterRestart(t *testing.T) {
	store := newGuardTestStore()
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	ctx := withIcebergBatch(context.Background(), icebergBatch{kind: "cdc", batchID: "batch-3", sourceEnd: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}})
	accepted := 0
	err := sink.withCommitSlot(ctx, commitProgress{operation: "delete-append-equality", icebergCommit: true}, func(ctx context.Context) error {
		accepted++
		return errors.New("catalog accepted commit but response was lost")
	})
	if err == nil || accepted != 1 {
		t.Fatalf("unknown commit result=(%d accepted, %v), want one accepted and error", accepted, err)
	}
	// Even a later or inconsistent checkpoint must not resolve PREPARED.
	store.offset = &meta.Offset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}
	assertGuardBlocksRestart(t, store)
	if accepted != 1 {
		t.Fatalf("replay executed after restart: accepted=%d", accepted)
	}
}

func TestIcebergGuardConfirmedCommitAndCheckpointAllowRestart(t *testing.T) {
	store := newGuardTestStore()
	sink := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	ctx := withIcebergBatch(context.Background(), icebergBatch{kind: "cdc", batchID: "batch-4", sourceEnd: &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}})
	if err := sink.withCommitSlot(ctx, commitProgress{operation: "append", icebergCommit: true}, func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("confirmed commit: %v", err)
	}
	sink.pendingOffset = &model.SourceOffset{BinlogFile: "mysql-bin.000001", BinlogPos: 42}
	if err := sink.commitPendingOffset(context.Background(), true); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if store.saveCount != 1 {
		t.Fatalf("checkpoint saves=%d, want 1", store.saveCount)
	}
	restarted := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	if err := restarted.checkIcebergRecovery(context.Background()); err != nil {
		t.Fatalf("restart after confirmed commit and checkpoint: %v", err)
	}
}

func TestIcebergGuardBinlogCheckpointCoverage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		savedFile string
		savedPos  uint32
		endFile   string
		endPos    uint32
		want      bool
	}{
		{name: "same_file_at_end", savedFile: "mysql-bin.000001", savedPos: 42, endFile: "mysql-bin.000001", endPos: 42, want: true},
		{name: "same_file_behind", savedFile: "mysql-bin.000001", savedPos: 41, endFile: "mysql-bin.000001", endPos: 42},
		{name: "rotation", savedFile: "mysql-bin.000002", savedPos: 4, endFile: "mysql-bin.000001", endPos: 42, want: true},
		{name: "unrelated_prefix", savedFile: "other-bin.000002", savedPos: 4, endFile: "mysql-bin.000001", endPos: 42},
		{name: "unparseable", savedFile: "mysql-bin.new", savedPos: 4, endFile: "mysql-bin.000001", endPos: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := binlogOffsetCovers(tc.savedFile, tc.savedPos, tc.endFile, tc.endPos); got != tc.want {
				t.Fatalf("coverage=%t, want %t", got, tc.want)
			}
		})
	}
}

func assertGuardBlocksRestart(t *testing.T, store *guardTestStore) {
	t.Helper()
	restarted := &Sink{jobID: "job", stateKey: "state", offsetSto: store}
	in := make(chan model.Event)
	close(in)
	err := restarted.Run(context.Background(), in)
	if err == nil || !strings.Contains(err.Error(), "recovery BLOCKED") {
		t.Fatalf("restart result=%v, want recovery BLOCKED", err)
	}
}
