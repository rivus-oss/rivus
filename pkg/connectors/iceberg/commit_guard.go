package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/util"
)

type icebergBatchKey struct{}

type icebergBatch struct {
	kind          string
	batchID       string
	sourceStart   *model.SourceOffset
	sourceEnd     *model.SourceOffset
	snapshotTable string
	snapshotStart int64
	snapshotEnd   int64
	replayKeys    string
}

func withIcebergBatch(ctx context.Context, batch icebergBatch) context.Context {
	return context.WithValue(ctx, icebergBatchKey{}, batch)
}

func cdcIcebergBatch(sourceKey string, events []model.Event) icebergBatch {
	batch := icebergBatch{kind: "cdc"}
	keys := make([]icebergReplayEvent, 0, len(events))
	complete := true
	for _, ev := range events {
		if key, err := icebergReplayKey(ev); err == nil {
			keys = append(keys, icebergReplayEvent{Key: key, Offset: ev.SourceOffset})
		} else {
			complete = false
		}
	}
	encoded, _ := json.Marshal(icebergReplayManifest{Version: 2, Events: keys})
	if complete {
		batch.replayKeys = string(encoded)
	}
	for _, ev := range events {
		if ev.SourceOffset.Valid() {
			if batch.sourceStart == nil {
				cp := *ev.SourceOffset
				batch.sourceStart = &cp
			}
			cp := *ev.SourceOffset
			batch.sourceEnd = &cp
		}
	}
	rangeID := fmt.Sprintf("%s:%s-%s", sourceKey, checkpointOffsetText(batch.sourceStart), checkpointOffsetText(batch.sourceEnd))
	digest := sha256.Sum256([]byte(rangeID))
	batch.batchID = fmt.Sprintf("cdc:%s:%x", sourceKey, digest[:12])
	return batch
}

func snapshotIcebergBatch(sourceKey string, start, end int64) icebergBatch {
	return icebergBatch{
		kind: "snapshot", batchID: fmt.Sprintf("snapshot:%s:%d-%d", sourceKey, start, end),
		snapshotTable: sourceKey, snapshotStart: start, snapshotEnd: end,
	}
}

func (s *Sink) icebergIntentStore() (meta.IcebergCommitIntentStore, error) {
	if s.offsetSto == nil {
		return nil, errors.New("Iceberg durable commit guard requires a metadata store")
	}
	store, ok := s.offsetSto.(meta.IcebergCommitIntentStore)
	if !ok {
		return nil, errors.New("Iceberg metadata store does not support durable commit intents")
	}
	return store, nil
}

func (s *Sink) guardedIcebergCommit(ctx context.Context, progress commitProgress, fn func(context.Context) error) error {
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return util.Permanent(err)
	}
	store, err := s.icebergIntentStore()
	if err != nil {
		return util.Permanent(err)
	}
	batch, ok := ctx.Value(icebergBatchKey{}).(icebergBatch)
	if !ok {
		batch.kind = "internal"
	}
	operationID := uuid.NewString()
	if batch.batchID == "" {
		batch.batchID = "internal:" + progress.operation + ":" + operationID
	}
	intent := meta.IcebergCommitIntent{
		OperationID: operationID, JobID: s.jobID, StateKey: s.checkpointKey(), BatchID: batch.batchID,
		Kind: batch.kind, SnapshotTable: batch.snapshotTable,
		SnapshotStartOffset: batch.snapshotStart, SnapshotEndOffset: batch.snapshotEnd,
		Status:      meta.IcebergCommitPrepared,
		SourceTable: progress.sourceKey, TargetNamespace: progress.targetNamespace, TargetTable: progress.targetTable, TargetUUID: progress.targetUUID, Operation: progress.operation, ReplayKeys: batch.replayKeys,
	}
	if s.scopedNativeWrites && batch.kind == "snapshot" {
		intent.BatchID = meta.ScopedNativeSnapshotBatchID(s.scopedRebuildToken, batch.snapshotTable, batch.snapshotStart, batch.snapshotEnd)
		state := s.states[progress.sourceKey]
		if state == nil || state.table == nil {
			return util.Permanent(fmt.Errorf("native rebuild target identity missing"))
		}
		intent.TargetUUID = state.table.Metadata().TableUUID().String()
	}
	if batch.sourceStart.Valid() {
		intent.SourceStartFile, intent.SourceStartPos = batch.sourceStart.BinlogFile, batch.sourceStart.BinlogPos
	}
	if batch.sourceEnd.Valid() {
		intent.SourceEndFile, intent.SourceEndPos = batch.sourceEnd.BinlogFile, batch.sourceEnd.BinlogPos
	}
	if err := store.BeginIcebergCommit(ctx, intent); err != nil {
		return util.Permanent(fmt.Errorf("persist Iceberg commit intent before catalog call operation=%s batch=%s: %w", operationID, batch.batchID, err))
	}
	log.Printf("[iceberg][job %s] commit intent PREPARED operation_id=%s batch=%s kind=%s source=%s-%s snapshot=%s:%d-%d",
		s.jobID, operationID, batch.batchID, batch.kind, checkpointOffsetText(batch.sourceStart), checkpointOffsetText(batch.sourceEnd), batch.snapshotTable, batch.snapshotStart, batch.snapshotEnd)
	err = fn(context.WithValue(ctx, icebergIntentKey{}, intent))
	// Cancellation leaves PREPARED for the next owner to verify. Never detach
	// result persistence from the worker lifetime after an in-flight request.
	finishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err == nil {
		if markErr := store.MarkIcebergCommitCommitted(finishCtx, intent.StateKey, operationID); markErr != nil {
			unknown := util.Permanent(fmt.Errorf("Iceberg commit outcome UNKNOWN; catalog returned success but marking intent committed failed operation=%s: %w", operationID, markErr))
			return s.recoverUnknownCommit(ctx, intent, progress, unknown)
		}
		if batch.kind == "internal" {
			if deleteErr := store.DeleteIcebergCommit(finishCtx, intent.StateKey, operationID); deleteErr != nil {
				return util.Permanent(fmt.Errorf("Iceberg internal commit confirmed but intent cleanup failed operation=%s: %w", operationID, deleteErr))
			}
		}
		return nil
	}
	var preparation *icebergPreparationError
	if errors.As(err, &preparation) {
		// Preparation has positively failed before any catalog request. Allow
		// bounded cleanup after its deadline, still through the ownership-fenced
		// store. Success and uncertain catalog outcomes stay bound to ctx above.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cleanupCancel()
		if deleteErr := store.DeleteIcebergCommit(cleanupCtx, intent.StateKey, operationID); deleteErr != nil {
			return util.Permanent(fmt.Errorf("Iceberg preparation failed before catalog call; intent cleanup failed operation=%s: %w; preparation error: %w", operationID, deleteErr, err))
		}
		return err
	}
	if errors.Is(err, icetable.ErrCommitFailed) && !util.IsPermanent(err) {
		if deleteErr := store.DeleteIcebergCommit(finishCtx, intent.StateKey, operationID); deleteErr != nil {
			return util.Permanent(fmt.Errorf("Iceberg rejected commit intent cleanup failed operation=%s: %w", operationID, deleteErr))
		}
		return err
	}
	unknown := util.Permanent(fmt.Errorf("Iceberg commit outcome UNKNOWN; durable intent operation=%s batch=%s remains PREPARED, replay blocked until recovery: %w", operationID, batch.batchID, err))
	return s.recoverUnknownCommit(ctx, intent, progress, unknown)
}

func (s *Sink) checkIcebergRecovery(ctx context.Context) error {
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return util.Permanent(err)
	}
	// Ad-hoc maintenance sinks without a metadata store do not run the
	// replication pipeline. Managed jobs always have a durable state key.
	if strings.TrimSpace(s.stateKey) == "" && s.offsetSto == nil {
		return nil
	}
	store, err := s.icebergIntentStore()
	if err != nil {
		return util.Permanent(err)
	}
	intents, err := store.ListIcebergCommits(ctx, s.checkpointKey())
	if err != nil {
		return util.Permanent(fmt.Errorf("read Iceberg commit intents before replay: %w", err))
	}
	var offset *meta.Offset
	var progress *meta.SnapshotProgress
	var snapshot *meta.SnapshotState
	if len(intents) > 0 {
		offset, err = s.offsetSto.GetOffset(ctx, s.checkpointKey())
		if err != nil {
			return util.Permanent(fmt.Errorf("read checkpoint for Iceberg commit recovery: %w", err))
		}
		progress, err = s.offsetSto.GetSnapshotProgress(ctx, s.checkpointKey())
		if err != nil {
			return util.Permanent(fmt.Errorf("read snapshot progress for Iceberg commit recovery: %w", err))
		}
		snapshot, err = s.offsetSto.GetSnapshotState(ctx, s.checkpointKey())
		if err != nil {
			return util.Permanent(fmt.Errorf("read snapshot state for Iceberg commit recovery: %w", err))
		}
	}
	committedTargets := make(map[string]string)
	committedEvidence := make(map[string]meta.IcebergCommitIntent)
	for _, intent := range intents {
		s.recoveryOperation = intent.OperationID
		if s.scopedNativeWrites && intent.Kind == "snapshot" && !meta.IsScopedNativeSnapshotIntent(intent, s.jobID, s.checkpointKey(), s.scopedRebuildToken) {
			return util.Permanent(fmt.Errorf("native snapshot receipt belongs to another epoch operation=%s", intent.OperationID))
		}
		if intent.Kind == "rebuild" {
			s.trinoRebuildMu.Lock()
			found := false
			var readErr error
			if intent.Status == meta.IcebergCommitCommitted {
				found = true
				readErr = s.validateTrinoRebuildIntentIdentity(intent)
				target := intent.TargetNamespace + "." + intent.TargetTable
				if readErr == nil && committedTargets[target] == "" {
					tbl, err := s.catalog.LoadTable(ctx, namespaceIdentifier(intent.TargetNamespace, intent.TargetTable))
					readErr = err
					if err == nil {
						committedTargets[target] = tbl.Metadata().TableUUID().String()
						committedEvidence[target] = intent
					}
				}
				if readErr == nil && committedTargets[target] != intent.TargetUUID {
					readErr = fmt.Errorf("Trino rebuild target generation changed operation=%s", intent.OperationID)
				}
			} else if reader, ok := store.(meta.IcebergCommitReader); ok {
				latest, err := reader.GetIcebergCommit(ctx, s.checkpointKey(), intent.OperationID)
				readErr = err
				if err == nil && latest != nil {
					found = true
					readErr = s.confirmTrinoRebuildIntent(ctx, *latest)
				}
			} else {
				latest, err := store.ListIcebergCommits(ctx, s.checkpointKey())
				readErr = err
				if err == nil {
					for _, durable := range latest {
						if durable.OperationID == intent.OperationID {
							found = true
							readErr = s.confirmTrinoRebuildIntent(ctx, durable)
							break
						}
					}
				}
			}
			s.trinoRebuildMu.Unlock()
			if readErr != nil {
				return util.Permanent(fmt.Errorf("Trino rebuild recovery operation=%s: %w", intent.OperationID, readErr))
			}
			if !found {
				return util.Permanent(fmt.Errorf("Trino rebuild intent disappeared operation=%s", intent.OperationID))
			}
			if intent.Operation == "trino-merge" {
				if s.scopedLegacyMerges == nil {
					s.scopedLegacyMerges = make(map[string]bool)
				}
				s.scopedLegacyMerges[intent.SourceTable] = true
			}
			if snapshot.CompletionConfirmed() {
				if err := store.DeleteIcebergCommit(ctx, intent.StateKey, intent.OperationID); err != nil {
					return util.Permanent(err)
				}
			}
			continue
		}
		if intent.Status == meta.IcebergCommitCommitted && icebergIntentCovered(intent, offset, progress, snapshot) {
			if err := store.DeleteIcebergCommit(ctx, s.checkpointKey(), intent.OperationID); err != nil {
				return util.Permanent(fmt.Errorf("clear checkpointed Iceberg commit intent operation=%s: %w", intent.OperationID, err))
			}
			continue
		}
		if intent.Status != meta.IcebergCommitPrepared && intent.Status != meta.IcebergCommitCommitted {
			return util.Permanent(fmt.Errorf("Iceberg recovery BLOCKED operation=%s invalid status=%s", intent.OperationID, intent.Status))
		}
		if err := s.proveIcebergIntent(ctx, intent); err != nil {
			return util.Permanent(fmt.Errorf("Iceberg recovery BLOCKED job=%s operation_id=%s batch=%s status=%s kind=%s: %w", s.jobID, intent.OperationID, intent.BatchID, intent.Status, intent.Kind, err))
		}
		if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
			return util.Permanent(err)
		}
		if !icebergIntentCovered(intent, offset, progress, snapshot) {
			if err := s.retainRecoveryReceipt(intent, offset); err != nil {
				return util.Permanent(fmt.Errorf("Iceberg recovery BLOCKED operation=%s: %w", intent.OperationID, err))
			}
		}
		if intent.Status == meta.IcebergCommitPrepared {
			if err := store.MarkIcebergCommitCommitted(ctx, intent.StateKey, intent.OperationID); err != nil {
				return util.Permanent(fmt.Errorf("persist proven Iceberg outcome operation=%s: %w", intent.OperationID, err))
			}
		}
		// Metadata operations can be repeated idempotently by source replay; they
		// must never consume the enclosing data event or advance a source offset.
		if recoverableMetadataOperation(intent.Operation) || icebergIntentCovered(intent, offset, progress, snapshot) {
			if err := store.DeleteIcebergCommit(ctx, intent.StateKey, intent.OperationID); err != nil {
				return util.Permanent(err)
			}
		}
		log.Printf("[iceberg][job %s] reconciled accepted operation=%s batch=%s; resuming source replay", s.jobID, intent.OperationID, intent.BatchID)
	}
	// Generation checks are shared across confirmed statements for one table,
	// then refreshed once before startup admits source replay.
	for target, intent := range committedEvidence {
		tbl, err := s.catalog.LoadTable(ctx, namespaceIdentifier(intent.TargetNamespace, intent.TargetTable))
		if err != nil {
			return util.Permanent(err)
		}
		if tbl.Metadata().TableUUID().String() != committedTargets[target] {
			return util.Permanent(fmt.Errorf("Trino rebuild target generation changed during reconciliation"))
		}
	}

	return meta.CheckOwnership(ctx, s.offsetSto)
}

// Normal checkpoint cleanup must not install startup replay receipts. Live
// batches have already consumed their source events, even when the latest
// transaction checkpoint is behind their row offsets.
func (s *Sink) cleanupCheckpointedIcebergCommits(ctx context.Context, offset *meta.Offset) error {
	if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
		return util.Permanent(err)
	}
	store, err := s.icebergIntentStore()
	if err != nil {
		return util.Permanent(err)
	}
	intents, err := store.ListIcebergCommits(ctx, s.checkpointKey())
	if err != nil {
		return util.Permanent(fmt.Errorf("read Iceberg intents for checkpoint cleanup: %w", err))
	}
	for _, intent := range intents {
		if intent.Status != meta.IcebergCommitCommitted || intent.Kind != "cdc" || !icebergIntentCovered(intent, offset, nil, nil) {
			continue
		}
		if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
			return util.Permanent(err)
		}
		if err := store.DeleteIcebergCommit(ctx, s.checkpointKey(), intent.OperationID); err != nil {
			return util.Permanent(fmt.Errorf("clear checkpointed Iceberg commit intent operation=%s: %w", intent.OperationID, err))
		}
	}
	return nil
}

func icebergIntentCovered(intent meta.IcebergCommitIntent, offset *meta.Offset, progress *meta.SnapshotProgress, snapshot *meta.SnapshotState) bool {
	switch intent.Kind {
	case "cdc":
		return offset != nil && intent.SourceEndFile != "" && binlogOffsetCovers(offset.BinlogFile, offset.BinlogPos, intent.SourceEndFile, intent.SourceEndPos)
	case "snapshot":
		if snapshot.CompletionConfirmed() {
			return true
		}
		return progress != nil && progress.TableName == intent.SnapshotTable && progress.NextOffset >= intent.SnapshotEndOffset
	default:
		return false
	}
}

func binlogOffsetCovers(savedFile string, savedPos uint32, endFile string, endPos uint32) bool {
	if savedFile == endFile {
		return savedPos >= endPos
	}
	savedBase, savedSeq, savedOK := parseBinlogSequence(savedFile)
	endBase, endSeq, endOK := parseBinlogSequence(endFile)
	return savedOK && endOK && savedBase == endBase && savedSeq > endSeq
}

func parseBinlogSequence(file string) (string, uint64, bool) {
	dot := strings.LastIndexByte(file, '.')
	if dot < 0 || dot == len(file)-1 {
		return "", 0, false
	}
	n, err := strconv.ParseUint(file[dot+1:], 10, 64)
	return file[:dot], n, err == nil
}
