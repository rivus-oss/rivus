package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	iceberglib "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type icebergIntentKey struct{}

// The fingerprint binds catalog evidence to the full durable intent, rather
// than trusting a batch ID or an offset alone. Status is deliberately excluded.
func icebergIntentFingerprint(intent meta.IcebergCommitIntent) string {
	intent.Status = ""
	intent.TrinoQueryID = "" // Mutable receipt, not part of the submitted statement.
	encoded, _ := json.Marshal(intent)
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest)
}

func icebergCommitProperties(ctx context.Context, props iceberglib.Properties) iceberglib.Properties {
	intent, ok := ctx.Value(icebergIntentKey{}).(meta.IcebergCommitIntent)
	if !ok {
		return props
	}
	result := maps.Clone(props)
	if result == nil {
		result = make(iceberglib.Properties)
	}
	result["rivus.operation_id"] = intent.OperationID
	result["rivus.commit."+intent.OperationID] = icebergIntentFingerprint(intent)
	return result
}

func icebergReplayKey(ev model.Event) (string, error) {
	if ev.TraceID == "" || !ev.SourceOffset.Valid() {
		return "", fmt.Errorf("missing stable binlog row identity")
	}

	// Trace IDs include the binlog row index. Timestamps, schema fetch results,
	// and enrichment are not part of the source mutation's stable identity.
	encoded, err := json.Marshal(struct {
		Type                   model.EventType
		TraceID, Schema, Table string
		Data, OldData          map[string]interface{}
		Offset                 *model.SourceOffset
	}{ev.Type, ev.TraceID, ev.Schema, ev.Table, ev.Data, ev.OldData, ev.SourceOffset})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	identity, _ := json.Marshal(struct {
		TraceID, Schema, Table string
		Offset                 *model.SourceOffset
	}{ev.TraceID, ev.Schema, ev.Table, ev.SourceOffset})
	identityDigest := sha256.Sum256(identity)
	return fmt.Sprintf("%x.%x", identityDigest, digest), nil
}

func recoverableDataOperation(op string) bool {
	switch op {
	case "append", "delete-append-equality", "delete-equality", "snapshot-rolling-append":
		return true
	default:
		return false
	}
}

func recoverableMetadataOperation(op string) bool {
	return op == "schema" || op == "schema-rewrite" || op == "create_table" || op == "snapshot-rolling-properties"
}

// Only positive catalog evidence is conclusive. Absence may mean a request is
// still in flight, history has expired, or the table has been replaced.
func (s *Sink) proveIcebergIntent(ctx context.Context, intent meta.IcebergCommitIntent) error {
	_, err := s.loadProvenIcebergTable(ctx, intent)
	return err
}

func (s *Sink) loadProvenIcebergTable(ctx context.Context, intent meta.IcebergCommitIntent) (*icetable.Table, error) {
	if s.catalog == nil || intent.TargetTable == "" || intent.TargetNamespace == "" || intent.OperationID == "" {
		return nil, fmt.Errorf("catalog or durable target identity unavailable")
	}
	if !recoverableDataOperation(intent.Operation) && !recoverableMetadataOperation(intent.Operation) {
		return nil, fmt.Errorf("operation %q has no automatic replay recovery", intent.Operation)
	}
	if intent.SourceTable != "" {
		dot := strings.LastIndexByte(intent.SourceTable, '.')
		if dot < 1 {
			return nil, fmt.Errorf("invalid durable source table")
		}
		namespace, target := s.ResolveTarget(intent.SourceTable[:dot], intent.SourceTable[dot+1:])
		if namespace != intent.TargetNamespace || target != intent.TargetTable {
			return nil, fmt.Errorf("source target mapping changed since commit")
		}
	}

	ident := append(strings.Split(intent.TargetNamespace, "."), intent.TargetTable)
	tbl, err := s.catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("load recovery target: %w", err)
	}
	if intent.TargetUUID != "" && tbl.Metadata().TableUUID().String() != intent.TargetUUID {
		return nil, fmt.Errorf("native recovery target generation changed")
	}
	key, want := "rivus.commit."+intent.OperationID, icebergIntentFingerprint(intent)
	if recoverableMetadataOperation(intent.Operation) && intent.Operation != "schema-rewrite" {
		if tbl.Properties()[key] == want {
			return tbl, nil
		}
		return nil, fmt.Errorf("metadata commit marker absent or mismatched")
	}
	// Require the evidence on the current main ancestry. A snapshot on an
	// abandoned branch or rolled-back history cannot justify skipping data.
	for snap := tbl.CurrentSnapshot(); snap != nil; {
		if snap.Summary != nil && snap.Summary.Properties[key] == want {
			return tbl, nil
		}
		if snap.ParentSnapshotID == nil {
			break
		}
		snap = tbl.SnapshotByID(*snap.ParentSnapshotID)
	}
	return nil, fmt.Errorf("matching operation not found in retained main snapshot ancestry")
}

// Version 2 keeps each event's offset alongside the original identity hash.
// The complete manifest is fingerprinted before the catalog call. This lets
// restart distinguish a durably checkpointed prefix from the replay suffix.
// Legacy string arrays remain readable and retain their original fingerprint.
type icebergReplayEvent struct {
	Key    string              `json:"key"`
	Offset *model.SourceOffset `json:"offset"`
}

type icebergReplayManifest struct {
	Version int                  `json:"version"`
	Events  []icebergReplayEvent `json:"events"`
}

func decodeIcebergReplayEvents(intent meta.IcebergCommitIntent) ([]icebergReplayEvent, error) {
	var events []icebergReplayEvent
	if strings.HasPrefix(strings.TrimSpace(intent.ReplayKeys), "[") {
		var keys []string
		if err := json.Unmarshal([]byte(intent.ReplayKeys), &keys); err != nil {
			return nil, err
		}
		for _, key := range keys {
			events = append(events, icebergReplayEvent{Key: key})
		}
	} else {
		var manifest icebergReplayManifest
		if err := json.Unmarshal([]byte(intent.ReplayKeys), &manifest); err != nil {
			return nil, err
		}
		if manifest.Version != 2 {
			return nil, fmt.Errorf("unsupported CDC replay manifest version=%d", manifest.Version)
		}
		events = manifest.Events
		for _, ev := range events {
			if !ev.Offset.Valid() || !binlogOffsetCovers(ev.Offset.BinlogFile, ev.Offset.BinlogPos, intent.SourceStartFile, intent.SourceStartPos) || !binlogOffsetCovers(intent.SourceEndFile, intent.SourceEndPos, ev.Offset.BinlogFile, ev.Offset.BinlogPos) {
				return nil, fmt.Errorf("CDC replay event offset outside durable commit range")
			}
		}
	}
	if len(events) == 0 || intent.SourceTable == "" || intent.SourceEndFile == "" || intent.SourceEndPos == 0 {
		return nil, fmt.Errorf("CDC intent lacks stable source event identities")
	}
	for _, ev := range events {
		if len(ev.Key) != 129 || ev.Key[64] != '.' {
			return nil, fmt.Errorf("invalid CDC event identity")
		}
	}
	return events, nil
}

func (s *Sink) retainRecoveryReceipt(intent meta.IcebergCommitIntent, checkpoint *meta.Offset) error {
	if s.recoveryLoaded == nil {
		s.recoveryLoaded = make(map[string]bool)
		s.recoveryEvents = make(map[string]int)
		s.recoveryEventPayloads = make(map[string]string)
	}
	if s.recoveryLoaded[intent.OperationID] {
		return nil
	}
	if recoverableDataOperation(intent.Operation) {
		switch intent.Kind {
		case "cdc":
			events, err := decodeIcebergReplayEvents(intent)
			if err != nil {
				return err
			}
			for _, ev := range events {
				key := ev.Key
				if previous := s.recoveryEventPayloads[key[:64]]; previous != "" && previous != key {
					return fmt.Errorf("conflicting payloads for one binlog event identity")
				}
				s.recoveryEventPayloads[key[:64]] = key
				if checkpoint != nil && ev.Offset.Valid() && binlogOffsetCovers(checkpoint.BinlogFile, checkpoint.BinlogPos, ev.Offset.BinlogFile, ev.Offset.BinlogPos) {
					continue
				}
				s.recoveryEvents[key]++
			}
		case "snapshot":
			if intent.SnapshotTable == "" || intent.SnapshotStartOffset < 0 || intent.SnapshotEndOffset <= intent.SnapshotStartOffset {
				return fmt.Errorf("invalid snapshot recovery range")
			}
		default:
			return fmt.Errorf("data operation has no source recovery range")
		}
	}
	s.recoveryLoaded[intent.OperationID] = true
	s.recoveryIntents = append(s.recoveryIntents, intent)
	return nil
}

func (s *Sink) skipRecoveredCDCEvent(ev model.Event) (bool, error) {
	if ev.Type != model.EventTypeInsert && ev.Type != model.EventTypeUpdate && ev.Type != model.EventTypeDelete {
		return false, nil
	}
	key, err := icebergReplayKey(ev)
	if err != nil {
		return false, nil
	}
	if expected := s.recoveryEventPayloads[key[:64]]; expected != "" && expected != key {
		return false, fmt.Errorf("Iceberg recovery BLOCKED: source payload changed for retained binlog row identity %s", ev.TraceID)
	}
	if s.recoveryEvents[key] <= 0 {
		return false, nil
	}
	s.recoveryEvents[key]--
	return true, nil
}

func (s *Sink) checkRecoveryReplayComplete(off *model.SourceOffset) error {
	if !off.Valid() {
		return nil
	}
	for _, intent := range s.recoveryIntents {
		if intent.Kind != "cdc" || !recoverableDataOperation(intent.Operation) || !binlogOffsetCovers(off.BinlogFile, off.BinlogPos, intent.SourceEndFile, intent.SourceEndPos) {
			continue
		}
		events, err := decodeIcebergReplayEvents(intent)
		if err != nil {
			return err
		}
		for _, ev := range events {
			if s.recoveryEvents[ev.Key] > 0 {
				return fmt.Errorf("Iceberg recovery BLOCKED operation=%s: checkpoint reached commit range before all source event identities replayed", intent.OperationID)
			}
		}
	}
	return nil
}

type snapshotRecoveryRows struct {
	rows  []map[string]interface{}
	start int64
}

// Subtract proven row ranges before splitting chunks, so changing batch size
// cannot duplicate a previously accepted snapshot chunk.
func (s *Sink) unrecoveredSnapshotRows(source string, start int64, rows []map[string]interface{}) []snapshotRecoveryRows {
	var result []snapshotRecoveryRows
	spanStart := -1
	for i := 0; i < len(rows); i++ {
		covered := false
		for _, intent := range s.recoveryIntents {
			if intent.Kind == "snapshot" && recoverableDataOperation(intent.Operation) && intent.SnapshotTable == source && start+int64(i) >= intent.SnapshotStartOffset && start+int64(i) < intent.SnapshotEndOffset {
				covered = true
				break
			}
		}
		if covered && spanStart >= 0 {
			result = append(result, snapshotRecoveryRows{rows[spanStart:i], start + int64(spanStart)})
			spanStart = -1
		}
		if !covered && spanStart < 0 {
			spanStart = i
		}
	}
	if spanStart >= 0 {
		result = append(result, snapshotRecoveryRows{rows[spanStart:], start + int64(spanStart)})
	}
	return result
}
