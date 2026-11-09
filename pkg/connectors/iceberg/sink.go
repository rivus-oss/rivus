package iceberg

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	iceberglib "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icerest "github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/observability"
	"github.com/gerinsp/rivus/pkg/schemachange"
	"github.com/gerinsp/rivus/pkg/util"
)

const (
	defaultIcebergRESTURI     = "http://gravitino:9001/iceberg"
	defaultIcebergCatalogName = "raw"
	catalogInitTimeout        = time.Minute
	catalogInitAttemptTimeout = 15 * time.Second
	checkpointCleanupTimeout  = 2 * time.Second
	checkpointCleanupInterval = 30 * time.Second
)

type Sink struct {
	jobID                string
	stateKey             string
	jobName              string
	cfg                  config.IcebergConfig
	retry                config.RetryPolicy
	recoveryPolicy       *config.RetryPolicy
	recoveryWait         func(context.Context, time.Duration) error
	recoveryOperation    string
	offsetSto            meta.OffsetStore
	progress             connector.ProgressReporter
	catalog              icecatalog.Catalog
	maintenance          *tableMaintenanceMonitor
	maintenanceSignals   *nativeMaintenanceSignaler
	maintenanceOwnership connector.MaintenanceOwnershipLifecycle
	snapshotSpoolDir     string
	authoritativeRebuild bool
	scopedRebuildToken   string
	scopedNativeWrites   bool
	scopedLegacyMerges   map[string]bool
	trinoRebuildMu       sync.Mutex

	equalityCommitter cdcEqualityCommitter

	mu                         sync.Mutex
	sourceSchemas              map[string]*model.TableSchema
	states                     map[string]*tableState
	pendingOffset              *model.SourceOffset
	recoveryIntents            []meta.IcebergCommitIntent
	recoveryLoaded             map[string]bool
	recoveryEvents             map[string]int
	recoveryEventPayloads      map[string]string
	lastCheckpointCommitAt     time.Time
	lastCheckpointBlockedLogAt time.Time
	pendingCheckpointCleanup   *meta.Offset
	lastCheckpointCleanupAt    time.Time
}

func (s *Sink) MaintenanceOwnershipLifecycle() connector.MaintenanceOwnershipLifecycle {
	if s == nil {
		return nil
	}
	return s.maintenanceOwnership
}

type tableState struct {
	sourceKey               string
	targetNamespace         string
	targetTable             string
	sourceSchema            *model.TableSchema
	table                   *icetable.Table
	snapshotAppendSafe      bool
	snapshotReplaceApplied  bool
	snapshotTruncateApplied bool
	pending                 []model.Event
	pendingBytes            int64
	firstPendingAt          time.Time
	lastEventAt             time.Time
	lastTouchedAt           time.Time
	lastFlushAt             time.Time
	lastFlushDuration       time.Duration
	flushCount              uint64
	snapshotSpool           *snapshotSpool
}

type reducedBatch struct {
	filter      iceberglib.BooleanExpression
	deleteKeys  []map[string]interface{}
	deleteRows  []pendingDelete
	pkCols      []string
	rows        []map[string]interface{}
	deleteCount int
}

type commitProgress struct {
	// The proven table replaces a lost catalog response; the write callback
	// must never be called again merely to recover its return value.
	recoveredTable  **icetable.Table
	operation       string
	icebergCommit   bool
	sourceKey       string
	targetNamespace string
	targetTable     string
	rowCount        int
	deleteCount     int
}

type flushResult struct {
	operation   string
	rowCount    int
	deleteCount int
}

const (
	snapshotWriteModeAuto                = "auto"
	snapshotWriteModeOverwrite           = "overwrite"
	snapshotWriteModeDeleteAppend        = "delete-append"
	snapshotWriteModeAppend              = "append"
	snapshotWriteModeReplaceFilterAppend = "replace-filter-append"
	snapshotWriteModeTruncateAppend      = "truncate-append"

	snapshotReplaceDeleteExecutorNative = "native"
	snapshotReplaceDeleteExecutorTrino  = "trino"
	cdcDeleteExecutorEquality           = "equality"
)

func normalizeSnapshotWriteMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return snapshotWriteModeOverwrite
	}
	return mode
}

func normalizeSnapshotReplaceDeleteExecutor(executor string) string {
	executor = strings.ToLower(strings.TrimSpace(executor))
	if executor == "" {
		return snapshotReplaceDeleteExecutorNative
	}
	return executor
}

func isValidSnapshotReplaceDeleteExecutor(executor string) bool {
	switch normalizeSnapshotReplaceDeleteExecutor(executor) {
	case snapshotReplaceDeleteExecutorNative, snapshotReplaceDeleteExecutorTrino:
		return true
	default:
		return false
	}
}

func isValidCDCDeleteExecutor(executor string) bool {
	switch normalizeSnapshotReplaceDeleteExecutor(executor) {
	case snapshotReplaceDeleteExecutorNative, snapshotReplaceDeleteExecutorTrino, cdcDeleteExecutorEquality:
		return true
	default:
		return false
	}
}

func isValidSnapshotWriteMode(mode string) bool {
	switch normalizeSnapshotWriteMode(mode) {
	case snapshotWriteModeAuto, snapshotWriteModeOverwrite, snapshotWriteModeDeleteAppend, snapshotWriteModeAppend, snapshotWriteModeReplaceFilterAppend, snapshotWriteModeTruncateAppend:
		return true
	default:
		return false
	}
}

func cdcKeyDeleteExecutor(cfg config.IcebergConfig) string {
	executor := normalizeSnapshotReplaceDeleteExecutor(cfg.CDCDeleteExecutor)
	switch executor {
	case snapshotReplaceDeleteExecutorTrino, cdcDeleteExecutorEquality:
		return executor
	case snapshotReplaceDeleteExecutorNative:
		if strings.TrimSpace(cfg.TrinoDelete.URI) != "" {
			return snapshotReplaceDeleteExecutorTrino
		}
	}
	return ""
}

func unsupportedNativeKeyDeleteError(kind, sourceKey string) error {
	guidance := "a Trino delete executor"
	if kind == "CDC" {
		guidance = "a Trino delete executor, or explicit cdc_delete_executor: equality for isolated compatibility testing"
	}
	return util.Permanent(fmt.Errorf("%s key delete for %s requires %s; implicit native equality delete is disabled so compatibility testing is intentional",
		kind, sourceKey, guidance))
}

func validateSnapshotTruncateConfig(cfg config.IcebergConfig) error {
	if cfg.SnapshotTruncateAllowPatterns {
		return nil
	}
	for _, pattern := range cfg.SnapshotTruncateTables {
		if strings.ContainsAny(pattern, "*?") {
			return fmt.Errorf("snapshot_truncate_tables entry %q uses a wildcard; set snapshot_truncate_allow_patterns: true only when full target truncation is intentional", pattern)
		}
	}
	return nil
}

func snapshotTablePatternMatches(pattern, sourceKey, srcSchema, srcTable string, allowPattern bool) bool {
	if pattern == sourceKey {
		return true
	}
	if allowPattern && matchSourceOverrideKey(pattern, srcSchema, srcTable) {
		return true
	}
	return false
}

var globalCommitLimiter struct {
	once  sync.Once
	slots chan struct{}
	limit int
}

func NewSink(jobID, stateKey, jobName string, cfg config.IcebergConfig, retry config.RetryPolicy, offsetSto meta.OffsetStore, progress connector.ProgressReporter) (*Sink, error) {
	return newSinkWithRebuild(jobID, stateKey, jobName, cfg, retry, offsetSto, progress, "")
}

func newSinkWithRebuild(jobID, stateKey, jobName string, cfg config.IcebergConfig, retry config.RetryPolicy, offsetSto meta.OffsetStore, progress connector.ProgressReporter, scopedToken string) (*Sink, error) {
	return newSinkWithRebuildMode(jobID, stateKey, jobName, cfg, retry, offsetSto, progress, scopedToken, false)
}

func newSinkWithRebuildMode(jobID, stateKey, jobName string, cfg config.IcebergConfig, retry config.RetryPolicy, offsetSto meta.OffsetStore, progress connector.ProgressReporter, scopedToken string, native bool) (*Sink, error) {
	cfg.SnapshotWriteMode = normalizeSnapshotWriteMode(cfg.SnapshotWriteMode)
	if !isValidSnapshotWriteMode(cfg.SnapshotWriteMode) {
		return nil, fmt.Errorf("invalid iceberg snapshot_write_mode %q (valid: %s, %s, %s, %s, %s, %s)",
			cfg.SnapshotWriteMode,
			snapshotWriteModeAuto,
			snapshotWriteModeOverwrite,
			snapshotWriteModeDeleteAppend,
			snapshotWriteModeAppend,
			snapshotWriteModeReplaceFilterAppend,
			snapshotWriteModeTruncateAppend,
		)
	}
	cfg.SnapshotReplaceDeleteExecutor = normalizeSnapshotReplaceDeleteExecutor(cfg.SnapshotReplaceDeleteExecutor)
	if !isValidSnapshotReplaceDeleteExecutor(cfg.SnapshotReplaceDeleteExecutor) {
		return nil, fmt.Errorf("invalid iceberg snapshot_replace_delete_executor %q (valid: %s, %s)",
			cfg.SnapshotReplaceDeleteExecutor,
			snapshotReplaceDeleteExecutorNative,
			snapshotReplaceDeleteExecutorTrino,
		)
	}
	cfg.CDCDeleteExecutor = normalizeSnapshotReplaceDeleteExecutor(cfg.CDCDeleteExecutor)
	if !isValidCDCDeleteExecutor(cfg.CDCDeleteExecutor) {
		return nil, fmt.Errorf("invalid iceberg cdc_delete_executor %q (valid: %s, %s, %s)",
			cfg.CDCDeleteExecutor,
			snapshotReplaceDeleteExecutorNative,
			snapshotReplaceDeleteExecutorTrino,
			cdcDeleteExecutorEquality,
		)
	}
	if (cfg.SnapshotReplaceDeleteExecutor == snapshotReplaceDeleteExecutorTrino || cfg.CDCDeleteExecutor == snapshotReplaceDeleteExecutorTrino) && strings.TrimSpace(cfg.TrinoDelete.URI) == "" {
		return nil, fmt.Errorf("iceberg trino delete executor requires trino_delete.uri")
	}
	if err := validateSnapshotTruncateConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateAutomaticTableMaintenanceConfig(cfg); err != nil {
		return nil, err
	}

	// Catalog setup performs network discovery. Bound it so an unreachable REST
	// endpoint fails the job instead of occupying a snapshot slot indefinitely.
	catalogCtx, cancelCatalog := context.WithTimeout(context.Background(), catalogInitTimeout)
	defer cancelCatalog()
	cat, err := newCatalogWithRetry(catalogCtx, cfg)
	if err != nil {
		return nil, err
	}

	sink := &Sink{
		scopedRebuildToken: scopedToken,
		scopedNativeWrites: native,
		jobID:              jobID,
		stateKey:           stateKey,
		jobName:            strings.TrimSpace(jobName),
		cfg:                cfg,
		retry:              retry,
		offsetSto:          offsetSto,
		progress:           progress,
		catalog:            cat,
		sourceSchemas:      make(map[string]*model.TableSchema),
		states:             make(map[string]*tableState),
	}
	if strings.TrimSpace(stateKey) != "" || offsetSto != nil {
		sink.recoveryPolicy = automaticRecoveryPolicy(retry)
	}
	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), catalogInitTimeout)
	recoveryErr := sink.reconcileAutomatically(recoveryCtx)
	cancelRecovery()
	if recoveryErr != nil {
		return nil, recoveryErr
	}
	sink.equalityCommitter = rivusEqualityCommitter{sink: sink}
	sink.maintenance = newTableMaintenanceMonitor(jobID, sink.jobName, cfg, sink)
	sink.maintenanceSignals, err = newNativeMaintenanceSignaler(jobID, cfg)
	if err != nil {
		return nil, err
	}
	if snapshotRollingEnabled(cfg) || native {
		spoolDir, err := prepareSnapshotSpoolDirectory(cfg.SnapshotSpoolDirectory, jobID, stateKey)
		if err != nil {
			return nil, err
		}
		sink.snapshotSpoolDir = spoolDir
	}
	return sink, nil
}

func (s *Sink) SetTableMaintenanceStatusReporter(reporter connector.TableMaintenanceStatusReporter) {
	if reporter == nil {
		return
	}
	if s.maintenance == nil {
		reporter(&connector.TableMaintenanceStatus{
			Enabled: false,
			State:   "disabled",
		})
		return
	}
	s.maintenance.setStatusReporter(reporter)
}

func (s *Sink) withCommitSlot(ctx context.Context, progress commitProgress, fn func(context.Context) error) error {
	release, err := acquireGlobalCommitSlot(ctx, s.cfg.MaxConcurrentCommits, func() func() {
		return s.startCommitProgressHeartbeat(ctx, "sink_commit_wait", "Waiting for Iceberg commit slot", progress)
	})
	if err != nil {
		return err
	}
	defer release()

	stop := s.startCommitProgressHeartbeat(ctx, "sink_commit", fmt.Sprintf("Committing Iceberg %s", progress.operation), progress)
	defer stop()

	if progress.icebergCommit && (strings.TrimSpace(s.stateKey) != "" || s.offsetSto != nil) {
		return s.guardedIcebergCommit(ctx, progress, fn)
	}
	return fn(ctx)
}

func acquireGlobalCommitSlot(ctx context.Context, configured int, onWait func() func()) (func(), error) {
	globalCommitLimiter.once.Do(func() {
		limit := configured
		if raw := strings.TrimSpace(os.Getenv("RIVUS_ICEBERG_MAX_CONCURRENT_COMMITS")); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			} else {
				log.Printf("[iceberg] invalid RIVUS_ICEBERG_MAX_CONCURRENT_COMMITS=%q, using configured=%d", raw, configured)
			}
		}
		if limit <= 0 {
			limit = 2
		}
		globalCommitLimiter.limit = limit
		globalCommitLimiter.slots = make(chan struct{}, limit)
		log.Printf("[iceberg] max concurrent commits=%d", limit)
	})

	select {
	case globalCommitLimiter.slots <- struct{}{}:
		return func() { <-globalCommitLimiter.slots }, nil
	default:
	}

	var stopWaiting func()
	if onWait != nil {
		stopWaiting = onWait()
	}
	defer func() {
		if stopWaiting != nil {
			stopWaiting()
		}
	}()

	select {
	case globalCommitLimiter.slots <- struct{}{}:
		return func() { <-globalCommitLimiter.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Sink) startCommitProgressHeartbeat(ctx context.Context, phase, summary string, progress commitProgress) func() {
	if s.progress == nil {
		return func() {}
	}

	send := func() {
		s.progress(connector.ProgressInfo{
			Phase:            phase,
			Summary:          summary,
			Detail:           s.commitProgressDetail(progress),
			CurrentTable:     progress.sourceKey,
			CurrentTableRows: int64(progress.rowCount),
		})
	}
	send()

	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				send()
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		once.Do(func() { close(done) })
	}
}

func (s *Sink) commitProgressDetail(progress commitProgress) string {
	parts := make([]string, 0, 5)
	if progress.sourceKey != "" {
		parts = append(parts, fmt.Sprintf("source=%s", progress.sourceKey))
	}
	target := tableKey(progress.targetNamespace, progress.targetTable)
	if strings.Trim(target, ".") != "" {
		parts = append(parts, fmt.Sprintf("target=%s", target))
	}
	if progress.rowCount > 0 {
		parts = append(parts, fmt.Sprintf("rows=%d", progress.rowCount))
	}
	if progress.deleteCount > 0 {
		parts = append(parts, fmt.Sprintf("delete_keys=%d", progress.deleteCount))
	}
	if globalCommitLimiter.limit > 0 {
		parts = append(parts, fmt.Sprintf("commit_slots=%d/%d", len(globalCommitLimiter.slots), globalCommitLimiter.limit))
	}
	return strings.Join(parts, " | ")
}

func (s *Sink) checkpointKey() string {
	if strings.TrimSpace(s.stateKey) != "" {
		return s.stateKey
	}
	return s.jobID
}

func (s *Sink) rememberOffset(off *model.SourceOffset) {
	if !off.Valid() {
		return
	}
	cp := *off
	s.mu.Lock()
	s.pendingOffset = &cp
	s.mu.Unlock()
}

func (s *Sink) hasPendingEventsLocked() bool {
	for _, state := range s.states {
		if len(state.pending) > 0 {
			return true
		}
	}
	return false
}

func (s *Sink) pendingEventsSummaryLocked() string {
	parts := make([]string, 0, len(s.states))
	for _, state := range s.states {
		if len(state.pending) == 0 {
			continue
		}
		age := "-"
		if !state.firstPendingAt.IsZero() {
			age = time.Since(state.firstPendingAt).Round(time.Second).String()
		}
		parts = append(parts, fmt.Sprintf("%s pending=%d age=%s bytes=%d", state.sourceKey, len(state.pending), age, state.pendingBytes))
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, "; ")
}

func checkpointOffsetText(off *model.SourceOffset) string {
	if off == nil || !off.Valid() {
		return "-"
	}
	return fmt.Sprintf("%s:%d", off.BinlogFile, off.BinlogPos)
}

func (s *Sink) reportCheckpointPending(off *model.SourceOffset, tables string) {
	if s.progress == nil {
		return
	}
	pos := checkpointOffsetText(off)
	s.progress(connector.ProgressInfo{
		Phase:                   "sink_checkpoint_wait",
		Summary:                 "Checkpoint waiting for pending sink events",
		Detail:                  fmt.Sprintf("pos=%s | reason=pending_events | tables=%s", pos, firstNonEmpty(tables, "unknown")),
		CheckpointPending:       true,
		CheckpointReason:        "pending_events",
		CheckpointPosition:      pos,
		CheckpointPendingTables: tables,
	})
}

func (s *Sink) reportCheckpointCommitted(off *model.SourceOffset) {
	if s.progress == nil {
		return
	}
	pos := checkpointOffsetText(off)
	s.progress(connector.ProgressInfo{
		Phase:              "sink_checkpoint",
		Summary:            "Checkpoint committed",
		Detail:             fmt.Sprintf("pos=%s", pos),
		CheckpointPosition: pos,
	})
}

func (s *Sink) commitPendingOffset(ctx context.Context, force bool) error {
	if s.offsetSto == nil {
		return nil
	}
	// Retry maintenance on idle ticks as well as source checkpoints. Its
	// watermark only comes from a successfully saved checkpoint.
	if err := s.cleanupPendingCheckpoint(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	if s.pendingOffset == nil {
		s.mu.Unlock()
		return nil
	}
	if s.hasPendingEventsLocked() {
		now := time.Now()
		off := *s.pendingOffset
		tables := s.pendingEventsSummaryLocked()
		shouldLog := now.Sub(s.lastCheckpointBlockedLogAt) >= 30*time.Second
		if shouldLog {
			s.lastCheckpointBlockedLogAt = now
		}
		s.mu.Unlock()
		if shouldLog {
			log.Printf(
				"[iceberg][job %s] checkpoint pending pos=%s:%d reason=pending_events tables=%s",
				s.jobID,
				off.BinlogFile,
				off.BinlogPos,
				tables,
			)
		}
		s.reportCheckpointPending(&off, tables)
		return nil
	}
	now := time.Now()
	if !s.checkpointSaveDueLocked(now, force) {
		s.mu.Unlock()
		return nil
	}
	off := *s.pendingOffset
	s.mu.Unlock()

	if err := s.checkRecoveryReplayComplete(&off); err != nil {
		return util.Permanent(err)
	}

	saveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)

	checkpointKey := s.checkpointKey()
	err := connector.SaveSourceOffset(saveCtx, s.offsetSto, checkpointKey, &off)
	cancel()
	if err != nil {
		if errors.Is(err, meta.ErrWorkerOwnershipLost) {
			return util.Permanent(err)
		}
		log.Printf("[iceberg][job %s] save offset error key=%s pos=%s:%d: %v", s.jobID, checkpointKey, off.BinlogFile, off.BinlogPos, err)
		return nil
	}
	s.mu.Lock()
	s.lastCheckpointCommitAt = time.Now()
	s.pendingCheckpointCleanup = &meta.Offset{BinlogFile: off.BinlogFile, BinlogPos: off.BinlogPos}
	if s.pendingOffset != nil && s.pendingOffset.BinlogFile == off.BinlogFile && s.pendingOffset.BinlogPos == off.BinlogPos {
		s.pendingOffset = nil
	}
	s.mu.Unlock()

	log.Printf("[iceberg][job %s] committed offset key=%s pos=%s:%d", s.jobID, checkpointKey, off.BinlogFile, off.BinlogPos)
	s.reportCheckpointCommitted(&off)
	return s.cleanupPendingCheckpoint(ctx)
}

// Covered COMMITTED receipts are safe to retain until a later tick or restart.
// Cleanup must neither borrow the checkpoint's remaining timeout nor turn a
// transient metadata error into a failed pipeline after that checkpoint saved.
// Keep the worker lifetime and ownership fence: only maintenance is deferred.
func (s *Sink) cleanupPendingCheckpoint(ctx context.Context) error {
	s.mu.Lock()
	if s.pendingCheckpointCleanup == nil || time.Since(s.lastCheckpointCleanupAt) < checkpointCleanupInterval {
		s.mu.Unlock()
		return nil
	}
	off := *s.pendingCheckpointCleanup
	s.lastCheckpointCleanupAt = time.Now()
	s.mu.Unlock()

	cleanupCtx, cancel := context.WithTimeout(ctx, checkpointCleanupTimeout)
	err := s.cleanupCheckpointedIcebergCommits(cleanupCtx, &off)
	cancel()
	if err != nil {
		if errors.Is(err, meta.ErrWorkerOwnershipLost) {
			return util.Permanent(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("[iceberg][job %s] checkpoint receipt cleanup deferred pos=%s:%d retry_after=%s: %v", s.jobID, off.BinlogFile, off.BinlogPos, checkpointCleanupInterval, err)
		return nil
	}
	s.mu.Lock()
	if s.pendingCheckpointCleanup != nil && *s.pendingCheckpointCleanup == off {
		s.pendingCheckpointCleanup = nil
	}
	// Back off only after a failure. Healthy checkpoints retain their normal
	// immediate cleanup, including the final checkpoint before an idle period.
	s.lastCheckpointCleanupAt = time.Time{}
	s.mu.Unlock()
	return nil
}

func (s *Sink) checkpointSaveDueLocked(now time.Time, force bool) bool {
	if force || s.lastCheckpointCommitAt.IsZero() {
		return true
	}
	interval := time.Duration(s.cfg.CheckpointSaveIntervalSeconds) * time.Second
	return interval <= 0 || now.Sub(s.lastCheckpointCommitAt) >= interval
}

func newCatalog(ctx context.Context, cfg config.IcebergConfig) (icecatalog.Catalog, error) {
	uri := catalogRESTURI(cfg)
	if uri == "" {
		return nil, fmt.Errorf("iceberg_native requires rest_uri/catalog_uri or ICEBERG_REST_URI/ICEBERG_CATALOG_URI")
	}

	warehouse, err := catalogWarehouse(cfg)
	if err != nil {
		return nil, err
	}
	if warehouse == "" {
		return nil, fmt.Errorf("iceberg_native requires warehouse/catalog_name or ICEBERG_WAREHOUSE/ICEBERG_CATALOG_NAME")
	}

	props, err := catalogS3Properties(cfg)
	if err != nil {
		return nil, err
	}

	opts := []icerest.Option{
		icerest.WithWarehouseLocation(warehouse),
		icerest.WithAdditionalProps(props),
	}

	if headers := catalogRESTHeaders(cfg); len(headers) > 0 {
		opts = append(opts, icerest.WithHeaders(headers))
	}

	credential := firstNonEmpty(cfg.Credential, os.Getenv("ICEBERG_CREDENTIAL"), os.Getenv("GRAVITINO_CREDENTIAL"))
	oauthToken := firstNonEmpty(cfg.OAuthToken, os.Getenv("ICEBERG_OAUTH_TOKEN"))
	scope := firstNonEmpty(cfg.Scope, os.Getenv("ICEBERG_SCOPE"), os.Getenv("GRAVITINO_SCOPE"))
	if oauthToken != "" {
		opts = append(opts, icerest.WithOAuthToken(oauthToken))
	} else if credential != "" {
		opts = append(opts, icerest.WithCredential(credential))
	}
	if scope != "" {
		opts = append(opts, icerest.WithScope(scope))
	}
	if rawAuthURI := firstNonEmpty(cfg.OAuthTokenURI, os.Getenv("ICEBERG_OAUTH_TOKEN_URI"), os.Getenv("ICEBERG_REST_AUTH_URI"), os.Getenv("GRAVITINO_OAUTH_TOKEN_URI")); rawAuthURI != "" {
		authURI, err := url.Parse(rawAuthURI)
		if err != nil {
			return nil, fmt.Errorf("invalid iceberg oauth token uri %q: %w", rawAuthURI, err)
		}
		opts = append(opts, icerest.WithAuthURI(authURI))
	}
	if cfg.Prefix != "" {
		opts = append(opts, icerest.WithPrefix(cfg.Prefix))
	}
	if cfg.TLSInsecureSkipVerify {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		opts = append(opts, icerest.WithTLSConfig(tlsConfig), icerest.WithOAuthTLSConfig(tlsConfig))
	}

	cat, err := icerest.NewCatalog(ctx, "rivus", uri, opts...)
	if err != nil {
		return nil, catalogInitializationError(uri, warehouse, err)
	}
	return cat, nil
}

type catalogOpener func(context.Context, config.IcebergConfig) (icecatalog.Catalog, error)

func newCatalogWithRetry(ctx context.Context, cfg config.IcebergConfig) (icecatalog.Catalog, error) {
	return retryCatalogInitialization(ctx, cfg, newCatalog)
}

func retryCatalogInitialization(ctx context.Context, cfg config.IcebergConfig, open catalogOpener) (icecatalog.Catalog, error) {
	if open == nil {
		return nil, errors.New("iceberg catalog opener is nil")
	}

	policy := config.RetryPolicy{
		MaxAttempts: 3,
		BaseBackoff: 500 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
	}
	var cat icecatalog.Catalog
	attempt := 0
	err := util.RetryWithBackoff(ctx, policy, func() error {
		attempt++
		attemptCtx, cancel := context.WithTimeout(ctx, catalogInitAttemptTimeout)
		candidate, err := open(attemptCtx, cfg)
		cancel()
		if err == nil {
			cat = candidate
			return nil
		}
		if errors.Is(err, icerest.ErrUnauthorized) {
			return util.Permanent(err)
		}
		log.Printf("[iceberg] catalog initialization attempt=%d failed: %v", attempt, err)
		return err
	})
	if err != nil {
		return nil, err
	}
	return cat, nil
}

func catalogRESTURI(cfg config.IcebergConfig) string {
	return firstNonEmpty(
		cfg.RestURI,
		cfg.CatalogURI,
		os.Getenv("ICEBERG_REST_URI"),
		os.Getenv("ICEBERG_CATALOG_URI"),
		defaultIcebergRESTURI,
	)
}

func catalogWarehouse(cfg config.IcebergConfig) (string, error) {
	if warehouse := strings.TrimSpace(cfg.Warehouse); warehouse != "" {
		return warehouse, nil
	}

	catalogName := firstNonEmpty(cfg.CatalogName, os.Getenv("ICEBERG_CATALOG_NAME"), defaultIcebergCatalogName)
	template := firstNonEmpty(cfg.WarehouseTemplate, os.Getenv("ICEBERG_WAREHOUSE_TEMPLATE"))
	if template != "" {
		warehouse, err := applyCatalogWarehouseTemplate(template, catalogName)
		if err != nil {
			return "", err
		}
		return warehouse, nil
	}

	return firstNonEmpty(os.Getenv("ICEBERG_WAREHOUSE"), catalogName), nil
}

func applyCatalogWarehouseTemplate(template, catalogName string) (string, error) {
	unknownPlaceholders := strings.ReplaceAll(template, "{catalog}", "")
	if strings.Contains(unknownPlaceholders, "{") || strings.Contains(unknownPlaceholders, "}") {
		return "", fmt.Errorf("iceberg warehouse_template only supports the {catalog} placeholder")
	}
	warehouse := strings.TrimSpace(strings.ReplaceAll(template, "{catalog}", strings.TrimSpace(catalogName)))
	if warehouse == "" {
		return "", fmt.Errorf("iceberg warehouse_template resolved to an empty warehouse")
	}
	return warehouse, nil
}

func catalogS3Properties(cfg config.IcebergConfig) (iceberglib.Properties, error) {
	props := iceberglib.Properties{}
	if region := firstNonEmpty(cfg.S3Region, os.Getenv("ICEBERG_S3_REGION"), os.Getenv("AWS_REGION"), os.Getenv("AWS_DEFAULT_REGION")); region != "" {
		props["s3.region"] = region
		props["client.region"] = region
	}
	if accessKey := firstNonEmpty(os.Getenv("ICEBERG_S3_ACCESS_KEY_ID"), os.Getenv("AWS_ACCESS_KEY_ID")); accessKey != "" {
		props["s3.access-key-id"] = accessKey
	}
	if secret := firstNonEmpty(os.Getenv("ICEBERG_S3_SECRET_ACCESS_KEY"), os.Getenv("AWS_SECRET_ACCESS_KEY")); secret != "" {
		props["s3.secret-access-key"] = secret
	}
	if token := firstNonEmpty(os.Getenv("ICEBERG_S3_SESSION_TOKEN"), os.Getenv("AWS_SESSION_TOKEN")); token != "" {
		props["s3.session-token"] = token
	}
	if endpoint := firstNonEmpty(cfg.S3Endpoint, os.Getenv("ICEBERG_S3_ENDPOINT"), os.Getenv("AWS_S3_ENDPOINT"), os.Getenv("AWS_ENDPOINT_URL_S3")); endpoint != "" {
		props["s3.endpoint"] = endpoint
	}
	compat, err := catalogS3CompatMode(cfg)
	if err != nil {
		return nil, err
	}
	props[iceio.S3CompatMode] = strconv.FormatBool(compat)

	if pathStyle := firstNonEmpty(cfg.S3PathStyle, os.Getenv("ICEBERG_S3_PATH_STYLE"), os.Getenv("AWS_S3_PATH_STYLE")); pathStyle != "" {
		enabled, err := parseBoolEnvValue("iceberg s3_path_style", pathStyle)
		if err != nil {
			return nil, err
		}
		props["s3.force-virtual-addressing"] = strconv.FormatBool(!enabled)
	} else if forcePathStyle := firstNonEmpty(os.Getenv("ICEBERG_S3_FORCE_PATH_STYLE"), os.Getenv("AWS_S3_FORCE_PATH_STYLE")); forcePathStyle != "" {
		enabled, err := parseBoolEnvValue("iceberg force path style", forcePathStyle)
		if err != nil {
			return nil, err
		}
		if enabled {
			props["s3.force-virtual-addressing"] = "false"
		}
	}

	return props, nil
}

// Custom object stores may reject the AWS SDK's checksum trailers and
// aws-chunked framing. Keep AWS defaults on AWS endpoints and allow an explicit
// override for compatible stores or AWS endpoints behind a custom DNS name.
func catalogS3CompatMode(cfg config.IcebergConfig) (bool, error) {
	if raw := firstNonEmpty(cfg.S3CompatMode, os.Getenv("ICEBERG_S3_COMPAT_MODE")); raw != "" {
		return parseBoolEnvValue("iceberg s3_compat_mode", raw)
	}
	endpoint := firstNonEmpty(cfg.S3Endpoint, os.Getenv("ICEBERG_S3_ENDPOINT"), os.Getenv("AWS_S3_ENDPOINT"), os.Getenv("AWS_ENDPOINT_URL_S3"))
	if endpoint == "" {
		return false, nil
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" {
		return false, fmt.Errorf("invalid iceberg S3 endpoint")
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	return host != "amazonaws.com" && !strings.HasSuffix(host, ".amazonaws.com") &&
		host != "amazonaws.com.cn" && !strings.HasSuffix(host, ".amazonaws.com.cn"), nil
}

func catalogRESTHeaders(cfg config.IcebergConfig) map[string]string {
	if header := firstNonEmpty(cfg.RESTAuthHeader, os.Getenv("ICEBERG_REST_AUTH_HEADER")); header != "" {
		return map[string]string{"Authorization": header}
	}

	username := firstNonEmpty(cfg.RESTBasicUsername, os.Getenv("ICEBERG_REST_BASIC_USERNAME"), os.Getenv("GRAVITINO_SIMPLE_AUTH_USER"))
	if username == "" {
		return nil
	}
	password := firstNonEmpty(cfg.RESTBasicPassword, os.Getenv("ICEBERG_REST_BASIC_PASSWORD"), os.Getenv("GRAVITINO_SIMPLE_AUTH_PASSWORD"))
	token := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	return map[string]string{"Authorization": "Basic " + token}
}

func parseBoolEnvValue(name, raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be one of true/false, 1/0, yes/no, on/off; got %q", name, raw)
	}
}

func catalogInitializationError(uri, warehouse string, err error) error {
	return icebergOperationError(fmt.Sprintf("initialize iceberg REST catalog uri=%q warehouse=%q", uri, warehouse), err)
}

func icebergOperationError(action string, err error) error {
	if err == nil {
		return nil
	}
	rawMessage := strings.TrimSpace(err.Error())

	if errors.Is(err, icerest.ErrUnauthorized) {
		if rawMessage == "" || rawMessage == ":" {
			return fmt.Errorf("%s: unauthorized response with an empty body; verify credential/oauth_token, scope, and catalog permissions: %w", action, icerest.ErrUnauthorized)
		}
		return fmt.Errorf("%s: unauthorized; verify credential/oauth_token, scope, and catalog permissions: %w", action, err)
	}
	if rawMessage == "" || rawMessage == ":" {
		return fmt.Errorf("%s: REST catalog returned an empty error response: %w", action, err)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func (s *Sink) operationError(action string, err error) error {
	warehouse := firstNonEmpty(s.cfg.Warehouse, os.Getenv("ICEBERG_WAREHOUSE"))
	return icebergOperationError(fmt.Sprintf("%s warehouse=%q", action, warehouse), err)
}

func (s *Sink) stateOperationError(action string, state *tableState, err error) error {
	if state == nil {
		return s.operationError(action, err)
	}
	return s.operationError(
		fmt.Sprintf("%s source=%q target=%q", action, state.sourceKey, tableKey(state.targetNamespace, state.targetTable)),
		err,
	)
}

func (s *Sink) RegisterSourceSchema(schema, table string, sourceSchema *model.TableSchema) error {
	if sourceSchema == nil {
		return fmt.Errorf("source schema is nil for %s.%s", schema, table)
	}

	key := tableKey(schema, table)

	s.mu.Lock()
	defer s.mu.Unlock()

	augmentedSchema := s.augmentSchemaForMetadata(sourceSchema)
	s.sourceSchemas[key] = copyTableSchema(augmentedSchema)
	st := s.stateForKeyLocked(key)
	st.sourceSchema = copyTableSchema(augmentedSchema)
	st.lastTouchedAt = time.Now()
	return nil
}

func (s *Sink) ResolveTarget(srcSchema, srcTable string) (string, string) {
	sourceKey := tableKey(srcSchema, srcTable)
	targetNamespace := strings.TrimSpace(srcSchema)
	if s.cfg.DefaultNamespace != "" {
		targetNamespace = s.cfg.DefaultNamespace
	}
	targetTable := strings.TrimSpace(srcTable)

	if override, ok := s.resolveTargetOverride(sourceKey, srcSchema, srcTable); ok {
		if strings.TrimSpace(override.Namespace) != "" {
			targetNamespace = strings.TrimSpace(override.Namespace)
		}
		if strings.TrimSpace(override.Table) != "" {
			targetTable = strings.TrimSpace(override.Table)
		}
	}

	return targetNamespace, targetTable
}

func (s *Sink) resolveTargetOverride(sourceKey, srcSchema, srcTable string) (config.IcebergTarget, bool) {
	if override, ok := s.cfg.Overrides[sourceKey]; ok {
		return override, true
	}

	matches := make([]string, 0)
	for key := range s.cfg.Overrides {
		if key == sourceKey || !matchSourceOverrideKey(key, srcSchema, srcTable) {
			continue
		}
		matches = append(matches, key)
	}
	if len(matches) == 0 {
		return config.IcebergTarget{}, false
	}
	sort.Slice(matches, func(i, j int) bool {
		leftScore := globSpecificity(matches[i])
		rightScore := globSpecificity(matches[j])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return matches[i] < matches[j]
	})
	return s.cfg.Overrides[matches[0]], true
}

func (s *Sink) snapshotReplaceFilterForState(state *tableState) (config.IcebergSnapshotReplaceFilterConfig, bool) {
	if state == nil || state.sourceSchema == nil {
		return config.IcebergSnapshotReplaceFilterConfig{}, false
	}
	if filter, ok := s.cfg.SnapshotReplaceFilters[state.sourceKey]; ok {
		return filter, true
	}

	matches := make([]string, 0)
	srcSchema := state.sourceSchema.SchemaName
	srcTable := state.sourceSchema.TableName
	for key := range s.cfg.SnapshotReplaceFilters {
		if key == state.sourceKey || !matchSourceOverrideKey(key, srcSchema, srcTable) {
			continue
		}
		matches = append(matches, key)
	}
	if len(matches) == 0 {
		return config.IcebergSnapshotReplaceFilterConfig{}, false
	}
	sort.Slice(matches, func(i, j int) bool {
		leftScore := globSpecificity(matches[i])
		rightScore := globSpecificity(matches[j])
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return matches[i] < matches[j]
	})
	return s.cfg.SnapshotReplaceFilters[matches[0]], true
}

func (s *Sink) snapshotTruncateForState(state *tableState) bool {
	if state == nil || state.sourceSchema == nil {
		return false
	}
	srcSchema := state.sourceSchema.SchemaName
	srcTable := state.sourceSchema.TableName
	for _, pattern := range s.cfg.SnapshotTruncateExcludeTables {
		if snapshotTablePatternMatches(pattern, state.sourceKey, srcSchema, srcTable, true) {
			return false
		}
	}
	for _, pattern := range s.cfg.SnapshotTruncateTables {
		if snapshotTablePatternMatches(pattern, state.sourceKey, srcSchema, srcTable, s.cfg.SnapshotTruncateAllowPatterns) {
			return true
		}
	}
	return false
}

func (s *Sink) snapshotReplaceFilterExpression(state *tableState) (iceberglib.BooleanExpression, string, error) {
	if state == nil || state.table == nil || state.table.Schema() == nil {
		return nil, "", fmt.Errorf("table handle is nil for snapshot replace filter")
	}
	filterCfg, ok := s.snapshotReplaceFilterForState(state)
	if !ok {
		return nil, "", fmt.Errorf("snapshot_write_mode %s requires snapshot_replace_filters entry for %s", snapshotWriteModeReplaceFilterAppend, state.sourceKey)
	}

	field, ok := state.table.Schema().FindFieldByNameCaseInsensitive(filterCfg.Column)
	if !ok {
		return nil, "", fmt.Errorf("snapshot replace filter field %q not found in iceberg schema for %s", filterCfg.Column, state.sourceKey)
	}
	op, err := snapshotReplaceFilterOperation(filterCfg.Op)
	if err != nil {
		return nil, "", err
	}
	lit, err := literalForValue(field.Type, filterCfg.Value)
	if err != nil {
		return nil, "", fmt.Errorf("snapshot replace filter field %s: %w", field.Name, err)
	}

	description := fmt.Sprintf("%s %s %s", field.Name, filterCfg.Op, filterCfg.Value)
	return iceberglib.LiteralPredicate(op, iceberglib.Reference(field.Name), lit), description, nil
}

func (s *Sink) snapshotReplaceFilterTrinoSQL(state *tableState) (string, string, error) {
	if state == nil || state.table == nil || state.table.Schema() == nil {
		return "", "", fmt.Errorf("table handle is nil for snapshot replace filter")
	}
	filterCfg, ok := s.snapshotReplaceFilterForState(state)
	if !ok {
		return "", "", fmt.Errorf("snapshot_write_mode %s requires snapshot_replace_filters entry for %s", snapshotWriteModeReplaceFilterAppend, state.sourceKey)
	}

	field, ok := state.table.Schema().FindFieldByNameCaseInsensitive(filterCfg.Column)
	if !ok {
		return "", "", fmt.Errorf("snapshot replace filter field %q not found in iceberg schema for %s", filterCfg.Column, state.sourceKey)
	}
	op, err := trinoSnapshotReplaceFilterOperator(filterCfg.Op)
	if err != nil {
		return "", "", err
	}
	lit, err := trinoLiteralForValue(field.Type, filterCfg.Value)
	if err != nil {
		return "", "", fmt.Errorf("snapshot replace filter field %s: %w", field.Name, err)
	}

	catalog := strings.TrimSpace(s.cfg.TrinoDelete.Catalog)
	if catalog == "" {
		catalog = strings.TrimSpace(s.cfg.Warehouse)
	}
	if catalog == "" {
		return "", "", fmt.Errorf("iceberg trino_delete.catalog is empty and warehouse fallback is empty")
	}

	description := fmt.Sprintf("%s %s %s", field.Name, filterCfg.Op, filterCfg.Value)
	query := fmt.Sprintf("DELETE FROM %s WHERE %s %s %s",
		quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable),
		quoteTrinoIdentifier(field.Name),
		op,
		lit,
	)
	return query, description, nil
}

func (s *Sink) snapshotTruncateTrinoSQL(state *tableState) (string, error) {
	if state == nil {
		return "", fmt.Errorf("table state is nil for snapshot truncate")
	}
	catalog := strings.TrimSpace(s.cfg.TrinoDelete.Catalog)
	if catalog == "" {
		catalog = strings.TrimSpace(s.cfg.Warehouse)
	}
	if catalog == "" {
		return "", fmt.Errorf("iceberg trino_delete.catalog is empty and warehouse fallback is empty")
	}
	if strings.TrimSpace(state.targetNamespace) == "" || strings.TrimSpace(state.targetTable) == "" {
		return "", fmt.Errorf("target table is incomplete for snapshot truncate")
	}
	return fmt.Sprintf("DELETE FROM %s", quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable)), nil
}

func (s *Sink) keyDeleteTrinoSQL(state *tableState, keys []map[string]interface{}) (string, error) {
	if state == nil || state.table == nil || state.table.Schema() == nil {
		return "", fmt.Errorf("table handle is nil for key delete")
	}
	if len(keys) == 0 {
		return "", fmt.Errorf("key delete requires at least one key")
	}
	catalog := strings.TrimSpace(s.cfg.TrinoDelete.Catalog)
	if catalog == "" {
		catalog = strings.TrimSpace(s.cfg.Warehouse)
	}
	if catalog == "" {
		return "", fmt.Errorf("iceberg trino_delete.catalog is empty and warehouse fallback is empty")
	}

	predicates := make([]string, 0, len(keys))
	for _, key := range keys {
		parts := make([]string, 0, len(key))
		for col, value := range key {
			field, ok := state.table.Schema().FindFieldByNameCaseInsensitive(col)
			if !ok {
				return "", fmt.Errorf("field %s not found in iceberg schema", col)
			}
			quotedField := quoteTrinoIdentifier(field.Name)
			if normalized, partial := normalizeMySQLPartialTemporalValueForType(field.Type, value, !field.Required); partial && normalized == nil {
				parts = append(parts, quotedField+" IS NULL")
				continue
			} else if partial {
				value = normalized
			}
			lit, err := trinoLiteralForAny(field.Type, value)
			if err != nil {
				return "", fmt.Errorf("field %s: %w", field.Name, err)
			}
			parts = append(parts, fmt.Sprintf("%s = %s", quotedField, lit))
		}
		if len(parts) == 0 {
			return "", fmt.Errorf("empty delete key for %s", state.sourceKey)
		}
		sort.Strings(parts)
		predicates = append(predicates, "("+strings.Join(parts, " AND ")+")")
	}

	return fmt.Sprintf("DELETE FROM %s WHERE %s",
		quoteTrinoQualifiedName(catalog, state.targetNamespace, state.targetTable),
		strings.Join(predicates, " OR "),
	), nil
}

func snapshotReplaceFilterOperation(raw string) (iceberglib.Operation, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "=", "==", "eq":
		return iceberglib.OpEQ, nil
	case "!=", "<>", "ne", "neq":
		return iceberglib.OpNEQ, nil
	case ">", "gt":
		return iceberglib.OpGT, nil
	case ">=", "gte", "gteq":
		return iceberglib.OpGTEQ, nil
	case "<", "lt":
		return iceberglib.OpLT, nil
	case "<=", "lte", "lteq":
		return iceberglib.OpLTEQ, nil
	default:
		return 0, fmt.Errorf("unsupported snapshot replace filter op %q", raw)
	}
}

func trinoSnapshotReplaceFilterOperator(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "=", "==", "eq":
		return "=", nil
	case "!=", "<>", "ne", "neq":
		return "<>", nil
	case ">", "gt":
		return ">", nil
	case ">=", "gte", "gteq":
		return ">=", nil
	case "<", "lt":
		return "<", nil
	case "<=", "lte", "lteq":
		return "<=", nil
	default:
		return "", fmt.Errorf("unsupported snapshot replace filter op %q", raw)
	}
}

func quoteTrinoQualifiedName(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteTrinoIdentifier(part))
	}
	return strings.Join(quoted, ".")
}

func quoteTrinoIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteTrinoStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func trinoLiteralForValue(typ iceberglib.Type, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("empty literal is not allowed for snapshot replace filter")
	}

	switch typ.(type) {
	case iceberglib.DateType:
		dateValue, err := normalizeTrinoDateLiteral(value)
		if err != nil {
			return "", err
		}
		return "DATE " + quoteTrinoStringLiteral(dateValue), nil
	case iceberglib.TimestampType, iceberglib.TimestampNsType:
		tsValue, err := normalizeTrinoTimestampLiteral(value)
		if err != nil {
			return "", err
		}
		return "TIMESTAMP " + quoteTrinoStringLiteral(tsValue), nil
	case iceberglib.TimestampTzType, iceberglib.TimestampTzNsType:
		tsValue, err := normalizeTrinoTimestampLiteral(value)
		if err != nil {
			return "", err
		}
		return "TIMESTAMP " + quoteTrinoStringLiteral(tsValue), nil
	case iceberglib.StringType, iceberglib.UUIDType:
		return quoteTrinoStringLiteral(value), nil
	case iceberglib.BooleanType:
		switch strings.ToLower(value) {
		case "true", "false":
			return strings.ToLower(value), nil
		default:
			return "", fmt.Errorf("invalid boolean literal %q", raw)
		}
	case iceberglib.Int32Type, iceberglib.Int64Type, iceberglib.Float32Type, iceberglib.Float64Type, iceberglib.DecimalType:
		if strings.ContainsAny(value, "'\";") {
			return "", fmt.Errorf("invalid numeric literal %q", raw)
		}
		return value, nil
	default:
		return "", fmt.Errorf("unsupported trino snapshot replace filter type %s", typ)
	}
}

func trinoLiteralForAny(typ iceberglib.Type, value interface{}) (string, error) {
	if value == nil {
		return "", fmt.Errorf("nil literal is not allowed for key delete")
	}

	switch typ.(type) {
	case iceberglib.DateType:
		tm, err := toTime(value)
		if err != nil {
			return "", err
		}
		return "DATE " + quoteTrinoStringLiteral(tm.UTC().Format("2006-01-02")), nil
	case iceberglib.TimestampType, iceberglib.TimestampNsType, iceberglib.TimestampTzType, iceberglib.TimestampTzNsType:
		tm, err := toTime(value)
		if err != nil {
			return "", err
		}
		return "TIMESTAMP " + quoteTrinoStringLiteral(tm.UTC().Format("2006-01-02 15:04:05")), nil
	case iceberglib.StringType, iceberglib.UUIDType:
		switch v := value.(type) {
		case []byte:
			return quoteTrinoStringLiteral(string(v)), nil
		default:
			return quoteTrinoStringLiteral(toString(value)), nil
		}
	case iceberglib.BooleanType:
		v, err := toBool(value)
		if err != nil {
			return "", err
		}
		if v {
			return "true", nil
		}
		return "false", nil
	case iceberglib.Int32Type, iceberglib.Int64Type:
		v, err := toInt64(value)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(v, 10), nil
	case iceberglib.Float32Type, iceberglib.Float64Type:
		v, err := toFloat64(value)
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case iceberglib.DecimalType:
		switch v := value.(type) {
		case json.Number:
			return v.String(), nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprint(v), nil
		}
	default:
		return "", fmt.Errorf("unsupported trino key delete type %s", typ)
	}
}

func normalizeTrinoDateLiteral(value string) (string, error) {
	if len(value) >= len("2006-01-02") {
		candidate := value[:len("2006-01-02")]
		if _, err := time.Parse("2006-01-02", candidate); err == nil {
			return candidate, nil
		}
	}
	tm, err := toTime(value)
	if err != nil {
		return "", err
	}
	return tm.UTC().Format("2006-01-02"), nil
}

func normalizeTrinoTimestampLiteral(value string) (string, error) {
	value = strings.TrimSuffix(strings.ReplaceAll(value, "T", " "), "Z")
	if len(value) == len("2006-01-02") {
		value += " 00:00:00"
	}
	if dot := strings.IndexByte(value, '.'); dot >= 0 && len(value) > dot+7 {
		value = value[:dot+7]
	}
	layouts := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if _, err := time.Parse(layout, value); err == nil {
			if layout == "2006-01-02" {
				return value + " 00:00:00", nil
			}
			return value, nil
		}
	}
	tm, err := toTime(value)
	if err != nil {
		return "", err
	}
	return tm.UTC().Format("2006-01-02 15:04:05"), nil
}

func (s *Sink) EnsureTable(ctx context.Context, targetSchema, targetTable string, schema *model.TableSchema) error {
	if schema == nil {
		return fmt.Errorf("source schema is nil")
	}

	sourceKey := tableKey(schema.SchemaName, schema.TableName)
	schema = s.augmentSchemaForMetadata(schema)
	s.mu.Lock()
	state := s.stateForKeyLocked(sourceKey)
	state.sourceSchema = copyTableSchema(schema)
	state.targetNamespace = targetSchema
	state.targetTable = targetTable
	s.mu.Unlock()
	observability.RegisterSourceTable(s.jobID, sourceKey)
	observability.SetSinkType(s.jobID, sourceKey, "iceberg")
	observability.SetTargetTable(s.jobID, sourceKey, tableKey(targetSchema, targetTable))

	pkCols, err := s.primaryKeysFor(sourceKey, schema)
	if err != nil {
		return util.Permanent(err)
	}

	if err := s.ensureNamespace(ctx, targetSchema); err != nil {
		return err
	}

	tbl, created, err := s.loadOrCreateTable(ctx, targetSchema, targetTable, schema, pkCols)
	if err != nil {
		return err
	}
	snapshotAppendSafe := created
	if !created {
		tbl, err = s.syncTableSchema(ctx, tbl, targetSchema, targetTable, schema, pkCols)
		if err != nil {
			return err
		}
		if normalizeSnapshotWriteMode(s.cfg.SnapshotWriteMode) == snapshotWriteModeAuto {
			modeState := &tableState{
				sourceKey:          sourceKey,
				sourceSchema:       schema,
				snapshotAppendSafe: snapshotAppendSafe,
			}
			if s.shouldCountSnapshotAutoTarget(modeState) {
				rows, err := countIcebergTableRows(ctx, tbl)
				if err != nil {
					return s.operationError(fmt.Sprintf("count table target=%q for snapshot auto mode", tableKey(targetSchema, targetTable)), err)
				}
				snapshotAppendSafe = rows == 0
				modeState.snapshotAppendSafe = snapshotAppendSafe
				selectedMode := s.snapshotWriteModeForTableState(modeState)
				log.Printf("[iceberg][job %s] snapshot auto target=%s rows=%d selected_mode=%s",
					s.jobID,
					tableKey(targetSchema, targetTable),
					rows,
					selectedMode,
				)
			} else {
				selectedMode := s.snapshotWriteModeForTableState(modeState)
				log.Printf("[iceberg][job %s] snapshot auto target=%s count_skipped=true selected_mode=%s",
					s.jobID,
					tableKey(targetSchema, targetTable),
					selectedMode,
				)
			}
		}
	} else if normalizeSnapshotWriteMode(s.cfg.SnapshotWriteMode) == snapshotWriteModeAuto {
		log.Printf("[iceberg][job %s] snapshot auto target=%s created=true selected_mode=%s",
			s.jobID,
			tableKey(targetSchema, targetTable),
			snapshotWriteModeAppend,
		)
	}

	s.mu.Lock()
	now := time.Now()
	state = s.stateForKeyLocked(sourceKey)
	state.table = tbl
	state.snapshotAppendSafe = snapshotAppendSafe
	state.lastTouchedAt = now
	s.updateTargetTableStatesLocked(targetSchema, targetTable, tbl, now)
	s.mu.Unlock()
	if s.maintenance != nil {
		s.maintenance.registerTable(config.IcebergTarget{Namespace: targetSchema, Table: targetTable}, tbl, now)
	}

	return nil
}

func (s *Sink) SkipSnapshotTableWithoutPrimaryKey(sourceSchema, sourceTable string, schema *model.TableSchema) bool {
	if !s.cfg.SkipSnapshotTablesWithoutPK {
		return false
	}
	sourceKey := tableKey(sourceSchema, sourceTable)
	_, err := s.primaryKeysFor(sourceKey, s.augmentSchemaForMetadata(schema))
	return err != nil
}

func (s *Sink) shouldCountSnapshotAutoTarget(state *tableState) bool {
	if normalizeSnapshotWriteMode(s.cfg.SnapshotWriteMode) != snapshotWriteModeAuto {
		return false
	}
	if state == nil {
		return true
	}
	if state.snapshotAppendSafe {
		return false
	}
	if _, ok := s.snapshotReplaceFilterForState(state); ok {
		return false
	}
	if s.snapshotTruncateForState(state) {
		return false
	}
	return true
}

func (s *Sink) CountTargetRows(ctx context.Context, targetSchema, targetTable string) (int64, error) {
	tbl, err := s.catalog.LoadTable(ctx, namespaceIdentifier(targetSchema, targetTable))
	if err != nil {
		if errors.Is(err, icecatalog.ErrNoSuchTable) {
			return 0, nil
		}
		return 0, s.operationError(fmt.Sprintf("load table target=%q", tableKey(targetSchema, targetTable)), err)
	}
	rows, err := countIcebergTableRows(ctx, tbl)
	if err != nil {
		return 0, s.operationError(fmt.Sprintf("count table target=%q", tableKey(targetSchema, targetTable)), err)
	}
	return rows, nil
}

func (s *Sink) ResetTargetTable(ctx context.Context, targetSchema, targetTable string) error {
	ident := namespaceIdentifier(targetSchema, targetTable)
	tbl, err := s.catalog.LoadTable(ctx, ident)
	if err != nil {
		if errors.Is(err, icecatalog.ErrNoSuchTable) {
			return nil
		}
		return s.operationError(fmt.Sprintf("load table for reset target=%q", tableKey(targetSchema, targetTable)), err)
	}

	if s.cfg.SnapshotReplaceDeleteExecutor == snapshotReplaceDeleteExecutorTrino {
		state := &tableState{
			sourceKey:       tableKey(targetSchema, targetTable),
			targetNamespace: targetSchema,
			targetTable:     targetTable,
			table:           tbl,
		}
		if err := s.ensureSnapshotTruncateAppliedWithTrino(ctx, state); err != nil {
			return s.operationError(fmt.Sprintf("reset table target=%q", tableKey(targetSchema, targetTable)), err)
		}
		s.mu.Lock()
		s.updateTargetTableStatesLocked(targetSchema, targetTable, state.table, time.Now())
		for _, candidate := range s.states {
			if sameTarget(candidate.targetNamespace, targetSchema) && sameTarget(candidate.targetTable, targetTable) {
				candidate.snapshotTruncateApplied = true
			}
		}
		s.mu.Unlock()
		return nil
	}

	props := s.snapshotPropsForTarget(targetSchema, targetTable)
	var updated *icetable.Table
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       "reset",
		icebergCommit:   true,
		recoveredTable:  &updated,
		targetNamespace: targetSchema,
		targetTable:     targetTable,
	}, func(ctx context.Context) error {
		var commitErr error
		updated, commitErr = commitDelete(ctx, tbl, iceberglib.AlwaysTrue{}, icebergCommitProperties(ctx, props), icetable.WithDeleteConcurrency(s.cfg.DeleteConcurrency))
		return commitErr
	})
	if err != nil {
		return s.operationError(fmt.Sprintf("reset table target=%q", tableKey(targetSchema, targetTable)), err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, state := range s.states {
		if sameTarget(state.targetNamespace, targetSchema) && sameTarget(state.targetTable, targetTable) {
			state.table = updated
			state.lastTouchedAt = time.Now()
			state.snapshotTruncateApplied = true
		}
	}
	return nil
}

func (s *Sink) Run(ctx context.Context, in <-chan model.Event) error {
	defer s.cleanupSnapshotSpoolDirectory()
	if err := s.reconcileAutomatically(ctx); err != nil {
		log.Printf("[iceberg][job %s] %v", s.jobID, err)
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	if s.maintenance != nil {
		go s.maintenance.run(ctx)
	}
	if s.maintenanceSignals != nil {
		go s.maintenanceSignals.run(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-in:
			if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
				return util.Permanent(err)
			}
			if !ok {
				return s.flushAll(ctx)
			}
			if err := s.handleEvent(ctx, ev); err != nil {
				return err
			}
		case now := <-ticker.C:
			if err := s.flushDue(ctx, now); err != nil {
				return err
			}
			s.evictIdle(now)
		}
	}
}

func (s *Sink) handleEvent(ctx context.Context, ev model.Event) error {
	if ev.Type == model.EventTypeSnapshotStart || ev.Type == model.EventTypeSnapshotComplete {
		return s.handleSnapshotBarrier(ctx, ev)
	}
	if ev.Type == model.EventTypeCheckpoint {
		s.rememberOffset(ev.SourceOffset)
		return s.commitPendingOffset(ctx, false)
	}
	if ev.Type == model.EventTypeSnapshotBatch {
		return s.handleSnapshotBatch(ctx, ev)
	}
	if ev.Type == model.EventTypeSnapshotTableEnd {
		return s.handleSnapshotTableComplete(ctx, ev)
	}
	if ev.Type == model.EventTypeDDL || ev.SourceOffset.Valid() {
		ctx = withIcebergBatch(ctx, cdcIcebergBatch(tableKey(ev.Schema, ev.Table), []model.Event{ev}))
	}

	if skipped, err := s.skipRecoveredCDCEvent(ev); err != nil {
		return util.Permanent(err)
	} else if skipped {
		return nil
	}

	sourceKey := tableKey(ev.Schema, ev.Table)
	state, err := s.ensureOperationalState(ctx, sourceKey)
	if err != nil {
		return err
	}
	if err := s.flushPriorPendingForTarget(ctx, state); err != nil {
		return err
	}

	if ev.Type == model.EventTypeDDL {
		if err := s.flushState(ctx, state); err != nil {
			return err
		}
		return s.applyDDL(ctx, state, ev)
	}

	now := time.Now()
	if len(state.pending) == 0 {
		state.firstPendingAt = now
	}
	state.pending = append(state.pending, ev)
	state.pendingBytes += estimateEventBytes(ev)
	state.lastEventAt = now
	state.lastTouchedAt = now
	if len(state.pending) >= s.cfg.BatchSize || s.pendingBytesLimitReached(state) {
		return s.flushState(ctx, state)
	}
	return nil
}

func (s *Sink) handleSnapshotBarrier(ctx context.Context, ev model.Event) (err error) {
	if ev.Ack != nil {
		defer func() {
			ev.Ack <- err
			close(ev.Ack)
		}()
	}

	switch ev.Type {
	case model.EventTypeSnapshotStart:
		if s.authoritativeRebuild || s.scopedRebuildToken != "" {
			if s.offsetSto == nil {
				return fmt.Errorf("snapshot rebuild requires durable metadata")
			}
			if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
				return util.Permanent(err)
			}
			state, err := s.offsetSto.GetSnapshotState(ctx, s.checkpointKey())
			if err != nil {
				return err
			}
			if state == nil || state.Done || state.StartFile == "" || state.StartPos == 0 {
				return fmt.Errorf("snapshot rebuild BLOCKED: missing unfinished snapshot start after target reset")
			}
			if s.scopedRebuildToken != "" {
				if err := s.confirmScopedResetBarrier(ctx); err != nil {
					return err
				}
			}
			if err := meta.CheckOwnership(ctx, s.offsetSto); err != nil {
				return util.Permanent(err)
			}
			// The rebuild token belongs to this submission. Its snapshot start
			// is saved only after authoritative preflight reset succeeds. Resume
			// therefore appends the remaining snapshot rows without re-truncating
			// a prefix already committed by the previous owner.
			s.mu.Lock()
			for _, target := range s.states {
				target.snapshotTruncateApplied = true
				if s.scopedRebuildToken != "" {
					target.snapshotReplaceApplied = true
				}
			}
			s.mu.Unlock()
		}
		s.cleanupSnapshotSpools()
		if s.maintenanceSignals != nil {
			s.maintenanceSignals.setSnapshotComplete(false)
		}
		if s.maintenance != nil {
			s.maintenance.pauseSubmissions()
		}
	case model.EventTypeSnapshotComplete:
		// A rolling batch ACK confirms only local spool durability. Completion
		// must not bypass the table-end catalog commit, even with no pending rows.
		for _, state := range s.states {
			if state != nil && state.snapshotSpool != nil {
				return fmt.Errorf("snapshot completion BLOCKED: uncommitted rolling spool for %s", state.sourceKey)
			}
		}
		if err := s.flushAll(ctx); err != nil {
			return err
		}
		if err := connector.ConfirmSnapshotCompletion(ctx, s.offsetSto, s.checkpointKey()); err != nil {
			return err
		}
		if s.scopedRebuildToken != "" {
			if err := s.cleanupScopedRebuildIntents(ctx); err != nil {
				return err
			}
		}
		if s.maintenance != nil {
			s.maintenance.resumeSubmissions()
		}
		if s.maintenanceSignals != nil {
			s.maintenanceSignals.setSnapshotComplete(true)
		}
	}
	return nil
}

func (s *Sink) handleSnapshotBatch(ctx context.Context, ev model.Event) (err error) {
	if ev.Ack != nil {
		defer func() {
			ev.Ack <- err
			close(ev.Ack)
		}()
	}
	if len(ev.Rows) == 0 {
		return nil
	}

	sourceKey := tableKey(ev.Schema, ev.Table)
	ctx = withIcebergBatch(ctx, snapshotIcebergBatch(sourceKey, ev.SnapshotStartOffset, ev.SnapshotStartOffset+int64(len(ev.Rows))))
	state, err := s.ensureOperationalState(ctx, sourceKey)
	if err != nil {
		return err
	}
	if s.scopedNativeWrites {
		if err := s.validateScopedSnapshotKeys(state, ev.Rows); err != nil {
			return util.Permanent(err)
		}
	}
	if err := s.flushPriorPendingForTarget(ctx, state); err != nil {
		return err
	}
	if err := s.flushState(ctx, state); err != nil {
		return err
	}
	if ev.SnapshotRolling && len(s.unrecoveredSnapshotRows(sourceKey, ev.SnapshotStartOffset, ev.Rows)) == 0 {
		return nil
	}
	if ev.SnapshotRolling && snapshotRollingEnabled(s.cfg) && supportsRollingSnapshotMode(s.snapshotWriteModeForTableState(state)) {
		if ev.SnapshotStartOffset == 0 {
			s.resetSnapshotSpool(state)
		}
		if err := s.appendSnapshotSpool(ctx, state, ev.Rows, ev.Timestamp, ev.SnapshotStartOffset); err != nil {
			return err
		}
		clearRows(ev.Rows)
		return nil
	}

	startedAt := time.Now()
	result := flushResult{operation: "snapshot-batch"}
	for _, span := range s.unrecoveredSnapshotRows(sourceKey, ev.SnapshotStartOffset, ev.Rows) {
		chunks := splitRowsByLimits(span.rows, s.cfg.SnapshotBatchSize, int64(s.cfg.MaxBatchBytes))
		if s.scopedNativeWrites && len(chunks) > 1 {
			spanCtx := withIcebergBatch(ctx, snapshotIcebergBatch(sourceKey, span.start, span.start+int64(len(span.rows))))
			if err := s.refreshStateTable(spanCtx, state); err != nil {
				return err
			}
			chunkResult, err := s.flushScopedNativeSnapshotSpool(spanCtx, state, span.rows, ev.Timestamp, span.start)
			if err != nil {
				return err
			}
			result.operation = chunkResult.operation
			result.rowCount += chunkResult.rowCount
			result.deleteCount += chunkResult.deleteCount
			continue
		}
		rowOffset := 0
		for _, rows := range chunks {
			batchStartOffset := span.start + int64(rowOffset)
			chunkCtx := withIcebergBatch(ctx, snapshotIcebergBatch(sourceKey, batchStartOffset, batchStartOffset+int64(len(rows))))
			var chunkResult flushResult
			if err := util.RetryWithBackoff(chunkCtx, s.retry, func() error {
				if err := s.refreshStateTable(chunkCtx, state); err != nil {
					return err
				}
				var flushErr error
				chunkResult, flushErr = s.flushSnapshotRowsOnce(chunkCtx, state, rows, ev.Timestamp, batchStartOffset)
				return flushErr
			}); err != nil {
				return err
			}
			result.operation = chunkResult.operation
			result.rowCount += chunkResult.rowCount
			result.deleteCount += chunkResult.deleteCount
			rowOffset += len(rows)
		}
	}

	completedAt := time.Now()
	duration := completedAt.Sub(startedAt)

	s.mu.Lock()
	state.lastTouchedAt = completedAt
	state.lastFlushAt = completedAt
	state.lastFlushDuration = duration
	state.flushCount++
	flushCount := state.flushCount
	s.mu.Unlock()

	log.Printf(
		"[iceberg][job %s] snapshot-batch table=%s op=%s rows=%d deletes=%d duration=%s flush_count=%d",
		s.jobID,
		state.sourceKey,
		result.operation,
		result.rowCount,
		result.deleteCount,
		duration.Round(time.Millisecond),
		flushCount,
	)
	observability.RecordSinkFlush(s.jobID, state.sourceKey, tableKey(state.targetNamespace, state.targetTable), "snapshot", result.operation, 0, result.rowCount, result.deleteCount, duration)
	clearRows(ev.Rows)
	return s.commitPendingOffset(ctx, true)
}

func (s *Sink) pendingBytesLimitReached(state *tableState) bool {
	return s.cfg.MaxBatchBytes > 0 && state.pendingBytes >= int64(s.cfg.MaxBatchBytes)
}

func splitRowsByLimits(rows []map[string]interface{}, maxRows int, maxBytes int64) [][]map[string]interface{} {
	if len(rows) == 0 {
		return nil
	}
	if maxRows <= 0 {
		maxRows = len(rows)
	}

	out := make([][]map[string]interface{}, 0, 1)
	start := 0
	var batchBytes int64
	for idx, row := range rows {
		rowBytes := estimateRowBytes(row)
		if idx > start && ((idx-start) >= maxRows || (maxBytes > 0 && batchBytes+rowBytes > maxBytes)) {
			out = append(out, rows[start:idx])
			start = idx
			batchBytes = 0
		}
		batchBytes += rowBytes
	}
	out = append(out, rows[start:])
	return out
}

func (s *Sink) flushDue(ctx context.Context, now time.Time) error {
	s.mu.Lock()
	states, checkpointBarrier, checkpointOffset := s.flushStatesDueLocked(now)
	s.mu.Unlock()

	if checkpointBarrier {
		log.Printf(
			"[iceberg][job %s] checkpoint barrier flushing pending tables=%d pos=%s",
			s.jobID,
			len(states),
			checkpointOffsetText(checkpointOffset),
		)
	}
	for _, st := range states {
		if err := s.flushState(ctx, st); err != nil {
			return err
		}
	}
	return s.commitPendingOffset(ctx, false)
}

func (s *Sink) flushStatesDueLocked(now time.Time) ([]*tableState, bool, *model.SourceOffset) {
	due := make([]*tableState, 0, len(s.states))
	pending := make([]*tableState, 0, len(s.states))
	for _, st := range s.states {
		if st == nil || len(st.pending) == 0 {
			continue
		}
		pending = append(pending, st)
		if s.shouldFlushStateLocked(st, now) {
			due = append(due, st)
		}
	}

	checkpointBarrier := s.pendingOffset != nil && len(due) > 0
	if checkpointBarrier {
		// The run loop is single-threaded, so selecting every pending table
		// here creates one barrier: no later event is handled until all of
		// these writes finish and the pending offset can be committed.
		due = pending
	}
	sort.Slice(due, func(i, j int) bool {
		return due[i].sourceKey < due[j].sourceKey
	})

	var checkpointOffset *model.SourceOffset
	if checkpointBarrier {
		offsetCopy := *s.pendingOffset
		checkpointOffset = &offsetCopy
	}
	return due, checkpointBarrier, checkpointOffset
}

func (s *Sink) shouldFlushStateLocked(st *tableState, now time.Time) bool {
	if st == nil || len(st.pending) == 0 {
		return false
	}
	flushAfter := s.effectiveFlushAfterLocked()
	if flushAfter <= 0 {
		return true
	}
	if !st.lastEventAt.IsZero() && now.Sub(st.lastEventAt) >= flushAfter {
		return true
	}
	if !st.firstPendingAt.IsZero() && now.Sub(st.firstPendingAt) >= flushAfter {
		return true
	}
	return false
}

func (s *Sink) effectiveFlushAfterLocked() time.Duration {
	flushAfter := time.Duration(s.cfg.FlushSeconds) * time.Second
	checkpointFlushAfter := time.Duration(s.cfg.CheckpointFlushSeconds) * time.Second
	if s.pendingOffset != nil && checkpointFlushAfter > 0 && (flushAfter <= 0 || flushAfter > checkpointFlushAfter) {
		return checkpointFlushAfter
	}
	return flushAfter
}

func (s *Sink) flushAll(ctx context.Context) error {
	s.mu.Lock()
	states := make([]*tableState, 0, len(s.states))
	for _, st := range s.states {
		if len(st.pending) > 0 {
			states = append(states, st)
		}
	}
	s.mu.Unlock()

	for _, st := range states {
		if err := s.flushState(ctx, st); err != nil {
			return err
		}
	}
	return s.commitPendingOffset(ctx, true)
}

func (s *Sink) ApplyEvents(ctx context.Context, events []model.Event, schemas map[string]*model.TableSchema) error {
	for _, schema := range schemas {
		if schema == nil {
			continue
		}
		if err := s.RegisterSourceSchema(schema.SchemaName, schema.TableName, schema); err != nil {
			return util.Permanent(err)
		}
	}

	grouped := make(map[string][]model.Event)
	for _, ev := range events {
		if ev.Type == model.EventTypeDDL {
			return util.Permanent(fmt.Errorf("iceberg correction does not support DDL events for %s.%s", ev.Schema, ev.Table))
		}
		grouped[tableKey(ev.Schema, ev.Table)] = append(grouped[tableKey(ev.Schema, ev.Table)], ev)
	}

	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, sourceKey := range keys {
		state, err := s.ensureOperationalState(ctx, sourceKey)
		if err != nil {
			return err
		}

		startedAt := time.Now()
		var result flushResult
		batch := append([]model.Event(nil), grouped[sourceKey]...)
		if err := util.RetryWithBackoff(ctx, s.retry, func() error {
			if err := s.refreshStateTable(ctx, state); err != nil {
				return err
			}
			var flushErr error
			result, flushErr = s.flushEventsOnce(ctx, state, batch)
			return flushErr
		}); err != nil {
			return err
		}

		completedAt := time.Now()
		duration := completedAt.Sub(startedAt)

		s.mu.Lock()
		state.lastTouchedAt = completedAt
		state.lastFlushAt = completedAt
		state.lastFlushDuration = duration
		state.flushCount++
		flushCount := state.flushCount
		s.mu.Unlock()

		log.Printf(
			"[iceberg][job %s] correction table=%s op=%s events=%d rows=%d deletes=%d duration=%s flush_count=%d",
			s.jobID,
			state.sourceKey,
			result.operation,
			len(batch),
			result.rowCount,
			result.deleteCount,
			duration.Round(time.Millisecond),
			flushCount,
		)
		observability.RecordSinkFlush(s.jobID, state.sourceKey, tableKey(state.targetNamespace, state.targetTable), "correction", result.operation, len(batch), result.rowCount, result.deleteCount, duration)
	}

	return nil
}

func (s *Sink) flushState(ctx context.Context, state *tableState) error {
	s.mu.Lock()
	events := append([]model.Event(nil), state.pending...)
	s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}
	ctx = withIcebergBatch(ctx, cdcIcebergBatch(state.sourceKey, events))

	startedAt := time.Now()
	var result flushResult
	if err := util.RetryWithBackoff(ctx, s.retry, func() error {
		if err := s.refreshStateTable(ctx, state); err != nil {
			return err
		}
		var err error
		result, err = s.flushEventsOnce(ctx, state, events)
		return err
	}); err != nil {
		return err
	}

	completedAt := time.Now()
	duration := completedAt.Sub(startedAt)

	s.mu.Lock()
	if len(state.pending) <= len(events) {
		clearEvents(state.pending)
		state.pending = state.pending[:0]
		state.pendingBytes = 0
		state.firstPendingAt = time.Time{}
	} else {
		clearEvents(state.pending[:len(events)])
		state.pending = append([]model.Event(nil), state.pending[len(events):]...)
		state.pendingBytes = estimateEventsBytes(state.pending)
		state.firstPendingAt = completedAt
	}
	state.lastTouchedAt = completedAt
	state.lastFlushAt = completedAt
	state.lastFlushDuration = duration
	state.flushCount++
	flushCount := state.flushCount
	s.mu.Unlock()

	log.Printf(
		"[iceberg][job %s] flush table=%s op=%s events=%d rows=%d deletes=%d duration=%s flush_count=%d",
		s.jobID,
		state.sourceKey,
		result.operation,
		len(events),
		result.rowCount,
		result.deleteCount,
		duration.Round(time.Millisecond),
		flushCount,
	)
	observability.RecordSinkFlush(s.jobID, state.sourceKey, tableKey(state.targetNamespace, state.targetTable), "stream", result.operation, len(events), result.rowCount, result.deleteCount, duration)

	return s.commitPendingOffset(ctx, true)
}

func clearEvents(events []model.Event) {
	for i := range events {
		events[i] = model.Event{}
	}
}

func clearRows(rows []map[string]interface{}) {
	for i := range rows {
		rows[i] = nil
	}
}

func estimateEventsBytes(events []model.Event) int64 {
	var total int64
	for _, ev := range events {
		total += estimateEventBytes(ev)
	}
	return total
}

func estimateEventBytes(ev model.Event) int64 {
	total := int64(256)
	total += int64(len(ev.Schema) + len(ev.Table) + len(ev.TraceID) + len(ev.DDL))
	total += estimateRowBytes(ev.Data)
	total += estimateRowBytes(ev.OldData)
	for _, row := range ev.Rows {
		total += estimateRowBytes(row)
	}
	return total
}

func estimateRowBytes(row map[string]interface{}) int64 {
	if len(row) == 0 {
		return 0
	}
	total := int64(64 + len(row)*32)
	for key, value := range row {
		total += int64(len(key)) + estimateValueBytes(value)
	}
	return total
}

func estimateValueBytes(value interface{}) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	case time.Time:
		return 24
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return 16
	case fmt.Stringer:
		return int64(len(v.String()))
	default:
		return int64(len(fmt.Sprint(v)))
	}
}

func (s *Sink) flushPriorPendingForTarget(ctx context.Context, current *tableState) error {
	s.mu.Lock()
	states := make([]*tableState, 0)
	for _, state := range s.states {
		if state.sourceKey == current.sourceKey || len(state.pending) == 0 {
			continue
		}
		if sameTarget(state.targetNamespace, current.targetNamespace) && sameTarget(state.targetTable, current.targetTable) {
			states = append(states, state)
		}
	}
	sort.Slice(states, func(i, j int) bool {
		left := states[i].lastEventAt
		right := states[j].lastEventAt
		if !left.Equal(right) {
			if left.IsZero() {
				return false
			}
			if right.IsZero() {
				return true
			}
			return left.Before(right)
		}
		return states[i].sourceKey < states[j].sourceKey
	})
	s.mu.Unlock()

	for _, state := range states {
		if err := s.flushState(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) flushEventsOnce(ctx context.Context, state *tableState, events []model.Event) (flushResult, error) {
	if state.table == nil {
		return flushResult{}, util.Permanent(fmt.Errorf("table handle is nil for %s", state.sourceKey))
	}
	if state.sourceSchema == nil {
		return flushResult{}, util.Permanent(fmt.Errorf("source schema is nil for %s", state.sourceKey))
	}

	pkCols, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}

	batch, err := s.reduceEvents(ctx, state, events, pkCols)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}
	if batch.filter == nil && len(batch.rows) == 0 {
		return flushResult{operation: "noop"}, nil
	}

	return s.flushReducedBatchOnce(ctx, state, batch)
}

func (s *Sink) flushSnapshotRowsOnce(ctx context.Context, state *tableState, rows []map[string]interface{}, ts time.Time, snapshotStartOffset int64) (flushResult, error) {
	if state.table == nil {
		return flushResult{}, util.Permanent(fmt.Errorf("table handle is nil for %s", state.sourceKey))
	}
	if state.sourceSchema == nil {
		return flushResult{}, util.Permanent(fmt.Errorf("source schema is nil for %s", state.sourceKey))
	}

	pkCols, err := s.primaryKeysFor(state.sourceKey, state.sourceSchema)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}

	snapshotMode := s.snapshotWriteModeForTableState(state)
	needsPerRowDeleteFilter := snapshotMode != snapshotWriteModeAppend && snapshotMode != snapshotWriteModeReplaceFilterAppend && snapshotMode != snapshotWriteModeTruncateAppend
	pendingRows := make([]pendingRow, 0, len(rows))
	var deleteKeys []map[string]interface{}
	if needsPerRowDeleteFilter {
		deleteKeys = make([]map[string]interface{}, 0, len(rows))
	}
	for idx, row := range rows {
		key, _, err := keyFromRow(row, pkCols)
		if err != nil {
			return flushResult{}, util.Permanent(err)
		}
		if needsPerRowDeleteFilter {
			deleteKeys = append(deleteKeys, key)
		}
		pendingRows = append(pendingRows, pendingRow{
			key: key,
			row: cloneMap(row),
			event: model.Event{
				Type:      model.EventTypeInsert,
				Schema:    state.sourceSchema.SchemaName,
				Table:     state.sourceSchema.TableName,
				Timestamp: ts,
				Origin:    model.EventOriginSnapshot,
			},
			pos: idx,
		})
	}

	enrichedRows, err := s.enrichPendingRows(ctx, state, pendingRows, pkCols)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}

	var filter iceberglib.BooleanExpression
	if len(deleteKeys) > 0 {
		filter, err = buildKeyFilter(state.table.Schema(), deleteKeys)
		if err != nil {
			return flushResult{}, util.Permanent(err)
		}
	}

	if filter == nil && len(enrichedRows) == 0 {
		return flushResult{operation: "noop"}, nil
	}
	return s.flushSnapshotReducedBatchOnce(ctx, state, &reducedBatch{
		filter:      filter,
		deleteKeys:  deleteKeys,
		pkCols:      append([]string(nil), pkCols...),
		rows:        enrichedRows,
		deleteCount: len(deleteKeys),
	}, snapshotStartOffset)
}

func (s *Sink) flushSnapshotReducedBatchOnce(ctx context.Context, state *tableState, batch *reducedBatch, snapshotStartOffset int64) (flushResult, error) {
	if s.scopedRebuildToken != "" {
		return s.flushScopedSnapshot(ctx, state, batch.rows)
	}
	switch s.snapshotWriteModeForTableState(state) {
	case snapshotWriteModeAppend:
		return s.flushSnapshotAppendOnce(ctx, state, batch.rows)
	case snapshotWriteModeReplaceFilterAppend:
		return s.flushSnapshotReplaceFilterAppendOnce(ctx, state, batch.rows, snapshotStartOffset)
	case snapshotWriteModeTruncateAppend:
		return s.flushSnapshotTruncateAppendOnce(ctx, state, batch.rows, snapshotStartOffset)
	case snapshotWriteModeDeleteAppend:
		return s.flushSnapshotDeleteAppendOnce(ctx, state, batch)
	default:
		return s.flushReducedBatchOnce(ctx, state, batch)
	}
}

func (s *Sink) snapshotWriteModeForState(snapshotAppendSafe bool) string {
	mode := normalizeSnapshotWriteMode(s.cfg.SnapshotWriteMode)
	if mode != snapshotWriteModeAuto {
		return mode
	}
	if snapshotAppendSafe {
		return snapshotWriteModeAppend
	}
	return snapshotWriteModeDeleteAppend
}

func (s *Sink) snapshotWriteModeForTableState(state *tableState) string {
	if state == nil {
		return s.snapshotWriteModeForState(false)
	}
	mode := normalizeSnapshotWriteMode(s.cfg.SnapshotWriteMode)
	if mode != snapshotWriteModeAuto {
		return mode
	}
	if state.snapshotAppendSafe {
		return snapshotWriteModeAppend
	}
	if _, ok := s.snapshotReplaceFilterForState(state); ok {
		return snapshotWriteModeReplaceFilterAppend
	}
	if s.snapshotTruncateForState(state) {
		return snapshotWriteModeTruncateAppend
	}
	return snapshotWriteModeDeleteAppend
}

func (s *Sink) flushSnapshotAppendOnce(ctx context.Context, state *tableState, rows []map[string]interface{}) (flushResult, error) {
	result := flushResult{
		operation: snapshotWriteModeAppend,
		rowCount:  len(rows),
	}
	if len(rows) == 0 {
		result.operation = "noop"
		return result, nil
	}

	reader, release, err := buildRecordReader(state.table.Schema(), rows)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}
	defer release()

	props := s.snapshotProps(state)
	var updated *icetable.Table
	var writeStartedAt time.Time
	var writeCallDuration time.Duration
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		icebergCommit:   true,
		recoveredTable:  &updated,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
		rowCount:        result.rowCount,
		deleteCount:     result.deleteCount,
	}, func(ctx context.Context) error {
		writeStartedAt = time.Now()
		var commitErr error
		updated, commitErr = commitAppend(ctx, state.table, reader, icebergCommitProperties(ctx, props))
		writeCallDuration = time.Since(writeStartedAt)
		return commitErr
	})
	s.logWriteTiming(state, result, err, writeStartedAt, writeCallDuration)
	if err != nil {
		return flushResult{}, s.stateOperationError(result.operation, state, err)
	}
	s.updateStateTableAfterWrite(state, updated)
	return result, nil
}

func (s *Sink) flushSnapshotReplaceFilterAppendOnce(ctx context.Context, state *tableState, rows []map[string]interface{}, snapshotStartOffset int64) (flushResult, error) {
	if err := s.ensureSnapshotReplaceFilterApplied(ctx, state, snapshotStartOffset); err != nil {
		return flushResult{}, err
	}
	return s.flushSnapshotAppendOnce(ctx, state, rows)
}

func (s *Sink) flushSnapshotTruncateAppendOnce(ctx context.Context, state *tableState, rows []map[string]interface{}, snapshotStartOffset int64) (flushResult, error) {
	if err := s.ensureSnapshotTruncateApplied(ctx, state, snapshotStartOffset); err != nil {
		return flushResult{}, err
	}
	return s.flushSnapshotAppendOnce(ctx, state, rows)
}

func (s *Sink) ensureSnapshotReplaceFilterApplied(ctx context.Context, state *tableState, snapshotStartOffset int64) error {
	s.mu.Lock()
	applied := state.snapshotReplaceApplied
	s.mu.Unlock()
	if applied {
		return nil
	}

	if snapshotStartOffset > 0 {
		return util.Permanent(fmt.Errorf("snapshot_write_mode %s cannot resume %s from snapshot offset %d without a confirmed replace-filter delete in this process; clear snapshot progress and restart this snapshot from offset 0",
			snapshotWriteModeReplaceFilterAppend,
			state.sourceKey,
			snapshotStartOffset,
		))
	}

	if s.cfg.SnapshotReplaceDeleteExecutor == snapshotReplaceDeleteExecutorTrino {
		return s.ensureSnapshotReplaceFilterAppliedWithTrino(ctx, state)
	}

	return util.Permanent(fmt.Errorf("snapshot_write_mode %s for %s requires snapshot_replace_delete_executor: trino; native filtered delete is disabled because iceberg-go can panic while evaluating manifest metrics with null partition values",
		snapshotWriteModeReplaceFilterAppend,
		state.sourceKey,
	))
}

func (s *Sink) ensureSnapshotReplaceFilterAppliedWithTrino(ctx context.Context, state *tableState) error {
	query, description, err := s.snapshotReplaceFilterTrinoSQL(state)
	if err != nil {
		return util.Permanent(err)
	}

	result := flushResult{operation: "replace-filter-delete-trino"}
	var startedAt time.Time
	var duration time.Duration
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
	}, func(ctx context.Context) error {
		startedAt = time.Now()
		execErr := s.execTrinoStatement(ctx, query, state.targetNamespace)
		duration = time.Since(startedAt)
		return execErr
	})
	s.logWriteTiming(state, result, err, startedAt, duration)
	if err != nil {
		return s.stateOperationError(result.operation, state, err)
	}
	if err := s.refreshStateTable(ctx, state); err != nil {
		return err
	}
	s.markSnapshotReplaceApplied(state)
	log.Printf("[iceberg][job %s] snapshot replace filter applied table=%s target=%s filter=%s executor=trino",
		s.jobID,
		state.sourceKey,
		tableKey(state.targetNamespace, state.targetTable),
		description,
	)
	return nil
}

type trinoStatementResponse struct {
	ID      string                 `json:"id"`
	InfoURI string                 `json:"infoUri"`
	NextURI string                 `json:"nextUri"`
	Error   *trinoStatementError   `json:"error"`
	Stats   *trinoStatementStats   `json:"stats"`
	Data    []json.RawMessage      `json:"data"`
	Columns []trinoStatementColumn `json:"columns"`
}

type trinoStatementError struct {
	Message   string `json:"message"`
	ErrorName string `json:"errorName"`
	ErrorType string `json:"errorType"`
}

type trinoStatementStats struct {
	State string `json:"state"`
}

type trinoStatementColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (s *Sink) execTrinoStatement(ctx context.Context, query, schema string) error {
	_, err := s.execTrinoStatementTracked(ctx, query, schema, nil)
	return err
}

func (s *Sink) execTrinoStatementTracked(ctx context.Context, query, schema string, accepted func(string) error) ([][]json.RawMessage, error) {
	cfg := s.cfg.TrinoDelete
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.URI), "/") + "/v1/statement"
	client := s.trinoHTTPClient()
	authMode := s.trinoAuthMode()
	user := strings.TrimSpace(cfg.User)
	if user == "" {
		user = "rivus"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(query))
	if err != nil {
		return nil, err
	}
	s.setTrinoHeaders(req, schema)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("trino statement request auth_mode=%s user=%s uri=%s: %w", authMode, user, endpoint, err)
	}
	defer resp.Body.Close()

	statement, err := decodeTrinoStatementResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("trino statement response auth_mode=%s user=%s uri=%s: %w", authMode, user, endpoint, err)
	}
	queryID := statement.ID
	if accepted != nil {
		if queryID == "" {
			return nil, fmt.Errorf("Trino response has no query identity; outcome UNKNOWN")
		}
		if err := accepted(queryID); err != nil {
			return nil, fmt.Errorf("persist Trino query identity before polling: %w", err)
		}
	}
	if err := statementError(statement); err != nil {
		return nil, err
	}
	var data [][]json.RawMessage
	collect := func(statement trinoStatementResponse) error {
		for _, row := range statement.Data {
			var values []json.RawMessage
			if err := json.Unmarshal(row, &values); err != nil {
				return err
			}
			data = append(data, values)
		}
		return nil
	}
	if err := collect(statement); err != nil {
		return nil, err
	}

	nextURI := statement.NextURI
	for nextURI != "" {
		if accepted != nil {
			base, baseErr := url.Parse(endpoint)
			next, nextErr := url.Parse(nextURI)
			if baseErr != nil || nextErr != nil || base.Scheme != next.Scheme || base.Host != next.Host || next.User != nil {
				return nil, fmt.Errorf("Trino polling URI does not match configured coordinator")
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, nextURI, nil)
		if err != nil {
			return nil, err
		}
		s.setTrinoHeaders(req, schema)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("trino statement poll auth_mode=%s user=%s uri=%s: %w", authMode, user, nextURI, err)
		}
		statement, err = decodeTrinoStatementResponse(resp)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("trino statement poll response auth_mode=%s user=%s uri=%s: %w", authMode, user, nextURI, err)
		}
		if accepted != nil && statement.ID != queryID {
			return nil, fmt.Errorf("Trino query identity changed while polling")
		}
		if err := statementError(statement); err != nil {
			return nil, err
		}
		if err := collect(statement); err != nil {
			return nil, err
		}
		nextURI = statement.NextURI
	}

	return data, nil
}

func (s *Sink) trinoAuthMode() string {
	cfg := s.cfg.TrinoDelete
	if strings.TrimSpace(cfg.AccessToken) != "" {
		return "bearer"
	}
	if strings.TrimSpace(cfg.Password) != "" {
		return "basic"
	}
	return "none"
}

func (s *Sink) trinoHTTPClient() *http.Client {
	if !s.cfg.TrinoDelete.TLSInsecureSkipVerify {
		return http.DefaultClient
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func (s *Sink) setTrinoHeaders(req *http.Request, schema string) {
	cfg := s.cfg.TrinoDelete
	user := strings.TrimSpace(cfg.User)
	if user == "" {
		user = "rivus"
	}
	source := strings.TrimSpace(cfg.Source)
	if source == "" {
		source = "rivus"
	}
	catalog := strings.TrimSpace(cfg.Catalog)
	if catalog == "" {
		catalog = strings.TrimSpace(s.cfg.Warehouse)
	}

	req.Header.Set("X-Trino-User", user)
	req.Header.Set("X-Trino-Source", source)
	if catalog != "" {
		req.Header.Set("X-Trino-Catalog", catalog)
	}
	if schema = strings.TrimSpace(schema); schema != "" {
		req.Header.Set("X-Trino-Schema", schema)
	}
	if token := strings.TrimSpace(cfg.AccessToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if password := strings.TrimSpace(cfg.Password); password != "" {
		req.SetBasicAuth(user, password)
	}
}

func decodeTrinoStatementResponse(resp *http.Response) (trinoStatementResponse, error) {
	defer io.Copy(io.Discard, resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return trinoStatementResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return trinoStatementResponse{}, fmt.Errorf("trino statement http status=%d body=%q", resp.StatusCode, truncateForLog(string(body), 500))
	}
	var statement trinoStatementResponse
	if err := json.Unmarshal(body, &statement); err != nil {
		return trinoStatementResponse{}, fmt.Errorf("decode trino statement response: %w body=%q", err, truncateForLog(string(body), 500))
	}
	return statement, nil
}

func statementError(statement trinoStatementResponse) error {
	if statement.Error == nil {
		return nil
	}
	parts := []string{"trino statement failed"}
	if statement.ID != "" {
		parts = append(parts, "id="+statement.ID)
	}
	if statement.Error.ErrorName != "" {
		parts = append(parts, "name="+statement.Error.ErrorName)
	}
	if statement.Error.ErrorType != "" {
		parts = append(parts, "type="+statement.Error.ErrorType)
	}
	if statement.Error.Message != "" {
		parts = append(parts, "message="+statement.Error.Message)
	}
	return fmt.Errorf("%s", strings.Join(parts, " "))
}

func truncateForLog(value string, maxLen int) string {
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return value[:maxLen] + "..."
}

func (s *Sink) logSnapshotReplaceDeleteContext(ctx context.Context, state *tableState, filterDescription string) {
	if state == nil || state.table == nil {
		return
	}

	props := state.table.Properties()
	writeDeleteMode := props.Get(icetable.WriteDeleteModeKey, icetable.WriteDeleteModeDefault)
	metricsProps := selectedProperties(props, "write.metadata.metrics.")
	meta := state.table.Metadata()
	spec := state.table.Spec()

	snapshotID := int64(0)
	manifestCount := 0
	manifestAddedFiles := int64(0)
	manifestExistingFiles := int64(0)
	if snapshot := state.table.CurrentSnapshot(); snapshot != nil {
		snapshotID = snapshot.SnapshotID
		if fs, err := state.table.FS(ctx); err == nil {
			if manifests, err := snapshot.Manifests(fs); err == nil {
				manifestCount = len(manifests)
				for _, manifest := range manifests {
					manifestAddedFiles += int64(manifest.AddedDataFiles())
					manifestExistingFiles += int64(manifest.ExistingDataFiles())
				}
			} else {
				log.Printf("[WARN][iceberg][job %s] replace-filter-delete preflight manifest scan failed table=%s target=%s error=%q",
					s.jobID,
					state.sourceKey,
					tableKey(state.targetNamespace, state.targetTable),
					err,
				)
			}
		} else {
			log.Printf("[WARN][iceberg][job %s] replace-filter-delete preflight fs open failed table=%s target=%s error=%q",
				s.jobID,
				state.sourceKey,
				tableKey(state.targetNamespace, state.targetTable),
				err,
			)
		}
	}

	log.Printf("[iceberg][job %s] replace-filter-delete preflight table=%s target=%s filter=%s format_version=%d write_delete_mode=%s partition_spec_id=%d partition_fields=%d current_snapshot=%d manifests=%d manifest_added_files=%d manifest_existing_files=%d metrics_props=%s",
		s.jobID,
		state.sourceKey,
		tableKey(state.targetNamespace, state.targetTable),
		filterDescription,
		meta.Version(),
		writeDeleteMode,
		spec.ID(),
		spec.NumFields(),
		snapshotID,
		manifestCount,
		manifestAddedFiles,
		manifestExistingFiles,
		formatProperties(metricsProps),
	)
}

func selectedProperties(props iceberglib.Properties, prefix string) iceberglib.Properties {
	selected := iceberglib.Properties{}
	for key, value := range props {
		if strings.HasPrefix(key, prefix) {
			selected[key] = value
		}
	}
	return selected
}

func formatProperties(props iceberglib.Properties) string {
	if len(props) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+props[key])
	}
	return strings.Join(parts, ",")
}

func (s *Sink) ensureSnapshotTruncateApplied(ctx context.Context, state *tableState, snapshotStartOffset int64) error {
	s.mu.Lock()
	applied := state.snapshotTruncateApplied
	s.mu.Unlock()
	if applied {
		return nil
	}

	if snapshotStartOffset > 0 {
		return util.Permanent(fmt.Errorf("snapshot_write_mode %s cannot resume %s from snapshot offset %d without a confirmed truncate in this process; clear snapshot progress and restart this snapshot from offset 0",
			snapshotWriteModeTruncateAppend,
			state.sourceKey,
			snapshotStartOffset,
		))
	}

	if s.cfg.SnapshotReplaceDeleteExecutor == snapshotReplaceDeleteExecutorTrino {
		return s.ensureSnapshotTruncateAppliedWithTrino(ctx, state)
	}

	result := flushResult{operation: "truncate"}
	props := s.snapshotProps(state)
	var updated *icetable.Table
	var writeStartedAt time.Time
	var writeCallDuration time.Duration
	err := s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		icebergCommit:   true,
		recoveredTable:  &updated,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
	}, func(ctx context.Context) error {
		writeStartedAt = time.Now()
		var commitErr error
		updated, commitErr = commitDelete(ctx, state.table, iceberglib.AlwaysTrue{}, icebergCommitProperties(ctx, props), icetable.WithDeleteConcurrency(s.cfg.DeleteConcurrency))
		writeCallDuration = time.Since(writeStartedAt)
		return commitErr
	})
	s.logWriteTiming(state, result, err, writeStartedAt, writeCallDuration)
	if err != nil {
		return s.stateOperationError(result.operation, state, err)
	}
	s.updateStateTableAfterWrite(state, updated)
	s.markSnapshotTruncateApplied(state)
	log.Printf("[iceberg][job %s] snapshot truncate applied table=%s target=%s",
		s.jobID,
		state.sourceKey,
		tableKey(state.targetNamespace, state.targetTable),
	)
	return nil
}

func (s *Sink) ensureSnapshotTruncateAppliedWithTrino(ctx context.Context, state *tableState) error {
	query, err := s.snapshotTruncateTrinoSQL(state)
	if err != nil {
		return util.Permanent(err)
	}

	result := flushResult{operation: "truncate-trino"}
	var startedAt time.Time
	var duration time.Duration
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
	}, func(ctx context.Context) error {
		startedAt = time.Now()
		execErr := s.execTrinoStatement(ctx, query, state.targetNamespace)
		duration = time.Since(startedAt)
		return execErr
	})
	s.logWriteTiming(state, result, err, startedAt, duration)
	if err != nil {
		return s.stateOperationError(result.operation, state, err)
	}
	if err := s.refreshStateTable(ctx, state); err != nil {
		return err
	}
	s.markSnapshotTruncateApplied(state)
	log.Printf("[iceberg][job %s] snapshot truncate applied table=%s target=%s executor=trino",
		s.jobID,
		state.sourceKey,
		tableKey(state.targetNamespace, state.targetTable),
	)
	return nil
}

func (s *Sink) applyKeyDeleteWithTrino(ctx context.Context, state *tableState, batch *reducedBatch, operation string) error {
	if batch == nil || len(batch.deleteKeys) == 0 {
		return nil
	}

	result := flushResult{
		operation:   operation,
		rowCount:    len(batch.rows),
		deleteCount: batch.deleteCount,
	}
	var startedAt time.Time
	var duration time.Duration
	err := s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
		rowCount:        result.rowCount,
		deleteCount:     result.deleteCount,
	}, func(ctx context.Context) error {
		startedAt = time.Now()
		for _, keys := range splitDeleteKeys(batch.deleteKeys, 500) {
			query, err := s.keyDeleteTrinoSQL(state, keys)
			if err != nil {
				duration = time.Since(startedAt)
				return util.Permanent(err)
			}
			if err := s.execTrinoStatement(ctx, query, state.targetNamespace); err != nil {
				duration = time.Since(startedAt)
				return err
			}
		}
		duration = time.Since(startedAt)
		return nil
	})
	s.logWriteTiming(state, result, err, startedAt, duration)
	if err != nil {
		return s.stateOperationError(result.operation, state, err)
	}
	return s.refreshStateTable(ctx, state)
}

func splitDeleteKeys(keys []map[string]interface{}, maxKeys int) [][]map[string]interface{} {
	if len(keys) == 0 {
		return nil
	}
	if maxKeys <= 0 || len(keys) <= maxKeys {
		return [][]map[string]interface{}{keys}
	}
	out := make([][]map[string]interface{}, 0, (len(keys)+maxKeys-1)/maxKeys)
	for start := 0; start < len(keys); start += maxKeys {
		end := start + maxKeys
		if end > len(keys) {
			end = len(keys)
		}
		out = append(out, keys[start:end])
	}
	return out
}

func (s *Sink) markSnapshotReplaceApplied(state *tableState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.snapshotReplaceApplied = true
}

func (s *Sink) markSnapshotTruncateApplied(state *tableState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.snapshotTruncateApplied = true
}

func (s *Sink) flushSnapshotDeleteAppendOnce(ctx context.Context, state *tableState, batch *reducedBatch) (flushResult, error) {
	if batch.filter == nil {
		return s.flushSnapshotAppendOnce(ctx, state, batch.rows)
	}

	result := flushResult{
		operation:   snapshotWriteModeDeleteAppend,
		rowCount:    len(batch.rows),
		deleteCount: batch.deleteCount,
	}
	if len(batch.rows) == 0 && batch.deleteCount == 0 {
		result.operation = "noop"
		return result, nil
	}

	if s.cfg.SnapshotReplaceDeleteExecutor == snapshotReplaceDeleteExecutorTrino {
		if err := s.applyKeyDeleteWithTrino(ctx, state, batch, "snapshot-delete-trino"); err != nil {
			return flushResult{}, err
		}
		if len(batch.rows) == 0 {
			result.operation = "snapshot-delete-trino"
			return result, nil
		}
		appendResult, err := s.flushSnapshotAppendOnce(ctx, state, batch.rows)
		if err != nil {
			return flushResult{}, err
		}
		result.operation = "snapshot-delete-append-trino"
		result.rowCount = appendResult.rowCount
		return result, nil
	}
	if len(batch.deleteKeys) > 0 {
		return flushResult{}, unsupportedNativeKeyDeleteError("snapshot delete-append", state.sourceKey)
	}
	if batch.filter != nil {
		return flushResult{}, util.Permanent(fmt.Errorf("snapshot delete-append for %s built a delete filter without primary-key delete keys; native filtered delete is disabled because iceberg-go can panic while evaluating manifest metrics with null partition values", state.sourceKey))
	}

	reader, release, err := buildRecordReader(state.table.Schema(), batch.rows)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}
	defer release()

	props := s.snapshotProps(state)
	var updated *icetable.Table
	var writeStartedAt time.Time
	var writeCallDuration time.Duration
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       result.operation,
		icebergCommit:   true,
		recoveredTable:  &updated,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
		rowCount:        result.rowCount,
		deleteCount:     result.deleteCount,
	}, func(ctx context.Context) error {
		writeStartedAt = time.Now()
		txn := state.table.NewTransaction()
		if err := txn.Delete(ctx, batch.filter, icebergCommitProperties(ctx, props), icetable.WithDeleteConcurrency(s.cfg.DeleteConcurrency)); err != nil {
			writeCallDuration = time.Since(writeStartedAt)
			return err
		}
		if err := txn.Append(ctx, reader, icebergCommitProperties(ctx, props)); err != nil {
			writeCallDuration = time.Since(writeStartedAt)
			return err
		}
		var commitErr error
		updated, commitErr = txn.Commit(ctx)
		writeCallDuration = time.Since(writeStartedAt)
		return classifyCatalogCommitError(updated, commitErr)
	})
	s.logWriteTiming(state, result, err, writeStartedAt, writeCallDuration)
	if err != nil {
		return flushResult{}, s.stateOperationError(result.operation, state, err)
	}
	s.updateStateTableAfterWrite(state, updated)
	return result, nil
}

func (s *Sink) flushReducedBatchOnce(ctx context.Context, state *tableState, batch *reducedBatch) (flushResult, error) {
	if len(batch.deleteKeys) > 0 {
		switch cdcKeyDeleteExecutor(s.cfg) {
		case snapshotReplaceDeleteExecutorTrino:
			if s.cfg.CDCDeleteExecutor != snapshotReplaceDeleteExecutorTrino {
				log.Printf("[WARN][iceberg][job %s] CDC key delete using trino fallback table=%s target=%s because implicit native equality delete is disabled",
					s.jobID,
					state.sourceKey,
					tableKey(state.targetNamespace, state.targetTable),
				)
			}
			return s.flushReducedBatchWithTrinoDeleteOnce(ctx, state, batch)
		case cdcDeleteExecutorEquality:
			return s.flushReducedBatchWithEqualityDeleteOnce(ctx, state, batch)
		default:
			return flushResult{}, unsupportedNativeKeyDeleteError("CDC", state.sourceKey)
		}
	}
	if s.cfg.CDCDeleteExecutor == snapshotReplaceDeleteExecutorTrino && batch.filter != nil {
		return s.flushReducedBatchWithTrinoDeleteOnce(ctx, state, batch)
	}
	if batch.filter != nil {
		return flushResult{}, util.Permanent(fmt.Errorf("CDC delete for %s built a delete filter without primary-key delete keys; native filtered delete is disabled because iceberg-go can panic while evaluating manifest metrics with null partition values", state.sourceKey))
	}

	props := s.snapshotProps(state)
	var updated *icetable.Table
	var err error
	result := flushResult{
		rowCount:    len(batch.rows),
		deleteCount: batch.deleteCount,
	}
	var writeStartedAt time.Time
	var writeCallDuration time.Duration

	switch {
	case batch.filter == nil:
		reader, release, err := buildRecordReader(state.table.Schema(), batch.rows)
		if err != nil {
			return flushResult{}, util.Permanent(err)
		}
		defer release()

		result.operation = "append"
		err = s.withCommitSlot(ctx, commitProgress{
			operation:       result.operation,
			icebergCommit:   true,
			recoveredTable:  &updated,
			sourceKey:       state.sourceKey,
			targetNamespace: state.targetNamespace,
			targetTable:     state.targetTable,
			rowCount:        result.rowCount,
			deleteCount:     result.deleteCount,
		}, func(ctx context.Context) error {
			writeStartedAt = time.Now()
			var commitErr error
			updated, commitErr = commitAppend(ctx, state.table, reader, icebergCommitProperties(ctx, props))
			writeCallDuration = time.Since(writeStartedAt)
			return commitErr
		})
	case len(batch.rows) == 0:
		result.operation = "delete"
		err = s.withCommitSlot(ctx, commitProgress{
			operation:       result.operation,
			icebergCommit:   true,
			recoveredTable:  &updated,
			sourceKey:       state.sourceKey,
			targetNamespace: state.targetNamespace,
			targetTable:     state.targetTable,
			rowCount:        result.rowCount,
			deleteCount:     result.deleteCount,
		}, func(ctx context.Context) error {
			writeStartedAt = time.Now()
			var commitErr error
			updated, commitErr = commitDelete(ctx, state.table, batch.filter, icebergCommitProperties(ctx, props), icetable.WithDeleteConcurrency(s.cfg.DeleteConcurrency))
			writeCallDuration = time.Since(writeStartedAt)
			return commitErr
		})
	default:
		reader, release, err := buildRecordReader(state.table.Schema(), batch.rows)
		if err != nil {
			return flushResult{}, util.Permanent(err)
		}
		defer release()

		result.operation = "delete-append"
		err = s.withCommitSlot(ctx, commitProgress{
			operation:       result.operation,
			icebergCommit:   true,
			recoveredTable:  &updated,
			sourceKey:       state.sourceKey,
			targetNamespace: state.targetNamespace,
			targetTable:     state.targetTable,
			rowCount:        result.rowCount,
			deleteCount:     result.deleteCount,
		}, func(ctx context.Context) error {
			writeStartedAt = time.Now()
			txn := state.table.NewTransaction()
			if err := txn.Delete(ctx, batch.filter, icebergCommitProperties(ctx, props), icetable.WithDeleteConcurrency(s.cfg.DeleteConcurrency)); err != nil {
				writeCallDuration = time.Since(writeStartedAt)
				return err
			}
			if err := txn.Append(ctx, reader, icebergCommitProperties(ctx, props)); err != nil {
				writeCallDuration = time.Since(writeStartedAt)
				return err
			}
			var commitErr error
			updated, commitErr = txn.Commit(ctx)
			writeCallDuration = time.Since(writeStartedAt)
			return classifyCatalogCommitError(updated, commitErr)
		})
	}
	s.logWriteTiming(state, result, err, writeStartedAt, writeCallDuration)
	if err != nil {
		return flushResult{}, s.stateOperationError(result.operation, state, err)
	}

	s.updateStateTableAfterWrite(state, updated)
	return result, nil
}

func (s *Sink) flushReducedBatchWithTrinoDeleteOnce(ctx context.Context, state *tableState, batch *reducedBatch) (flushResult, error) {
	result := flushResult{
		operation:   "delete-append-trino",
		rowCount:    len(batch.rows),
		deleteCount: batch.deleteCount,
	}
	if len(batch.rows) == 0 {
		result.operation = "delete-trino"
		if err := s.applyKeyDeleteWithTrino(ctx, state, batch, result.operation); err != nil {
			return flushResult{}, err
		}
		return result, nil
	}

	if err := s.applyKeyDeleteWithTrino(ctx, state, batch, "delete-trino"); err != nil {
		return flushResult{}, err
	}

	reader, release, err := buildRecordReader(state.table.Schema(), batch.rows)
	if err != nil {
		return flushResult{}, util.Permanent(err)
	}
	defer release()

	props := s.snapshotProps(state)
	var updated *icetable.Table
	var writeStartedAt time.Time
	var writeCallDuration time.Duration
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       "append-after-delete-trino",
		icebergCommit:   true,
		recoveredTable:  &updated,
		sourceKey:       state.sourceKey,
		targetNamespace: state.targetNamespace,
		targetTable:     state.targetTable,
		rowCount:        result.rowCount,
		deleteCount:     result.deleteCount,
	}, func(ctx context.Context) error {
		writeStartedAt = time.Now()
		var commitErr error
		updated, commitErr = commitAppend(ctx, state.table, reader, icebergCommitProperties(ctx, props))
		writeCallDuration = time.Since(writeStartedAt)
		return commitErr
	})
	appendResult := result
	appendResult.operation = "append-after-delete-trino"
	s.logWriteTiming(state, appendResult, err, writeStartedAt, writeCallDuration)
	if err != nil {
		return flushResult{}, s.stateOperationError(result.operation, state, err)
	}

	s.updateStateTableAfterWrite(state, updated)
	return result, nil
}

func (s *Sink) flushReducedBatchWithEqualityDeleteOnce(ctx context.Context, state *tableState, batch *reducedBatch) (flushResult, error) {
	result := flushResult{
		operation:   "delete-append-equality",
		rowCount:    len(batch.rows),
		deleteCount: batch.deleteCount,
	}
	if len(batch.rows) == 0 {
		result.operation = "delete-equality"
		if err := s.applyEqualityDelta(ctx, state, batch, result.operation); err != nil {
			return flushResult{}, err
		}
		return result, nil
	}

	if err := s.applyEqualityDelta(ctx, state, batch, result.operation); err != nil {
		return flushResult{}, err
	}
	return result, nil
}

func (s *Sink) logWriteTiming(state *tableState, result flushResult, err error, startedAt time.Time, duration time.Duration) {
	if startedAt.IsZero() {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	errDetail := ""
	if err != nil {
		errDetail = fmt.Sprintf(" error=%q", err.Error())
	}
	log.Printf("[iceberg][job %s] write timing table=%s target=%s op=%s rows=%d deletes=%d status=%s write_duration=%s%s",
		s.jobID,
		state.sourceKey,
		tableKey(state.targetNamespace, state.targetTable),
		result.operation,
		result.rowCount,
		result.deleteCount,
		status,
		duration.Round(time.Millisecond),
		errDetail,
	)
	observability.RecordIcebergWrite(s.jobID, state.sourceKey, tableKey(state.targetNamespace, state.targetTable), result.operation, status, result.rowCount, result.deleteCount, duration)
}

func (s *Sink) updateStateTableAfterWrite(state *tableState, updated *icetable.Table) {
	s.recordTableSnapshotMetrics(state, updated)
	if s.maintenanceSignals != nil && state != nil {
		s.maintenanceSignals.observeTable(config.IcebergTarget{Namespace: state.targetNamespace, Table: state.targetTable}, updated)
	}
	if s.maintenance != nil && state != nil {
		s.maintenance.observeTable(config.IcebergTarget{Namespace: state.targetNamespace, Table: state.targetTable}, updated, time.Now())
	}
	s.mu.Lock()
	now := time.Now()
	state.table = updated
	s.updateTargetTableStatesLocked(state.targetNamespace, state.targetTable, updated, now)
	s.mu.Unlock()
}

func (s *Sink) recordTableSnapshotMetrics(state *tableState, tbl *icetable.Table) {
	if state == nil || tbl == nil || tbl.CurrentSnapshot() == nil {
		return
	}
	snapshot := tbl.CurrentSnapshot()
	totalRecords, totalSizeBytes, ok := icebergSnapshotTotals(snapshot)
	if !ok {
		return
	}
	observability.RecordIcebergTableSnapshot(
		s.jobID,
		state.sourceKey,
		tableKey(state.targetNamespace, state.targetTable),
		snapshot.SnapshotID,
		totalRecords,
		totalSizeBytes,
	)
}

func icebergSnapshotTotals(snapshot *icetable.Snapshot) (totalRecords int64, totalSizeBytes int64, ok bool) {
	if snapshot == nil || snapshot.Summary == nil || snapshot.Summary.Properties == nil {
		return 0, 0, false
	}
	records, recordsOK := parseIcebergSummaryInt(snapshot.Summary.Properties["total-records"])
	sizeBytes, sizeOK := parseIcebergSummaryInt(snapshot.Summary.Properties["total-files-size"])
	if !recordsOK && !sizeOK {
		return 0, 0, false
	}
	return records, sizeBytes, true
}

func parseIcebergSummaryInt(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func (s *Sink) reduceEvents(ctx context.Context, state *tableState, events []model.Event, pkCols []string) (*reducedBatch, error) {
	pendingRows, deleteRows, err := reduceEventsDetailed(events, pkCols)
	if err != nil {
		return nil, err
	}

	rows, err := s.enrichPendingRows(ctx, state, pendingRows, pkCols)
	if err != nil {
		return nil, err
	}

	deleteKeys := make([]map[string]interface{}, 0, len(deleteRows))
	for _, row := range deleteRows {
		deleteKeys = append(deleteKeys, row.key)
	}

	var filter iceberglib.BooleanExpression
	if len(deleteKeys) > 0 {
		filter, err = buildKeyFilter(state.table.Schema(), deleteKeys)
		if err != nil {
			return nil, err
		}
	}

	return &reducedBatch{
		filter:      filter,
		deleteKeys:  deleteKeys,
		deleteRows:  deleteRows,
		pkCols:      append([]string(nil), pkCols...),
		rows:        rows,
		deleteCount: len(deleteKeys),
	}, nil
}

func (s *Sink) applyDDL(ctx context.Context, state *tableState, ev model.Event) error {
	ddl := ev.DDL
	if isCreateTableDDL(ddl) && ev.SourceSchema != nil {
		targetNamespace, targetTable := s.ResolveTarget(ev.SourceSchema.SchemaName, ev.SourceSchema.TableName)
		if err := s.EnsureTable(ctx, targetNamespace, targetTable, ev.SourceSchema); err != nil {
			return err
		}
		log.Printf("[iceberg][job %s] create-table schema refreshed source=%s target=%s",
			s.jobID,
			tableKey(ev.SourceSchema.SchemaName, ev.SourceSchema.TableName),
			tableKey(targetNamespace, targetTable),
		)
		return nil
	}

	changes := ev.SchemaChanges
	var err error
	if changes == nil {
		changes, err = schemachange.ParseMySQLDDL(ddl)
		if err != nil {
			return util.Permanent(err)
		}
	}
	plan, skip, err := ddlActionsFromSchemaChanges(changes)
	if err != nil {
		return util.Permanent(err)
	}
	if skip {
		log.Printf("[iceberg][job %s] skip DDL for %s: %s", s.jobID, state.sourceKey, ddl)
		return nil
	}
	if len(plan) == 0 {
		return nil
	}

	desiredSourceSchema := applyDDLToSourceSchema(state.sourceSchema, plan)
	var updated *icetable.Table
	alreadyApplied := false
	err = util.RetryWithBackoff(ctx, s.retry, func() error {
		if err := s.refreshStateTable(ctx, state); err != nil {
			return err
		}
		if ddlActionsAlreadyApplied(state.table.Schema(), plan, s.cfg) {
			updated = state.table
			alreadyApplied = true
			return nil
		}

		txn := state.table.NewTransaction()
		updater := txn.UpdateSchema(false, s.cfg.AllowUnsafeTypeChanges)
		for _, action := range plan {
			if err := applyDDLAction(updater, action, s.cfg); err != nil {
				return util.Permanent(err)
			}
		}
		if err := updater.Commit(); err != nil {
			return util.Permanent(err)
		}
		commitErr := s.withCommitSlot(ctx, commitProgress{
			operation:       "schema",
			icebergCommit:   true,
			recoveredTable:  &updated,
			sourceKey:       state.sourceKey,
			targetNamespace: state.targetNamespace,
			targetTable:     state.targetTable,
		}, func(ctx context.Context) error {
			var err error
			if err = txn.SetProperties(icebergCommitProperties(ctx, nil)); err != nil {
				return err
			}
			updated, err = txn.Commit(ctx)
			return classifyCatalogCommitError(updated, err)
		})
		return commitErr
	})
	if err != nil {
		return s.stateOperationError("apply DDL", state, err)
	}

	s.mu.Lock()
	s.updateTargetTableStatesLocked(state.targetNamespace, state.targetTable, updated, time.Now())
	state.table = updated
	state.sourceSchema = desiredSourceSchema
	s.sourceSchemas[state.sourceKey] = copyTableSchema(state.sourceSchema)
	state.lastTouchedAt = time.Now()
	s.mu.Unlock()

	if alreadyApplied {
		log.Printf("[iceberg][job %s] ddl-already-applied table=%s actions=%d", s.jobID, state.sourceKey, len(plan))
	} else {
		log.Printf("[iceberg][job %s] ddl-applied table=%s actions=%d", s.jobID, state.sourceKey, len(plan))
	}

	return nil
}

func applyDDLAction(updater *icetable.UpdateSchema, action ddlAction, cfg config.IcebergConfig) error {
	switch action.Kind {
	case ddlActionAddColumn:
		typ, err := icebergTypeForColumn(action.Column)
		if err != nil {
			return err
		}
		required := false
		updater.AddColumn([]string{action.Column.Name}, typ, "", required, nil)
		moveIcebergDDLColumn(updater, action)
		return nil
	case ddlActionDropColumn:
		if !cfg.AllowDropColumn {
			return fmt.Errorf("drop column is disabled, column=%s", action.OldName)
		}
		updater.DeleteColumn([]string{action.OldName})
		return nil
	case ddlActionRenameColumn:
		if !cfg.AllowRenameColumn {
			return fmt.Errorf("rename column is disabled, from=%s to=%s", action.OldName, action.NewName)
		}
		updater.RenameColumn([]string{action.OldName}, action.NewName)
		return nil
	case ddlActionUpdateColumn:
		colType, err := icebergTypeForColumn(action.Column)
		if err != nil {
			return err
		}
		update := icetable.ColumnUpdate{
			FieldType: iceberglib.Optional[iceberglib.Type]{Val: colType, Valid: true},
		}
		if cfg.AllowUnsafeTypeChanges {
			update.Required = iceberglib.Optional[bool]{Val: !action.Column.IsNullable, Valid: true}
		}
		updater.UpdateColumn([]string{action.Column.Name}, update)
		moveIcebergDDLColumn(updater, action)
		return nil
	default:
		return fmt.Errorf("unsupported ddl action %q", action.Kind)
	}
}

func moveIcebergDDLColumn(updater *icetable.UpdateSchema, action ddlAction) {
	switch action.Position {
	case model.ColumnPositionFirst:
		updater.MoveFirst([]string{action.Column.Name})
	case model.ColumnPositionAfter:
		if strings.TrimSpace(action.AfterColumn) != "" {
			updater.MoveAfter([]string{action.Column.Name}, []string{action.AfterColumn})
		}
	}
}

func (s *Sink) refreshStateTable(ctx context.Context, state *tableState) error {
	if state.table == nil {
		tbl, err := s.catalog.LoadTable(ctx, namespaceIdentifier(state.targetNamespace, state.targetTable))
		if err != nil {
			return s.stateOperationError("load table", state, err)
		}
		s.mu.Lock()
		now := time.Now()
		state.table = tbl
		s.updateTargetTableStatesLocked(state.targetNamespace, state.targetTable, tbl, now)
		s.mu.Unlock()
		return nil
	}

	if err := state.table.Refresh(ctx); err != nil {
		return s.stateOperationError("refresh table", state, err)
	}
	return nil
}

func (s *Sink) evictIdle(now time.Time) {
	if s.cfg.IdleTableEvictSeconds <= 0 {
		return
	}

	ttl := time.Duration(s.cfg.IdleTableEvictSeconds) * time.Second
	evicted := make([]string, 0)

	s.mu.Lock()
	for _, st := range s.states {
		if st.table == nil || len(st.pending) > 0 || st.snapshotSpool != nil {
			continue
		}

		lastTouched := st.lastTouchedAt
		if lastTouched.IsZero() {
			lastTouched = st.lastEventAt
		}
		if lastTouched.IsZero() || now.Sub(lastTouched) < ttl {
			continue
		}

		st.table = nil
		evicted = append(evicted, st.sourceKey)
	}
	s.mu.Unlock()

	for _, sourceKey := range evicted {
		log.Printf("[iceberg][job %s] evicted idle table handle=%s idle_for=%s", s.jobID, sourceKey, ttl)
	}
}

func (s *Sink) ensureNamespace(ctx context.Context, namespace string) error {
	ident := namespaceOnlyIdentifier(namespace)
	exists, err := s.namespaceExists(ctx, ident)
	if err != nil {
		return s.operationError(fmt.Sprintf("check namespace=%q", namespace), err)
	}
	if exists {
		return nil
	}
	if err := s.catalog.CreateNamespace(ctx, ident, nil); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return s.operationError(fmt.Sprintf("create namespace=%q", namespace), err)
	}
	return nil
}

// namespaceExists works around REST catalogs that reject the Iceberg HEAD
// namespace endpoint with HTTP 400. Gravitino supports the equivalent GET and
// namespace-listing endpoints, so use those only for this compatibility error.
// Other catalog errors must remain visible rather than being mistaken for a
// missing namespace.
func (s *Sink) namespaceExists(ctx context.Context, ident icetable.Identifier) (bool, error) {
	if len(ident) == 0 {
		return false, fmt.Errorf("empty namespace identifier")
	}
	exists, err := s.catalog.CheckNamespaceExists(ctx, ident)
	if err == nil {
		return exists, nil
	}
	if !errors.Is(err, icerest.ErrBadRequest) {
		return false, err
	}

	if _, loadErr := s.catalog.LoadNamespaceProperties(ctx, ident); loadErr == nil {
		return true, nil
	} else if errors.Is(loadErr, icecatalog.ErrNoSuchNamespace) {
		return false, nil
	} else if !errors.Is(loadErr, icerest.ErrBadRequest) {
		return false, fmt.Errorf("HEAD namespace check failed: %v; GET namespace fallback failed: %w", err, loadErr)
	}

	parent := ident[:len(ident)-1]
	namespaces, listErr := s.catalog.ListNamespaces(ctx, parent)
	if listErr != nil {
		if errors.Is(listErr, icecatalog.ErrNoSuchNamespace) {
			return false, nil
		}
		return false, fmt.Errorf("HEAD namespace check failed: %v; namespace listing fallback failed: %w", err, listErr)
	}
	for _, candidate := range namespaces {
		if slices.Equal(candidate, ident) {
			return true, nil
		}
	}
	return false, nil
}

func (s *Sink) loadOrCreateTable(ctx context.Context, namespace, tableName string, schema *model.TableSchema, pkCols []string) (*icetable.Table, bool, error) {
	ident := namespaceIdentifier(namespace, tableName)
	tbl, err := s.catalog.LoadTable(ctx, ident)
	if err == nil {
		return tbl, false, nil
	}

	exists, existsErr := s.catalog.CheckTableExists(ctx, ident)
	if existsErr == nil && exists {
		tbl, err = s.catalog.LoadTable(ctx, ident)
		if err != nil {
			return nil, false, s.operationError(fmt.Sprintf("load table target=%q", tableKey(namespace, tableName)), err)
		}
		return tbl, false, nil
	}

	sc, err := buildIcebergSchema(schema, pkCols)
	if err != nil {
		return nil, false, util.Permanent(err)
	}

	var created *icetable.Table
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       "create_table",
		icebergCommit:   true,
		recoveredTable:  &created,
		targetNamespace: namespace,
		targetTable:     tableName,
	}, func(ctx context.Context) error {
		var commitErr error
		created, commitErr = s.catalog.CreateTable(ctx, ident, sc, icecatalog.WithProperties(icebergCommitProperties(ctx, s.defaultTableProperties())))
		return commitErr
	})
	if err != nil {
		return nil, false, s.operationError(fmt.Sprintf("create table target=%q", tableKey(namespace, tableName)), err)
	}
	return created, true, nil
}

func (s *Sink) syncTableSchema(ctx context.Context, tbl *icetable.Table, targetNamespace, targetTable string, sourceSchema *model.TableSchema, pkCols []string) (*icetable.Table, error) {
	txn := tbl.NewTransaction()
	updater := txn.UpdateSchema(false, s.cfg.AllowUnsafeTypeChanges)

	changed, err := syncSchema(updater, tbl.Schema(), sourceSchema, pkCols, s.cfg)
	if err != nil {
		return nil, util.Permanent(err)
	}
	if !changed {
		return tbl, nil
	}

	if err := updater.Commit(); err != nil {
		return nil, err
	}
	var updated *icetable.Table
	err = s.withCommitSlot(ctx, commitProgress{
		operation:       "schema",
		icebergCommit:   true,
		recoveredTable:  &updated,
		sourceKey:       tableKey(sourceSchema.SchemaName, sourceSchema.TableName),
		targetNamespace: targetNamespace,
		targetTable:     targetTable,
	}, func(ctx context.Context) error {
		var commitErr error
		if commitErr = txn.SetProperties(icebergCommitProperties(ctx, nil)); commitErr != nil {
			return commitErr
		}
		updated, commitErr = txn.Commit(ctx)
		return classifyCatalogCommitError(updated, commitErr)
	})
	if err != nil {
		return nil, s.operationError("sync table schema", err)
	}
	return updated, nil
}

func (s *Sink) defaultTableProperties() iceberglib.Properties {
	props := iceberglib.Properties{}
	for key, value := range s.cfg.TableProperties {
		props[key] = value
	}
	if enabled, versions := metadataCleanupPolicyConfig(s.cfg.TableMaintenance); enabled && (s.cfg.TableMaintenance.Enabled || s.cfg.TableMaintenance.NativeEnabled) && versions > 0 {
		for key, value := range metadataCleanupProperties(versions) {
			props[key] = value
		}
	}

	if _, ok := props[icetable.PropertyFormatVersion]; !ok {
		props[icetable.PropertyFormatVersion] = "2"
	}
	if _, ok := props[icetable.WriteDeleteModeKey]; !ok {
		props[icetable.WriteDeleteModeKey] = icetable.WriteModeMergeOnRead
	}
	if _, ok := props["write.update.mode"]; !ok {
		props["write.update.mode"] = icetable.WriteModeMergeOnRead
	}
	if _, ok := props["write.merge.mode"]; !ok {
		props["write.merge.mode"] = icetable.WriteModeMergeOnRead
	}
	return props
}

func (s *Sink) snapshotProps(state *tableState) iceberglib.Properties {
	props := iceberglib.Properties{
		"rivus.job_id":       s.jobID,
		"rivus.source_table": state.sourceKey,
	}
	if s.jobName != "" {
		props["rivus.job_name"] = s.jobName
	}
	return props
}

func (s *Sink) snapshotPropsForTarget(targetNamespace, targetTable string) iceberglib.Properties {
	props := iceberglib.Properties{
		"rivus.job_id":       s.jobID,
		"rivus.target_table": tableKey(targetNamespace, targetTable),
	}
	if s.jobName != "" {
		props["rivus.job_name"] = s.jobName
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, state := range s.states {
		if sameTarget(state.targetNamespace, targetNamespace) && sameTarget(state.targetTable, targetTable) {
			props["rivus.source_table"] = state.sourceKey
			return props
		}
	}
	return props
}

func countIcebergTableRows(ctx context.Context, tbl *icetable.Table) (int64, error) {
	if tbl == nil || tbl.CurrentSnapshot() == nil {
		return 0, nil
	}

	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return 0, err
	}

	var total int64
	deleted := []icetable.FileScanTask{}
	for _, task := range tasks {
		if task.File == nil {
			continue
		}
		if len(task.DeleteFiles) > 0 || len(task.EqualityDeleteFiles) > 0 {
			deleted = append(deleted, task)
		} else {
			total += task.File.Count()
		}
	}
	if len(deleted) > 0 {
		_, records, err := tbl.Scan().ReadTasks(ctx, deleted)
		if err != nil {
			return 0, err
		}
		for record, err := range records {
			if err != nil {
				return 0, err
			}
			total += record.NumRows()
			record.Release()
		}
	}

	return total, nil
}

func sameTarget(left, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
}

func (s *Sink) updateTargetTableStatesLocked(targetNamespace, targetTable string, tbl *icetable.Table, touchedAt time.Time) {
	for _, state := range s.states {
		if sameTarget(state.targetNamespace, targetNamespace) && sameTarget(state.targetTable, targetTable) {
			state.table = tbl
			state.lastTouchedAt = touchedAt
		}
	}
}

func matchSourceOverrideKey(patternKey, srcSchema, srcTable string) bool {
	schemaPattern, tablePattern, ok := splitOverrideKey(patternKey)
	if !ok {
		return matchGlob(patternKey, srcTable)
	}
	return matchGlob(schemaPattern, srcSchema) && matchGlob(tablePattern, srcTable)
}

func splitOverrideKey(key string) (string, string, bool) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(key)), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	schemaPattern := strings.TrimSpace(parts[0])
	tablePattern := strings.TrimSpace(parts[1])
	if schemaPattern == "" || tablePattern == "" {
		return "", "", false
	}
	return schemaPattern, tablePattern, true
}

func matchGlob(pattern, value string) bool {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	value = strings.ToLower(strings.TrimSpace(value))
	if pattern == "" || pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == value
	}

	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == value
	}
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(value[pos:], part)
		if idx < 0 {
			return false
		}
		if i == 0 && idx != 0 {
			return false
		}
		pos += idx + len(part)
	}
	last := parts[len(parts)-1]
	return last == "" || strings.HasSuffix(value, last)
}

func globSpecificity(pattern string) int {
	score := 0
	for _, r := range pattern {
		if r != '*' {
			score++
		}
	}
	return score
}

func (s *Sink) primaryKeysFor(sourceKey string, schema *model.TableSchema) ([]string, error) {
	if configured, ok := s.cfg.PrimaryKeys[sourceKey]; ok && len(configured) > 0 {
		return append([]string(nil), configured...), nil
	}

	keys := make([]string, 0)
	for _, col := range schema.Columns {
		if col.IsPK {
			keys = append(keys, col.Name)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("iceberg_native requires primary keys for %s", sourceKey)
	}
	return keys, nil
}

func (s *Sink) ensureOperationalState(ctx context.Context, sourceKey string) (*tableState, error) {
	s.mu.Lock()
	state := s.stateForKeyLocked(sourceKey)
	sourceSchema := copyTableSchema(state.sourceSchema)
	s.mu.Unlock()

	if state.table != nil && sourceSchema != nil {
		return state, nil
	}
	if sourceSchema == nil {
		return nil, util.Permanent(fmt.Errorf("missing source schema for %s", sourceKey))
	}

	targetNamespace, targetTable := s.ResolveTarget(sourceSchema.SchemaName, sourceSchema.TableName)
	if err := s.EnsureTable(ctx, targetNamespace, targetTable, sourceSchema); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateForKeyLocked(sourceKey), nil
}

func (s *Sink) ensureState(sourceKey string) *tableState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateForKeyLocked(sourceKey)
}

func (s *Sink) stateForKeyLocked(sourceKey string) *tableState {
	if st, ok := s.states[sourceKey]; ok {
		return st
	}
	st := &tableState{sourceKey: sourceKey}
	s.states[sourceKey] = st
	return st
}

func namespaceOnlyIdentifier(namespace string) []string {
	parts := strings.Split(strings.TrimSpace(namespace), ".")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func namespaceIdentifier(namespace, table string) []string {
	out := namespaceOnlyIdentifier(namespace)
	out = append(out, strings.TrimSpace(table))
	return out
}

func tableKey(schema, table string) string {
	return strings.ToLower(strings.TrimSpace(schema) + "." + strings.TrimSpace(table))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
