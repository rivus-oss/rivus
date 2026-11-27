package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"reflect"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/observability"
	"github.com/gerinsp/rivus/pkg/schemachange"
	"github.com/gerinsp/rivus/pkg/util"
)

type Source struct {
	jobID    string
	stateKey string

	cfg       config.MySQLConfig
	retry     config.RetryPolicy
	db        *sql.DB
	offsetSto meta.OffsetStore
	progress  connector.ProgressReporter

	allowedTables          map[string]bool // key lower "db.table"
	skipSnapshotTables     map[string]bool
	snapshotBatchEvents    bool
	rollingSnapshotBatches bool
	scopedRebuildToken     string
}

func NewSource(jobID, stateKey string, cfg config.MySQLConfig, retry config.RetryPolicy, offsetSto meta.OffsetStore, progress connector.ProgressReporter) (*Source, error) {
	for table, tableCfg := range cfg.TableConfigs {
		if err := config.ValidateMySQLSnapshotCutoff(tableCfg); err != nil {
			return nil, fmt.Errorf("table %s: %w", table, err)
		}
	}
	log.Printf("[mysql][SIGNATURE] NEW BUILD %s", time.Now().Format(time.RFC3339Nano))

	// Note: DSN butuh 1 database. Untuk multi-db, kita pakai cfg.Database (legacy) kalau ada,
	// kalau kosong, pakai "information_schema" supaya tetap bisa connect.
	dbForDSN := cfg.Database
	if strings.TrimSpace(dbForDSN) == "" {
		dbForDSN = "information_schema"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&charset=utf8mb4,utf8&interpolateParams=true&timeout=10s&readTimeout=5m&writeTimeout=5m",
		cfg.User, cfg.Password, cfg.Addr, dbForDSN)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetConnMaxLifetime(20 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("mysql ping failed: %w", err)
	}

	resolveCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	expandedTables, err := expandConfiguredTables(resolveCtx, cfg.Tables, func(ctx context.Context, dbName string) ([]string, error) {
		return listBaseTablesInSchema(ctx, db, dbName)
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	var skippedObjects []string
	cfg.Tables, skippedObjects, err = filterConfiguredTablesToBaseTables(resolveCtx, expandedTables, func(ctx context.Context, dbName, tableName string) (string, error) {
		return lookupTableType(ctx, db, dbName, tableName)
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	for _, name := range skippedObjects {
		log.Printf("[mysql][job %s] skip non-base object from mysql.tables: %s", jobID, name)
	}

	allowed := make(map[string]bool, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tt := strings.ToLower(strings.TrimSpace(t))
		if tt == "" {
			continue
		}
		allowed[tt] = true
		observability.RegisterSourceTable(jobID, tt)
	}

	return &Source{
		jobID:         jobID,
		stateKey:      stateKey,
		cfg:           cfg,
		retry:         retry,
		db:            db,
		offsetSto:     offsetSto,
		progress:      progress,
		allowedTables: allowed,
	}, nil
}

func (s *Source) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Source) UseSnapshotBatchEvents(enabled bool) {
	s.snapshotBatchEvents = enabled
}

func (s *Source) UseRollingSnapshotBatches(enabled bool) {
	s.rollingSnapshotBatches = enabled
}

func (s *Source) SetSinkType(sinkType string) {
	for table := range s.allowedTables {
		observability.SetSinkType(s.jobID, table, sinkType)
	}
}

func (s *Source) checkpointKey() string {
	if strings.TrimSpace(s.stateKey) != "" {
		return s.stateKey
	}
	return s.jobID
}

func (s *Source) resumeOffset(ctx context.Context) (*meta.Offset, error) {
	key := s.checkpointKey()
	off, err := s.offsetSto.GetOffset(ctx, key)
	if err != nil || off != nil || key == s.jobID {
		return off, err
	}

	legacy, err := s.offsetSto.GetOffset(ctx, s.jobID)
	if err != nil || legacy == nil {
		return legacy, err
	}
	if err := s.offsetSto.SaveOffset(ctx, key, *legacy); err != nil {
		return nil, fmt.Errorf("migrate legacy checkpoint failed: %w", err)
	}
	if err := s.offsetSto.DeleteJobState(ctx, s.jobID); err != nil {
		return nil, fmt.Errorf("cleanup legacy checkpoint failed: %w", err)
	}
	log.Printf("[mysql][job %s] migrated legacy CDC checkpoint to internal state key", s.jobID)
	return legacy, nil
}

func (s *Source) clearLegacyCheckpoint(ctx context.Context) error {
	if s.offsetSto == nil || s.checkpointKey() == s.jobID {
		return nil
	}
	if err := s.offsetSto.DeleteJobState(ctx, s.jobID); err != nil {
		return fmt.Errorf("cleanup legacy checkpoint failed: %w", err)
	}
	return nil
}

func (s *Source) resetInitialCheckpoint(ctx context.Context) error {
	if s.offsetSto == nil {
		return nil
	}
	if s.scopedRebuildToken != "" {
		if err := meta.CheckScopedRebuildEmptyCheckpoint(ctx, s.offsetSto, s.jobID, s.checkpointKey()); err != nil {
			return err
		}
		return meta.CheckScopedRebuildIntents(ctx, s.offsetSto, s.jobID, s.checkpointKey(), s.scopedRebuildToken, false)
	}
	if err := s.clearLegacyCheckpoint(ctx); err != nil {
		return err
	}
	if err := s.offsetSto.DeleteJobState(ctx, s.checkpointKey()); err != nil {
		return fmt.Errorf("reset initial checkpoint failed: %w", err)
	}
	return nil
}

func (s *Source) reportProgress(info connector.ProgressInfo) {
	if s.progress == nil {
		return
	}
	s.progress(info)
}

func (s *Source) reportSnapshotProgress(tableName string, tableIndex, completedTables, totalTables int, rows int64, resumed bool) {
	action := "Loading snapshot"
	if resumed {
		action = "Resuming snapshot"
	}

	summary := action
	if totalTables > 0 && tableIndex > 0 {
		summary = fmt.Sprintf("%s table %d/%d", action, tableIndex, totalTables)
	}

	s.reportProgress(connector.ProgressInfo{
		Phase:             "snapshot",
		Summary:           summary,
		Detail:            fmt.Sprintf("%s | %d rows emitted", tableName, rows),
		CurrentTable:      tableName,
		CurrentTableIndex: tableIndex,
		CompletedTables:   completedTables,
		TotalTables:       totalTables,
		CurrentTableRows:  rows,
	})
}

func (s *Source) emitSnapshotEvent(ctx context.Context, out chan<- model.Event, ev model.Event, tableName string, tableIndex, totalTables int, rowsEmitted int64) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case out <- ev:
		return nil
	case <-timer.C:
		log.Printf("[WARN][mysql][job %s] snapshot backpressure table=%s rows_emitted=%d waiting_for_sink=true", s.jobID, tableName, rowsEmitted)
		s.reportProgress(connector.ProgressInfo{
			Phase:             "snapshot",
			Summary:           fmt.Sprintf("Waiting for sink flush on table %d/%d", tableIndex, totalTables),
			Detail:            fmt.Sprintf("%s | %d rows emitted, sink is slower than snapshot reader", tableName, rowsEmitted),
			CurrentTable:      tableName,
			CurrentTableIndex: tableIndex,
			CompletedTables:   tableIndex - 1,
			TotalTables:       totalTables,
			CurrentTableRows:  rowsEmitted,
		})
		select {
		case out <- ev:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) emitSnapshotBatch(ctx context.Context, out chan<- model.Event, ev model.Event, tableName string, tableIndex, totalTables int, rowsEmitted int64, keepAlive func(context.Context) error) error {
	ack := make(chan error, 1)
	ev.Ack = ack

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case out <- ev:
	case <-timer.C:
		log.Printf("[WARN][mysql][job %s] snapshot batch backpressure table=%s rows_emitted=%d rows=%d waiting_for_sink=true", s.jobID, tableName, rowsEmitted, len(ev.Rows))
		s.reportProgress(connector.ProgressInfo{
			Phase:             "snapshot",
			Summary:           fmt.Sprintf("Waiting for sink flush on table %d/%d", tableIndex, totalTables),
			Detail:            fmt.Sprintf("%s | %d rows emitted, sink is slower than snapshot reader", tableName, rowsEmitted),
			CurrentTable:      tableName,
			CurrentTableIndex: tableIndex,
			CompletedTables:   tableIndex - 1,
			TotalTables:       totalTables,
			CurrentTableRows:  rowsEmitted,
		})
		select {
		case out <- ev:
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	warnTimer := time.NewTimer(30 * time.Second)
	defer warnTimer.Stop()
	warnC := warnTimer.C

	var keepAliveTicker *time.Ticker
	var keepAliveC <-chan time.Time
	if keepAlive != nil {
		keepAliveTicker = time.NewTicker(60 * time.Second)
		keepAliveC = keepAliveTicker.C
		defer keepAliveTicker.Stop()
	}

	for {
		select {
		case err, ok := <-ack:
			if !ok {
				return fmt.Errorf("snapshot batch ACK closed without result")
			}
			return err
		case <-warnC:
			warnC = nil
			log.Printf("[WARN][mysql][job %s] snapshot batch flush waiting table=%s rows_emitted=%d rows=%d", s.jobID, tableName, rowsEmitted, len(ev.Rows))
			s.reportProgress(connector.ProgressInfo{
				Phase:             "snapshot",
				Summary:           fmt.Sprintf("Waiting for sink batch acknowledgement on table %d/%d", tableIndex, totalTables),
				Detail:            fmt.Sprintf("%s | %d rows emitted, waiting for sink acknowledgement", tableName, rowsEmitted),
				CurrentTable:      tableName,
				CurrentTableIndex: tableIndex,
				CompletedTables:   tableIndex - 1,
				TotalTables:       totalTables,
				CurrentTableRows:  rowsEmitted,
			})
		case <-keepAliveC:
			keepAliveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := keepAlive(keepAliveCtx)
			cancel()
			if err != nil {
				log.Printf("[WARN][mysql][job %s] snapshot keepalive failed table=%s rows_emitted=%d: %v", s.jobID, tableName, rowsEmitted, err)
			} else {
				log.Printf("[mysql][job %s] snapshot keepalive ok table=%s rows_emitted=%d", s.jobID, tableName, rowsEmitted)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Source) emitSnapshotBarrier(ctx context.Context, out chan<- model.Event, eventType model.EventType) error {
	ack := make(chan error, 1)
	ev := model.Event{
		Type:      eventType,
		Origin:    model.EventOriginSnapshot,
		Timestamp: time.Now(),
		Ack:       ack,
	}

	select {
	case out <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err, ok := <-ack:
		if !ok {
			return fmt.Errorf("snapshot barrier ACK closed without result")
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Source) emitSnapshotTableComplete(ctx context.Context, out chan<- model.Event, dbName, tableName string, rowsEmitted int64) error {
	ack := make(chan error, 1)
	ev := model.Event{
		Type:                model.EventTypeSnapshotTableEnd,
		Schema:              dbName,
		Table:               tableName,
		Origin:              model.EventOriginSnapshot,
		Timestamp:           time.Now(),
		SnapshotStartOffset: rowsEmitted,
		SnapshotRolling:     true,
		Ack:                 ack,
	}

	select {
	case out <- ev:
	case <-ctx.Done():
		return ctx.Err()
	}

	warnTimer := time.NewTimer(30 * time.Second)
	defer warnTimer.Stop()
	for {
		select {
		case err, ok := <-ack:
			if !ok {
				return fmt.Errorf("snapshot table ACK closed without result")
			}
			return err
		case <-warnTimer.C:
			log.Printf("[WARN][mysql][job %s] waiting for rolled snapshot commit table=%s.%s rows=%d", s.jobID, dbName, tableName, rowsEmitted)
			warnTimer.Reset(30 * time.Second)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Completion is recorded by the sink after its data barrier, before ACK. This
// handshake can be repeated after a crash without replaying a completed load.
func (s *Source) completeSnapshot(ctx context.Context, out chan<- model.Event) error {
	if err := s.emitSnapshotBarrier(ctx, out, model.EventTypeSnapshotComplete); err != nil {
		return fmt.Errorf("complete snapshot barrier failed: %w", err)
	}
	if s.offsetSto != nil {
		st, err := s.offsetSto.GetSnapshotState(ctx, s.checkpointKey())
		if err != nil {
			return fmt.Errorf("read snapshot completion failed: %w", err)
		}
		if !st.CompletionConfirmed() {
			return fmt.Errorf("snapshot completion BLOCKED: ACK has no confirmed durable completion")
		}
		if err := s.offsetSto.ClearSnapshotProgress(ctx, s.checkpointKey()); err != nil {
			return fmt.Errorf("clear completed snapshot progress failed: %w", err)
		}
	}
	return nil
}

func clearSnapshotRows(rows []map[string]interface{}) {
	for i := range rows {
		rows[i] = nil
	}
}

func (s *Source) SkipSnapshotTables(tables []connector.TableRef) {
	if len(tables) == 0 {
		s.skipSnapshotTables = nil
		return
	}
	s.skipSnapshotTables = make(map[string]bool, len(tables))
	for _, table := range tables {
		key := strings.ToLower(strings.TrimSpace(table.Schema + "." + table.Table))
		if key != "." && key != "" {
			s.skipSnapshotTables[key] = true
		}
	}
}

func (s *Source) shouldSkipSnapshotTable(fullName string) bool {
	if len(s.skipSnapshotTables) == 0 {
		return false
	}
	return s.skipSnapshotTables[strings.ToLower(strings.TrimSpace(fullName))]
}

func (s *Source) tableRefs() []connector.TableRef {
	out := make([]connector.TableRef, 0, len(s.cfg.Tables))
	for _, full := range s.cfg.Tables {
		db, tbl, ok := splitDBTable(full)
		if !ok {
			continue
		}
		out = append(out, connector.TableRef{Schema: db, Table: tbl})
	}
	return out
}

func (s *Source) reportStreamingProgress(startFrom *gomysql.Position) {
	summary := "CDC streaming from latest"
	detail := fmt.Sprintf("Listening for changes across %d table(s)", len(s.cfg.Tables))
	info := connector.ProgressInfo{
		Phase:           "streaming",
		Summary:         summary,
		Detail:          detail,
		CompletedTables: len(s.cfg.Tables),
		TotalTables:     len(s.cfg.Tables),
	}
	if startFrom != nil {
		summary = "CDC streaming"
		detail = fmt.Sprintf("CDC started from %s:%d across %d table(s)", startFrom.Name, startFrom.Pos, len(s.cfg.Tables))
		info.Summary = summary
		info.Detail = detail
		info.CDCStartFile = startFrom.Name
		info.CDCStartPos = startFrom.Pos
		info.CDCCurrentFile = startFrom.Name
		info.CDCCurrentPos = startFrom.Pos
	}

	s.reportProgress(info)
}

// ---------------- CDC handler ----------------

type cdcHandler struct {
	canal.DummyEventHandler

	jobID             string
	allowed           map[string]bool // key lower "db.table"
	currentBinlogFile string
	startBinlogFile   string
	startBinlogPos    uint32
	totalTables       int
	progress          connector.ProgressReporter
	lastProgressAt    time.Time
	lastProgressFile  string
	lastProgressPos   uint32
	lastCheckpointLog time.Time
	schemaFetcher     func(context.Context, string, string) (*model.TableSchema, error)

	out chan<- model.Event
	ctx context.Context
}

func (h *cdcHandler) String() string { return "gosync-cdc-handler" }

func (h *cdcHandler) reportLivePosition(file string, pos uint32, force bool) {
	if h.progress == nil || strings.TrimSpace(file) == "" || pos == 0 {
		return
	}
	now := time.Now()
	if !force && h.lastProgressFile == file && h.lastProgressPos == pos {
		return
	}
	if !force && h.lastProgressFile == file && now.Sub(h.lastProgressAt) < 2*time.Second {
		return
	}
	h.lastProgressAt = now
	h.lastProgressFile = file
	h.lastProgressPos = pos

	detail := fmt.Sprintf("CDC current %s:%d", file, pos)
	if strings.TrimSpace(h.startBinlogFile) != "" && h.startBinlogPos > 0 {
		detail = fmt.Sprintf("CDC current %s:%d; started from %s:%d across %d table(s)", file, pos, h.startBinlogFile, h.startBinlogPos, h.totalTables)
	} else if h.totalTables > 0 {
		detail = fmt.Sprintf("CDC current %s:%d across %d table(s)", file, pos, h.totalTables)
	}

	h.progress(connector.ProgressInfo{
		Phase:           "streaming",
		Summary:         "CDC streaming",
		Detail:          detail,
		CompletedTables: h.totalTables,
		TotalTables:     h.totalTables,
		CDCStartFile:    h.startBinlogFile,
		CDCStartPos:     h.startBinlogPos,
		CDCCurrentFile:  file,
		CDCCurrentPos:   pos,
	})
}

func (h *cdcHandler) emit(ev model.Event) error {
	if h.ctx != nil {
		if err := h.ctx.Err(); err != nil {
			return err
		}
	}

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case h.out <- ev:
		return nil

	case <-timer.C:
		log.Printf("[WARN][job %s] backpressure: sink slow, waiting...", h.jobID)
		file := h.currentBinlogFile
		pos := uint32(0)
		if ev.SourceOffset != nil {
			if strings.TrimSpace(ev.SourceOffset.BinlogFile) != "" {
				file = ev.SourceOffset.BinlogFile
			}
			pos = ev.SourceOffset.BinlogPos
		}
		h.reportBackpressure(file, pos, "Event buffer remained full for 10 seconds; sink throughput is below source throughput")
		select {
		case h.out <- ev:
			h.reportLivePosition(file, pos, true)
			return nil
		case <-h.ctx.Done():
			return h.ctx.Err()
		}

	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}

func (h *cdcHandler) reportBackpressure(file string, pos uint32, detail string) {
	if h.progress == nil {
		return
	}
	h.progress(connector.ProgressInfo{
		Phase:           "streaming",
		Summary:         "Waiting for sink flush",
		Detail:          detail,
		CompletedTables: h.totalTables,
		TotalTables:     h.totalTables,
		CDCStartFile:    h.startBinlogFile,
		CDCStartPos:     h.startBinlogPos,
		CDCCurrentFile:  file,
		CDCCurrentPos:   pos,
	})
}

func affectedRowsCount(e *canal.RowsEvent) int {
	if e == nil {
		return 0
	}
	if e.Action == canal.UpdateAction {
		return len(e.Rows) / 2
	}
	return len(e.Rows)
}

func summarizeRowsEventKeys(e *canal.RowsEvent, maxSamples int) string {
	if e == nil || e.Table == nil || len(e.Rows) == 0 || maxSamples <= 0 {
		return "-"
	}

	keyCols := e.Table.PKColumns
	if len(keyCols) == 0 && len(e.Table.Columns) > 0 {
		keyCols = []int{0}
	}
	if len(keyCols) == 0 {
		return "-"
	}

	samples := make([]string, 0, maxSamples)
	addSample := func(row []interface{}) {
		if len(samples) >= maxSamples {
			return
		}
		parts := make([]string, 0, len(keyCols))
		for _, idx := range keyCols {
			if idx < 0 || idx >= len(e.Table.Columns) || idx >= len(row) {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s=%s", e.Table.Columns[idx].Name, logValue(row[idx])))
		}
		if len(parts) > 0 {
			samples = append(samples, strings.Join(parts, ","))
		}
	}

	switch e.Action {
	case canal.UpdateAction:
		for i := 1; i < len(e.Rows); i += 2 {
			addSample(e.Rows[i])
		}
	default:
		for _, row := range e.Rows {
			addSample(row)
		}
	}

	if len(samples) == 0 {
		return "-"
	}
	out := strings.Join(samples, ";")
	if affectedRowsCount(e) > len(samples) {
		out += ";..."
	}
	return out
}

func logValue(v interface{}) string {
	s := strings.ReplaceAll(fmt.Sprint(v), "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func eventLogPos(e *canal.RowsEvent) uint32 {
	if e == nil || e.Header == nil {
		return 0
	}
	return e.Header.LogPos
}

func eventTimestamp(e *canal.RowsEvent) time.Time {
	if e != nil && e.Header != nil && e.Header.Timestamp > 0 {
		return time.Unix(int64(e.Header.Timestamp), 0).UTC()
	}
	return time.Now()
}

func (h *cdcHandler) sourceOffsetFor(e *canal.RowsEvent) *model.SourceOffset {
	pos := eventLogPos(e)
	if pos == 0 {
		return nil
	}
	h.reportLivePosition(h.currentBinlogFile, pos, false)
	return &model.SourceOffset{
		BinlogFile: h.currentBinlogFile,
		BinlogPos:  pos,
	}
}

func (h *cdcHandler) traceIDFor(e *canal.RowsEvent, rowIndex int) string {
	file := strings.TrimSpace(h.currentBinlogFile)
	if file == "" {
		file = "unknown-binlog"
	}
	action := "row"
	if e != nil && strings.TrimSpace(e.Action) != "" {
		action = e.Action
	}
	return fmt.Sprintf("%s:%d:%s:%d", file, eventLogPos(e), action, rowIndex)
}

func rowKeySummary(e *canal.RowsEvent, row []interface{}) string {
	if e == nil || e.Table == nil || len(row) == 0 {
		return "-"
	}
	keyCols := e.Table.PKColumns
	if len(keyCols) == 0 && len(e.Table.Columns) > 0 {
		keyCols = []int{0}
	}
	if len(keyCols) == 0 {
		return "-"
	}

	parts := make([]string, 0, len(keyCols))
	for _, idx := range keyCols {
		if idx < 0 || idx >= len(e.Table.Columns) || idx >= len(row) {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", e.Table.Columns[idx].Name, logValue(row[idx])))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ",")
}

func changedColumnsForRows(e *canal.RowsEvent, oldRow, newRow []interface{}, maxColumns int) string {
	if e == nil || e.Table == nil || maxColumns <= 0 {
		return "-"
	}

	cols := make([]string, 0, maxColumns)
	for j, col := range e.Table.Columns {
		if j >= len(oldRow) || j >= len(newRow) || reflect.DeepEqual(oldRow[j], newRow[j]) {
			continue
		}
		cols = append(cols, col.Name)
		if len(cols) >= maxColumns {
			return strings.Join(cols, ",") + ",..."
		}
	}
	if len(cols) == 0 {
		return "-"
	}
	return strings.Join(cols, ",")
}

func summarizeUpdateChangedColumns(e *canal.RowsEvent, maxColumns int) string {
	if e == nil || e.Table == nil || e.Action != canal.UpdateAction || maxColumns <= 0 {
		return "-"
	}

	seen := make(map[string]struct{})
	cols := make([]string, 0, maxColumns)
	for i := 0; i+1 < len(e.Rows); i += 2 {
		oldRow := e.Rows[i]
		newRow := e.Rows[i+1]
		for j, col := range e.Table.Columns {
			if j >= len(oldRow) || j >= len(newRow) || reflect.DeepEqual(oldRow[j], newRow[j]) {
				continue
			}
			if _, ok := seen[col.Name]; ok {
				continue
			}
			seen[col.Name] = struct{}{}
			cols = append(cols, col.Name)
			if len(cols) >= maxColumns {
				return strings.Join(cols, ",") + ",..."
			}
		}
	}
	if len(cols) == 0 {
		return "-"
	}
	return strings.Join(cols, ",")
}

func (h *cdcHandler) logEmitted(ev model.Event, keySummary, changedSummary string) {
	pos := "-"
	if ev.SourceOffset != nil {
		pos = fmt.Sprintf("%s:%d", firstNonEmpty(ev.SourceOffset.BinlogFile, "unknown-binlog"), ev.SourceOffset.BinlogPos)
	}
	log.Printf("[mysql][job %s] CDC emitted trace_id=%s action=%s table=%s.%s source_pos=%s key=%s changed=%s",
		h.jobID, ev.TraceID, ev.Type, ev.Schema, ev.Table, pos, keySummary, changedSummary)
}

func (h *cdcHandler) OnRow(e *canal.RowsEvent) error {
	if e.Table == nil {
		return nil
	}

	key := strings.ToLower(e.Table.Schema + "." + e.Table.Name)
	if !h.allowed[key] {
		return nil
	}

	log.Printf("[mysql][job %s] CDC received action=%s rows=%d table=%s.%s source_pos=%s:%d keys=%s changed=%s",
		h.jobID, e.Action, affectedRowsCount(e), e.Table.Schema, e.Table.Name, firstNonEmpty(h.currentBinlogFile, "unknown-binlog"), eventLogPos(e), summarizeRowsEventKeys(e, 3), summarizeUpdateChangedColumns(e, 8))
	observability.RecordMySQLCDC(h.jobID, e.Table.Schema, e.Table.Name, e.Action, affectedRowsCount(e))

	eventTime := eventTimestamp(e)
	sourceOffset := h.sourceOffsetFor(e)

	switch e.Action {
	case canal.InsertAction:
		for rowIndex, row := range e.Rows {
			data := make(map[string]interface{}, len(e.Table.Columns))
			for i, col := range e.Table.Columns {
				data[col.Name] = row[i]
			}
			ev := model.Event{
				Type:         model.EventTypeInsert,
				TraceID:      h.traceIDFor(e, rowIndex),
				Schema:       e.Table.Schema,
				Table:        e.Table.Name,
				Data:         data,
				Timestamp:    eventTime,
				Origin:       model.EventOriginCDC,
				SourceOffset: sourceOffset,
			}
			if err := h.emit(ev); err != nil {
				return err
			}
			h.logEmitted(ev, rowKeySummary(e, row), "-")
		}

	case canal.UpdateAction:
		for i, rowIndex := 0, 0; i < len(e.Rows); i, rowIndex = i+2, rowIndex+1 {
			if i+1 >= len(e.Rows) {
				return fmt.Errorf("mysql update rows event has odd row count for %s.%s: %d", e.Table.Schema, e.Table.Name, len(e.Rows))
			}
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]
			changed := changedColumnsForRows(e, oldRow, newRow, 8)

			oldData := make(map[string]interface{}, len(e.Table.Columns))
			newData := make(map[string]interface{}, len(e.Table.Columns))
			for j, col := range e.Table.Columns {
				oldData[col.Name] = oldRow[j]
				newData[col.Name] = newRow[j]
			}
			ev := model.Event{
				Type:         model.EventTypeUpdate,
				TraceID:      h.traceIDFor(e, rowIndex),
				Schema:       e.Table.Schema,
				Table:        e.Table.Name,
				Data:         newData,
				OldData:      oldData,
				Timestamp:    eventTime,
				Origin:       model.EventOriginCDC,
				SourceOffset: sourceOffset,
			}
			if err := h.emit(ev); err != nil {
				return err
			}
			h.logEmitted(ev, rowKeySummary(e, newRow), changed)
		}

	case canal.DeleteAction:
		for rowIndex, row := range e.Rows {
			data := make(map[string]interface{}, len(e.Table.Columns))
			for i, col := range e.Table.Columns {
				data[col.Name] = row[i]
			}
			ev := model.Event{
				Type:         model.EventTypeDelete,
				TraceID:      h.traceIDFor(e, rowIndex),
				Schema:       e.Table.Schema,
				Table:        e.Table.Name,
				Data:         data,
				Timestamp:    eventTime,
				Origin:       model.EventOriginCDC,
				SourceOffset: sourceOffset,
			}
			if err := h.emit(ev); err != nil {
				return err
			}
			h.logEmitted(ev, rowKeySummary(e, row), "-")
		}
	}

	return nil
}

func (h *cdcHandler) OnDDL(header *replication.EventHeader, nextPos gomysql.Position, qe *replication.QueryEvent) error {
	stmt := strings.TrimSpace(string(qe.Query))
	if stmt == "" {
		return nil
	}

	dbFromEvent := strings.ToLower(strings.TrimSpace(string(qe.Schema)))
	tbl := strings.ToLower(extractTableNameFromDDL(stmt))
	if tbl == "" {
		return nil
	}

	db := dbFromEvent
	if db == "" {
		db2 := extractDBNameFromDDL(stmt)
		db = strings.ToLower(strings.TrimSpace(db2))
	}
	if db == "" {
		return nil
	}

	key := strings.ToLower(db + "." + tbl)
	if !h.allowed[key] {
		return nil
	}

	schemaChanges, err := schemachange.ParseMySQLDDL(stmt)
	if err != nil {
		return fmt.Errorf("parse schema change for %s: %w", key, err)
	}

	ddlEvent := model.Event{
		Type:          model.EventTypeDDL,
		Schema:        db,
		Table:         tbl,
		DDL:           stmt,
		Timestamp:     time.Now(),
		Origin:        model.EventOriginCDC,
		SchemaChanges: schemaChanges,
	}
	ddlFile := strings.TrimSpace(nextPos.Name)
	if ddlFile == "" {
		ddlFile = h.currentBinlogFile
	}
	if ddlFile != "" && nextPos.Pos > 0 {
		ddlEvent.SourceOffset = &model.SourceOffset{BinlogFile: ddlFile, BinlogPos: nextPos.Pos}
	}
	if isCreateTableDDL(stmt) && h.schemaFetcher != nil {
		schema, err := h.schemaFetcher(h.ctx, db, tbl)
		if err != nil {
			log.Printf("[WARN][mysql][job %s] fetch schema after CREATE TABLE failed %s.%s: %v", h.jobID, db, tbl, err)
		} else {
			ddlEvent.SourceSchema = schema
		}
	}

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case h.out <- ddlEvent:
		return nil

	case <-timer.C:
		log.Printf("[WARN][job %s] backpressure on DDL, waiting... %s.%s", h.jobID, db, tbl)
		file := strings.TrimSpace(nextPos.Name)
		if file == "" {
			file = h.currentBinlogFile
		}
		h.reportBackpressure(file, nextPos.Pos, fmt.Sprintf("DDL event buffer remained full for 10 seconds while applying %s.%s", db, tbl))
		select {
		case h.out <- ddlEvent:
			h.reportLivePosition(file, nextPos.Pos, true)
			return nil
		case <-h.ctx.Done():
			return h.ctx.Err()
		}

	case <-h.ctx.Done():
		return h.ctx.Err()
	}
}

func (h *cdcHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	if rotateEvent != nil && len(rotateEvent.NextLogName) > 0 {
		h.currentBinlogFile = string(rotateEvent.NextLogName)
		log.Printf("[mysql][job %s] CDC rotate binlog file=%s pos=%d", h.jobID, h.currentBinlogFile, rotateEvent.Position)
		h.reportLivePosition(h.currentBinlogFile, uint32(rotateEvent.Position), true)
	}
	return nil
}

func (h *cdcHandler) OnPosSynced(header *replication.EventHeader, pos gomysql.Position, set gomysql.GTIDSet, force bool) error {
	// Canal.Close invokes OnPosSynced to publish one final checkpoint. Once the
	// source context is cancelled, the pipeline may be shutting down and its
	// output channel must no longer receive events.
	if h.ctx != nil {
		if err := h.ctx.Err(); err != nil {
			return err
		}
	}

	if strings.TrimSpace(pos.Name) == "" || pos.Pos == 0 {
		return nil
	}
	h.reportLivePosition(pos.Name, uint32(pos.Pos), false)

	eventType := "unknown"
	if header != nil {
		eventType = header.EventType.String()
	}

	ev := model.Event{
		Type:      model.EventTypeCheckpoint,
		TraceID:   fmt.Sprintf("%s:%d:checkpoint", pos.Name, pos.Pos),
		Timestamp: time.Now(),
		Origin:    model.EventOriginCDC,
		SourceOffset: &model.SourceOffset{
			BinlogFile: pos.Name,
			BinlogPos:  uint32(pos.Pos),
		},
	}
	if err := h.emit(ev); err != nil {
		return err
	}
	now := time.Now()
	if force || h.lastCheckpointLog.IsZero() || now.Sub(h.lastCheckpointLog) >= 30*time.Second {
		log.Printf("[mysql][job %s] CDC checkpoint emitted trace_id=%s pos=%s:%d event=%s force=%t",
			h.jobID, ev.TraceID, pos.Name, pos.Pos, eventType, force)
		h.lastCheckpointLog = now
	}
	return nil
}

// ---------------- Schema fetch ----------------

func (s *Source) FetchSchemaFor(ctx context.Context, dbName, tableName string) (*model.TableSchema, error) {
	q := `
	SELECT
	COLUMN_NAME,
	DATA_TYPE,
	COLUMN_TYPE,
	IS_NULLABLE,
	COLUMN_KEY,
	CHARACTER_MAXIMUM_LENGTH,
	NUMERIC_PRECISION,
	NUMERIC_SCALE
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	ORDER BY ORDINAL_POSITION`

	rows, err := s.db.QueryContext(ctx, q, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []model.TableColumn
	for rows.Next() {
		var (
			name, dataType, columnType, isNullable, columnKey string
			charMax                                           sql.NullInt64
			numPrec                                           sql.NullInt64
			numScale                                          sql.NullInt64
		)
		if err := rows.Scan(&name, &dataType, &columnType, &isNullable, &columnKey, &charMax, &numPrec, &numScale); err != nil {
			return nil, err
		}

		var cml *int64
		if charMax.Valid {
			v := charMax.Int64
			cml = &v
		}
		var np *int64
		if numPrec.Valid {
			v := numPrec.Int64
			np = &v
		}
		var ns *int64
		if numScale.Valid {
			v := numScale.Int64
			ns = &v
		}

		cols = append(cols, model.TableColumn{
			Name:       name,
			DataType:   strings.ToLower(dataType),
			ColumnType: strings.ToLower(columnType),

			CharMaxLen: cml,
			NumPrec:    np,
			NumScale:   ns,

			IsNullable: isNullable == "YES",
			IsPK:       columnKey == "PRI",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !hasPrimaryKey(cols) {
		pkCols, err := s.fetchPrimaryKeysFallback(ctx, dbName, tableName)
		if err != nil {
			log.Printf("[mysql][job %s] primary key fallback lookup failed for %s.%s: %v", s.jobID, dbName, tableName, err)
		} else if len(pkCols) > 0 {
			pkSet := make(map[string]struct{}, len(pkCols))
			for _, name := range pkCols {
				pkSet[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
			}
			for i := range cols {
				if _, ok := pkSet[strings.ToLower(cols[i].Name)]; ok {
					cols[i].IsPK = true
				}
			}
		}
	}

	schema := &model.TableSchema{
		SchemaName: dbName,
		TableName:  tableName,
		Columns:    cols,
	}
	s.addSnapshotExtraColumnsToSchema(schema)
	return schema, nil
}

func hasPrimaryKey(cols []model.TableColumn) bool {
	for _, col := range cols {
		if col.IsPK {
			return true
		}
	}
	return false
}

func (s *Source) addSnapshotExtraColumnsToSchema(schema *model.TableSchema) {
	if schema == nil {
		return
	}
	cfg, ok := s.tableConfigForTable(schema.SchemaName, schema.TableName)
	if !ok || len(cfg.SnapshotExtraColumns) == 0 {
		return
	}
	existing := make(map[string]struct{}, len(schema.Columns))
	for _, col := range schema.Columns {
		existing[strings.ToLower(strings.TrimSpace(col.Name))] = struct{}{}
	}
	for _, extra := range cfg.SnapshotExtraColumns {
		name := strings.TrimSpace(extra.Name)
		if name == "" {
			continue
		}
		lower := strings.ToLower(name)
		if _, ok := existing[lower]; ok {
			continue
		}
		isNullable := true
		if extra.Nullable != nil {
			isNullable = *extra.Nullable
		}
		schema.Columns = append(schema.Columns, model.TableColumn{
			Name:       name,
			DataType:   strings.ToLower(strings.TrimSpace(extra.DataType)),
			ColumnType: strings.ToLower(strings.TrimSpace(extra.ColumnType)),
			IsNullable: isNullable,
		})
		existing[lower] = struct{}{}
	}
}

func expandConfiguredTables(ctx context.Context, entries []string, listTables func(context.Context, string) ([]string, error)) ([]string, error) {
	out := make([]string, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))

	for _, entry := range entries {
		dbName, tableName, wildcard, ok := parseConfiguredTableEntry(entry)
		if !ok {
			return nil, fmt.Errorf("bad mysql.tables entry: %q (expected db.table or db.*)", entry)
		}

		if !wildcard {
			full := dbName + "." + tableName
			key := strings.ToLower(full)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, full)
			continue
		}

		tablePattern := tableName
		if tablePattern == "" {
			tablePattern = "*"
		}

		tables, err := listTables(ctx, dbName)
		if err != nil {
			return nil, fmt.Errorf("expand mysql.tables wildcard %q failed: %w", dbName+"."+tablePattern, err)
		}

		matched := 0
		for _, tbl := range tables {
			if !matchTableGlob(tablePattern, tbl) {
				continue
			}
			matched++
			full := dbName + "." + tbl
			key := strings.ToLower(full)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, full)
		}
		if matched == 0 {
			return nil, fmt.Errorf("mysql.tables wildcard %q matched no tables", dbName+"."+tablePattern)
		}
	}

	return out, nil
}

func parseConfiguredTableEntry(raw string) (dbName, tableName string, wildcard bool, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false, false
	}

	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return "", "", false, false
	}

	dbName = strings.ToLower(strings.TrimSpace(parts[0]))
	tableName = strings.ToLower(strings.TrimSpace(parts[1]))
	if dbName == "" || tableName == "" {
		return "", "", false, false
	}
	if tableName == "*" {
		return dbName, "", true, true
	}
	if strings.Contains(tableName, "*") {
		return dbName, tableName, true, true
	}
	return dbName, tableName, false, true
}

func matchTableGlob(pattern, table string) bool {
	pattern = strings.ToLower(strings.TrimSpace(pattern))
	table = strings.ToLower(strings.TrimSpace(table))
	if pattern == "" || pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == table
	}

	expr := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", ".*") + "$"
	ok, err := regexp.MatchString(expr, table)
	return err == nil && ok
}

func listBaseTablesInSchema(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const q = `
	SELECT TABLE_NAME
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
	ORDER BY TABLE_NAME`

	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 16)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func lookupTableType(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	const q = `
	SELECT TABLE_TYPE
	FROM INFORMATION_SCHEMA.TABLES
	WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	LIMIT 1`

	var tableType string
	if err := db.QueryRowContext(ctx, q, schema, table).Scan(&tableType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("mysql.tables entry %q not found", schema+"."+table)
		}
		return "", err
	}
	return strings.ToUpper(strings.TrimSpace(tableType)), nil
}

func filterConfiguredTablesToBaseTables(ctx context.Context, entries []string, lookup func(context.Context, string, string) (string, error)) ([]string, []string, error) {
	out := make([]string, 0, len(entries))
	skipped := make([]string, 0)

	for _, entry := range entries {
		dbName, tableName, ok := splitDBTable(entry)
		if !ok {
			return nil, nil, fmt.Errorf("bad mysql.tables entry: %q (expected db.table)", entry)
		}

		tableType, err := lookup(ctx, dbName, tableName)
		if err != nil {
			return nil, nil, err
		}
		if tableType != "BASE TABLE" {
			skipped = append(skipped, dbName+"."+tableName)
			continue
		}

		out = append(out, dbName+"."+tableName)
	}

	if len(out) == 0 {
		return nil, skipped, fmt.Errorf("mysql.tables resolved to no base tables after skipping non-table objects")
	}

	return out, skipped, nil
}

func (s *Source) fetchPrimaryKeysFallback(ctx context.Context, dbName, tableName string) ([]string, error) {
	q := fmt.Sprintf(
		"SHOW KEYS FROM `%s` FROM `%s` WHERE Key_name = 'PRIMARY'",
		escapeMySQLIdentifier(tableName),
		escapeMySQLIdentifier(dbName),
	)

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colNameIdx := -1
	for i, name := range cols {
		if strings.EqualFold(name, "Column_name") {
			colNameIdx = i
			break
		}
	}
	if colNameIdx < 0 {
		return nil, fmt.Errorf("show keys result does not include Column_name")
	}

	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	out := make([]string, 0, 4)
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		switch v := values[colNameIdx].(type) {
		case string:
			if vv := strings.TrimSpace(v); vv != "" {
				out = append(out, vv)
			}
		case []byte:
			if vv := strings.TrimSpace(string(v)); vv != "" {
				out = append(out, vv)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func escapeMySQLIdentifier(v string) string {
	return strings.ReplaceAll(v, "`", "``")
}

// ---------------- Snapshot ----------------

type snapshotResumeState struct {
	rowsEmitted int64
	cursorJSON  string
}

type snapshotQueryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type snapshotPlan struct {
	dbName      string
	tableName   string
	fullName    string
	selectList  string
	filter      string
	orderCols   []string
	keyCols     []string
	useKeyset   bool
	quotedOrder string
}

type snapshotCursor struct {
	Columns []string              `json:"columns"`
	Values  []snapshotCursorValue `json:"values"`
}

type snapshotCursorValue struct {
	Kind    string  `json:"kind"`
	String  string  `json:"string,omitempty"`
	Int64   int64   `json:"int64,omitempty"`
	Uint64  uint64  `json:"uint64,omitempty"`
	Float64 float64 `json:"float64,omitempty"`
	Bool    bool    `json:"bool,omitempty"`
	Time    string  `json:"time,omitempty"`
}

func (s *Source) RunInitialSnapshotAll(ctx context.Context, out chan<- model.Event) error {
	if err := s.requireResolvedSnapshotCutoffs(); err != nil {
		return err
	}
	totalTables := len(s.cfg.Tables)
	for i, full := range s.cfg.Tables {
		if s.shouldSkipSnapshotTable(full) {
			tableIndex := i + 1
			s.reportProgress(connector.ProgressInfo{
				Phase:             "snapshot",
				Summary:           fmt.Sprintf("Skipping snapshot table %d/%d", tableIndex, totalTables),
				Detail:            fmt.Sprintf("%s | source and target row counts match", full),
				CurrentTable:      full,
				CurrentTableIndex: tableIndex,
				CompletedTables:   tableIndex,
				TotalTables:       totalTables,
			})
			log.Printf("[mysql][job %s] snapshot skip %s because source and target row counts match", s.jobID, full)
			continue
		}

		dbName, tblName, ok := splitDBTable(full)
		if !ok {
			return fmt.Errorf("bad mysql.tables entry: %q (expected db.table)", full)
		}
		if err := s.runInitialSnapshotOne(ctx, out, dbName, tblName, snapshotResumeState{}, i, totalTables); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) ResumeInitialSnapshotAll(ctx context.Context, out chan<- model.Event) error {
	if err := s.requireResolvedSnapshotCutoffs(); err != nil {
		return err
	}
	startTable := ""
	resume := snapshotResumeState{}

	if s.offsetSto != nil {
		progress, err := s.offsetSto.GetSnapshotProgress(ctx, s.checkpointKey())
		if err != nil {
			return fmt.Errorf("get snapshot progress failed: %w", err)
		}
		if progress != nil {
			startTable = strings.ToLower(strings.TrimSpace(progress.TableName))
			if progress.NextOffset > 0 {
				resume.rowsEmitted = progress.NextOffset
			}
			resume.cursorJSON = progress.CursorJSON
		}
	}

	startIndex := 0
	if startTable != "" {
		found := false
		for i, full := range s.cfg.Tables {
			if strings.ToLower(strings.TrimSpace(full)) == startTable {
				startIndex = i
				found = true
				break
			}
		}
		if !found {
			log.Printf("[mysql][job %s] snapshot progress table %q no longer exists in config, restarting snapshot from the beginning", s.jobID, startTable)
			startTable = ""
			resume = snapshotResumeState{}
		}
	}

	totalTables := len(s.cfg.Tables)
	for i := startIndex; i < len(s.cfg.Tables); i++ {
		full := s.cfg.Tables[i]
		dbName, tblName, ok := splitDBTable(full)
		if !ok {
			return fmt.Errorf("bad mysql.tables entry: %q (expected db.table)", full)
		}

		state := snapshotResumeState{}
		if startTable != "" && strings.ToLower(strings.TrimSpace(full)) == startTable {
			state = resume
		}

		if err := s.runInitialSnapshotOne(ctx, out, dbName, tblName, state, i, totalTables); err != nil {
			return err
		}
	}

	return nil
}

func (s *Source) RunInitialSnapshotAllFromSavedProgress(ctx context.Context, out chan<- model.Event) error {
	resume, err := s.shouldResumeSnapshotProgress(ctx)
	if err != nil {
		return err
	}
	if resume {
		log.Printf("[mysql][job %s] snapshot retry resuming from saved progress", s.jobID)
		return s.ResumeInitialSnapshotAll(ctx, out)
	}
	return s.RunInitialSnapshotAll(ctx, out)
}

func (s *Source) shouldResumeSnapshotProgress(ctx context.Context) (bool, error) {
	if s.offsetSto == nil {
		return false, nil
	}
	progress, err := s.offsetSto.GetSnapshotProgress(ctx, s.checkpointKey())
	if err != nil {
		return false, fmt.Errorf("get snapshot progress failed: %w", err)
	}
	if progress == nil {
		return false, nil
	}
	return strings.TrimSpace(progress.TableName) != "", nil
}

func (s *Source) resumeSnapshotOnlyProgress(ctx context.Context, out chan<- model.Event, resume func(context.Context, chan<- model.Event) error) (bool, error) {
	progress, err := s.offsetSto.GetSnapshotProgress(ctx, s.checkpointKey())
	if err != nil {
		return false, fmt.Errorf("get snapshot progress failed: %w", err)
	}
	if progress == nil {
		return false, nil
	}
	// Legacy snapshot-only jobs had progress but no job_snapshots row. Preserve
	// their cursor while creating an incomplete state for the new handshake.
	if err := s.offsetSto.SaveSnapshotStart(ctx, s.checkpointKey(), meta.Offset{}); err != nil {
		return true, fmt.Errorf("save snapshot-only start failed: %w", err)
	}

	tableName := strings.TrimSpace(progress.TableName)
	detail := "Continuing snapshot-only load from saved table progress"
	if tableName != "" {
		detail = fmt.Sprintf("Continuing snapshot-only load from %s", tableName)
	}
	s.reportProgress(connector.ProgressInfo{
		Phase:            "snapshot",
		Summary:          "Resuming snapshot-only load",
		Detail:           detail,
		CurrentTable:     tableName,
		CurrentTableRows: progress.NextOffset,
		TotalTables:      len(s.cfg.Tables),
	})
	if err := s.emitSnapshotBarrier(ctx, out, model.EventTypeSnapshotStart); err != nil {
		return true, fmt.Errorf("start snapshot barrier failed: %w", err)
	}

	if err := util.RetryWithBackoff(ctx, s.retry, func() error {
		return resume(ctx, out)
	}); err != nil {
		return true, err
	}

	if err := s.completeSnapshot(ctx, out); err != nil {
		return true, err
	}
	s.reportProgress(connector.ProgressInfo{
		Phase:           "snapshot_complete",
		Summary:         "Snapshot-only resume complete",
		Detail:          fmt.Sprintf("Loaded %d table(s) without CDC", len(s.cfg.Tables)),
		CompletedTables: len(s.cfg.Tables),
		TotalTables:     len(s.cfg.Tables),
	})
	return true, nil
}

func (s *Source) runInitialSnapshotOne(ctx context.Context, out chan<- model.Event, dbName, tblName string, resume snapshotResumeState, tableIdx int, totalTables int) error {
	chunk := s.cfg.ChunkSize
	if chunk <= 0 {
		chunk = 1000
	}
	if s.snapshotBatchEvents && s.cfg.SnapshotBatchSize > 0 {
		chunk = s.cfg.SnapshotBatchSize
	}

	plan, err := s.buildSnapshotPlan(ctx, dbName, tblName)
	if err != nil {
		return err
	}

	cursor, rowsEmitted, cursorJSON := s.restoreSnapshotCursor(plan, resume)
	rollingTable := s.rollingSnapshotBatches && rowsEmitted == 0

	log.Printf("[mysql][job %s] snapshot start %s rows_emitted=%d mode=%s",
		s.jobID,
		plan.fullName,
		rowsEmitted,
		plan.paginationMode(),
	)
	if plan.filter != "" {
		log.Printf("[mysql][job %s] snapshot filter enabled table=%s filter=%s", s.jobID, plan.fullName, plan.filter)
	}

	total := int(rowsEmitted)
	tableIndex := tableIdx + 1
	resumed := rowsEmitted > 0 || strings.TrimSpace(cursorJSON) != ""
	s.reportSnapshotProgress(plan.fullName, tableIndex, tableIdx, totalTables, rowsEmitted, resumed)

	if s.offsetSto != nil {
		if err := s.offsetSto.SaveSnapshotProgress(ctx, s.checkpointKey(), plan.fullName, rowsEmitted, cursorJSON); err != nil {
			return fmt.Errorf("save snapshot progress failed %s rows=%d: %w", plan.fullName, rowsEmitted, err)
		}
	}

	if err := s.withConsistentSnapshot(ctx, func(q snapshotQueryer) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			query, args, err := plan.buildSnapshotQuery(chunk, rowsEmitted, cursor)
			if err != nil {
				return err
			}

			log.Printf("[mysql][job %s] snapshot query table=%s rows_emitted=%d limit=%d mode=%s",
				s.jobID,
				plan.fullName,
				rowsEmitted,
				chunk,
				plan.paginationMode(),
			)

			queryStartedAt := time.Now()
			rows, err := q.QueryContext(ctx, query, args...)
			queryOpenDuration := time.Since(queryStartedAt)
			if err != nil {
				return fmt.Errorf("snapshot query failed %s rows=%d duration=%s: %w", plan.fullName, rowsEmitted, queryOpenDuration.Round(time.Millisecond), err)
			}

			cols, err := rows.Columns()
			if err != nil {
				rows.Close()
				return fmt.Errorf("snapshot query columns failed %s rows=%d duration=%s: %w", plan.fullName, rowsEmitted, time.Since(queryStartedAt).Round(time.Millisecond), err)
			}

			values := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}

			events := make([]model.Event, 0, chunk)
			batchRows := make([]map[string]interface{}, 0, chunk)
			rowCount := 0
			now := time.Now()
			var lastCursor *snapshotCursor

			scanStartedAt := time.Now()
			for rows.Next() {
				if err := rows.Scan(ptrs...); err != nil {
					rows.Close()
					return err
				}

				data := make(map[string]interface{}, len(cols))
				for i, c := range cols {
					data[c] = values[i]
				}

				if s.snapshotBatchEvents {
					batchRows = append(batchRows, data)
				} else {
					events = append(events, model.Event{
						Type:      model.EventTypeInsert,
						Schema:    dbName,
						Table:     tblName,
						Data:      data,
						Timestamp: now,
						Origin:    model.EventOriginSnapshot,
					})
				}

				if plan.useKeyset {
					lastCursor, err = newSnapshotCursor(plan.keyCols, data)
					if err != nil {
						rows.Close()
						return err
					}
				}

				rowCount++
			}
			scanDuration := time.Since(scanStartedAt)
			fetchDuration := time.Since(queryStartedAt)

			if err := rows.Err(); err != nil {
				rows.Close()
				return fmt.Errorf("snapshot query scan failed %s rows=%d query_open_duration=%s scan_duration=%s fetch_duration=%s: %w",
					plan.fullName,
					rowsEmitted,
					queryOpenDuration.Round(time.Millisecond),
					scanDuration.Round(time.Millisecond),
					fetchDuration.Round(time.Millisecond),
					err,
				)
			}
			rows.Close()

			log.Printf("[mysql][job %s] snapshot query done table=%s rows_emitted=%d rows=%d limit=%d mode=%s query_open_duration=%s scan_duration=%s fetch_duration=%s",
				s.jobID,
				plan.fullName,
				rowsEmitted,
				rowCount,
				chunk,
				plan.paginationMode(),
				queryOpenDuration.Round(time.Millisecond),
				scanDuration.Round(time.Millisecond),
				fetchDuration.Round(time.Millisecond),
			)

			if rowCount == 0 {
				return nil
			}

			if s.snapshotBatchEvents {
				ev := model.Event{
					Type:                model.EventTypeSnapshotBatch,
					Schema:              dbName,
					Table:               tblName,
					Rows:                batchRows,
					Timestamp:           now,
					Origin:              model.EventOriginSnapshot,
					SnapshotStartOffset: rowsEmitted,
					SnapshotRolling:     rollingTable,
				}
				keepAlive := func(ctx context.Context) error {
					_, err := q.ExecContext(ctx, "DO 1")
					return err
				}
				if err := s.emitSnapshotBatch(ctx, out, ev, plan.fullName, tableIndex, totalTables, rowsEmitted+int64(rowCount), keepAlive); err != nil {
					return err
				}
				clearSnapshotRows(batchRows)
			} else {
				for i := range events {
					if err := s.emitSnapshotEvent(ctx, out, events[i], plan.fullName, tableIndex, totalTables, rowsEmitted+int64(i)); err != nil {
						return err
					}
					events[i] = model.Event{}
				}
			}

			rowsEmitted += int64(rowCount)
			if plan.useKeyset {
				cursor = lastCursor
				cursorJSON, err = encodeSnapshotCursor(cursor)
				if err != nil {
					return err
				}
			}

			if s.offsetSto != nil && !rollingTable {
				if err := s.offsetSto.SaveSnapshotProgress(ctx, s.checkpointKey(), plan.fullName, rowsEmitted, cursorJSON); err != nil {
					return fmt.Errorf("save snapshot progress failed %s rows=%d: %w", plan.fullName, rowsEmitted, err)
				}
			}
			s.reportSnapshotProgress(plan.fullName, tableIndex, tableIdx, totalTables, rowsEmitted, resumed)
			total += rowCount
			log.Printf("[mysql][job %s] snapshot chunk done table=%s rows=%d total=%d", s.jobID, plan.fullName, rowCount, total)
		}
	}); err != nil {
		return err
	}

	if rollingTable {
		if err := s.emitSnapshotTableComplete(ctx, out, dbName, tblName, rowsEmitted); err != nil {
			return fmt.Errorf("commit rolled snapshot table %s: %w", plan.fullName, err)
		}
		if s.offsetSto != nil {
			if err := s.offsetSto.SaveSnapshotProgress(ctx, s.checkpointKey(), plan.fullName, rowsEmitted, cursorJSON); err != nil {
				return fmt.Errorf("save rolled snapshot progress failed %s rows=%d: %w", plan.fullName, rowsEmitted, err)
			}
		}
	}

	log.Printf("[mysql][job %s] snapshot finished %s total=%d", s.jobID, plan.fullName, total)
	s.reportProgress(connector.ProgressInfo{
		Phase:             "snapshot",
		Summary:           fmt.Sprintf("Snapshot table %d/%d complete", tableIndex, totalTables),
		Detail:            fmt.Sprintf("%s | %d rows emitted", plan.fullName, rowsEmitted),
		CurrentTable:      plan.fullName,
		CurrentTableIndex: tableIndex,
		CompletedTables:   tableIndex,
		TotalTables:       totalTables,
		CurrentTableRows:  rowsEmitted,
	})
	return nil
}

func (s *Source) buildSnapshotPlan(ctx context.Context, dbName, tblName string) (*snapshotPlan, error) {
	schema, err := s.FetchSchemaFor(ctx, dbName, tblName)
	if err != nil {
		return nil, fmt.Errorf("fetch schema failed %s.%s: %w", dbName, tblName, err)
	}
	if schema == nil || len(schema.Columns) == 0 {
		return nil, fmt.Errorf("snapshot requires schema for %s.%s", dbName, tblName)
	}

	keyCols := make([]string, 0)
	orderCols := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		orderCols = append(orderCols, col.Name)
		if col.IsPK {
			keyCols = append(keyCols, col.Name)
		}
	}

	configuredKeyCols, hasConfiguredKeyCols, err := s.snapshotKeyColumnsForTable(dbName, tblName, schema)
	if err != nil {
		return nil, err
	}
	if hasConfiguredKeyCols {
		keyCols = configuredKeyCols
		log.Printf("[mysql][job %s] snapshot %s.%s using configured key columns=%s", s.jobID, dbName, tblName, strings.Join(keyCols, ","))
	}

	useKeyset := len(keyCols) > 0
	if useKeyset {
		orderCols = append([]string(nil), keyCols...)
	} else {
		log.Printf("[mysql][job %s] snapshot %s.%s has no primary key; falling back to ordered OFFSET pagination", s.jobID, dbName, tblName)
	}

	return &snapshotPlan{
		dbName:      dbName,
		tableName:   tblName,
		fullName:    strings.ToLower(dbName + "." + tblName),
		selectList:  s.snapshotSelectListForTable(dbName, tblName),
		filter:      s.snapshotFilterForTable(dbName, tblName),
		orderCols:   orderCols,
		keyCols:     keyCols,
		useKeyset:   useKeyset,
		quotedOrder: quotedColumnList(orderCols),
	}, nil
}

func (s *Source) CountRows(ctx context.Context, dbName, tblName string) (int64, error) {
	if err := s.requireResolvedSnapshotCutoffs(); err != nil {
		return 0, err
	}
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s`.`%s`",
		escapeMySQLIdentifier(dbName),
		escapeMySQLIdentifier(tblName),
	)
	if filter := s.snapshotFilterForTable(dbName, tblName); filter != "" {
		query += " WHERE (" + filter + ")"
	}

	var count int64
	if err := s.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Source) snapshotFilterForTable(dbName, tblName string) string {
	if cfg, ok := s.tableConfigForTable(dbName, tblName); ok {
		return renderSnapshotConfigExpression(cfg.Filter, dbName, tblName)
	}

	return ""
}

func (s *Source) snapshotSelectListForTable(dbName, tblName string) string {
	selectList := fmt.Sprintf("`%s`.*", escapeMySQLIdentifier(tblName))
	cfg, ok := s.tableConfigForTable(dbName, tblName)
	if !ok || len(cfg.SnapshotExtraColumns) == 0 {
		return selectList
	}
	parts := []string{selectList}
	for _, extra := range cfg.SnapshotExtraColumns {
		expr := renderSnapshotConfigExpression(extra.Expression, dbName, tblName)
		if strings.TrimSpace(expr) == "" || strings.TrimSpace(extra.Name) == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("(%s) AS `%s`", expr, escapeMySQLIdentifier(extra.Name)))
	}
	return strings.Join(parts, ", ")
}

func renderSnapshotConfigExpression(expr, dbName, tblName string) string {
	out := strings.TrimSpace(expr)
	replacements := map[string]string{
		"{{schema}}": escapeMySQLIdentifier(dbName),
		"{{table}}":  escapeMySQLIdentifier(tblName),
	}
	for key, value := range replacements {
		out = strings.ReplaceAll(out, key, value)
	}
	return out
}

func (s *Source) tableConfigForTable(dbName, tblName string) (config.MySQLTableConfig, bool) {
	if len(s.cfg.TableConfigs) == 0 {
		return config.MySQLTableConfig{}, false
	}

	fullName := strings.ToLower(strings.TrimSpace(dbName + "." + tblName))
	if cfg, ok := s.cfg.TableConfigs[fullName]; ok {
		return cfg, true
	}

	shortName := strings.ToLower(strings.TrimSpace(tblName))
	if cfg, ok := s.cfg.TableConfigs[shortName]; ok {
		return cfg, true
	}

	type match struct {
		key   string
		cfg   config.MySQLTableConfig
		score int
	}
	matches := make([]match, 0)
	for key, cfg := range s.cfg.TableConfigs {
		if !strings.ContainsAny(key, "*?") {
			continue
		}
		if mysqlTableConfigPatternMatches(key, fullName, shortName) {
			matches = append(matches, match{
				key:   key,
				cfg:   cfg,
				score: mysqlTableConfigPatternSpecificity(key),
			})
		}
	}
	if len(matches) > 0 {
		sort.Slice(matches, func(i, j int) bool {
			if matches[i].score != matches[j].score {
				return matches[i].score > matches[j].score
			}
			return matches[i].key < matches[j].key
		})
		return matches[0].cfg, true
	}

	return config.MySQLTableConfig{}, false
}

func mysqlTableConfigPatternMatches(patternKey, fullName, shortName string) bool {
	patternKey = strings.ToLower(strings.TrimSpace(patternKey))
	fullName = strings.ToLower(strings.TrimSpace(fullName))
	shortName = strings.ToLower(strings.TrimSpace(shortName))
	if patternKey == "" {
		return false
	}
	if ok, err := path.Match(patternKey, fullName); err == nil && ok {
		return true
	}
	if !strings.Contains(patternKey, ".") {
		if ok, err := path.Match(patternKey, shortName); err == nil && ok {
			return true
		}
	}
	return false
}

func mysqlTableConfigPatternSpecificity(patternKey string) int {
	score := 0
	for _, ch := range patternKey {
		switch ch {
		case '*', '?':
			continue
		default:
			score++
		}
	}
	return score
}

func (s *Source) snapshotKeyColumnsForTable(dbName, tblName string, schema *model.TableSchema) ([]string, bool, error) {
	cfg, ok := s.tableConfigForTable(dbName, tblName)
	if !ok || len(cfg.SnapshotKeyColumns) == 0 {
		return nil, false, nil
	}

	actualByLower := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		actualByLower[strings.ToLower(strings.TrimSpace(col.Name))] = col.Name
	}

	out := make([]string, 0, len(cfg.SnapshotKeyColumns))
	seen := make(map[string]struct{}, len(cfg.SnapshotKeyColumns))
	for _, raw := range cfg.SnapshotKeyColumns {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		actual, exists := actualByLower[key]
		if !exists {
			return nil, false, fmt.Errorf("snapshot_key_columns for %s.%s references missing column %q", dbName, tblName, name)
		}
		if _, exists := seen[key]; exists {
			return nil, false, fmt.Errorf("snapshot_key_columns for %s.%s contains duplicate column %q", dbName, tblName, name)
		}
		seen[key] = struct{}{}
		out = append(out, actual)
	}
	if len(out) == 0 {
		return nil, false, nil
	}
	return out, true, nil
}

func (s *Source) restoreSnapshotCursor(plan *snapshotPlan, resume snapshotResumeState) (*snapshotCursor, int64, string) {
	rowsEmitted := resume.rowsEmitted
	cursorJSON := strings.TrimSpace(resume.cursorJSON)

	if !plan.useKeyset {
		return nil, rowsEmitted, ""
	}
	if rowsEmitted == 0 && cursorJSON == "" {
		return nil, 0, ""
	}
	if cursorJSON == "" {
		log.Printf("[mysql][job %s] snapshot progress for %s is missing keyset cursor; restarting current table from the beginning", s.jobID, plan.fullName)
		return nil, 0, ""
	}

	cursor, err := decodeSnapshotCursor(cursorJSON)
	if err != nil {
		log.Printf("[mysql][job %s] snapshot progress for %s has invalid keyset cursor: %v; restarting current table from the beginning", s.jobID, plan.fullName, err)
		return nil, 0, ""
	}
	if !cursor.matchesColumns(plan.keyCols) {
		log.Printf("[mysql][job %s] snapshot progress for %s uses outdated key columns; restarting current table from the beginning", s.jobID, plan.fullName)
		return nil, 0, ""
	}

	return cursor, rowsEmitted, cursorJSON
}

func (s *Source) withConsistentSnapshot(ctx context.Context, fn func(snapshotQueryer) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("snapshot connection failed: %w", err)
	}

	cleanup := func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(rollbackCtx, "ROLLBACK")
		_ = conn.Close()
	}
	defer cleanup()

	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("set snapshot isolation failed: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return fmt.Errorf("start consistent snapshot failed: %w", err)
	}

	return fn(conn)
}

func (p *snapshotPlan) paginationMode() string {
	if p.useKeyset {
		return "keyset"
	}
	return "offset"
}

func (p *snapshotPlan) buildSnapshotQuery(limit int, rowsEmitted int64, cursor *snapshotCursor) (string, []interface{}, error) {
	selectList := strings.TrimSpace(p.selectList)
	if selectList == "" {
		selectList = "*"
	}
	base := fmt.Sprintf("SELECT %s FROM `%s`.`%s`", selectList, escapeMySQLIdentifier(p.dbName), escapeMySQLIdentifier(p.tableName))
	clauses := make([]string, 0, 2)
	args := make([]interface{}, 0, len(p.keyCols)+2)
	if p.filter != "" {
		clauses = append(clauses, "("+p.filter+")")
	}

	if p.useKeyset {
		if cursor != nil {
			predicate, predicateArgs, err := buildSnapshotCursorPredicate(p.keyCols, cursor)
			if err != nil {
				return "", nil, err
			}
			wrappedPredicate := "(" + predicate + ")"
			if len(clauses) > 0 {
				wrappedPredicate = "(" + wrappedPredicate + ")"
			}
			clauses = append(clauses, wrappedPredicate)
			args = append(args, predicateArgs...)
		}
		query := base
		if len(clauses) > 0 {
			query += " WHERE " + strings.Join(clauses, " AND ")
		}
		args = append(args, limit)
		return fmt.Sprintf("%s ORDER BY %s LIMIT ?", query, p.quotedOrder), args, nil
	}

	query := base
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	args = append(args, limit, rowsEmitted)
	return fmt.Sprintf("%s ORDER BY %s LIMIT ? OFFSET ?", query, p.quotedOrder), args, nil
}

func quotedColumnList(cols []string) string {
	parts := make([]string, 0, len(cols))
	for _, col := range cols {
		parts = append(parts, fmt.Sprintf("`%s`", escapeMySQLIdentifier(col)))
	}
	return strings.Join(parts, ", ")
}

func buildSnapshotCursorPredicate(cols []string, cursor *snapshotCursor) (string, []interface{}, error) {
	if cursor == nil {
		return "", nil, nil
	}
	if !cursor.matchesColumns(cols) {
		return "", nil, fmt.Errorf("snapshot cursor columns do not match current key columns")
	}

	values, err := cursor.queryArgs()
	if err != nil {
		return "", nil, err
	}

	orParts := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols)*(len(cols)+1)/2)
	for i := range cols {
		andParts := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			andParts = append(andParts, fmt.Sprintf("`%s` = ?", escapeMySQLIdentifier(cols[j])))
			args = append(args, values[j])
		}
		andParts = append(andParts, fmt.Sprintf("`%s` > ?", escapeMySQLIdentifier(cols[i])))
		args = append(args, values[i])
		predicate := strings.Join(andParts, " AND ")
		if len(cols) > 1 {
			predicate = "(" + predicate + ")"
		}
		orParts = append(orParts, predicate)
	}

	return strings.Join(orParts, " OR "), args, nil
}

func newSnapshotCursor(cols []string, data map[string]interface{}) (*snapshotCursor, error) {
	values := make([]snapshotCursorValue, 0, len(cols))
	for _, col := range cols {
		v, ok := data[col]
		if !ok {
			return nil, fmt.Errorf("snapshot cursor column %q not found in row", col)
		}
		encoded, err := encodeSnapshotCursorValue(v)
		if err != nil {
			return nil, fmt.Errorf("snapshot cursor column %q: %w", col, err)
		}
		values = append(values, encoded)
	}

	return &snapshotCursor{
		Columns: append([]string(nil), cols...),
		Values:  values,
	}, nil
}

func encodeSnapshotCursor(cursor *snapshotCursor) (string, error) {
	if cursor == nil {
		return "", nil
	}

	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("marshal snapshot cursor failed: %w", err)
	}
	return string(raw), nil
}

func decodeSnapshotCursor(raw string) (*snapshotCursor, error) {
	var cursor snapshotCursor
	if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot cursor failed: %w", err)
	}
	if len(cursor.Values) == 0 {
		return nil, fmt.Errorf("snapshot cursor is empty")
	}
	return &cursor, nil
}

func encodeSnapshotCursorValue(v interface{}) (snapshotCursorValue, error) {
	switch t := v.(type) {
	case nil:
		return snapshotCursorValue{Kind: "null"}, nil
	case []byte:
		return snapshotCursorValue{Kind: "string", String: string(t)}, nil
	case string:
		return snapshotCursorValue{Kind: "string", String: t}, nil
	case int:
		return snapshotCursorValue{Kind: "int64", Int64: int64(t)}, nil
	case int8:
		return snapshotCursorValue{Kind: "int64", Int64: int64(t)}, nil
	case int16:
		return snapshotCursorValue{Kind: "int64", Int64: int64(t)}, nil
	case int32:
		return snapshotCursorValue{Kind: "int64", Int64: int64(t)}, nil
	case int64:
		return snapshotCursorValue{Kind: "int64", Int64: t}, nil
	case uint:
		return snapshotCursorValue{Kind: "uint64", Uint64: uint64(t)}, nil
	case uint8:
		return snapshotCursorValue{Kind: "uint64", Uint64: uint64(t)}, nil
	case uint16:
		return snapshotCursorValue{Kind: "uint64", Uint64: uint64(t)}, nil
	case uint32:
		return snapshotCursorValue{Kind: "uint64", Uint64: uint64(t)}, nil
	case uint64:
		return snapshotCursorValue{Kind: "uint64", Uint64: t}, nil
	case float32:
		return snapshotCursorValue{Kind: "float64", Float64: float64(t)}, nil
	case float64:
		return snapshotCursorValue{Kind: "float64", Float64: t}, nil
	case bool:
		return snapshotCursorValue{Kind: "bool", Bool: t}, nil
	case time.Time:
		return snapshotCursorValue{Kind: "time", Time: t.Format(time.RFC3339Nano)}, nil
	default:
		return snapshotCursorValue{}, fmt.Errorf("unsupported cursor value type %T", v)
	}
}

func (c *snapshotCursor) matchesColumns(cols []string) bool {
	if c == nil || len(c.Values) != len(cols) {
		return false
	}
	if len(c.Columns) == 0 {
		return false
	}
	for i, col := range cols {
		if !strings.EqualFold(strings.TrimSpace(c.Columns[i]), strings.TrimSpace(col)) {
			return false
		}
	}
	return true
}

func (c *snapshotCursor) queryArgs() ([]interface{}, error) {
	out := make([]interface{}, 0, len(c.Values))
	for _, value := range c.Values {
		v, err := value.decode()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (v snapshotCursorValue) decode() (interface{}, error) {
	switch v.Kind {
	case "null":
		return nil, nil
	case "string":
		return v.String, nil
	case "int64":
		return v.Int64, nil
	case "uint64":
		return v.Uint64, nil
	case "float64":
		return v.Float64, nil
	case "bool":
		return v.Bool, nil
	case "time":
		tm, err := time.Parse(time.RFC3339Nano, v.Time)
		if err != nil {
			return nil, fmt.Errorf("parse snapshot cursor time failed: %w", err)
		}
		return tm, nil
	default:
		return nil, fmt.Errorf("unsupported snapshot cursor kind %q", v.Kind)
	}
}

// ---------------- CDC runner ----------------

const cdcLagMonitorInterval = 30 * time.Second

func runMySQLBackground(jobID, operation string, run func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = util.Permanent(fmt.Errorf("mysql %s panic: %v", operation, recovered))
			log.Printf("[mysql][job %s] %s panic: %v\n%s", jobID, operation, recovered, debug.Stack())
		}
	}()
	return run()
}

func (s *Source) monitorCDCLag(ctx context.Context) {
	if s.progress == nil || ctx.Err() != nil {
		return
	}

	report := func() {
		if err := s.reportCDCLag(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[mysql][job %s] CDC lag monitor error: %v", s.jobID, err)
		}
	}
	report()

	ticker := time.NewTicker(cdcLagMonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}

func (s *Source) reportCDCLag(ctx context.Context) error {
	var checkpoint *meta.Offset
	if s.offsetSto != nil {
		var err error
		checkpoint, err = s.offsetSto.GetOffset(ctx, s.checkpointKey())
		if err != nil {
			return fmt.Errorf("get checkpoint: %w", err)
		}
		if checkpoint == nil && s.checkpointKey() != s.jobID {
			checkpoint, err = s.offsetSto.GetOffset(ctx, s.jobID)
			if err != nil {
				return fmt.Errorf("get legacy checkpoint: %w", err)
			}
		}
	}

	latest, err := s.getMasterPos(ctx)
	if err != nil {
		return fmt.Errorf("get latest binlog position: %w", err)
	}
	if latest == nil {
		return nil
	}
	earliestFile, latestAvailableFile, availableCount, err := s.availableBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("get available binlogs: %w", err)
	}

	info := connector.ProgressInfo{
		Phase:               "cdc_health",
		CDCLatestFile:       latest.Name,
		CDCLatestPos:        latest.Pos,
		CDCEarliestFile:     earliestFile,
		CDCAvailableBinlogs: availableCount,
		CDCBinlogStatus:     "no_checkpoint",
	}
	if checkpoint == nil || strings.TrimSpace(checkpoint.BinlogFile) == "" || checkpoint.BinlogPos == 0 {
		s.reportProgress(info)
		return nil
	}

	lagFiles, ok := binlogFileLag(checkpoint.BinlogFile, latest.Name)
	if !ok {
		return fmt.Errorf("cannot compare checkpoint file %q with latest file %q", checkpoint.BinlogFile, latest.Name)
	}
	info.CDCCheckpointFile = checkpoint.BinlogFile
	info.CDCCheckpointPos = checkpoint.BinlogPos
	info.CDCLagFiles = lagFiles
	info.CDCBinlogStatus = binlogCheckpointStatus(checkpoint.BinlogFile, earliestFile, latestAvailableFile, availableCount)
	s.reportProgress(info)
	return nil
}

func binlogCheckpointStatus(checkpointFile, earliestFile, latestFile string, availableCount int) string {
	if availableCount <= 0 || strings.TrimSpace(earliestFile) == "" || strings.TrimSpace(latestFile) == "" {
		return "no_binlogs"
	}

	checkpointPrefix, checkpointNumber, checkpointOK := splitBinlogFileNumber(checkpointFile)
	earliestPrefix, earliestNumber, earliestOK := splitBinlogFileNumber(earliestFile)
	latestPrefix, latestNumber, latestOK := splitBinlogFileNumber(latestFile)
	if !checkpointOK || !earliestOK || !latestOK || checkpointPrefix != earliestPrefix || checkpointPrefix != latestPrefix {
		return "missing"
	}
	if checkpointNumber < earliestNumber {
		return "purged"
	}
	if checkpointNumber <= latestNumber {
		return "available"
	}
	return "missing"
}

func binlogFileLag(checkpointFile, latestFile string) (int, bool) {
	checkpointPrefix, checkpointNumber, ok := splitBinlogFileNumber(checkpointFile)
	if !ok {
		return 0, false
	}
	latestPrefix, latestNumber, ok := splitBinlogFileNumber(latestFile)
	if !ok || checkpointPrefix != latestPrefix {
		return 0, false
	}
	if latestNumber <= checkpointNumber {
		return 0, true
	}
	return latestNumber - checkpointNumber, true
}

func splitBinlogFileNumber(file string) (string, int, bool) {
	file = strings.TrimSpace(file)
	end := len(file)
	start := end
	for start > 0 {
		ch := file[start-1]
		if ch < '0' || ch > '9' {
			break
		}
		start--
	}
	if start == end {
		return "", 0, false
	}
	number, err := strconv.Atoi(file[start:end])
	if err != nil {
		return "", 0, false
	}
	return file[:start], number, true
}

func (s *Source) runBinlogCanal(ctx context.Context, out chan<- model.Event, startFrom *gomysql.Position) error {
	s.reportStreamingProgress(startFrom)
	lagMonitorCtx, stopLagMonitor := context.WithCancel(ctx)
	defer stopLagMonitor()
	go func() {
		if err := runMySQLBackground(s.jobID, "CDC lag monitor", func() error {
			s.monitorCDCLag(lagMonitorCtx)
			return nil
		}); err != nil {
			log.Printf("[mysql][job %s] CDC lag monitor stopped: %v", s.jobID, err)
		}
	}()

	cCfg := canal.NewDefaultConfig()
	cCfg.Addr = s.cfg.Addr
	cCfg.User = s.cfg.User
	cCfg.Password = s.cfg.Password
	cCfg.Flavor = "mysql"
	cCfg.Charset = "utf8mb4"

	cCfg.Dump.ExecutionPath = ""

	// Subscribe multiple tables
	pats := make([]string, 0, len(s.cfg.Tables))
	for _, full := range s.cfg.Tables {
		dbName, tblName, ok := splitDBTable(full)
		if !ok {
			continue
		}
		pats = append(pats, fmt.Sprintf("^%s\\.%s$",
			regexp.QuoteMeta(dbName),
			regexp.QuoteMeta(tblName),
		))
	}
	cCfg.IncludeTableRegex = pats

	cCfg.HeartbeatPeriod = 2 * time.Second
	cCfg.ReadTimeout = 5 * time.Minute

	return util.RetryWithBackoff(ctx, s.retry, func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// ServerID harus unik (kalau banyak instance/job sama jalan bareng)
		cCfg.ServerID = uint32(time.Now().UnixNano()%1000000) + 10000
		log.Printf("[mysql][job %s] CDC setup addr=%s server_id=%d tables=%d",
			s.jobID, s.cfg.Addr, cCfg.ServerID, len(s.cfg.Tables))

		c, err := canal.NewCanal(cCfg)
		if err != nil {
			log.Printf("[mysql][job %s] failed to create canal: %v", s.jobID, err)
			return err
		}
		var closeOnce sync.Once
		closeCanal := func() {
			closeOnce.Do(c.Close)
		}
		defer closeCanal()

		currentBinlogFile := ""
		startBinlogFile := ""
		startBinlogPos := uint32(0)
		if startFrom != nil {
			currentBinlogFile = startFrom.Name
			startBinlogFile = startFrom.Name
			startBinlogPos = startFrom.Pos
		} else if pos, posErr := s.getMasterPos(ctx); posErr == nil && pos != nil {
			currentBinlogFile = pos.Name
		} else if posErr != nil {
			log.Printf("[mysql][job %s] CDC unable to resolve current binlog file for trace ids: %v", s.jobID, posErr)
		}

		handler := &cdcHandler{
			jobID:             s.jobID,
			allowed:           s.allowedTables,
			currentBinlogFile: currentBinlogFile,
			startBinlogFile:   startBinlogFile,
			startBinlogPos:    startBinlogPos,
			totalTables:       len(s.cfg.Tables),
			progress:          s.progress,
			schemaFetcher:     s.FetchSchemaFor,
			out:               out,
			ctx:               ctx,
		}
		c.SetEventHandler(handler)

		done := make(chan error, 1)

		if startFrom != nil {
			s.logBinlogResumeDiagnostics(ctx, startFrom)
			log.Printf("[mysql][job %s] CDC RunFrom %s:%d", s.jobID, startFrom.Name, startFrom.Pos)
			go func() {
				err := runMySQLBackground(s.jobID, "CDC RunFrom", func() error {
					return c.RunFrom(*startFrom)
				})
				log.Printf("[mysql][job %s] canal RunFrom(%s:%d) returned: %T %v",
					s.jobID, startFrom.Name, startFrom.Pos, err, err)
				done <- err
			}()
		} else {
			log.Printf("[mysql][job %s] CDC Run() from latest", s.jobID)
			go func() {
				err := runMySQLBackground(s.jobID, "CDC Run", c.Run)
				log.Printf("[mysql][job %s] canal Run() returned: %T %v", s.jobID, err, err)
				done <- err
			}()
		}

		select {
		case err := <-done:
			if err != nil {
				err = s.classifyBinlogStartError(ctx, err, startFrom)
				log.Printf("[mysql][job %s] canal Run error: %T %v", s.jobID, err, err)
				return err
			}
			return nil
		case <-ctx.Done():
			// Close synchronously and wait for Run/RunFrom to exit before returning.
			// This guarantees the job cannot close the shared event channel while a
			// canal callback is still capable of emitting a final checkpoint.
			log.Printf("[mysql][job %s] context cancelled, closing canal", s.jobID)
			closeCanal()
			<-done
			return ctx.Err()
		}
	})
}

func (s *Source) logBinlogResumeDiagnostics(ctx context.Context, startFrom *gomysql.Position) {
	if startFrom == nil {
		return
	}

	diagnostics := s.binlogCheckpointDiagnostics(ctx)
	if diagnostics == "" {
		diagnostics = "diagnostics unavailable."
	}
	log.Printf("[mysql][job %s] CDC RunFrom diagnostic checkpoint=%s:%d %s",
		s.jobID, startFrom.Name, startFrom.Pos, diagnostics)
}

func (s *Source) classifyBinlogStartError(ctx context.Context, err error, startFrom *gomysql.Position) error {
	if !isInvalidBinlogCheckpointError(err) {
		return classifyBinlogStartError(err, startFrom)
	}
	return classifyBinlogStartErrorWithDiagnostics(err, startFrom, s.binlogCheckpointDiagnostics(ctx))
}

func classifyBinlogStartError(err error, startFrom *gomysql.Position) error {
	return classifyBinlogStartErrorWithDiagnostics(err, startFrom, "")
}

func classifyBinlogStartErrorWithDiagnostics(err error, startFrom *gomysql.Position, diagnostics string) error {
	if err == nil {
		return nil
	}
	if !isInvalidBinlogCheckpointError(err) {
		return err
	}

	checkpoint := "latest checkpoint"
	if startFrom != nil {
		checkpoint = fmt.Sprintf("%s:%d", startFrom.Name, startFrom.Pos)
	}

	if diagnostics != "" {
		diagnostics = " " + diagnostics
	}

	return util.Permanent(fmt.Errorf(
		"mysql no longer accepts binlog checkpoint %s (ERROR 1236).%s Snapshot rows may already have been emitted, but CDC cannot continue from that checkpoint. This usually means the binlog file was purged/rotated or the saved position is no longer valid. Delete the saved checkpoint and rerun with mode=initial for a fresh snapshot, or use mode=latest to continue from the current binlog head and accept the gap: %w",
		checkpoint,
		diagnostics,
		err,
	))
}

func (s *Source) binlogCheckpointDiagnostics(ctx context.Context) string {
	if s == nil {
		return ""
	}

	parts := make([]string, 0, 2)
	if detail := s.savedOffsetDiagnostic(ctx); detail != "" {
		parts = append(parts, detail)
	}
	if detail := s.availableBinlogsDiagnostic(ctx); detail != "" {
		parts = append(parts, detail)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

func (s *Source) savedOffsetDiagnostic(ctx context.Context) string {
	if s.offsetSto == nil {
		return "Saved checkpoint metadata unavailable: no meta store configured."
	}

	key := s.checkpointKey()
	off, err := s.offsetSto.GetOffset(ctx, key)
	if err != nil {
		return fmt.Sprintf("Saved checkpoint metadata unavailable for key=%s: %v.", key, err)
	}
	if off == nil && key != s.jobID {
		legacy, legacyErr := s.offsetSto.GetOffset(ctx, s.jobID)
		if legacyErr != nil {
			return fmt.Sprintf("Saved checkpoint metadata unavailable for key=%s legacy_key=%s: %v.", key, s.jobID, legacyErr)
		}
		if legacy != nil {
			key = s.jobID
			off = legacy
		}
	}
	if off == nil {
		return fmt.Sprintf("No saved CDC checkpoint row found for key=%s.", key)
	}

	updatedAt := "unknown"
	if !off.UpdatedAt.IsZero() {
		updatedAt = off.UpdatedAt.Format(time.RFC3339)
	}
	return fmt.Sprintf("Saved checkpoint row key=%s pos=%s:%d updated_at=%s.", key, off.BinlogFile, off.BinlogPos, updatedAt)
}

func (s *Source) availableBinlogsDiagnostic(ctx context.Context) string {
	if s.db == nil {
		return "MySQL binary log list unavailable: source DB is not connected."
	}

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	first, last, count, err := s.availableBinlogs(queryCtx)
	if err != nil {
		return fmt.Sprintf("MySQL binary log list unavailable: %v.", err)
	}
	if count == 0 {
		return "MySQL binary log list is empty."
	}
	return fmt.Sprintf("MySQL binary logs currently available earliest=%s latest=%s count=%d.", first, last, count)
}

func (s *Source) availableBinlogs(ctx context.Context) (first string, last string, count int, err error) {
	rows, err := s.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return "", "", 0, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", "", 0, err
	}
	if len(cols) == 0 {
		return "", "", 0, fmt.Errorf("SHOW BINARY LOGS returned no columns")
	}

	for rows.Next() {
		values := make([]sql.RawBytes, len(cols))
		scan := make([]any, len(cols))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return "", "", 0, err
		}
		name := string(values[0])
		if count == 0 {
			first = name
		}
		last = name
		count++
	}
	if err := rows.Err(); err != nil {
		return "", "", 0, err
	}
	return first, last, count, nil
}

func isInvalidBinlogCheckpointError(err error) bool {
	var myErr *gomysql.MyError
	if errors.As(err, &myErr) {
		return myErr.Code == 1236
	}

	return strings.Contains(strings.ToLower(err.Error()), "error 1236")
}

const (
	showBinaryLogStatusQuery = "SHOW BINARY LOG STATUS"
	showMasterStatusQuery    = "SHOW MASTER STATUS"
)

type masterPosRow interface {
	Scan(dest ...any) error
}

type masterPosQueryFunc func(context.Context, string) masterPosRow

func (s *Source) getMasterPos(ctx context.Context) (*gomysql.Position, error) {
	return getMasterPosWithQuery(ctx, func(ctx context.Context, query string) masterPosRow {
		return s.db.QueryRowContext(ctx, query)
	})
}

func getMasterPosWithQuery(ctx context.Context, queryRow masterPosQueryFunc) (*gomysql.Position, error) {
	pos, err := queryMasterPos(ctx, queryRow, showBinaryLogStatusQuery)
	if err == nil {
		return pos, nil
	}
	if !isUnsupportedBinaryLogStatusError(err) {
		return nil, err
	}

	fallbackPos, fallbackErr := queryMasterPos(ctx, queryRow, showMasterStatusQuery)
	if fallbackErr == nil {
		return fallbackPos, nil
	}

	return nil, fmt.Errorf("%s failed: %v; %s failed: %w", showBinaryLogStatusQuery, err, showMasterStatusQuery, fallbackErr)
}

func queryMasterPos(ctx context.Context, queryRow masterPosQueryFunc, query string) (*gomysql.Position, error) {
	row := queryRow(ctx, query)
	var file string
	var pos uint32
	var binlogDoDB, binlogIgnoreDB, executedGtidSet sql.NullString
	if err := row.Scan(&file, &pos, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s returned no rows; MySQL binary log coordinates are unavailable from this endpoint: %w", query, err)
		}
		return nil, err
	}
	return &gomysql.Position{Name: file, Pos: pos}, nil
}

func isUnsupportedBinaryLogStatusError(err error) bool {
	var myErr *mysqldriver.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == 1064
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "syntax") && strings.Contains(msg, "binary log status")
}

func (s *Source) RunFull(ctx context.Context, out chan<- model.Event, mode config.JobMode) error {
	return s.runFull(ctx, out, mode, mode)
}

func (s *Source) RunFullWithStoredMode(ctx context.Context, out chan<- model.Event, mode config.JobMode, storedMode config.JobMode) error {
	if storedMode == "" {
		storedMode = mode
	}
	return s.runFull(ctx, out, mode, storedMode)
}

// RunFull
func (s *Source) runFull(ctx context.Context, out chan<- model.Event, mode config.JobMode, storedMode config.JobMode) error {
	if err := s.requireResolvedSnapshotCutoffs(); err != nil {
		return err
	}
	switch mode {

	// =========================
	// 1️⃣ INITIAL (DEFAULT)
	// fresh snapshot + CDC
	// =========================
	case config.JobModeInitial, config.JobModeSnapshotHandoff:
		snapshotHandoff := mode == config.JobModeSnapshotHandoff
		summary := "Preparing initial snapshot"
		detail := fmt.Sprintf("Queued %d table(s) for snapshot", len(s.cfg.Tables))
		if snapshotHandoff {
			summary = "Preparing snapshot worker load"
			detail = fmt.Sprintf("Queued %d table(s) for snapshot before CDC handoff", len(s.cfg.Tables))
		}
		s.reportProgress(connector.ProgressInfo{
			Phase:       "snapshot",
			Summary:     summary,
			Detail:      detail,
			TotalTables: len(s.cfg.Tables),
		})
		if s.offsetSto != nil {
			if err := s.resetInitialCheckpoint(ctx); err != nil {
				return err
			}
		}

		// capture start pos
		startPos, err := s.getMasterPos(ctx)
		if err != nil {
			return fmt.Errorf("get master pos failed: %w", err)
		}

		if s.offsetSto != nil {
			if err := s.offsetSto.SaveSnapshotStart(ctx, s.checkpointKey(), meta.Offset{
				BinlogFile: startPos.Name,
				BinlogPos:  uint32(startPos.Pos),
				UpdatedAt:  time.Now(),
			}); err != nil {
				return fmt.Errorf("save snapshot start failed: %w", err)
			}
		}
		if err := s.emitSnapshotBarrier(ctx, out, model.EventTypeSnapshotStart); err != nil {
			return fmt.Errorf("start snapshot barrier failed: %w", err)
		}

		if err := util.RetryWithBackoff(ctx, s.retry, func() error {
			return s.RunInitialSnapshotAllFromSavedProgress(ctx, out)
		}); err != nil {
			return err
		}

		if err := s.completeSnapshot(ctx, out); err != nil {
			return err
		}
		completeDetail := fmt.Sprintf("Starting CDC for %d table(s)", len(s.cfg.Tables))
		if snapshotHandoff {
			completeDetail = fmt.Sprintf("Ready to hand %d table(s) to the streaming worker", len(s.cfg.Tables))
		}
		s.reportProgress(connector.ProgressInfo{
			Phase:           "snapshot_complete",
			Summary:         "Initial snapshot complete",
			Detail:          completeDetail,
			CompletedTables: len(s.cfg.Tables),
			TotalTables:     len(s.cfg.Tables),
		})
		if snapshotHandoff {
			return nil
		}

		return s.runBinlogCanal(ctx, out, startPos)

	// =========================
	// 1️⃣ SNAPSHOT-ONLY
	// fresh snapshot only, no CDC/binlog
	// =========================
	case config.JobModeSnapshotOnly:
		s.reportProgress(connector.ProgressInfo{
			Phase:       "snapshot",
			Summary:     "Preparing snapshot-only load",
			Detail:      fmt.Sprintf("Queued %d table(s) for snapshot without CDC", len(s.cfg.Tables)),
			TotalTables: len(s.cfg.Tables),
		})
		if s.offsetSto != nil {
			if err := s.resetInitialCheckpoint(ctx); err != nil {
				return err
			}
			if err := s.offsetSto.SaveSnapshotStart(ctx, s.checkpointKey(), meta.Offset{}); err != nil {
				return fmt.Errorf("save snapshot-only start failed: %w", err)
			}
		}
		if err := s.emitSnapshotBarrier(ctx, out, model.EventTypeSnapshotStart); err != nil {
			return fmt.Errorf("start snapshot barrier failed: %w", err)
		}

		if err := util.RetryWithBackoff(ctx, s.retry, func() error {
			return s.RunInitialSnapshotAllFromSavedProgress(ctx, out)
		}); err != nil {
			return err
		}

		if err := s.completeSnapshot(ctx, out); err != nil {
			return err
		}
		s.reportProgress(connector.ProgressInfo{
			Phase:           "snapshot_complete",
			Summary:         "Snapshot-only load complete",
			Detail:          fmt.Sprintf("Loaded %d table(s) without CDC", len(s.cfg.Tables)),
			CompletedTables: len(s.cfg.Tables),
			TotalTables:     len(s.cfg.Tables),
		})
		return nil

	// =========================
	// 2️⃣ RESUME
	// lanjut dari checkpoint snapshot atau offset terakhir
	// =========================
	case config.JobModeResume, config.JobModeSnapshotHandoffResume:
		snapshotHandoff := mode == config.JobModeSnapshotHandoffResume
		if s.offsetSto == nil {
			return fmt.Errorf("resume requires meta store")
		}
		s.reportProgress(connector.ProgressInfo{
			Phase:       "snapshot",
			Summary:     "Checking saved snapshot state",
			Detail:      fmt.Sprintf("Inspecting progress for %d table(s)", len(s.cfg.Tables)),
			TotalTables: len(s.cfg.Tables),
		})

		st, err := s.offsetSto.GetSnapshotState(ctx, s.checkpointKey())
		if err != nil {
			return fmt.Errorf("get snapshot state failed: %w", err)
		}

		if st != nil {
			if st.Done {
				if !st.CompletionConfirmed() {
					return fmt.Errorf("snapshot completion BLOCKED: legacy done state has no sink confirmation for job_id=%s state_key=%s completion_version=%d; verify the original snapshot and follow docs/legacy-snapshot-recovery.md; resubmit alone cannot repair this metadata", s.jobID, s.checkpointKey(), st.CompletionVersion)
				}
				if err := s.completeSnapshot(ctx, out); err != nil {
					return err
				}
				if storedMode == config.JobModeSnapshotOnly {
					return nil
				}
				if snapshotHandoff {
					s.reportProgress(connector.ProgressInfo{
						Phase:           "snapshot_complete",
						Summary:         "Initial snapshot already complete",
						Detail:          fmt.Sprintf("Ready to hand %d table(s) to the streaming worker", len(s.cfg.Tables)),
						CompletedTables: len(s.cfg.Tables),
						TotalTables:     len(s.cfg.Tables),
					})
					return nil
				}
				off, err := s.resumeOffset(ctx)
				if err != nil {
					return fmt.Errorf("get offset failed: %w", err)
				}
				if off != nil && off.BinlogFile != "" && off.BinlogPos > 0 {
					p := gomysql.Position{Name: off.BinlogFile, Pos: uint32(off.BinlogPos)}
					log.Printf("[mysql][job %s] resume CDC from offset %s:%d", s.jobID, p.Name, p.Pos)
					return s.runBinlogCanal(ctx, out, &p)
				}
				if st.StartFile != "" && st.StartPos > 0 {
					p := gomysql.Position{Name: st.StartFile, Pos: st.StartPos}
					log.Printf("[mysql][job %s] resume CDC from snapshot start %s:%d", s.jobID, p.Name, p.Pos)
					return s.runBinlogCanal(ctx, out, &p)
				}
				return fmt.Errorf("resume requested but no saved CDC checkpoint found for job_id=%s", s.jobID)
			}

			if storedMode != config.JobModeSnapshotOnly && (st.StartFile == "" || st.StartPos == 0) {
				return fmt.Errorf("resume requested but snapshot start checkpoint is incomplete for job_id=%s", s.jobID)
			}
			if err := s.emitSnapshotBarrier(ctx, out, model.EventTypeSnapshotStart); err != nil {
				return fmt.Errorf("start snapshot barrier failed: %w", err)
			}

			if err := util.RetryWithBackoff(ctx, s.retry, func() error {
				return s.ResumeInitialSnapshotAll(ctx, out)
			}); err != nil {
				return err
			}

			if err := s.completeSnapshot(ctx, out); err != nil {
				return err
			}
			completeDetail := fmt.Sprintf("Starting CDC for %d table(s)", len(s.cfg.Tables))
			if snapshotHandoff {
				completeDetail = fmt.Sprintf("Ready to hand %d table(s) to the streaming worker", len(s.cfg.Tables))
			}
			s.reportProgress(connector.ProgressInfo{
				Phase:           "snapshot_complete",
				Summary:         "Snapshot resume complete",
				Detail:          completeDetail,
				CompletedTables: len(s.cfg.Tables),
				TotalTables:     len(s.cfg.Tables),
			})
			if snapshotHandoff || storedMode == config.JobModeSnapshotOnly {
				return nil
			}

			p := gomysql.Position{Name: st.StartFile, Pos: st.StartPos}
			log.Printf("[mysql][job %s] snapshot resume complete, continuing CDC from %s:%d", s.jobID, p.Name, p.Pos)
			return s.runBinlogCanal(ctx, out, &p)
		}

		off, err := s.resumeOffset(ctx)
		if err != nil {
			return fmt.Errorf("get offset failed: %w", err)
		}
		if off != nil && off.BinlogFile != "" && off.BinlogPos > 0 {
			p := gomysql.Position{Name: off.BinlogFile, Pos: uint32(off.BinlogPos)}
			log.Printf("[mysql][job %s] resume CDC from offset %s:%d", s.jobID, p.Name, p.Pos)
			return s.runBinlogCanal(ctx, out, &p)
		}

		if storedMode == config.JobModeSnapshotOnly {
			if resumed, err := s.resumeSnapshotOnlyProgress(ctx, out, s.ResumeInitialSnapshotAll); resumed || err != nil {
				return err
			}
		}

		return fmt.Errorf("resume requested but no checkpoint exists for job_id=%s", s.jobID)

	// =========================
	// 3️⃣ LATEST-OFFSET
	// resume ONLY if offset exists
	// =========================
	case config.JobModeLatestOffset:
		if s.offsetSto == nil {
			return fmt.Errorf("latest-offset requires meta store")
		}

		off, err := s.resumeOffset(ctx)
		if err != nil {
			return err
		}
		if off == nil || off.BinlogFile == "" || off.BinlogPos == 0 {
			return fmt.Errorf("latest-offset requested but no offset found for job_id=%s", s.jobID)
		}

		p := gomysql.Position{Name: off.BinlogFile, Pos: uint32(off.BinlogPos)}
		return s.runBinlogCanal(ctx, out, &p)

	// =========================
	// 4️⃣ LATEST
	// ignore snapshot & offset
	// =========================
	case config.JobModeLatest:
		if err := s.clearLegacyCheckpoint(ctx); err != nil {
			return err
		}
		return s.runBinlogCanal(ctx, out, nil)

	default:
		return fmt.Errorf("unknown job mode: %s", mode)
	}
}

// ---------------- Helpers ----------------

func splitDBTable(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", false
	}
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	db := strings.TrimSpace(parts[0])
	tbl := strings.TrimSpace(parts[1])
	if db == "" || tbl == "" {
		return "", "", false
	}
	return strings.ToLower(db), strings.ToLower(tbl), true
}

// Very simple DDL parsing helpers (best-effort)
func isCreateTableDDL(ddl string) bool {
	low := strings.ToLower(ddl)
	low = strings.ReplaceAll(low, "\n", " ")
	low = strings.ReplaceAll(low, "\t", " ")
	low = strings.TrimSpace(low)
	return strings.HasPrefix(low, "create table")
}

func extractTableNameFromDDL(ddl string) string {
	low := strings.ToLower(ddl)
	low = strings.ReplaceAll(low, "\n", " ")
	low = strings.ReplaceAll(low, "\t", " ")
	low = strings.TrimSpace(low)

	// handle: alter table `tbl` ...
	if strings.HasPrefix(low, "alter table") ||
		strings.HasPrefix(low, "create table") ||
		strings.HasPrefix(low, "drop table") ||
		strings.HasPrefix(low, "truncate table") {
		parts := strings.Fields(low)
		if len(parts) < 3 {
			return ""
		}
		identIdx := 2
		if strings.HasPrefix(low, "create table") && len(parts) >= 6 &&
			parts[2] == "if" && parts[3] == "not" && parts[4] == "exists" {
			identIdx = 5
		}
		ident := strings.Trim(parts[identIdx], "`")
		// may be db.tbl
		if strings.Contains(ident, ".") {
			ps := strings.Split(ident, ".")
			return strings.Trim(ps[len(ps)-1], "`")
		}
		return ident
	}
	return ""
}

func extractDBNameFromDDL(ddl string) string {
	low := strings.ToLower(ddl)
	low = strings.ReplaceAll(low, "\n", " ")
	low = strings.ReplaceAll(low, "\t", " ")
	low = strings.TrimSpace(low)

	if strings.HasPrefix(low, "alter table") ||
		strings.HasPrefix(low, "create table") ||
		strings.HasPrefix(low, "drop table") ||
		strings.HasPrefix(low, "truncate table") {
		parts := strings.Fields(low)
		if len(parts) < 3 {
			return ""
		}
		identIdx := 2
		if strings.HasPrefix(low, "create table") && len(parts) >= 6 &&
			parts[2] == "if" && parts[3] == "not" && parts[4] == "exists" {
			identIdx = 5
		}
		ident := strings.Trim(parts[identIdx], "`")
		if strings.Contains(ident, ".") {
			ps := strings.Split(ident, ".")
			return strings.Trim(ps[0], "`")
		}
	}
	return ""
}
