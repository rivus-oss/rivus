package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type JobError struct {
	Component string    `json:"component"`
	Message   string    `json:"message"`
	Time      time.Time `json:"time"`
}

type JobProgress struct {
	Phase                   string `json:"phase"`
	Summary                 string `json:"summary"`
	Detail                  string `json:"detail,omitempty"`
	CurrentTable            string `json:"current_table,omitempty"`
	CurrentTableIndex       int    `json:"current_table_index,omitempty"`
	CompletedTables         int    `json:"completed_tables,omitempty"`
	TotalTables             int    `json:"total_tables,omitempty"`
	CurrentTableRows        int64  `json:"current_table_rows,omitempty"`
	CDCStartFile            string `json:"cdc_start_file,omitempty"`
	CDCStartPos             uint32 `json:"cdc_start_pos,omitempty"`
	CDCCurrentFile          string `json:"cdc_current_file,omitempty"`
	CDCCurrentPos           uint32 `json:"cdc_current_pos,omitempty"`
	CDCCheckpointFile       string `json:"cdc_checkpoint_file,omitempty"`
	CDCCheckpointPos        uint32 `json:"cdc_checkpoint_pos,omitempty"`
	CDCLatestFile           string `json:"cdc_latest_file,omitempty"`
	CDCLatestPos            uint32 `json:"cdc_latest_pos,omitempty"`
	CDCLagFiles             int    `json:"cdc_lag_files,omitempty"`
	CDCEarliestFile         string `json:"cdc_earliest_file,omitempty"`
	CDCAvailableBinlogs     int    `json:"cdc_available_binlogs,omitempty"`
	CDCBinlogStatus         string `json:"cdc_binlog_status,omitempty"`
	CheckpointPending       bool   `json:"checkpoint_pending,omitempty"`
	CheckpointReason        string `json:"checkpoint_reason,omitempty"`
	CheckpointPosition      string `json:"checkpoint_position,omitempty"`
	CheckpointPendingTables string `json:"checkpoint_pending_tables,omitempty"`
	SinkPhase               string `json:"sink_phase,omitempty"`
	SinkSummary             string `json:"sink_summary,omitempty"`
	SinkDetail              string `json:"sink_detail,omitempty"`
	SinkTable               string `json:"sink_table,omitempty"`
	SinkRows                int64  `json:"sink_rows,omitempty"`
	RecoveryStatus          string `json:"recovery_status,omitempty"`
	RecoveryOperationID     string `json:"recovery_operation_id,omitempty"`
	RecoveryAttempts        int    `json:"recovery_attempts,omitempty"`
	RecoveryLastError       string `json:"recovery_last_error,omitempty"`
	RecoveryCheckpoint      string `json:"recovery_checkpoint,omitempty"`
	RecoveryStartedAt       string `json:"recovery_started_at,omitempty"`
}

type Checkpoint struct {
	MetaKey           string                       `json:"meta_key,omitempty"`
	CDCOffset         *CheckpointOffset            `json:"cdc_offset,omitempty"`
	SnapshotState     *CheckpointSnapshotState     `json:"snapshot_state,omitempty"`
	SnapshotProgress  *CheckpointSnapshotProgress  `json:"snapshot_progress,omitempty"`
	BinlogDiagnostics *CheckpointBinlogDiagnostics `json:"binlog_diagnostics,omitempty"`
	Error             string                       `json:"error,omitempty"`
}

type CheckpointOffset struct {
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type CheckpointSnapshotState struct {
	StartFile string `json:"start_file,omitempty"`
	StartPos  uint32 `json:"start_pos,omitempty"`
	Done      bool   `json:"done"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

type CheckpointSnapshotProgress struct {
	TableName  string `json:"table_name,omitempty"`
	NextOffset int64  `json:"next_offset,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type CheckpointBinlogDiagnostics struct {
	CheckpointFile                    string `json:"checkpoint_file,omitempty"`
	CheckpointPos                     uint32 `json:"checkpoint_pos,omitempty"`
	CheckpointUpdatedAt               string `json:"checkpoint_updated_at,omitempty"`
	CheckpointAgeSec                  int64  `json:"checkpoint_age_sec,omitempty"`
	EarliestFile                      string `json:"earliest_file,omitempty"`
	EarliestFirstEventAt              string `json:"earliest_first_event_at,omitempty"`
	EarliestFirstEvent                string `json:"earliest_first_event,omitempty"`
	EarliestFirstEventError           string `json:"earliest_first_event_error,omitempty"`
	CheckpointToEarliestFirstEventSec int64  `json:"checkpoint_to_earliest_first_event_sec,omitempty"`
	LatestFile                        string `json:"latest_file,omitempty"`
	LatestCreatedAt                   string `json:"latest_created_at,omitempty"`
	LatestFirstEventAt                string `json:"latest_first_event_at,omitempty"`
	LatestFirstEvent                  string `json:"latest_first_event,omitempty"`
	LatestFirstEventError             string `json:"latest_first_event_error,omitempty"`
	CheckpointToLatestFirstEventSec   int64  `json:"checkpoint_to_latest_first_event_sec,omitempty"`
	AvailableCount                    int    `json:"available_count,omitempty"`
	ObservedAt                        string `json:"observed_at,omitempty"`
	SourceServerTime                  string `json:"source_server_time,omitempty"`
	CheckpointAvailable               bool   `json:"checkpoint_available"`
	Status                            string `json:"status"`
	Error                             string `json:"error,omitempty"`
}

type Job struct {
	displayName   string
	displayConfig *config.JobConfig
	Config        *config.JobConfig
	Created       time.Time
	Updated       time.Time

	mu                   sync.RWMutex
	status               JobStatus
	errors               []JobError
	progress             *JobProgress
	maintenance          *connector.TableMaintenanceStatus
	maintenanceOwnership connector.MaintenanceOwnershipLifecycle
	cancelFunc           context.CancelFunc
	sourceCancelFunc     context.CancelFunc
	runDone              chan struct{}
	runMode              config.JobMode
	pauseRequested       bool

	statusListener     func(JobStatus)
	progressListener   func(*JobProgress)
	persistenceMu      sync.Mutex
	persistenceDeleted bool
	submissionID       string

	// runtime deps
	registry *connector.Registry

	// meta checkpoint
	metaStore    meta.OffsetStore
	metaKey      string
	runtimeFence func(meta.OffsetStore, string) (meta.OffsetStore, error)

	// graph snapshot
	graph *JobGraph
}

// NOTE: constructor now needs registry
func NewJob(cfg *config.JobConfig, reg *connector.Registry) *Job {
	now := time.Now()
	j := &Job{
		Config:       cfg,
		Created:      now,
		Updated:      now,
		status:       JobStatusCreated,
		registry:     reg,
		submissionID: uuid.NewString(),
	}
	// metaKey will be built at Start(), because it depends on chosen source/sink config
	return j
}

func (j *Job) SubmissionID() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.submissionID
}

func (j *Job) MetaKey() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.metaKey
}

func (j *Job) currentRunMode() config.JobMode {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.runMode
}

// ensureMetaKey resolves the durable checkpoint identity once and then keeps
// it stable for the lifetime of the job. Split workers restore this value from
// job_registry so they cannot derive a different key from a JSON-round-tripped
// copy of the submitted connector config.
func (j *Job) ensureMetaKey() string {
	j.mu.RLock()
	key := strings.TrimSpace(j.metaKey)
	cfg := j.Config
	j.mu.RUnlock()
	if key != "" || cfg == nil {
		return key
	}

	srcType, srcCfg := j.pickSource()
	sinkType, sinkCfg := j.pickSink()
	computed := buildMetaKey(
		cfg.ID,
		string(normalizeMode(cfg.Mode)),
		srcType, srcCfg,
		sinkType, sinkCfg,
	)

	j.mu.Lock()
	if strings.TrimSpace(j.metaKey) == "" {
		j.metaKey = computed
	}
	key = j.metaKey
	j.mu.Unlock()
	return key
}

func (j *Job) GetStatus() JobStatus {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.status
}

func (j *Job) StatusValue() JobStatus { // helper for API
	return j.GetStatus()
}

func (j *Job) GetErrors() []JobError {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make([]JobError, len(j.errors))
	copy(out, j.errors)
	return out
}

func (j *Job) GetLastError() *JobError {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if len(j.errors) == 0 {
		return nil
	}
	le := j.errors[len(j.errors)-1] // copy value
	return &le
}

func (j *Job) Progress() *JobProgress {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if j.progress == nil {
		return nil
	}

	cp := *j.progress
	return &cp
}

func (j *Job) TableMaintenanceStatus() *connector.TableMaintenanceStatus {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return cloneTableMaintenanceStatus(j.maintenance)
}

func (j *Job) updateTableMaintenanceStatus(status *connector.TableMaintenanceStatus) {
	j.mu.Lock()
	j.maintenance = cloneTableMaintenanceStatus(status)
	j.mu.Unlock()
}

func cloneTableMaintenanceStatus(status *connector.TableMaintenanceStatus) *connector.TableMaintenanceStatus {
	if status == nil {
		return nil
	}
	cloned := *status
	cloned.Tables = make([]connector.TableMaintenanceTableStatus, len(status.Tables))
	copy(cloned.Tables, status.Tables)
	for index := range cloned.Tables {
		cloned.Tables[index].Operations = append([]string(nil), status.Tables[index].Operations...)
	}
	return &cloned
}

func (j *Job) Checkpoint() *Checkpoint {
	store, key, readerErr := j.checkpointReader()
	if store == nil || strings.TrimSpace(key) == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cp := &Checkpoint{MetaKey: key}
	if readerErr != nil {
		cp.Error = readerErr.Error()
		return cp
	}

	off, err := store.GetOffset(ctx, key)
	if err != nil {
		cp.Error = err.Error()
		return cp
	}
	if off != nil {
		cp.CDCOffset = &CheckpointOffset{
			BinlogFile: off.BinlogFile,
			BinlogPos:  off.BinlogPos,
			UpdatedAt:  formatCheckpointTime(off.UpdatedAt),
		}
		cp.BinlogDiagnostics = j.checkpointBinlogDiagnostics(ctx, off)
	}

	st, err := store.GetSnapshotState(ctx, key)
	if err != nil {
		cp.Error = err.Error()
		return cp
	}
	if st != nil {
		cp.SnapshotState = &CheckpointSnapshotState{
			StartFile: st.StartFile,
			StartPos:  st.StartPos,
			Done:      st.CompletionConfirmed(),
			UpdatedAt: formatCheckpointTime(st.UpdatedAt),
		}
	}

	progress, err := store.GetSnapshotProgress(ctx, key)
	if err != nil {
		cp.Error = err.Error()
		return cp
	}
	if progress != nil {
		cp.SnapshotProgress = &CheckpointSnapshotProgress{
			TableName:  progress.TableName,
			NextOffset: progress.NextOffset,
			UpdatedAt:  formatCheckpointTime(progress.UpdatedAt),
		}
	}

	if cp.CDCOffset == nil && cp.SnapshotState == nil && cp.SnapshotProgress == nil {
		return nil
	}
	return cp
}

func (j *Job) checkpointReader() (meta.OffsetStore, string, error) {
	j.mu.RLock()
	store := j.metaStore
	key := strings.TrimSpace(j.metaKey)
	j.mu.RUnlock()

	if store != nil && key != "" {
		return store, key, nil
	}

	if key == "" {
		key = j.ensureMetaKey()
	}

	if store == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var err error
		store, err = j.ensureMetaStore(ctx)
		if err != nil {
			return nil, key, err
		}
	}

	j.mu.Lock()
	if strings.TrimSpace(j.metaKey) == "" {
		j.metaKey = key
	}
	j.mu.Unlock()

	return store, key, nil
}

func (j *Job) ensureMetaStore(ctx context.Context) (meta.OffsetStore, error) {
	j.mu.RLock()
	store := j.metaStore
	cfg := j.Config
	j.mu.RUnlock()
	if store != nil || cfg == nil || strings.TrimSpace(cfg.Meta.MySQLDSN) == "" {
		return store, nil
	}

	created, err := meta.NewMySQLOffsetStore(cfg.Meta.MySQLDSN)
	if err != nil {
		return nil, err
	}
	if err := created.Init(ctx); err != nil {
		_ = created.Close()
		return nil, err
	}

	j.mu.Lock()
	if j.metaStore == nil {
		j.metaStore = created
		store = created
		created = nil
	} else {
		store = j.metaStore
	}
	j.mu.Unlock()
	if created != nil {
		_ = created.Close()
	}
	return store, nil
}

func closeOffsetStore(store meta.OffsetStore) {
	if closer, ok := store.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func (j *Job) closeMetaStore() {
	j.mu.Lock()
	store := j.metaStore
	j.metaStore = nil
	j.mu.Unlock()
	closeOffsetStore(store)
}

func (j *Job) checkpointBinlogDiagnostics(ctx context.Context, off *meta.Offset) *CheckpointBinlogDiagnostics {
	if off == nil {
		return nil
	}
	diag := &CheckpointBinlogDiagnostics{
		CheckpointFile:      off.BinlogFile,
		CheckpointPos:       off.BinlogPos,
		CheckpointUpdatedAt: formatCheckpointTime(off.UpdatedAt),
		ObservedAt:          time.Now().Format(time.RFC3339),
		Status:              "unknown",
	}
	if !off.UpdatedAt.IsZero() {
		diag.CheckpointAgeSec = int64(time.Since(off.UpdatedAt).Seconds())
	}

	cfg, ok := j.mysqlSourceConfig()
	if !ok {
		diag.Error = "source is not mysql or mysql config is unavailable"
		return diag
	}

	dbName := strings.TrimSpace(cfg.Database)
	if dbName == "" {
		dbName = "information_schema"
	}
	dsn := drivermysql.NewConfig()
	dsn.User = cfg.User
	dsn.Passwd = cfg.Password
	dsn.Net = "tcp"
	dsn.Addr = cfg.Addr
	dsn.DBName = dbName
	dsn.ParseTime = true
	dsn.InterpolateParams = true
	dsn.Timeout = 10 * time.Second
	dsn.ReadTimeout = 10 * time.Second
	dsn.WriteTimeout = 10 * time.Second
	dsn.Params = map[string]string{"charset": "utf8mb4"}

	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		diag.Error = err.Error()
		return diag
	}
	defer db.Close()

	if serverTime, err := queryMySQLServerTime(ctx, db); err == nil {
		diag.SourceServerTime = formatCheckpointTime(serverTime)
		if !off.UpdatedAt.IsZero() {
			diag.CheckpointAgeSec = int64(serverTime.Sub(off.UpdatedAt).Seconds())
		}
	}

	first, last, count, err := showBinaryLogs(ctx, db)
	if err != nil {
		diag.Error = err.Error()
		return diag
	}

	diag.EarliestFile = first
	diag.LatestFile = last
	diag.AvailableCount = count
	if strings.TrimSpace(first) != "" {
		eventCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		eventTime, eventType, eventErr := firstBinlogEventTime(eventCtx, cfg, first)
		cancel()
		if eventErr != nil {
			diag.EarliestFirstEventError = eventErr.Error()
		} else {
			diag.EarliestFirstEventAt = formatCheckpointTime(eventTime)
			diag.EarliestFirstEvent = eventType
			if !off.UpdatedAt.IsZero() {
				diag.CheckpointToEarliestFirstEventSec = int64(eventTime.Sub(off.UpdatedAt).Seconds())
			}
		}
	}
	if strings.TrimSpace(last) != "" {
		if strings.TrimSpace(first) == strings.TrimSpace(last) {
			diag.LatestFirstEventAt = diag.EarliestFirstEventAt
			diag.LatestFirstEvent = diag.EarliestFirstEvent
			diag.LatestFirstEventError = diag.EarliestFirstEventError
			diag.CheckpointToLatestFirstEventSec = diag.CheckpointToEarliestFirstEventSec
		} else {
			eventCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			eventTime, eventType, eventErr := firstBinlogEventTime(eventCtx, cfg, last)
			cancel()
			if eventErr != nil {
				diag.LatestFirstEventError = eventErr.Error()
			} else {
				diag.LatestFirstEventAt = formatCheckpointTime(eventTime)
				diag.LatestFirstEvent = eventType
				if !off.UpdatedAt.IsZero() {
					diag.CheckpointToLatestFirstEventSec = int64(eventTime.Sub(off.UpdatedAt).Seconds())
				}
			}
		}
	}
	diag.CheckpointAvailable = binlogFileInRange(off.BinlogFile, first, last)
	diag.Status = checkpointBinlogStatus(off.BinlogFile, first, last, count)
	return diag
}

func checkpointBinlogStatus(checkpointFile, first, last string, count int) string {
	checkpointFile = strings.TrimSpace(checkpointFile)
	first = strings.TrimSpace(first)
	last = strings.TrimSpace(last)
	switch {
	case first == "" || last == "" || count == 0:
		return "no_binlogs"
	case binlogFileInRange(checkpointFile, first, last):
		return "available"
	default:
		checkpointPrefix, checkpointNumber, checkpointOK := splitBinlogFile(checkpointFile)
		firstPrefix, firstNumber, firstOK := splitBinlogFile(first)
		if checkpointOK && firstOK && checkpointPrefix == firstPrefix && checkpointNumber < firstNumber {
			return "purged"
		}
		return "missing"
	}
}

// durableCheckpointBinlogStatus checks whether the saved binlog still exists.
func (j *Job) durableCheckpointBinlogStatus(ctx context.Context) (string, error) {
	store, key, err := j.checkpointReader()
	if err != nil {
		return "", err
	}
	if store == nil || strings.TrimSpace(key) == "" {
		return "", nil
	}
	off, err := store.GetOffset(ctx, key)
	if err != nil {
		return "", err
	}
	checkpointFile := ""
	var checkpointPos uint32
	if off != nil {
		checkpointFile = strings.TrimSpace(off.BinlogFile)
		checkpointPos = off.BinlogPos
	}
	if checkpointFile == "" || checkpointPos == 0 {
		state, stateErr := store.GetSnapshotState(ctx, key)
		if stateErr != nil {
			return "", stateErr
		}
		if state != nil {
			checkpointFile = strings.TrimSpace(state.StartFile)
			checkpointPos = state.StartPos
		}
	}
	if checkpointFile == "" || checkpointPos == 0 {
		return "", nil
	}

	cfg, ok := j.mysqlSourceConfig()
	if !ok {
		return "", nil
	}
	dbName := strings.TrimSpace(cfg.Database)
	if dbName == "" {
		dbName = "information_schema"
	}
	dsn := drivermysql.NewConfig()
	dsn.User = cfg.User
	dsn.Passwd = cfg.Password
	dsn.Net = "tcp"
	dsn.Addr = cfg.Addr
	dsn.DBName = dbName
	dsn.ParseTime = true
	dsn.InterpolateParams = true
	dsn.Timeout = 3 * time.Second
	dsn.ReadTimeout = 3 * time.Second
	dsn.WriteTimeout = 3 * time.Second
	dsn.Params = map[string]string{"charset": "utf8mb4"}

	db, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return "", err
	}
	defer db.Close()
	first, last, count, err := showBinaryLogs(ctx, db)
	if err != nil {
		return "", err
	}
	return checkpointBinlogStatus(checkpointFile, first, last, count), nil
}

func firstBinlogEventTime(ctx context.Context, cfg config.MySQLConfig, file string) (time.Time, string, error) {
	file = strings.TrimSpace(file)
	if file == "" {
		return time.Time{}, "", fmt.Errorf("binlog file is empty")
	}

	host, port, err := parseMySQLHostPort(cfg.Addr)
	if err != nil {
		return time.Time{}, "", err
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:        diagnosticServerID(),
		Flavor:          "mysql",
		Host:            host,
		Port:            port,
		User:            cfg.User,
		Password:        cfg.Password,
		Charset:         "utf8mb4",
		HeartbeatPeriod: time.Second,
		ReadTimeout:     2 * time.Second,
	})
	defer syncer.Close()

	streamer, err := syncer.StartSync(gomysql.Position{Name: file, Pos: 4})
	if err != nil {
		return time.Time{}, "", err
	}

	for i := 0; i < 8; i++ {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return time.Time{}, "", err
		}
		if ev == nil || ev.Header == nil || ev.Header.Timestamp == 0 {
			continue
		}
		return time.Unix(int64(ev.Header.Timestamp), 0), ev.Header.EventType.String(), nil
	}
	return time.Time{}, "", fmt.Errorf("no timestamped event found in %s", file)
}

func parseMySQLHostPort(addr string) (string, uint16, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "127.0.0.1", 3306, nil
	}

	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.Count(addr, ":") == 0 || strings.Contains(err.Error(), "missing port in address") {
			return addr, 3306, nil
		}
		return "", 0, err
	}
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}

	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	if port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("mysql port out of range: %d", port)
	}
	return host, uint16(port), nil
}

func diagnosticServerID() uint32 {
	return uint32(2000000000 + time.Now().UnixNano()%100000000)
}

func (j *Job) mysqlSourceConfig() (config.MySQLConfig, bool) {
	if j == nil || j.Config == nil {
		return config.MySQLConfig{}, false
	}
	sourceType, sourceCfg := j.pickSource()
	if strings.TrimSpace(sourceType) == "" {
		sourceType = "mysql"
	}
	if !strings.EqualFold(sourceType, "mysql") {
		return config.MySQLConfig{}, false
	}

	switch cfg := sourceCfg.(type) {
	case config.MySQLConfig:
		return config.NormalizeMySQLConfig(cfg), true
	case *config.MySQLConfig:
		if cfg == nil {
			return config.MySQLConfig{}, false
		}
		return config.NormalizeMySQLConfig(*cfg), true
	default:
		b, err := json.Marshal(cfg)
		if err != nil {
			return config.MySQLConfig{}, false
		}
		var out config.MySQLConfig
		if err := json.Unmarshal(b, &out); err != nil {
			return config.MySQLConfig{}, false
		}
		return config.NormalizeMySQLConfig(out), true
	}
}

func showBinaryLogs(ctx context.Context, db *sql.DB) (string, string, int, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
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

	values := make([]sql.RawBytes, len(cols))
	scan := make([]any, len(cols))
	for i := range values {
		scan[i] = &values[i]
	}

	first := ""
	last := ""
	count := 0
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return "", "", 0, err
		}
		file := string(values[0])
		if count == 0 {
			first = file
		}
		last = file
		count++
	}
	if err := rows.Err(); err != nil {
		return "", "", 0, err
	}
	return first, last, count, nil
}

func queryMySQLServerTime(ctx context.Context, db *sql.DB) (time.Time, error) {
	var unixMicros int64
	if err := db.QueryRowContext(ctx, "SELECT CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS SIGNED)").Scan(&unixMicros); err != nil {
		return time.Time{}, err
	}
	return mysqlServerTimeFromUnixMicros(unixMicros), nil
}

func mysqlServerTimeFromUnixMicros(unixMicros int64) time.Time {
	return time.UnixMicro(unixMicros)
}

func binlogFileInRange(file, first, last string) bool {
	file = strings.TrimSpace(file)
	first = strings.TrimSpace(first)
	last = strings.TrimSpace(last)
	if file == "" || first == "" || last == "" {
		return false
	}
	filePrefix, fileNumber, fileOK := splitBinlogFile(file)
	firstPrefix, firstNumber, firstOK := splitBinlogFile(first)
	lastPrefix, lastNumber, lastOK := splitBinlogFile(last)
	if fileOK && firstOK && lastOK && filePrefix == firstPrefix && filePrefix == lastPrefix {
		return fileNumber >= firstNumber && fileNumber <= lastNumber
	}
	return file == first || file == last
}

func splitBinlogFile(file string) (string, uint64, bool) {
	file = strings.TrimSpace(file)
	dot := strings.LastIndexByte(file, '.')
	if dot <= 0 || dot == len(file)-1 {
		return "", 0, false
	}
	number, err := strconv.ParseUint(file[dot+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return file[:dot], number, true
}

func formatCheckpointTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func (j *Job) setStatus(s JobStatus) {
	j.mu.Lock()
	previous, listener, jobID, changed := j.setStatusLocked(s)
	j.mu.Unlock()

	if !changed {
		return
	}
	j.notifyStatusTransition(jobID, previous, s, listener)
}

func (j *Job) setStatusLocked(s JobStatus) (JobStatus, func(JobStatus), string, bool) {
	if j.status == s {
		return j.status, nil, "", false
	}

	previous := j.status
	j.status = s
	j.Updated = time.Now()
	j.syncProgressForStatusLocked(s)

	// keep graph status in-sync (best effort)
	if j.graph != nil {
		j.graph.Status = s
		for i := range j.graph.Nodes {
			j.graph.Nodes[i].Status = s
		}
	}

	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	return previous, j.statusListener, jobID, true
}

func (j *Job) notifyStatusTransition(jobID string, previous, current JobStatus, listener func(JobStatus)) {
	log.Printf("[job %s] status=%s previous_status=%s", jobID, current, previous)
	if listener != nil {
		listener(current)
	}
}

func (j *Job) syncProgressForStatusLocked(status JobStatus) {
	switch status {
	case JobStatusPending:
		if j.progress == nil {
			j.progress = &JobProgress{
				Phase:   "preflight",
				Summary: "Preparing job",
			}
		}
	case JobStatusQueued:
		j.progress = &JobProgress{
			Phase:   "queued",
			Summary: "Waiting for snapshot slot",
		}
	case JobStatusRunning:
		if j.progress == nil {
			j.progress = &JobProgress{
				Phase:   "running",
				Summary: "Job is running",
			}
		}
	case JobStatusPausing:
		j.progress = finalizeJobProgress(j.progress, "pausing", "Draining pending events before pause")
	case JobStatusPaused:
		j.progress = finalizeJobProgress(j.progress, "paused", "Job paused at a committed checkpoint")
	case JobStatusStopped:
		j.progress = finalizeJobProgress(j.progress, "stopped", "Job stopped")
	case JobStatusFailed:
		j.progress = finalizeJobProgress(j.progress, "failed", "Job failed")
	case JobStatusDone:
		j.progress = finalizeJobProgress(j.progress, "done", "Job completed")
	}
}

func finalizeJobProgress(prev *JobProgress, phase, summary string) *JobProgress {
	next := &JobProgress{
		Phase:   phase,
		Summary: summary,
	}
	if prev == nil {
		return next
	}

	*next = *prev
	prevSummary := strings.TrimSpace(prev.Summary)
	next.Phase = phase
	next.Summary = summary
	if strings.TrimSpace(next.Detail) == "" && prevSummary != "" && prevSummary != summary {
		next.Detail = prevSummary
	}
	return next
}

func (j *Job) updateProgress(info connector.ProgressInfo) {
	progress := &JobProgress{
		Phase:                   strings.TrimSpace(info.Phase),
		Summary:                 strings.TrimSpace(info.Summary),
		Detail:                  strings.TrimSpace(info.Detail),
		CurrentTable:            strings.TrimSpace(info.CurrentTable),
		CurrentTableIndex:       info.CurrentTableIndex,
		CompletedTables:         info.CompletedTables,
		TotalTables:             info.TotalTables,
		CurrentTableRows:        info.CurrentTableRows,
		CDCStartFile:            strings.TrimSpace(info.CDCStartFile),
		CDCStartPos:             info.CDCStartPos,
		CDCCurrentFile:          strings.TrimSpace(info.CDCCurrentFile),
		CDCCurrentPos:           info.CDCCurrentPos,
		CDCCheckpointFile:       strings.TrimSpace(info.CDCCheckpointFile),
		CDCCheckpointPos:        info.CDCCheckpointPos,
		CDCLatestFile:           strings.TrimSpace(info.CDCLatestFile),
		CDCLatestPos:            info.CDCLatestPos,
		CDCLagFiles:             info.CDCLagFiles,
		CDCEarliestFile:         strings.TrimSpace(info.CDCEarliestFile),
		CDCAvailableBinlogs:     info.CDCAvailableBinlogs,
		CDCBinlogStatus:         strings.TrimSpace(info.CDCBinlogStatus),
		CheckpointPending:       info.CheckpointPending,
		CheckpointReason:        strings.TrimSpace(info.CheckpointReason),
		CheckpointPosition:      strings.TrimSpace(info.CheckpointPosition),
		CheckpointPendingTables: strings.TrimSpace(info.CheckpointPendingTables),
		RecoveryStatus:          info.RecoveryStatus,
		RecoveryOperationID:     info.RecoveryOperationID,
		RecoveryAttempts:        info.RecoveryAttempts,
		RecoveryLastError:       info.RecoveryLastError,
		RecoveryCheckpoint:      info.RecoveryCheckpoint,
	}
	if !info.RecoveryStartedAt.IsZero() {
		progress.RecoveryStartedAt = info.RecoveryStartedAt.UTC().Format(time.RFC3339Nano)
	}

	j.mu.Lock()
	previousPhase := ""
	if j.progress != nil {
		previousPhase = j.progress.Phase
	}
	// Preflight starts a new runtime attempt. Do not carry sink/checkpoint
	// details from the previous attempt into the new one; doing so made a job
	// look as if it was still streaming while it was actually blocked in setup.
	if strings.EqualFold(strings.TrimSpace(progress.Phase), "preflight") {
		// Keep the freshly reported preflight progress as-is.
	} else if isCDCHealthRuntimeProgress(progress.Phase) && j.progress != nil {
		progress = mergeCDCHealthProgress(*j.progress, *progress)
	} else if isSinkRuntimeProgress(progress.Phase) && j.progress != nil {
		progress = mergeSinkRuntimeProgress(*j.progress, *progress)
	} else if j.progress != nil {
		progress = mergeSourceRuntimeProgress(*j.progress, *progress)
	}
	j.progress = progress
	j.Updated = time.Now()
	listener := j.progressListener
	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	j.mu.Unlock()

	if progress.Phase != "" && !strings.EqualFold(strings.TrimSpace(previousPhase), progress.Phase) {
		log.Printf("[job %s] progress phase=%s summary=%s detail=%s", jobID, progress.Phase, progress.Summary, progress.Detail)
	}
	if listener != nil {
		listener(progress)
	}
}

func isCDCHealthRuntimeProgress(phase string) bool {
	return strings.EqualFold(strings.TrimSpace(phase), "cdc_health")
}

func mergeCDCHealthProgress(previous, incoming JobProgress) *JobProgress {
	merged := previous
	// The CDC health monitor only runs after the source has entered binlog
	// streaming. Treat its heartbeat as authoritative phase evidence so a
	// missed/throttled initial streaming update cannot leave the durable UI
	// stuck on SNAPSHOT forever.
	merged.Phase = "streaming"
	if graphPhase(&previous) != "streaming" {
		merged.Summary = "CDC streaming"
		merged.Detail = "CDC health heartbeat received"
	}
	merged.CDCCheckpointFile = incoming.CDCCheckpointFile
	merged.CDCCheckpointPos = incoming.CDCCheckpointPos
	merged.CDCLatestFile = incoming.CDCLatestFile
	merged.CDCLatestPos = incoming.CDCLatestPos
	merged.CDCLagFiles = incoming.CDCLagFiles
	merged.CDCEarliestFile = incoming.CDCEarliestFile
	merged.CDCAvailableBinlogs = incoming.CDCAvailableBinlogs
	merged.CDCBinlogStatus = incoming.CDCBinlogStatus
	return &merged
}

func isSinkRuntimeProgress(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "sink_commit", "sink_commit_wait", "sink_checkpoint", "sink_checkpoint_wait", "sink_recovery":
		return true
	default:
		return false
	}
}

func mergeSinkRuntimeProgress(previous, incoming JobProgress) *JobProgress {
	merged := previous
	if strings.TrimSpace(merged.Phase) == "" || isSinkRuntimeProgress(merged.Phase) {
		merged.Phase = "running"
		if strings.TrimSpace(merged.Summary) == "" || strings.EqualFold(strings.TrimSpace(merged.Summary), strings.TrimSpace(previous.SinkSummary)) {
			merged.Summary = "Job is running"
		}
	}
	merged.SinkPhase = strings.TrimSpace(incoming.Phase)
	merged.SinkSummary = strings.TrimSpace(incoming.Summary)
	merged.SinkDetail = strings.TrimSpace(incoming.Detail)
	if strings.TrimSpace(incoming.CurrentTable) != "" {
		merged.SinkTable = strings.TrimSpace(incoming.CurrentTable)
	}
	merged.SinkRows = incoming.CurrentTableRows
	merged.CheckpointPending = incoming.CheckpointPending
	merged.CheckpointReason = incoming.CheckpointReason
	merged.CheckpointPosition = incoming.CheckpointPosition
	merged.CheckpointPendingTables = incoming.CheckpointPendingTables
	if incoming.RecoveryStatus != "" {
		copyRecoveryProgress(&merged, incoming)
	}
	return &merged
}

func copyRecoveryProgress(to *JobProgress, from JobProgress) {
	to.RecoveryStatus = from.RecoveryStatus
	to.RecoveryOperationID = from.RecoveryOperationID
	to.RecoveryAttempts = from.RecoveryAttempts
	to.RecoveryLastError = from.RecoveryLastError
	to.RecoveryCheckpoint = from.RecoveryCheckpoint
	to.RecoveryStartedAt = from.RecoveryStartedAt
}

func mergeSourceRuntimeProgress(previous, incoming JobProgress) *JobProgress {
	merged := incoming
	merged.SinkPhase = previous.SinkPhase
	merged.SinkSummary = previous.SinkSummary
	merged.SinkDetail = previous.SinkDetail
	merged.SinkTable = previous.SinkTable
	merged.SinkRows = previous.SinkRows
	copyRecoveryProgress(&merged, previous)
	merged.CheckpointPending = previous.CheckpointPending
	merged.CheckpointReason = previous.CheckpointReason
	merged.CheckpointPosition = previous.CheckpointPosition
	merged.CheckpointPendingTables = previous.CheckpointPendingTables
	if strings.TrimSpace(incoming.CDCCheckpointFile) == "" {
		merged.CDCCheckpointFile = previous.CDCCheckpointFile
		merged.CDCCheckpointPos = previous.CDCCheckpointPos
	}
	if strings.TrimSpace(incoming.CDCLatestFile) == "" {
		merged.CDCLatestFile = previous.CDCLatestFile
		merged.CDCLatestPos = previous.CDCLatestPos
		merged.CDCLagFiles = previous.CDCLagFiles
	}
	if strings.TrimSpace(incoming.CDCBinlogStatus) == "" {
		merged.CDCEarliestFile = previous.CDCEarliestFile
		merged.CDCAvailableBinlogs = previous.CDCAvailableBinlogs
		merged.CDCBinlogStatus = previous.CDCBinlogStatus
	}
	return &merged
}

func (j *Job) addError(component string, err error) {
	if err == nil {
		return
	}
	j.mu.Lock()
	j.errors = append(j.errors, JobError{
		Component: component,
		Message:   err.Error(),
		Time:      time.Now(),
	})
	j.Updated = time.Now()
	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	j.mu.Unlock()
	log.Printf("[job %s] %s error: %v", jobID, component, err)
}

func closeConnector(value any) {
	if closer, ok := value.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func (j *Job) setCancel(cancel context.CancelFunc) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cancelFunc = cancel
}

func (j *Job) setSourceCancel(cancel context.CancelFunc) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.sourceCancelFunc = cancel
}

func (j *Job) takeSourceCancel() context.CancelFunc {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.sourceCancelFunc
	j.sourceCancelFunc = nil
	return c
}

func (j *Job) setPauseRequested(requested bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.pauseRequested = requested
}

func (j *Job) isPauseRequested() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.pauseRequested
}

func (j *Job) setRunDone(done chan struct{}) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.runDone = done
}

func (j *Job) takeCancel() context.CancelFunc {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.cancelFunc
	j.cancelFunc = nil
	return c
}

func (j *Job) currentRunDone() chan struct{} {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.runDone
}

func (j *Job) waitRunDone(timeout time.Duration) bool {
	done := j.currentRunDone()
	if done == nil {
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (j *Job) runActive() bool {
	done := j.currentRunDone()
	if done == nil {
		return false
	}

	select {
	case <-done:
		return false
	default:
		return true
	}
}

func (j *Job) setStatusListener(listener func(JobStatus)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.statusListener = listener
}

func (j *Job) setProgressListener(listener func(*JobProgress)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.progressListener = listener
}

func (j *Job) markPersistenceDeleted() {
	j.mu.Lock()
	j.persistenceDeleted = true
	j.mu.Unlock()
}

func (j *Job) isPersistenceDeleted() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.persistenceDeleted
}

func (j *Job) failStart(component string, err error, cancel context.CancelFunc) error {
	if err == nil {
		return nil
	}
	j.addError(component, err)
	j.setStatus(JobStatusFailed)
	if cancel != nil {
		cancel()
	}
	j.closeMetaStore()
	return err
}

func (j *Job) recoverPipelinePanic(component string, cancel context.CancelFunc) {
	recovered := recover()
	if recovered == nil {
		return
	}

	err := fmt.Errorf("%s panic: %v", component, recovered)
	jobID := ""
	if j.Config != nil {
		jobID = j.Config.ID
	}
	log.Printf("[job %s] %s panic: %v\n%s", jobID, component, recovered, debug.Stack())
	j.addError(component, err)
	j.setStatus(JobStatusFailed)
	if cancel != nil {
		cancel()
	}
}

// requestStop makes cancellation visible immediately while the pipeline drains.
func (j *Job) requestStop() bool {
	cancel := j.takeCancel()
	if cancel != nil {
		cancel()
	}
	if sourceCancel := j.takeSourceCancel(); sourceCancel != nil {
		sourceCancel()
	}

	st := j.GetStatus()
	if st == JobStatusDone || st == JobStatusFailed {
		return false
	}
	j.setStatus(JobStatusStopped)
	return true
}

// RequestPause stops only the source. The source goroutine then closes the
// event channel, allowing the sink to drain, flush, and commit its checkpoint.
func (j *Job) RequestPause() bool {
	j.mu.Lock()
	if j.status != JobStatusRunning || j.sourceCancelFunc == nil {
		j.mu.Unlock()
		return false
	}

	sourceCancel := j.sourceCancelFunc
	j.sourceCancelFunc = nil
	j.pauseRequested = true
	previous, listener, jobID, changed := j.setStatusLocked(JobStatusPausing)
	j.mu.Unlock()

	if changed {
		j.notifyStatusTransition(jobID, previous, JobStatusPausing, listener)
	}
	sourceCancel()
	return true
}

// Stop waits for full pipeline shutdown after exposing STOPPED immediately.
func (j *Job) Stop() {
	if !j.requestStop() {
		return
	}
	if !j.waitRunDone(30 * time.Second) {
		log.Printf("[job %s] stop timeout waiting for pipeline shutdown", j.Config.ID)
	}
}

func (j *Job) StopAsync() {
	if !j.requestStop() {
		return
	}
	go func() {
		if !j.waitRunDone(30 * time.Second) {
			log.Printf("[job %s] stop timeout waiting for pipeline shutdown", j.Config.ID)
		}
	}()
}

// Check retained recovery state before removing a stopped job from the UI.
func (j *Job) checkDeleteAdmission() error {
	j.mu.RLock()
	hasStore := j.metaStore != nil
	j.mu.RUnlock()
	if !hasStore && (j.Config == nil || strings.TrimSpace(j.Config.Meta.MySQLDSN) == "") {
		return nil
	}
	store, key, err := j.checkpointReader()
	if err != nil {
		return err
	}
	checker, ok := store.(meta.JobStateCleanupChecker)
	if !ok {
		// Existing stores still enforce their transactional cleanup contract.
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, candidate := range []string{j.Config.ID, key} {
		if candidate == "" {
			continue
		}
		if err := checker.CheckJobStateCleanup(ctx, candidate); err != nil {
			return fmt.Errorf("cannot delete job_id=%s while recovery state is retained: %w; resolve the commit or use a configured snapshot rebuild after binlog purge", j.Config.ID, err)
		}
		if key == j.Config.ID {
			break
		}
	}
	return nil
}

// Delete metadata state when the drained job is removed from the manager/UI.
func (j *Job) CleanupMeta() error {
	j.mu.RLock()
	store := j.metaStore
	key := j.metaKey
	j.mu.RUnlock()
	if store == nil && key != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := j.ensureMetaStore(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("open metadata store for cleanup: %w", err)
		}
	}

	j.mu.Lock()
	store = j.metaStore
	j.metaStore = nil
	key = j.metaKey
	j.mu.Unlock()

	if store == nil {
		return nil
	}
	defer closeOffsetStore(store)
	if key == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return store.DeleteJobState(ctx, key)
}

// ---- NEW: Graph accessor ----
func (j *Job) Graph() *JobGraph {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.buildGraphLocked()
}

// ---- connector selection ----

func (j *Job) pickSource() (typ string, cfg any) {
	// v2 preferred
	if j.Config.Source != nil && strings.TrimSpace(j.Config.Source.Type) != "" {
		return strings.ToLower(strings.TrimSpace(j.Config.Source.Type)), j.Config.Source.Config
	}
	// legacy fallback
	return "mysql", j.Config.MySQL
}

func (j *Job) pickSink() (typ string, cfg any) {
	if j.Config.Sink != nil && strings.TrimSpace(j.Config.Sink.Type) != "" {
		return strings.ToLower(strings.TrimSpace(j.Config.Sink.Type)), j.Config.Sink.Config
	}
	return "doris", j.Config.Doris
}

// ---- preflight generic (capability-based) ----

func (j *Job) preflight(ctx context.Context, src connector.Source, sink connector.Sink, mode config.JobMode) error {
	if err := validateSnapshotOnlyCountResume(j.Config); err != nil {
		return err
	}
	lister, hasLister := src.(connector.TableLister)
	sp, hasSP := src.(connector.SchemaProvider)
	tm, hasTM := sink.(connector.TableManager)
	sc, hasSC := sink.(connector.SchemaConsumer)
	authoritativeSink, hasAuthoritativeInitialSnapshot := sink.(connector.AuthoritativeInitialSnapshot)
	freshAuthoritativeSnapshot := hasAuthoritativeInitialSnapshot && authoritativeSink.RequiresInitialSnapshotReset() &&
		(mode == config.JobModeInitial || mode == config.JobModeSnapshotHandoff)
	rebuildToken := j.Config.Metadata[config.IcebergRebuildFullResetKey]
	scopedToken := j.Config.Metadata[config.IcebergRebuildScopedKey]
	scopedRebuild := scopedToken != "" && scopedToken == j.SubmissionID() && (mode == config.JobModeInitial || mode == config.JobModeSnapshotHandoff)
	if scopedRebuild {
		if err := config.ValidateIcebergSnapshotRebuild(j.Config); err != nil {
			return err
		}
		freshAuthoritativeSnapshot = true
	}
	if rebuildToken != "" && rebuildToken == j.SubmissionID() && (mode == config.JobModeInitial || mode == config.JobModeSnapshotHandoff) {
		if err := config.ValidateIcebergSnapshotRebuild(j.Config); err != nil {
			return err
		}
		freshAuthoritativeSnapshot = true
	}
	safeSnapshotReload := freshAuthoritativeSnapshot ||
		(mode == config.JobModeSnapshotOnly && snapshotOnlyCountResumeEnabled(j.Config.Metadata))
	pkSkipper, skipMissingPK := sink.(connector.SnapshotPrimaryKeySkipper)
	skipMissingPK = mode == config.JobModeSnapshotOnly && skipMissingPK

	if safeSnapshotReload && !hasTM {
		return fmt.Errorf("safe snapshot reload requires sink connector table manager support")
	}
	if !hasTM && !hasSC {
		return nil
	}
	if !hasLister || !hasSP {
		return fmt.Errorf("source connector does not support schema discovery required by sink")
	}

	var targetResetter connector.TargetTableResetter
	var scopedResetter connector.ScopedSnapshotTargetResetter
	var sourceFilters connector.SnapshotFilterExpressionProvider
	var snapshotSkipper connector.SnapshotTableSkipper
	if safeSnapshotReload {
		if scopedRebuild {
			var ok bool
			scopedResetter, ok = sink.(connector.ScopedSnapshotTargetResetter)
			if !ok {
				return fmt.Errorf("scoped snapshot rebuild requires durable scoped reset support")
			}
			sourceFilters, ok = src.(connector.SnapshotFilterExpressionProvider)
			if !ok {
				return fmt.Errorf("scoped snapshot rebuild requires source filter expressions")
			}
		}
		var ok bool
		targetResetter, ok = sink.(connector.TargetTableResetter)
		if !ok {
			return fmt.Errorf("safe snapshot reload requires sink connector table reset support")
		}
	}

	var resolver connector.TargetResolver
	if tr, ok := sink.(connector.TargetResolver); ok {
		resolver = tr
	}

	type snapshotReloadTarget struct {
		schema string
		table  string
		source connector.TableRef
		filter string
	}

	reloadTargets := make(map[string]*snapshotReloadTarget)
	skipTables := make([]connector.TableRef, 0)
	for _, t := range lister.Tables() {
		if safeSnapshotReload && !scopedRebuild {
			if filtered, ok := src.(connector.SnapshotFilterProvider); ok && filtered.HasSnapshotFilter(t.Schema, t.Table) {
				return fmt.Errorf("full target reset is unsafe for source table %s.%s with a snapshot filter; disable the full reset or remove the source filter", t.Schema, t.Table)
			}
		}
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 45*time.Second)
		schema, err := sp.FetchSchema(fetchCtx, t.Schema, t.Table)
		fetchCancel()
		if err != nil {
			return fmt.Errorf("preflight fetch schema %s.%s failed: %w", t.Schema, t.Table, err)
		}
		if skipMissingPK && pkSkipper.SkipSnapshotTableWithoutPrimaryKey(t.Schema, t.Table, schema) {
			if snapshotSkipper == nil {
				var ok bool
				snapshotSkipper, ok = src.(connector.SnapshotTableSkipper)
				if !ok {
					return fmt.Errorf("snapshot-only missing primary key skip requires source connector snapshot skip support")
				}
			}
			log.Printf("[job %s] snapshot-only skip %s.%s because source table has no primary key", j.Config.ID, t.Schema, t.Table)
			skipTables = append(skipTables, t)
			continue
		}

		if hasSC {
			if err := sc.RegisterSourceSchema(t.Schema, t.Table, schema); err != nil {
				return err
			}
		}

		if hasTM {
			targetDB, targetTbl := t.Schema, t.Table
			if resolver != nil {
				targetDB, targetTbl = resolver.ResolveTarget(t.Schema, t.Table)
			}

			ensureCtx, ensureCancel := context.WithTimeout(ctx, 45*time.Second)
			err := tm.EnsureTable(ensureCtx, targetDB, targetTbl, schema)
			ensureCancel()
			if err != nil {
				return fmt.Errorf("preflight ensure table %s.%s failed: %w", targetDB, targetTbl, err)
			}

			if safeSnapshotReload {
				filter := ""
				if scopedRebuild {
					filter = sourceFilters.SnapshotFilterExpression(t.Schema, t.Table)
					if err := scopedResetter.ValidateSnapshotSourceTable(ctx, t.Schema, t.Table, filter); err != nil {
						return err
					}
				}
				targetName := strings.ToLower(strings.TrimSpace(targetDB + "." + targetTbl))
				group := reloadTargets[targetName]
				if group != nil && scopedRebuild && group.source != t {
					return fmt.Errorf("scoped snapshot rebuild does not support multiple sources sharing target %s", targetName)
				}
				if group == nil {
					group = &snapshotReloadTarget{
						schema: targetDB,
						table:  targetTbl,
						source: t,
						filter: filter,
					}
					reloadTargets[targetName] = group
				}
			}
		}
	}
	if safeSnapshotReload {
		for targetName, group := range reloadTargets {
			if scopedRebuild {
				if err := scopedResetter.ResetSnapshotSourceTable(ctx, group.source.Schema, group.source.Table, group.filter); err != nil {
					return fmt.Errorf("scoped reset target=%s: %w", targetName, err)
				}
				continue
			}
			log.Printf("[job %s] safe snapshot reload reset target=%s; resume mode uses saved snapshot cursor", j.Config.ID, targetName)
			if err := targetResetter.ResetTargetTable(ctx, group.schema, group.table); err != nil {
				return fmt.Errorf("reset target table failed %s.%s: %w", group.schema, group.table, err)
			}
		}
	}
	if snapshotSkipper != nil {
		snapshotSkipper.SkipSnapshotTables(skipTables)
	}
	return nil
}

func (j *Job) Start() error {
	return j.startWithMode(j.Config.Mode)
}

func (j *Job) Resume() error {
	return j.startWithMode(config.JobModeResume)
}

func (j *Job) startWithMode(mode config.JobMode) (err error) {
	effectiveMode := normalizeMode(mode)
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}

		err = fmt.Errorf("job start panic: %v", recovered)
		jobID := ""
		if j.Config != nil {
			jobID = j.Config.ID
		}
		log.Printf("[job %s] start panic: %v\n%s", jobID, recovered, debug.Stack())
		j.addError("system", err)
		j.setStatus(JobStatusFailed)
		if cancel := j.takeCancel(); cancel != nil {
			cancel()
		}
	}()

	st := j.GetStatus()
	if st == JobStatusRunning || st == JobStatusPending || st == JobStatusPausing {
		return nil
	}
	if st == JobStatusDone {
		return fmt.Errorf("job already DONE")
	}

	if j.registry == nil {
		err := fmt.Errorf("job registry is nil (JobManager must pass registry to NewJob)")
		return j.failStart("system", err, nil)
	}
	if err := validateSnapshotOnlyCountResume(j.Config); err != nil {
		return j.failStart("system", err, nil)
	}

	j.mu.Lock()
	j.runMode = effectiveMode
	j.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	j.setCancel(cancel)
	j.setPauseRequested(false)
	sourceCtx, sourceCancel := context.WithCancel(ctx)
	j.setSourceCancel(sourceCancel)

	// PENDING = fase setup/preflight
	j.setStatus(JobStatusPending)
	j.updateProgress(connector.ProgressInfo{
		Phase:   "preflight",
		Summary: "Preparing job",
		Detail:  "Validating source schema and target tables",
	})

	events := make(chan model.Event, j.Config.BufferSize)

	// init meta store
	if j.Config.Meta.MySQLDSN != "" {
		if _, err := j.ensureMetaStore(ctx); err != nil {
			return j.failStart("system", err, cancel)
		}
	}

	// pick connectors
	if token := j.Config.Metadata[config.IcebergRebuildScopedKey]; token != "" && token == j.SubmissionID() {
		store, err := j.ensureMetaStore(ctx)
		if err != nil {
			return j.failStart("system", err, cancel)
		}
		state, err := store.GetSnapshotState(ctx, j.ensureMetaKey())
		if err != nil {
			return j.failStart("system", err, cancel)
		}
		if state == nil {
			if err := meta.CheckScopedRebuildEmptyCheckpoint(ctx, store, j.Config.ID, j.ensureMetaKey()); err != nil {
				return j.failStart("system", err, cancel)
			}
			effectiveMode = config.JobModeSnapshotHandoff
		} else if !state.Done {
			effectiveMode = config.JobModeSnapshotHandoffResume
		}
	}
	srcType, srcCfg := j.pickSource()
	sinkType, sinkCfg := j.pickSink()

	// build new metaKey (allowed to change)
	j.mu.Lock()
	j.runMode = effectiveMode
	j.mu.Unlock()
	storedMode := normalizeMode(j.Config.Mode)
	log.Printf("[job %s] start mode=%s source=%s sink=%s", j.Config.ID, effectiveMode, srcType, sinkType)
	metaKey := j.ensureMetaKey()

	// store metakey
	j.mu.Lock()
	j.metaKey = metaKey
	runtimeMetaStore := j.metaStore
	runtimeFence := j.runtimeFence
	j.mu.Unlock()
	if runtimeFence != nil {
		var err error
		runtimeMetaStore, err = runtimeFence(runtimeMetaStore, metaKey)
		if err != nil {
			return j.failStart("worker ownership", err, cancel)
		}
		if err := meta.CheckOwnership(ctx, runtimeMetaStore); err != nil {
			return j.failStart("worker ownership", err, cancel)
		}
	}
	if effectiveMode == config.JobModeInitial || effectiveMode == config.JobModeSnapshotOnly || effectiveMode == config.JobModeSnapshotHandoff {
		if err := j.checkFreshSnapshotRecovery(ctx, runtimeMetaStore, metaKey); err != nil {
			return j.failStart("snapshot recovery", err, cancel)
		}
	}

	jctx := connector.JobContext{
		JobID:        j.Config.ID,
		SubmissionID: j.SubmissionID(),
		JobName:      j.Config.Name,
		MetaKey:      metaKey,
		Mode:         effectiveMode,
		StoredMode:   storedMode,
		SinkType:     sinkType,
		SinkConfig:   sinkCfg,
		SourceType:   srcType,
		SourceConfig: srcCfg,
		JobConfig:    j.Config,
		Retry:        j.Config.Retry,
		MetaStore:    runtimeMetaStore,
		Metadata:     j.Config.Metadata,
		ReportProgress: func(info connector.ProgressInfo) {
			j.updateProgress(info)
		},
	}

	// instantiate source/sink from registry
	src, err := j.registry.NewSource(srcType, jctx, srcCfg)
	if err != nil {
		return j.failStart("source", err, cancel)
	}
	if lister, ok := src.(connector.TableLister); ok {
		jctx.SourceTables = append([]connector.TableRef(nil), lister.Tables()...)
	}
	if resolver, ok := src.(connector.SnapshotScopeResolver); ok {
		sinkCfg, err = resolver.ResolveSnapshotScope(ctx, jctx)
		if err != nil {
			closeConnector(src)
			return j.failStart("snapshot cutoff", err, cancel)
		}
		jctx.SinkConfig = sinkCfg
	}

	sink, err := j.registry.NewSink(sinkType, jctx, sinkCfg)
	if err != nil {
		closeConnector(src)
		return j.failStart("sink", err, cancel)
	}
	if provider, ok := sink.(connector.MaintenanceOwnershipProvider); ok {
		lifecycle := provider.MaintenanceOwnershipLifecycle()
		if lifecycle != nil {
			if err := lifecycle.Reserve(ctx); err != nil {
				closeConnector(src)
				closeConnector(sink)
				return j.failStart("maintenance ownership", err, cancel)
			}
			j.mu.Lock()
			j.maintenanceOwnership = lifecycle
			j.mu.Unlock()
		}
	}
	j.updateTableMaintenanceStatus(nil)
	if reporter, ok := sink.(connector.TableMaintenanceStatusReporterSetter); ok {
		reporter.SetTableMaintenanceStatusReporter(j.updateTableMaintenanceStatus)
	}

	// build minimal graph snapshot (refreshed on-demand by Graph())
	j.mu.Lock()
	j.graph = &JobGraph{
		JobID:  j.Config.ID,
		Status: j.status,
		Nodes: []GraphNode{
			{ID: "source:" + srcType, Type: NodeSource, Label: "Source (" + srcType + ")", Status: j.status},
			{ID: "buffer:events", Type: NodeBuffer, Label: "Event Buffer", Status: j.status},
			{ID: "sink:" + sinkType, Type: NodeSink, Label: "Sink (" + sinkType + ")", Status: j.status},
		},
		Edges: []GraphEdge{
			{From: "source:" + srcType, To: "buffer:events"},
			{From: "buffer:events", To: "sink:" + sinkType},
		},
	}
	j.mu.Unlock()

	runDone := make(chan struct{})
	var runWG sync.WaitGroup
	runWG.Add(2)
	j.setRunDone(runDone)
	go func() {
		runWG.Wait()
		j.closeMetaStore()
		close(runDone)
	}()

	// source goroutine
	go func() {
		defer runWG.Done()
		defer close(events)
		defer closeConnector(src)
		defer j.recoverPipelinePanic("source", cancel)

		// Preflight ensure table (generic)
		if err := j.preflight(ctx, src, sink, effectiveMode); err != nil {
			if ctx.Err() != nil {
				return
			}
			j.addError("system", err)
			j.setStatus(JobStatusFailed)
			cancel()
			return
		}

		// Preflight sukses => RUNNING (kalau belum di-stop)
		if ctx.Err() != nil {
			return
		}
		j.setStatus(JobStatusRunning)

		if err := src.Run(sourceCtx, events); err != nil {
			// Pause cancels only the source so the sink can drain normally.
			if sourceCtx.Err() != nil && j.isPauseRequested() {
				return
			}
			// Kalau error karena cancel (Stop), jangan FAILED
			if ctx.Err() != nil {
				return
			}
			j.addError("source", err)
			j.setStatus(JobStatusFailed)
			cancel()
			return
		}
	}()

	// sink goroutine
	go func() {
		defer runWG.Done()
		defer closeConnector(sink)
		defer j.recoverPipelinePanic("sink", cancel)
		if err := sink.Run(ctx, events); err != nil {
			// kalau stop/cancel, jangan FAILED
			if ctx.Err() != nil {
				return
			}
			j.addError("sink", err)
			j.setStatus(JobStatusFailed)
			cancel()
			return
		}

		// sink selesai normal
		if ctx.Err() == nil {
			if j.isPauseRequested() {
				j.setStatus(JobStatusPaused)
			} else {
				j.setStatus(JobStatusDone)
			}
		}
	}()

	return nil
}

func normalizeMode(m config.JobMode) config.JobMode {
	switch m {
	case config.JobModeInitial, config.JobModeSnapshotOnly, config.JobModeSnapshotHandoff, config.JobModeSnapshotHandoffResume, config.JobModeResume, config.JobModeLatestOffset, config.JobModeLatest:
		return m
	default:
		return config.JobModeInitial
	}
}

func snapshotOnlyCountResumeEnabled(metadata map[string]string) bool {
	return metadataBool(metadata, "snapshot_only_count_resume")
}

func validateSnapshotOnlyCountResume(cfg *config.JobConfig) error {
	if cfg == nil || !snapshotOnlyCountResumeEnabled(cfg.Metadata) {
		return nil
	}
	if normalizeMode(cfg.Mode) != config.JobModeSnapshotOnly {
		return fmt.Errorf("metadata.snapshot_only_count_resume is only supported for mode: snapshot-only (got %s)", cfg.Mode)
	}
	return nil
}

func metadataBool(metadata map[string]string, key string) bool {
	if len(metadata) == 0 {
		return false
	}
	raw, ok := metadata[key]
	if !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on", "enabled":
		return true
	default:
		return false
	}
}
