package doris

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
	"github.com/gerinsp/rivus/pkg/observability"
	"github.com/gerinsp/rivus/pkg/schemachange"
	"github.com/gerinsp/rivus/pkg/util"
)

// =======================
// Airflow-compatible CSV
// =======================

// FIELD_SEP di file: ASCII 0x1F (Unit Separator)
const fieldSep = "\x1f"

// Representasi aman untuk HTTP header Doris
const headerFieldSep = "\\x1f"

// Escape char yang "dibersihkan" dari konten (Airflow pakai ^ untuk escape file,
// tapi di Go kita pilih: jangan pakai quoting/escaping sama sekali, jadi konten dibersihkan).
const escapeChar = "^"

const (
	streamLoadBufferSize     = 64 * 1024
	retainedBatchCapLimit    = 1024
	dorisMaxVarcharLen       = 65533
	dorisMaxDecimalPrecision = 38
	dorisDeleteMarkerColumn  = "__rivus_delete"
)

// =======================

var (
	dorisColumnNamePattern = regexp.MustCompile(`^[.a-zA-Z0-9_+\-/?@#$%^&*"\s,:]{1,256}$`)
)

type columnBinding struct {
	Source     string
	Target     string
	TargetType string
	Nullable   bool
}

type Sink struct {
	jobID      string
	stateKey   string
	offsetSto  meta.OffsetStore
	cfg        config.DorisConfig
	retry      config.RetryPolicy
	httpClient *http.Client
	sqlDB      *sql.DB

	mu             sync.RWMutex
	columns        map[string][]string        // key "db.table" -> target cols order
	columnBindings map[string][]columnBinding // key "db.table" -> source->target cols order
	primaryKeys    map[string][]string        // key "db.table" -> source primary-key columns

	maxLen   map[string]map[string]int  // "db.table" -> col -> max chars (varchar/char)
	isString map[string]map[string]bool // "db.table" -> col -> string-ish

	sendBatchOverride func(context.Context, string, string, []model.Event) error
}

type loadStatus struct {
	State  string
	Reason string
	Raw    map[string]string // opsional untuk debug
}

type dorisFlushCounter struct {
	events  int
	rows    int
	deletes int
}

func NewSink(jobID, stateKey string, cfg config.DorisConfig, retry config.RetryPolicy, offsetSto meta.OffsetStore) (*Sink, error) {
	httpClient := &http.Client{Timeout: 60 * time.Second}

	// MySQL FE host buat Exec DDL (biasanya 9030)
	host := strings.TrimPrefix(cfg.HTTPHost, "http://")
	host = strings.TrimPrefix(host, "https://")
	if i := strings.Index(host, "/"); i >= 0 {
		host = host[:i]
	}
	if i := strings.Index(host, ":"); i >= 0 {
		host = host[:i]
	}

	mysqlPort := cfg.MySQLPort
	if mysqlPort <= 0 {
		mysqlPort = 9030
	}
	mysqlFE := fmt.Sprintf("%s:%d", host, mysqlPort)

	// Doris MySQL protocol needs a database in DSN; pick a reasonable default.
	dbForDSN := cfg.DefaultDatabase
	if dbForDSN == "" {
		dbForDSN = cfg.Database
	}
	if dbForDSN == "" {
		dbForDSN = "information_schema"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4,utf8&parseTime=true&timeout=10s&readTimeout=30s&writeTimeout=30s",
		cfg.User, cfg.Password, mysqlFE, dbForDSN)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetConnMaxLifetime(20 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("doris mysql(9030) ping failed: %w", err)
	}

	return &Sink{
		jobID:          jobID,
		stateKey:       stateKey,
		offsetSto:      offsetSto,
		cfg:            cfg,
		retry:          retry,
		httpClient:     httpClient,
		sqlDB:          db,
		columns:        make(map[string][]string),
		columnBindings: make(map[string][]columnBinding),
		primaryKeys:    make(map[string][]string),
		maxLen:         make(map[string]map[string]int),
		isString:       make(map[string]map[string]bool),
	}, nil
}

func (s *Sink) Close() error {
	if s == nil || s.sqlDB == nil {
		return nil
	}
	return s.sqlDB.Close()
}

func (s *Sink) checkpointKey() string {
	if strings.TrimSpace(s.stateKey) != "" {
		return s.stateKey
	}
	return s.jobID
}

func keyCompatibleStringType(c model.TableColumn) string {
	if c.CharMaxLen != nil && *c.CharMaxLen > 0 {
		n := *c.CharMaxLen
		if n > dorisMaxVarcharLen {
			n = dorisMaxVarcharLen
		}
		return fmt.Sprintf("VARCHAR(%d)", n)
	}
	return "VARCHAR(1024)"
}

func normalizedDecimalPrecisionScale(precision, scale int64) (int64, int64) {
	if precision <= 0 {
		return 27, 9
	}
	if scale < 0 {
		scale = 0
	}
	if scale > precision {
		scale = precision
	}
	return precision, scale
}

func oversizedDecimalFallbackType(precision, scale int64) string {
	length := precision + 1 // sign
	if scale > 0 {
		length++ // decimal point
	}
	if length <= 0 {
		length = 1024
	}
	if length > dorisMaxVarcharLen {
		length = dorisMaxVarcharLen
	}
	return fmt.Sprintf("VARCHAR(%d)", length)
}

func decimalTypeForDoris(precision, scale int64) string {
	precision, scale = normalizedDecimalPrecisionScale(precision, scale)
	if precision > dorisMaxDecimalPrecision {
		return oversizedDecimalFallbackType(precision, scale)
	}
	return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
}

func mapMySQLColumnToDoris(c model.TableColumn, keyColumn bool) string {
	t := strings.ToLower(strings.TrimSpace(c.DataType))
	unsigned := strings.Contains(strings.ToLower(c.ColumnType), "unsigned")

	switch t {
	// integer family
	case "tinyint":
		if !unsigned && schemachange.MySQLColumnTypeHasWidth(c.ColumnType, "tinyint", 1) {
			return "BOOLEAN"
		}
		return "BIGINT"
	case "int", "bigint", "smallint", "mediumint", "year":
		return "BIGINT"

	// float/double
	case "float", "double", "real":
		if keyColumn {
			return "DECIMAL(27,9)"
		}
		return "DOUBLE"

	// decimal/numeric: ikut precision/scale kalau ada
	case "decimal", "numeric":
		if c.NumPrec != nil {
			scale := int64(0)
			if c.NumScale != nil {
				scale = *c.NumScale
			}
			return decimalTypeForDoris(*c.NumPrec, scale)
		}
		// fallback aman
		return "DECIMAL(27,9)"

	case "datetime", "timestamp":
		return "DATETIME"
	case "date":
		return "DATE"
	case "bool", "boolean":
		return "BOOLEAN"
	case "bit":
		if schemachange.MySQLColumnTypeHasWidth(c.ColumnType, "bit", 1) {
			return "BOOLEAN"
		}
		if keyColumn {
			return keyCompatibleStringType(c)
		}
		return "STRING"

	// char/varchar: ikut length
	case "char", "varchar":
		if c.CharMaxLen != nil && *c.CharMaxLen > 0 {
			// Doris VARCHAR ada limit max; key column tidak boleh STRING.
			if *c.CharMaxLen > dorisMaxVarcharLen {
				if keyColumn {
					return keyCompatibleStringType(c)
				}
				return "STRING"
			}
			return fmt.Sprintf("VARCHAR(%d)", *c.CharMaxLen)
		}
		// fallback
		return "VARCHAR(1024)"

	// text family: paling tepat STRING
	case "text", "tinytext", "mediumtext", "longtext":
		if keyColumn {
			return keyCompatibleStringType(c)
		}
		return "STRING"

	default:
		// Doris key column tidak boleh STRING, jadi fallback key dipaksa ke VARCHAR.
		if keyColumn {
			return keyCompatibleStringType(c)
		}
		return "STRING"
	}
}

func (s *Sink) EnsureTable(ctx context.Context, targetDB, targetTable string, schema *model.TableSchema) error {
	if len(schema.Columns) == 0 {
		return fmt.Errorf("empty schema for %s.%s", schema.SchemaName, schema.TableName)
	}
	desiredColumns := desiredDorisColumns(schema)
	sourceKey := dorisTableKey(schema.SchemaName, schema.TableName)
	targetKey := dorisTableKey(targetDB, targetTable)
	if sourceKey != "" {
		observability.RegisterSourceTable(s.jobID, sourceKey)
		observability.SetSinkType(s.jobID, sourceKey, "doris")
		observability.SetTargetTable(s.jobID, sourceKey, targetKey)
	}

	// pk/non-pk index
	pkIdx := make([]int, 0)
	nonPkIdx := make([]int, 0)
	for i := range schema.Columns {
		if schema.Columns[i].IsPK {
			pkIdx = append(pkIdx, i)
		} else {
			nonPkIdx = append(nonPkIdx, i)
		}
	}
	if len(pkIdx) == 0 {
		pkIdx = []int{0}
		tmp := nonPkIdx[:0]
		for _, i := range nonPkIdx {
			if i != 0 {
				tmp = append(tmp, i)
			}
		}
		nonPkIdx = tmp
	}

	orderedIdx := make([]int, 0, len(schema.Columns))
	orderedIdx = append(orderedIdx, pkIdx...)
	orderedIdx = append(orderedIdx, nonPkIdx...)

	key := strings.ToLower(targetDB + "." + targetTable)

	// init caches ONCE
	s.mu.Lock()
	if s.maxLen[key] == nil {
		s.maxLen[key] = make(map[string]int)
	}
	if s.isString[key] == nil {
		s.isString[key] = make(map[string]bool)
	}
	s.mu.Unlock()

	colsDef := make([]string, 0, len(orderedIdx))
	colsOrder := make([]string, 0, len(orderedIdx))
	bindings := make([]columnBinding, 0, len(orderedIdx))
	keyIndexes := make(map[int]struct{}, len(pkIdx))
	usedTargetNames := make(map[string]int, len(orderedIdx))
	for _, idx := range pkIdx {
		keyIndexes[idx] = struct{}{}
	}
	for _, i := range orderedIdx {
		c := schema.Columns[i]
		targetColName := sanitizeDorisColumnName(c.Name, len(bindings), usedTargetNames)
		_, keyColumn := keyIndexes[i]
		dorisType := mapMySQLColumnToDoris(c, keyColumn)

		// string-ish + maxLen from dorisType
		dtU := strings.ToUpper(strings.TrimSpace(dorisType))
		isStr := dtU == "STRING" || strings.HasPrefix(dtU, "VARCHAR(") || strings.HasPrefix(dtU, "CHAR(")

		ml := 0
		if strings.HasPrefix(dtU, "VARCHAR(") || strings.HasPrefix(dtU, "CHAR(") {
			l := strings.IndexByte(dtU, '(')
			r := strings.IndexByte(dtU, ')')
			if l > 0 && r > l+1 {
				if n, err := strconv.Atoi(strings.TrimSpace(dtU[l+1 : r])); err == nil && n > 0 {
					ml = n
				}
			}
		}

		s.mu.Lock()
		s.isString[key][targetColName] = isStr
		if ml > 0 {
			s.maxLen[key][targetColName] = ml
		}
		s.mu.Unlock()

		if targetColName != c.Name {
			log.Printf("[doris] sanitize source column %q -> target column %q for %s.%s", c.Name, targetColName, targetDB, targetTable)
		}

		col := fmt.Sprintf("`%s` %s", targetColName, dorisType)
		if !c.IsNullable {
			col += " NOT NULL"
		}
		colsDef = append(colsDef, col)
		colsOrder = append(colsOrder, targetColName)
		bindings = append(bindings, columnBinding{
			Source:     c.Name,
			Target:     targetColName,
			TargetType: dorisType,
			Nullable:   c.IsNullable,
		})
	}

	// UNIQUE KEY prefix
	pks := make([]string, 0, len(pkIdx))
	for i := 0; i < len(pkIdx); i++ {
		pks = append(pks, fmt.Sprintf("`%s`", bindings[i].Target))
	}

	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
		%s
		)
		UNIQUE KEY(%s)
		DISTRIBUTED BY HASH(%s) BUCKETS 10
		PROPERTIES (
		"replication_num" = "1",
		"enable_unique_key_merge_on_write" = "true"
		);`,
		targetDB,
		targetTable,
		strings.Join(colsDef, ",\n  "),
		strings.Join(pks, ","),
		pks[0],
	)

	log.Printf("[doris] ensure table: %s", ddl)

	s.mu.Lock()
	s.columns[key] = colsOrder
	s.columnBindings[key] = bindings
	primaryKeys := make([]string, 0, len(pkIdx))
	for i := 0; i < len(pkIdx); i++ {
		primaryKeys = append(primaryKeys, bindings[i].Source)
	}
	s.primaryKeys[key] = primaryKeys
	s.mu.Unlock()

	if err := util.RetryWithBackoff(ctx, s.retry, func() error {
		_, err := s.sqlDB.ExecContext(ctx, ddl)
		return err
	}); err != nil {
		return err
	}

	if err := s.reconcileTableSchemaOnce(ctx, targetDB, targetTable, desiredColumns); err != nil {
		return err
	}
	log.Printf("[doris][job %s] startup schema reconciled target=%s.%s", s.jobID, targetDB, targetTable)
	return nil
}

func (s *Sink) CountTargetRows(ctx context.Context, targetDB, targetTable string) (int64, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		quoteDorisIdentifier(targetDB),
		quoteDorisIdentifier(targetTable),
	)

	var count int64
	if err := s.sqlDB.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Sink) ResetTargetTable(ctx context.Context, targetDB, targetTable string) error {
	stmt := fmt.Sprintf(
		"TRUNCATE TABLE %s.%s",
		quoteDorisIdentifier(targetDB),
		quoteDorisIdentifier(targetTable),
	)
	return util.RetryWithBackoff(ctx, s.retry, func() error {
		_, err := s.sqlDB.ExecContext(ctx, stmt)
		return err
	})
}

func (s *Sink) applyDDL(ctx context.Context, targetDB, targetTable string, ev model.Event) error {
	changes := ev.SchemaChanges
	if changes == nil {
		var err error
		changes, err = schemachange.ParseMySQLDDL(ev.DDL)
		if err != nil {
			return err
		}
	}
	stmts, ok, reason := s.translateSchemaChangesToDoris(changes, targetDB, targetTable)
	if !ok {
		log.Printf("[doris] skip DDL: %s | reason=%s", ev.DDL, reason)
		return nil
	}

	log.Printf("[doris] apply translated DDL (%d stmt): %v", len(stmts), stmts)

	for _, st := range stmts {
		st = strings.TrimSpace(st)
		if st == "" {
			continue
		}
		// Retry only the failed statement so a later failure does not replay
		// earlier ADD/DROP/RENAME operations that have already succeeded.
		if err := util.RetryWithBackoff(ctx, s.retry, func() error {
			if err := s.execDDL(ctx, st); err != nil {
				return fmt.Errorf("exec ddl failed: %w | stmt=%s", err, st)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

type streamLoadResp struct {
	Status               string `json:"Status"`
	Message              string `json:"Message"`
	Label                string `json:"Label"`
	NumberTotalRows      int64  `json:"NumberTotalRows"`
	NumberLoadedRows     int64  `json:"NumberLoadedRows"`
	NumberFilteredRows   int64  `json:"NumberFilteredRows"`
	NumberUnselectedRows int64  `json:"NumberUnselectedRows"`
	ErrorURL             string `json:"ErrorURL,omitempty"`
}

func sanitizeCell(v string) string {
	if v == "" {
		return v
	}

	v = strings.ReplaceAll(v, "\r\n", " ")
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "\r", " ")

	v = strings.ReplaceAll(v, fieldSep, " ")
	v = strings.ReplaceAll(v, `"`, `'`)
	v = strings.ReplaceAll(v, escapeChar, " ")

	return v
}

func quoteDorisIdentifier(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "`", "``")
	return "`" + name + "`"
}

func sanitizeDorisColumnName(name string, ordinal int, used map[string]int) string {
	name = strings.TrimSpace(name)

	base := name
	if name == "" || !dorisColumnNamePattern.MatchString(name) {
		var b strings.Builder
		b.Grow(len(name))
		lastUnderscore := false
		for _, r := range name {
			switch {
			case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_':
				b.WriteRune(r)
				lastUnderscore = false
			default:
				if !lastUnderscore {
					b.WriteByte('_')
					lastUnderscore = true
				}
			}
		}
		base = strings.Trim(b.String(), "_")
	}
	if base == "" {
		base = fmt.Sprintf("col_%d", ordinal+1)
	}
	if len(base) > 256 {
		base = strings.TrimRight(base[:256], "_")
		if base == "" {
			base = fmt.Sprintf("col_%d", ordinal+1)
		}
	}
	if used == nil {
		return base
	}

	candidate := base
	for suffix := 2; ; suffix++ {
		key := strings.ToLower(candidate)
		if _, exists := used[key]; !exists {
			used[key] = 1
			return candidate
		}

		next := fmt.Sprintf("%s_%d", base, suffix)
		if len(next) > 256 {
			maxBaseLen := 256 - len(fmt.Sprintf("_%d", suffix))
			if maxBaseLen < 1 {
				maxBaseLen = 1
			}
			trimmed := base
			if len(trimmed) > maxBaseLen {
				trimmed = strings.TrimRight(trimmed[:maxBaseLen], "_")
				if trimmed == "" {
					trimmed = "col"
				}
			}
			next = fmt.Sprintf("%s_%d", trimmed, suffix)
		}
		candidate = next
	}
}

func buildColumnsHeader(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted = append(quoted, quoteDorisIdentifier(c))
	}
	return strings.Join(quoted, ",")
}

func parseRedirectHostOverride(raw string) (string, int) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", 0
	}

	candidate := raw
	if !strings.Contains(candidate, "://") {
		candidate = "//" + candidate
	}

	u, err := url.Parse(candidate)
	if err == nil && u.Host != "" {
		host := strings.TrimSpace(u.Hostname())
		if host == "" {
			return "", 0
		}
		if port := strings.TrimSpace(u.Port()); port != "" {
			if n, convErr := strconv.Atoi(port); convErr == nil && n > 0 {
				return host, n
			}
		}
		return host, 0
	}

	return raw, 0
}

func joinHostPort(host string, port int) string {
	if port > 0 {
		return net.JoinHostPort(host, strconv.Itoa(port))
	}
	return host
}

func rewriteStreamLoadRedirectURL(rawURL *url.URL, cfg config.DorisConfig) string {
	if rawURL == nil {
		return ""
	}

	overrideHost, overrideHostPort := parseRedirectHostOverride(cfg.BEHTTPHost)
	if overrideHost == "" && cfg.BEHTTPPort <= 0 {
		return rawURL.String()
	}

	host := rawURL.Hostname()
	if overrideHost != "" {
		host = overrideHost
	} else if feHost, _ := parseRedirectHostOverride(cfg.HTTPHost); feHost != "" {
		host = feHost
	}

	port := 0
	if rawPort := strings.TrimSpace(rawURL.Port()); rawPort != "" {
		if n, err := strconv.Atoi(rawPort); err == nil && n > 0 {
			port = n
		}
	}
	if overrideHostPort > 0 {
		port = overrideHostPort
	}
	if cfg.BEHTTPPort > 0 {
		port = cfg.BEHTTPPort
	}

	cloned := *rawURL
	cloned.Host = joinHostPort(host, port)
	return cloned.String()
}

func (s *Sink) getColumnRuntimeMeta(targetKey string, cols []string) ([]bool, []int) {
	s.mu.RLock()
	stringMap := s.isString[targetKey]
	maxLenMap := s.maxLen[targetKey]
	s.mu.RUnlock()

	isString := make([]bool, len(cols))
	maxLen := make([]int, len(cols))
	for i, c := range cols {
		if stringMap != nil {
			isString[i] = stringMap[c]
		}
		if maxLenMap != nil {
			maxLen[i] = maxLenMap[c]
		}
	}
	return isString, maxLen
}

func (s *Sink) writeBatchPayload(
	w io.Writer,
	bindings []columnBinding,
	isString []bool,
	maxLen []int,
	batch []model.Event,
) error {
	bw := bufio.NewWriterSize(w, streamLoadBufferSize)

	for _, ev := range batch {
		for i, binding := range bindings {
			if i > 0 {
				if err := bw.WriteByte(fieldSep[0]); err != nil {
					return err
				}
			}

			val, ok := normalizeDorisValueForColumn(ev.Data[binding.Source], binding)
			if !ok {
				if _, err := bw.WriteString("\\N"); err != nil {
					return err
				}
				continue
			}

			if val != "" {
				if isString[i] {
					val = sanitizeCell(val)
					val = normalizeToASCII(val)
					if ml := maxLen[i]; ml > 0 {
						val = truncateChars(val, ml) // ASCII-only => aman byte==char
					}
				} else {
					val = sanitizeCell(val)
				}

				if _, err := bw.WriteString(val); err != nil {
					return err
				}
			}
		}

		if len(bindings) > 0 {
			if err := bw.WriteByte(fieldSep[0]); err != nil {
				return err
			}
		}
		deleteMarker := "0"
		if ev.Type == model.EventTypeDelete {
			deleteMarker = "1"
		}
		if _, err := bw.WriteString(deleteMarker); err != nil {
			return err
		}

		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}

	return bw.Flush()
}

func resetBatch(batch []model.Event) []model.Event {
	if len(batch) == 0 {
		if cap(batch) > retainedBatchCapLimit {
			return nil
		}
		return batch[:0]
	}

	if cap(batch) > retainedBatchCapLimit {
		return nil
	}

	clear(batch)
	return batch[:0]
}

func eventTraceID(ev model.Event) string {
	if strings.TrimSpace(ev.TraceID) == "" {
		return "-"
	}
	return ev.TraceID
}

func eventSourceOffsetText(ev model.Event) string {
	if ev.SourceOffset == nil || ev.SourceOffset.BinlogPos == 0 {
		return "-"
	}
	file := strings.TrimSpace(ev.SourceOffset.BinlogFile)
	if file == "" {
		file = "unknown-binlog"
	}
	return fmt.Sprintf("%s:%d", file, ev.SourceOffset.BinlogPos)
}

func batchTraceSummary(batch []model.Event) string {
	if len(batch) == 0 {
		return "-"
	}

	traces := make([]string, 0, 5)
	for _, ev := range batch {
		if strings.TrimSpace(ev.TraceID) == "" {
			continue
		}
		traces = append(traces, ev.TraceID)
		if len(traces) >= 5 {
			break
		}
	}
	if len(traces) == 0 {
		return "-"
	}
	out := strings.Join(traces, ",")
	if len(batch) > len(traces) {
		out += ",..."
	}
	return out
}

type preparedDorisEvent struct {
	event model.Event
	pos   int
}

// prepareBatch converts sink-independent CDC semantics into Doris row
// operations. In particular, a MySQL UPDATE that changes any primary-key
// column must delete the old key before upserting the AFTER image. It also
// collapses multiple operations for one key so a Stream Load contains one
// deterministic final operation per key.
func (s *Sink) prepareBatch(targetDB, targetTable string, batch []model.Event) ([]model.Event, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	targetKey := strings.ToLower(targetDB + "." + targetTable)
	s.mu.RLock()
	pkCols := append([]string(nil), s.primaryKeys[targetKey]...)
	s.mu.RUnlock()
	if len(pkCols) == 0 {
		return batch, nil
	}

	preparedByKey := make(map[string]preparedDorisEvent, len(batch))
	pos := 0
	remember := func(ev model.Event) error {
		key, err := dorisEventKey(ev.Data, pkCols)
		if err != nil {
			return fmt.Errorf("prepare Doris %s for %s: %w", ev.Type, targetKey, err)
		}
		preparedByKey[key] = preparedDorisEvent{event: ev, pos: pos}
		pos++
		return nil
	}

	for _, ev := range batch {
		if ev.Type == model.EventTypeUpdate && ev.OldData != nil {
			oldKey, err := dorisEventKey(ev.OldData, pkCols)
			if err != nil {
				return nil, fmt.Errorf("prepare Doris UPDATE old key for %s: %w", targetKey, err)
			}
			newKey, err := dorisEventKey(ev.Data, pkCols)
			if err != nil {
				return nil, fmt.Errorf("prepare Doris UPDATE new key for %s: %w", targetKey, err)
			}
			if oldKey != newKey {
				deleteEvent := ev
				deleteEvent.Type = model.EventTypeDelete
				deleteEvent.Data = ev.OldData
				deleteEvent.OldData = nil
				if err := remember(deleteEvent); err != nil {
					return nil, err
				}
			}
		}
		if err := remember(ev); err != nil {
			return nil, err
		}
	}

	prepared := make([]preparedDorisEvent, 0, len(preparedByKey))
	for _, ev := range preparedByKey {
		prepared = append(prepared, ev)
	}
	sort.Slice(prepared, func(i, j int) bool { return prepared[i].pos < prepared[j].pos })

	out := make([]model.Event, 0, len(prepared))
	for _, ev := range prepared {
		out = append(out, ev.event)
	}
	return out, nil
}

func dorisEventKey(row map[string]interface{}, pkCols []string) (string, error) {
	if row == nil {
		return "", fmt.Errorf("row is nil")
	}

	values := make([]interface{}, 0, len(pkCols))
	for _, col := range pkCols {
		value, ok := row[col]
		if !ok {
			return "", fmt.Errorf("primary-key column %q is missing", col)
		}
		if value == nil {
			return "", fmt.Errorf("primary-key column %q is NULL", col)
		}
		values = append(values, value)
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("encode primary key: %w", err)
	}
	return string(raw), nil
}

func (s *Sink) sendBatch(ctx context.Context, targetDB, targetTable string, batch []model.Event) error {
	if len(batch) == 0 {
		return nil
	}

	bindings := s.getColumnBindingsForTarget(ctx, targetDB, targetTable, batch[0])
	cols := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		cols = append(cols, binding.Target)
	}
	targetKey := strings.ToLower(targetDB + "." + targetTable)
	isString, maxLen := s.getColumnRuntimeMeta(targetKey, cols)
	url := fmt.Sprintf("%s/api/%s/%s/_stream_load", s.cfg.HTTPHost, targetDB, targetTable)
	label := fmt.Sprintf("gosync_%d", time.Now().UnixNano())
	columnsHeader := buildColumnsHeader(append(append([]string(nil), cols...), dorisDeleteMarkerColumn))
	var payload bytes.Buffer
	if err := s.writeBatchPayload(&payload, bindings, isString, maxLen, batch); err != nil {
		return err
	}
	bodyBytes := payload.Bytes()
	traces := batchTraceSummary(batch)

	log.Printf("[doris][job %s] stream_load start label=%s target=%s.%s rows=%d traces=%s",
		s.jobID, label, targetDB, targetTable, len(batch), traces)

	start := time.Now()
	err := util.RetryWithBackoff(ctx, s.retry, func() error {
		targetURL := url
		var resp *http.Response
		var body []byte
		var err error

		for redirects := 0; redirects < 3; redirects++ {
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(bodyBytes))
			if reqErr != nil {
				return reqErr
			}

			// Handle Doris FE -> BE redirects ourselves so auth/header/body stay intact.
			req.GetBody = nil
			req.SetBasicAuth(s.cfg.User, s.cfg.Password)
			req.Header.Set("label", label)
			req.Header.Set("format", "csv")
			req.Header.Set("columns", columnsHeader)
			req.Header.Set("merge_type", "MERGE")
			req.Header.Set("delete", dorisDeleteMarkerColumn+" = 1")
			req.Header.Set("strict_mode", "true")
			req.Header.Set("column_separator", headerFieldSep)
			req.Header.Set("null_value", "\\N")
			req.Header.Set("Expect", "100-continue")
			req.Header.Set("Content-Type", "text/csv")

			resp, err = s.httpClient.Do(req)
			if err != nil {
				return err
			}

			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusTemporaryRedirect && resp.StatusCode != http.StatusPermanentRedirect {
				break
			}

			nextURL, locErr := resp.Location()
			if locErr != nil {
				return fmt.Errorf("doris stream load redirect without valid Location header: %w", locErr)
			}
			rewrittenURL := rewriteStreamLoadRedirectURL(nextURL, s.cfg)
			if rewrittenURL != nextURL.String() {
				log.Printf("[doris] rewrite stream load redirect %s -> %s", nextURL.String(), rewrittenURL)
			}
			targetURL = rewrittenURL
		}

		if resp == nil {
			return fmt.Errorf("doris stream load did not produce a response")
		}
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
			return fmt.Errorf("doris stream load too many redirects for %s", targetTable)
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err := fmt.Errorf("doris stream load http=%s body=%s", resp.Status, string(body))
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return util.Permanent(err)
			}
			return err
		}

		var r streamLoadResp
		if err := json.Unmarshal(body, &r); err != nil {
			return fmt.Errorf("doris stream load bad json body=%s", string(body))
		}

		if strings.ToLower(strings.TrimSpace(r.Status)) != "success" {
			// Option B: kalau label sudah ada, jangan fail.
			// Poll status label tersebut sampai FINISHED/CANCELLED.
			if isLabelAlreadyExists(r.Status, r.Message) {
				log.Printf("[doris] label already exists, polling label=%s (db=%s)", label, targetDB)
				return s.waitLoadLabelDone(ctx, targetDB, label)
			}

			return util.Permanent(fmt.Errorf(
				"doris stream load not success: status=%s msg=%s total=%d loaded=%d filtered=%d label=%s error_url=%s body=%s",
				r.Status, r.Message, r.NumberTotalRows, r.NumberLoadedRows, r.NumberFilteredRows, r.Label, r.ErrorURL, string(body),
			))
		}

		if err := validateStreamLoadCounts(r, len(batch)); err != nil {
			return util.Permanent(fmt.Errorf(
				"doris stream load incomplete: %w label=%s target=%s.%s error_url=%s body=%s",
				err, r.Label, targetDB, targetTable, r.ErrorURL, string(body),
			))
		}

		log.Printf("[doris][job %s] stream_load success label=%s target=%s.%s total=%d loaded=%d traces=%s",
			s.jobID, r.Label, targetDB, targetTable, r.NumberTotalRows, r.NumberLoadedRows, traces)

		return nil
	})
	if err != nil {
		return err
	}
	s.recordDorisSinkFlush(batch, dorisTableKey(targetDB, targetTable), time.Since(start))
	return nil
}

func validateStreamLoadCounts(resp streamLoadResp, expectedRows int) error {
	if resp.NumberFilteredRows != 0 {
		return fmt.Errorf("filtered rows=%d", resp.NumberFilteredRows)
	}
	if resp.NumberUnselectedRows != 0 {
		return fmt.Errorf("unselected rows=%d", resp.NumberUnselectedRows)
	}
	expected := int64(expectedRows)
	if resp.NumberTotalRows != expected {
		return fmt.Errorf("total rows=%d expected=%d", resp.NumberTotalRows, expected)
	}
	if resp.NumberLoadedRows != expected {
		return fmt.Errorf("loaded rows=%d expected=%d", resp.NumberLoadedRows, expected)
	}
	return nil
}

func (s *Sink) recordDorisSinkFlush(batch []model.Event, targetTable string, duration time.Duration) {
	bySource := make(map[string]dorisFlushCounter)
	for _, ev := range batch {
		sourceTable := dorisTableKey(ev.Schema, ev.Table)
		if sourceTable == "" {
			continue
		}
		counter := bySource[sourceTable]
		counter.events++
		counter.rows++
		if ev.Type == model.EventTypeDelete {
			counter.deletes++
		}
		bySource[sourceTable] = counter
	}

	for sourceTable, counter := range bySource {
		observability.RegisterSourceTable(s.jobID, sourceTable)
		observability.SetSinkType(s.jobID, sourceTable, "doris")
		observability.SetTargetTable(s.jobID, sourceTable, targetTable)
		observability.RecordSinkFlush(s.jobID, sourceTable, targetTable, "stream", "stream_load", counter.events, counter.rows, counter.deletes, duration)
	}
}

func (s *Sink) sendBatchForRun(ctx context.Context, targetDB, targetTable string, batch []model.Event) error {
	prepared, err := s.prepareBatch(targetDB, targetTable, batch)
	if err != nil {
		return err
	}
	if s.sendBatchOverride != nil {
		return s.sendBatchOverride(ctx, targetDB, targetTable, prepared)
	}
	return s.sendBatch(ctx, targetDB, targetTable, prepared)
}

type dorisRunState struct {
	sink                 *Sink
	batches              map[string][]model.Event
	pendingOffset        *model.SourceOffset
	pendingOffsetTraceID string
}

func newDorisRunState(sink *Sink) *dorisRunState {
	return &dorisRunState{
		sink:    sink,
		batches: make(map[string][]model.Event),
	}
}

func (state *dorisRunState) rememberOffset(off *model.SourceOffset, traceID string) {
	if !off.Valid() {
		return
	}
	cp := *off
	state.pendingOffset = &cp
	state.pendingOffsetTraceID = traceID
}

func (state *dorisRunState) allBatchesEmpty() bool {
	for _, batch := range state.batches {
		if len(batch) > 0 {
			return false
		}
	}
	return true
}

func (state *dorisRunState) commitPendingOffset(commitCtx context.Context) error {
	if state.pendingOffset == nil || state.sink.offsetSto == nil || !state.allBatchesEmpty() {
		return nil
	}

	saveCtx, cancel := context.WithTimeout(commitCtx, 2*time.Second)
	defer cancel()

	if err := connector.SaveSourceOffset(saveCtx, state.sink.offsetSto, state.sink.checkpointKey(), state.pendingOffset); err != nil {
		log.Printf("[doris][job %s] save offset error pos=%s:%d: %v", state.sink.jobID, state.pendingOffset.BinlogFile, state.pendingOffset.BinlogPos, err)
		return nil
	}
	log.Printf("[doris][job %s] committed offset trace_id=%s pos=%s:%d", state.sink.jobID, state.pendingOffsetTraceID, state.pendingOffset.BinlogFile, state.pendingOffset.BinlogPos)
	state.pendingOffset = nil
	state.pendingOffsetTraceID = ""
	return nil
}

func (state *dorisRunState) flushAll(flushCtx context.Context) error {
	for key, batch := range state.batches {
		if len(batch) == 0 {
			continue
		}
		db, table, ok := splitDBTable(key)
		if !ok {
			continue
		}
		if err := state.sink.sendBatchForRun(flushCtx, db, table, batch); err != nil {
			return err
		}
		log.Printf("[doris][job %s] flushed target=%s rows=%d traces=%s", state.sink.jobID, key, len(batch), batchTraceSummary(batch))
		state.batches[key] = resetBatch(batch)
	}
	return state.commitPendingOffset(flushCtx)
}

func (state *dorisRunState) handleEvent(procCtx context.Context, ev model.Event) error {
	if ev.Type == model.EventTypeSnapshotStart || ev.Type == model.EventTypeSnapshotComplete {
		var err error
		if ev.Type == model.EventTypeSnapshotComplete {
			err = state.flushAll(procCtx)
			if err == nil {
				err = connector.ConfirmSnapshotCompletion(procCtx, state.sink.offsetSto, state.sink.checkpointKey())
			}
		}
		if ev.Ack != nil {
			ev.Ack <- err
			close(ev.Ack)
		}
		return err
	}
	if ev.Type == model.EventTypeCheckpoint {
		// Checkpoints can arrive after every binlog event. Coalesce them and
		// persist only at a normal flush boundary so they do not turn each row
		// into a separate Doris Stream Load.
		state.rememberOffset(ev.SourceOffset, eventTraceID(ev))
		return nil
	}
	if ev.Type == model.EventTypeSnapshotBatch {
		return state.handleSnapshotBatch(procCtx, ev)
	}

	targetDB, targetTable := state.sink.resolveTarget(ev.Schema, ev.Table)
	targetKey := strings.ToLower(targetDB + "." + targetTable)

	if ev.Type == model.EventTypeDDL {
		if err := state.flushAll(procCtx); err != nil {
			return err
		}
		if err := state.sink.applyDDL(procCtx, targetDB, targetTable, ev); err != nil {
			log.Printf("[doris] DDL error: %v", err)
			return err
		}
		return nil
	}

	batch := append(state.batches[targetKey], ev)
	state.batches[targetKey] = batch
	log.Printf("[doris][job %s] buffered trace_id=%s action=%s source=%s.%s target=%s batch_rows=%d source_pos=%s",
		state.sink.jobID, eventTraceID(ev), ev.Type, ev.Schema, ev.Table, targetKey, len(batch), eventSourceOffsetText(ev))

	if len(batch) >= state.sink.cfg.BatchSize {
		db, table, _ := splitDBTable(targetKey)
		if err := state.sink.sendBatchForRun(procCtx, db, table, batch); err != nil {
			log.Printf("[doris] flush error: %v", err)
			return err
		}
		log.Printf("[doris][job %s] flushed target=%s rows=%d traces=%s", state.sink.jobID, targetKey, len(batch), batchTraceSummary(batch))
		state.batches[targetKey] = resetBatch(batch)
		if err := state.commitPendingOffset(procCtx); err != nil {
			return err
		}
	}
	return nil
}

func (state *dorisRunState) handleSnapshotBatch(ctx context.Context, ev model.Event) (err error) {
	if ev.Ack != nil {
		defer func() {
			ev.Ack <- err
			close(ev.Ack)
		}()
	}
	if len(ev.Rows) == 0 {
		return nil
	}

	if err := state.flushAll(ctx); err != nil {
		return err
	}

	targetDB, targetTable := state.sink.resolveTarget(ev.Schema, ev.Table)
	batchSize := state.sink.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = len(ev.Rows)
	}
	for start := 0; start < len(ev.Rows); start += batchSize {
		end := start + batchSize
		if end > len(ev.Rows) {
			end = len(ev.Rows)
		}

		batch := make([]model.Event, 0, end-start)
		for _, row := range ev.Rows[start:end] {
			batch = append(batch, model.Event{
				Type:      model.EventTypeInsert,
				Schema:    ev.Schema,
				Table:     ev.Table,
				Data:      row,
				Timestamp: ev.Timestamp,
				Origin:    model.EventOriginSnapshot,
			})
		}
		if err := state.sink.sendBatchForRun(ctx, targetDB, targetTable, batch); err != nil {
			return err
		}
		log.Printf("[doris][job %s] snapshot batch flushed target=%s.%s rows=%d offset=%d",
			state.sink.jobID, targetDB, targetTable, len(batch), ev.SnapshotStartOffset+int64(start))
	}
	return state.commitPendingOffset(ctx)
}

func (s *Sink) Run(ctx context.Context, in <-chan model.Event) error {
	state := newDorisRunState(s)
	flushTicker := time.NewTicker(time.Duration(s.cfg.FlushSeconds) * time.Second)
	defer flushTicker.Stop()

	drainAfterCancel := func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		log.Printf("[doris] context cancelled, draining buffered events before stop")
		for {
			select {
			case <-shutdownCtx.Done():
				return shutdownCtx.Err()
			case ev, ok := <-in:
				if !ok {
					return state.flushAll(shutdownCtx)
				}
				if err := state.handleEvent(shutdownCtx, ev); err != nil {
					return err
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			if err := drainAfterCancel(); err != nil {
				log.Printf("[doris] drain on cancel error: %v", err)
			}
			return ctx.Err()

		case <-flushTicker.C:
			if err := state.flushAll(ctx); err != nil {
				log.Printf("[doris] flush error: %v", err)
				return err
			}

		case ev, ok := <-in:
			if !ok {
				if err := state.flushAll(ctx); err != nil {
					return err
				}
				return nil
			}
			if err := state.handleEvent(ctx, ev); err != nil {
				return err
			}
		}
	}
}

func (s *Sink) resolveTarget(srcDB, srcTable string) (string, string) {
	srcDB = strings.TrimSpace(srcDB)
	srcTable = strings.TrimSpace(srcTable)
	key := strings.ToLower(srcDB + "." + srcTable)

	if s.cfg.Overrides != nil {
		if t, ok := s.cfg.Overrides[key]; ok {
			db := t.Database
			if db == "" {
				db = srcDB
			}
			tbl := t.Table
			if tbl == "" {
				tbl = srcTable
			}
			return db, tbl
		}

		if t, ok := s.cfg.Overrides[strings.ToLower(srcDB+".*")]; ok {
			db := t.Database
			if db == "" {
				db = srcDB
			}
			tbl := t.Table
			if tbl == "" || tbl == "*" {
				tbl = srcTable
			}
			return db, tbl
		}
	}

	if s.cfg.DefaultDatabase != "" {
		return s.cfg.DefaultDatabase, srcTable
	}

	return srcDB, srcTable
}

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
	return db, tbl, true
}

func dorisTableKey(schema, table string) string {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	if schema == "" {
		return strings.ToLower(table)
	}
	if table == "" {
		return strings.ToLower(schema)
	}
	return strings.ToLower(schema + "." + table)
}

func (s *Sink) getColumnBindingsForTarget(ctx context.Context, targetDB, targetTable string, ev model.Event) []columnBinding {
	key := strings.ToLower(targetDB + "." + targetTable)

	// 1) cache hit (paling ideal: dari EnsureTable / schema)
	s.mu.RLock()
	if bindings, ok := s.columnBindings[key]; ok && len(bindings) > 0 {
		out := make([]columnBinding, len(bindings))
		copy(out, bindings)
		s.mu.RUnlock()
		return out
	}
	s.mu.RUnlock()

	// 2) coba ambil dari Doris (DESC) lalu cache
	if tableColumns, err := s.fetchDorisTableColumns(ctx, targetDB, targetTable); err == nil && len(tableColumns) > 0 {
		bindings := make([]columnBinding, 0, len(tableColumns))
		cols := make([]string, 0, len(tableColumns))
		for _, col := range tableColumns {
			cols = append(cols, col.Name)
			bindings = append(bindings, columnBinding{
				Source:     col.Name,
				Target:     col.Name,
				TargetType: col.Type,
				Nullable:   col.IsNullable,
			})
		}
		s.mu.Lock()
		s.columns[key] = cols
		s.columnBindings[key] = bindings
		s.mu.Unlock()

		out := make([]columnBinding, len(bindings))
		copy(out, bindings)
		return out
	}

	// 3) last resort: deterministik dari event (stabil utk event yang sama)
	// NOTE: ini hanya supaya jalan, tapi tidak seaman "DESC table".
	cols := make([]string, 0, len(ev.Data))
	for k := range ev.Data {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	usedTargetNames := make(map[string]int, len(cols))
	bindings := make([]columnBinding, 0, len(cols))
	for i, col := range cols {
		targetColName := sanitizeDorisColumnName(col, i, usedTargetNames)
		bindings = append(bindings, columnBinding{Source: col, Target: targetColName})
	}
	return bindings
}

func (s *Sink) fetchColumnsFromDoris(ctx context.Context, db, table string) ([]string, error) {
	tableColumns, err := s.fetchDorisTableColumns(ctx, db, table)
	if err != nil {
		return nil, err
	}
	cols := make([]string, 0, len(tableColumns))
	for _, col := range tableColumns {
		cols = append(cols, col.Name)
	}
	return cols, nil
}

func normalizeDorisValue(v any) (string, bool) {
	if v == nil {
		return "", false
	}

	var s string
	switch x := v.(type) {
	case []byte:
		s = string(x)
	default:
		s = fmt.Sprintf("%v", x)
	}
	s = strings.TrimSpace(s)

	// zero DATETIME / DATE dari MySQL.
	// MySQL bisa menyimpan "0000-00-00" / "0000-00-00 00:00:00" sebagai "zero date".
	// Doris tidak menerima nilai ini untuk kolom NOT NULL, jadi kita map ke tanggal default yang valid.
	if s == "0000-00-00" {
		return "1970-01-01", true
	}
	if s == "0000-00-00 00:00:00" {
		return "1970-01-01 00:00:00", true
	}

	return s, true
}

func normalizeDorisValueForColumn(v any, binding columnBinding) (string, bool) {
	s, ok := dorisValueText(v)
	if !ok {
		return "", false
	}
	if !isDorisTemporalType(binding.TargetType) {
		return normalizeDorisValue(v)
	}

	fixed, partial := util.NormalizeMySQLPartialDate(s)
	if !partial {
		return s, true
	}
	if binding.Nullable {
		return "", false
	}
	return fixed, true
}

func dorisValueText(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	if raw, ok := v.([]byte); ok {
		return strings.TrimSpace(string(raw)), true
	}
	return strings.TrimSpace(fmt.Sprintf("%v", v)), true
}

func isDorisTemporalType(raw string) bool {
	t := strings.ToUpper(strings.TrimSpace(raw))
	return t == "DATE" || t == "DATEV2" || strings.HasPrefix(t, "DATETIME")
}

// ---- Stream Load label status polling (Option B) ----

func isLabelAlreadyExists(status, msg string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	m := strings.ToLower(msg)

	// Doris biasanya: Status="Label Already Exists"
	// Message mengandung: "[LABEL_ALREADY_EXISTS]"
	if strings.Contains(s, "label already exists") {
		return true
	}
	if strings.Contains(m, "label_already_exists") || strings.Contains(m, "label already exists") {
		return true
	}
	return false
}

// Doris SHOW LOAD output bisa beda versi.
// Kita scan pakai columns dan ambil kolom "State" (atau "state") secara dinamis.
func (s *Sink) getLoadStateByLabel(ctx context.Context, db, label string) (state string, found bool, err error) {
	// Aman pakai backtick untuk db
	q := fmt.Sprintf("SHOW LOAD FROM `%s` WHERE Label = '%s'", db, strings.ReplaceAll(label, "'", "''"))

	rows, err := s.sqlDB.QueryContext(ctx, q)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", false, err
	}

	stateIdx := -1
	for i, c := range cols {
		if strings.EqualFold(c, "State") {
			stateIdx = i
			break
		}
	}
	if stateIdx < 0 {
		// fallback: beberapa versi pakai "state"
		for i, c := range cols {
			if strings.EqualFold(c, "state") {
				stateIdx = i
				break
			}
		}
	}
	if stateIdx < 0 {
		return "", false, fmt.Errorf("SHOW LOAD missing State column; cols=%v", cols)
	}

	// Scan 1 row pertama saja (label biasanya unik)
	for rows.Next() {
		vals := make([]any, len(cols))
		bufs := make([]sql.RawBytes, len(cols))
		for i := range vals {
			vals[i] = &bufs[i]
		}

		if err := rows.Scan(vals...); err != nil {
			return "", false, err
		}

		st := strings.TrimSpace(string(bufs[stateIdx]))
		if st == "" {
			return "", true, nil
		}
		return st, true, nil
	}

	if err := rows.Err(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

// Tunggu sampai label selesai.
// - FINISHED => sukses
// - CANCELLED => error
// - lainnya => polling sampai timeout ctx atau maxWait
func (s *Sink) waitLoadLabelDone(ctx context.Context, db, label string) error {
	const maxWait = 5 * time.Minute

	deadline := time.Now().Add(maxWait)
	sleep := 1 * time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting doris load label=%s", label)
		}

		ls, found, err := s.getLoadStatusByLabel(ctx, db, label)
		if err != nil {
			log.Printf("[doris] wait label=%s: show load error: %v", label, err)
			time.Sleep(sleep)
			if sleep < 5*time.Second {
				sleep += 500 * time.Millisecond
			}
			continue
		}
		if !found {
			time.Sleep(sleep)
			continue
		}

		up := strings.ToUpper(strings.TrimSpace(ls.State))
		switch up {
		case "FINISHED":
			log.Printf("[doris] label=%s finished (treat as success)", label)
			return nil
		case "CANCELLED":
			// kalau reason kosong, tetap kasih minimal info raw keys biar gampang debug
			if strings.TrimSpace(ls.Reason) != "" {
				return fmt.Errorf("doris load cancelled label=%s reason=%s", label, ls.Reason)
			}
			return fmt.Errorf("doris load cancelled label=%s (no reason column found)", label)
		default:
			time.Sleep(sleep)
			if sleep < 5*time.Second {
				sleep += 500 * time.Millisecond
			}
		}
	}
}

func (s *Sink) getLoadStatusByLabel(ctx context.Context, db, label string) (st loadStatus, found bool, err error) {
	q := fmt.Sprintf("SHOW LOAD FROM `%s` WHERE Label = '%s'", db, strings.ReplaceAll(label, "'", "''"))

	rows, err := s.sqlDB.QueryContext(ctx, q)
	if err != nil {
		return loadStatus{}, false, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return loadStatus{}, false, err
	}

	// helper cari index kolom by possible names
	findIdx := func(names ...string) int {
		for _, want := range names {
			for i, c := range cols {
				if strings.EqualFold(c, want) {
					return i
				}
			}
		}
		return -1
	}

	stateIdx := findIdx("State", "state")
	// Doris berbeda versi: kadang Reason / ErrorMsg / Message / Msg / FailMsg / ErrorMessage
	reasonIdx := findIdx("Reason", "reason", "ErrorMsg", "errormsg", "Message", "message", "Msg", "msg", "FailMsg", "failmsg", "ErrorMessage", "errormessage")

	if stateIdx < 0 {
		return loadStatus{}, false, fmt.Errorf("SHOW LOAD missing State column; cols=%v", cols)
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		bufs := make([]sql.RawBytes, len(cols))
		for i := range vals {
			vals[i] = &bufs[i]
		}

		if err := rows.Scan(vals...); err != nil {
			return loadStatus{}, false, err
		}

		raw := make(map[string]string, len(cols))
		for i, c := range cols {
			raw[c] = strings.TrimSpace(string(bufs[i]))
		}

		state := strings.TrimSpace(string(bufs[stateIdx]))
		reason := ""
		if reasonIdx >= 0 {
			reason = strings.TrimSpace(string(bufs[reasonIdx]))
		}

		return loadStatus{
			State:  state,
			Reason: reason,
			Raw:    raw,
		}, true, nil
	}

	if err := rows.Err(); err != nil {
		return loadStatus{}, false, err
	}
	return loadStatus{}, false, nil
}
