package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var envPlaceholderPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

type JobMode string

const (
	JobModeInitial      JobMode = "initial"       // snapshot + streaming
	JobModeSnapshotOnly JobMode = "snapshot-only" // snapshot only, no CDC/binlog
	// JobModeSnapshotHandoff is an internal split-worker phase. It writes a
	// resumable initial snapshot checkpoint but stops before starting CDC.
	JobModeSnapshotHandoff JobMode = "snapshot-handoff"
	// JobModeSnapshotHandoffResume resumes an interrupted split-worker snapshot
	// and stops at the same handoff boundary before CDC.
	JobModeSnapshotHandoffResume JobMode = "snapshot-handoff-resume"
	JobModeResume                JobMode = "resume"        // lanjutkan dari checkpoint snapshot/offset terakhir
	JobModeLatestOffset          JobMode = "latest-offset" // resume dari offset (error kalau belum ada)
	JobModeLatest                JobMode = "latest"        // streaming dari latest (ignore offset/snapshot)
	// JobModeMaintenanceOnly is a durable Iceberg maintenance registration.
	// It is submitted through the maintenance-monitor API and never creates a
	// source, snapshot, or CDC pipeline.
	JobModeMaintenanceOnly JobMode = "maintenance-only"
)

type RetryPolicy struct {
	MaxAttempts int           `yaml:"max_attempts" json:"max_attempts"`
	BaseBackoff time.Duration `yaml:"base_backoff" json:"base_backoff"`
	MaxBackoff  time.Duration `yaml:"max_backoff" json:"max_backoff"`
}

type MySQLConfig struct {
	Addr     string `yaml:"addr" json:"addr"` // "mysql:3306"
	User     string `yaml:"user" json:"user"`
	Password string `yaml:"password" json:"password"`

	// Backward compatible (single table):
	Database string `yaml:"database" json:"database"`
	Table    string `yaml:"table" json:"table"`

	// Grouped multi-db selection. Rivus expands databases x table_names into db.table entries.
	Databases  []string `yaml:"databases" json:"databases"`
	TableNames []string `yaml:"table_names" json:"table_names"`

	// Explicit table selection: list of "db.table" or "db.*"
	Tables []string `yaml:"tables" json:"tables"`

	// Optional per-table snapshot settings keyed by "db.table" (or bare table name).
	TableConfigs map[string]MySQLTableConfig `yaml:"table_configs" json:"table_configs"`

	ChunkSize         int `yaml:"chunk_size" json:"chunk_size"`                   // legacy per-row snapshot chunk size
	SnapshotBatchSize int `yaml:"snapshot_batch_size" json:"snapshot_batch_size"` // batch snapshot size for sinks that support batch events
}

type MySQLTableConfig struct {
	// Filter is appended to the initial snapshot query as a raw WHERE condition.
	// It is ignored during CDC/binlog streaming.
	Filter string `yaml:"filter" json:"filter"`
	// SnapshotKeyColumns overrides initial snapshot keyset pagination/order columns.
	// Use a unique tie-breaker as the last column, for example ["created_at", "id"].
	// It is ignored during CDC/binlog streaming.
	SnapshotKeyColumns []string `yaml:"snapshot_key_columns" json:"snapshot_key_columns"`
	// SnapshotExtraColumns appends computed columns to the initial snapshot SELECT.
	// It is ignored during CDC/binlog streaming.
	SnapshotExtraColumns []MySQLSnapshotExtraColumnConfig `yaml:"snapshot_extra_columns" json:"snapshot_extra_columns"`
}

type MySQLSnapshotExtraColumnConfig struct {
	Name       string `yaml:"name" json:"name"`
	Expression string `yaml:"expression" json:"expression"`
	DataType   string `yaml:"data_type" json:"data_type"`
	ColumnType string `yaml:"column_type" json:"column_type"`
	Nullable   *bool  `yaml:"nullable" json:"nullable"`
}

type DorisTarget struct {
	Database string `yaml:"database" json:"database"`
	Table    string `yaml:"table" json:"table"`
}

type IcebergTarget struct {
	Namespace string `yaml:"namespace" json:"namespace"`
	Table     string `yaml:"table" json:"table"`
}

type IcebergMetadataColumnsConfig struct {
	CreatedAt   IcebergCreatedAtColumnConfig `yaml:"created_at" json:"created_at"`
	UpdatedAt   IcebergTimestampColumnConfig `yaml:"updated_at" json:"updated_at"`
	ETLLoadedAt IcebergTimestampColumnConfig `yaml:"etl_loaded_at" json:"etl_loaded_at"`
}

type IcebergSnapshotReplaceFilterConfig struct {
	Column string `yaml:"column" json:"column"`
	Op     string `yaml:"op" json:"op"`
	Value  string `yaml:"value" json:"value"`
}

type IcebergCreatedAtColumnConfig struct {
	Enabled       bool              `yaml:"enabled" json:"enabled"`
	Name          string            `yaml:"name" json:"name"`
	SourceColumns map[string]string `yaml:"source_columns" json:"source_columns"`
}

type IcebergTimestampColumnConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Name    string `yaml:"name" json:"name"`
}

// IcebergTableMaintenanceConfig configures asynchronous Iceberg maintenance.
// RunnerURI selects runner-app; the Spark fields provide a direct Spark
// Standalone compatibility backend. AppResource must be reachable by Spark
// workers when the direct backend is used.
type IcebergTableMaintenanceConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
	// Tables is required by maintenance-only monitors because they do not have
	// a source table selection from which target tables can be derived, unless
	// CatalogMonitoring is enabled.
	Tables                              []IcebergTarget                `yaml:"tables" json:"tables"`
	CatalogMonitoring                   IcebergCatalogMonitoringConfig `yaml:"catalog_monitoring" json:"catalog_monitoring"`
	NativeEnabled                       bool                           `yaml:"native_enabled" json:"native_enabled"`
	Executor                            string                         `yaml:"executor" json:"executor"`
	NativeSignalDelaySeconds            int                            `yaml:"native_signal_delay_seconds" json:"native_signal_delay_seconds"`
	NativeIdleCheckIntervalSeconds      int                            `yaml:"native_idle_check_interval_seconds" json:"native_idle_check_interval_seconds"`
	NativeOrphanIntervalSeconds         int                            `yaml:"native_orphan_interval_seconds" json:"native_orphan_interval_seconds"`
	NativeOrphanInactiveIntervalSeconds int                            `yaml:"native_orphan_inactive_interval_seconds" json:"native_orphan_inactive_interval_seconds"`
	MetadataCleanupEnabled              *bool                          `yaml:"metadata_cleanup_enabled,omitempty" json:"metadata_cleanup_enabled,omitempty"`
	MetadataPreviousVersionsMax         int                            `yaml:"metadata_previous_versions_max,omitempty" json:"metadata_previous_versions_max,omitempty"`
	NativeMaxSelectedInputBytes         int64                          `yaml:"native_max_selected_input_bytes" json:"native_max_selected_input_bytes"`
	NativeMaxSelectedFiles              int                            `yaml:"native_max_selected_files" json:"native_max_selected_files"`
	NativeMaxEqualityDeleteFiles        int                            `yaml:"native_max_equality_delete_files" json:"native_max_equality_delete_files"`
	NativeTargetFileSizeBytes           int64                          `yaml:"native_target_file_size_bytes" json:"native_target_file_size_bytes"`
	NativeScanConcurrency               int                            `yaml:"native_scan_concurrency" json:"native_scan_concurrency"`
	NativeTimeoutSeconds                int                            `yaml:"native_timeout_seconds" json:"native_timeout_seconds"`
	WorkerTempDirectory                 string                         `yaml:"worker_temp_directory" json:"worker_temp_directory"`
	RunnerURI                           string                         `yaml:"runner_uri" json:"runner_uri"`
	RunnerAPIToken                      string                         `yaml:"runner_api_token" json:"runner_api_token,omitempty"`
	RunnerResourceProfile               string                         `yaml:"runner_resource_profile" json:"runner_resource_profile"`
	SparkRESTURI                        string                         `yaml:"spark_rest_uri" json:"spark_rest_uri"`
	SparkMaster                         string                         `yaml:"spark_master" json:"spark_master"`
	AppResource                         string                         `yaml:"app_resource" json:"app_resource"`
	MainClass                           string                         `yaml:"main_class" json:"main_class"`
	ClientSparkVersion                  string                         `yaml:"client_spark_version" json:"client_spark_version"`
	CatalogName                         string                         `yaml:"catalog_name" json:"catalog_name"`
	RESTAuthHeader                      string                         `yaml:"rest_auth_header" json:"rest_auth_header"`
	PollIntervalSeconds                 int                            `yaml:"poll_interval_seconds" json:"poll_interval_seconds"`
	MaxConcurrentJobs                   int                            `yaml:"max_concurrent_jobs" json:"max_concurrent_jobs"`
	DataFilesThreshold                  int                            `yaml:"data_files_threshold" json:"data_files_threshold"`
	EqualityDeleteFilesThreshold        int                            `yaml:"equality_delete_files_threshold" json:"equality_delete_files_threshold"`
	PositionDeleteFilesThreshold        int                            `yaml:"position_delete_files_threshold" json:"position_delete_files_threshold"`
	SmallFileSizeBytes                  int64                          `yaml:"small_file_size_bytes" json:"small_file_size_bytes"`
	SmallFilesMinCount                  int                            `yaml:"small_files_min_count" json:"small_files_min_count"`
	SmallFilesMinTotalBytes             int64                          `yaml:"small_files_min_total_bytes" json:"small_files_min_total_bytes"`
	ExpireSnapshotsIntervalSeconds      int                            `yaml:"expire_snapshots_interval_seconds" json:"expire_snapshots_interval_seconds"`
	ExpireSnapshotsOlderThanHours       float64                        `yaml:"expire_snapshots_older_than_hours" json:"expire_snapshots_older_than_hours"`
	ExpireSnapshotsRetainLast           int                            `yaml:"expire_snapshots_retain_last" json:"expire_snapshots_retain_last"`
	OrphanCleanupIntervalSeconds        int                            `yaml:"orphan_cleanup_interval_seconds" json:"orphan_cleanup_interval_seconds"`
	OrphanCleanupOlderThanHours         float64                        `yaml:"orphan_cleanup_older_than_hours" json:"orphan_cleanup_older_than_hours"`
	CompactOptions                      map[string]any                 `yaml:"compact_options" json:"compact_options"`
	SparkProperties                     map[string]string              `yaml:"spark_properties" json:"spark_properties"`
	EnvironmentVariables                map[string]string              `yaml:"environment_variables" json:"environment_variables"`
}

// IcebergCatalogMonitoringConfig discovers catalogs through the Gravitino
// management API, then discovers matching namespaces and tables through the
// Iceberg REST API. It is intentionally separate from Tables so static targets
// and dynamic discovery have unambiguous configuration shapes.
type IcebergCatalogMonitoringConfig struct {
	Enabled                  bool                                `yaml:"enabled" json:"enabled"`
	APIURI                   string                              `yaml:"api_uri" json:"api_uri"`
	Metalake                 string                              `yaml:"metalake" json:"metalake"`
	CatalogPatterns          []string                            `yaml:"catalog_patterns" json:"catalog_patterns"`
	NamespacePatterns        []string                            `yaml:"namespace_patterns" json:"namespace_patterns"`
	TablePatterns            []string                            `yaml:"table_patterns" json:"table_patterns"`
	Exclude                  []IcebergCatalogMonitoringExclusion `yaml:"exclude" json:"exclude"`
	DiscoveryIntervalSeconds int                                 `yaml:"discovery_interval_seconds" json:"discovery_interval_seconds"`
}

// IcebergCatalogMonitoringExclusion removes matching targets from one dynamic
// monitor. Omitting Namespace excludes the whole matching catalog; omitting
// Table excludes the whole matching namespace.
type IcebergCatalogMonitoringExclusion struct {
	Catalog   string `yaml:"catalog" json:"catalog"`
	Namespace string `yaml:"namespace" json:"namespace"`
	Table     string `yaml:"table" json:"table"`
	Reason    string `yaml:"reason" json:"reason,omitempty"`
}

type IcebergConfig struct {
	RestURI                       string                                        `yaml:"rest_uri" json:"rest_uri"`
	CatalogURI                    string                                        `yaml:"catalog_uri" json:"catalog_uri"`
	CatalogName                   string                                        `yaml:"catalog_name" json:"catalog_name"`
	Warehouse                     string                                        `yaml:"warehouse" json:"warehouse"`
	WarehouseTemplate             string                                        `yaml:"warehouse_template" json:"warehouse_template"`
	Credential                    string                                        `yaml:"credential" json:"credential"`
	OAuthToken                    string                                        `yaml:"oauth_token" json:"oauth_token"`
	OAuthTokenURI                 string                                        `yaml:"oauth_token_uri" json:"oauth_token_uri"`
	Scope                         string                                        `yaml:"scope" json:"scope"`
	Prefix                        string                                        `yaml:"prefix" json:"prefix"`
	RESTAuthHeader                string                                        `yaml:"rest_auth_header" json:"rest_auth_header"`
	RESTBasicUsername             string                                        `yaml:"rest_basic_username" json:"rest_basic_username"`
	RESTBasicPassword             string                                        `yaml:"rest_basic_password" json:"rest_basic_password"`
	S3Endpoint                    string                                        `yaml:"s3_endpoint" json:"s3_endpoint"`
	S3Region                      string                                        `yaml:"s3_region" json:"s3_region"`
	S3PathStyle                   string                                        `yaml:"s3_path_style" json:"s3_path_style"`
	S3CompatMode                  string                                        `yaml:"s3_compat_mode" json:"s3_compat_mode"`
	TLSInsecureSkipVerify         bool                                          `yaml:"tls_insecure_skip_verify" json:"tls_insecure_skip_verify"`
	DefaultNamespace              string                                        `yaml:"default_namespace" json:"default_namespace"`
	Overrides                     map[string]IcebergTarget                      `yaml:"overrides" json:"overrides"`
	PrimaryKeys                   map[string][]string                           `yaml:"primary_keys" json:"primary_keys"`
	SkipSnapshotTablesWithoutPK   bool                                          `yaml:"skip_snapshot_tables_without_primary_key" json:"skip_snapshot_tables_without_primary_key"`
	BatchSize                     int                                           `yaml:"batch_size" json:"batch_size"`
	SnapshotBatchSize             int                                           `yaml:"snapshot_batch_size" json:"snapshot_batch_size"`
	SnapshotRollingEnabled        *bool                                         `yaml:"snapshot_rolling_enabled" json:"snapshot_rolling_enabled,omitempty"`
	SnapshotTargetFileSizeBytes   int64                                         `yaml:"snapshot_target_file_size_bytes" json:"snapshot_target_file_size_bytes"`
	SnapshotParquetRowGroupRows   int                                           `yaml:"snapshot_parquet_row_group_rows" json:"snapshot_parquet_row_group_rows"`
	SnapshotSpoolDirectory        string                                        `yaml:"snapshot_spool_directory" json:"snapshot_spool_directory"`
	SnapshotSpoolMaxBytes         int64                                         `yaml:"snapshot_spool_max_bytes" json:"snapshot_spool_max_bytes"`
	SnapshotWriteMode             string                                        `yaml:"snapshot_write_mode" json:"snapshot_write_mode"`
	SnapshotReplaceDeleteExecutor string                                        `yaml:"snapshot_replace_delete_executor" json:"snapshot_replace_delete_executor"`
	CDCDeleteExecutor             string                                        `yaml:"cdc_delete_executor" json:"cdc_delete_executor"`
	SnapshotReplaceFilters        map[string]IcebergSnapshotReplaceFilterConfig `yaml:"snapshot_replace_filters" json:"snapshot_replace_filters"`
	SnapshotTruncateTables        []string                                      `yaml:"snapshot_truncate_tables" json:"snapshot_truncate_tables"`
	SnapshotTruncateExcludeTables []string                                      `yaml:"snapshot_truncate_exclude_tables" json:"snapshot_truncate_exclude_tables"`
	SnapshotTruncateAllowPatterns bool                                          `yaml:"snapshot_truncate_allow_patterns" json:"snapshot_truncate_allow_patterns"`
	MaxBatchBytes                 ByteSize                                      `yaml:"max_batch_bytes" json:"max_batch_bytes"`
	MaxConcurrentCommits          int                                           `yaml:"max_concurrent_iceberg_commits" json:"max_concurrent_iceberg_commits"`
	FlushSeconds                  int                                           `yaml:"flush_seconds" json:"flush_seconds"`
	CheckpointFlushSeconds        int                                           `yaml:"checkpoint_flush_seconds" json:"checkpoint_flush_seconds"`
	CheckpointSaveIntervalSeconds int                                           `yaml:"checkpoint_save_interval_seconds" json:"checkpoint_save_interval_seconds"`
	DeleteConcurrency             int                                           `yaml:"delete_concurrency" json:"delete_concurrency"`
	IdleTableEvictSeconds         int                                           `yaml:"idle_table_evict_seconds" json:"idle_table_evict_seconds"`
	TrinoDelete                   IcebergTrinoDeleteConfig                      `yaml:"trino_delete" json:"trino_delete"`
	TableProperties               map[string]string                             `yaml:"table_properties" json:"table_properties"`
	MetadataColumns               IcebergMetadataColumnsConfig                  `yaml:"metadata_columns" json:"metadata_columns"`
	TableMaintenance              IcebergTableMaintenanceConfig                 `yaml:"table_maintenance" json:"table_maintenance"`
	AllowDropColumn               bool                                          `yaml:"allow_drop_column" json:"allow_drop_column"`
	AllowRenameColumn             bool                                          `yaml:"allow_rename_column" json:"allow_rename_column"`
	AllowUnsafeTypeChanges        bool                                          `yaml:"allow_unsafe_type_changes" json:"allow_unsafe_type_changes"`
}

type IcebergTrinoDeleteConfig struct {
	URI                   string `yaml:"uri" json:"uri"`
	User                  string `yaml:"user" json:"user"`
	Password              string `yaml:"password" json:"password"`
	Source                string `yaml:"source" json:"source"`
	Catalog               string `yaml:"catalog" json:"catalog"`
	AccessToken           string `yaml:"access_token" json:"access_token"`
	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify" json:"tls_insecure_skip_verify"`
}

type ByteSize int64

func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	if value == nil {
		return nil
	}
	parsed, err := parseByteSize(value.Value)
	if err != nil {
		return err
	}
	*b = ByteSize(parsed)
	return nil
}

func (b *ByteSize) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		*b = ByteSize(n)
		return nil
	}

	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := parseByteSize(raw)
	if err != nil {
		return err
	}
	*b = ByteSize(parsed)
	return nil
}

func parseByteSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}

	unit := int64(1)
	number := raw
	upper := strings.ToUpper(strings.ReplaceAll(raw, " ", ""))
	units := []struct {
		suffix     string
		multiplier int64
	}{
		{"GIB", 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MIB", 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KIB", 1024},
		{"KB", 1024},
		{"B", 1},
	}
	for _, candidate := range units {
		if strings.HasSuffix(upper, candidate.suffix) {
			unit = candidate.multiplier
			number = strings.TrimSuffix(upper, candidate.suffix)
			break
		}
	}

	value, err := strconv.ParseFloat(strings.TrimSpace(number), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", raw, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("invalid negative byte size %q", raw)
	}
	return int64(value * float64(unit)), nil
}

type DorisConfig struct {
	HTTPHost string `yaml:"http_host" json:"http_host"` // http://doris-fe:8030

	// Optional override for FE -> BE stream load redirects.
	// If BEHTTPPort is set but BEHTTPHost is empty, Rivus reuses the host from HTTPHost.
	BEHTTPHost string `yaml:"be_http_host" json:"be_http_host"`
	BEHTTPPort int    `yaml:"be_http_port" json:"be_http_port"`

	MySQLPort int `yaml:"mysql_port" json:"mysql_port"`

	// Backward compatible (single table):
	Database string `yaml:"database" json:"database"`
	Table    string `yaml:"table" json:"table"`

	// New routing:
	// If set, default target db becomes this; target table stays same as source table (unless override).
	DefaultDatabase string `yaml:"default_database" json:"default_database"`

	// overrides key:
	// - "source_db.source_table" for exact table mapping
	// - "source_db.*" for schema-level mapping while keeping source table names
	Overrides map[string]DorisTarget `yaml:"overrides" json:"overrides"`

	User         string `yaml:"user" json:"user"`
	Password     string `yaml:"password" json:"password"`
	BatchSize    int    `yaml:"batch_size" json:"batch_size"`
	FlushSeconds int    `yaml:"flush_seconds" json:"flush_seconds"`
}

type MetaConfig struct {
	MySQLDSN string `yaml:"mysql_dsn" json:"mysql_dsn"` // "user:pass@tcp(meta-mysql:3306)/gosync_meta"
}

type JobNotificationsConfig struct {
	Telegram TelegramNotificationConfig `yaml:"telegram" json:"telegram"`
}

type TelegramNotificationConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	BotToken        string `yaml:"bot_token" json:"bot_token"`
	ChatID          string `yaml:"chat_id" json:"chat_id"`
	UIBaseURL       string `yaml:"ui_base_url" json:"ui_base_url"`
	NotifyJobFailed bool   `yaml:"notify_job_failed" json:"notify_job_failed"`
	NotifyCDCLag    bool   `yaml:"notify_cdc_lag" json:"notify_cdc_lag"`
	// NotifyCheckpointPurged is populated from
	// RIVUS_TELEGRAM_NOTIFY_CHECKPOINT_PURGED. It is intentionally not exposed
	// in per-job configuration because a purged checkpoint is a platform-wide
	// recovery risk.
	NotifyCheckpointPurged bool `yaml:"-" json:"-"`
	NotifyBackpressure     bool `yaml:"notify_backpressure" json:"notify_backpressure"`
	CDCLagFilesThreshold   int  `yaml:"cdc_lag_files_threshold" json:"cdc_lag_files_threshold"`
	AlertCooldownSeconds   int  `yaml:"alert_cooldown_seconds" json:"alert_cooldown_seconds"`
}

type JobConfig struct {
	ID   string  `yaml:"id" json:"id"`
	Name string  `yaml:"name" json:"name"`
	Mode JobMode `yaml:"mode" json:"mode"`

	Source *ConnectorSpec `yaml:"source" json:"source"`
	Sink   *ConnectorSpec `yaml:"sink" json:"sink"`

	MySQL MySQLConfig `yaml:"mysql" json:"mysql"`
	Doris DorisConfig `yaml:"doris" json:"doris"`

	Retry         RetryPolicy            `yaml:"retry" json:"retry"`
	BufferSize    int                    `yaml:"buffer_size" json:"buffer_size"`
	Metadata      map[string]string      `yaml:"metadata" json:"metadata"`
	Meta          MetaConfig             `yaml:"meta" json:"meta"`
	Notifications JobNotificationsConfig `yaml:"notifications" json:"notifications"`
}

type ConnectorSpec struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config" json:"config"`
}

func setDefaults(cfg *JobConfig) {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1000
	}
	if cfg.MySQL.ChunkSize <= 0 {
		cfg.MySQL.ChunkSize = 1000
	}
	if cfg.MySQL.SnapshotBatchSize <= 0 {
		cfg.MySQL.SnapshotBatchSize = 10000
	}
	if cfg.Doris.BatchSize <= 0 {
		cfg.Doris.BatchSize = 500
	}
	if cfg.Doris.FlushSeconds <= 0 {
		cfg.Doris.FlushSeconds = 3
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 5
	}
	if cfg.Retry.BaseBackoff == 0 {
		cfg.Retry.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.Retry.MaxBackoff == 0 {
		cfg.Retry.MaxBackoff = 10 * time.Second
	}
	if cfg.Doris.MySQLPort <= 0 {
		cfg.Doris.MySQLPort = 9030
	}
	normalize(cfg)
}

func ApplyDefaults(cfg *JobConfig) {
	if cfg == nil {
		return
	}
	setDefaults(cfg)
}

func normalize(cfg *JobConfig) {
	cfg.MySQL = NormalizeMySQLConfig(cfg.MySQL)

	// ---- doris overrides normalize ----
	if cfg.Doris.Overrides != nil {
		norm := make(map[string]DorisTarget, len(cfg.Doris.Overrides))
		for k, v := range cfg.Doris.Overrides {
			kk := strings.ToLower(strings.TrimSpace(k))
			if kk == "" {
				continue
			}
			norm[kk] = v
		}
		cfg.Doris.Overrides = norm
	}

	normalizeNotifications(&cfg.Notifications)
}

func NormalizeMySQLConfig(cfg MySQLConfig) MySQLConfig {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 1000
	}
	if cfg.SnapshotBatchSize <= 0 {
		cfg.SnapshotBatchSize = 10000
	}

	cfg.Database = strings.TrimSpace(cfg.Database)
	cfg.Table = strings.TrimSpace(cfg.Table)
	cfg.Databases = normalizeLowerStringSlice(cfg.Databases)
	cfg.TableNames = normalizeLowerStringSlice(cfg.TableNames)
	cfg.Tables = normalizeMySQLTables(cfg)

	if cfg.TableConfigs != nil {
		norm := make(map[string]MySQLTableConfig, len(cfg.TableConfigs))
		for k, v := range cfg.TableConfigs {
			kk := strings.ToLower(strings.TrimSpace(k))
			if kk == "" {
				continue
			}
			v.Filter = normalizeSnapshotFilter(v.Filter)
			v.SnapshotKeyColumns = normalizeTrimmedStringSlice(v.SnapshotKeyColumns)
			v.SnapshotExtraColumns = normalizeMySQLSnapshotExtraColumns(v.SnapshotExtraColumns)
			norm[kk] = v
		}
		cfg.TableConfigs = norm
	}

	return cfg
}

func normalizeMySQLSnapshotExtraColumns(cols []MySQLSnapshotExtraColumnConfig) []MySQLSnapshotExtraColumnConfig {
	if len(cols) == 0 {
		return nil
	}
	out := make([]MySQLSnapshotExtraColumnConfig, 0, len(cols))
	for _, col := range cols {
		col.Name = strings.TrimSpace(col.Name)
		col.Expression = normalizeSnapshotFilter(col.Expression)
		col.DataType = strings.ToLower(strings.TrimSpace(col.DataType))
		col.ColumnType = strings.ToLower(strings.TrimSpace(col.ColumnType))
		if col.ColumnType == "" {
			col.ColumnType = col.DataType
		}
		if col.Name == "" || col.Expression == "" || col.DataType == "" {
			continue
		}
		out = append(out, col)
	}
	return out
}

func normalizeMySQLTables(cfg MySQLConfig) []string {
	out := make([]string, 0, len(cfg.Tables)+(len(cfg.Databases)*len(cfg.TableNames))+1)
	seen := make(map[string]struct{}, cap(out))
	add := func(raw string) {
		entry := strings.ToLower(strings.TrimSpace(raw))
		if entry == "" {
			return
		}
		if _, exists := seen[entry]; exists {
			return
		}
		seen[entry] = struct{}{}
		out = append(out, entry)
	}

	for _, table := range cfg.Tables {
		add(table)
	}

	for _, dbName := range cfg.Databases {
		for _, tableName := range cfg.TableNames {
			add(fmt.Sprintf("%s.%s", dbName, tableName))
		}
	}

	if len(out) == 0 && cfg.Database != "" && cfg.Table != "" {
		add(fmt.Sprintf("%s.%s", cfg.Database, cfg.Table))
	}

	return out
}

func normalizeLowerStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		v := strings.ToLower(strings.TrimSpace(value))
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

func normalizeTrimmedStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

func normalizeNotifications(cfg *JobNotificationsConfig) {
	cfg.Telegram.BotToken = strings.TrimSpace(cfg.Telegram.BotToken)
	cfg.Telegram.ChatID = strings.TrimSpace(cfg.Telegram.ChatID)
	cfg.Telegram.UIBaseURL = strings.TrimRight(strings.TrimSpace(cfg.Telegram.UIBaseURL), "/")
}

func normalizeSnapshotFilter(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, "; \t\r\n")
	if strings.HasPrefix(strings.ToLower(raw), "where ") {
		raw = strings.TrimSpace(raw[6:])
	}
	return raw
}

func LoadJobConfigFromBytes(b []byte) (*JobConfig, error) {
	configs, err := LoadJobConfigsFromBytes(b)
	if err != nil {
		return nil, err
	}
	if len(configs) == 0 {
		return nil, errors.New("job config is empty")
	}
	if len(configs) > 1 {
		return nil, fmt.Errorf("expected single job config, got %d YAML documents", len(configs))
	}
	return configs[0], nil
}

func LoadJobConfigsFromBytes(b []byte) ([]*JobConfig, error) {
	b = []byte(ExpandEnvPlaceholders(string(b)))

	dec := yaml.NewDecoder(bytes.NewReader(b))
	configs := make([]*JobConfig, 0, 1)
	for {
		var cfg JobConfig
		if err := dec.Decode(&cfg); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if isEmptyJobConfig(&cfg) {
			continue
		}
		ApplyDefaults(&cfg)
		configs = append(configs, &cfg)
	}
	return configs, nil
}

func isEmptyJobConfig(cfg *JobConfig) bool {
	if cfg == nil {
		return true
	}
	return strings.TrimSpace(cfg.ID) == "" &&
		strings.TrimSpace(cfg.Name) == "" &&
		cfg.Source == nil &&
		cfg.Sink == nil &&
		len(cfg.MySQL.Tables) == 0 &&
		strings.TrimSpace(cfg.MySQL.Database) == "" &&
		strings.TrimSpace(cfg.MySQL.Table) == "" &&
		len(cfg.Doris.Overrides) == 0
}

func ExpandEnvPlaceholders(raw string) string {
	return envPlaceholderPattern.ReplaceAllStringFunc(raw, func(match string) string {
		submatches := envPlaceholderPattern.FindStringSubmatch(match)
		if len(submatches) != 2 {
			return match
		}
		if value, ok := os.LookupEnv(submatches[1]); ok {
			return value
		}
		return match
	})
}
