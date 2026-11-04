package connector

import (
	"context"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/model"
)

type JobContext struct {
	JobID        string
	SubmissionID string
	JobName      string
	MetaKey      string
	Mode         config.JobMode
	StoredMode   config.JobMode
	SinkType     string
	SinkConfig   any
	SourceType   string
	SourceConfig any
	JobConfig    *config.JobConfig

	Retry     config.RetryPolicy
	MetaStore meta.OffsetStore

	Metadata       map[string]string
	ReportProgress ProgressReporter
}

type MaintenanceOwnershipLifecycle interface {
	Reserve(context.Context) error
	SnapshotCompleted(context.Context) error
	Release(context.Context) error
}

type MaintenanceOwnershipProvider interface {
	MaintenanceOwnershipLifecycle() MaintenanceOwnershipLifecycle
}

type Source interface {
	Run(ctx context.Context, out chan<- model.Event) error
}

type Sink interface {
	Run(ctx context.Context, in <-chan model.Event) error
}

type ProgressInfo struct {
	RecoveryStatus          string
	RecoveryOperationID     string
	RecoveryAttempts        int
	RecoveryLastError       string
	RecoveryCheckpoint      string
	RecoveryStartedAt       time.Time
	Phase                   string
	Summary                 string
	Detail                  string
	CurrentTable            string
	CurrentTableIndex       int
	CompletedTables         int
	TotalTables             int
	CurrentTableRows        int64
	CDCStartFile            string
	CDCStartPos             uint32
	CDCCurrentFile          string
	CDCCurrentPos           uint32
	CDCCheckpointFile       string
	CDCCheckpointPos        uint32
	CDCLatestFile           string
	CDCLatestPos            uint32
	CDCLagFiles             int
	CDCEarliestFile         string
	CDCAvailableBinlogs     int
	CDCBinlogStatus         string
	CheckpointPending       bool
	CheckpointReason        string
	CheckpointPosition      string
	CheckpointPendingTables string
}

type ProgressReporter func(ProgressInfo)

// TableMaintenanceStatus is a read-only snapshot of the Iceberg maintenance
// monitor. Connectors publish it to the job runtime so the API and UI never
// need to scan object storage or table manifests on every page refresh.
type TableMaintenanceStatus struct {
	Enabled                       bool                          `json:"enabled"`
	State                         string                        `json:"state"`
	CatalogName                   string                        `json:"catalog_name,omitempty"`
	RunnerResourceProfile         string                        `json:"runner_resource_profile,omitempty"`
	MaxConcurrentJobs             int                           `json:"max_concurrent_jobs,omitempty"`
	DataFilesThreshold            int                           `json:"data_files_threshold,omitempty"`
	EqualityDeleteFilesThreshold  int                           `json:"equality_delete_files_threshold,omitempty"`
	PositionDeleteFilesThreshold  int                           `json:"position_delete_files_threshold,omitempty"`
	SmallFileSizeBytes            int64                         `json:"small_file_size_bytes,omitempty"`
	SmallFilesMinCount            int                           `json:"small_files_min_count,omitempty"`
	SmallFilesMinTotalBytes       int64                         `json:"small_files_min_total_bytes,omitempty"`
	ActiveDataFiles               int                           `json:"active_data_files"`
	ActiveEqualityDeleteFiles     int                           `json:"active_equality_delete_files"`
	ActivePositionDeleteFiles     int                           `json:"active_position_delete_files"`
	ApplicableEqualityDeleteFiles int                           `json:"applicable_equality_delete_files"`
	ApplicablePositionDeleteFiles int                           `json:"applicable_position_delete_files"`
	EligibleSmallFiles            int                           `json:"eligible_small_files"`
	EligibleSmallBytes            int64                         `json:"eligible_small_bytes"`
	TablesTotal                   int                           `json:"tables_total"`
	TablesScanned                 int                           `json:"tables_scanned"`
	TablesReady                   int                           `json:"tables_ready"`
	ActiveRuns                    int                           `json:"active_runs"`
	InventoryErrors               int                           `json:"inventory_errors"`
	Paused                        bool                          `json:"paused"`
	CheckedAt                     string                        `json:"checked_at,omitempty"`
	Tables                        []TableMaintenanceTableStatus `json:"tables"`
}

type TableMaintenanceTableStatus struct {
	Catalog                       string   `json:"catalog,omitempty"`
	Namespace                     string   `json:"namespace"`
	Table                         string   `json:"table"`
	Identifier                    string   `json:"identifier"`
	State                         string   `json:"state"`
	ActiveDataFiles               int      `json:"active_data_files"`
	ActiveEqualityDeleteFiles     int      `json:"active_equality_delete_files"`
	ActivePositionDeleteFiles     int      `json:"active_position_delete_files"`
	ApplicableEqualityDeleteFiles int      `json:"applicable_equality_delete_files"`
	ApplicablePositionDeleteFiles int      `json:"applicable_position_delete_files"`
	EligibleSmallFiles            int      `json:"eligible_small_files"`
	EligibleSmallBytes            int64    `json:"eligible_small_bytes"`
	CompactableFiles              int      `json:"compactable_files"`
	CompactableBytes              int64    `json:"compactable_bytes"`
	CompactionGroups              int      `json:"compaction_groups"`
	PartitionFields               []string `json:"partition_fields"`
	NewDataFiles                  int      `json:"new_data_files"`
	NewEqualityDeleteFiles        int      `json:"new_equality_delete_files"`
	CheckedAt                     string   `json:"checked_at,omitempty"`
	Error                         string   `json:"error,omitempty"`
	SubmissionID                  string   `json:"submission_id,omitempty"`
	Operations                    []string `json:"operations,omitempty"`
}

type TableMaintenanceStatusReporter func(*TableMaintenanceStatus)

type TableMaintenanceStatusReporterSetter interface {
	SetTableMaintenanceStatusReporter(TableMaintenanceStatusReporter)
}

// Optional capabilities (engine akan cek via type assertion)

type TableRef struct {
	Schema string
	Table  string
}

type TableLister interface {
	Tables() []TableRef
}

type SchemaProvider interface {
	FetchSchema(ctx context.Context, schema, table string) (*model.TableSchema, error)
}

type TableRowCounter interface {
	CountRows(ctx context.Context, schema, table string) (int64, error)
}

type TableManager interface {
	EnsureTable(ctx context.Context, targetSchema, targetTable string, schema *model.TableSchema) error
}

type TargetResolver interface {
	ResolveTarget(srcSchema, srcTable string) (targetSchema, targetTable string)
}

type SchemaConsumer interface {
	RegisterSourceSchema(schema, table string, sourceSchema *model.TableSchema) error
}

type SnapshotTableSkipper interface {
	SkipSnapshotTables(tables []TableRef)
}

// SnapshotFilterProvider reports source tables whose initial snapshot reads
// only a subset of rows. A full target reset is unsafe for those tables.
type SnapshotFilterProvider interface {
	HasSnapshotFilter(schema, table string) bool
}

type SnapshotFilterExpressionProvider interface {
	SnapshotFilterExpression(schema, table string) string
}

// Validate all selected tables before any reset, including empty sources.
type ScopedSnapshotTargetResetter interface {
	ValidateSnapshotSourceTable(context.Context, string, string, string) error
	ResetSnapshotSourceTable(context.Context, string, string, string) error
}

type SnapshotPrimaryKeySkipper interface {
	SkipSnapshotTableWithoutPrimaryKey(schema, table string, sourceSchema *model.TableSchema) bool
}

type TargetTableRowCounter interface {
	CountTargetRows(ctx context.Context, targetSchema, targetTable string) (int64, error)
}

type TargetTableResetter interface {
	ResetTargetTable(ctx context.Context, targetSchema, targetTable string) error
}

// AuthoritativeInitialSnapshot marks sinks whose fresh initial snapshot must
// replace, rather than only upsert into, the existing target contents.
type AuthoritativeInitialSnapshot interface {
	RequiresInitialSnapshotReset() bool
}
