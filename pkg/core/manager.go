package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/meta"
)

var (
	ErrJobNotFound            = errors.New("job not found")
	ErrJobResubmitNotAllowed  = errors.New("job resubmit not allowed")
	ErrJobStillStopping       = errors.New("job pipeline is still stopping")
	ErrJobPauseNotAllowed     = errors.New("job pause not allowed")
	ErrJobManagerShuttingDown = errors.New("job manager is shutting down")
	ErrJobWorkerLeaseLost     = errors.New("job worker lease is no longer owned")
	ErrJobDeleting            = errors.New("job deletion is still being finalized")
)

const defaultMaxConcurrentSnapshotJobs = 2

type WorkerRole string

const (
	WorkerRoleAll       WorkerRole = "all"
	WorkerRoleSnapshot  WorkerRole = "snapshot"
	WorkerRoleStreaming WorkerRole = "streaming"
)

const (
	defaultWorkerPollInterval  = 2 * time.Second
	defaultWorkerLeaseDuration = 30 * time.Second
	// Claim one new job per reconciliation pass. Connector construction can
	// involve network discovery, so bulk-claiming every runnable job gives all
	// of them the same short lease before they can actually be started.
	defaultWorkerClaimLimit = 1
)

const (
	defaultFailureNotificationRetryInitial = time.Second
	defaultFailureNotificationRetryMax     = 10 * time.Minute
)

type JobInfo struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Status       JobStatus    `json:"status"`
	HealthStatus string       `json:"health_status,omitempty"`
	HealthDetail string       `json:"health_detail,omitempty"`
	Created      string       `json:"created"`
	Updated      string       `json:"updated"`
	MetaKey      string       `json:"meta_key"`
	SinkType     string       `json:"sink_type"`
	ErrorCount   int          `json:"error_count"`
	LastError    *JobError    `json:"last_error,omitempty"`
	Progress     *JobProgress `json:"progress,omitempty"`
}

type JobManager struct {
	lifecycleMu     sync.RWMutex
	mu              sync.RWMutex
	jobs            map[string]*Job
	deletingJobs    map[string]struct{}
	reg             *connector.Registry
	failureNotifier jobFailureNotifier
	healthNotifier  jobHealthNotifier

	healthAlertMu       sync.Mutex
	healthAlertLastSent map[string]time.Time
	healthAlertActive   map[string]string

	failureDeliveryMu        sync.Mutex
	deferredFailureJobs      map[string]struct{}
	activeFailureDeliveries  map[string]struct{}
	completedFailureDelivery map[string]struct{}
	failureRetryInitial      time.Duration
	failureRetryMax          time.Duration

	jobStore             meta.JobStore
	defaultMetaMySQL     string
	autoResume           bool
	jobStoreReady        bool
	jobStoreReadyLock    sync.Mutex
	workerRole           WorkerRole
	workerID             string
	workerPollInterval   time.Duration
	workerLeaseDuration  time.Duration
	executionRoles       map[string]meta.JobExecutionRole
	workerLeases         map[string]string
	progressPersistMu    sync.Mutex
	lastProgressPersist  map[string]time.Time
	maintenanceReleaseMu sync.Mutex
	maintenanceReleases  map[string]struct{}

	maxConcurrentSnapshotJobs int
	snapshotQueue             []string
	snapshotQueueModes        map[string]config.JobMode
	startingSnapshotJobs      map[string]struct{}
	shuttingDown              bool
	restartResumeJobs         map[string]struct{}
}

type JobManagerOption func(*JobManager)

func WithJobStore(store meta.JobStore) JobManagerOption {
	return func(m *JobManager) {
		m.jobStore = store
	}
}

func WithDefaultMetaMySQLDSN(dsn string) JobManagerOption {
	return func(m *JobManager) {
		m.defaultMetaMySQL = strings.TrimSpace(dsn)
	}
}

// WithAutoResume controls whether jobs persisted with RUNNING intent are
// automatically resumed during manager startup. Jobs are always restored and
// remain available for manual resume when this is disabled.
func WithAutoResume(enabled bool) JobManagerOption {
	return func(m *JobManager) {
		m.autoResume = enabled
	}
}

func WithMaxConcurrentSnapshotJobs(limit int) JobManagerOption {
	return func(m *JobManager) {
		m.maxConcurrentSnapshotJobs = limit
	}
}

func WithWorkerRole(role WorkerRole) JobManagerOption {
	return func(m *JobManager) {
		m.workerRole = normalizeWorkerRole(role)
	}
}

func WithWorkerID(id string) JobManagerOption {
	return func(m *JobManager) {
		if id = strings.TrimSpace(id); id != "" {
			m.workerID = id
		}
	}
}

func WithWorkerTiming(pollInterval, leaseDuration time.Duration) JobManagerOption {
	return func(m *JobManager) {
		if pollInterval > 0 {
			m.workerPollInterval = pollInterval
		}
		if leaseDuration > 0 {
			m.workerLeaseDuration = leaseDuration
		}
	}
}

func withJobFailureNotifier(notifier jobFailureNotifier) JobManagerOption {
	return func(m *JobManager) {
		m.failureNotifier = notifier
	}
}

func withJobHealthNotifier(notifier jobHealthNotifier) JobManagerOption {
	return func(m *JobManager) {
		m.healthNotifier = notifier
	}
}

func withFailureNotificationRetry(initial, max time.Duration) JobManagerOption {
	return func(m *JobManager) {
		if initial > 0 {
			m.failureRetryInitial = initial
		}
		if max > 0 {
			m.failureRetryMax = max
		}
	}
}

func NewJobManager(reg *connector.Registry, opts ...JobManagerOption) *JobManager {
	telegramNotifier := newTelegramJobFailureNotifier(nil)
	m := &JobManager{
		jobs:                      make(map[string]*Job),
		deletingJobs:              make(map[string]struct{}),
		reg:                       reg,
		failureNotifier:           telegramNotifier,
		healthNotifier:            telegramNotifier,
		healthAlertLastSent:       make(map[string]time.Time),
		healthAlertActive:         make(map[string]string),
		deferredFailureJobs:       make(map[string]struct{}),
		activeFailureDeliveries:   make(map[string]struct{}),
		completedFailureDelivery:  make(map[string]struct{}),
		failureRetryInitial:       defaultFailureNotificationRetryInitial,
		failureRetryMax:           defaultFailureNotificationRetryMax,
		autoResume:                false,
		maxConcurrentSnapshotJobs: snapshotJobLimitFromEnv(),
		snapshotQueueModes:        make(map[string]config.JobMode),
		startingSnapshotJobs:      make(map[string]struct{}),
		restartResumeJobs:         make(map[string]struct{}),
		workerRole:                WorkerRoleAll,
		workerID:                  defaultWorkerID(),
		workerPollInterval:        defaultWorkerPollInterval,
		workerLeaseDuration:       defaultWorkerLeaseDuration,
		executionRoles:            make(map[string]meta.JobExecutionRole),
		workerLeases:              make(map[string]string),
		lastProgressPersist:       make(map[string]time.Time),
		maintenanceReleases:       make(map[string]struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

func (m *JobManager) Submit(cfg *config.JobConfig) (*Job, error) {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()

	cfg = m.normalizeConfig(cfg)
	if cfg == nil {
		return nil, errors.New("job config is nil")
	}
	if cfg.Mode == config.JobModeMaintenanceOnly {
		return nil, errors.New("maintenance-only configs must be submitted through Maintenance Monitors")
	}
	if err := validateSnapshotOnlyCountResume(cfg); err != nil {
		return nil, err
	}

	job := NewJob(cfg, m.reg)
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return nil, ErrJobManagerShuttingDown
	}
	if _, deleting := m.deletingJobs[cfg.ID]; deleting {
		m.mu.Unlock()
		return nil, fmt.Errorf("job id %q: %w", cfg.ID, ErrJobDeleting)
	}
	if _, exists := m.jobs[cfg.ID]; exists {
		m.mu.Unlock()
		return nil, errors.New("job id already exists")
	}
	m.jobs[cfg.ID] = job
	executionRole := meta.JobExecutionRoleAll
	if m.workerRole != WorkerRoleAll {
		executionRole = executionRoleForConfig(cfg)
	}
	m.executionRoles[cfg.ID] = executionRole
	if m.workerRole != WorkerRoleAll {
		m.mu.Unlock()
		job.setStatus(JobStatusQueued)
		job.updateProgress(connector.ProgressInfo{
			Phase:   "queued",
			Summary: "Waiting for " + strings.ToLower(string(executionRole)) + " worker",
			Detail:  "The durable worker lease prevents duplicate execution",
		})
		m.attachStatusListener(job)
		if err := m.saveManagedJobRecord(context.Background(), job, meta.DesiredStateRunning, JobStatusQueued); err != nil {
			m.mu.Lock()
			delete(m.jobs, cfg.ID)
			delete(m.executionRoles, cfg.ID)
			m.mu.Unlock()
			return nil, err
		}
		return job, nil
	}
	shouldQueue := m.shouldQueueSnapshotStartLocked(job, cfg.Mode)
	if shouldQueue {
		m.enqueueSnapshotJobLocked(cfg.ID, cfg.Mode)
	}
	m.mu.Unlock()
	m.attachStatusListener(job)

	if shouldQueue {
		job.setStatus(JobStatusQueued)
		job.updateProgress(connector.ProgressInfo{
			Phase:   "queued",
			Summary: "Waiting in queue",
			Detail:  "The job is waiting for an active snapshot/preflight slot to finish",
		})
		return job, nil
	}

	m.deferFailureNotification(cfg.ID)
	if err := m.startJob(job, cfg.Mode, true); err != nil {
		m.finishDeferredFailureNotification(job, false)
		log.Printf("[job-manager] job start failed job=%s: %v", cfg.ID, err)
		return nil, err
	}
	m.finishDeferredFailureNotification(job, true)
	return job, nil
}

type SubmitResult struct {
	ID     string    `json:"id"`
	Name   string    `json:"name,omitempty"`
	Status JobStatus `json:"status,omitempty"`
	Action string    `json:"action"`
	Error  string    `json:"error,omitempty"`
}

func (m *JobManager) SubmitMany(configs []*config.JobConfig) []SubmitResult {
	results := make([]SubmitResult, 0, len(configs))
	seen := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		if cfg == nil {
			results = append(results, SubmitResult{Action: "failed", Error: "job config is nil"})
			continue
		}
		id := strings.TrimSpace(cfg.ID)
		name := strings.TrimSpace(cfg.Name)
		if id == "" {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "failed", Error: "job id is empty"})
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "skipped", Error: "duplicate job id in submitted file"})
			continue
		}
		seen[id] = struct{}{}
		if m.HasJob(id) {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "skipped", Error: "job id already exists"})
			continue
		}
		job, err := m.Submit(cfg)
		if err != nil {
			results = append(results, SubmitResult{ID: id, Name: name, Action: "failed", Error: err.Error()})
			continue
		}
		results = append(results, SubmitResult{
			ID:     job.Config.ID,
			Name:   job.Name(),
			Status: job.GetStatus(),
			Action: "submitted",
		})
	}
	return results
}

func (m *JobManager) Cancel(id string) error {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()

	m.mu.RLock()
	if m.shuttingDown {
		m.mu.RUnlock()
		return ErrJobManagerShuttingDown
	}
	job, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return ErrJobNotFound
	}
	if job.GetStatus() == JobStatusQueued {
		m.mu.Lock()
		m.removeSnapshotQueueLocked(id)
		m.mu.Unlock()
		job.setStatus(JobStatusStopped)
		return nil
	}
	job.StopAsync()
	return nil
}

func (m *JobManager) Pause(id string) error {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()

	m.mu.RLock()
	if m.shuttingDown {
		m.mu.RUnlock()
		return ErrJobManagerShuttingDown
	}
	job, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return ErrJobNotFound
	}
	if !job.RequestPause() {
		return ErrJobPauseNotAllowed
	}
	return nil
}

func (m *JobManager) Resubmit(id string) (*Job, error) {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()

	m.mu.RLock()
	if m.shuttingDown {
		m.mu.RUnlock()
		return nil, ErrJobManagerShuttingDown
	}
	job, ok := m.jobs[id]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrJobNotFound
	}

	switch job.GetStatus() {
	case JobStatusFailed, JobStatusStopped, JobStatusPaused:
		if m.workerRole != WorkerRoleAll {
			job.setStatus(JobStatusQueued)
			job.updateProgress(connector.ProgressInfo{
				Phase:   "queued",
				Summary: "Waiting for assigned worker",
				Detail:  "The job will resume from its durable checkpoint",
			})
			return job, nil
		}
		if job.runActive() {
			if !job.waitRunDone(500 * time.Millisecond) {
				return nil, ErrJobStillStopping
			}
		}
		mode := config.JobModeResume
		freshSnapshot, err := jobFreshSnapshotOnResubmit(job)
		if err != nil {
			return nil, err
		}
		if freshSnapshot {
			mode = config.JobModeInitial
			if normalizeMode(job.Config.Mode) == config.JobModeSnapshotOnly {
				mode = config.JobModeSnapshotOnly
			}
		}
		log.Printf("[job %s] resubmit requested mode=%s previous_status=%s", id, mode, job.GetStatus())
		if m.queueOrStart(job, mode, false) {
			return job, nil
		}
		if err := m.startJob(job, mode, false); err != nil {
			return nil, err
		}
		return job, nil
	default:
		return nil, ErrJobResubmitNotAllowed
	}
}

func (m *JobManager) Get(id string) (*Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}

func (m *JobManager) HasJob(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.jobs[id]
	return ok
}

func (m *JobManager) List() []JobInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]JobInfo, 0, len(m.jobs))
	for _, j := range m.jobs {
		errors := j.GetErrors()
		var lastError *JobError
		if len(errors) > 0 {
			last := errors[len(errors)-1]
			lastError = &last
		}
		health := j.Health()
		out = append(out, JobInfo{
			ID:           j.Config.ID,
			Name:         j.Name(),
			Status:       j.GetStatus(),
			HealthStatus: health.Status,
			HealthDetail: health.Detail,
			Created:      j.CreatedTime().Format("2006-01-02 15:04:05"),
			Updated:      j.UpdatedTime().Format("2006-01-02 15:04:05"),
			MetaKey:      j.MetaKey(),
			SinkType:     sinkTypeFromConfig(j.Config),
			ErrorCount:   len(errors),
			LastError:    lastError,
			Progress:     j.Progress(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Updated != out[j].Updated {
			return out[i].Updated > out[j].Updated
		}
		if out[i].Created != out[j].Created {
			return out[i].Created > out[j].Created
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// RefreshPersistedViews lets the API process display progress written by the
// other split worker. Active local jobs keep their in-memory progress, which
// is newer than the durable copy.
func (m *JobManager) RefreshPersistedViews(ctx context.Context) error {
	if m.jobStore == nil || m.workerRole == WorkerRoleAll {
		return nil
	}
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return err
	}
	records, err := m.jobStore.LoadJobs(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		m.mu.RLock()
		job := m.jobs[record.ID]
		m.mu.RUnlock()
		if job == nil || job.runActive() {
			continue
		}
		m.restoreJobSnapshot(job, record, false)
	}
	return nil
}

func sinkTypeFromConfig(cfg *config.JobConfig) string {
	if cfg == nil {
		return ""
	}
	if cfg.Sink == nil {
		return ""
	}
	if strings.TrimSpace(cfg.Sink.Type) == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(cfg.Sink.Type))
}

func (m *JobManager) RestorePersistedJobs(ctx context.Context) error {
	if m.jobStore == nil {
		return nil
	}
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return err
	}
	if m.workerRole != WorkerRoleAll {
		if _, ok := m.jobStore.(meta.JobWorkerStore); !ok {
			return errors.New("split snapshot/streaming workers require a durable JobWorkerStore")
		}
	}

	records, err := m.jobStore.LoadJobs(ctx)
	if err != nil {
		return err
	}
	if !m.autoResume {
		log.Printf("[job-manager] automatic job resume disabled; loading %d persisted job(s) without starting them", len(records))
	}

	for _, record := range records {
		cfg := m.normalizeConfig(record.Config)
		if cfg == nil {
			log.Printf("[job-manager] skip persisted job %s: empty config", record.ID)
			continue
		}
		if strings.TrimSpace(cfg.ID) == "" {
			cfg.ID = record.ID
		}
		if strings.TrimSpace(cfg.Name) == "" {
			cfg.Name = record.Name
		}
		if strings.TrimSpace(cfg.ID) == "" {
			log.Printf("[job-manager] skip persisted job with empty id")
			continue
		}

		job := m.newManagedJob(cfg)
		resumeRequested := strings.EqualFold(string(record.DesiredState), string(meta.DesiredStateRunning))
		shouldResume := m.workerRole == WorkerRoleAll && m.autoResume && resumeRequested
		m.restoreJobSnapshot(job, record, m.workerRole == WorkerRoleAll && resumeRequested)

		m.mu.Lock()
		if _, exists := m.jobs[cfg.ID]; exists {
			m.mu.Unlock()
			log.Printf("[job-manager] skip duplicate persisted job id=%s", cfg.ID)
			continue
		}
		m.jobs[cfg.ID] = job
		executionRole := record.ExecutionRole
		if executionRole == "" || executionRole == meta.JobExecutionRoleAll {
			if m.workerRole == WorkerRoleAll {
				executionRole = meta.JobExecutionRoleAll
			} else {
				executionRole = executionRoleForConfig(cfg)
			}
		}
		m.executionRoles[cfg.ID] = executionRole
		m.mu.Unlock()
		if m.workerRole != WorkerRoleAll && (record.ExecutionRole == "" || record.ExecutionRole == meta.JobExecutionRoleAll) {
			if err := m.saveManagedJobRecord(ctx, job, record.DesiredState, parsePersistedStatus(record.LastStatus)); err != nil {
				return fmt.Errorf("assign persisted job worker role job=%s: %w", cfg.ID, err)
			}
		}

		if !shouldResume {
			continue
		}
		if m.queueOrStart(job, config.JobModeResume, false) {
			continue
		}
		if err := m.startJob(job, config.JobModeResume, false); err != nil {
			log.Printf("[job-manager] auto-resume failed job=%s: %v", cfg.ID, err)
		}
	}

	return m.restorePendingFailureNotifications(ctx)
}

func (m *JobManager) Delete(id string) error {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()

	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return ErrJobManagerShuttingDown
	}
	job, ok := m.jobs[id]
	if !ok {
		m.mu.Unlock()
		return ErrJobNotFound
	}
	// A stopped job cannot drain its retained receipts any further. Report
	// admission failure synchronously instead of returning "deleted" and then
	// restoring the job asynchronously. Cleanup still checks under SQL locks.
	if !job.runActive() {
		m.mu.Unlock()
		if err := job.checkDeleteAdmission(); err != nil {
			return err
		}
		m.mu.Lock()
		if m.shuttingDown {
			m.mu.Unlock()
			return ErrJobManagerShuttingDown
		}
		if m.jobs[id] != job {
			m.mu.Unlock()
			return ErrJobNotFound
		}
	}
	executionRole := m.executionRoles[id]
	delete(m.jobs, id)
	delete(m.executionRoles, id)
	delete(m.workerLeases, id)
	m.deletingJobs[id] = struct{}{}
	m.removeSnapshotQueueLocked(id)
	delete(m.startingSnapshotJobs, id)
	m.mu.Unlock()

	// Detach from the API immediately, but retain a durable STOPPING row and
	// reservation until every source/sink goroutine has drained.
	job.setStatusListener(nil)
	job.setProgressListener(nil)
	job.requestStop()
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.deletingJobs, id)
			m.mu.Unlock()
		}()
		if err := m.saveManagedJobRecord(context.Background(), job, meta.DesiredStateStopped, JobStatus("STOPPING")); err != nil {
			log.Printf("[job-manager] persist deleting job state failed job=%s: %v", id, err)
		}
		job.markPersistenceDeleted()
		if !job.waitRunDone(30 * time.Second) {
			log.Printf("[job %s] delete still waiting for pipeline shutdown; ownership remains reserved", id)
			if done := job.currentRunDone(); done != nil {
				<-done
			}
		}
		m.applyMaintenanceOwnershipStatus(job, job.GetStatus(), true)
		if err := job.CleanupMeta(); err != nil {
			// A retained intent must keep its job configuration available for recovery.
			// Delete is asynchronous, so surface the failure by restoring a FAILED job.
			job.mu.Lock()
			job.persistenceDeleted = false
			job.mu.Unlock()
			job.addError("metadata cleanup", err)
			job.setStatus(JobStatusFailed)
			m.mu.Lock()
			m.jobs[id] = job
			m.executionRoles[id] = executionRole
			m.mu.Unlock()
			m.attachStatusListener(job)
			if saveErr := m.saveManagedJobRecord(context.Background(), job, meta.DesiredStateStopped, JobStatusFailed); saveErr != nil {
				log.Printf("[job-manager] persist cleanup failure job=%s: %v", id, saveErr)
			}
			return
		}
		if err := m.deletePersistedJobForJob(context.Background(), job, id); err != nil {
			log.Printf("[job-manager] delete persisted job failed job=%s: %v", id, err)
		}
	}()
	return nil
}

func (m *JobManager) normalizeConfig(cfg *config.JobConfig) *config.JobConfig {
	if cfg == nil {
		return nil
	}

	cloned := *cfg
	if strings.TrimSpace(cloned.Meta.MySQLDSN) == "" && m.defaultMetaMySQL != "" {
		cloned.Meta.MySQLDSN = m.defaultMetaMySQL
	}
	config.ApplyDefaults(&cloned)
	return &cloned
}

func (m *JobManager) newManagedJob(cfg *config.JobConfig) *Job {
	job := NewJob(cfg, m.reg)
	m.attachStatusListener(job)
	return job
}

func shouldReleaseMaintenanceOwnership(mode config.JobMode, status JobStatus, deleted bool) bool {
	if deleted {
		return true
	}
	if normalizeMode(mode) == config.JobModeSnapshotOnly {
		return status == JobStatusDone
	}
	return status == JobStatusStopped || status == JobStatusFailed
}

func (m *JobManager) applyMaintenanceOwnershipStatus(job *Job, status JobStatus, deleted bool) {
	if job == nil || job.Config == nil || !shouldReleaseMaintenanceOwnership(job.Config.Mode, status, deleted) {
		return
	}
	job.mu.RLock()
	lifecycle := job.maintenanceOwnership
	job.mu.RUnlock()
	if lifecycle == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var err error
	if !deleted && normalizeMode(job.Config.Mode) == config.JobModeSnapshotOnly && status == JobStatusDone {
		err = lifecycle.SnapshotCompleted(ctx)
	} else {
		err = lifecycle.Release(ctx)
	}
	if err != nil {
		log.Printf("[job-manager] maintenance ownership transition failed job=%s status=%s: %v", job.Config.ID, status, err)
	}
}

func (m *JobManager) deferMaintenanceOwnershipStatus(job *Job, status JobStatus) {
	if job == nil || job.Config == nil {
		return
	}
	done := job.currentRunDone()
	if done == nil {
		m.applyMaintenanceOwnershipStatus(job, status, false)
		return
	}
	key := job.Config.ID + "\x00" + job.SubmissionID()
	m.maintenanceReleaseMu.Lock()
	if _, exists := m.maintenanceReleases[key]; exists {
		m.maintenanceReleaseMu.Unlock()
		return
	}
	m.maintenanceReleases[key] = struct{}{}
	m.maintenanceReleaseMu.Unlock()
	go func() {
		<-done
		m.maintenanceReleaseMu.Lock()
		delete(m.maintenanceReleases, key)
		m.maintenanceReleaseMu.Unlock()
		finalStatus := job.GetStatus()
		if err := m.saveManagedJobRecordIfCurrent(context.Background(), job, finalStatus, m.desiredStateForJobStatus(job.Config.ID, finalStatus), finalStatus); err != nil && !errors.Is(err, ErrJobWorkerLeaseLost) {
			log.Printf("[job-manager] persist drained ownership state failed job=%s status=%s: %v", job.Config.ID, finalStatus, err)
		}
		m.applyMaintenanceOwnershipStatus(job, finalStatus, false)
		if m.workerRole != WorkerRoleAll && snapshotStatusReleasesSlot(finalStatus) {
			m.releaseWorkerLease(job.Config.ID, job.SubmissionID())
		}
		if snapshotStatusReleasesSlot(finalStatus) {
			m.startQueuedSnapshotJobsAsync()
		}
	}()
}

func (m *JobManager) attachStatusListener(job *Job) {
	job.setStatusListener(func(status JobStatus) {
		if status == JobStatusFailed && m.failureNotifier != nil && !m.failureNotificationDeferred(job.Config.ID) {
			// Persist the outbox record as part of handling the FAILED transition,
			// before asynchronous delivery begins.
			m.enqueueFailureNotification(job)
		}
		persistedStatus := status
		desired := m.desiredStateForJobStatus(job.Config.ID, status)
		releaseAfterDrain := shouldReleaseMaintenanceOwnership(job.Config.Mode, status, false) && job.runActive()
		if releaseAfterDrain {
			persistedStatus = JobStatus("STOPPING")
		}
		runMode := job.currentRunMode()
		if m.workerRole == WorkerRoleSnapshot && status == JobStatusDone && normalizeMode(job.Config.Mode) != config.JobModeSnapshotOnly &&
			(runMode == config.JobModeSnapshotHandoff || runMode == config.JobModeSnapshotHandoffResume) {
			m.mu.Lock()
			m.executionRoles[job.Config.ID] = meta.JobExecutionRoleStreaming
			m.mu.Unlock()
			desired = meta.DesiredStateRunning
			persistedStatus = JobStatusStopped
			log.Printf("[job-manager] snapshot handoff ready job=%s next_role=streaming", job.Config.ID)
		}
		if err := m.saveManagedJobRecordIfCurrent(context.Background(), job, status, desired, persistedStatus); err != nil && !errors.Is(err, ErrJobWorkerLeaseLost) {
			log.Printf("[job-manager] persist job state failed job=%s status=%s: %v", job.Config.ID, status, err)
		}
		if releaseAfterDrain {
			m.deferMaintenanceOwnershipStatus(job, status)
		} else {
			m.applyMaintenanceOwnershipStatus(job, status, false)
		}
		if !releaseAfterDrain && m.workerRole != WorkerRoleAll && snapshotStatusReleasesSlot(status) {
			m.releaseWorkerLease(job.Config.ID, job.SubmissionID())
		}
		if !releaseAfterDrain && snapshotStatusReleasesSlot(status) {
			m.startQueuedSnapshotJobsAsync()
		}
	})
	job.setProgressListener(func(progress *JobProgress) {
		m.persistSnapshotProgress(job)
		if snapshotProgressReleasesSlot(progress) {
			m.startQueuedSnapshotJobsAsync()
		}
		m.maybeNotifyJobHealth(job, progress)
	})
}

// Snapshot and streaming workers run in separate containers. Persist a
// throttled worker view so the API/UI control-plane container can show the
// actual phase, table, CDC position, and sink progress from either worker.
func (m *JobManager) persistSnapshotProgress(job *Job) {
	if (m.workerRole != WorkerRoleSnapshot && m.workerRole != WorkerRoleStreaming) || job == nil || job.Config == nil || job.isPersistenceDeleted() {
		return
	}

	now := time.Now()
	m.progressPersistMu.Lock()
	last := m.lastProgressPersist[job.Config.ID]
	if !last.IsZero() && now.Sub(last) < time.Second {
		m.progressPersistMu.Unlock()
		return
	}
	m.lastProgressPersist[job.Config.ID] = now
	m.progressPersistMu.Unlock()

	go func(id string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		status := job.GetStatus()
		if err := m.saveManagedJobRecordIfCurrent(ctx, job, status, m.desiredStateForJobStatus(id, status), status); err != nil && !errors.Is(err, ErrJobWorkerLeaseLost) {
			log.Printf("[job-manager] persist worker progress failed job=%s role=%s: %v", id, m.workerRole, err)
		}
	}(job.Config.ID)
}

// Shutdown drains active jobs to their latest committed checkpoint while
// preserving their RUNNING intent in the job registry. A replacement Rivus
// process can then restore those jobs automatically in resume mode.
func (m *JobManager) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Wait for any accepted lifecycle mutation to finish, then close the gate
	// before deciding which jobs must resume in the replacement process.
	m.lifecycleMu.Lock()
	m.mu.Lock()
	m.shuttingDown = true
	jobs := make([]*Job, 0, len(m.jobs))
	for id, job := range m.jobs {
		if job == nil {
			continue
		}
		if m.workerRole != WorkerRoleAll {
			if _, leased := m.workerLeases[id]; !leased {
				continue
			}
		}
		switch job.GetStatus() {
		case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing:
			m.restartResumeJobs[id] = struct{}{}
			jobs = append(jobs, job)
		}
	}
	m.mu.Unlock()
	m.lifecycleMu.Unlock()

	if len(jobs) == 0 {
		m.closeAllJobMetaStores()
		return nil
	}

	log.Printf("[job-manager] graceful shutdown draining jobs=%d", len(jobs))

	var persistErrs []error
	for _, job := range jobs {
		status := job.GetStatus()
		if err := m.saveManagedJobRecordIfCurrent(ctx, job, status, meta.DesiredStateRunning, status); err != nil {
			persistErrs = append(persistErrs, fmt.Errorf("persist restart intent job=%s: %w", job.Config.ID, err))
		}

		switch status {
		case JobStatusRunning:
			if !job.RequestPause() {
				job.requestStop()
			}
		case JobStatusCreated, JobStatusPending:
			// No source events can be accepted safely until preflight finishes.
			// Cancel setup and restart it from persisted state in the new process.
			job.requestStop()
		case JobStatusQueued, JobStatusPausing:
			// Queued jobs have no active pipeline. Pausing jobs are already
			// draining their source and sink.
		}
	}

	for _, job := range jobs {
		done := job.currentRunDone()
		if done == nil {
			continue
		}
		select {
		case <-done:
		case <-ctx.Done():
			for _, remaining := range jobs {
				remaining.requestStop()
			}
			return errors.Join(
				errors.Join(persistErrs...),
				fmt.Errorf("graceful shutdown deadline exceeded: %w", ctx.Err()),
			)
		}
	}

	for _, job := range jobs {
		status := job.GetStatus()
		if err := m.saveManagedJobRecordIfCurrent(ctx, job, status, meta.DesiredStateRunning, status); err != nil {
			persistErrs = append(persistErrs, fmt.Errorf("persist drained job=%s: %w", job.Config.ID, err))
		} else if m.workerRole != WorkerRoleAll {
			m.releaseWorkerLeaseAfterDrain(job.Config.ID, job.SubmissionID(), true)
		}
	}

	log.Printf("[job-manager] graceful shutdown drain complete jobs=%d", len(jobs))
	m.closeAllJobMetaStores()
	return errors.Join(persistErrs...)
}

func (m *JobManager) closeAllJobMetaStores() {
	m.mu.RLock()
	jobs := make([]*Job, 0, len(m.jobs))
	for _, job := range m.jobs {
		if job != nil {
			jobs = append(jobs, job)
		}
	}
	m.mu.RUnlock()
	for _, job := range jobs {
		job.closeMetaStore()
	}
	if closer, ok := m.jobStore.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

func (m *JobManager) desiredStateForJobStatus(jobID string, status JobStatus) meta.DesiredState {
	m.mu.RLock()
	_, resumeAfterRestart := m.restartResumeJobs[jobID]
	m.mu.RUnlock()
	if resumeAfterRestart {
		return meta.DesiredStateRunning
	}
	return desiredStateForStatus(status)
}

func (m *JobManager) deferFailureNotification(jobID string) {
	m.failureDeliveryMu.Lock()
	m.deferredFailureJobs[jobID] = struct{}{}
	m.failureDeliveryMu.Unlock()
}

func (m *JobManager) finishDeferredFailureNotification(job *Job, accepted bool) {
	if job == nil || job.Config == nil {
		return
	}
	m.failureDeliveryMu.Lock()
	delete(m.deferredFailureJobs, job.Config.ID)
	m.failureDeliveryMu.Unlock()
	if accepted && job.GetStatus() == JobStatusFailed {
		m.enqueueFailureNotification(job)
	}
}

func (m *JobManager) failureNotificationDeferred(jobID string) bool {
	m.failureDeliveryMu.Lock()
	defer m.failureDeliveryMu.Unlock()
	_, ok := m.deferredFailureJobs[jobID]
	return ok
}

func (m *JobManager) enqueueFailureNotification(job *Job) {
	payload, ok := buildJobFailureNotification(job)
	if !ok {
		log.Printf("[job-manager] skipped failed notification job=%s channel=telegram reason=disabled_or_missing_configuration", job.Config.ID)
		return
	}
	notification := buildPersistedFailureNotification(job, payload)
	persisted := false
	if store, ok := m.jobStore.(meta.FailureNotificationStore); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.SaveFailureNotification(ctx, notification)
		cancel()
		if err != nil {
			log.Printf("[job-manager] persist failed notification error job=%s incident=%s: %v", notification.JobID, notification.IncidentID, err)
		} else {
			persisted = true
		}
	}
	m.scheduleFailureNotification(job, notification, persisted)
}

func (m *JobManager) scheduleFailureNotification(job *Job, notification meta.FailureNotification, persisted bool) {
	if job == nil || job.Config == nil || notification.IncidentID == "" {
		return
	}
	m.failureDeliveryMu.Lock()
	if _, ok := m.completedFailureDelivery[notification.IncidentID]; ok {
		m.failureDeliveryMu.Unlock()
		return
	}
	if _, ok := m.activeFailureDeliveries[notification.IncidentID]; ok {
		m.failureDeliveryMu.Unlock()
		return
	}
	m.activeFailureDeliveries[notification.IncidentID] = struct{}{}
	m.failureDeliveryMu.Unlock()

	go m.deliverFailureNotification(job, notification, persisted)
}

func (m *JobManager) deliverFailureNotification(job *Job, notification meta.FailureNotification, persisted bool) {
	defer func() {
		m.failureDeliveryMu.Lock()
		delete(m.activeFailureDeliveries, notification.IncidentID)
		m.failureDeliveryMu.Unlock()
	}()

	store, _ := m.jobStore.(meta.FailureNotificationStore)
	persistBackoff := m.failureRetryInitial
	for store != nil && !persisted {
		if !m.jobIsManaged(job) {
			return
		}
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.SaveFailureNotification(saveCtx, notification)
		cancel()
		if err == nil {
			persisted = true
			break
		}
		log.Printf("[job-manager] persist failed notification error job=%s incident=%s: %v", notification.JobID, notification.IncidentID, err)
		if !waitForFailureNotificationRetry(persistBackoff) {
			return
		}
		persistBackoff = nextFailureNotificationBackoff(persistBackoff, m.failureRetryMax)
	}

	for {
		if !m.jobIsManaged(job) {
			return
		}
		if delay := time.Until(notification.NextAttemptAt); !notification.NextAttemptAt.IsZero() && delay > 0 {
			if !waitForFailureNotificationRetry(delay) {
				return
			}
		}

		payload, ok := jobFailureNotificationFromPersisted(job, notification)
		if !ok {
			notification.State = meta.FailureNotificationFailed
			notification.LastError = "notification disabled or missing configuration"
			notification.UpdatedAt = time.Now().UTC()
			m.saveFailureNotificationState(store, notification)
			log.Printf("[job-manager] stopped failed notification job=%s incident=%s reason=disabled_or_missing_configuration", notification.JobID, notification.IncidentID)
			return
		}

		attemptCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := m.failureNotifier.NotifyJobFailed(attemptCtx, payload)
		cancel()
		notification.Attempts++
		notification.UpdatedAt = time.Now().UTC()

		if err == nil {
			notification.State = meta.FailureNotificationSent
			notification.LastError = ""
			notification.NextAttemptAt = time.Time{}
			notification.SentAt = notification.UpdatedAt
			m.saveFailureNotificationState(store, notification)
			m.failureDeliveryMu.Lock()
			m.completedFailureDelivery[notification.IncidentID] = struct{}{}
			m.failureDeliveryMu.Unlock()
			log.Printf("[job-manager] sent failed notification job=%s channel=telegram incident=%s attempts=%d", notification.JobID, notification.IncidentID, notification.Attempts)
			return
		}

		notification.LastError = err.Error()
		if isPermanentJobFailureDeliveryError(err) {
			notification.State = meta.FailureNotificationFailed
			notification.NextAttemptAt = time.Time{}
			m.saveFailureNotificationState(store, notification)
			log.Printf("[job-manager] permanent failed notification delivery job=%s channel=telegram incident=%s attempts=%d: %v", notification.JobID, notification.IncidentID, notification.Attempts, err)
			return
		}

		delay := m.failureNotificationRetryDelay(notification.Attempts, err)
		notification.State = meta.FailureNotificationPending
		notification.NextAttemptAt = time.Now().UTC().Add(delay)
		m.saveFailureNotificationState(store, notification)
		log.Printf("[job-manager] retry failed notification delivery job=%s channel=telegram incident=%s attempts=%d retry_in=%s: %v",
			notification.JobID, notification.IncidentID, notification.Attempts, delay.Round(time.Millisecond), err)
	}
}

func (m *JobManager) failureNotificationRetryDelay(attempts int, err error) time.Duration {
	if retryAfter := jobFailureRetryAfter(err); retryAfter > 0 {
		return retryAfter
	}
	delay := m.failureRetryInitial
	for i := 1; i < attempts; i++ {
		delay = nextFailureNotificationBackoff(delay, m.failureRetryMax)
	}
	return delay
}

func nextFailureNotificationBackoff(current, max time.Duration) time.Duration {
	if current <= 0 {
		current = defaultFailureNotificationRetryInitial
	}
	next := current * 2
	if max > 0 && next > max {
		return max
	}
	return next
}

func waitForFailureNotificationRetry(delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C
	return true
}

func (m *JobManager) saveFailureNotificationState(store meta.FailureNotificationStore, notification meta.FailureNotification) {
	if store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.SaveFailureNotification(ctx, notification); err != nil {
		log.Printf("[job-manager] persist failed notification state error job=%s incident=%s state=%s: %v",
			notification.JobID, notification.IncidentID, notification.State, err)
	}
}

func (m *JobManager) restorePendingFailureNotifications(ctx context.Context) error {
	store, ok := m.jobStore.(meta.FailureNotificationStore)
	if !ok {
		return nil
	}
	notifications, err := store.LoadPendingFailureNotifications(ctx)
	if err != nil {
		return err
	}
	for _, notification := range notifications {
		m.mu.RLock()
		job := m.jobs[notification.JobID]
		m.mu.RUnlock()
		if job == nil {
			log.Printf("[job-manager] skip orphaned failed notification incident=%s job=%s", notification.IncidentID, notification.JobID)
			continue
		}
		m.scheduleFailureNotification(job, notification, true)
	}
	return nil
}

func (m *JobManager) jobIsManaged(job *Job) bool {
	if job == nil || job.Config == nil {
		return false
	}
	m.mu.RLock()
	managed := m.jobs[job.Config.ID] == job
	m.mu.RUnlock()
	return managed
}

func (m *JobManager) maybeNotifyJobHealth(job *Job, progress *JobProgress) {
	if m.healthNotifier == nil || job == nil || job.GetStatus() != JobStatusRunning || progress == nil {
		return
	}
	tg, ok := jobHealthTelegramConfig(job.Config)
	if !ok {
		return
	}

	purgedCheckpoint := strings.EqualFold(strings.TrimSpace(progress.CDCBinlogStatus), "purged")
	if tg.NotifyCheckpointPurged {
		incident := ""
		if purgedCheckpoint {
			incident = strings.TrimSpace(progress.CDCCheckpointFile) + "->" + strings.TrimSpace(progress.CDCEarliestFile)
		}
		if m.updateJobHealthIncident(job.Config.ID, jobHealthAlertCheckpointPurged, incident) {
			if payload, ok := buildJobHealthNotification(job, progress, jobHealthAlertCheckpointPurged); ok {
				payload.Incident = incident
				m.dispatchJobHealthNotification(payload)
			}
		}
	}

	lagging := !purgedCheckpoint && strings.TrimSpace(progress.CDCLatestFile) != "" &&
		progress.CDCLagFiles >= tg.CDCLagFilesThreshold
	if tg.NotifyCDCLag && m.updateJobHealthIncident(job.Config.ID, jobHealthAlertCDCLag, activeHealthIncident(lagging)) &&
		strings.TrimSpace(progress.CDCLatestFile) != "" &&
		progress.CDCLagFiles >= tg.CDCLagFilesThreshold {
		if payload, ok := buildJobHealthNotification(job, progress, jobHealthAlertCDCLag); ok {
			payload.Incident = "active"
			m.dispatchJobHealthNotification(payload)
		}
	}

	backpressured := isBackpressureProgress(progress)
	if tg.NotifyBackpressure && m.updateJobHealthIncident(job.Config.ID, jobHealthAlertBackpressure, activeHealthIncident(backpressured)) {
		if payload, ok := buildJobHealthNotification(job, progress, jobHealthAlertBackpressure); ok {
			payload.Incident = "active"
			m.dispatchJobHealthNotification(payload)
		}
	}
}

func (m *JobManager) updateJobHealthIncident(jobID string, alertType jobHealthAlertType, incident string) bool {
	key := strings.TrimSpace(jobID) + ":" + string(alertType)
	incident = strings.TrimSpace(incident)
	m.healthAlertMu.Lock()
	defer m.healthAlertMu.Unlock()

	if incident == "" {
		delete(m.healthAlertActive, key)
		delete(m.healthAlertLastSent, key)
		return false
	}
	if m.healthAlertActive[key] == incident {
		return false
	}
	m.healthAlertActive[key] = incident
	delete(m.healthAlertLastSent, key)
	return true
}

func (m *JobManager) clearJobHealthIncident(payload jobHealthNotification) {
	key := payload.JobID + ":" + string(payload.AlertType)
	m.healthAlertMu.Lock()
	if payload.Incident == "" || m.healthAlertActive[key] == payload.Incident {
		delete(m.healthAlertActive, key)
		delete(m.healthAlertLastSent, key)
	}
	m.healthAlertMu.Unlock()
}

func activeHealthIncident(active bool) string {
	if active {
		return "active"
	}
	return ""
}

func (m *JobManager) dispatchJobHealthNotification(payload jobHealthNotification) {
	cooldown := time.Duration(payload.Telegram.AlertCooldownSeconds) * time.Second
	if cooldown <= 0 {
		cooldown = 10 * time.Minute
	}
	key := payload.JobID + ":" + string(payload.AlertType)
	now := time.Now()

	m.healthAlertMu.Lock()
	if last := m.healthAlertLastSent[key]; !last.IsZero() && now.Sub(last) < cooldown {
		m.healthAlertMu.Unlock()
		return
	}
	m.healthAlertLastSent[key] = now
	m.healthAlertMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := m.healthNotifier.NotifyJobHealth(ctx, payload); err != nil {
			m.clearJobHealthIncident(payload)
			log.Printf("[job-manager] health notification delivery failed job=%s alert=%s channel=telegram: %v",
				payload.JobID, payload.AlertType, err)
			return
		}
		log.Printf("[job-manager] health notification sent job=%s alert=%s channel=telegram",
			payload.JobID, payload.AlertType)
	}()
}

func (m *JobManager) restoreJobSnapshot(job *Job, record meta.PersistedJob, resumeOnBoot bool) {
	now := time.Now()
	created := record.CreatedAt
	if created.IsZero() {
		created = now
	}
	updated := record.UpdatedAt
	if updated.IsZero() {
		updated = created
	}
	status := parsePersistedStatus(record.LastStatus)
	var progress *JobProgress
	if len(record.ProgressJSON) > 0 {
		if err := json.Unmarshal(record.ProgressJSON, &progress); err != nil {
			log.Printf("[job-manager] ignore invalid persisted progress job=%s: %v", record.ID, err)
		}
	}
	if resumeOnBoot {
		switch status {
		case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing:
			status = JobStatusStopped
		}
	}

	job.mu.Lock()
	if strings.TrimSpace(record.SubmissionID) != "" {
		job.submissionID = strings.TrimSpace(record.SubmissionID)
	}
	if strings.TrimSpace(record.MetaKey) != "" {
		// The submitting control plane is authoritative for the checkpoint
		// identity. Always accept the durable value so a master that previously
		// derived a local fallback converges with the worker that writes offsets.
		job.metaKey = strings.TrimSpace(record.MetaKey)
	}
	if record.Name != "" {
		job.displayName = record.Name
	}
	if record.Config != nil {
		job.displayConfig = record.Config
	}
	job.Created = created
	job.Updated = updated
	job.status = status
	job.progress = progress
	job.errors = make([]JobError, len(record.Errors))
	for i, persistedErr := range record.Errors {
		job.errors[i] = JobError{
			Component: persistedErr.Component,
			Message:   persistedErr.Message,
			Time:      persistedErr.Time,
		}
	}
	job.mu.Unlock()
}

func (m *JobManager) ensureJobStoreReady(ctx context.Context) error {
	if m.jobStore == nil {
		return nil
	}

	m.jobStoreReadyLock.Lock()
	defer m.jobStoreReadyLock.Unlock()

	if m.jobStoreReady {
		return nil
	}
	if err := m.jobStore.Init(ctx); err != nil {
		return err
	}
	m.jobStoreReady = true
	return nil
}

func (m *JobManager) saveJobRecord(ctx context.Context, job *Job, desired meta.DesiredState, status JobStatus) error {
	if m.jobStore == nil || job == nil || job.Config == nil {
		return nil
	}
	metaKey := job.ensureMetaKey()

	job.mu.RLock()
	cfg := job.Config
	name := ""
	if cfg != nil {
		name = cfg.Name
	}
	created := job.Created
	updated := job.Updated
	var progressJSON []byte
	if job.progress != nil {
		var err error
		progressJSON, err = json.Marshal(job.progress)
		if err != nil {
			job.mu.RUnlock()
			return fmt.Errorf("encode job progress: %w", err)
		}
	}
	errorHistory := make([]meta.PersistedJobError, len(job.errors))
	for i, jobErr := range job.errors {
		errorHistory[i] = meta.PersistedJobError{
			Component: jobErr.Component,
			Message:   jobErr.Message,
			Time:      jobErr.Time,
		}
	}
	job.mu.RUnlock()

	if cfg == nil {
		return nil
	}

	saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.ensureJobStoreReady(saveCtx); err != nil {
		return err
	}

	record := meta.PersistedJob{
		ID:            cfg.ID,
		SubmissionID:  job.SubmissionID(),
		Name:          name,
		MetaKey:       metaKey,
		Config:        m.normalizeConfig(cfg),
		DesiredState:  desired,
		ExecutionRole: m.executionRoleForJob(cfg.ID),
		LastStatus:    string(status),
		Errors:        errorHistory,
		ProgressJSON:  progressJSON,
		CreatedAt:     created,
		UpdatedAt:     updated,
	}
	if m.workerRole == WorkerRoleSnapshot || m.workerRole == WorkerRoleStreaming {
		owner := m.claimedWorkerLeaseOwner(job)
		if owner == "" {
			// A combined API/worker process persists a newly submitted job before
			// any worker has claimed it. Only that pre-claim queued state may use
			// the unguarded insert/upsert path; execution callbacks must be fenced.
			if !job.runActive() && (status == JobStatusCreated || status == JobStatusQueued) {
				return m.jobStore.SaveJob(saveCtx, record)
			}
			return ErrJobWorkerLeaseLost
		}
		claimedStore, ok := m.jobStore.(meta.ClaimedJobStore)
		if !ok {
			return errors.New("durable worker persistence requires a ClaimedJobStore")
		}
		saved, err := claimedStore.SaveClaimedJob(saveCtx, record, owner)
		if err != nil {
			return err
		}
		if !saved {
			return ErrJobWorkerLeaseLost
		}
		return nil
	}
	return m.jobStore.SaveJob(saveCtx, record)
}

func (m *JobManager) claimedWorkerLeaseOwner(job *Job) string {
	if m.workerRole == WorkerRoleAll || job == nil || job.Config == nil || strings.TrimSpace(job.Config.ID) == "" {
		return ""
	}
	m.mu.RLock()
	submissionID, claimed := m.workerLeases[job.Config.ID]
	owner := m.workerID
	m.mu.RUnlock()
	if !claimed || submissionID != job.SubmissionID() {
		return ""
	}
	return owner
}

// saveManagedJobRecord serializes persisted writes with deletion and refuses
// to save a job after deletion has started. Without this guard, a status
// callback already in flight can recreate a record after Delete removes it.
func (m *JobManager) saveManagedJobRecord(ctx context.Context, job *Job, desired meta.DesiredState, status JobStatus) error {
	if job == nil || job.Config == nil {
		return nil
	}

	job.persistenceMu.Lock()
	defer job.persistenceMu.Unlock()

	if job.isPersistenceDeleted() {
		return nil
	}

	return m.saveJobRecord(ctx, job, desired, status)
}

// saveManagedJobRecordIfCurrent prevents a stale persistence callback from
// writing an older status after the job has already advanced. This matters for
// very fast snapshot jobs where RUNNING can race with DONE, and for progress
// callbacks that were captured just before a terminal transition.
func (m *JobManager) saveManagedJobRecordIfCurrent(ctx context.Context, job *Job, expectedStatus JobStatus, desired meta.DesiredState, status JobStatus) error {
	if job == nil || job.Config == nil {
		return nil
	}

	job.persistenceMu.Lock()
	defer job.persistenceMu.Unlock()

	if job.isPersistenceDeleted() {
		return nil
	}
	if job.GetStatus() != expectedStatus {
		return nil
	}

	return m.saveJobRecord(ctx, job, desired, status)
}

func (m *JobManager) deletePersistedJob(ctx context.Context, id string) error {
	if m.jobStore == nil {
		return nil
	}

	deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.ensureJobStoreReady(deleteCtx); err != nil {
		return err
	}
	return m.jobStore.DeleteJob(deleteCtx, id)
}

func (m *JobManager) deletePersistedJobForJob(ctx context.Context, job *Job, id string) error {
	if job == nil {
		return m.deletePersistedJob(ctx, id)
	}

	job.markPersistenceDeleted()
	job.persistenceMu.Lock()
	defer job.persistenceMu.Unlock()
	return m.deletePersistedJob(ctx, id)
}

func desiredStateForStatus(status JobStatus) meta.DesiredState {
	switch status {
	case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing:
		return meta.DesiredStateRunning
	default:
		return meta.DesiredStateStopped
	}
}

func parsePersistedStatus(raw string) JobStatus {
	switch JobStatus(strings.ToUpper(strings.TrimSpace(raw))) {
	case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing, JobStatusPaused, JobStatusFailed, JobStatusStopped, JobStatusStopping, JobStatusDone:
		return JobStatus(strings.ToUpper(strings.TrimSpace(raw)))
	default:
		return JobStatusCreated
	}
}

func snapshotJobLimitFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("RIVUS_MAX_CONCURRENT_SNAPSHOT_JOBS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("RIVUS_MAX_CONCURRENT_SNAPSHOTS"))
	}
	if raw == "" {
		return defaultMaxConcurrentSnapshotJobs
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("[job-manager] invalid snapshot concurrency limit %q, using default %d", raw, defaultMaxConcurrentSnapshotJobs)
		return defaultMaxConcurrentSnapshotJobs
	}
	return limit
}

func ParseWorkerRole(raw string) (WorkerRole, error) {
	role := normalizeWorkerRole(WorkerRole(raw))
	if role == "" {
		return "", fmt.Errorf("invalid RIVUS_WORKER_ROLE %q; expected all, snapshot, or streaming", raw)
	}
	return role, nil
}

func normalizeWorkerRole(role WorkerRole) WorkerRole {
	switch WorkerRole(strings.ToLower(strings.TrimSpace(string(role)))) {
	case "", WorkerRoleAll:
		return WorkerRoleAll
	case WorkerRoleSnapshot:
		return WorkerRoleSnapshot
	case WorkerRoleStreaming:
		return WorkerRoleStreaming
	default:
		return ""
	}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "rivus"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func executionRoleForConfig(cfg *config.JobConfig) meta.JobExecutionRole {
	if cfg == nil {
		return meta.JobExecutionRoleStreaming
	}
	switch normalizeMode(cfg.Mode) {
	case config.JobModeInitial, config.JobModeSnapshotOnly, config.JobModeSnapshotHandoff, config.JobModeSnapshotHandoffResume:
		return meta.JobExecutionRoleSnapshot
	default:
		return meta.JobExecutionRoleStreaming
	}
}

func (m *JobManager) executionRoleForJob(jobID string) meta.JobExecutionRole {
	if m.workerRole == WorkerRoleAll {
		return meta.JobExecutionRoleAll
	}
	m.mu.RLock()
	role := m.executionRoles[jobID]
	m.mu.RUnlock()
	if role == "" {
		return meta.JobExecutionRoleStreaming
	}
	return role
}

func (m *JobManager) durableWorkerRole() meta.JobExecutionRole {
	switch m.workerRole {
	case WorkerRoleSnapshot:
		return meta.JobExecutionRoleSnapshot
	case WorkerRoleStreaming:
		return meta.JobExecutionRoleStreaming
	default:
		return meta.JobExecutionRoleAll
	}
}

// RunWorker continuously reconciles jobs assigned to this process role. It is
// used by the standalone snapshot-worker command and by a streaming API
// server configured with RIVUS_WORKER_ROLE=streaming.
func (m *JobManager) RunWorker(ctx context.Context) error {
	if m.workerRole == WorkerRoleAll {
		return errors.New("durable worker loop requires snapshot or streaming role")
	}
	store, ok := m.jobStore.(meta.JobWorkerStore)
	if !ok {
		return errors.New("durable worker loop requires a JobWorkerStore")
	}
	if err := m.ensureJobStoreReady(ctx); err != nil {
		return err
	}

	log.Printf("[job-manager] worker started role=%s owner=%s poll=%s lease=%s", m.workerRole, m.workerID, m.workerPollInterval, m.workerLeaseDuration)
	// Lease renewal must never share the connector-startup call stack. A slow
	// source/sink constructor previously blocked reconciliation long enough for
	// every already-running job lease to expire while its durable status stayed
	// RUNNING.
	renewCtx, stopRenewal := context.WithCancel(ctx)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		m.runWorkerLeaseRenewal(renewCtx, store)
	}()
	defer func() {
		stopRenewal()
		<-renewalDone
	}()

	if err := m.claimAndStartWorkerJobs(ctx, store); err != nil {
		log.Printf("[job-manager] initial worker reconciliation failed role=%s: %v", m.workerRole, err)
	}
	ticker := time.NewTicker(m.workerPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.claimAndStartWorkerJobs(ctx, store); err != nil && ctx.Err() == nil {
				log.Printf("[job-manager] worker reconciliation failed role=%s: %v", m.workerRole, err)
			}
		}
	}
}

func (m *JobManager) reconcileWorkerJobs(ctx context.Context, store meta.JobWorkerStore) error {
	if err := m.renewWorkerJobLeases(ctx, store); err != nil {
		return err
	}
	return m.claimAndStartWorkerJobs(ctx, store)
}

func (m *JobManager) runWorkerLeaseRenewal(ctx context.Context, store meta.JobWorkerStore) {
	interval := m.workerPollInterval
	if maxInterval := m.workerLeaseDuration / 3; maxInterval > 0 && interval > maxInterval {
		interval = maxInterval
	}
	if interval <= 0 {
		interval = time.Second
	}

	renew := func() {
		if err := m.renewWorkerJobLeases(ctx, store); err != nil && ctx.Err() == nil {
			log.Printf("[job-manager] worker lease renewal failed role=%s: %v", m.workerRole, err)
		}
	}
	renew()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renew()
		}
	}
}

func (m *JobManager) renewWorkerJobLeases(ctx context.Context, store meta.JobWorkerStore) error {
	role := m.durableWorkerRole()

	m.mu.RLock()
	owned := make([]*Job, 0)
	for id := range m.workerLeases {
		job := m.jobs[id]
		if job == nil || m.executionRoles[id] != role {
			continue
		}
		switch job.GetStatus() {
		case JobStatusCreated, JobStatusQueued, JobStatusPending, JobStatusRunning, JobStatusPausing:
			owned = append(owned, job)
		case JobStatusFailed, JobStatusStopped, JobStatusDone:
			// The terminal callback first persists STOPPING while the pipeline
			// drains. Keep its lease until the final status is saved; otherwise
			// a concurrent renewal can detach that callback and strand STOPPING.
			if job.runActive() {
				owned = append(owned, job)
			}
		}
	}
	m.mu.RUnlock()
	// Keep a slow registry row from holding up every other job's heartbeat.
	// Bound each attempt, including SQL pool waits, so renewal can retry while
	// the current lease is still valid. The ownership check remains unchanged.
	const parallelRenewals = 4
	waves := (len(owned) + parallelRenewals - 1) / parallelRenewals
	if waves == 0 {
		return nil
	}
	// Budget the entire pass, not just one attempt: with many blocked rows,
	// a fixed five-second timeout per wave can exceed the lease itself.
	budget := m.workerLeaseDuration / 6
	if budget <= 0 || budget > 5*time.Second {
		budget = 5 * time.Second
	}
	timeout := budget / time.Duration(waves)
	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	renew := func(job *Job) error {
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		renewed, err := store.RenewJobLease(attemptCtx, job.Config.ID, job.SubmissionID(), m.workerID, m.workerLeaseDuration)
		if err != nil {
			return fmt.Errorf("renew worker lease job=%s: %w", job.Config.ID, err)
		}
		if !renewed {
			if job.runActive() {
				log.Printf("[job-manager] worker lease lost; stopping job=%s role=%s", job.Config.ID, m.workerRole)
				// This process no longer owns the durable record. Stop only the
				// local pipeline; a late status callback must not overwrite the
				// desired state or lease now controlled by another worker.
				job.setStatusListener(nil)
				job.setProgressListener(nil)
				job.requestStop()
			}
			m.releaseWorkerLeaseAfterDrainContext(attemptCtx, job.Config.ID, job.SubmissionID(), false)
		}
		return nil
	}
	jobs := make(chan *Job, len(owned))
	for _, job := range owned {
		jobs <- job
	}
	close(jobs)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var errs []error
	for i := 0; i < parallelRenewals && i < len(owned); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := renew(job); err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (m *JobManager) claimAndStartWorkerJobs(ctx context.Context, store meta.JobWorkerStore) error {
	role := m.durableWorkerRole()
	records, err := store.ClaimJobs(ctx, role, m.workerID, defaultWorkerClaimLimit, m.workerLeaseDuration)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := m.startClaimedWorkerJob(record); err != nil {
			log.Printf("[job-manager] claimed job start failed job=%s role=%s: %v", record.ID, m.workerRole, err)
		}
	}
	return nil
}

func (m *JobManager) startClaimedWorkerJob(record meta.PersistedJob) error {
	cfg := m.normalizeConfig(record.Config)
	if cfg == nil || strings.TrimSpace(cfg.ID) == "" {
		return errors.New("claimed job has empty config or id")
	}

	// A deleted job can be resubmitted with the same job ID before the old
	// pipeline has completely stopped. Worker IDs are stable for the process,
	// so the new row may be leased by the same owner. Fence and drain the old
	// in-memory submission before attaching the newly claimed generation.
	m.mu.RLock()
	staleJob := m.jobs[cfg.ID]
	m.mu.RUnlock()
	if staleJob != nil && sameJobSubmission(staleJob, record.SubmissionID) && !sameJobExecutionConfig(staleJob.Config, cfg) && staleJob.runActive() {
		// In-place config upgrades are admitted only after ownership release.
		// A still-draining local run must finish before its config is replaced.
		return fmt.Errorf("cached job config changed while the previous run is draining: %w", ErrJobStillStopping)
	}
	// An in-place protocol upgrade keeps the submission and checkpoint epoch.
	// Replace its cached execution config too, without mutating the config read
	// by API/runtime goroutines or admitting a new run before the old one drains.
	if staleJob != nil && (!sameJobSubmission(staleJob, record.SubmissionID) || !sameJobExecutionConfig(staleJob.Config, cfg)) {
		log.Printf(
			"[job-manager] replacing stale worker submission job=%s old_submission=%s new_submission=%s",
			cfg.ID,
			staleJob.SubmissionID(),
			record.SubmissionID,
		)
		staleJob.markPersistenceDeleted()
		staleJob.setStatusListener(nil)
		staleJob.setProgressListener(nil)
		if staleJob.runActive() {
			staleJob.requestStop()
			if !staleJob.waitRunDone(30 * time.Second) {
				m.releaseWorkerLease(cfg.ID, record.SubmissionID)
				return fmt.Errorf("stale job submission %s is still stopping: %w", cfg.ID, ErrJobStillStopping)
			}
		}
		staleJob.closeMetaStore()
		m.mu.Lock()
		if m.jobs[cfg.ID] == staleJob {
			delete(m.jobs, cfg.ID)
			delete(m.executionRoles, cfg.ID)
			delete(m.workerLeases, cfg.ID)
		}
		m.mu.Unlock()
	}

	m.mu.Lock()
	job := m.jobs[cfg.ID]
	if job == nil {
		job = m.newManagedJob(cfg)
		m.jobs[cfg.ID] = job
	}
	m.executionRoles[cfg.ID] = m.durableWorkerRole()
	m.workerLeases[cfg.ID] = record.SubmissionID
	m.mu.Unlock()
	// A job may be reclaimed by this process after a previous lease was lost.
	// Restore manager callbacks before starting or observing the new lease.
	m.attachStatusListener(job)

	if job.runActive() {
		return nil
	}
	m.restoreJobSnapshot(job, record, true)
	sinkType, _ := job.pickSink()
	job.mu.Lock()
	job.runtimeFence = nil
	if strings.EqualFold(sinkType, "iceberg_native") {
		fencer, capable := m.jobStore.(meta.WorkerOffsetFencer)
		token := meta.WorkerOwnership{JobID: cfg.ID, SubmissionID: record.SubmissionID, Owner: m.workerID}
		job.runtimeFence = func(store meta.OffsetStore, stateKey string) (meta.OffsetStore, error) {
			if !capable {
				return nil, errors.New("Iceberg split worker requires transactional checkpoint ownership fencing")
			}
			token.StateKey = stateKey
			return fencer.FenceWorkerOffsets(store, token)
		}
	}
	job.mu.Unlock()

	mode := config.JobModeResume
	if m.workerRole == WorkerRoleSnapshot {
		// The resume marker distinguishes resubmits from new queued jobs.
		firstAttempt := !record.ResumeRequested && snapshotFirstAttempt(record.LastStatus, true)
		// A job can be paused while it is waiting in the snapshot queue, before
		// MySQL has written its first snapshot checkpoint. In that case PAUSED
		// must not turn an initial job into a resume attempt: there is nothing to
		// resume yet, and the source correctly rejects it. Treat it as a fresh
		// initial snapshot instead.
		if !firstAttempt && normalizeMode(cfg.Mode) == config.JobModeInitial {
			checkpointExists, err := m.snapshotCheckpointExists(job)
			if err != nil {
				log.Printf("[job-manager] snapshot checkpoint inspection skipped job=%s: %v", cfg.ID, err)
			} else if !checkpointExists {
				firstAttempt = snapshotFirstAttempt(record.LastStatus, false)
				log.Printf("[job-manager] starting initial snapshot without checkpoint job=%s previous_status=%s", cfg.ID, record.LastStatus)
			}
		}
		if normalizeMode(cfg.Mode) != config.JobModeSnapshotOnly {
			mode = config.JobModeSnapshotHandoffResume
			if firstAttempt {
				mode = config.JobModeSnapshotHandoff
			}
		} else {
			// Resuming snapshot-only through the ordinary resume mode can enter
			// CDC after completing an interrupted snapshot. Reuse the internal
			// handoff-resume boundary, but keep the durable role as SNAPSHOT so
			// the job finishes instead of being handed to streaming.
			mode = config.JobModeSnapshotHandoffResume
			if firstAttempt {
				mode = config.JobModeSnapshotOnly
			}
		}
	} else if !record.ResumeRequested && (strings.EqualFold(record.LastStatus, string(JobStatusCreated)) || strings.EqualFold(record.LastStatus, string(JobStatusQueued))) {
		mode = normalizeMode(cfg.Mode)
	}

	if m.queueOrStart(job, mode, false) {
		return nil
	}
	return m.startJob(job, mode, false)
}

func sameJobExecutionConfig(a, b *config.JobConfig) bool {
	if a == nil || b == nil {
		return a == b
	}
	left, right := *a, *b
	// Renaming is a display change and must not replace an active pipeline.
	left.Name, right.Name = "", ""
	l, err := json.Marshal(left)
	if err != nil {
		return false
	}
	r, err := json.Marshal(right)
	return err == nil && bytes.Equal(l, r)
}

func sameJobSubmission(job *Job, submissionID string) bool {
	if job == nil {
		return false
	}
	submissionID = strings.TrimSpace(submissionID)
	// Empty IDs exist only in legacy/in-memory stores. MySQLJobStore.Init
	// backfills every durable row before workers can claim it.
	return submissionID == "" || job.SubmissionID() == submissionID
}

func snapshotFirstAttempt(lastStatus string, checkpointExists bool) bool {
	return strings.EqualFold(lastStatus, string(JobStatusCreated)) ||
		strings.EqualFold(lastStatus, string(JobStatusQueued)) ||
		!checkpointExists
}

func (m *JobManager) releaseWorkerLease(jobID, submissionID string) {
	m.releaseWorkerLeaseAfterDrain(jobID, submissionID, false)
}

func (m *JobManager) releaseWorkerLeaseAfterDrain(jobID, submissionID string, shutdownDrainComplete bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.releaseWorkerLeaseAfterDrainContext(ctx, jobID, submissionID, shutdownDrainComplete)
}

func (m *JobManager) releaseWorkerLeaseAfterDrainContext(ctx context.Context, jobID, submissionID string, shutdownDrainComplete bool) {
	m.mu.Lock()
	if _, restarting := m.restartResumeJobs[jobID]; !shutdownDrainComplete && m.shuttingDown && restarting {
		// Shutdown must persist its final drained state while it still owns
		// the lease. PAUSED callbacks used to release it before that write.
		m.mu.Unlock()
		return
	}
	if currentSubmissionID, ok := m.workerLeases[jobID]; ok && currentSubmissionID == submissionID {
		delete(m.workerLeases, jobID)
	}
	m.mu.Unlock()
	store, ok := m.jobStore.(meta.JobWorkerStore)
	if !ok || strings.TrimSpace(jobID) == "" {
		return
	}
	if err := store.ReleaseJobLease(ctx, jobID, submissionID, m.workerID); err != nil {
		log.Printf("[job-manager] release worker lease failed job=%s owner=%s: %v", jobID, m.workerID, err)
	}
}

func (m *JobManager) startJob(job *Job, mode config.JobMode, removeOnStartFailure bool) error {
	if job == nil || job.Config == nil {
		return errors.New("job is nil")
	}
	if m.snapshotGateApplies(job.Config, mode) {
		m.mu.Lock()
		m.startingSnapshotJobs[job.Config.ID] = struct{}{}
		m.mu.Unlock()
		defer func() {
			m.mu.Lock()
			delete(m.startingSnapshotJobs, job.Config.ID)
			m.mu.Unlock()
			m.startQueuedSnapshotJobsAsync()
		}()
	}

	err := job.startWithMode(mode)
	if err == nil {
		status := job.GetStatus()
		desired := m.desiredStateForJobStatus(job.Config.ID, status)
		if saveErr := m.saveManagedJobRecordIfCurrent(context.Background(), job, status, desired, status); saveErr != nil {
			job.requestStop()
			err = saveErr
		}
	}
	if err != nil && removeOnStartFailure {
		m.mu.Lock()
		delete(m.jobs, job.Config.ID)
		delete(m.executionRoles, job.Config.ID)
		delete(m.workerLeases, job.Config.ID)
		m.removeSnapshotQueueLocked(job.Config.ID)
		delete(m.startingSnapshotJobs, job.Config.ID)
		m.mu.Unlock()
		job.setStatusListener(nil)
		job.setProgressListener(nil)
		job.requestStop()
		if deleteErr := m.deletePersistedJobForJob(context.Background(), job, job.Config.ID); deleteErr != nil {
			log.Printf("[job-manager] delete failed submit record job=%s: %v", job.Config.ID, deleteErr)
		}
	}
	return err
}

func (m *JobManager) queueOrStart(job *Job, mode config.JobMode, removeOnStartFailure bool) bool {
	bypassSnapshotGate := normalizeMode(mode) == config.JobModeResume && m.resumeCanBypassSnapshotGate(job)
	m.mu.Lock()
	shouldQueue := !bypassSnapshotGate && m.shouldQueueSnapshotStartLocked(job, mode)
	if shouldQueue {
		m.enqueueSnapshotJobLocked(job.Config.ID, mode)
	}
	m.mu.Unlock()
	if shouldQueue {
		job.setStatus(JobStatusQueued)
		job.updateProgress(connector.ProgressInfo{
			Phase:   "queued",
			Summary: "Waiting in queue",
			Detail:  "The job is waiting for an active snapshot/preflight slot to finish",
		})
		return true
	}
	return false
}

func (m *JobManager) startQueuedSnapshotJobsAsync() {
	go m.startQueuedSnapshotJobs()
}

func (m *JobManager) startQueuedSnapshotJobs() {
	m.lifecycleMu.RLock()
	defer m.lifecycleMu.RUnlock()

	for {
		m.mu.Lock()
		if m.shuttingDown || !m.hasSnapshotSlotLocked() || len(m.snapshotQueue) == 0 {
			m.mu.Unlock()
			return
		}
		id := m.snapshotQueue[0]
		m.snapshotQueue = m.snapshotQueue[1:]
		mode := m.snapshotQueueModes[id]
		delete(m.snapshotQueueModes, id)
		job := m.jobs[id]
		if job == nil || job.GetStatus() != JobStatusQueued {
			m.mu.Unlock()
			continue
		}
		m.startingSnapshotJobs[id] = struct{}{}
		m.mu.Unlock()

		log.Printf("[job-manager] starting queued snapshot job=%s mode=%s", id, mode)
		if err := job.startWithMode(mode); err != nil {
			log.Printf("[job-manager] queued job start failed job=%s: %v", id, err)
			job.setStatus(JobStatusFailed)
		}
		status := job.GetStatus()
		desired := m.desiredStateForJobStatus(job.Config.ID, status)
		if err := m.saveManagedJobRecordIfCurrent(context.Background(), job, status, desired, status); err != nil {
			log.Printf("[job-manager] persist queued job start failed job=%s: %v", id, err)
		}
		m.mu.Lock()
		delete(m.startingSnapshotJobs, id)
		m.mu.Unlock()
	}
}

func (m *JobManager) shouldQueueSnapshotStartLocked(job *Job, mode config.JobMode) bool {
	if job == nil || job.Config == nil || !m.snapshotGateApplies(job.Config, mode) {
		return false
	}
	return !m.hasSnapshotSlotLocked()
}

func (m *JobManager) snapshotGateApplies(cfg *config.JobConfig, mode config.JobMode) bool {
	if cfg == nil || m.maxConcurrentSnapshotJobs == 0 || m.workerRole == WorkerRoleStreaming {
		return false
	}
	switch normalizeMode(mode) {
	case config.JobModeInitial, config.JobModeSnapshotOnly, config.JobModeSnapshotHandoff, config.JobModeSnapshotHandoffResume:
		return true
	case config.JobModeResume:
		stored := normalizeMode(cfg.Mode)
		return stored == config.JobModeInitial || stored == config.JobModeSnapshotOnly
	default:
		return false
	}
}

func (m *JobManager) resumeCanBypassSnapshotGate(job *Job) bool {
	if job == nil || job.Config == nil {
		return false
	}
	storedMode := normalizeMode(job.Config.Mode)
	if storedMode != config.JobModeInitial && storedMode != config.JobModeSnapshotOnly {
		return true
	}
	if progressIndicatesCDC(job.Progress()) {
		return true
	}

	dsn := strings.TrimSpace(job.Config.Meta.MySQLDSN)
	if dsn == "" {
		return false
	}

	metaKey := job.ensureMetaKey()

	store, err := meta.NewMySQLOffsetStore(dsn)
	if err != nil {
		log.Printf("[job-manager] resume checkpoint inspection skipped job=%s: %v", job.Config.ID, err)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		log.Printf("[job-manager] resume checkpoint inspection init skipped job=%s: %v", job.Config.ID, err)
		return false
	}

	st, err := store.GetSnapshotState(ctx, metaKey)
	if err != nil {
		log.Printf("[job-manager] resume snapshot state inspection skipped job=%s: %v", job.Config.ID, err)
		return false
	}
	if st != nil {
		return st.CompletionConfirmed()
	}

	off, err := store.GetOffset(ctx, metaKey)
	if err != nil {
		log.Printf("[job-manager] resume offset inspection skipped job=%s: %v", job.Config.ID, err)
		return false
	}
	return off != nil && strings.TrimSpace(off.BinlogFile) != "" && off.BinlogPos > 0
}

// snapshotCheckpointExists reports whether an initial job has any durable
// checkpoint that can safely be resumed. A PAUSED durable job without one is
// a queued/cancelled first attempt, not an interrupted snapshot.
func (m *JobManager) snapshotCheckpointExists(job *Job) (bool, error) {
	if job == nil || job.Config == nil {
		return false, errors.New("job config is unavailable")
	}

	dsn := strings.TrimSpace(job.Config.Meta.MySQLDSN)
	if dsn == "" {
		return false, errors.New("meta MySQL DSN is unavailable")
	}

	metaKey := job.ensureMetaKey()

	store, err := meta.NewMySQLOffsetStore(dsn)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Init(ctx); err != nil {
		return false, err
	}

	state, err := store.GetSnapshotState(ctx, metaKey)
	if err != nil {
		return false, err
	}
	if state != nil {
		return true, nil
	}

	progress, err := store.GetSnapshotProgress(ctx, metaKey)
	if err != nil {
		return false, err
	}
	if progress != nil {
		return true, nil
	}

	offset, err := store.GetOffset(ctx, metaKey)
	if err != nil {
		return false, err
	}
	return offset != nil && strings.TrimSpace(offset.BinlogFile) != "" && offset.BinlogPos > 0, nil
}

func (m *JobManager) hasSnapshotSlotLocked() bool {
	if m.maxConcurrentSnapshotJobs <= 0 {
		return true
	}
	return m.activeSnapshotJobsLocked() < m.maxConcurrentSnapshotJobs
}

func (m *JobManager) activeSnapshotJobsLocked() int {
	active := len(m.startingSnapshotJobs)
	for id, job := range m.jobs {
		if job == nil || job.Config == nil || !m.snapshotGateApplies(job.Config, job.Config.Mode) {
			continue
		}
		if m.workerRole != WorkerRoleAll {
			if m.executionRoles[id] != m.durableWorkerRole() {
				continue
			}
			if _, leased := m.workerLeases[id]; !leased {
				continue
			}
		}
		status := job.GetStatus()
		if status != JobStatusPending && status != JobStatusRunning && status != JobStatusPausing {
			continue
		}
		if snapshotProgressReleasesSlot(job.Progress()) {
			continue
		}
		active++
	}
	return active
}

func (m *JobManager) enqueueSnapshotJobLocked(id string, mode config.JobMode) {
	if _, exists := m.snapshotQueueModes[id]; exists {
		return
	}
	m.snapshotQueue = append(m.snapshotQueue, id)
	m.snapshotQueueModes[id] = mode
	log.Printf("[job-manager] queued snapshot job=%s mode=%s queue_len=%d active=%d limit=%d",
		id, mode, len(m.snapshotQueue), m.activeSnapshotJobsLocked(), m.maxConcurrentSnapshotJobs)
}

func (m *JobManager) removeSnapshotQueueLocked(id string) {
	delete(m.snapshotQueueModes, id)
	if len(m.snapshotQueue) == 0 {
		return
	}
	out := m.snapshotQueue[:0]
	for _, queuedID := range m.snapshotQueue {
		if queuedID != id {
			out = append(out, queuedID)
		}
	}
	m.snapshotQueue = out
}

func snapshotStatusReleasesSlot(status JobStatus) bool {
	switch status {
	case JobStatusPaused, JobStatusFailed, JobStatusStopped, JobStatusDone:
		return true
	default:
		return false
	}
}

func snapshotProgressReleasesSlot(progress *JobProgress) bool {
	if progress == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(progress.Phase)) {
	case "snapshot_complete", "streaming", "done", "failed", "stopped":
		return true
	default:
		// CDC health is emitted only by a live streaming source. These fields
		// protect the snapshot gate when an older persisted phase was stale.
		return strings.TrimSpace(progress.CDCCurrentFile) != "" ||
			strings.TrimSpace(progress.CDCCheckpointFile) != ""
	}
}

func progressIndicatesCDC(progress *JobProgress) bool {
	if progress == nil {
		return false
	}
	phase := strings.ToLower(strings.TrimSpace(progress.Phase))
	if phase == "streaming" || phase == "snapshot_complete" {
		return true
	}
	detail := strings.ToLower(strings.TrimSpace(progress.Detail))
	return strings.Contains(detail, "cdc streaming") || strings.Contains(detail, "listening from")
}
