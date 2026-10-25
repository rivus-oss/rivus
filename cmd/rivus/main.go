package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gerinsp/rivus/pkg/api"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/connectors/doris"
	"github.com/gerinsp/rivus/pkg/connectors/iceberg"
	"github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

const defaultGracefulShutdownTimeout = 90 * time.Second

func main() {
	logCloser, err := setupLogging()
	if err != nil {
		log.Fatalf("logging setup error: %v", err)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "maintenance-worker":
			if err := runMaintenanceWorkerCommand(os.Args[2:]); err != nil {
				log.Fatalf("maintenance worker error: %v", err)
			}
			return
		case "snapshot-worker":
			if err := runJobWorkerCommand("snapshot-worker", os.Args[2:], core.WorkerRoleSnapshot); err != nil {
				log.Fatalf("snapshot worker error: %v", err)
			}
			return
		case "streaming-worker":
			if err := runJobWorkerCommand("streaming-worker", os.Args[2:], core.WorkerRoleStreaming); err != nil {
				log.Fatalf("streaming worker error: %v", err)
			}
			return
		case "master":
			if err := runServerWithRole(os.Args[2:], core.WorkerRoleMaster); err != nil {
				log.Fatalf("master error: %v", err)
			}
			return
		}
	}

	// Backward-compatible server mode. Existing deployments may continue using
	// RIVUS_WORKER_ROLE=all/snapshot/streaming while migrating to explicit
	// master and standalone worker commands.
	if err := runServer(os.Args[1:]); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func newConnectorRegistry() *connector.Registry {
	reg := connector.NewRegistry()
	mysql.Register(reg)
	doris.Register(reg)
	iceberg.Register(reg)
	return reg
}

func runJobWorkerCommand(command string, args []string, role core.WorkerRole) error {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	workerID := fs.String("worker-id", strings.TrimSpace(os.Getenv("RIVUS_WORKER_ID")), "stable worker identity (defaults to hostname-pid)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if role != core.WorkerRoleSnapshot && role != core.WorkerRoleStreaming {
		return fmt.Errorf("%s requires snapshot or streaming role", command)
	}

	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	if dsn == "" {
		return fmt.Errorf("%s requires RIVUS_META_MYSQL_DSN", command)
	}
	jobStore, err := meta.NewMySQLJobStore(dsn)
	if err != nil {
		return err
	}
	manager := core.NewJobManager(newConnectorRegistry(),
		core.WithJobStore(jobStore),
		core.WithDefaultMetaMySQLDSN(dsn),
		core.WithWorkerRole(role),
		core.WithWorkerID(*workerID),
		core.WithWorkerTiming(workerTimingFromEnv()),
	)
	restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 15*time.Second)
	err = manager.RestorePersistedJobs(restoreCtx)
	restoreCancel()
	if err != nil {
		return err
	}

	workerCtx, stopWorker := context.WithCancel(context.Background())
	stopHeartbeat, err := startRuntimeHeartbeat(workerCtx, dsn, string(role), *workerID)
	if err != nil {
		stopWorker()
		return err
	}
	defer stopHeartbeat()
	var workerWG sync.WaitGroup
	errCh := make(chan error, 2)
	workerWG.Add(2)
	go func() {
		defer workerWG.Done()
		errCh <- manager.RunWorker(workerCtx)
	}()
	go func() {
		defer workerWG.Done()
		errCh <- manager.RunControlObserver(workerCtx)
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	var runErr error
	select {
	case <-signalCtx.Done():
	case runErr = <-errCh:
	}

	timeout := gracefulShutdownTimeoutFromEnv()
	leaseDuration := timeout + 30*time.Second

	// Freeze new reconciliation first. Extend the leases both before and after
	// the worker goroutines exit: the first extension protects jobs already
	// owned when SIGTERM arrives, while the second catches a claim that was
	// completing concurrently with cancellation.
	stopWorker()
	leaseErr := extendOwnedWorkerLeasesForShutdownBounded(manager, jobStore, leaseDuration)
	workerWG.Wait()
	leaseErr = errors.Join(leaseErr, extendOwnedWorkerLeasesForShutdownBounded(manager, jobStore, leaseDuration))
	if leaseErr != nil {
		log.Printf("worker shutdown lease extension error: %v", leaseErr)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := manager.Shutdown(shutdownCtx)
	return errors.Join(runErr, leaseErr, shutdownErr)
}

func extendOwnedWorkerLeasesForShutdownBounded(manager *core.JobManager, store meta.JobWorkerStore, leaseDuration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return extendOwnedWorkerLeasesForShutdown(ctx, manager, store, leaseDuration)
}

func extendOwnedWorkerLeasesForShutdown(ctx context.Context, manager *core.JobManager, store meta.JobWorkerStore, leaseDuration time.Duration) error {
	if manager == nil || store == nil {
		return nil
	}
	owner, leases := manager.WorkerLeaseSnapshot()
	if strings.TrimSpace(owner) == "" || len(leases) == 0 {
		return nil
	}
	if leaseDuration <= 0 {
		leaseDuration = 2 * time.Minute
	}

	var errs []error
	for _, lease := range leases {
		renewed, err := store.RenewJobLease(ctx, lease.JobID, lease.SubmissionID, owner, leaseDuration)
		if err != nil {
			errs = append(errs, fmt.Errorf("extend worker lease job=%s: %w", lease.JobID, err))
			continue
		}
		if !renewed {
			// Cancel/delete may have fenced this lease concurrently. That is safe
			// and should not turn shutdown itself into a failure.
			continue
		}
	}
	return errors.Join(errs...)
}

func runMaintenanceWorkerCommand(args []string) error {
	fs := flag.NewFlagSet("maintenance-worker", flag.ContinueOnError)
	queue := fs.Bool("queue", false, "continue polling the durable maintenance queue")
	pollSeconds := fs.Int("poll-interval-seconds", 0, "worker poll interval in seconds (0 uses env/default)")
	leaseSeconds := fs.Int("lease-seconds", 0, "task lease duration in seconds (0 uses env/default)")
	taskPageSize := fs.Int("task-page-size", 0, "maximum tasks claimed per parent run")
	duePageSize := fs.Int("due-page-size", 0, "maximum due table states read per operation")
	workerID := fs.String("worker-id", strings.TrimSpace(os.Getenv("RIVUS_WORKER_ID")), "stable worker identity (defaults to hostname-pid)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var pollInterval, leaseDuration time.Duration
	if *pollSeconds > 0 {
		pollInterval = time.Duration(*pollSeconds) * time.Second
	}
	if *leaseSeconds > 0 {
		leaseDuration = time.Duration(*leaseSeconds) * time.Second
	}
	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	if dsn == "" {
		return fmt.Errorf("maintenance-worker requires RIVUS_META_MYSQL_DSN")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var stopHeartbeat func()
	for delay := time.Second; ; delay = min(2*delay, 30*time.Second) {
		var err error
		stopHeartbeat, err = startRuntimeHeartbeat(ctx, dsn, "maintenance", *workerID)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		if !*queue || !meta.IsTransientMetadataError(err) {
			return err
		}
		log.Printf("[maintenance-worker] runtime registry unavailable; retrying in %s: %v", delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	defer stopHeartbeat()
	return iceberg.RunMaintenanceWorkerBounded(ctx, dsn, iceberg.MaintenanceWorkerOptions{
		Queue:         *queue,
		PollInterval:  pollInterval,
		LeaseDuration: leaseDuration,
		TaskPageSize:  *taskPageSize,
		DuePageSize:   *duePageSize,
		WorkerID:      strings.TrimSpace(*workerID),
	})
}

func runServer(args []string) error {
	return runServerWithRole(args, "")
}

func runServerWithRole(args []string, forcedRole core.WorkerRole) error {
	fs := flag.NewFlagSet("rivus", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	uiDir := fs.String("ui-dir", "./ui", "UI directory")
	if err := fs.Parse(args); err != nil {
		return err
	}

	workerRole := forcedRole
	if workerRole == "" {
		var err error
		workerRole, err = core.ParseWorkerRole(os.Getenv("RIVUS_WORKER_ROLE"))
		if err != nil {
			return err
		}
	}

	jobManagerOpts := make([]core.JobManagerOption, 0, 5)
	jobManagerOpts = append(jobManagerOpts, core.WithAutoResume(envBool("RIVUS_AUTO_RESUME", false)))
	if workerRole == core.WorkerRoleMaster {
		jobManagerOpts = append(jobManagerOpts, core.WithControlPlaneRole())
	} else {
		jobManagerOpts = append(jobManagerOpts, core.WithWorkerRole(workerRole))
	}
	jobManagerOpts = append(jobManagerOpts,
		core.WithWorkerID(strings.TrimSpace(os.Getenv("RIVUS_WORKER_ID"))),
		core.WithWorkerTiming(workerTimingFromEnv()),
	)

	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	if workerRole == core.WorkerRoleMaster && dsn == "" {
		return errors.New("master requires RIVUS_META_MYSQL_DSN")
	}
	var durableWorkerStore meta.JobWorkerStore
	if dsn != "" {
		jobStore, err := meta.NewMySQLJobStore(dsn)
		if err != nil {
			return err
		}
		durableWorkerStore = jobStore
		jobManagerOpts = append(jobManagerOpts,
			core.WithJobStore(jobStore),
			core.WithDefaultMetaMySQLDSN(dsn),
		)
	}

	jobManager := core.NewJobManager(newConnectorRegistry(), jobManagerOpts...)
	restoreCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := jobManager.RestorePersistedJobs(restoreCtx); err != nil {
		cancel()
		return err
	}
	cancel()

	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	stopHeartbeat := func() {}
	if dsn != "" {
		heartbeatStop, heartbeatErr := startRuntimeHeartbeat(workerCtx, dsn, string(workerRole), strings.TrimSpace(os.Getenv("RIVUS_WORKER_ID")))
		if heartbeatErr != nil {
			return heartbeatErr
		}
		stopHeartbeat = heartbeatStop
	}
	defer stopHeartbeat()
	var workerWG sync.WaitGroup
	var workerErr <-chan error
	if workerRole == core.WorkerRoleSnapshot || workerRole == core.WorkerRoleStreaming {
		ch := make(chan error, 2)
		workerErr = ch
		workerWG.Add(2)
		go func() {
			defer workerWG.Done()
			ch <- jobManager.RunWorker(workerCtx)
		}()
		go func() {
			defer workerWG.Done()
			ch <- jobManager.RunControlObserver(workerCtx)
		}()
	}

	authConfig, err := api.LoadAuthConfigFromEnv()
	if err != nil {
		return err
	}
	apiServer := api.NewServer(jobManager, *uiDir, authConfig)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           apiServer.Router(),
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("Starting rivus role=%s on %s ...", workerRole, *addr)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- httpServer.ListenAndServe()
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	select {
	case err := <-serverErr:
		stopWorker()
		workerWG.Wait()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-workerErr:
		stopWorker()
		workerWG.Wait()
		if err != nil {
			_ = httpServer.Close()
			return err
		}
		return nil
	case <-signalCtx.Done():
		timeout := gracefulShutdownTimeoutFromEnv()
		leaseDuration := timeout + 30*time.Second
		log.Printf("Shutdown signal received; draining jobs for up to %s", timeout)

		stopWorker()
		var leaseErr error
		if workerRole == core.WorkerRoleSnapshot || workerRole == core.WorkerRoleStreaming {
			leaseErr = extendOwnedWorkerLeasesForShutdownBounded(jobManager, durableWorkerStore, leaseDuration)
		}
		workerWG.Wait()
		if workerRole == core.WorkerRoleSnapshot || workerRole == core.WorkerRoleStreaming {
			leaseErr = errors.Join(leaseErr, extendOwnedWorkerLeasesForShutdownBounded(jobManager, durableWorkerStore, leaseDuration))
			if leaseErr != nil {
				log.Printf("worker shutdown lease extension error: %v", leaseErr)
			}
		}

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
		drainErr := jobManager.Shutdown(shutdownCtx)
		shutdownCancel()
		if drainErr != nil {
			log.Printf("job drain error: %v", drainErr)
		}

		httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := httpServer.Shutdown(httpCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
			_ = httpServer.Close()
		}
		httpCancel()
		log.Printf("Rivus shutdown complete")
		return nil
	}
}

func workerTimingFromEnv() (time.Duration, time.Duration) {
	poll := time.Duration(envInt("RIVUS_WORKER_POLL_INTERVAL_SECONDS", 2)) * time.Second
	lease := time.Duration(envInt("RIVUS_WORKER_LEASE_SECONDS", 30)) * time.Second
	return poll, lease
}

func gracefulShutdownTimeoutFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("RIVUS_SHUTDOWN_TIMEOUT_SECONDS"))
	if raw == "" {
		return defaultGracefulShutdownTimeout
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		log.Printf("invalid RIVUS_SHUTDOWN_TIMEOUT_SECONDS=%q; using %s", raw, defaultGracefulShutdownTimeout)
		return defaultGracefulShutdownTimeout
	}
	return time.Duration(seconds) * time.Second
}

func setupLogging() (io.Closer, error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := logConfigFromEnv()
	if !cfg.enabled {
		return nil, nil
	}

	w, err := newRotatingLogWriter(cfg)
	if err != nil {
		return nil, err
	}

	output := io.Writer(w)
	if cfg.stderrEnabled {
		output = io.MultiWriter(os.Stderr, w)
	}
	log.SetOutput(output)
	log.Printf("[logging] writing logs dir=%s prefix=%s retention_days=%d max_size_mb=%d max_total_size_mb=%d stderr=%t",
		cfg.dir, cfg.prefix, cfg.retentionDays, cfg.maxSizeMB, cfg.maxTotalSizeMB, cfg.stderrEnabled)
	return w, nil
}
