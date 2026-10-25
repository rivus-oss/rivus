package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

type Server struct {
	jobManager           *core.JobManager
	uiDir                string
	metrics              *MetricsSampler
	auth                 AuthConfig
	authSessions         *authSessionStore
	runtimeStoreOnce     sync.Once
	runtimeStore         runtimeInstanceReader
	runtimeStoreErr      error
	maintenanceStoreMu   sync.Mutex
	maintenanceStore     *meta.IcebergMaintenanceStore
	maintenanceStoreErr  error
	maintenanceRetryAt   time.Time
	maintenanceStoreInit func(context.Context, string) (*meta.IcebergMaintenanceStore, error)
	maintenanceMonitors  maintenanceMonitorRepository
}

func NewServer(jm *core.JobManager, uiDir string, auth AuthConfig) *Server {
	ms, err := NewMetricsSampler()
	if err != nil {
		// if failed, keep running without metrics
		ms = nil
	} else {
		ms.Start()
	}

	return &Server{
		jobManager:   jm,
		uiDir:        uiDir,
		metrics:      ms,
		auth:         auth,
		authSessions: newAuthSessionStore(auth),
	}
}

func noStoreUI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Dashboard assets are deployed together with the binary. Do not let an
		// old ES module survive a rollout and keep stale UI behavior in the browser.
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /auth/status", s.handleAuthStatus)
	mux.HandleFunc("GET /login", s.handleLoginPage)
	mux.HandleFunc("GET /login.html", s.handleLoginPage)
	mux.HandleFunc("GET /favicon.svg", s.handleFavicon)
	mux.HandleFunc("GET /rivus-favicon.svg", s.handleFavicon)
	mux.HandleFunc("GET /rivus-logo.png", s.handleLogo)
	mux.HandleFunc("GET /api/version", s.handleVersion)
	mux.HandleFunc("GET /api/runtime/versions", s.requireAPIAuth(s.handleRuntimeVersions))
	mux.HandleFunc("POST /auth/login", s.handleLogin)
	mux.HandleFunc("POST /auth/logout", s.handleLogout)
	mux.HandleFunc("GET /metrics", s.handlePrometheusMetrics)

	mux.HandleFunc("/api/jobs", s.requireAPIAuth(s.handleJobs))
	mux.HandleFunc("GET /api/jobs/{id}", s.requireAPIAuth(s.handleJobDetail))
	mux.HandleFunc("POST /api/jobs/{id}/iceberg/orphans", s.requireAPIAuth(s.handleQueuedJobIcebergOrphans))
	mux.HandleFunc("POST /api/jobs/{id}/iceberg/maintenance", s.requireAPIAuth(s.handleQueuedJobIcebergMaintenance))
	mux.HandleFunc("/api/jobs/", s.requireAPIAuth(s.handleJobByID))

	mux.HandleFunc("GET /api/iceberg/maintenance/summary", s.requireAPIAuth(s.handleMaintenanceSummary))
	mux.HandleFunc("GET /api/iceberg/maintenance/failures", s.requireAPIAuth(s.handleMaintenanceFailures))
	mux.HandleFunc("POST /api/iceberg/maintenance/failures/{id}/retry", s.requireAPIAuth(s.handleMaintenanceFailureRetry))
	mux.HandleFunc("GET /api/iceberg/maintenance/runs", s.requireAPIAuth(s.handleMaintenanceRuns))
	mux.HandleFunc("GET /api/iceberg/maintenance/runs/{id}", s.requireAPIAuth(s.handleMaintenanceRun))
	mux.HandleFunc("GET /api/iceberg/maintenance/tables/{key...}", s.requireAPIAuth(s.handleMaintenanceTableState))
	mux.HandleFunc("GET /api/iceberg/maintenance/monitors", s.requireAPIAuth(s.handleMaintenanceMonitors))
	mux.HandleFunc("POST /api/iceberg/maintenance/monitors", s.requireAPIAuth(s.handleMaintenanceMonitors))
	mux.HandleFunc("GET /api/iceberg/maintenance/monitors/{id}", s.requireAPIAuth(s.handleMaintenanceMonitor))
	mux.HandleFunc("DELETE /api/iceberg/maintenance/monitors/{id}", s.requireAPIAuth(s.handleMaintenanceMonitor))
	mux.HandleFunc("POST /api/iceberg/maintenance/monitors/{id}/pause", s.requireAPIAuth(s.handleMaintenanceMonitorPause))
	mux.HandleFunc("POST /api/iceberg/maintenance/monitors/{id}/resume", s.requireAPIAuth(s.handleMaintenanceMonitorResume))
	mux.HandleFunc("POST /api/iceberg/maintenance/monitors/{id}/run", s.requireAPIAuth(s.handleMaintenanceMonitorRun))

	mux.HandleFunc("/api/metrics", s.requireAPIAuth(s.handleMetrics))
	mux.HandleFunc("/api/table-metrics", s.requireAPIAuth(s.handleTableMetrics))
	// The master reads all role directories from one shared log root while each
	// runtime keeps its own rotating files under master/streaming/snapshot/maintenance.
	mux.HandleFunc("GET /api/logs", s.requireAPIAuth(s.handleSplitLogs))
	mux.HandleFunc("GET /api/logs/tail", s.requireAPIAuth(s.handleSplitLogTail))
	mux.HandleFunc("GET /api/logs/download", s.requireAPIAuth(s.handleSplitLogDownload))

	mux.Handle("GET /api/jobs/{id}/graph", s.requireAPIAuth(s.handleGetJobGraph))

	fs := http.FileServer(http.Dir(s.uiDir))
	mux.Handle("/", s.requirePageAuth(noStoreUI(fs)))

	return mux
}
