package api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
)

const (
	maintenanceStoreInitTimeout = 10 * time.Second
	maintenanceStoreRetryDelay  = 5 * time.Second
)

// Share successful initialization between API and job-detail requests, but do
// not retain a transient startup failure for the lifetime of the master.
func (s *Server) maintenanceStoreForView(ctx context.Context) (*meta.IcebergMaintenanceStore, error) {
	s.maintenanceStoreMu.Lock()
	defer s.maintenanceStoreMu.Unlock()
	if s.maintenanceStore != nil {
		return s.maintenanceStore, nil
	}
	if s.maintenanceStoreErr != nil && time.Now().Before(s.maintenanceRetryAt) {
		return nil, s.maintenanceStoreErr
	}
	dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN"))
	var store *meta.IcebergMaintenanceStore
	var err error
	if dsn == "" {
		err = fmt.Errorf("maintenance metadata store is not configured")
	} else {
		base := context.Background()
		if ctx != nil {
			base = context.WithoutCancel(ctx)
		}
		initCtx, cancel := context.WithTimeout(base, maintenanceStoreInitTimeout)
		initialize := s.maintenanceStoreInit
		if initialize == nil {
			initialize = initializeMaintenanceStore
		}
		store, err = initialize(initCtx, dsn)
		cancel()
		if err == nil && store == nil {
			err = fmt.Errorf("maintenance metadata store is unavailable")
		}
	}
	if err != nil {
		if store != nil {
			_ = store.Close()
		}
		s.maintenanceStoreErr = err
		s.maintenanceRetryAt = time.Now().Add(maintenanceStoreRetryDelay)
		return nil, err
	}
	s.maintenanceStore = store
	s.maintenanceStoreErr = nil
	s.maintenanceRetryAt = time.Time{}
	return store, nil
}

func initializeMaintenanceStore(ctx context.Context, dsn string) (*meta.IcebergMaintenanceStore, error) {
	store, err := meta.NewIcebergMaintenanceStore(dsn)
	if err != nil {
		return nil, err
	}
	if err := store.Init(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}
