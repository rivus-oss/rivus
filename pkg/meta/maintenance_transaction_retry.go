package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

const maintenanceTransactionMaxAttempts = 5

// A cancelled MySQL query can remain blocked on the server after the client
// connection closes. Limit server-side row-lock waits too, and restore the
// connection's setting before returning it to the shared maintenance pool.
func (s *IcebergMaintenanceStore) beginMaintenanceOwnershipTransaction(ctx context.Context) (*sql.Tx, func(), error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	var tx *sql.Tx
	var previous int
	changed := false
	cleanup := func() {
		if tx != nil {
			_ = tx.Rollback()
		}
		if changed {
			resetCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := conn.ExecContext(resetCtx, `SET SESSION innodb_lock_wait_timeout=?`, previous)
			cancel()
			if err != nil {
				// Do not leak a changed session setting to another operation.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
		_ = conn.Close()
	}
	if err := conn.QueryRowContext(ctx, `SELECT @@SESSION.innodb_lock_wait_timeout`).Scan(&previous); err != nil {
		cleanup()
		return nil, nil, err
	}
	if previous > 1 {
		if _, err := conn.ExecContext(ctx, `SET SESSION innodb_lock_wait_timeout=1`); err != nil {
			cleanup()
			return nil, nil, err
		}
		changed = true
	}
	tx, err = conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return tx, cleanup, nil
}

func retryMaintenanceTransaction(ctx context.Context, operation func() error) error {
	var err error
	for attempt := 0; attempt < maintenanceTransactionMaxAttempts; attempt++ {
		err = operation()
		if err == nil || !isRetryableMaintenanceTransactionError(err) {
			return err
		}
		if attempt == maintenanceTransactionMaxAttempts-1 {
			break
		}
		delay := 25 * time.Millisecond * time.Duration(1<<attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func isRetryableMaintenanceTransactionError(err error) bool {
	var mysqlErr *drivermysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1205 || mysqlErr.Number == 1213
}
