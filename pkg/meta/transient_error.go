package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"

	drivermysql "github.com/go-sql-driver/mysql"
)

// IsTransientMetadataError identifies availability failures that can be retried
// at a durable coordinator boundary. It must not be used to blindly repeat an
// Iceberg commit or to bypass a failed ownership fence.
func IsTransientMetadataError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	for _, target := range []error{context.DeadlineExceeded, sql.ErrTxDone, driver.ErrBadConn, drivermysql.ErrInvalidConn, io.EOF, io.ErrUnexpectedEOF, ErrMaintenanceDiscoverySuperseded} {
		if errors.Is(err, target) {
			return true
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	var mysqlError *drivermysql.MySQLError
	if errors.As(err, &mysqlError) {
		switch mysqlError.Number {
		case 1040, 1042, 1053, 1158, 1159, 1160, 1161, 1205, 1213, 2006, 2013, 3024:
			return true
		}
	}
	return false
}
