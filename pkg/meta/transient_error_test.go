package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"
)

func TestTransientMetadataAvailabilityFailures(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded, sql.ErrTxDone, driver.ErrBadConn, drivermysql.ErrInvalidConn, io.EOF,
		&net.DNSError{Err: "i/o timeout", Name: "meta-mysql", IsTimeout: true},
		&drivermysql.MySQLError{Number: 1205}, &drivermysql.MySQLError{Number: 1213},
	} {
		if !IsTransientMetadataError(fmt.Errorf("reconcile maintenance reservations: %w", err)) {
			t.Errorf("availability failure should be retryable: %v", err)
		}
	}
	for _, err := range []error{nil, context.Canceled, sql.ErrNoRows, errors.New("worker ownership lost"), errors.New("invalid maintenance config"), &drivermysql.MySQLError{Number: 1045}} {
		if IsTransientMetadataError(err) {
			t.Errorf("configuration, cancellation or ownership error must not be retried as availability: %v", err)
		}
	}
}
