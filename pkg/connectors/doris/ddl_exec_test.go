package doris

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/model"
)

func dorisNoopTestError() error {
	return &mysql.MySQLError{Number: 1105, Message: "errCode = 2, detailMessage = Nothing is changed. please check your alter stmt."}
}

func TestIsDorisNoopAlterError(t *testing.T) {
	const alter = "ALTER TABLE `asmat_daytrans`.`tbl_md_kota` MODIFY COLUMN `ImageUrl` VARCHAR(1000) NULL"
	for _, tt := range []struct {
		name string
		stmt string
		err  error
		want bool
	}{
		{"unchanged alter", alter, dorisNoopTestError(), true},
		{"wrapped error", alter, fmt.Errorf("exec: %w", dorisNoopTestError()), true},
		{"success", alter, nil, false},
		{"other Doris error", alter, &mysql.MySQLError{Number: 1105, Message: "errCode = 2, detailMessage = No backend available for load"}, false},
		{"other code", alter, &mysql.MySQLError{Number: 1064, Message: "Nothing is changed. please check your alter stmt."}, false},
		{"untyped error", alter, errors.New("Nothing is changed. please check your alter stmt."), false},
		{"other statement", "TRUNCATE TABLE `db`.`tbl`", dorisNoopTestError(), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDorisNoopAlterError(tt.stmt, tt.err); got != tt.want {
				t.Fatalf("isDorisNoopAlterError() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestApplyDDLContinuesAfterUnchangedModify(t *testing.T) {
	var executed []string
	sink := newDDLTestSink(t, func(stmt string) error {
		executed = append(executed, stmt)
		if len(executed) == 1 {
			return dorisNoopTestError()
		}
		return nil
	})
	err := sink.applyDDL(context.Background(), "asmat_daytrans", "tbl_md_kota", model.Event{
		DDL: "ALTER TABLE tbl_md_kota MODIFY COLUMN ImageUrl varchar(1000) NULL, ADD COLUMN note text NULL",
	})
	if err != nil {
		t.Fatalf("applyDDL() returned error: %v", err)
	}
	want := []string{
		"ALTER TABLE `asmat_daytrans`.`tbl_md_kota` MODIFY COLUMN `ImageUrl` VARCHAR(1000) NULL",
		"ALTER TABLE `asmat_daytrans`.`tbl_md_kota` ADD COLUMN `note` STRING NULL",
	}
	if !reflect.DeepEqual(executed, want) {
		t.Fatalf("executed = %q, want %q", executed, want)
	}
}

func TestApplyDDLRetriesOnlyFailedStatement(t *testing.T) {
	var executed []string
	sink := newDDLTestSink(t, func(stmt string) error {
		executed = append(executed, stmt)
		if len(executed) == 2 {
			return errors.New("temporary connection failure")
		}
		return nil
	})
	err := sink.applyDDL(context.Background(), "analytics", "orders", model.Event{
		DDL: "ALTER TABLE orders ADD COLUMN note text NULL, ADD COLUMN status int NULL",
	})
	if err != nil {
		t.Fatalf("applyDDL() returned error: %v", err)
	}
	want := []string{
		"ALTER TABLE `analytics`.`orders` ADD COLUMN `note` STRING NULL",
		"ALTER TABLE `analytics`.`orders` ADD COLUMN `status` BIGINT NULL",
		"ALTER TABLE `analytics`.`orders` ADD COLUMN `status` BIGINT NULL",
	}
	if !reflect.DeepEqual(executed, want) {
		t.Fatalf("executed = %q, want %q", executed, want)
	}
}

func TestApplyDDLPreservesRealError(t *testing.T) {
	wantErr := &mysql.MySQLError{Number: 1105, Message: "errCode = 2, detailMessage = Invalid column order"}
	attempts := 0
	sink := newDDLTestSink(t, func(string) error {
		attempts++
		return wantErr
	})
	err := sink.applyDDL(context.Background(), "analytics", "orders", model.Event{
		DDL: "ALTER TABLE orders MODIFY COLUMN note text NULL",
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want wrapped %v", err, wantErr)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func newDDLTestSink(t *testing.T, exec func(string) error) *Sink {
	t.Helper()
	db := sql.OpenDB(ddlTestConnector{exec: exec})
	t.Cleanup(func() { _ = db.Close() })
	return &Sink{
		sqlDB: db,
		retry: config.RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	}
}

type ddlTestConnector struct{ exec func(string) error }

func (c ddlTestConnector) Connect(context.Context) (driver.Conn, error) {
	return ddlTestConn{exec: c.exec}, nil
}

func (c ddlTestConnector) Driver() driver.Driver { return ddlTestDriver{} }

type ddlTestDriver struct{}

func (ddlTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type ddlTestConn struct{ exec func(string) error }

func (c ddlTestConn) ExecContext(_ context.Context, stmt string, _ []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), c.exec(stmt)
}

func (ddlTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}

func (ddlTestConn) Begin() (driver.Tx, error) { return nil, errors.New("begin unsupported") }
func (ddlTestConn) Close() error              { return nil }
