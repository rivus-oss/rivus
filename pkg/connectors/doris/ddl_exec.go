package doris

import (
	"context"
	"errors"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Doris reports an error when an ALTER would leave the schema unchanged.
// This is successful for CDC replay and for reconciliation against a target
// that already has the requested schema. Other 1105 errors must still fail.
func isDorisNoopAlterError(stmt string, err error) bool {
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "ALTER TABLE ") {
		return false
	}
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1105 &&
		strings.HasSuffix(strings.ToLower(strings.TrimSpace(mysqlErr.Message)),
			"nothing is changed. please check your alter stmt.")
}

func (s *Sink) execDDL(ctx context.Context, stmt string) error {
	_, err := s.sqlDB.ExecContext(ctx, stmt)
	if isDorisNoopAlterError(stmt, err) {
		log.Printf("[doris][job %s] DDL already applied (no schema change) | stmt=%s", s.jobID, stmt)
		return nil
	}
	return err
}
