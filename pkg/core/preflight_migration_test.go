package core

import (
	"context"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/model"
)

type preparationTimeoutSink struct {
	countResumeSink
	timeout   time.Duration
	remaining time.Duration
}

func (s *preparationTimeoutSink) TablePreparationTimeout() time.Duration { return s.timeout }
func (s *preparationTimeoutSink) EnsureTable(ctx context.Context, schema, table string, source *model.TableSchema) error {
	deadline, ok := ctx.Deadline()
	if ok {
		s.remaining = time.Until(deadline)
	}
	return s.countResumeSink.EnsureTable(ctx, schema, table, source)
}

func TestPreflightHonorsMigrationTimeoutAndParentCancellation(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		timeout, parent, want time.Duration
	}{
		{"migration", 900 * time.Second, 0, 900 * time.Second},
		{"ordinary minimum", 10 * time.Second, 0, 45 * time.Second},
		{"parent deadline", 900 * time.Second, time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.parent > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.parent)
				defer cancel()
			}
			source := &countResumeSource{tables: []connector.TableRef{{Schema: "app", Table: "orders"}}}
			sink := &preparationTimeoutSink{timeout: tc.timeout}
			job := NewJob(newTestJobConfig("migration-preflight"), connector.NewRegistry())
			if err := job.preflight(ctx, source, sink, config.JobModeLatest); err != nil {
				t.Fatal(err)
			}
			if sink.remaining > tc.want || sink.remaining < tc.want-time.Second {
				t.Fatalf("preflight deadline remaining=%s want approximately=%s", sink.remaining, tc.want)
			}
		})
	}
}
