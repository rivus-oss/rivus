package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/connector"
)

type scopeTestSource struct {
	closingSource
	resolveErr error
}

func (s *scopeTestSource) Tables() []connector.TableRef {
	return []connector.TableRef{{Schema: "app", Table: "orders"}, {Schema: "archive", Table: "orders"}}
}

func (s *scopeTestSource) ResolveSnapshotScope(ctx context.Context, jctx connector.JobContext) (any, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return map[string]any{"cutoff": "2026-10-01"}, s.resolveErr
}

func TestSnapshotScopeIsResolvedBeforeSinkConstruction(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "resolved", true: "blocked"}[fail], func(t *testing.T) {
			source := &scopeTestSource{}
			if fail {
				source.resolveErr = errors.New("unclear cutoff")
			}
			reg := connector.NewRegistry()
			reg.RegisterSource("test_source", func(connector.JobContext, any) (connector.Source, error) { return source, nil })
			built := 0
			reg.RegisterSink("test_sink", func(jctx connector.JobContext, cfg any) (connector.Sink, error) {
				built++
				if len(jctx.SourceTables) != 2 || jctx.SourceTables[0].Schema != "app" || jctx.SourceTables[1].Schema != "archive" {
					t.Error("sink did not receive the resolved source table selection")
				}
				if cfg.(map[string]any)["cutoff"] != "2026-10-01" || jctx.SinkConfig.(map[string]any)["cutoff"] != "2026-10-01" {
					t.Error("sink did not receive the resolved scope")
				}
				return &closingSink{}, nil
			})
			job := NewJob(newTestJobConfig("snapshot-scope"), reg)
			job.metaStore = &closingOffsetStore{}
			err := job.startWithMode(config.JobModeLatest)
			if fail {
				if err == nil || built != 0 || source.closes() != 1 {
					t.Fatalf("constructed sink or leaked source after failed scope: err=%v built=%d closes=%d", err, built, source.closes())
				}
				return
			}
			if err != nil || built != 1 || !job.waitRunDone(2*time.Second) {
				t.Fatalf("resolved job failed: err=%v built=%d", err, built)
			}
			if len(job.Config.Sink.Config) != 0 {
				t.Fatal("runtime scope changed the persisted job configuration")
			}
		})
	}
}
