package iceberg

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
)

func TestMonitorSpecificityOrder(t *testing.T) {
	if !(specificityTable > specificityNamespace &&
		specificityNamespace > specificityCatalog &&
		specificityCatalog > specificityMetalake) {
		t.Fatal("monitor specificity order is invalid")
	}
}

func TestMonitorDiscoveryDeltaLeavesUnchangedTargetsUntouched(t *testing.T) {
	previous := []maintenanceMonitorTarget{{Catalog: "asmat", Namespace: "sales", Table: "orders"}}
	delta := maintenanceDiscoveryDelta(previous, previous)
	if len(delta.Added) != 0 || len(delta.Removed) != 0 {
		t.Fatalf("unexpected delta: %#v", delta)
	}
}

func TestRepeatedLargeDiscoveryProducesNoTargetChanges(t *testing.T) {
	targets := make([]maintenanceMonitorTarget, 6700)
	for i := range targets {
		targets[i] = maintenanceMonitorTarget{
			Catalog: "asmat", Namespace: "warehouse", Table: fmt.Sprintf("table_%04d", i),
		}
	}
	delta := maintenanceDiscoveryDelta(targets, append([]maintenanceMonitorTarget(nil), targets...))
	if len(delta.Added) != 0 || len(delta.Removed) != 0 {
		t.Fatalf("unchanged 6,700-table discovery produced delta: added=%d removed=%d", len(delta.Added), len(delta.Removed))
	}
}

func TestFailedDiscoveryDoesNotRetireTargets(t *testing.T) {
	previous := []maintenanceMonitorTarget{{Catalog: "asmat", Namespace: "sales", Table: "orders"}}
	got := targetsAfterDiscovery(previous, nil, errors.New("gravitino timeout"))
	if !reflect.DeepEqual(got, previous) {
		t.Fatalf("targets = %#v, want previous %#v", got, previous)
	}
}

func TestSuccessfulEmptyDiscoveryRetiresTargets(t *testing.T) {
	previous := []maintenanceMonitorTarget{{Catalog: "asmat", Namespace: "sales", Table: "orders"}}
	delta := maintenanceDiscoveryDelta(previous, []maintenanceMonitorTarget{})
	if len(delta.Removed) != 1 || delta.Removed[0] != previous[0] {
		t.Fatalf("removed = %#v, want %#v", delta.Removed, previous)
	}
}

func TestEqualSpecificityOwnershipIsStable(t *testing.T) {
	current := monitorOwnershipCandidate{OwnerID: "monitor:first", Specificity: specificityNamespace}
	candidate := monitorOwnershipCandidate{OwnerID: "monitor:second", Specificity: specificityNamespace}
	if got := preferMonitorOwner(current, candidate); got != current {
		t.Fatalf("winner = %#v, want stable current owner %#v", got, current)
	}
	if got := preferMonitorOwner(candidate, current); got != candidate {
		t.Fatalf("winner = %#v, want stable current owner %#v", got, candidate)
	}
}

type maintenanceDiscoveryCatalogStub struct {
	namespaces map[string][]icetable.Identifier
	tables     map[string][]icetable.Identifier
	tableCalls map[string]int
}

func (s maintenanceDiscoveryCatalogStub) ListNamespaces(_ context.Context, parent icetable.Identifier) ([]icetable.Identifier, error) {
	return s.namespaces[strings.Join(parent, ".")], nil
}

func (s maintenanceDiscoveryCatalogStub) ListTables(_ context.Context, namespace icetable.Identifier) iter.Seq2[icetable.Identifier, error] {
	return func(yield func(icetable.Identifier, error) bool) {
		key := strings.Join(namespace, ".")
		if s.tableCalls != nil {
			s.tableCalls[key]++
		}
		for _, table := range s.tables[key] {
			if !yield(table, nil) {
				return
			}
		}
	}
}

func maintenanceMonitorConfigForTest() *config.JobConfig {
	return &config.JobConfig{
		ID:   "warehouse-maintenance",
		Name: "Warehouse Maintenance",
		Mode: config.JobModeMaintenanceOnly,
		Sink: &config.ConnectorSpec{
			Type: "iceberg_native",
			Config: map[string]any{
				"rest_uri":  "http://iceberg-rest:8181",
				"warehouse": "asmat",
				"table_maintenance": map[string]any{
					"enabled":      true,
					"executor":     "native",
					"catalog_name": "asmat",
					"tables": []any{
						map[string]any{"namespace": "barayax_bronze", "table": "tbl_absen"},
						map[string]any{"namespace": "barayax_bronze", "table": "tbl_absen"},
					},
				},
			},
		},
	}
}

func TestPrepareMaintenanceMonitorConfigNormalizesExplicitTables(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["native_expire_interval_seconds"] = 12345
	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		t.Fatalf("PrepareMaintenanceMonitorConfig returned error: %v", err)
	}
	if normalized.Mode != config.JobModeMaintenanceOnly || normalized.Source != nil {
		t.Fatalf("normalized monitor mode/source = %s/%#v", normalized.Mode, normalized.Source)
	}
	if len(targets) != 1 || targets[0].Namespace != "barayax_bronze" || targets[0].Table != "tbl_absen" {
		t.Fatalf("normalized targets = %#v", targets)
	}
	if got := rawInt(rawMaintenanceMap(normalized.Sink.Config), "native_expire_interval_seconds", 0); got != 12345 {
		t.Fatalf("raw maintenance setting was not preserved: got %d", got)
	}
	catalog, executor, profile, described, err := DescribeMaintenanceMonitorConfig(normalized)
	if err != nil {
		t.Fatalf("DescribeMaintenanceMonitorConfig returned error: %v", err)
	}
	if catalog != "asmat" || executor != "native" || profile != "small" || len(described) != 1 {
		t.Fatalf("description catalog=%q executor=%q profile=%q targets=%#v", catalog, executor, profile, described)
	}
}

func TestPrepareMaintenanceMonitorConfigRejectsIngestionAndTableWildcards(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	cfg.Source = &config.ConnectorSpec{Type: "mysql", Config: map[string]any{}}
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "must not define a source") {
		t.Fatalf("source validation error = %v", err)
	}

	cfg = maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["tables"] = []any{map[string]any{"namespace": "barayax_bronze", "table": "*"}}
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "catalog_monitoring") {
		t.Fatalf("wildcard validation error = %v", err)
	}
}

func TestPrepareMaintenanceMonitorConfigAcceptsCatalogMonitoring(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	delete(maintenance, "tables")
	maintenance["catalog_monitoring"] = map[string]any{
		"enabled": true, "api_uri": "http://gravitino:8090", "metalake": "lakehouse",
		"catalog_patterns": []any{"*"}, "namespace_patterns": []any{"tenant_*"}, "table_patterns": []any{"*"},
	}
	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		t.Fatalf("catalog monitoring config returned error: %v", err)
	}
	if len(targets) != 0 {
		t.Fatalf("static targets = %#v, want none", targets)
	}
	catalog, _, _, _, err := DescribeMaintenanceMonitorConfig(normalized)
	if err != nil {
		t.Fatal(err)
	}
	if catalog != "auto:*" {
		t.Fatalf("catalog label = %q, want auto:*", catalog)
	}
}

func TestCatalogMonitoringExclusionsAreScopedByCatalogNamespaceAndTable(t *testing.T) {
	monitoring := config.IcebergCatalogMonitoringConfig{Exclude: []config.IcebergCatalogMonitoringExclusion{
		{Catalog: "archive"},
		{Catalog: "asmat", Namespace: "analytics"},
		{Catalog: "asmat", Namespace: "reporting", Table: "temporary_*"},
	}}
	for _, target := range []maintenanceMonitorTarget{
		{Catalog: "archive", Namespace: "anything", Table: "orders"},
		{Catalog: "asmat", Namespace: "analytics", Table: "daily_summary"},
		{Catalog: "asmat", Namespace: "reporting", Table: "temporary_2026"},
	} {
		if !catalogMonitoringTargetExcluded(monitoring, target) {
			t.Fatalf("target %#v was not excluded", target)
		}
	}
	for _, target := range []maintenanceMonitorTarget{
		{Catalog: "ds", Namespace: "analytics", Table: "daily_summary"},
		{Catalog: "asmat", Namespace: "reporting", Table: "daily_summary"},
	} {
		if catalogMonitoringTargetExcluded(monitoring, target) {
			t.Fatalf("target %#v was unexpectedly excluded", target)
		}
	}
	if !catalogMonitoringCatalogExcluded(monitoring, "archive") || catalogMonitoringCatalogExcluded(monitoring, "asmat") {
		t.Fatal("whole-catalog exclusion did not distinguish archive from scoped asmat exclusions")
	}
}

func TestPrepareMaintenanceMonitorConfigValidatesCatalogMonitoringExclusions(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	delete(maintenance, "tables")
	maintenance["catalog_monitoring"] = map[string]any{
		"enabled": true, "api_uri": "http://gravitino:8090", "metalake": "lakehouse",
		"exclude": []any{map[string]any{"catalog": "asmat", "table": "temporary_*"}},
	}
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "namespace is required") {
		t.Fatalf("exclusion validation error = %v", err)
	}
}

func TestMaintenanceTargetMatchesGlobSelectors(t *testing.T) {
	selectors := []config.IcebergTarget{{Namespace: "ga4_*", Table: "daily_*"}}
	if !maintenanceTargetMatches(selectors, "ga4_bronze", "daily_overview") {
		t.Fatal("expected namespace and table glob to match")
	}
	if maintenanceTargetMatches(selectors, "analytics", "daily_overview") {
		t.Fatal("unexpected namespace match")
	}
}

func TestExpandMaintenanceMonitorTargetsDiscoversNewNamespacesAndTables(t *testing.T) {
	cat := maintenanceDiscoveryCatalogStub{
		namespaces: map[string][]icetable.Identifier{
			"":          {{"analytics"}, {"ga4_bronze"}},
			"analytics": {{"analytics", "daily"}},
		},
		tables: map[string][]icetable.Identifier{
			"analytics":       {{"analytics", "summary"}},
			"analytics.daily": {{"analytics", "daily", "overview"}},
			"ga4_bronze":      {{"ga4_bronze", "daily_overview"}, {"ga4_bronze", "users"}},
		},
	}
	targets, err := expandMaintenanceMonitorTargets(context.Background(), cat, []config.IcebergTarget{
		{Namespace: "ga4_*", Table: "daily_*"},
		{Namespace: "analytics*", Table: "*"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []config.IcebergTarget{
		{Namespace: "analytics", Table: "summary"},
		{Namespace: "analytics.daily", Table: "overview"},
		{Namespace: "ga4_bronze", Table: "daily_overview"},
	}
	if len(targets) != len(want) {
		t.Fatalf("targets = %#v, want %#v", targets, want)
	}
	for i := range want {
		if targets[i] != want[i] {
			t.Fatalf("targets[%d] = %#v, want %#v", i, targets[i], want[i])
		}
	}
}

func TestExpandMaintenanceMonitorTargetsSkipsExcludedNamespaceBeforeListingTables(t *testing.T) {
	cat := maintenanceDiscoveryCatalogStub{
		namespaces: map[string][]icetable.Identifier{"": {{"analytics"}, {"streaming"}}},
		tables: map[string][]icetable.Identifier{
			"analytics": {{"analytics", "historical"}},
			"streaming": {{"streaming", "events"}},
		},
		tableCalls: make(map[string]int),
	}
	targets, err := expandMaintenanceMonitorTargets(context.Background(), cat, []config.IcebergTarget{{Namespace: "*", Table: "*"}}, func(namespace string) bool {
		return namespace == "analytics"
	})
	if err != nil {
		t.Fatal(err)
	}
	if cat.tableCalls["analytics"] != 0 {
		t.Fatalf("excluded namespace was listed %d time(s)", cat.tableCalls["analytics"])
	}
	if got, want := targets, []config.IcebergTarget{{Namespace: "streaming", Table: "events"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("targets = %#v, want %#v", got, want)
	}
}

func TestExpandMaintenanceMonitorTargetsHonorsDiscoveryTableLimit(t *testing.T) {
	const tableCount = 10638
	tables := make([]icetable.Identifier, tableCount)
	for i := range tables {
		tables[i] = icetable.Identifier{"analytics", fmt.Sprintf("table_%05d", i)}
	}
	cat := maintenanceDiscoveryCatalogStub{
		tables: map[string][]icetable.Identifier{"analytics": tables},
	}
	for _, tc := range []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "default", wantErr: true},
		{name: "larger_override", value: "50000"},
		{name: "exact_boundary", value: "10638"},
		{name: "over_boundary", value: "10637", wantErr: true},
		{name: "invalid", value: "invalid", wantErr: true},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "whitespace", value: " 50000 "},
		{name: "overflow", value: "999999999999999999999999999999", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES", tc.value)
			targets, err := expandMaintenanceMonitorTargets(context.Background(), cat, []config.IcebergTarget{{Namespace: "analytics", Table: "*"}})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES") {
					t.Fatalf("expected actionable discovery limit error, got %v", err)
				}
				if targets != nil {
					t.Fatal("failed discovery must not return partial membership")
				}
				return
			}
			if err != nil || len(targets) != tableCount {
				t.Fatalf("discovered %d tables, error=%v; want %d tables", len(targets), err, tableCount)
			}
		})
	}
}

func TestCatalogMonitorDiscoveryTableLimitSpansCatalogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/metalakes/lakehouse/catalogs":
			_, _ = w.Write([]byte(`{"code":0,"identifiers":[{"name":"first"},{"name":"second"}]}`))
		case "/api/metalakes/lakehouse/catalogs/first", "/api/metalakes/lakehouse/catalogs/second":
			_, _ = w.Write([]byte(`{"code":0,"catalog":{"type":"relational","provider":"lakehouse-iceberg"}}`))
		case "/v1/config":
			_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
		case "/v1/namespaces":
			if r.URL.Query().Get("parent") == "" {
				_, _ = w.Write([]byte(`{"namespaces":[["analytics"]]}`))
			} else {
				_, _ = w.Write([]byte(`{"namespaces":[]}`))
			}
		case "/v1/namespaces/analytics/tables":
			_, _ = w.Write([]byte(`{"identifiers":[{"namespace":["analytics"],"name":"orders"},{"namespace":["analytics"],"name":"users"}]}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := maintenanceMonitorConfigForTest()
	cfg.Sink.Config["rest_uri"] = server.URL
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	delete(maintenance, "tables")
	maintenance["catalog_monitoring"] = map[string]any{
		"enabled": true, "api_uri": server.URL, "metalake": "lakehouse",
	}
	normalized, _, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	iceCfg, err := decodeIcebergConfig(normalized.Sink.Config)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []string{"3", "4"} {
		t.Run(limit, func(t *testing.T) {
			t.Setenv("RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES", limit)
			targets, err := discoverCatalogMonitorTargets(context.Background(), normalized, iceCfg)
			if limit == "3" {
				if err == nil || !strings.Contains(err.Error(), "catalog monitoring selected more than 3 tables") || !strings.Contains(err.Error(), "RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES") {
					t.Fatalf("expected aggregate discovery limit error, got %v", err)
				}
				if targets != nil {
					t.Fatal("failed discovery must not return partial membership")
				}
				return
			}
			if err != nil || len(targets) != 4 {
				t.Fatalf("discovered %d tables, error=%v; want 4 across two catalogs", len(targets), err)
			}
		})
	}
}

func TestListGravitinoCatalogsFiltersPatternsAndProviders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/metalakes/lakehouse/catalogs":
			_, _ = w.Write([]byte(`{"code":0,"identifiers":[{"name":"asmat"},{"name":"mysql"},{"name":"other"}]}`))
		case "/api/metalakes/lakehouse/catalogs/asmat":
			_, _ = w.Write([]byte(`{"code":0,"catalog":{"type":"relational","provider":"lakehouse-iceberg"}}`))
		case "/api/metalakes/lakehouse/catalogs/mysql":
			_, _ = w.Write([]byte(`{"code":0,"catalog":{"type":"relational","provider":"jdbc-mysql"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	catalogs, err := listGravitinoCatalogs(context.Background(), config.IcebergCatalogMonitoringConfig{
		APIURI: server.URL, Metalake: "lakehouse", CatalogPatterns: []string{"as*", "mysql"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(catalogs) != 1 || catalogs[0] != "asmat" {
		t.Fatalf("catalogs = %#v, want [asmat]", catalogs)
	}
}

func TestMaintenanceWorkerConfigForCatalogUsesStateCatalog(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	delete(maintenance, "tables")
	maintenance["catalog_monitoring"] = map[string]any{
		"enabled": true, "api_uri": "http://gravitino:8090", "metalake": "lakehouse",
	}
	catalogCfg, err := maintenanceMonitorConfigForCatalog(cfg, "new_catalog")
	if err != nil {
		t.Fatal(err)
	}
	_, sinkCfg := jobSinkSpec(catalogCfg)
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		t.Fatal(err)
	}
	if iceCfg.Warehouse != "new_catalog" || iceCfg.CatalogName != "new_catalog" || iceCfg.TableMaintenance.CatalogName != "new_catalog" {
		t.Fatalf("catalog config = warehouse:%q catalog:%q maintenance:%q", iceCfg.Warehouse, iceCfg.CatalogName, iceCfg.TableMaintenance.CatalogName)
	}
}

func TestPrepareMaintenanceMonitorConfigRequiresRunnerForHybrid(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["executor"] = "hybrid"
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "runner_uri") {
		t.Fatalf("hybrid backend validation error = %v", err)
	}
}

func TestPrepareMaintenanceMonitorConfigEnforcesOwnerIDLimit(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	cfg.ID = strings.Repeat("a", maxMaintenanceMonitorIDLength+1)
	if _, _, err := PrepareMaintenanceMonitorConfig(cfg); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("monitor id length validation error = %v", err)
	}
}

func TestPrepareMaintenanceMonitorConfigAcceptsCompactTableList(t *testing.T) {
	cfg := maintenanceMonitorConfigForTest()
	maintenance := cfg.Sink.Config["table_maintenance"].(map[string]any)
	maintenance["namespace"] = []any{"barayax_bronze"}
	maintenance["tables"] = []any{"tbl_absen", "tbl_employee", "attendance_daily"}

	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		t.Fatalf("PrepareMaintenanceMonitorConfig returned error: %v", err)
	}
	if len(targets) != 3 {
		t.Fatalf("normalized targets = %#v", targets)
	}
	for _, target := range targets {
		if target.Namespace != "barayax_bronze" {
			t.Fatalf("target namespace = %q", target.Namespace)
		}
	}
	maintenance = rawMaintenanceMap(normalized.Sink.Config)
	if _, exists := maintenance["namespace"]; exists {
		t.Fatalf("compact namespace was not removed from normalized config: %#v", maintenance)
	}
}
