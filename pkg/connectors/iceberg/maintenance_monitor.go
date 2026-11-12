package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/gerinsp/rivus/pkg/config"
	"gopkg.in/yaml.v3"
)

var maintenanceMonitorIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

const maxMaintenanceMonitorIDLength = 247 // "monitor:" + ID must fit owner_job_id VARCHAR(255).

const (
	defaultMaintenanceDiscoveryInterval   = 5 * time.Minute
	defaultMaxDiscoveredMaintenanceTables = 10000
	maxDiscoveredMaintenanceNamespaces    = 10000
	maintenanceDiscoveryTableLimitEnv     = "RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES"
)

func maintenanceDiscoveryTableLimit() int {
	return intEnv(maintenanceDiscoveryTableLimitEnv, defaultMaxDiscoveredMaintenanceTables)
}

// PrepareMaintenanceMonitorConfig validates and normalizes a standalone,
// long-running Iceberg maintenance registration. It deliberately rejects a
// source connector so submitting a monitor can never start ingestion.
func PrepareMaintenanceMonitorConfig(cfg *config.JobConfig) (*config.JobConfig, []config.IcebergTarget, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("maintenance monitor config is nil")
	}
	cloned := *cfg
	config.ApplyDefaults(&cloned)
	cloned.ID = strings.TrimSpace(cloned.ID)
	cloned.Name = strings.TrimSpace(cloned.Name)
	if cloned.ID == "" {
		return nil, nil, fmt.Errorf("maintenance monitor id is required")
	}
	if len(cloned.ID) > maxMaintenanceMonitorIDLength {
		return nil, nil, fmt.Errorf("maintenance monitor id exceeds %d characters", maxMaintenanceMonitorIDLength)
	}
	if !maintenanceMonitorIDPattern.MatchString(cloned.ID) {
		return nil, nil, fmt.Errorf("maintenance monitor id must contain only letters, numbers, dots, underscores, or hyphens")
	}
	if cloned.Name == "" {
		cloned.Name = cloned.ID
	}
	if len(cloned.Name) > 255 {
		return nil, nil, fmt.Errorf("maintenance monitor name exceeds 255 characters")
	}
	if cloned.Mode == "" {
		cloned.Mode = config.JobModeMaintenanceOnly
	}
	if cloned.Mode != config.JobModeMaintenanceOnly {
		return nil, nil, fmt.Errorf("maintenance monitor mode must be %q", config.JobModeMaintenanceOnly)
	}
	if cloned.Source != nil {
		return nil, nil, fmt.Errorf("maintenance-only monitors must not define a source connector")
	}

	sinkType, sinkCfg := jobSinkSpec(&cloned)
	if !strings.EqualFold(sinkType, "iceberg_native") {
		return nil, nil, fmt.Errorf("maintenance monitor sink is %q, not iceberg_native", sinkType)
	}
	normalizedSinkCfg, err := configMap(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	if err := config.NormalizeIcebergMaintenanceTargets(normalizedSinkCfg); err != nil {
		return nil, nil, err
	}
	sinkCfg = normalizedSinkCfg
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	settings, err := nativeMaintenanceSettingsFromRaw(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	if settings.Executor != maintenanceExecutorNative {
		if err := validateMaintenanceBackend(iceCfg.TableMaintenance); err != nil {
			return nil, nil, err
		}
	}
	if !iceCfg.TableMaintenance.Enabled {
		return nil, nil, fmt.Errorf("iceberg table_maintenance.enabled must be true")
	}
	monitoring := iceCfg.TableMaintenance.CatalogMonitoring
	rawTargets := iceCfg.TableMaintenance.Tables
	if monitoring.Enabled && len(rawTargets) > 0 {
		return nil, nil, fmt.Errorf("table_maintenance.catalog_monitoring and table_maintenance.tables are mutually exclusive")
	}
	if monitoring.Enabled {
		if err := validateCatalogMonitoringConfig(monitoring); err != nil {
			return nil, nil, err
		}
	} else if len(rawTargets) == 0 {
		return nil, nil, fmt.Errorf("iceberg table_maintenance.tables must contain at least one namespace/table target")
	}
	for _, target := range rawTargets {
		if strings.TrimSpace(target.Namespace) == "" || strings.TrimSpace(target.Table) == "" {
			return nil, nil, fmt.Errorf("maintenance table namespace and table are required")
		}
		if hasGlobMeta(target.Namespace) || hasGlobMeta(target.Table) {
			return nil, nil, fmt.Errorf("table_maintenance.tables requires exact namespace/table names; use catalog_monitoring patterns for automatic discovery")
		}
	}
	if !monitoring.Enabled {
		if catalogName := maintenanceCatalogName(iceCfg); !sparkCatalogNamePattern.MatchString(catalogName) {
			return nil, nil, fmt.Errorf("invalid Iceberg catalog name %q", catalogName)
		}
		if _, err := maintenanceIdentityCatalogName(iceCfg); err != nil {
			return nil, nil, err
		}
	}
	targets := dedupeTargets(rawTargets)
	if len(targets) > maxMaintenanceTables {
		return nil, nil, fmt.Errorf("maintenance monitor selects %d tables; maximum is %d", len(targets), maxMaintenanceTables)
	}

	// Preserve the complete generic sink configuration persisted by the API.
	// Some maintenance settings are intentionally consumed from the raw map for
	// backward compatibility, so re-encoding only the typed IcebergConfig would
	// silently discard them. Only replace the validated table list here.
	encoded, err := configMap(sinkCfg)
	if err != nil {
		return nil, nil, err
	}
	maintenance := rawMaintenanceMap(encoded)
	if maintenance == nil {
		return nil, nil, fmt.Errorf("iceberg table_maintenance config is required")
	}
	if !monitoring.Enabled {
		maintenance["tables"] = targets
	}
	cloned.Sink = &config.ConnectorSpec{Type: "iceberg_native", Config: encoded}
	return &cloned, targets, nil
}

func hasGlobMeta(value string) bool {
	return strings.ContainsAny(value, "*?[")
}

func catalogMonitoringEnabled(cfg config.IcebergConfig) bool {
	return cfg.TableMaintenance.CatalogMonitoring.Enabled
}

func maintenanceDiscoveryInterval(cfg config.IcebergConfig) time.Duration {
	seconds := cfg.TableMaintenance.CatalogMonitoring.DiscoveryIntervalSeconds
	if seconds == 0 {
		seconds = int(defaultMaintenanceDiscoveryInterval / time.Second)
	}
	if seconds < 30 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

func discoverMaintenanceMonitorTargets(ctx context.Context, iceCfg config.IcebergConfig, selectors []config.IcebergTarget) ([]config.IcebergTarget, error) {
	return discoverMaintenanceMonitorTargetsFiltered(ctx, iceCfg, selectors, nil)
}

func discoverMaintenanceMonitorTargetsFiltered(ctx context.Context, iceCfg config.IcebergConfig, selectors []config.IcebergTarget, excludedNamespace func(string) bool) ([]config.IcebergTarget, error) {
	cat, err := newCatalogWithRetry(ctx, iceCfg)
	if err != nil {
		return nil, err
	}
	return expandMaintenanceMonitorTargets(ctx, cat, selectors, excludedNamespace)
}

type maintenanceMonitorTarget struct {
	Catalog   string
	Namespace string
	Table     string
}

const (
	specificityMetalake  = 1
	specificityCatalog   = 2
	specificityNamespace = 3
	specificityTable     = 4
)

type maintenanceMonitorDiscoveryDelta struct {
	Added   []maintenanceMonitorTarget
	Removed []maintenanceMonitorTarget
}

type monitorOwnershipCandidate struct {
	OwnerID     string
	Specificity int
}

func maintenanceDiscoveryDelta(previous, discovered []maintenanceMonitorTarget) maintenanceMonitorDiscoveryDelta {
	previousByKey := make(map[string]maintenanceMonitorTarget, len(previous))
	discoveredByKey := make(map[string]maintenanceMonitorTarget, len(discovered))
	for _, target := range previous {
		previousByKey[canonicalMaintenanceTableKey(target.Catalog, target.Namespace, target.Table)] = target
	}
	for _, target := range discovered {
		discoveredByKey[canonicalMaintenanceTableKey(target.Catalog, target.Namespace, target.Table)] = target
	}
	delta := maintenanceMonitorDiscoveryDelta{}
	for key, target := range discoveredByKey {
		if _, exists := previousByKey[key]; !exists {
			delta.Added = append(delta.Added, target)
		}
	}
	for key, target := range previousByKey {
		if _, exists := discoveredByKey[key]; !exists {
			delta.Removed = append(delta.Removed, target)
		}
	}
	sortMaintenanceMonitorTargets(delta.Added)
	sortMaintenanceMonitorTargets(delta.Removed)
	return delta
}

func targetsAfterDiscovery(previous, discovered []maintenanceMonitorTarget, discoveryErr error) []maintenanceMonitorTarget {
	if discoveryErr != nil {
		return previous
	}
	return discovered
}

func preferMonitorOwner(current, candidate monitorOwnershipCandidate) monitorOwnershipCandidate {
	if candidate.Specificity > current.Specificity {
		return candidate
	}
	return current
}

func monitorSpecificity(iceCfg config.IcebergConfig, target maintenanceMonitorTarget) int {
	monitoring := iceCfg.TableMaintenance.CatalogMonitoring
	if !monitoring.Enabled {
		return specificityTable
	}
	best := specificityMetalake
	for _, catalogPattern := range defaultDiscoveryPatterns(monitoring.CatalogPatterns) {
		catalogMatch, err := path.Match(catalogPattern, target.Catalog)
		if err != nil || !catalogMatch || hasGlobMeta(catalogPattern) {
			continue
		}
		candidate := specificityCatalog
		for _, namespacePattern := range defaultDiscoveryPatterns(monitoring.NamespacePatterns) {
			namespaceMatch, namespaceErr := path.Match(namespacePattern, target.Namespace)
			if namespaceErr != nil || !namespaceMatch || hasGlobMeta(namespacePattern) {
				continue
			}
			candidate = specificityNamespace
			for _, tablePattern := range defaultDiscoveryPatterns(monitoring.TablePatterns) {
				tableMatch, tableErr := path.Match(tablePattern, target.Table)
				if tableErr == nil && tableMatch && !hasGlobMeta(tablePattern) {
					candidate = specificityTable
					break
				}
			}
		}
		if candidate > best {
			best = candidate
		}
	}
	return best
}

func sortMaintenanceMonitorTargets(targets []maintenanceMonitorTarget) {
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Catalog != targets[j].Catalog {
			return targets[i].Catalog < targets[j].Catalog
		}
		if targets[i].Namespace != targets[j].Namespace {
			return targets[i].Namespace < targets[j].Namespace
		}
		return targets[i].Table < targets[j].Table
	})
}

func validateCatalogMonitoringConfig(monitoring config.IcebergCatalogMonitoringConfig) error {
	rawURI := firstNonEmpty(strings.TrimSpace(monitoring.APIURI), strings.TrimSpace(os.Getenv("GRAVITINO_URI")))
	parsed, err := url.Parse(rawURI)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("table_maintenance.catalog_monitoring.api_uri must be a valid http(s) Gravitino URI")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("table_maintenance.catalog_monitoring.api_uri must use http or https")
	}
	if strings.TrimSpace(monitoring.Metalake) == "" {
		return fmt.Errorf("table_maintenance.catalog_monitoring.metalake is required")
	}
	for field, patterns := range map[string][]string{
		"catalog_patterns": monitoring.CatalogPatterns, "namespace_patterns": monitoring.NamespacePatterns, "table_patterns": monitoring.TablePatterns,
	} {
		for _, pattern := range defaultDiscoveryPatterns(patterns) {
			if _, err := path.Match(pattern, pattern); err != nil {
				return fmt.Errorf("invalid catalog_monitoring.%s pattern %q: %w", field, pattern, err)
			}
		}
	}
	for index, exclusion := range monitoring.Exclude {
		catalogPattern := strings.TrimSpace(exclusion.Catalog)
		namespacePattern := strings.TrimSpace(exclusion.Namespace)
		tablePattern := strings.TrimSpace(exclusion.Table)
		if catalogPattern == "" {
			return fmt.Errorf("catalog_monitoring.exclude[%d].catalog is required", index)
		}
		if tablePattern != "" && namespacePattern == "" {
			return fmt.Errorf("catalog_monitoring.exclude[%d].namespace is required when table is set", index)
		}
		for field, pattern := range map[string]string{
			"catalog": catalogPattern, "namespace": namespacePattern, "table": tablePattern,
		} {
			if pattern == "" {
				continue
			}
			if _, err := path.Match(pattern, pattern); err != nil {
				return fmt.Errorf("invalid catalog_monitoring.exclude[%d].%s pattern %q: %w", index, field, pattern, err)
			}
		}
	}
	return nil
}

func defaultDiscoveryPatterns(patterns []string) []string {
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if pattern = strings.TrimSpace(pattern); pattern != "" {
			out = append(out, pattern)
		}
	}
	if len(out) == 0 {
		return []string{"*"}
	}
	return out
}

func discoverCatalogMonitorTargets(ctx context.Context, cfg *config.JobConfig, iceCfg config.IcebergConfig) ([]maintenanceMonitorTarget, error) {
	maxTables := maintenanceDiscoveryTableLimit()
	monitoring := iceCfg.TableMaintenance.CatalogMonitoring
	catalogs, err := listGravitinoCatalogs(ctx, monitoring)
	if err != nil {
		return nil, err
	}
	namespacePatterns := defaultDiscoveryPatterns(monitoring.NamespacePatterns)
	tablePatterns := defaultDiscoveryPatterns(monitoring.TablePatterns)
	selectors := make([]config.IcebergTarget, 0, len(namespacePatterns)*len(tablePatterns))
	for _, namespacePattern := range namespacePatterns {
		for _, tablePattern := range tablePatterns {
			selectors = append(selectors, config.IcebergTarget{Namespace: namespacePattern, Table: tablePattern})
		}
	}

	var out []maintenanceMonitorTarget
	for _, catalogName := range catalogs {
		if catalogMonitoringCatalogExcluded(monitoring, catalogName) {
			continue
		}
		catalogCfg, err := maintenanceMonitorConfigForCatalog(cfg, catalogName)
		if err != nil {
			return nil, err
		}
		_, sinkCfg := jobSinkSpec(catalogCfg)
		catalogIceCfg, err := decodeIcebergConfig(sinkCfg)
		if err != nil {
			return nil, err
		}
		targets, err := discoverMaintenanceMonitorTargetsFiltered(ctx, catalogIceCfg, selectors, func(namespace string) bool {
			return catalogMonitoringNamespaceExcluded(monitoring, catalogName, namespace)
		})
		if err != nil {
			return nil, fmt.Errorf("discover catalog %s: %w", catalogName, err)
		}
		for _, target := range targets {
			discovered := maintenanceMonitorTarget{Catalog: catalogName, Namespace: target.Namespace, Table: target.Table}
			if catalogMonitoringTargetExcluded(monitoring, discovered) {
				continue
			}
			out = append(out, discovered)
			if len(out) > maxTables {
				return nil, fmt.Errorf("catalog monitoring selected more than %d tables; increase %s or narrow the discovery patterns", maxTables, maintenanceDiscoveryTableLimitEnv)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Catalog != out[j].Catalog {
			return out[i].Catalog < out[j].Catalog
		}
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Table < out[j].Table
	})
	return out, nil
}

func catalogMonitoringCatalogExcluded(monitoring config.IcebergCatalogMonitoringConfig, catalog string) bool {
	for _, exclusion := range monitoring.Exclude {
		if strings.TrimSpace(exclusion.Namespace) != "" || strings.TrimSpace(exclusion.Table) != "" {
			continue
		}
		matched, err := path.Match(strings.TrimSpace(exclusion.Catalog), catalog)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func catalogMonitoringTargetExcluded(monitoring config.IcebergCatalogMonitoringConfig, target maintenanceMonitorTarget) bool {
	for _, exclusion := range monitoring.Exclude {
		catalogPattern := strings.TrimSpace(exclusion.Catalog)
		namespacePattern := firstNonEmpty(strings.TrimSpace(exclusion.Namespace), "*")
		tablePattern := firstNonEmpty(strings.TrimSpace(exclusion.Table), "*")
		catalogMatch, catalogErr := path.Match(catalogPattern, target.Catalog)
		namespaceMatch, namespaceErr := path.Match(namespacePattern, target.Namespace)
		tableMatch, tableErr := path.Match(tablePattern, target.Table)
		if catalogErr == nil && namespaceErr == nil && tableErr == nil && catalogMatch && namespaceMatch && tableMatch {
			return true
		}
	}
	return false
}

func catalogMonitoringNamespaceExcluded(monitoring config.IcebergCatalogMonitoringConfig, catalog, namespace string) bool {
	for _, exclusion := range monitoring.Exclude {
		if strings.TrimSpace(exclusion.Namespace) == "" || strings.TrimSpace(exclusion.Table) != "" {
			continue
		}
		catalogMatch, catalogErr := path.Match(strings.TrimSpace(exclusion.Catalog), catalog)
		namespaceMatch, namespaceErr := path.Match(strings.TrimSpace(exclusion.Namespace), namespace)
		if catalogErr == nil && namespaceErr == nil && catalogMatch && namespaceMatch {
			return true
		}
	}
	return false
}

func listGravitinoCatalogs(ctx context.Context, monitoring config.IcebergCatalogMonitoringConfig) ([]string, error) {
	baseURI := firstNonEmpty(strings.TrimSpace(monitoring.APIURI), strings.TrimSpace(os.Getenv("GRAVITINO_URI")))
	parsed, err := url.Parse(baseURI)
	if err != nil {
		return nil, err
	}
	parsed = parsed.JoinPath("api", "metalakes", strings.TrimSpace(monitoring.Metalake), "catalogs")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list Gravitino catalogs: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list Gravitino catalogs returned %s", resp.Status)
	}
	var payload struct {
		Code        int `json:"code"`
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode Gravitino catalog list: %w", err)
	}
	if payload.Code != 0 {
		return nil, fmt.Errorf("Gravitino catalog list returned code %d", payload.Code)
	}
	patterns := defaultDiscoveryPatterns(monitoring.CatalogPatterns)
	var catalogs []string
	for _, identifier := range payload.Identifiers {
		name := strings.TrimSpace(identifier.Name)
		if name == "" || !matchesAnyPattern(patterns, name) {
			continue
		}
		icebergCatalog, err := gravitinoCatalogIsIceberg(ctx, baseURI, monitoring.Metalake, name)
		if err != nil {
			return nil, err
		}
		if icebergCatalog {
			catalogs = append(catalogs, name)
		}
	}
	sort.Strings(catalogs)
	return catalogs, nil
}

func gravitinoCatalogIsIceberg(ctx context.Context, baseURI, metalake, catalogName string) (bool, error) {
	parsed, err := url.Parse(baseURI)
	if err != nil {
		return false, err
	}
	parsed = parsed.JoinPath("api", "metalakes", strings.TrimSpace(metalake), "catalogs", strings.TrimSpace(catalogName))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("inspect Gravitino catalog %s: %w", catalogName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Errorf("inspect Gravitino catalog %s returned %s", catalogName, resp.Status)
	}
	var payload struct {
		Code    int `json:"code"`
		Catalog struct {
			Type     string `json:"type"`
			Provider string `json:"provider"`
		} `json:"catalog"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return false, fmt.Errorf("decode Gravitino catalog %s: %w", catalogName, err)
	}
	if payload.Code != 0 {
		return false, fmt.Errorf("inspect Gravitino catalog %s returned code %d", catalogName, payload.Code)
	}
	return strings.EqualFold(payload.Catalog.Type, "relational") && strings.EqualFold(payload.Catalog.Provider, "lakehouse-iceberg"), nil
}

func matchesAnyPattern(patterns []string, value string) bool {
	for _, pattern := range patterns {
		if matched, err := path.Match(pattern, value); err == nil && matched {
			return true
		}
	}
	return false
}

func maintenanceMonitorConfigForCatalog(cfg *config.JobConfig, catalogName string) (*config.JobConfig, error) {
	if cfg == nil {
		return nil, fmt.Errorf("maintenance monitor config is nil")
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var cloned config.JobConfig
	if err := json.Unmarshal(payload, &cloned); err != nil {
		return nil, err
	}
	_, rawSink := jobSinkSpec(&cloned)
	sinkCfg, ok := rawSink.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("maintenance monitor sink config is invalid")
	}
	catalogName = strings.TrimSpace(catalogName)
	sinkCfg["warehouse"] = catalogName
	sinkCfg["catalog_name"] = catalogName
	maintenance := rawMaintenanceMap(sinkCfg)
	if maintenance == nil {
		return nil, fmt.Errorf("iceberg table_maintenance config is required")
	}
	maintenance["catalog_name"] = catalogName
	return &cloned, nil
}

type maintenanceDiscoveryCatalog interface {
	ListTables(context.Context, icetable.Identifier) iter.Seq2[icetable.Identifier, error]
	ListNamespaces(context.Context, icetable.Identifier) ([]icetable.Identifier, error)
}

func expandMaintenanceMonitorTargets(ctx context.Context, cat maintenanceDiscoveryCatalog, selectors []config.IcebergTarget, excludedNamespaces ...func(string) bool) ([]config.IcebergTarget, error) {
	maxTables := maintenanceDiscoveryTableLimit()
	var excludedNamespace func(string) bool
	if len(excludedNamespaces) > 0 {
		excludedNamespace = excludedNamespaces[0]
	}
	targets := make(map[string]config.IcebergTarget)
	namespaces := make(map[string]struct{})
	needsNamespaceListing := false
	for _, selector := range selectors {
		if hasGlobMeta(selector.Namespace) {
			needsNamespaceListing = true
		} else {
			namespaces[strings.TrimSpace(selector.Namespace)] = struct{}{}
		}
	}

	if needsNamespaceListing {
		discovered, err := listMaintenanceNamespaces(ctx, cat, excludedNamespace)
		if err != nil {
			return nil, fmt.Errorf("list Iceberg namespaces for maintenance discovery: %w", err)
		}
		for _, namespace := range discovered {
			namespaces[namespace] = struct{}{}
		}
	}

	for namespace := range namespaces {
		if excludedNamespace != nil && excludedNamespace(namespace) {
			continue
		}
		needsTableListing := false
		for _, selector := range selectors {
			matched, err := path.Match(selector.Namespace, namespace)
			if err != nil {
				return nil, err
			}
			if matched {
				needsTableListing = true
				break
			}
		}
		if !needsTableListing {
			continue
		}
		for ident, listErr := range cat.ListTables(ctx, namespaceOnlyIdentifier(namespace)) {
			if listErr != nil {
				return nil, fmt.Errorf("list Iceberg tables in namespace %s: %w", namespace, listErr)
			}
			if len(ident) < 2 {
				continue
			}
			discoveredNamespace := strings.Join(ident[:len(ident)-1], ".")
			tableName := ident[len(ident)-1]
			if !maintenanceTargetMatches(selectors, discoveredNamespace, tableName) {
				continue
			}
			key := discoveredNamespace + "\x00" + tableName
			targets[key] = config.IcebergTarget{Namespace: discoveredNamespace, Table: tableName}
			if len(targets) > maxTables {
				return nil, fmt.Errorf("maintenance discovery selected more than %d tables; increase %s or narrow the discovery patterns", maxTables, maintenanceDiscoveryTableLimitEnv)
			}
		}
	}

	out := make([]config.IcebergTarget, 0, len(targets))
	for _, target := range targets {
		out = append(out, target)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace == out[j].Namespace {
			return out[i].Table < out[j].Table
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out, nil
}

func listMaintenanceNamespaces(ctx context.Context, cat maintenanceDiscoveryCatalog, excludedNamespaces ...func(string) bool) ([]string, error) {
	var excludedNamespace func(string) bool
	if len(excludedNamespaces) > 0 {
		excludedNamespace = excludedNamespaces[0]
	}
	queue, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(queue))
	var out []string
	for len(queue) > 0 {
		namespace := queue[0]
		queue = queue[1:]
		name := strings.Join(namespace, ".")
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		if excludedNamespace != nil && excludedNamespace(name) {
			seen[name] = struct{}{}
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
		children, childErr := cat.ListNamespaces(ctx, namespace)
		if childErr != nil {
			return nil, childErr
		}
		queue = append(queue, children...)
		if len(seen)+len(queue) > maxDiscoveredMaintenanceNamespaces {
			return nil, fmt.Errorf("maintenance discovery found more than %d namespaces", maxDiscoveredMaintenanceNamespaces)
		}
	}
	sort.Strings(out)
	return out, nil
}

func maintenanceTargetMatches(selectors []config.IcebergTarget, namespace, table string) bool {
	for _, selector := range selectors {
		namespaceMatch, namespaceErr := path.Match(selector.Namespace, namespace)
		tableMatch, tableErr := path.Match(selector.Table, table)
		if namespaceErr == nil && tableErr == nil && namespaceMatch && tableMatch {
			return true
		}
	}
	return false
}

// DescribeMaintenanceMonitorConfig returns the safe fields needed by the
// monitor API and UI without exposing catalog or runner credentials.
func DescribeMaintenanceMonitorConfig(cfg *config.JobConfig) (string, string, string, []config.IcebergTarget, error) {
	normalized, targets, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		return "", "", "", nil, err
	}
	_, sinkCfg := jobSinkSpec(normalized)
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return "", "", "", nil, err
	}
	executor := strings.ToLower(strings.TrimSpace(iceCfg.TableMaintenance.Executor))
	if executor == "" {
		executor = "hybrid"
	}
	profile := strings.ToLower(strings.TrimSpace(iceCfg.TableMaintenance.RunnerResourceProfile))
	if profile == "" {
		profile = "small"
	}
	catalogLabel := maintenanceCatalogName(iceCfg)
	if catalogMonitoringEnabled(iceCfg) {
		catalogLabel = "auto:" + strings.Join(defaultDiscoveryPatterns(iceCfg.TableMaintenance.CatalogMonitoring.CatalogPatterns), ",")
	}
	return catalogLabel, executor, profile, targets, nil
}

// MaintenanceMonitorExcludedScopeCount reports configured exclusions without
// enumerating the tables intentionally hidden beneath a catalog or namespace.
func MaintenanceMonitorExcludedScopeCount(cfg *config.JobConfig) (int, error) {
	normalized, _, err := PrepareMaintenanceMonitorConfig(cfg)
	if err != nil {
		return 0, err
	}
	_, sinkCfg := jobSinkSpec(normalized)
	iceCfg, err := decodeIcebergConfig(sinkCfg)
	if err != nil {
		return 0, err
	}
	return len(iceCfg.TableMaintenance.CatalogMonitoring.Exclude), nil
}

func configMap(value any) (map[string]any, error) {
	payload, err := yaml.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode normalized maintenance config: %w", err)
	}
	var out map[string]any
	if err := yaml.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("decode normalized maintenance config: %w", err)
	}
	return out, nil
}
