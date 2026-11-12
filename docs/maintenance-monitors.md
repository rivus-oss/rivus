# Iceberg maintenance monitors

A maintenance monitor is a durable, long-running registration for existing
Iceberg tables. It does not create a source connector, run an initial snapshot,
or attach to a CDC stream. The shared `maintenance-worker --queue` process
inventories registered tables and schedules eligible compaction, snapshot
expiration, and orphan cleanup.

## Lifecycle

- `ACTIVE` monitors are inventoried and scheduled continuously.
- `PAUSED` monitors do not create or claim new work. An operation that already
  owns a lease is allowed to finish.
- Resuming a monitor makes it eligible again. Background inventory remains
  paced, so thousands of tables do not start scanning at once.
- Deleting a monitor disables future schedules and cancels queued/retry work.
  Iceberg data and historical maintenance runs are preserved.

The maintenance worker discovers monitor changes on its normal poll interval
(30 seconds by default). A monitor is a logical long-running owner; Rivus does
not create one goroutine or Spark application per monitor.

## Configuration

Submit the YAML through **Maintenance Monitors → New monitor** or
`POST /api/iceberg/maintenance/monitors` with content type
`application/x-yaml`.

```yaml
id: barayax-maintenance
name: Barayax Iceberg Maintenance
mode: maintenance-only
sink:
  type: iceberg_native
  config:
    rest_uri: "${ICEBERG_REST_URI}"
    warehouse: "${ICEBERG_WAREHOUSE}"
    table_maintenance:
      enabled: true
      executor: hybrid
      catalog_name: asmat
      runner_uri: "${RIVUS_RUNNER_URI}"
      runner_api_token: "${RUNNER_API_TOKEN}"
      runner_resource_profile: small
      namespace:
        - barayax_bronze
      tables:
        - tbl_absen
        - tbl_employee
        - attendance_daily
```

The compact form accepts exactly one `namespace` (either a string or a
one-item list) and several table names. The existing explicit form remains
available when a monitor spans namespaces:

```yaml
tables:
  - namespace: barayax_bronze
    table: tbl_absen
  - namespace: barayax_silver
    table: attendance_daily
```

For automatic catalog discovery, use the dedicated `catalog_monitoring` block.
The worker asks the Gravitino management API for matching catalogs, then asks
the Iceberg REST API for matching namespaces and tables. It refreshes every
five minutes by default and enrolls new matches without another submission:

```yaml
table_maintenance:
  enabled: true
  catalog_monitoring:
    enabled: true
    api_uri: ${GRAVITINO_URI}
    metalake: lakehouse
    discovery_interval_seconds: 300
    catalog_patterns:
      - "*"
    namespace_patterns:
      - "*"
    table_patterns:
      - "*"

    # Optional monitor-local exclusions. Missing namespace means the entire
    # catalog; missing table means the entire namespace.
    exclude:
      - catalog: archive
        reason: immutable catalog
      - catalog: asmat
        namespace: analytics
        reason: immutable schema; no new writes
      - catalog: asmat
        namespace: reporting
        table: "temporary_*"
```

Patterns use shell-style `*`, `?`, and character classes. Dynamic selection
uses the clearly separate `catalog_monitoring` block; it does not use a
confusing `tables: [{table: "*"}]` shape. The legacy `tables` field is only for
an explicit list of exact tables. One monitor cannot use both forms.

Exclusions are local to this monitor and are applied before discovered tables
are registered. This lets a general metalake monitor omit an immutable catalog,
one schema within a catalog, or selected table patterns. `catalog` is required;
`namespace` and `table` narrow the exclusion. A table exclusion must include
its namespace so the same table name in another schema remains eligible.

Catalog monitoring performs maintenance for **non-streaming tables only**.
Rivus still records a matching streaming or incomplete snapshot table in the
monitor's discovery results, but marks it “Used by streaming/snapshot” and does
not schedule monitor maintenance for it. This makes the protection visible and
lets the monitor claim the table automatically when that reservation is safely
released. A paused streaming job stays protected. A stopped streaming job
releases its protection. A snapshot-only job remains protected until its
snapshot succeeds durably (or the job is deleted).

## Layered monitors and precedence

You may register a whole-Metalake monitor, a catalog monitor, a schema monitor,
and exact-table monitors at the same time. Rivus resolves each physical table
with this precedence:

1. streaming job
2. incomplete snapshot-only job
3. exact table monitor
4. schema monitor
5. catalog monitor
6. whole-Metalake monitor

For example, a general Metalake monitor can cover newly created tables while a
more specific monitor gives `asmat.analytics.orders` a different maintenance
profile. Equal-specificity overlaps remain stable: the current owner keeps the
table and the other monitor reports “Owned by another monitor” instead of
ownership oscillating.

`warehouse` is the physical Gravitino/Iceberg catalog used for ownership.
`catalog_name` is only the Spark catalog alias used while executing maintenance;
it must not be used to decide whether two jobs refer to the same physical table.

The monitor API reports:

- `discovered_count`: current matching tables.
- `owned_count`: tables maintained by this monitor.
- `reserved_count`: tables protected by streaming or snapshot ingestion.
- `excluded_scope_count`: configured catalog/schema/table exclusion rules.
- `conflict_count`: tables owned by a more specific or already-established monitor.
- `retired_count`: tables no longer returned by a successful discovery.

If discovery fails, Rivus keeps the last successful membership and reports the
error. A successful empty discovery is different: it retires the old matches.

### Discovery table limit

`RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES` sets the maximum number of matching
tables discovered per monitor across its catalogs. It defaults to `10000`;
empty, invalid, zero, and negative values fall back to that default. Matching
streaming/snapshot tables count toward the limit even though their maintenance
is not owned by the monitor. The separate namespace discovery guard remains
10,000 namespaces per catalog.

For installations with more than 10,000 matching tables, set this in `.env`:

```dotenv
RIVUS_MAINTENANCE_MAX_DISCOVERED_TABLES=50000
```

Docker Compose forwards this setting to `rivus-maintenance`. Use an image built
with support for this setting, then recreate the maintenance container:

```sh
docker compose up -d --force-recreate rivus-maintenance
```

A container restart alone does not reload `.env`. Discovery retries on its
normal interval; a successful discovery updates membership and clears the
discovery error. Raising this limit does not increase maintenance concurrency.

## API

```text
GET    /api/iceberg/maintenance/monitors
POST   /api/iceberg/maintenance/monitors
GET    /api/iceberg/maintenance/monitors/{id}
POST   /api/iceberg/maintenance/monitors/{id}/pause
POST   /api/iceberg/maintenance/monitors/{id}/resume
POST   /api/iceberg/maintenance/monitors/{id}/run
DELETE /api/iceberg/maintenance/monitors/{id}
```

API responses expose table names and safe backend labels but never return the
stored sink configuration or credentials.
