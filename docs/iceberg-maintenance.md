# Hybrid Iceberg maintenance

Rivus runs automatic Iceberg maintenance in a separate durable worker. The CDC/API server never owns the maintenance schedule.

## Process model

```text
MySQL CDC -> rivus server -> Iceberg
                 |
                 | lightweight commit signals
                 v
          metadata MySQL
          | state / tasks / runs / results
          ^
          |
       rivus maintenance-worker --queue
          | native: compaction / expiration / orphan cleanup
          ` Spark: compaction through the existing runner/Spark backend
```

Run the server and worker as separate containers/processes using the same Rivus image:

```sh
/app/rivus -addr :8080 -ui-dir ./ui
/app/rivus maintenance-worker --queue
```

Both processes use the same `RIVUS_META_MYSQL_DSN`. The worker also needs the Iceberg REST/object-storage credentials from the persisted jobs. `hybrid` and `spark` executor modes need the existing runner-app or Spark configuration whenever compaction is sent to Spark.

## Configuration semantics

Automatic maintenance has one master switch and one executor selector:

```yaml
table_maintenance:
  enabled: true
  executor: hybrid
```

`enabled` controls whether automatic maintenance exists at all:

- `enabled: false` — no automatic maintenance tasks are scheduled.
- `enabled: true` — the durable maintenance worker owns scheduling and execution.

Existing Iceberg tables that are not owned by a snapshot/CDC pipeline can be
registered as long-running maintenance monitors. Monitors have their own UI and
lifecycle and never start ingestion. See [maintenance-monitors.md](maintenance-monitors.md).

`executor` controls **compaction** execution:

- `hybrid` — default. The worker analyzes the selected rewrite workload and chooses native or Spark.
- `native` — compaction stays in `iceberg-go`; there is no Spark fallback.
- `spark` — every automatic compaction task is submitted to Spark.

Snapshot expiration and orphan cleanup remain native worker operations in all three executor modes. `executor` only selects the compaction engine.

The historical `native_enabled` rollout flag is retired. Rivus removes it while decoding persisted/job configuration and does not use it to select a runtime path.

The CDC-side `tableMaintenanceMonitor` no longer schedules maintenance. Its compatibility layer only projects durable MySQL worker state into the existing job-details status shape so the UI can keep its inventory panel while MySQL remains the source of truth.

The background inventory pass also revisits inventories older than one hour when no explicit refresh is queued. This includes tables updated by external writers, which do not send Rivus CDC commit signals. It uses the existing limit of one background scan per worker poll; manual and commit-triggered requests retain priority. With thousands of tables, an inventory can remain stale while awaiting its turn. Paused monitors, unmanaged/retired tables, active inventory leases and scheduled retry backoff are respected.

Small-file totals span the whole table, while compaction eligibility is calculated within partitions. A date-partitioned table with one file per date can have hundreds of small files and zero legal compaction groups. Ordinary compaction cannot merge those files across dates; changing the partition layout is a separate table-design operation.

The shared job/monitor inventory panel shows the current partition spec under each table name (`Partitioned · date` or `Unpartitioned`) and keeps the existing file counts and thresholds. The reason column reports planner eligibility separately. Partition information is collected during the inventory scan and stored with its counts; older inventory without that metadata shows `Partitioning pending scan` until refreshed. Table type is never inferred from its name, file count, or maintenance status.

Table names include the catalog (`catalog.namespace.table`), including every level of a nested namespace. The API also exposes each row's `catalog` separately, so tables in different catalogs remain distinguishable in catalog-wide monitors and job details.

Recommended configuration:

```yaml
sink:
  type: iceberg_native
  config:
    # normal Iceberg REST/S3 settings...
    table_maintenance:
      enabled: true
      executor: hybrid

      # CDC signals compact work after a short quiet period; inactive tables get
      # only a weekly safety check.
      native_signal_delay_seconds: 300
      native_idle_check_interval_seconds: 604800

      # runner/Spark integration used by hybrid fallback or executor: spark
      runner_uri: http://runner-app:8001
      runner_api_token: ${RUNNER_API_TOKEN}
      runner_resource_profile: small
      catalog_name: rivus

      # hybrid native-compaction boundary. A table becomes eligible after
      # 200 active small files, 50 active equality-delete files, or 10 small
      # files totaling 256 MiB.
      small_file_size_bytes: 67108864
      small_files_min_count: 10
      small_files_min_total_bytes: 268435456
      native_max_selected_input_bytes: 536870912
      native_max_selected_files: 250
      native_max_equality_delete_files: 100
      position_delete_files_threshold: 25 # Compaction after this many active position deletes
      native_target_file_size_bytes: 134217728
      native_scan_concurrency: 1
      native_timeout_seconds: 600

      # native snapshot expiration
      native_expire_interval_seconds: 86400
      native_snapshot_max_age_hours: 168
      native_snapshot_retain_last: 10

      # native orphan cleanup
      native_orphan_interval_seconds: 86400
      native_orphan_inactive_interval_seconds: 604800
      native_orphan_min_age_hours: 168
      native_orphan_dry_run: false
      worker_temp_directory: /tmp/rivus-maintenance

      # retry and Spark status polling
      retry_limit: 5
      retry_base_backoff_seconds: 60
      spark_poll_interval_seconds: 5
      spark_timeout_seconds: 7200
```

`runner_resource_profile` is the minimum profile for automatic Spark
compaction. The worker raises a submission to `medium` when the selected
rewrite exceeds 512 MiB, 250 files, or 100 delete files, and to `large` when
it exceeds 2 GiB, 1,000 files, or 400 delete files. It selects `xlarge` above
8 GiB, 2,000 files, or 800 delete files. If Spark reports an executor heap
error, the next durable retry moves up another profile through `xlarge`. This
right-sizing is per maintenance submission and does not modify or resubmit the
CDC job.

If `executor` is omitted while maintenance is enabled, Rivus defaults it to `hybrid` for backward compatibility.

The 256 MiB value is an additional compaction trigger. It is not required once
the active small-file count reaches 200 or equality deletes reach 50.

## Durable metadata schema

The worker owns these MySQL tables:

- `iceberg_maintenance_state`: one row per canonical `catalog.namespace.table`.
- `iceberg_maintenance_tasks`: durable, idempotent tasks and leases.
- `iceberg_maintenance_runs`: parent runs grouping claimed tasks.
- `iceberg_maintenance_results`: per-table operation results and routing metadata.

The queue uses MySQL 8 `FOR UPDATE SKIP LOCKED`, lease expiry/recovery, deterministic jitter, and bounded pages. Iceberg table metadata is loaded only after a task is claimed.

### Worker environment

```env
RIVUS_META_MYSQL_DSN=rivus:change-me@tcp(meta-mysql:3306)/rivus_meta?parseTime=true
RIVUS_MAINTENANCE_GOMAXPROCS=4
RIVUS_MAINTENANCE_POLL_INTERVAL_SECONDS=30
RIVUS_MAINTENANCE_LEASE_SECONDS=900
RIVUS_MAINTENANCE_TASK_PAGE_SIZE=1
RIVUS_MAINTENANCE_DUE_PAGE_SIZE=100
GOMEMLIMIT=3GiB

# Native Compaction Overrides (optional)
RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_INPUT_BYTES=1073741824   # Default: 512 MB (536870912)
RIVUS_MAINTENANCE_NATIVE_MAX_SELECTED_FILES=300                # Default: 250
RIVUS_MAINTENANCE_NATIVE_MAX_EQUALITY_DELETE_FILES=150         # Default: 100

# Cleanup Overrides (optional)
RIVUS_MAINTENANCE_NATIVE_EXPIRE_INTERVAL_SECONDS=86400         # Default: 86400 (24h)
RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_MAX_AGE_HOURS=168            # Default: 168 (7 days)
RIVUS_MAINTENANCE_NATIVE_SNAPSHOT_RETAIN_LAST=10               # Default: 10
RIVUS_MAINTENANCE_NATIVE_ORPHAN_INTERVAL_SECONDS=86400         # Default: 86400 (24h)
RIVUS_MAINTENANCE_NATIVE_ORPHAN_INACTIVE_INTERVAL_SECONDS=604800 # Default: 604800 (7 days)
RIVUS_MAINTENANCE_NATIVE_ORPHAN_MIN_AGE_HOURS=168              # Default: 168 (7 days)
RIVUS_MAINTENANCE_NATIVE_ORPHAN_DRY_RUN=false
```

## UI and API

The job-details UI keeps the existing Iceberg file-inventory panel, but its automatic-maintenance state comes from the durable maintenance tables. It no longer depends on a CDC-process scheduler.

The **View maintenance runs** action opens the Data Platform maintenance UI:

```text
https://data-platform.asmat.app/ui/admin/iceberg-maintenance/runs
```

That page shows global queue health plus recent worker runs. Run details include operation, execution engine (`native` or `spark`), routing reason, attempts, input/output file counts and bytes, duration, and errors.

Read-only API endpoints:

```text
GET /api/iceberg/maintenance/summary
GET /api/iceberg/maintenance/runs?limit=50&offset=0
GET /api/iceberg/maintenance/runs/{id}?limit=100
GET /api/iceberg/maintenance/tables/{catalog.namespace.table}
```

The existing per-job manual maintenance endpoint remains available. Manual requests are separate from automatic worker scheduling.

## Compaction executor behavior

### `executor: hybrid`

Rivus runs the `iceberg-go` compaction planner and bases routing on selected rewrite groups rather than total table size. By default, compaction routes to Spark when any of these apply:

- selected input is greater than 512 MiB;
- more than 250 selected input files (data plus delete files) are involved;
- more than 100 equality-delete files are involved.

Otherwise the rewrite runs natively. A recognized native Parquet compressed/uncompressed page-size limit delegates the rewrite to Spark in hybrid mode, including subsequent retries. Other native failures keep the normal retry policy. Position-delete files make a table
eligible after `position_delete_files_threshold` is reached, but do not by
themselves force a small rewrite to Spark. It also becomes eligible when the
current snapshot has 200 active small files, 50 active equality-delete files,
or at least 10 small files totaling 256 MiB. Every result records the actual
`engine` and `routing_reason`.

### `executor: native`

The same planner determines the rewrite groups, but Spark routing is disabled.
If a workload exceeds a native safety boundary, Rivus records it as skipped
instead of doing an unsafe native rewrite. A native failure is retried/failed
according to the normal worker retry policy; it is never silently handed to
Spark.

### `executor: spark`

After the normal inventory and planner eligibility check, a compaction task is
sent to the configured Spark/runner backend. Snapshot expiration and orphan
cleanup are still performed natively by the maintenance worker.

Native compaction uses atomic `RewriteDataFiles`, applies equality deletes while reading, and uses Iceberg dead-equality-delete cleanup before removing equality-delete files. Sort/Z-order and other Spark-specific rewrite strategies remain Spark-only.

## S3-compatible uploads and whole-file rewrites

Rivus native FileIO defaults `s3.compat-mode=true` when the configured S3
endpoint is outside `amazonaws.com` / `amazonaws.com.cn`. This avoids AWS
checksum trailers and `aws-chunked` object-write framing that some compatible
stores reject with `Chunk_Size_Exceeded_Max_Object_Size`. Native compatibility
mode omits object-write checksum headers; use HTTPS for transport protection.
Required checksums on operations such as bulk deletion remain handled by the SDK.
Set sink `s3_compat_mode: "false"` or `ICEBERG_S3_COMPAT_MODE=false` to retain
normal SDK behavior. An explicit sink value takes precedence over the environment.
No endpoint / AWS endpoints retain the AWS defaults unless explicitly enabled.

The runner-app compatibility patch and image upgrade are separate from this
Rivus change. That patch uses the same `ICEBERG_S3_COMPAT_MODE` environment
contract so its SQL and Python runners disable
Iceberg chunked encoding and enable MD5-compatible checksums. `spark-submit`
sets `aws.requestChecksumCalculation=WHEN_REQUIRED` for both the driver and
executors before JVM startup. The direct Spark backend applies the same policy
from Rivus configuration. Native and Spark clients are independent; upgrading
only one image does not update the other upload path. A Rivus-only rollout
continues to delegate Spark jobs to the existing runner-app version.

Compaction planning collapses row-group scan ranges to one full-file task per
physical path **before** grouping and resource sizing. Associated deletes and
row-lineage metadata are preserved; conflicting task metadata fails closed.
This prevents duplicate data-file removals at `ReplaceFiles` without dropping
ranges or rewriting the same rows twice. Inventory uses the same normalization.

The native Parquet page-size guard is unchanged. A recognized page-size limit
routes hybrid compaction to Spark; `executor: native` retains the error and
never delegates. A successful Spark retry still depends on readable source
files and working storage. This fallback does not treat corrupt Parquet or
arbitrary upload errors as successful maintenance.

The separate runner-app patch retains up to eight exception/cause headers independently
of the rolling stderr tail, so S3 error codes and request IDs can survive later
stack frames and Spark shutdown logs. Full per-line logs remain available in
the runner job log store.

## CDC priority and snapshot barrier

Native maintenance does not pause CDC. Successful Rivus commits mark the table
inventory dirty and bring its metadata scan forward. The worker uses the
resulting *active* small-file and equality-delete counts—not raw commit
counts—to decide whether compaction is due. Automatic compaction first tries
twice without pausing CDC. In hybrid or Spark mode, attempt three and later
escalate to coordinated Spark only when the preceding failure is an Iceberg
commit or branch-change conflict. Runner-app then pauses only the Rivus writers
that target the table, waits for them to stop committing, and resumes them
after Spark finishes. Timeouts, resource failures, and other errors do not
pause writers. Direct/manual Spark compaction remains coordinated by default.
Snapshot expiration and orphan cleanup remain online and do not pause writers.

Compaction priority follows file pressure and projected CDC growth. Rivus can
promote queued work, start before a threshold is crossed, and schedule another
pass when the refreshed inventory is still above threshold. Table leases keep
operations on the same table sequential.

Initial snapshots remain protected by a snapshot-complete barrier. Native compaction verifies its starting snapshot before staging work. Iceberg commit conflicts are retryable maintenance failures, so CDC wins concurrent writes.

## Snapshot expiration

Snapshot expiration uses the public Iceberg Go transaction API. Rivus stages expiration first, calculates eligible snapshots, records a no-op as `skipped`, and commits only when expiration is needed. Iceberg branch/tag references remain protected by Iceberg semantics.

## Orphan cleanup activity and scheduling

Managed tables receive orphan cleanup daily when active and weekly when idle,
plus bounded scheduling jitter. A table is active when its latest known data
write is less than seven days old. This activity window is independent of the
cleanup interval. Idle tables are still cleaned, including empty tables and
failed writes that leave files without committing a snapshot.

Activity comes from Rivus commit signals and data-changing snapshots observed
during inventory scans, including Spark and Trino writes. Inventory follows the
current snapshot's retained ancestry and uses commit timestamps, not scan time.
Append, overwrite, and delete snapshots count as writes; replace snapshots and
snapshots marked `rivus.maintenance` do not. Compaction and repeated scans of an
unchanged table therefore do not keep it active. Expired history does not erase
a previously recorded write. A newly enrolled old table is not treated as
recently written just because it was first scanned today.

The maintenance worker alone chooses the orphan schedule from the owner's
configuration and the worker's environment. CDC signals update activity and
request inventory refresh; they do not change orphan deadlines. A streaming
process with a different environment therefore cannot override the worker's
configured cleanup cadence.
Upgrade the streaming processes as well as the maintenance worker to enforce
this ownership rule; an older streaming binary can still update orphan
deadlines. Previously stored earlier deadlines are preserved, so an existing
cleanup may run before a newly configured longer interval takes effect.

External activity is detected on the existing paced inventory pass, so detection
can lag with large table counts. On startup and each full state sync (five
minutes by default), the worker pulls existing orphan deadlines within the
configured active/idle interval without postponing earlier or overdue work.
This also brings old 30/90-day schedules forward. Explicit YAML/environment
interval overrides remain effective; update old explicit values to adopt the
new cadence. An omitted inactive interval defaults to at least the active
interval when a longer active interval is configured.

The minimum orphan age remains seven days. Daily cleanup only removes eligible,
unreferenced files; it does not remove recent files or data still referenced by
retained snapshots. Snapshot expiration keeps its separate daily schedule and
retention settings. Paused monitors and incomplete snapshots retain their
existing scheduling protections.

## Bounded-memory orphan cleanup

The worker buckets referenced and candidate object paths to disk so full table listings do not need to reside in RAM at once. Prefix mismatch is fail-closed. The orphan minimum age is seven days, and dry-run mode identifies candidates without deleting them.

## Rollout

Recommended rollout:

1. Start with `enabled: true` and `executor: hybrid` on a small workload.
2. Start one maintenance worker and use orphan dry-run first.
3. Observe queue age, retries/commit conflicts, native execution time, Spark routing, and object-store request volume.
4. Use `executor: native` only when you explicitly want no Spark compaction fallback, or `executor: spark` when all compaction should be isolated in Spark.
5. Expand gradually to hundreds and then thousands of tables.
6. Scale workers only after the single-worker behavior is stable; MySQL leases support multiple workers safely.

## Current limitations

- Native sort/Z-order is not implemented; use Spark for those strategies.
- Hybrid mode routes more than 100 equality deletes and workloads beyond the
  native file/byte limits to Spark. Position-delete work stays native while it
  remains within those safety limits. Native mode skips workloads beyond its
  safety limits instead of routing them to Spark.
- Heavy Spark execution keeps the existing per-table runner/Spark submission behavior.
- Native rewrite output is committed atomically, but exact per-attempt uncommitted output paths are not exposed by the current high-level `iceberg-go` rewrite API; the seven-day orphan-cleanup safety window protects cleanup.
