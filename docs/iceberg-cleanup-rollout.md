# Cleanup frequency and metadata growth rollout

This rollout changes cleanup frequency and enables bounded metadata JSON history.
It retains seven days of snapshot history, at least ten snapshots, and the
seven-day minimum age for orphan candidates. Ten snapshots is a minimum, not a
hard cap on recent snapshots. Live data and retained history can still grow.

## Configuration

```yaml
table_maintenance:
  enabled: true
  native_orphan_interval_seconds: 86400
  native_orphan_inactive_interval_seconds: 604800
  native_orphan_min_age_hours: 168
  native_expire_interval_seconds: 86400
  native_snapshot_max_age_hours: 168
  native_snapshot_retain_last: 10
  metadata_cleanup_enabled: true
  metadata_previous_versions_max: 10
```

The metadata policy is opt-in (default false). Explicit false overrides the
environment default. Environment names are
`RIVUS_MAINTENANCE_METADATA_CLEANUP_ENABLED` and
`RIVUS_MAINTENANCE_METADATA_PREVIOUS_VERSIONS_MAX`.

For enabled owners, Rivus creates new tables with automatic metadata deletion
enabled and reconciles existing/discovered tables during normal inventory scans.
It commits only if properties differ, using the catalog's optimistic transaction
requirements. It enforces the configured previous-version count even if a table
currently has a different count. This policy changes metadata properties without
expiring snapshots or rewriting data. Current metadata plus up to ten tracked
previous versions can remain. Untracked files accumulated before enabling the
policy require a separate orphan cleanup. A queued orphan dry run does not disable
this independently configured metadata policy.

## Release order

1. Deploy the new binary/UI with the metadata policy support.
2. Update Compose/environment and the actual YAML job/monitor definitions to
   the values above. Keep orphan concurrency at one for the initial rollout.
3. Review the affected persisted owners, then execute
   [maintenance-cleanup-policy.sql](../scripts/maintenance-cleanup-policy.sql)
   against the metadata database. This updates only enabled Iceberg jobs and
   monitors, preserving other configuration and runtime state. YAML and environment
   edits alone do not replace values already saved in `config_json`.
4. The maintenance worker reloads settings on its normal state sync (up to five
   minutes). Existing later orphan deadlines move within one day for active
   tables or seven days for inactive tables. Earlier/overdue deadlines stay put.
   A shorter interval does not automatically run the whole backlog immediately.
5. Inspect the Cleanup tab per operation. Check next due, last successful run,
   queued/running states, failed/skipped reasons, and recorded results.

The deployment repository's `configs/` directory is ignored by Git. Copy the
updated deployment definitions as well as releasing the tracked Compose change.
Do not resubmit ingestion jobs just to update maintenance policy.

## One-time metadata backlog

Start with the largest metadata prefixes, then continue with the remaining tables.
Produce a fresh inventory from the Iceberg catalog containing the following fields
(namespace is an array so nested namespaces remain unambiguous):

```json
[{"catalog":"ds","namespace":["ds"],"table":"crm_notification_log",
  "location":"s3://warehouse-ds/ds/crm_notification_log"}]
```

Generate read-only Spark previews:

```sh
python3 scripts/iceberg-metadata-dry-run.py inventory.json --output metadata-preview.sql
```

Run the generated SQL in Spark with the configured Iceberg catalog. It scans only
each table's `metadata` directory, compares against current table references, and
uses a fixed UTC cutoff seven days before generation. The script never generates
a deletion command. Review the returned candidate paths before running a cleanup.
Do not treat a raw storage listing or byte estimate as reference validation.

For the actual pass, use the existing Data Platform **Online cleanup** job with
explicit table selection, **Remove orphan files** and **Metadata-only orphan
cleanup** enabled, **Expire old snapshots** disabled, orphan retention seven
days, minimum metadata cleanup zero, and concurrency one. Start with a small
batch and verify the job succeeds before widening the selection. This pass does
not remove old Parquet history. Normal daily snapshot expiration and subsequent
whole-table orphan cleanup handle eligible data files separately.

The metadata folder can contain JSON, manifests, and statistics files. Its name
does not make every file disposable. Do not use `mc rm` or bypass Iceberg's
reference checks. Do not use the UI's SQL preview as a substitute for verifying
the actual metadata-only job settings.

## Measure before raising concurrency

Inspect the Cleanup tab's recent-attempt durations and results. Compare queued
eligible wait time with execution time: a queue delay and a slow storage scan
need different fixes. Sample the following read-only queries before and after
rollout:

```sql
SELECT operation, status, COUNT(*) AS tasks,
       MAX(GREATEST(0, TIMESTAMPDIFF(SECOND,
           GREATEST(created_at, not_before), UTC_TIMESTAMP()))) AS eligible_wait_seconds
FROM iceberg_maintenance_tasks
WHERE status IN ('queued', 'retry')
GROUP BY operation, status;

SELECT operation, COUNT(*) AS successful_attempts,
       ROUND(AVG(duration_ms)) AS average_duration_ms,
       MAX(duration_ms) AS maximum_duration_ms,
       SUM(deleted_bytes) AS recorded_deleted_bytes
FROM iceberg_maintenance_results
WHERE status='succeeded' AND created_at >= UTC_TIMESTAMP() - INTERVAL 1 DAY
GROUP BY operation;
```

Snapshot expiration does not currently measure reclaimed bytes, so its recorded
zero must not be interpreted as zero storage reclaimed. Separate Spark jobs may
not update the native worker's records. Measure net object-store usage over time
separately from bytes written or bytes reported by cleanup.

Sources: [Iceberg metadata maintenance](https://iceberg.apache.org/docs/latest/maintenance/#remove-old-metadata-files)
and [Spark orphan procedures](https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files).
