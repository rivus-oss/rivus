# Automatic snapshot cutoffs

MySQL tables that are archived regularly can contain a large recent block and
small groups of old rows. `snapshot_cutoff` detects a candidate monthly boundary
from row counts. Configure it once on the source; Rivus generates matching
source snapshot and Iceberg replacement filters for each selected table.

```yaml
source:
  type: mysql
  config:
    # Connection and table selection omitted here.
    table_configs:
      asmat_*.tbl_reservasi:
        snapshot_cutoff:
          column: TglBerangkat
          min_month_rows: 10000
          min_growth_ratio: 10
          lookback_months: 24
          query_timeout_seconds: 30
      asmat_*.tbl_transaksi_addon:
        snapshot_cutoff:
          column: WaktuTransaksi
sink:
  type: iceberg_native
  config:
    # Catalog and object-storage settings omitted here.
    snapshot_write_mode: auto-cutoff-v1
    snapshot_replace_delete_executor: trino
    trino_delete:
      uri: "${TRINO_URI}"
      catalog: asmat
      user: rivus
```

The four numeric settings above show the defaults. Lower `min_month_rows` for
smaller tables. `min_growth_ratio` must be greater than 1. `lookback_months`
accepts 1–120; the per-table query timeout accepts 1–600 seconds.

Remove the manual source `filter` and matching sink `snapshot_replace_filters`
entries for automatic tables. Keep manual filters for other tables if needed.
Automatic tables must use a real `DATE`, `DATETIME` or `TIMESTAMP` column. Native
full-table replacement, append-only modes and non-Iceberg sinks are rejected.
The implementation uses the existing Trino range-replacement path. The versioned
`auto-cutoff-v1` mode requires this worker version: older workers reject it instead
of silently ignoring the new source settings. The sink factory translates it to
`auto` only after the source has durably resolved the predicates. Upgrade workers
before submitting the updated configuration.

## Detection rule

1. Read the current month from MySQL. Count rows by month through the current
   month, including older and zero-date/NULL groups. Far-future dates cannot
   anchor detection. A date index helps these queries; a timeout fails startup.
2. Require the last **completed** month to have at least `min_month_rows` rows.
   A partial current month cannot determine the boundary.
3. Walk backward over at most `lookback_months` completed months. Choose the
   first month with at least `min_month_rows` rows whose preceding month has
   fewer than that threshold and whose count is at least `min_growth_ratio`
   times the preceding count. An empty preceding month also qualifies.
4. If there is no qualifying boundary, stop before creating the sink or resetting
   any target. Use an explicit pair of filters or adjust the rule after checking
   the counts. There is no fallback to the current month or the oldest date.

For example, 5,268 September rows and 119,928 October rows select October 1 with
the defaults. A source with populated October, November and December months
keeps the earlier qualifying boundary. In January, December is the last
completed month; the search crosses the year boundary normally.

For each table, a result such as `2026-10-01` produces:

```yaml
# Runtime source predicate:
filter: "`TglBerangkat` >= '2026-10-01'"
# Runtime sink replacement predicate:
column: tglberangkat
op: ">="
value: "2026-10-01"
```

Rows on or after the boundary, including future dates, remain in scope. Dates
before it and NULL dates are excluded from the source snapshot. CDC/binlog
streaming keeps its existing behavior; these predicates apply to snapshots.

Counts are a heuristic, not evidence that every older row is invalid or already
archived. A qualifying count jump does not prove that a month contains every
historical row. Use this feature only where that rule matches the archive
process. It does not verify backup-table contents. Coordinate snapshots with
the external archive process, which can still move rows during a snapshot.

## Saved scope and restarts

Before constructing the sink, Rivus saves **all** selected tables' results in
`job_snapshot_scopes`, keyed by the internal state key and submission ID. It
then reloads the saved plan before applying either filter. Duplicate writers
cannot overwrite the first plan. Split workers save it under their ownership
fence. Ordinary checkpoint resets retain these plans.

Retries, restarts and snapshot-to-CDC handoffs reuse the saved dates without
querying new counts. A new snapshot submission can detect new dates. This does
not continuously move the cutoff while an existing job is streaming.

Saved plans include a configuration fingerprint. Changed settings or corrupt
plans stop startup instead of silently changing a partially copied snapshot.
Enabling this feature on a job with existing checkpoints or retained commit
intents and no saved scope requires a controlled new snapshot submission; do
not delete checkpoints just to bypass that check. Purged-binlog recovery still
uses the existing [checkpoint recovery](iceberg-checkpoint-recovery.md) process.

Logs and preflight progress show each table's cutoff and excluded row count.
The plan JSON retains the reference month, boundary-month count and preceding
month count for inspection. Stored job YAML remains unchanged at runtime.
