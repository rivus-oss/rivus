-- Apply after deploying a Rivus build that supports metadata_cleanup_enabled.
-- This updates persisted configuration, not Iceberg data or task history.
-- The worker reloads settings during its normal state sync (up to five minutes)
-- and pulls existing orphan deadlines forward without postponing overdue work.
-- Review the affected owners before executing this file against production.

START TRANSACTION;

UPDATE job_registry
SET config_json=JSON_SET(config_json,
  '$.sink.config.table_maintenance.native_orphan_interval_seconds', 86400,
  '$.sink.config.table_maintenance.native_orphan_inactive_interval_seconds', 604800,
  '$.sink.config.table_maintenance.native_orphan_min_age_hours', 168,
  '$.sink.config.table_maintenance.native_expire_interval_seconds', 86400,
  '$.sink.config.table_maintenance.native_snapshot_max_age_hours', 168,
  '$.sink.config.table_maintenance.native_snapshot_retain_last', 10,
  '$.sink.config.table_maintenance.metadata_cleanup_enabled', CAST('true' AS JSON),
  '$.sink.config.table_maintenance.metadata_previous_versions_max', 10),
  updated_at=UTC_TIMESTAMP(6)
WHERE JSON_UNQUOTE(JSON_EXTRACT(config_json,'$.sink.type'))='iceberg_native'
  AND JSON_EXTRACT(config_json,'$.sink.config.table_maintenance.enabled')=CAST('true' AS JSON);

SELECT ROW_COUNT() AS updated_jobs;

UPDATE iceberg_maintenance_monitors
SET config_json=JSON_SET(config_json,
  '$.sink.config.table_maintenance.native_orphan_interval_seconds', 86400,
  '$.sink.config.table_maintenance.native_orphan_inactive_interval_seconds', 604800,
  '$.sink.config.table_maintenance.native_orphan_min_age_hours', 168,
  '$.sink.config.table_maintenance.native_expire_interval_seconds', 86400,
  '$.sink.config.table_maintenance.native_snapshot_max_age_hours', 168,
  '$.sink.config.table_maintenance.native_snapshot_retain_last', 10,
  '$.sink.config.table_maintenance.metadata_cleanup_enabled', CAST('true' AS JSON),
  '$.sink.config.table_maintenance.metadata_previous_versions_max', 10),
  updated_at=UTC_TIMESTAMP(6)
WHERE JSON_UNQUOTE(JSON_EXTRACT(config_json,'$.sink.type'))='iceberg_native'
  AND JSON_EXTRACT(config_json,'$.sink.config.table_maintenance.enabled')=CAST('true' AS JSON);

SELECT ROW_COUNT() AS updated_monitors;

COMMIT;
