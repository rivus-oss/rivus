# Production deployment

This guide describes the recommended split-runtime deployment for Rivus.

Rivus is a pre-1.0 project. Treat upgrades as controlled infrastructure changes: pin images, back up metadata, stop workers gracefully, and validate checkpoints before and after rollout.

## Recommended topology

Use one Rivus image with four runtime roles and one durable metadata database:

```text
                         ┌──────────────────────┐
                         │     rivus-master     │
                         │ API / UI / lifecycle │
                         └──────────┬───────────┘
                                    │
                         durable metadata MySQL
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
      rivus-streaming        rivus-snapshot       rivus-maintenance
         binlog CDC          initial snapshot     compact / expire / orphan
```

The roles communicate through durable metadata rather than in-process state. The master does not execute CDC or snapshot pipelines.

The maintenance queue worker backs off and retries transient metadata outages,
including DNS failures, transaction deadlines, and deadlocks. It drains its
executor pools before starting a new coordinator attempt; durable leases and
commit reconciliation still decide which tasks may run again. Shutdown remains
interruptible, while invalid configuration and ownership errors remain visible
instead of being treated as metadata availability failures. One-shot maintenance
commands continue to return their errors to the caller.

The master shares one initialized maintenance store across API and job-detail
requests. A failed initialization is retried after a short cooldown, so a startup
timeout does not permanently break the monitor page until the master restarts.

## Image pinning

Do not use `latest` for a production rollout.

Prefer either:

```text
ghcr.io/gerinsp/rivus:vX.Y.Z
```

or an immutable commit image:

```text
ghcr.io/gerinsp/rivus:sha-<commit>
```

The publish workflow also updates `latest` from `main`, so `latest` is intentionally a moving target.

## Metadata MySQL

The metadata database stores durable control-plane state including job registry records, offsets/checkpoints, snapshot progress, worker leases, and Iceberg maintenance state.

For production:

- keep metadata MySQL on persistent storage;
- back it up independently from Rivus containers;
- do not destroy its volume during routine Rivus redeployments;
- restrict network access to Rivus roles and administrators;
- monitor capacity and availability like any other stateful production database.

The repository Compose file includes MySQL for convenience. A production environment may run metadata MySQL in a separate Compose project, VM, managed database, or other independently managed service as long as every Rivus role uses the same `RIVUS_META_MYSQL_DSN`.

## Split runtime services

Use the following commands:

```text
/app/rivus master
/app/rivus streaming-worker
/app/rivus snapshot-worker
/app/rivus maintenance-worker --queue
```

A minimal production-shaped Compose layout looks like this:

```yaml
services:
  rivus:
    image: ghcr.io/gerinsp/rivus:sha-<commit>
    command: ["/app/rivus", "master"]
    restart: unless-stopped
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_API_TOKEN: ${RIVUS_API_TOKEN}
      RIVUS_SHUTDOWN_TIMEOUT_SECONDS: 90
      RIVUS_LOG_ROOT: /app/logs
      RIVUS_LOG_DIR: /app/logs/master
      RIVUS_LOG_PREFIX: rivus
      RIVUS_LOG_STDERR: "false"
    volumes:
      - ./logs:/app/logs
    ports:
      - "8080:8080"

  rivus-streaming:
    image: ghcr.io/gerinsp/rivus:sha-<commit>
    command: ["/app/rivus", "streaming-worker"]
    restart: unless-stopped
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: ${RIVUS_STREAMING_WORKER_ID:-}
      RIVUS_SHUTDOWN_TIMEOUT_SECONDS: 90
      RIVUS_LOG_ROOT: /app/logs
      RIVUS_LOG_DIR: /app/logs/streaming
      RIVUS_LOG_PREFIX: rivus-streaming
      RIVUS_LOG_STDERR: "false"
    volumes:
      - ./logs:/app/logs

  rivus-snapshot:
    image: ghcr.io/gerinsp/rivus:sha-<commit>
    command: ["/app/rivus", "snapshot-worker"]
    restart: unless-stopped
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: ${RIVUS_SNAPSHOT_WORKER_ID:-}
      RIVUS_SHUTDOWN_TIMEOUT_SECONDS: 90
      RIVUS_LOG_ROOT: /app/logs
      RIVUS_LOG_DIR: /app/logs/snapshot
      RIVUS_LOG_PREFIX: rivus-snapshot
      RIVUS_LOG_STDERR: "false"
    volumes:
      - ./logs:/app/logs
      - ./snapshot-spool:/var/lib/rivus/snapshot-spool

  rivus-maintenance:
    image: ghcr.io/gerinsp/rivus:sha-<commit>
    command: ["/app/rivus", "maintenance-worker", "--queue"]
    restart: unless-stopped
    stop_grace_period: 2m
    environment:
      RIVUS_META_MYSQL_DSN: ${RIVUS_META_MYSQL_DSN}
      RIVUS_WORKER_ID: ${RIVUS_MAINTENANCE_WORKER_ID:-}
      RIVUS_LOG_ROOT: /app/logs
      RIVUS_LOG_DIR: /app/logs/maintenance
      RIVUS_LOG_PREFIX: rivus-maintenance
      RIVUS_LOG_STDERR: "false"
    volumes:
      - ./logs:/app/logs
```

Add source, Doris, Iceberg, object-storage, runner, Telegram, and other role-specific environment settings as required by your jobs.

## Log layout

All roles should mount the same log root while writing to separate subdirectories:

```text
/app/logs/
├── master/
├── streaming/
├── snapshot/
└── maintenance/
```

Recommended variables:

```env
RIVUS_LOG_ROOT=/app/logs
RIVUS_MASTER_LOG_DIR=/app/logs/master
RIVUS_STREAMING_LOG_DIR=/app/logs/streaming
RIVUS_SNAPSHOT_LOG_DIR=/app/logs/snapshot
RIVUS_MAINTENANCE_LOG_DIR=/app/logs/maintenance
RIVUS_LOG_RETENTION_DAYS=7
RIVUS_LOG_MAX_SIZE_MB=64
RIVUS_LOG_MAX_TOTAL_SIZE_MB=2048
RIVUS_LOG_STDERR=false
```

The master dashboard can read all role directories from the shared root. CDC job log views prefer the streaming directory.

`RIVUS_LOG_STDERR=false` is recommended for normal production operation because CDC output can be high volume and Rivus already rotates its application files. Docker's own log driver can still be configured with a small bounded size for process-level stderr/stdout.

## Graceful shutdown

Rivus workers use `SIGTERM` to stop claiming new work and drain active execution toward durable checkpoints.

Keep:

```text
Docker stop_grace_period > RIVUS_SHUTDOWN_TIMEOUT_SECONDS
```

The repository defaults are:

```text
RIVUS_SHUTDOWN_TIMEOUT_SECONDS=90
stop_grace_period=2m
```

Do not routinely use `docker kill -9` or an equivalent forced termination for workers.

For a controlled replacement:

```sh
docker compose pull
docker compose stop -t 120 rivus rivus-streaming rivus-snapshot rivus-maintenance
docker compose up -d
```

If metadata MySQL is managed separately, do not stop or recreate it as part of the Rivus application rollout.

## Snapshot spool

Large initial snapshots can use disk-backed spool state. Persist the configured spool directory if you expect a snapshot to survive container replacement.

Example:

```yaml
volumes:
  - ./snapshot-spool:/var/lib/rivus/snapshot-spool
```

Size the volume for your largest expected in-progress snapshot behavior and monitor free space.

## Worker identity

When `RIVUS_WORKER_ID` is empty, Rivus generates a unique process identity. This is the safest default for Compose deployments that may be copied or scaled.

If you configure an explicit worker ID, ensure two live workers never use the same ID.

## Security

Before exposing the master outside a trusted internal network:

- enable UI login;
- set a strong `RIVUS_UI_SESSION_SECRET`;
- set `RIVUS_API_TOKEN` for protected API access;
- put TLS at the reverse proxy or ingress layer;
- keep metadata MySQL and worker-only endpoints private;
- inject source/database/object-storage credentials through a secret manager or protected environment file;
- rotate any credential that is accidentally written to logs, tickets, screenshots, or chat.

Never commit real credentials into example YAML, `.env`, or repository history.

## Source CDC prerequisites

Before enabling CDC, verify the source database configuration expected by the job and ensure the Rivus source account has the required replication/read permissions.

Operationally, binlog retention must be long enough to cover:

- planned maintenance windows;
- container replacement time;
- snapshot duration before streaming handoff;
- the longest outage you intend to recover from without taking a new snapshot.

Rivus cannot resume from a binlog file that the source has already purged.

To alert on that unsafe restart window, enable Telegram checkpoint-purge
notifications globally:

```env
RIVUS_TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
RIVUS_TELEGRAM_NOTIFY_CHECKPOINT_PURGED=true
```

This alert is controlled only by the environment variable; no per-job YAML is
required. Rivus sends one alert for each purged-checkpoint incident, re-arms
after the checkpoint becomes available again, and retries on the next health
probe if delivery fails.

### Runtime container versions

The authenticated master endpoint `GET /api/runtime/versions` reports the
running image tag, commit, build date, uptime, and heartbeat state for the
master, streaming, snapshot, and maintenance containers. Each process writes a
metadata heartbeat every 30 seconds; the endpoint marks it offline after 90
seconds without a heartbeat.

## Iceberg and object storage

For Iceberg jobs:

- keep REST catalog and object-storage credentials consistent across streaming, snapshot, and maintenance roles;
- ensure all three roles can reach the catalog and object store;
- use maintenance concurrency appropriate for the catalog/object-store capacity;
- monitor metadata growth, small-file growth, failed maintenance tasks, and object-store errors.

See [iceberg-maintenance.md](iceberg-maintenance.md).

## Production validation checklist

After deployment, validate all of the following before declaring the rollout complete:

- master UI/API is reachable and authenticated as expected;
- running jobs have the expected durable execution role;
- streaming jobs have an active lease and advancing progress/checkpoints;
- newly changed source rows arrive at the expected sink;
- snapshot jobs are claimed only by the snapshot worker;
- snapshot-to-streaming handoff completes for a test job when applicable;
- maintenance dashboard can read durable maintenance state;
- maintenance worker can claim and finish a safe test task;
- master can list `master/`, `streaming/`, `snapshot/`, and `maintenance/` log files;
- application logs rotate and do not grow without bound;
- metadata MySQL backup and restore procedures are documented and tested by the operator.

## Monitoring priorities

At minimum, monitor:

- process/container availability;
- job state and lease freshness;
- checkpoint/binlog position freshness;
- sink errors and retries;
- snapshot progress and spool disk usage;
- Iceberg maintenance queue depth, task age, retries, and failures;
- metadata MySQL availability and storage;
- object-storage capacity and error rate;
- Rivus log directory size.

## Rollback

If a new image behaves incorrectly:

1. stop the affected workers gracefully;
2. keep metadata MySQL unchanged;
3. redeploy the previously known-good image tag;
4. verify worker leases are reclaimed;
5. confirm CDC resumes from durable checkpoints;
6. validate sink data before reopening normal operations.

Do not manually rewrite checkpoint or lease records unless you understand the durable-state semantics and have a metadata backup.

For version-to-version planning, see [migration.md](migration.md) and [release-policy.md](release-policy.md).
