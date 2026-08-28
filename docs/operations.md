# Production operations

## Physical backup and restore

Portable project export is useful for migration. Disaster recovery needs the
complete physical state: SQLite catalog, delivery journal, every immutable
Parquet segment, and the full blob CAS including content not currently reachable
from one project.

The server holds an advisory lock at `RUNLOOM_DATA_DIR/runloom.lock` for its
entire lifetime. Physical backup and restore take the same lock and refuse to
run while the server is active. A backup uses SQLite's backup API, streams files
through 1 MiB buffers, records a SHA-256 inventory, excludes only incomplete
blob staging files, and publishes its directory atomically.

```bash
sudo systemctl stop runloom
sudo -u runloom env \
  RUNLOOM_DATA_DIR=/var/lib/runloom/data \
  RUNLOOM_METRICS_DIR=/var/lib/runloom/metrics \
  RUNLOOM_BLOBS_DIR=/srv/runloom/blobs \
  runloom backup /backups/runloom-$(date +%Y%m%d-%H%M%S)
sudo systemctl start runloom
```

For multi-terabyte blob stores, keep downtime short with filesystem snapshots.
Stop Runloom, snapshot every dataset that contains a configured root, then start
Runloom immediately. Replicate or run `runloom backup` against read-only mounts
of those snapshots while the live service continues. The stop/snapshot/start
window is what coordinates snapshots across the SSD and HDD/ZFS roots.

Restore only into empty roots:

```bash
sudo systemctl stop runloom
sudo -u runloom env \
  RUNLOOM_DATA_DIR=/var/lib/runloom/data \
  RUNLOOM_METRICS_DIR=/var/lib/runloom/metrics \
  RUNLOOM_BLOBS_DIR=/srv/runloom/blobs \
  runloom restore /backups/runloom-20260828-220000
sudo systemctl start runloom
```

Restore verifies every inventory digest and runs `PRAGMA integrity_check` before
returning. It never overlays an existing catalog or a non-empty metric/blob root.
Practice restore to separate paths and query representative long runs before
depending on a backup schedule.

## Diagnostics

`GET /api/v1/diagnostics` and `runloom doctor` report a bounded operational
snapshot:

- process uptime and catalog schema version;
- total, active, slow, and 5xx request counts;
- history query count plus total and maximum observed duration;
- available ingest and query worker permits; and
- the most recent 64 slow request paths, statuses, and durations.

Counters are in-memory and reset with the process. They diagnose contention and
slow endpoints without creating another persistent telemetry database. HTTP
logs remain available through journald.

## Pre-alpha schema changes

The catalog stores an explicit schema version. A binary refuses to open any
different version, including an older unversioned pre-alpha catalog. Runloom
does not carry migration or dual-read scaffolding before alpha.

Before changing binaries, take a physical backup. If the new pre-alpha binary
rejects the catalog, either restore the backup and matching binary, or start a
new empty instance by stopping the service and moving all three configured roots
to dated, recoverable names. Do not delete the old roots until the new instance
and any required project imports have been verified.
