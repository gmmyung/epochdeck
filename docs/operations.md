# Production operations

## Physical backup and restore

Portable project export is useful for migration. Disaster recovery needs the
complete server state: the SQLite catalog, every immutable Parquet segment, and
the full blob CAS including content not currently reachable from one project.
Client delivery journals live on each training machine and are backed up
separately from the server roots.

The server holds an advisory `runloom.lock` in each distinct canonical data,
metric, and blob root for its entire lifetime. It sorts and deduplicates the
canonical lock paths before taking all locks, so two instances cannot share an
external metric or blob root even when their data roots differ. Physical backup
and restore take the same complete lock set and refuse to run while any root is
active. Root-level lock files and incomplete metric/blob staging files are
coordination state and are excluded from backup payloads. A backup uses SQLite's
backup API, streams files through 1 MiB buffers, records a SHA-256 inventory,
syncs every generated directory bottom-up, and publishes its directory
atomically. The destination parent is synced after the rename, so a successful
return includes the complete published tree in the durability boundary.
New bundle roots are mode `0700` and their files are mode `0600`, independent
of the invoking shell's umask.

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

Restore streams each regular bundle file once into exclusive staging while
verifying every inventory digest, then runs `PRAGMA integrity_check`. Metric and
blob staging trees are synced bottom-up before their top-level entries are
installed and their destination roots are synced. The catalog is published and
its parent synced last, so a failed restore cannot expose manifests before their
durable files. It rejects symbolic-link
sources, duplicate inventory destinations, any bundle/storage-root overlap in
either ancestor direction, an existing catalog, and non-empty metric/blob
roots other than their acquired `runloom.lock` files. Metric and blob roots must
be disjoint; the data root may contain them but cannot equal or sit inside
either.
Practice restore to separate paths and query representative long runs before
depending on a backup schedule.

## Diagnostics

`GET /api/v1/diagnostics` and `runloom doctor` report a bounded operational
snapshot:

- process uptime;
- total, active, admission-rejected, slow, and 5xx request counts;
- history query count plus total and maximum observed duration;
- the 64-request API admission limit, two-request health limit, rejection count,
  and currently available permits;
- available ingest, blob-upload, artifact-I/O, 16-stream raw-download, and query
  worker permits;
- catalog, metric, and blob filesystem capacity and device identity; and
- the most recent 64 slow request paths, statuses, and durations.

Counters are in-memory and reset with the process. They diagnose contention and
slow endpoints without creating another persistent telemetry database. HTTP
logs remain available through journald.

Except for embedded static dashboard assets, a request must acquire a
non-queuing admission permit before body parsing. General API traffic has 64
permits and health checks have a separate two permits. A full pool returns HTTP
503 with `server_busy` and `Retry-After: 1` immediately. The permit remains with
the response body through EOF or disconnect, so at most 64 general API requests
can retain parsed bodies, wait on mutation/worker permits, or leave responses
outstanding. Raw blob and artifact-file responses additionally use a
16-permit stream pool so slow media clients cannot occupy all general slots.

## Disposable pre-alpha storage

Runloom has one current internal storage definition and no catalog-generation
marker or automatic upgrade path. When that definition changes, stop the
service, archive all three configured roots together, and start the new build
with empty roots. Physical backups preserve bytes and integrity but carry no
internal catalog or build compatibility marker; they are not storage-upgrade
vehicles. Keep the archived roots and exact creating build until the replacement
instance and any required project imports are verified.
