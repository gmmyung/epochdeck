# Production operations

## Network boundary

EpochDeck has no native application authentication or multi-user authorization.
Bind its HTTP listener to loopback and make an authenticated HTTPS reverse proxy
the only externally reachable service. The proxy must terminate TLS and enforce
access control before forwarding to EpochDeck. Never expose the EpochDeck HTTP
port directly on a public or private network interface.

## Physical backup and restore

Portable project export is useful for migration. Disaster recovery needs the
complete server state: the SQLite catalog, every immutable Parquet segment, and
the full blob CAS including content not currently reachable from one project.
Client delivery journals live on each training machine and are backed up
separately from the server roots.

The server holds an advisory `epochdeck.lock` in each distinct canonical data,
metric, and blob root for its entire lifetime. It sorts and deduplicates the
canonical lock paths before taking all locks, so two instances cannot share an
external metric or blob root even when their data roots differ. Physical backup
and restore take the same complete lock set and refuse to run while any root is
active. Root-level lock files and incomplete metric/blob staging files are
coordination state and are excluded from backup payloads. A backup uses SQLite's
backup API, streams files through 1 MiB buffers, records a SHA-256 inventory,
flushes every generated file, and publishes its directory atomically. On Unix,
every generated directory is fsynced bottom-up and the destination parent is
fsynced after the rename, so a successful return includes the complete tree in
the durability boundary. CPython exposes no equivalent directory fsync on
Windows; there EpochDeck validates every traversed directory and flushes file
contents, but cannot promise identical power-loss durability for directory
entries. New bundle roots are mode `0700` and their files are mode `0600` on
POSIX filesystems, independent of the invoking shell's umask.

Metric and blob roots must support same-filesystem hard links. The server probes
that exact publication primitive during startup and refuses an unsupported
filesystem instead of falling back to a non-atomic copy or replacement. The
staging and final directories are always inside the same configured root, so no
hard link is required between independently configured roots.

```bash
sudo systemctl stop epochdeck
sudo -u epochdeck env \
  EPOCHDECK_DATA_DIR=/var/lib/epochdeck/data \
  EPOCHDECK_METRICS_DIR=/var/lib/epochdeck/metrics \
  EPOCHDECK_BLOBS_DIR=/var/lib/epochdeck/blobs \
  epochdeck backup /backups/epochdeck-$(date +%Y%m%d-%H%M%S)
sudo systemctl start epochdeck
```

For large blob roots, keep downtime short with filesystem snapshots. Stop
EpochDeck, snapshot every filesystem volume that contains a configured root,
then start EpochDeck immediately. Replicate or run `epochdeck backup` against
read-only mounts of those snapshots while the live service continues. The
stop/snapshot/start window coordinates one consistent point across all
configured roots.

Restore only into empty roots:

```bash
sudo systemctl stop epochdeck
sudo -u epochdeck env \
  EPOCHDECK_DATA_DIR=/var/lib/epochdeck/data \
  EPOCHDECK_METRICS_DIR=/var/lib/epochdeck/metrics \
  EPOCHDECK_BLOBS_DIR=/var/lib/epochdeck/blobs \
  epochdeck restore /backups/epochdeck-20260828-220000
sudo systemctl start epochdeck
```

Restore streams each regular bundle file once into exclusive staging while
verifying every inventory digest, then runs `PRAGMA integrity_check`. Metric and
blob staging files are flushed before their top-level entries are installed.
Unix additionally fsyncs the staging trees and destination roots bottom-up, then
publishes the catalog and fsyncs its parent last, so a failed restore cannot
expose manifests before their durable files. Windows retains the file-flush and
publication ordering with the directory-fsync limitation above. Restore rejects
symbolic-link sources, duplicate inventory destinations, any bundle/storage-root overlap in
either ancestor direction, an existing catalog, and non-empty metric/blob
roots other than their acquired `epochdeck.lock` files. Metric and blob roots must
be disjoint; the data root may contain them but cannot equal or sit inside
either.
Practice restore to separate paths and query representative long runs before
depending on a backup schedule.

## Pre-alpha upgrade and rollback

Treat the executable and its three storage roots as one versioned unit. Before
an upgrade, stop the service, snapshot or archive the catalog, metric, and blob
roots together, and record the exact running binary checksum. Install the new
server and matching Python wheel, point the service at empty replacement roots,
then start it and run:

```bash
epochdeck doctor --server-url http://127.0.0.1:8787
curl --fail http://127.0.0.1:8787/api/v1/health
```

Log a sample run, inspect metrics and native media in the dashboard, and verify
a backup/restore exercise before importing production-scale data. To roll back,
stop the new process and restore the previous binary with its untouched matching
roots. Never open newer roots with an older binary or mix roots from different
deployments.

Dashboard branding is deployment configuration, not EpochDeck storage. Physical
backups do not include `/etc/epochdeck/epochdeck.env` or a file named by
`EPOCHDECK_DASHBOARD_LOGO_PATH`; back up and restore those files with the rest of
the host configuration. The server loads and validates branding once at
startup, so replacing a logo or changing the accent color requires a restart.

## Diagnostics

`GET /api/v1/diagnostics` and `epochdeck doctor` report a bounded operational
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

EpochDeck has one current internal storage definition and no catalog-generation
marker or automatic upgrade path. When that definition changes, stop the
service, archive all three configured roots together, and start the new build
with empty roots. Physical backups preserve bytes and integrity but carry no
internal catalog or build compatibility marker; they are not storage-upgrade
vehicles. Keep the archived roots and exact creating build until the replacement
instance and any required project imports are verified.
