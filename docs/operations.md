# Operations

This page covers backup, restore, upgrades, and diagnostics. Installation and
network exposure live in [self-hosting](deployment.md) and the
[security policy](../SECURITY.md).

## Backup

A physical backup must include all three roots together:

- SQLite catalog and metadata;
- immutable metric segments; and
- content-addressed blobs.

Stop the server before using the administration command:

```bash
sudo systemctl stop epochdeck
sudo -u epochdeck env \
  EPOCHDECK_DATA_DIR=/var/lib/epochdeck/data \
  EPOCHDECK_METRICS_DIR=/var/lib/epochdeck/metrics \
  EPOCHDECK_BLOBS_DIR=/var/lib/epochdeck/blobs \
  epochdeck backup /backups/epochdeck-$(date +%Y%m%d-%H%M%S)
sudo systemctl start epochdeck
```

The command refuses active roots, verifies content, and publishes a complete
backup atomically. Client delivery spools live on training machines and require
separate backups.

For large installations, stop the server briefly and snapshot every filesystem
containing a root at one coordinated point. Replicate the snapshots after the
server restarts.

## Restore

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

Restore verifies the inventory, file digests, and SQLite integrity before
publication. Practice restoring to separate paths and inspect representative
runs before relying on a backup schedule.

## Upgrade and rollback

Pre-alpha builds do not migrate stored data. Treat the binary and its three
storage roots as one versioned unit.

Before upgrading:

1. Stop the service.
2. Back up or snapshot all roots together.
3. Record the running binary checksum.
4. Install the matching new server and Python wheel.
5. Start the new build with empty roots.

Verify the replacement:

```bash
epochdeck doctor --server-url http://127.0.0.1:8787
curl --fail http://127.0.0.1:8787/api/v1/health
```

Log a sample run and inspect metrics and media. To roll back, stop the new build
and restore the previous binary with its untouched roots.

Dashboard branding is host configuration and is not included in physical
backups. Back up the environment file and custom logo separately.

## Diagnostics

Run:

```bash
epochdeck doctor --server-url http://127.0.0.1:8787
journalctl -u epochdeck --since today
```

`epochdeck doctor` reports a bounded snapshot of:

- uptime and request counts;
- rejected, slow, and failed requests;
- history-query timings;
- worker and stream capacity;
- storage capacity and device identity; and
- the 64 most recent slow requests.

Set `EPOCHDECK_SLOW_REQUEST_MS` to a value from 1 to 60,000 milliseconds to
change the slow-request threshold. Counters reset when the server restarts.

The server emits structured request logs through `tower-http`. systemd services
retain them in journald.
