# ADR 0011: Single-binary production operations

- Status: Accepted
- Date: 2026-08-28

## Context

The target installation is one Tailnet-only machine with metadata and metrics
on SSD and large content on a separate ZFS pool. Deployment must not reintroduce
a web process, reverse proxy, external database, or an unsafe backup race.
Pre-alpha schema changes also need an explicit failure mode without creating a
migration compatibility layer.

## Decision

The production Cargo feature embeds the Vite output into `runloom-server`.
Tailscale Serve terminates Tailnet-only HTTPS and proxies to a loopback-only
listener. A hardened systemd unit owns the process and configured storage roots.

The server holds one advisory lock in the data root for its lifetime. Physical
backup and restore require the same lock, making stopped-server coordination
observable rather than conventional. Backup uses SQLite's backup API and a
streaming SHA-256 inventory across journals, metrics, and the complete blob CAS.
Restore verifies the bundle first and refuses non-empty targets.

Every catalog has an explicit schema version. The server refuses any different
version. Before alpha, operators restore the matching binary and backup or move
all roots aside and start clean; the codebase does not ship migrations, dual
reads, or backfills.

Request telemetry is in-memory and bounded. Counters cover active, failed,
history, and slow requests, while a ring retains only the most recent 64 slow
records. The diagnostics endpoint also exposes schema version and worker permit
availability.

## Consequences

One executable serves both API and dashboard, and Tailscale remains the only TLS
and access-control layer. Backups cannot unknowingly race a live server.
Multi-terabyte installations can minimize downtime by stopping only long enough
to take coordinated filesystem snapshots, then copying snapshots asynchronously.

Pre-alpha catalog changes are intentionally disruptive and explicit. Operational
telemetry cannot grow storage or become another backup concern, but counters
reset at process restart.

## Rejected alternatives

### Run Vite or nginx in production

It adds a second service and a second asset-version lifecycle without improving
the Tailnet-only single-node design.

### Copy live storage roots directly

Catalog manifests can change while segments are compacted, producing a backup
whose SQLite state and files never existed together.

### Add automatic pre-alpha migrations

Maintaining old schemas would constrain redesign before the storage contract is
ready to stabilize and conflicts with the documented pre-alpha policy.
