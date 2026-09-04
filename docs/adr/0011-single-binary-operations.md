# ADR 0011: Single-binary production operations

- Status: Accepted; deployment boundary superseded by ADR 0016
- Date: 2026-08-28

## Context

The original target installation was one Tailnet-only machine with independently
configured storage roots. Deployment must not reintroduce a separate dashboard
process, external database, or an unsafe backup race. ADR 0016 replaces the
Tailnet-specific TLS and access-control decision with a standard reverse-proxy
boundary.

## Decision

The production Cargo feature embeds the Vite output into `epochdeck-server`.
The original deployment used Tailscale Serve to terminate HTTPS and proxy to a
loopback-only listener. A hardened systemd unit owns the process and configured
storage roots.

The server canonicalizes the configured data, metric, and blob roots, sorts and
deduplicates them, then holds an advisory `<canonical-root>/epochdeck.lock` for
every distinct mutable root for its lifetime. Physical backup and restore
require the same complete lock set, making stopped-server coordination
observable rather than conventional. This also prevents two processes with
different data roots from mutating or cleaning the same external metric or blob
root. Backup uses SQLite's backup API and a streaming SHA-256 inventory across
committed metrics and the complete blob CAS. Restore requires the current
backup-container shape, streams each no-follow regular source exactly once into
exclusively created staging while verifying the inventory, refuses unsafe root
layouts and non-empty targets, installs data-plane trees, and publishes the
catalog last. Backup and verified restore staging sync every directory
bottom-up with bounded, no-follow traversal before top-level renames; the
destination parent or storage root is then synced after each publication.

The catalog has one current disposable pre-alpha definition, with no internal
generation marker or upgrade logic. A storage-definition change is deployed by
archiving the old roots and starting the replacement build with empty roots.
Physical backup manifests describe container integrity only, not catalog or
build compatibility.

Request telemetry is in-memory and bounded. Counters cover active, failed,
history, and slow requests, while a ring retains only the most recent 64 slow
records. The diagnostics endpoint also exposes worker permit availability plus
capacity and string device identity for every configured storage root.

API admission is non-queuing and occurs before request-body parsing. General API
traffic has 64 permits, health checks have an independent two-permit pool, and
overload returns HTTP 503 with `Retry-After: 1`. A permit remains attached to
the response body until EOF or disconnect, bounding parsed requests and slow
response consumers as well as handler futures. Immutable blob and artifact-file
downloads additionally use a 16-stream pool so media clients cannot occupy all
general API capacity. Embedded static dashboard assets bypass API admission.

## Consequences

One executable serves both API and dashboard. The current TLS and access-control
boundary is defined by ADR 0016. Backups cannot unknowingly race a live server,
and an independently configured storage root cannot be shared by two live servers.
Multi-terabyte installations can minimize downtime by stopping only long enough
to take coordinated filesystem snapshots, then copying snapshots asynchronously.

The disposable catalog definition remains free to change directly. Operational
telemetry cannot grow storage or become another backup concern, but counters
reset at process restart. Admission bounds trade queueing latency for explicit,
retryable overload instead of allowing parsed bodies, lock waiters, or slow
downloads to accumulate without a limit.

## Rejected alternatives

### Run Vite or nginx in production

It adds a second asset-version lifecycle without improving the single-node
design. A reverse proxy remains necessary for the TLS and authentication
boundary described by ADR 0016, but it does not serve EpochDeck's static assets.

### Copy live storage roots directly

Catalog manifests can change while segments are compacted, producing a backup
whose SQLite state and files never existed together.
