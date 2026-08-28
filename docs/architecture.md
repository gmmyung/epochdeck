# System architecture

## Product boundary

Runloom is a complete experiment-tracking system: Python library, ingestion
server, query engine, rich-data store, CLI, and dashboard. Its public behavior
targets W&B compatibility. Trackio parity is the first finite milestone.

The implementation has no dependency on third-party hosting platforms or their
SDKs. Compatibility adapters translate at the Runloom boundary and do not shape
the internal storage model.

## Goals

Runloom targets a single self-hosted machine with a fast SSD and a large HDD or
ZFS pool. It must:

- retain complete histories without a storage quota;
- keep query memory bounded independently of run length;
- serve only metrics and resolution requested by the dashboard;
- remain responsive while imports and training uploads are active;
- recover safely after interruption;
- support rich media and versioned artifacts natively; and
- back up using ordinary filesystem and SQLite tooling.

## Components

### Rust service

The server owns ingestion, query scheduling, catalog transactions, storage
manifests, authentication, and HTTP delivery. Tokio and Axum provide the runtime
and HTTP boundary. CPU-heavy encoding and query work runs in bounded worker pools
rather than on async executor threads.

The `embedded-dashboard` production feature packages the built dashboard into
the server binary. Single-node operation requires no external database, queue,
object store, or runtime web directory.

### Catalog and ingest journal

SQLite is the transactional control plane. It stores projects, runs, metric
schemas, segment manifests, revisions, artifact manifests, lineage, and
idempotent ingest-batch records.

SQLite does not store complete metric histories as JSON. Incoming batches first
enter a bounded, crash-recoverable journal. Acknowledged batches can always be
replayed after a process or machine restart.

### Metric storage

Journal records are grouped by project, run, and metric signature, converted to
Arrow record batches, and flushed as immutable Parquet segments. A catalog
transaction makes each segment visible atomically.

The Python journal is append-only and fsynced before `log` returns. Its byte
offset advances atomically only after an idempotent server acknowledgement. The
server writes each batch to a temporary Parquet file, syncs it, installs it with
an atomic rename, and then commits its SQLite manifest. A client retry after a
crash therefore either installs the missing batch or receives the same duplicate
acknowledgement.

Parquet provides typed values, compression, column projection, row-group
statistics, immutable snapshot-friendly files, and one metric name per column
rather than per row. One background worker compacts adjacent, schema-compatible
segments in bounded streaming passes. The default pass merges at most 16 files
and 16,384 rows; hard limits prevent configuration from making a pass unbounded.

Compaction installs a new immutable file before atomically swapping catalog
manifests. History queries hold a shared snapshot guard from manifest lookup
through file reads, while only the short swap and retirement phase takes the
exclusive guard. Replaced paths remain in a durable SQLite retirement table
until filesystem deletion succeeds, so cleanup resumes safely after a crash.
Logical metric revisions do not change because compaction preserves every raw
sample. See [ADR 0002](adr/0002-bounded-metric-compaction.md).

### Query engine

The query layer operates on projected Arrow columns. Requests name runs, metric
columns, an x-axis, and display resolution. Downsampling occurs before response
serialization and preserves extrema within display buckets. The min/max sampler
walks segment-manifest pages against a fixed sequence extent and retains only
rows referenced by per-metric extrema. Its response memory is therefore bounded
by the requested point and column budgets rather than the raw run length.

Every query has cancellation, a memory budget, a maximum response-value budget,
a bounded worker permit, and cache keys scoped to per-run revisions. Response
budgets are not retention quotas; clients can request additional metrics or
pages without losing stored data.

### Media, artifacts, and traces

Media and artifact bytes live in a content-addressed store rooted separately
from metrics. The catalog contains small manifests, aliases, versions, and
lineage edges. Uploads are streamed, hashed, deduplicated, and atomically
installed.

Video supports HTTP range delivery. Structured trace names, timing, status,
attributes, relationships, and bounded message previews are indexed in SQLite.
Complete trace inputs, outputs, and messages use the same content-addressed blob
store as media and artifacts. None of these paths participate in scalar metric
queries.

### Python SDK

The Python package provides native Runloom types, a W&B-compatible public API,
offline and online modes, a CLI, and importers. Calls enqueue bounded batches,
and durable offline spooling prevents transient server failures from affecting
training. Config updates and summary values share the durable run metadata,
while the server applies their bounded JSON merges transactionally in SQLite.

Compatibility is explicit. The pre-alpha implementation targets one current
behavioral shape and does not carry data or SDK compatibility scaffolding;
unsupported semantics produce clear errors rather than inert arguments. The
HTTP namespace remains explicitly versioned under `/api/v1`.

### Import and export

The optional W&B importer is a Python boundary adapter. It scans source runs
lazily, admits a bounded number of run workers, and sends deterministic metric
batches. An fsynced checkpoint advances only after acknowledgement, allowing an
ambiguous accepted request to replay under the server's existing idempotency
contract. Source run files are chunked into ordinary CAS-backed artifacts.

Portable Runloom export traverses only public bounded APIs. It writes raw
full-resolution history pages, metadata JSON Lines, lineage links, and verified
referenced CAS bytes to a temporary directory, then atomically publishes a
format-versioned bundle. The exporter never requests dashboard samples or
buffers complete histories and files.

### Sweep scheduler

Sweep definitions, monotonic scheduler indexes, leased trials, and run bindings
live in SQLite. Grid scheduling uses mixed-radix selection and random scheduling
uses deterministic hashes, so neither path expands parameter combinations in
memory. A short transaction serializes claims. Metric batch acknowledgements
carry median-rule stop decisions back through the existing durable delivery
worker; no polling thread or scheduler queue enters training code.

### Reports

Reports are small project-scoped SQLite documents containing a bounded grid of
typed metric and Markdown panels. Metric panels hold run IDs and requested
column names, not materialized history. The catalog transaction validates that
every referenced run belongs to the report project.

The dashboard renders the grid directly from that definition. Metric histories
remain lazy, sampled, cancellable, and capped at four concurrent requests, so a
large report cannot turn into an unbounded fan-out or duplicate source data.

### Dashboard

The Svelte dashboard initially loads project, run, report, and metric metadata.
It requests values only for visible charts and selected metrics. Off-screen
charts use browser content visibility and intersection observers; bounded
columnar JSON responses render directly with Canvas.

Realtime updates are deltas keyed by run sequence. The dashboard never polls
complete histories.

## Storage layout

```text
RUNLOOM_DATA_DIR/
  catalog.sqlite3
  journal/

RUNLOOM_METRICS_DIR/
  projects/<project-id>/runs/<run-id>/segments/*.parquet

RUNLOOM_BLOBS_DIR/
  sha256/<prefix>/<digest>
  staging/
```

The data and metric roots belong on SSD. The blob root can be a bind-mounted ZFS
dataset on high-capacity disks.

The server holds `RUNLOOM_DATA_DIR/runloom.lock` while active. Physical backup
and restore take the same advisory lock, use a verified streaming inventory, and
therefore cannot race catalog or immutable-file mutation. Portable project
exports remain available through the HTTP API while the server is running.

## Performance budgets

Initial acceptance targets on a 2-core, 2 GiB container are:

- idle RSS below 100 MiB;
- routine query RSS below 500 MiB;
- project and run metadata responses below 100 ms;
- one warm metric chart below 500 ms;
- one-run history length not affecting response memory;
- dashboard and importer usable concurrently; and
- no queue or cache that can grow without a configured bound.

Benchmarks will reproduce the current workload shape: 200,000 rows in one run,
180 scalar metrics per row, multiple schemas, native videos, and simultaneous
ingestion and dashboard queries.
