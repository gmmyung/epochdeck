# System architecture

## Product boundary

EpochDeck is a complete experiment-tracking system: Python library, ingestion
server, query engine, rich-data store, CLI, and dashboard. Its public behavior
targets W&B compatibility. Trackio parity is the first finite milestone.

The implementation has no dependency on third-party hosting platforms or their
SDKs. Compatibility adapters translate at the EpochDeck boundary and do not shape
the internal storage model.

## Goals

EpochDeck targets one self-hosted machine with independently configurable data,
metric, and blob filesystem roots. It must:

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
manifests, and HTTP delivery. Tokio and Axum provide the runtime and HTTP
boundary. CPU-heavy encoding and query work runs in bounded worker pools rather
than on async executor threads. Native application authentication and multi-user
authorization are not implemented. The EpochDeck HTTP server binds to loopback;
an authenticated HTTPS reverse proxy terminates TLS and enforces access control
at the external boundary. The server port must never be exposed directly.

The `embedded-dashboard` production feature packages the built dashboard into
the server binary. Single-node operation requires no external database, queue,
object store, or runtime web directory.

### Catalog and client journal

SQLite is the transactional control plane. It stores projects, runs, metric
schemas, segment manifests, revisions, artifact manifests, lineage, and
idempotent ingest-batch records.

Dashboard list paths read small summary rows rather than complete documents.
The catalog maintains project run counts and per-run rich-key counts/latest
references in the same transactions as their source rows, avoiding list-time
scans of every run or rich value. Newest-first cursors resolve an ID to its
event-time/ID tuple, so deterministic imported UUIDs do not have to encode
chronology. See [ADR 0015](adr/0015-bounded-dashboard-discovery.md).
Every project-visible logical mutation also advances a catalog-backed monotonic
project token. Project summaries expose the token as an opaque decimal string,
allowing a streaming export to detect even a transient create-delete ABA across
its traversal. Physical metric compaction does not advance the logical token.

Explicit user summary data and the derived latest-metric preview are stored
separately. The explicit document has its own 256 KiB budget. The preview keeps
the lexicographically smallest 256 non-system metric keys and sets a sticky,
observable truncation flag if more keys exist; complete values remain in
Parquet and the metric-key catalog. Full run reads merge the preview with the
explicit layer taking precedence and also expose both source layers. A distinct
`document_revision` makes same-second config, explicit-summary, and finish
mutations observable without relying on SQLite timestamp precision.

SQLite does not store complete metric histories as JSON. Before an HTTP request,
the Python SDK writes each outgoing batch to its bounded, crash-recoverable local
journal. An acknowledgement advances that journal only after the server confirms
the exact request identity, so ambiguous requests can be replayed after a process
or machine restart.

Write-intent catalog transactions acquire SQLite's writer reservation before
their first read. This avoids deferred-transaction snapshot upgrades failing
when compaction commits concurrently, while WAL readers remain concurrent. The
writer wait is bounded to five seconds; exhaustion is exposed as retryable HTTP
503 `server_busy` with `Retry-After: 1` rather than an internal error.

### Metric storage

Submitted metric batches are grouped by project, run, and metric signature,
converted to Arrow record batches, and flushed as immutable Parquet segments. A
catalog transaction makes each segment visible atomically.

The Python journal is append-only and fsynced before `log` returns. Its byte
offset advances atomically only after an idempotent server acknowledgement. The
server writes each batch to an owned temporary Parquet file under
`EPOCHDECK_METRICS_DIR/staging`, syncs it, installs it with an atomic no-replace
operation, and then commits its SQLite manifest. The staging directory is on
the same filesystem as final segments, so hard-link installation remains
atomic. Startup proves that both the metric and blob roots support this
same-filesystem, no-replace hard-link operation and fails explicitly when the
filesystem cannot provide it. RAII removes temporary files on ordinary errors
and cancellation; the exclusive server startup path removes crash leftovers
before accepting work. A client retry after a crash therefore either installs
the missing batch or receives the same duplicate acknowledgement.

Published files are flushed after installation. Unix hosts then fsync the
parent directory. Windows reopens the file with write access before
`FlushFileBuffers`; because Windows has no portable directory-fsync equivalent,
the parent is validated as a directory and the flushed file and its metadata
form that platform's publication boundary.

Parquet provides typed values, compression, column projection, row-group
statistics, immutable snapshot-friendly files, and one metric name per column
rather than per row. One background worker compacts cohorts of at least four
adjacent, schema-compatible segments whose largest and smallest row counts are
within a factor of two. The default pass merges at most 16 files and 16,384
rows; hard limits prevent configuration from making a pass unbounded. Cohorting
avoids rewriting a growing active prefix for every small incoming batch.

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

Project comparison queries group requested columns by run and freeze one
sequence watermark per run under the metric snapshot barrier. Absolute step,
per-run relative step, and per-run elapsed milliseconds map onto one shared
bucket lattice; sparse series never acquire interpolated points. A bounded
2,048-entry/2-MiB per-key LRU reuses exact axis extents at an unchanged
first/last sequence watermark, including cached missing metrics, so natural
range replays can resolve the shared lattice without another storage scan. A
separate bounded per-series LRU reuses exact aggregates when a run's watermark,
alignment origin, viewport, and lattice are unchanged. Its 512-entry,
250,000-cell, and 32-MiB limits are independent eviction bounds, not history
retention limits.

The dashboard discovers union or intersection metric keys through a bounded
project-scoped catalog query for at most 32 selected runs. It then derives
deterministic chart request groups from the selected runs and requested keys.
Full-range and viewport refreshes reuse the same
group, so alignment origins and bucket lattices do not depend on scroll timing.
Only 24 Canvas charts are instantiated per page, at most four chart requests
are physically in flight, and offscreen histories leave component state. A
separate browser LRU is independently capped at 12 responses, 40,000 occupied
cells, and 4 MiB of estimated column payload. Navigation pages and run-resource
summaries retain explicit head/tail windows with pinned selections, while full
run, artifact, rich-value, and trace detail caches have smaller independent
caps. Truncation is shown in the UI instead of silently presenting a retained
window as a complete collection.
See [ADR 0013](adr/0013-multi-run-chart-comparison.md).

### Media, artifacts, and traces

Media and artifact bytes live in a content-addressed store rooted separately
from metrics. The catalog contains small manifests, aliases, versions, and
lineage edges. Uploads are streamed, hashed, deduplicated, and atomically
installed. At most eight new blob uploads stream concurrently. Artifact
manifest verification and ZIP delivery share a four-permit blocking-I/O pool,
so a parallel importer cannot create an unbounded filesystem work queue.

Video supports HTTP range delivery. Structured trace names, timing, status,
attributes, relationships, and bounded message previews are indexed in SQLite.
Complete trace inputs, outputs, and messages use the same content-addressed blob
store as media and artifacts. None of these paths participate in scalar metric
queries.

### Python SDK

The Python package provides native EpochDeck types, a W&B-compatible public API,
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
lazily, adapts history windows to sparse step domains, admits a bounded number
of run workers, and sends deterministic metric batches. One process exclusively
owns the fsynced checkpoint, which advances only after acknowledgement, allowing
an ambiguous accepted request to replay under the server's existing idempotency
contract. Source run files are chunked into ordinary CAS-backed artifacts,
logged artifacts preserve their canonical W&B `vN` in exact EpochDeck version
slots, and logged artifacts and history media use fixed transfer windows. Each
temporary file is uploaded and unlinked before the next is retained. Supported
media references become deterministic native rich values backed by the same
CAS. Cooperative cancellation stops queued work; a W&B SDK call already in
progress is the only non-preemptible unit.

Portable EpochDeck export traverses only public bounded APIs. It writes raw
full-resolution history pages, metadata JSON Lines, lineage links, and verified
referenced CAS bytes to a temporary directory, then atomically publishes a
portable current-format bundle. The exporter never requests dashboard samples
or buffers complete histories and files.

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
remain lazy, aggregated, cancellable, and capped at four concurrent requests,
so a large report cannot turn into an unbounded fan-out or duplicate source
data.

### Dashboard

The Svelte dashboard initially loads the server's immutable branding contract
alongside project, run, report, and metric metadata. Branding is validated and
loaded once at server startup; its optional logo is served only through a
same-origin bounded image endpoint.
It requests values only for visible charts and selected metrics. Off-screen
charts use browser content visibility and intersection observers; bounded
columnar JSON responses render directly with Canvas.

Run data is separated into summary, configuration, metrics, media, traces, and
artifact tabs. Config and summary documents render as locally searchable,
expandable trees. Metric keys are searchable, and each chart keeps interaction
state locally for pan/zoom/selection, manual linear or logarithmic domains,
smoothing, hover inspection, and line or exact min/max band display. The server
re-aggregates the settled step viewport after pan, wheel zoom, or region zoom.
Exact Parquet step statistics prune disjoint row groups; uncertain statistics
fall back to reading so the aggregate remains lossless. Smoothing applies only
to the bucket-last center line. Media snapshots are grouped by type and key into
step timelines.
Artifact manifests render as a tabbed file browser; whole ZIP responses are
produced with fixed buffers on a bounded download worker, and the worker permit
is retained through terminal stream-error delivery.

Live charts compare per-run metric revisions and request a fresh bounded
aggregate when a revision changes. `document_revision` invalidates selected
config and summary documents after a real document or finish mutation, and a
separate unified `rich_data_revision` invalidates already-open media, alert,
artifact, and trace resources after any real non-scalar mutation; idempotent
retries do not increment either counter. The dashboard polls all selected
running IDs through one bounded lightweight-summary query; it hydrates the
primary run's complete config/summary document only when that document is
visible. It does not append raw deltas to existing buckets because later
sequence values can
update an older step bucket. It can select multiple project runs and render
every requested metric on the same server-defined lattice. It never requests
complete histories for chart rendering. See
[ADR 0012](adr/0012-exact-bucket-chart-history.md),
[ADR 0013](adr/0013-multi-run-chart-comparison.md), and
[ADR 0015](adr/0015-bounded-dashboard-discovery.md).

## Storage layout

```text
EPOCHDECK_DATA_DIR/
  catalog.sqlite3
  epochdeck.lock

EPOCHDECK_METRICS_DIR/
  epochdeck.lock
  staging/
  projects/<project-id>/runs/<run-id>/segments/*.parquet

EPOCHDECK_BLOBS_DIR/
  epochdeck.lock
  sha256/<prefix>/<digest>
  staging/
```

The data, metric, and blob roots are independently configurable and may share a
parent or use separate filesystems, subject to the path constraints below.
The metric and blob filesystems must support hard links within each root; they
do not need to support links between roots.

The SQLite catalog has one current disposable pre-alpha definition. It carries
no internal generation marker or upgrade logic; a storage-definition change is
deployed by archiving the complete old root set and starting with empty roots.

After creating the directories, startup resolves symlinks and validates their
canonical paths. It also creates, links, flushes, and removes a bounded probe in
each immutable-file root before accepting requests. Metric and blob roots must
be disjoint: neither may equal or
contain the other. The data root may be a strict ancestor of either root (the
default layout), but it may not equal or live inside the metric or blob root.
This prevents staging, segment, CAS, lock, and catalog namespaces from mixing.
The server resolves the three canonical roots, sorts and deduplicates their
`epochdeck.lock` paths, and holds every advisory lock for its complete lifetime.
Only after acquiring the whole set may it clear metric or blob staging. Thus a
second instance with a different data root cannot mutate or clean a shared
external metric/blob root. Physical backup and restore acquire the identical
lock set and omit root lock files and staging trees from their payloads, so they
cannot race catalog or immutable-file mutation. Portable project exports remain
available through the HTTP API while the server is running.

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
