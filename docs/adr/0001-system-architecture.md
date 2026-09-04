# ADR 0001: Independent single-node columnar architecture

- Status: Accepted; deployment boundary superseded by ADR 0016
- Date: 2026-08-28

## Context

EpochDeck needs W&B-compatible behavior without inheriting a row-oriented storage
model or third-party hosting integrations. The original target deployment was a
trusted, Tailnet-only server with independently configured local storage roots.
A distributed control plane would add operational cost without solving the
primary query problem. ADR 0016 replaces the Tailnet-specific access boundary.

## Decision

EpochDeck will use:

- Rust, Tokio, and Axum for the server;
- Apache Arrow as the in-process columnar representation;
- Parquet as the immutable metric-segment format;
- SQLite as the transactional catalog and ingest journal index;
- a content-addressed filesystem store for media and artifacts;
- Svelte 5 and TypeScript for the standalone dashboard;
- a W&B-compatible Python SDK managed by uv; and
- Nix for reproducible development and builds.

Metric data and control metadata are separate storage planes. Dashboard queries
must project explicit metric columns and apply display-resolution budgets before
serialization.

The runtime dependency graph must not include Gradio, Hugging Face Hub,
Datasets, Spaces, Buckets, or related clients. EpochDeck owns its server,
authentication, storage, protocol, and dashboard.

## Consequences

The system can retain raw data while bounding dashboard work. Immutable
segments make backup and recovery straightforward, and independent roots let an
operator choose storage appropriate to each workload.

Parquet is not appendable, so EpochDeck needs a journal, atomic segment manifests,
and background compaction. Dynamic metric schemas require signature-aware
segments and schema reconciliation at query time. These are deliberate costs.

## Rejected alternatives

### Metric JSON in SQLite

Streaming would bound memory but would still read and parse irrelevant metric
values. A clean implementation should not preserve that limitation.

### Required PostgreSQL

PostgreSQL would improve concurrency but would add another service and would not
automatically provide efficient wide time-series projection.

### ClickHouse

ClickHouse is capable but operationally disproportionate for the initial
single-user deployment.

### Python server

Python remains appropriate for client compatibility and importers. The
long-lived server benefits from Rust's predictable memory, streaming I/O,
cancellation, and direct Arrow integration.
