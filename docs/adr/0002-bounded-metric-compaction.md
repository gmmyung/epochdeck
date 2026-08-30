# ADR 0002: Bounded metric-segment compaction

- Status: Accepted
- Date: 2026-08-28

## Context

Each acknowledged metric batch becomes an immutable Parquet segment. This keeps
ingestion replay-safe, but long imports can create hundreds of small files per
run. Query memory remains bounded, yet repeated file opens and metadata reads
eventually dominate dashboard latency.

Compaction must not introduce a retention quota, load an entire run into memory,
or let a query observe catalog manifests whose files have already been removed.
It also needs a recoverable boundary between installing a compacted file,
swapping manifests, and deleting replaced files.

## Decision

EpochDeck runs one cancellable background compaction worker. A candidate contains
at least four adjacent segments with the same metric-schema signature, and its
largest input has at most twice as many rows as its smallest input. This
size-tiered rule lets a live run accumulate a cohort before rewriting it instead
of repeatedly merging a growing head segment with every newly arrived batch.
Each default pass:

- selects at most 16 adjacent segments with the same metric-schema signature;
- caps the replacement at 16,384 rows;
- streams 1,024-row Arrow batches into Parquet row groups capped at 8,192 rows;
- installs the replacement with an atomic rename and filesystem sync;
- atomically records old paths for retirement, replaces their manifests, and
  leaves the logical metric revision unchanged; and
- deletes retired files only while holding the same snapshot barrier used by
  history queries.

The merge runs outside the snapshot write barrier, so dashboards and ingestion
continue while the new immutable file is built. The short manifest swap and
unlink phase waits for existing history snapshots. New queries then capture the
replacement manifest.

Retired paths remain in SQLite until deletion succeeds. A restart therefore
retries cleanup after a crash between the catalog swap and file deletion.
Compacted filenames are deterministic for their source set, so a crash before
the swap can safely reuse the atomically installed output. Hard limits cap a
configured pass at 64 input segments and 65,536 rows.

## Consequences

Compaction reduces file-open overhead without changing history values, ordering,
pagination, sampling, summaries, or cache revisions. Raw samples remain
lossless; only their physical grouping changes.

For a steady stream of similarly sized batches, four-way size tiers bound the
number of rewrites per row logarithmically rather than rewriting the complete
active prefix after each batch. A partial cohort remains as immutable source
segments until enough comparable neighbors arrive.

Schema changes form compaction boundaries. This avoids constructing a very wide
union schema and keeps merge memory proportional to one known metric signature.
Runs with rapidly alternating schemas may retain more segments until a future
schema-reconciliation compactor is justified by measurements.

The snapshot barrier is process-local, matching EpochDeck's single-service
deployment contract. Multiple server processes must not share one writable
catalog and metric root.

## Rejected alternatives

### Delete old files immediately after the catalog transaction

A query can read the old manifest list before the transaction and open a file
after deletion. Durable retirement plus the snapshot barrier closes that race.

### Merge every schema into one wide file

Unioning all run metrics would increase memory and rewrite sparse columns that
queries may never request. Signature-compatible compaction has a predictable
bound and preserves column locality.

### Compact only finished runs

Large imports and long-running training jobs need file-count control before
finish. Optimistic candidate validation makes compaction safe while new tail
segments are appended.
