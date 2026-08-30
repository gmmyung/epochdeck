# ADR 0014: Owned metric segment installation

- Status: Accepted
- Date: 2026-08-29

## Context

Metric batch paths are deterministic so an exact retry resolves to the same
Parquet segment. Previously, two concurrent requests could both write that
path, race while registering the batch in SQLite, and let the failing request
unlink the file accepted by the successful request. The catalog would then
reference a missing segment. Compaction had a related ambiguity when a
deterministic output already existed after an interrupted pass.

The server process already holds the exclusive EpochDeck storage lock, but HTTP
requests inside that process still execute concurrently. Cleanup therefore
needs both mutation ordering and an explicit notion of which operation
installed a file.

## Decision

The HTTP server serializes ingestion and finish mutations through a bounded set
of locks selected by run ID. A lock covers duplicate detection, Parquet write,
catalog registration, and error cleanup. Hash collisions may serialize
unrelated runs, but the lock set remains explicitly bounded.

MetricStore creates owned temporary files under
`EPOCHDECK_METRICS_DIR/staging`, on the same filesystem as final segments, and
installs completed files with an atomic, no-clobber hard link. Every write
reports either `InstalledNew` or `AlreadyPresent`; it never replaces an existing
deterministic path. Temporary files are fully written and synced before
installation. Their RAII owner removes them on error or cancellation. Startup
clears crash leftovers while holding the exclusive storage lock, and physical
backup omits the staging tree.

An operation may remove a final segment only when all of the following hold:

1. that operation received `InstalledNew`;
2. the path is absent from active and retired catalog manifests; and
3. the caller still owns the applicable mutation protocol.

Exact retries that receive `AlreadyPresent` never delete the shared path.
Compaction applies the same ownership rule when cancellation or catalog
replacement fails. Uncertain ownership favors retaining an orphan for later
reconciliation over deleting referenced data.

## Consequences

Concurrent duplicate ingestion is idempotent: one request creates the batch and
the other observes the accepted registration. A failed request cannot remove a
segment owned by a successful request. Compaction cancellation cleans up only
new, unregistered output.

The hard-link installation requires temporary and final segment files to live
on the same filesystem, which the dedicated staging directory guarantees
without scattering hidden temporary files through final segment trees. Bounded
striped locking can reduce ingestion parallelism for colliding run IDs, but
avoids an unbounded per-run lock map.

## Rejected alternatives

### Increase only SQLite's busy timeout

A busy timeout reduces transient lock errors but does not establish filesystem
ownership. The losing request could still delete the winner's segment.

### Overwrite the deterministic path with rename

Replacing an existing path erases ownership information and permits concurrent
writers to clobber a file already referenced by the catalog.

### Always delete on registration failure

Catalog failure does not prove that the final path is unreferenced. Data
integrity takes precedence over eager orphan cleanup.
