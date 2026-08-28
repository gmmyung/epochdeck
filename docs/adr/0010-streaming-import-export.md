# ADR 0010: Streaming import and export

- Status: Accepted
- Date: 2026-08-28

## Context

Large experiment projects cannot be migrated through one in-memory document.
W&B imports also need to survive process termination and ambiguous network
failures without duplicating accepted history. A portable Runloom export must
retain raw data rather than dashboard samples.

## Decision

The W&B adapter lives at the Python boundary and remains an optional dynamic
dependency. It derives a stable UUID from each source run path, scans history in
source order, and creates deterministic batches of at most 512 scalar points.
Checkpoint progress advances only after an idempotent server acknowledgement.
If a response is lost after commit, the same source rows produce the same batch
sequence, timestamps, steps, metrics, and digest on retry.

An fsynced JSON checkpoint records at most 100,000 runs and has a 64 MiB hard
limit. Up to sixteen workers may import independent runs; the default is four.
Run files are downloaded and uploaded in chunks of 64 entries, hashed while
bounded buffers are used, and committed as deterministic artifact requests.

The Runloom exporter follows every cursor and writes full-resolution history
pages with at most 32 columns and 5,000 rows. CAS content streams in 1 MiB
chunks, is deduplicated by destination path, and is verified before install.
The final bundle is exposed by one atomic directory rename.

## Consequences

Runtime memory is independent of total run length, file size, and project
history. Re-running an interrupted W&B import is safe without a remote job
queue. Export artifacts are implementation-neutral and directly inspectable.

The first importer preserves W&B run files as Runloom artifacts but does not yet
reconstruct W&B registry lineage or media-history references as native rich
rows. Those adapters can be added without changing scalar storage or the export
format.

## Rejected alternatives

### Load a W&B project into a dataframe

Memory would scale with total rows and metric width, precisely the failure mode
Runloom is intended to avoid.

### Checkpoint only after each run

A long run would restart from zero, and an ambiguous final batch response could
duplicate history.

### Export dashboard samples

Samples are a response budget, not source data. They cannot form a lossless
archive.
