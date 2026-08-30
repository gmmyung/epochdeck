# ADR 0010: Streaming import and export

- Status: Accepted
- Date: 2026-08-28

## Context

Large experiment projects cannot be migrated through one in-memory document.
W&B imports also need to survive process termination and ambiguous network
failures without duplicating accepted history. A portable EpochDeck export must
retain raw data rather than dashboard samples.

## Decision

The W&B adapter lives at the Python boundary and remains an optional dynamic
dependency. It derives a stable UUID from each source run path, scans history in
source order, and creates deterministic batches of at most 512 scalar points.
Checkpoint progress advances only after an idempotent server acknowledgement.
If a response is lost after commit, the same source rows produce the same batch
sequence, timestamps, steps, metrics, and digest on retry.

An fsynced JSON checkpoint records at most 100,000 runs and has a 64 MiB hard
limit. The importer takes an exclusive process lock for that checkpoint and
fails explicitly if an unfiltered source contains more runs; it never silently
truncates discovery. Up to sixteen workers may import independent runs; the
default is four. Cooperative cancellation stops admitting new work, bounds the
number of source calls that may finish, and leaves acknowledged watermarks
resumable.

Within each run, scalar rows retain their own contiguous checkpoint watermark
while a bounded transfer window fetches and uploads supported image, audio, and
video history. Deterministic rich IDs include the source row and occurrence, so
repeated equal media at one step cannot collide. Run files and logged artifacts
are streamed one file at a time through bounded temporary storage, hashed with
bounded buffers, and committed as deterministic artifact requests. Logged
artifacts preserve a canonical W&B `vN` as EpochDeck's explicit integer version;
run-file shards keep ordinary automatic allocation. The source run's update
token is checked again before completion; a moving source is retried instead of
publishing an incoherent checkpoint.

One source history row may contain at most 4,096 scalar metrics, 256 media
references, 65,536 traversed values, and 64 nested levels. Wide scalar rows are
split lexicographically into at most sixteen 256-key EpochDeck points with the
same step and timestamp. The source-row checkpoint advances only after every
point is acknowledged, so interruption replays the same deterministic requests.
Each retained media reference is capped at 64 KiB. Source keys outside the
EpochDeck key contract fail the run with the exact key rather than being renamed
or dropped.

The EpochDeck exporter follows every cursor and writes full-resolution history
pages with at most 32 columns and 5,000 rows. CAS content streams in 1 MiB
chunks, is deduplicated by destination path, and is verified before install.
Lightweight sweep and trial pages are hydrated one record at a time so the
portable files retain parameter definitions, early-termination settings, and
trial configurations. The exporter reads the project's opaque catalog-backed
mutation token before and after traversal; any intervening mutation, including a
create-delete ABA, prevents publication. Every completed file and directory is
fsynced before the final bundle is exposed by one atomic directory rename, then
the destination parent is fsynced.

## Consequences

Runtime memory is independent of total run length, file size, and project
history. Re-running an interrupted W&B import is safe without a remote job
queue. Export artifacts are implementation-neutral and directly inspectable.

The importer preserves W&B run files and logged output artifacts in EpochDeck's
CAS and reconstructs supported media-history references as native rich rows.
Registry-wide collections and input-artifact lineage remain a separate adapter
surface and do not change scalar storage or the export format.

## Rejected alternatives

### Load a W&B project into a dataframe

Memory would scale with total rows and metric width, precisely the failure mode
EpochDeck is intended to avoid.

### Checkpoint only after each run

A long run would restart from zero, and an ambiguous final batch response could
duplicate history.

### Export dashboard samples

Samples are a response budget, not source data. They cannot form a lossless
archive.
