# ADR 0005: Content-addressed rich values

- Status: Accepted
- Date: 2026-08-28

## Context

Images, audio, video, and tables may be much larger than scalar batches. Storing
their bytes in SQLite or Parquet would couple dashboard playback to metric
queries, prevent SSD/HDD separation, and make response memory depend on file
size. Offline logging and lost HTTP responses still require the same durability
and idempotency guarantees as scalar metrics.

## Decision

Rich content uses a SHA-256 content-addressed store rooted at
`RUNLOOM_BLOBS_DIR`. The SDK hashes and atomically installs content in its local
run spool before journaling a rich manifest. A fair background worker streams
the blob first and then creates the manifest. Retries reuse the digest and
UUIDv7 manifest identity.

The server streams request chunks into a staging file while hashing, syncs the
file, verifies the path digest, and installs it with an atomic no-replace hard
link. Existing digests are reused. Blob responses use the HTTP file service so
range requests, conditional delivery, and streaming do not require application
buffers.

SQLite stores only bounded rich manifests and dashboard previews. Tables are
streamed as standalone JSON blobs with a bounded row preview in the manifest.
Histograms store at most 512 exact bins inline. Media metadata is lazy and
native browser elements retrieve bytes directly from the CAS.

## Consequences

Blob capacity can be placed on HDD/ZFS independently from SSD metrics and the
catalog. Duplicate checkpoints or media consume one physical blob. Scalar
queries never inspect rich bytes, and video seeks read only requested ranges.

The CAS intentionally does not encode MIME type in the object identity. A rich
manifest supplies presentation metadata, while equal bytes remain deduplicated
across names and media uses. Garbage collection requires a later coordinated
reachability pass and is not performed implicitly.

## Rejected alternatives

### Store media in SQLite

Large BLOB pages would contend with run navigation and make ordinary catalog
backup and recovery proportional to media volume.

### Store media beside each run

Per-run copies make repeated checkpoints expensive and complicate artifact
lineage. Content addressing gives one immutable primitive for both rich values
and artifacts.

### Buffer uploads before hashing

File size is not a safe memory budget. Incremental hashing and file streaming
keep memory bounded independently of content length.
