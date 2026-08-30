# EpochDeck export format

An `epochdeck export` bundle is a portable directory for one project. It
contains only JSON, JSON Lines, and original content-addressed bytes. No file
requires SQLite, Parquet, or a running EpochDeck server to inspect.

Portable export requires every selected run to be finished and all project
writers to be quiesced. A running run is rejected. The exporter captures the
opaque project mutation token before traversal and verifies it afterward; any
project-visible change, including a transient create-delete, aborts without
publishing a bundle.

```text
manifest.json
reports.jsonl
sweeps.jsonl
sweep-trials.jsonl
artifacts.jsonl
runs/<run-id>/
  run.json
  metrics/0000.jsonl
  alerts.jsonl
  rich-values.jsonl
  traces.jsonl
  artifact-links.jsonl
blobs/sha256/<prefix>/<sha256>
```

Metric files each contain at most 32 columns. Every JSON line is one bounded
full-resolution history response with sequence, step, timestamp, and aligned
metric arrays. Following the pages in file order reconstructs every stored
point without dashboard sampling.

`sweeps.jsonl` contains complete sweep definitions, and each
`sweep-trials.jsonl` record contains its sweep ID and complete trial record.
The exporter hydrates these from their lightweight list summaries without
holding the project-wide collections in memory.

The top-level manifest is written last and records the current EpochDeck export
format, project, creation time, and resource counts. Export takes place in a
sibling temporary directory. Every blob is size-checked and SHA-256 verified
before the directory is atomically renamed to the requested destination. An
existing destination is never merged or overwritten. All bundle files are
fsynced before rename and the destination parent is fsynced afterward. Export
directories are mode `0700` and files are mode `0600`; grant broader filesystem
access explicitly when a bundle is meant to be shared.

Only referenced CAS content belongs to the portable project: rich values, trace
payloads, and artifact entries. Orphaned upload objects have no project
ownership and are not exported. Physical disaster-recovery snapshots of the
server's complete storage roots are a separate production-operations feature.
