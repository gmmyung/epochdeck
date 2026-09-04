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

The top-level manifest is written last. It records the format, project, creation
time, and resource counts.

Publication follows these rules:

- build in a sibling temporary directory;
- size-check and SHA-256 verify every blob;
- flush files before atomic rename;
- never merge into or overwrite an existing destination; and
- create POSIX directories as `0700` and files as `0600`.

Unix also fsyncs directories and the destination parent. Windows validates each
directory but cannot provide the same directory-entry power-loss guarantee.

Only referenced CAS content belongs to the portable project: rich values and
artifact entries. Orphaned upload objects have no project
ownership and are not exported. Physical disaster-recovery snapshots of the
server's complete storage roots are a separate production-operations feature.
