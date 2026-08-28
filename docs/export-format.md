# Runloom export format

A `runloom export` bundle is a portable directory for one project. Format
version 1 contains only JSON, JSON Lines, and original content-addressed bytes.
No file requires SQLite, Parquet, or a running Runloom server to inspect.

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

The top-level manifest is written last and records the format version, project,
creation time, and resource counts. Export takes place in a sibling temporary
directory. Every blob is size-checked and SHA-256 verified before the directory
is atomically renamed to the requested destination. An existing destination is
never merged or overwritten.

Only referenced CAS content belongs to the portable project: rich values, trace
payloads, and artifact entries. Orphaned upload objects have no project
ownership and are not exported. Physical disaster-recovery snapshots of the
server's complete storage roots are a separate production-operations feature.
