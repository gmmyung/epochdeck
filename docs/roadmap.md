# Roadmap

The feature checklist and compatibility definitions live in
[`compatibility.md`](compatibility.md). Milestones describe implementation order,
not a reduction in the final parity goal.

## Milestone 0: foundation

- Reproducible Nix development shell.
- Rust health API and SQLite catalog.
- Python health client and CLI.
- Standalone Svelte dashboard shell.
- Dependency guard against prohibited hosting integrations.
- CI for Rust, Python, and web validation.

## Milestone 1: run lifecycle and bounded metrics

- W&B-compatible `init`, `log`, and `finish` lifecycle.
- Online, offline, disabled, and resume modes.
- Idempotent batch-ingest protocol and durable spool.
- Arrow conversion and immutable Parquet segments.
- Config, summary, nested scalar metrics, and explicit steps.
- Metric schema and columnar series APIs.
- Workload-shaped performance benchmarks.

## Milestone 2: dashboard and monitoring

- Project and run navigation.
- Lazy, virtualized Canvas metric charts.
- Server-side min/max downsampling.
- Per-run revision caches and realtime deltas.
- System metrics and alerts.

## Milestone 3: rich data and artifacts

- Images, audio, video, tables, and histograms.
- Content-addressed blobs and range delivery.
- Artifact versions, aliases, downloads, and lineage.
- Structured traces and search.

## Milestone 4: APIs and automation

- Public query API and filtering.
- Sweeps, agents, scheduling, and early termination.
- Persisted reports and dashboard layouts.
- Resumable W&B importer and lossless Runloom export.

## Milestone 5: production operations

- Single-binary build with embedded dashboard.
- Tailnet-only deployment and systemd documentation.
- Coordinated catalog, segments, and blob backup/restore.
- Slow-query diagnostics, telemetry, and upgrade migrations.
