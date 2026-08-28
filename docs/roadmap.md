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

- [x] W&B-style `init`, `log`, and `finish` lifecycle for scalar runs.
- [x] Online, offline, disabled, and explicit resume modes.
- [x] Idempotent batch-ingest protocol and durable Python spool.
- [x] Arrow conversion and immutable wide Parquet segments.
- [x] Initial config, summary, nested scalar metrics, and explicit steps.
- [x] Metric-key discovery and bounded columnar history APIs.
- [x] Server-side, spike-preserving min/max downsampling.
- [x] Workload-shaped storage benchmark harness.
- [x] Config mutation and non-numeric summary semantics.
- [x] Background compaction.
- [x] Full W&B lifecycle contract suite for the supported scalar surface.

## Milestone 2: dashboard and monitoring

- [x] Project and run navigation.
- [x] Lazy, virtualized Canvas metric charts.
- [x] Server-side min/max downsampling.
- [x] Per-run revision caches and realtime deltas.
- [x] System metrics and alerts.

## Milestone 3: rich data and artifacts

- [x] Images, audio, video, tables, and histograms.
- [x] Content-addressed blobs and range delivery.
- [x] Artifact versions, aliases, downloads, and lineage.
- [x] Structured traces, messages, payloads, and full-text search.

## Milestone 4: APIs and automation

- [x] Public query API and filtering.
- [x] Sweeps, agents, scheduling, and early termination.
- Persisted reports and dashboard layouts.
- Resumable W&B importer and lossless Runloom export.

## Milestone 5: production operations

- Single-binary build with embedded dashboard.
- Tailnet-only deployment and systemd documentation.
- Coordinated catalog, segments, and blob backup/restore.
- Slow-query diagnostics, telemetry, and upgrade migrations.
