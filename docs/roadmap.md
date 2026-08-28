# Roadmap

The feature checklist and compatibility definitions live in
[`compatibility.md`](compatibility.md). Milestones describe implementation order,
not a reduction in the final parity goal.

## Milestone 0: foundation

- [x] Reproducible Nix development shell.
- [x] Rust health API and SQLite catalog.
- [x] Python health client and CLI.
- [x] Standalone Svelte dashboard shell.
- [x] Dependency guard against prohibited hosting integrations.
- [x] CI for Rust, Python, and web validation.

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
- [x] Per-run revision caches and bounded aggregate refreshes.
- [x] System metrics and alerts.
- [x] Full-width tabbed run workspace and expandable config/summary trees.
- [x] Searchable charts with hover inspection, pan, zoom, region selection,
      viewport re-sampling, configurable axes, smoothing, and exact line/band
      display.

## Milestone 3: rich data and artifacts

- [x] Images, audio, video, tables, and histograms.
- [x] Content-addressed blobs and range delivery.
- [x] Artifact versions, aliases, downloads, and lineage.
- [x] Structured traces, messages, payloads, and full-text search.
- [x] Key-grouped media timelines with step navigation and native playback.
- [x] Artifact tab/file browser and bounded whole-artifact ZIP streaming.

## Milestone 4: APIs and automation

- [x] Public query API and filtering.
- [x] Sweeps, agents, scheduling, and early termination.
- [x] Persisted reports and dashboard layouts.
- [x] Resumable W&B importer and lossless Runloom export.

## Milestone 5: production operations

- [x] Single-binary build with embedded dashboard.
- [x] Tailnet-only deployment and systemd documentation.
- [x] Coordinated catalog, segments, and blob backup/restore.
- [x] Slow-query diagnostics, bounded telemetry, and explicit pre-alpha schema checks.
