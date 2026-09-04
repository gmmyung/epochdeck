# Changelog

All notable changes to EpochDeck are recorded here. EpochDeck remains pre-alpha,
so storage and APIs may change without migration or compatibility shims.

## Unreleased

### Added

- Python clients can authenticate to an HTTPS reverse proxy with paired
  `EPOCHDECK_HTTP_USERNAME` and `EPOCHDECK_HTTP_PASSWORD` environment variables;
  credentials are rejected in server URLs and never enter the durable spool.
- GitHub prereleases build and smoke-test server archives natively for Linux
  x86_64/ARM64, macOS Intel/Apple Silicon, and Windows x86_64.

### Changed

- The supported remote-hosting topology now uses a standard authenticated HTTPS
  reverse proxy in front of the loopback server, with no Tailscale dependency.
- Deployment defaults keep all storage roots under `/var/lib/epochdeck` while
  preserving independent root configuration for operators that need it.
- Release builds use ordinary Cargo on matching native runners; Zig,
  cargo-zigbuild, emulation, and cross-compilation are no longer in the release
  toolchain.

## [0.1.0-alpha.1] - 2026-08-31

### Added

- Rust server with SQLite catalog, Arrow/Parquet histories, and split CAS storage.
- W&B-oriented Python SDK with durable online and offline delivery.
- Native rich media, artifacts, traces, sweeps, reports, alerts, and host telemetry.
- Lazy multi-run dashboard with bounded Canvas charts and searchable resources.
- Tailnet-only deployment, diagnostics, physical backup/restore, and project export.
- Bounded W&B importer for metrics, metadata, media, run files, and logged artifacts.

### Changed

- Adopted the EpochDeck identity across every package, executable, protocol,
  storage path, dashboard surface, and deployment artifact.
- Licensed EpochDeck under Apache-2.0 and embedded the license in every release
  distribution.
- Release versions now identify the software explicitly as an alpha prerelease.
- Internal Rust crates and the dashboard package are marked non-publishable.

### Fixed

- Clean builds now create the embedded dashboard before all-feature Rust checks.
- W&B import refuses truncated histories, mutable listing retries, duplicate runs,
  missing artifact APIs, and unbounded empty history scans.

### Breaking / pre-alpha storage

- There is no storage migration or backward-compatibility promise. Use empty roots
  for a new build and retain old roots with their exact creating binary.
