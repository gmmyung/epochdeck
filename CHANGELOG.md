# Changelog

All notable changes to EpochDeck are recorded here. EpochDeck remains pre-alpha,
so storage and APIs may change without migration or compatibility shims.

## Unreleased

### Added

- A minimal stacked-epoch logo now serves as the default dashboard mark and
  favicon, and the README presents the project with CI and release badges.
- Dashboard favicon configuration is independent from the logo, with safe
  server-side validation, content-versioned URLs, and Safari-compatible bundled
  ICO, PNG, Apple touch, and pinned-tab fallbacks.
- Metric catalog responses include the exact number of matching keys, allowing
  the dashboard to show total metric counts without eagerly loading histories.
- Python clients can authenticate to an HTTPS reverse proxy with paired
  `EPOCHDECK_HTTP_USERNAME` and `EPOCHDECK_HTTP_PASSWORD` environment variables;
  credentials are rejected in server URLs and never enter the durable spool.
- GitHub prereleases build and smoke-test server archives natively for Linux
  x86_64/ARM64, Apple Silicon macOS, and Windows x86_64.

### Changed

- The dashboard identity and server health now live in the navigation sidebar;
  light and dark themes use quieter chart dividers, transparent plot and legend
  surfaces, omit empty report navigation, and use more consistent sidebar and
  content alignment.
- Metric charts now render with uPlot; dashboard selects and enlarged-chart
  dialogs use Bits UI accessibility primitives, dashboard icons use Lucide, and
  the Python SDK derives its default spool root from the operating system's
  application-data convention.
- Metric chart action controls now reveal on chart hover or keyboard focus,
  remain present for touch input, and stay visible while settings or a modal are
  open.
- HTTP request logging now uses the configured `tower-http` tracing layer;
  custom middleware retains only bounded diagnostic counters and slow-request
  records.
- Removed abandoned dashboard request adapters and test-only server constructors
  from production builds, and made the Python active-run registry the single
  source for module-level `run`, `config`, and `summary` access.
- The supported remote-hosting topology now uses a standard authenticated HTTPS
  reverse proxy in front of the loopback server, with no Tailscale dependency.
- Deployment defaults keep all storage roots under `/var/lib/epochdeck` while
  preserving independent root configuration for operators that need it.
- Release builds use ordinary Cargo on matching native runners; Zig,
  cargo-zigbuild, emulation, and cross-compilation are no longer in the release
  toolchain.

### Fixed

- Mobile run cards no longer overlap inside the horizontally scrolling run
  selector, and its collapsed state stays in a compact strip above the run.
- W&B imports now recover from an initial authoritative-refresh failure, verify
  retried iterator identity in constant memory, reject shortened resumed
  listings, handle overflowing source numbers deterministically, recognize
  preempted runs, and validate exact provenance during lost-finish recovery.
- Metric chart wheel and keyboard zoom now change only the horizontal domain,
  leaving the vertical scale stable instead of producing a two-stage axis jump;
  high-frequency wheel input is coalesced to animation frames for smooth
  trackpad interaction.
- Empty zoomed or panned chart viewports retain the last plottable history so
  the chart remains interactive and can navigate back to sampled data.
- Wheel zoom stops at the finest domain containing a drawable sample pair,
  preventing sparse histories from disappearing inside an empty interval.
- Sparse chart samples remain connected across empty aggregation buckets;
  explicit missing metric values still break the rendered line.
- Chart settings remain above the plot interaction canvas in normal and
  enlarged chart layouts.
- Horizontal zoom-out stops at the observed full-data extent, so scrolling out
  from the default view no longer repositions the axis or requests empty space.
- Selecting a chart display, smoothing, or scale option no longer dismisses the
  settings panel when the select menu is rendered through its portal.
- Chart panning stops at the full-data boundaries while preserving viewport
  width, preventing out-of-range requests and delayed axis jumps.
- Metric hover crosshairs use a contrasting accent stroke above the uPlot grid.
- Windows backups now close SQLite handles before atomic publication, exports
  flush files through write-capable descriptors, and filename validation tests
  use paths that can be constructed on every supported platform.

## [0.1.0-alpha.1] - 2026-08-31

### Added

- Rust server with SQLite catalog, Arrow/Parquet histories, and split CAS storage.
- Python SDK with durable online and offline delivery.
- Native rich media, artifacts, traces, sweeps, reports, alerts, and host telemetry.
- Lazy multi-run dashboard with bounded uPlot charts and searchable resources.
- Standard reverse-proxy deployment, diagnostics, physical backup/restore, and
  project export.
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
