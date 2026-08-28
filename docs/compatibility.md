# Compatibility contract

Runloom treats compatibility as executable behavior, not matching names alone.
Every supported feature requires contract tests covering local behavior, remote
behavior, restart recovery, idempotency, and dashboard visibility where
applicable.

## Compatibility levels

- **Planned**: part of the product contract but not implemented.
- **Partial**: usable behavior exists, with documented missing semantics.
- **Compatible**: the supported behavior passes the compatibility suite.

The repository is pre-alpha. Partial means the implemented subset below is
usable and tested, not that the entire feature group is complete.

## Trackio parity milestone

| Feature group | Required behavior | Status |
|---|---|---|
| Run lifecycle | `init`, `log`, `finish`, resume, stable run IDs | Compatible |
| Configuration | immutable initial config plus controlled updates | Compatible |
| Summary | automatic latest-value summary and explicit overrides | Compatible |
| Scalar metrics | steps, timestamps, nested keys, batching | Compatible |
| System metrics | CPU, memory, disk, GPU when available | Compatible |
| Rich values | images, audio, video, tables, histograms | Compatible |
| Alerts | levels, titles, text, steps, timestamps | Compatible |
| Artifacts | manifests, versions, aliases, input/output links | Compatible |
| Traces | structured spans, messages, search metadata | Compatible |
| Python API | synchronous public API and background delivery | Partial |
| Remote server | authenticated ingestion and read APIs | Partial |
| CLI | list, get, query, sync, W&B import, Runloom export | Partial |
| Dashboard | projects, runs, metrics, media, artifacts, traces, reports | Partial |
| Import/export | lossless local export and resumable import | Partial |

Features tied specifically to third-party hosting platforms are deliberately
excluded. Runloom provides its own server, storage roots, and deployment model;
the documented pre-alpha deployment uses Tailnet policy as its access boundary.

## W&B parity roadmap

| Feature group | Required behavior | Status |
|---|---|---|
| Drop-in workflow | `import runloom as wandb` for common training code | Partial |
| Run modes | online, offline, disabled, resume policies | Compatible |
| Public API | projects, runs, filters, history, files, artifacts | Partial |
| Media semantics | captions, grouping, sequences, native playback | Partial |
| Tables | typed columns, incremental data, linked rich values | Partial |
| Artifacts | collections, versions, aliases, lineage, downloads | Compatible |
| Sweeps | definitions, agents, scheduling, early termination | Partial |
| Reports | persisted dashboard/report definitions | Partial |
| Groups and jobs | group, job type, tags, notes, ownership metadata | Planned |
| Importers | W&B API and export formats with resumable checkpoints | Partial |
| Compatibility errors | explicit diagnostics for unsupported behavior | Partial |

## Test strategy

The scalar lifecycle suite executes user-level workflows through the public
Python and HTTP boundaries and compares their observable sequences, steps,
metric keys, config, summary, and terminal state with checked-in golden data.
It covers online logging, offline restart, durable batch replay after a lost
response, authoritative remote resume positions, repeated offline sync,
idempotent finish, and recovery when a finish response is lost after commit.
The fixture is versioned at
`python/tests/contracts/fixtures/scalar_lifecycle.json`.

Future feature groups will add equivalent reference-implementation scenarios,
normalizing nondeterministic identifiers before comparison. A compatible row
above applies only to its documented required behavior; it does not imply rich
values, artifacts, sweeps, reports, or the complete W&B API.

Unsupported arguments must fail or warn explicitly. Runloom will not silently
accept inert compatibility flags.

The current partial scalar contract supports finite numeric and boolean values,
nested dictionaries flattened with `/`, explicit or automatic steps, durable
local spooling, resumable batch delivery, Parquet persistence, and bounded
column-projected history. Dashboard charts scan only requested columns and use
bounded exact min/max/last buckets that preserve source spikes without deleting
raw samples; settled step viewports are re-aggregated for zoom detail.
Background compaction transparently reduces immutable segment
counts without changing logical revisions or history results. Config supports
controlled shallow updates, and summaries support
bounded JSON strings, booleans, nulls, arrays, nested objects, and explicit
overrides. Strings in metric history, media, tables, and histograms remain
unsupported and fail explicitly.

The SDK records bounded host and process telemetry every 15 seconds after the
first user metric. Set `RUNLOOM_SYSTEM_METRICS_INTERVAL=0` to disable it. System
samples use the most recently completed user step and never change automatic
step progression or the run summary. Alerts use a separate fsynced journal,
sortable UUIDv7 identities, idempotent delivery, and a bounded dashboard list.

Native `Image`, `Audio`, `Video`, `Table`, and `Histogram` values share the
durable run step with scalar values. Media and table bytes are hashed and copied
into the local spool before `log` returns, uploaded by streaming PUT, and stored
once in the server's SHA-256 blob root. The dashboard uses lazy images,
metadata-only audio/video loading, HTTP range playback, bounded table previews,
Canvas histograms, and key-grouped step timelines. The wider W&B array
conversion and incremental mutable-table surface remains partial.

Artifacts are immutable project-scoped collections with transactional `v0`,
`v1`, ... version allocation, movable aliases, bounded manifests, exact retry
IDs, and explicit input/output run links. File content reuses the rich-data CAS;
individual downloads support ranges, and whole-artifact ZIP downloads stream
through a bounded producer without buffering an archive in memory. The dashboard
provides artifact tabs, directory navigation, and per-file downloads. The SDK
snapshots files into its spool before journaling a create operation and drains
creates and lineage links fairly with metrics, rich values, and alerts.

Structured traces store bounded searchable names, attributes, and message
previews in SQLite while complete JSON inputs, outputs, and messages live in the
content-addressed blob store. Spans have explicit trace and parent IDs, timing,
status, kind, and the current user step. The SDK journals each finished span and
payload before background delivery; exact request IDs make response-loss retries
idempotent. Search uses SQLite FTS over bounded previews, so query cost and
catalog growth do not depend on complete prompt or response payload size.

`runloom.Api` exposes bounded project discovery, lazy server-filtered run pages,
single-run lookup, full-resolution history scans, artifacts, and traces. Filters
currently support state, name, and typed top-level config/summary equality. The
wider W&B filter language, general run-file surface, and alternate ordering are
not yet implemented and fail explicitly.

Sweep definitions and trial claims are durable SQLite transactions. Grid and
random schedulers select from finite typed `values` sets without materializing
the Cartesian product. Agents lease claims, bind exactly one run, report a
terminal result idempotently, and inherit the scheduled config through ordinary
`runloom.init`. Optional median stopping compares bounded peer observations;
the batch acknowledgement sets `run.should_stop`, and the next `log` raises
`SweepEarlyStop`. Continuous distributions, Hyperband, and process-level remote
agents remain outside the current partial W&B surface and fail explicitly.

Reports are durable project-scoped grid definitions with typed metric and
Markdown panels. Report metric references are validated against project runs,
and the dashboard renders their histories with lazy exact-bucket aggregation and
a four-request concurrency cap. Definitions can be created, listed, replaced, and
deleted through the HTTP and public Python APIs. The Markdown renderer supports
safe headings, paragraphs, lists, and fenced code. Arbitrary W&B report blocks,
inline rich text, collaborative editing, and hosted sharing semantics remain
outside the current partial surface.

The W&B importer scans runs lazily with one to sixteen bounded workers. Its
history window adapts to sparse, high-valued step domains rather than walking
empty fixed-size ranges. Stable run, batch, rich-value, and artifact identities
plus an fsynced checkpoint make retries resumable and idempotent after ambiguous
responses. Scalar history, config, final source summary, source metadata, W&B
run files, and logged output artifacts are retained in CAS. Image, audio, and
video history references become native Runloom rich rows. Unsupported history
cells are counted in the final imported summary. Registry-wide and input
artifact lineage remain outside this partial importer.

Runloom project exports are lossless for the current supported Runloom surface.
They cursor-scan full-resolution metric columns and every paginated metadata
collection, retain artifact links and control-plane definitions, stream each
referenced CAS digest once, and verify content before an atomic directory
install. Orphaned, unreferenced blob uploads are deliberately excluded.
