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
| Summary | bounded latest-value preview, truncation signal, explicit overrides | Compatible |
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
SDK durability tests also exercise maximum-density metric points, rejection
before journal mutation, exact byte-budget batch splitting, retry body identity,
ACK-independent bounded summary-tail recovery, checkpoint intervals, malformed
snapshot offsets, and explicit-summary precedence after resume.
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
column-projected history. The Python SDK normalizes booleans to `0.0`/`1.0`,
requires unique string paths after flattening, and caps one traversal at 64
mapping levels and 65,536 values before journaling. Dashboard charts scan only requested columns and use
bounded exact min/max/last buckets that preserve source spikes without deleting
raw samples; settled viewports are re-aggregated for zoom detail. Project
comparisons overlay sparse series from up to 32 selected runs on a shared
absolute-step, relative-step, or elapsed-time lattice. They retain fixed
per-run sequence watermarks and never interpolate missing metrics.
Background compaction transparently reduces immutable segment
counts without changing logical revisions or history results. Config supports
controlled shallow updates. The Python SDK additionally rejects a config or
summary beyond 64 nesting levels or 65,536 JSON value nodes before durable
journaling. Across raw HTTP and SDK requests, every integer nested in config,
summary, rich/artifact/trace metadata or previews, document equality filters,
and sweep parameter values must remain in the exact JSON-safe range
`-9007199254740991` through `9007199254740991`; out-of-range integers fail
explicitly rather than being rounded by a browser. Explicit summary values have
an independent 256 KiB JSON budget and
override automatic values in the merged summary. The automatic
latest-value preview deterministically retains the lexicographically smallest
256 non-system metric keys and exposes `summary_truncated` once other keys have
been omitted. Full metric histories remain lossless and discoverable regardless
of that preview bound. Bare string values in scalar metric history remain
unsupported; native media and table values retain their documented captions,
metadata, and string cells.

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

Artifacts are immutable project-scoped collections with transactional automatic
version allocation, optional exact version slots for source-preserving imports,
bounded manifests, exact retry IDs, and explicit input/output run links. Alias
targets move only toward the highest version that requested them, so an older
backfill cannot overwrite a newer target. File content reuses the rich-data CAS;
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
catalog growth do not depend on complete prompt or response payload size. The
Python SDK caps attributes at 256 KiB and the complete JSON payload of each span
at 16 MiB, and rejects an oversized span before writing it to the durable spool.

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
empty fixed-size ranges. Since W&B materializes every row in a step window, the
current importer accepts only authoritative histories of at most 100,000 rows
and rejects missing or larger `historyLineCount` values. Stable run, batch,
rich-value, and artifact identities plus an exclusively locked, fsynced
checkpoint make retries resumable and idempotent after ambiguous responses; the
checkpoint also binds whether files are included. Media and artifacts use
separate bounded transfer windows, with temporary files released immediately
after upload.
Scalar history, config, final source summary, source metadata, W&B run files,
and logged output artifacts are retained in CAS. Image, audio, and video history
references become native Runloom rich rows, and logged artifacts preserve their
canonical W&B `vN` version number in Runloom's exact version slot. Only terminal
source runs are accepted, and the importer rejects a source whose update token
changes before completion. Unsupported history cells are counted in the final
imported summary. Registry-wide and input artifact lineage remain outside this
partial importer.

Runloom project exports are lossless for the current supported Runloom surface.
They cursor-scan full-resolution metric columns and every paginated metadata
collection, retain artifact links and control-plane definitions, stream each
referenced CAS digest once, and verify content before an atomic directory
install. Orphaned, unreferenced blob uploads are deliberately excluded.
