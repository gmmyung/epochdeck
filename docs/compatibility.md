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
| Artifacts | manifests, versions, aliases, input/output links | Planned |
| Traces | structured spans, messages, search metadata | Planned |
| Python API | synchronous public API and background delivery | Partial |
| Remote server | authenticated ingestion and read APIs | Partial |
| CLI | serve, list, get, query, import, export | Partial |
| Dashboard | projects, runs, metrics, media, artifacts, traces | Partial |
| Import/export | lossless local export and resumable import | Planned |

Features tied specifically to third-party hosting platforms are deliberately
excluded. Runloom provides its own server, storage roots, authentication, and
deployment model.

## W&B parity roadmap

| Feature group | Required behavior | Status |
|---|---|---|
| Drop-in workflow | `import runloom as wandb` for common training code | Partial |
| Run modes | online, offline, disabled, resume policies | Compatible |
| Public API | projects, runs, filters, history, files, artifacts | Planned |
| Media semantics | captions, grouping, sequences, native playback | Partial |
| Tables | typed columns, incremental data, linked rich values | Partial |
| Artifacts | collections, versions, aliases, lineage, downloads | Planned |
| Sweeps | definitions, agents, scheduling, early termination | Planned |
| Reports | persisted dashboard/report definitions | Planned |
| Groups and jobs | group, job type, tags, notes, ownership metadata | Planned |
| Importers | W&B API and export formats with resumable checkpoints | Planned |
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
column-projected history. Dashboard queries scan only requested columns and use
a bounded min/max representation that preserves local spikes without deleting
raw samples. Background compaction transparently reduces immutable segment
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
and Canvas histograms. The wider W&B media row-grouping, array conversion, and
incremental mutable-table surface remains partial.
