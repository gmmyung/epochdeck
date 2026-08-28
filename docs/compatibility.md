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
| Run lifecycle | `init`, `log`, `finish`, resume, stable run IDs | Partial |
| Configuration | immutable initial config plus controlled updates | Partial |
| Summary | automatic latest-value summary and explicit overrides | Partial |
| Scalar metrics | steps, timestamps, nested keys, batching | Partial |
| System metrics | CPU, memory, disk, GPU when available | Planned |
| Rich values | images, audio, video, tables, histograms | Planned |
| Alerts | levels, titles, text, steps, timestamps | Planned |
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
| Run modes | online, offline, disabled, resume policies | Partial |
| Public API | projects, runs, filters, history, files, artifacts | Planned |
| Media semantics | captions, grouping, sequences, native playback | Planned |
| Tables | typed columns, incremental data, linked rich values | Planned |
| Artifacts | collections, versions, aliases, lineage, downloads | Planned |
| Sweeps | definitions, agents, scheduling, early termination | Planned |
| Reports | persisted dashboard/report definitions | Planned |
| Groups and jobs | group, job type, tags, notes, ownership metadata | Planned |
| Importers | W&B API and export formats with resumable checkpoints | Planned |
| Compatibility errors | explicit diagnostics for unsupported behavior | Planned |

## Test strategy

The compatibility suite will run equivalent user-level scenarios against a
reference implementation and Runloom, normalize nondeterministic identifiers,
and compare observable results. Golden protocol fixtures cover offline queues,
server restarts, duplicate delivery, partial uploads, and interrupted imports.

Unsupported arguments must fail or warn explicitly. Runloom will not silently
accept inert compatibility flags.

The current partial scalar contract supports finite numeric and boolean values,
nested dictionaries flattened with `/`, explicit or automatic steps, durable
local spooling, resumable batch delivery, Parquet persistence, and bounded
column-projected history. Dashboard queries scan only requested columns and use
a bounded min/max representation that preserves local spikes without deleting
raw samples. Strings, media, tables, histograms, and config mutation remain
unsupported and fail explicitly.
