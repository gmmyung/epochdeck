# Compatibility contract

Runloom treats compatibility as executable behavior, not matching names alone.
Every supported feature requires contract tests covering local behavior, remote
behavior, restart recovery, idempotency, and dashboard visibility where
applicable.

## Compatibility levels

- **Planned**: part of the product contract but not implemented.
- **Partial**: usable behavior exists, with documented missing semantics.
- **Compatible**: the supported behavior passes the compatibility suite.

The initial repository is pre-alpha, so all feature groups begin as Planned.

## Trackio parity milestone

| Feature group | Required behavior | Status |
|---|---|---|
| Run lifecycle | `init`, `log`, `finish`, resume, stable run IDs | Planned |
| Configuration | immutable initial config plus controlled updates | Planned |
| Summary | automatic latest-value summary and explicit overrides | Planned |
| Scalar metrics | steps, timestamps, nested keys, batching | Planned |
| System metrics | CPU, memory, disk, GPU when available | Planned |
| Rich values | images, audio, video, tables, histograms | Planned |
| Alerts | levels, titles, text, steps, timestamps | Planned |
| Artifacts | manifests, versions, aliases, input/output links | Planned |
| Traces | structured spans, messages, search metadata | Planned |
| Python API | synchronous public API and background delivery | Planned |
| Remote server | authenticated ingestion and read APIs | Planned |
| CLI | serve, list, get, query, import, export | Planned |
| Dashboard | projects, runs, metrics, media, artifacts, traces | Planned |
| Import/export | lossless local export and resumable import | Planned |

Features tied specifically to third-party hosting platforms are deliberately
excluded. Runloom provides its own server, storage roots, authentication, and
deployment model.

## W&B parity roadmap

| Feature group | Required behavior | Status |
|---|---|---|
| Drop-in workflow | `import runloom as wandb` for common training code | Planned |
| Run modes | online, offline, disabled, resume policies | Planned |
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
