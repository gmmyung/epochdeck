# Compatibility

EpochDeck supports the behavior listed below. Matching a third-party name alone
does not count as compatibility; supported behavior must be observable and
tested.

> [!IMPORTANT]
> EpochDeck is pre-alpha. The current interface is documented, but backward
> compatibility and stored-data migrations are not yet promised.

## Status levels

| Status     | Meaning                                                             |
| ---------- | ------------------------------------------------------------------- |
| Compatible | The stated behavior is implemented and covered by contract tests.   |
| Partial    | A useful tested subset exists; missing behavior remains.            |
| Planned    | The feature is part of the intended surface but is not implemented. |

## Trackio parity

| Feature           | Supported behavior                                              | Status     |
| ----------------- | --------------------------------------------------------------- | ---------- |
| Run lifecycle     | `init`, `log`, `finish`, resume, and stable run IDs             | Compatible |
| Configuration     | Initial config and controlled updates                           | Compatible |
| Summary           | Latest-value preview, truncation signal, and explicit overrides | Compatible |
| Metrics           | Steps, timestamps, nested keys, batching, and system telemetry  | Compatible |
| Rich values       | Images, audio, video, tables, and histograms                    | Compatible |
| Alerts            | Levels, titles, text, steps, and timestamps                     | Compatible |
| Artifacts         | Manifests, versions, aliases, and lineage                       | Compatible |
| Python API        | Synchronous logging, background delivery, and read APIs         | Partial    |
| CLI               | Health, query, sync, W&B import, and EpochDeck export           | Partial    |
| Dashboard         | Runs, comparison charts, media, artifacts, and reports          | Partial    |
| Import and export | Resumable W&B import and lossless EpochDeck export              | Partial    |

## W&B compatibility

| Feature                                                | Status     |
| ------------------------------------------------------ | ---------- |
| `import epochdeck as ed` workflow                      | Partial    |
| Online, offline, disabled, and resume modes            | Compatible |
| Projects, runs, filters, history, files, and artifacts | Partial    |
| Media sequences and native playback                    | Partial    |
| Typed and incremental tables                           | Partial    |
| Artifact versions, aliases, lineage, and downloads     | Compatible |
| Finite sweeps, agents, and early termination           | Partial    |
| Persisted reports                                      | Partial    |
| Groups, jobs, tags, notes, and ownership metadata      | Planned    |
| W&B import with resumable checkpoints                  | Partial    |

Hosted-platform features are out of scope. EpochDeck owns its server, storage,
dashboard, and deployment model. Structured execution traces are deliberately
not supported.

## Contract rules

- Unsupported compatibility arguments fail or warn instead of doing nothing.
- Public Python and HTTP behavior is covered at the boundary.
- Queues, caches, queries, and responses have explicit bounds.
- Dashboard sampling never deletes or rewrites raw metric history.
- Trackio and W&B are compatibility references, not runtime dependencies.

For exact behavior, use the canonical reference for that interface:

- [Python SDK](../python/README.md)
- [HTTP API](api.md)
- [Export format](export-format.md)
- [Architecture and performance invariants](architecture.md)
- [Remaining work](roadmap.md)
