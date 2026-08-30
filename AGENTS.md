# Agent instructions

## Product contract

Runloom is a standalone experiment tracker. Trackio feature parity is the first
milestone; practical W&B feature parity is the overall contract. Compatibility
must be observable and tested.

Do not add Gradio, Hugging Face Hub, Datasets, Spaces, Buckets, or related
runtime dependencies or integrations. Runloom owns its server, dashboard,
storage, and authentication.

## Workflow

- Use `nix develop` for repository tools.
- Use Cargo for Rust, uv for Python, and pnpm for the dashboard.
- Run `just check` before committing.
- Keep changes inside this repository unless the user explicitly requests an
  external deployment or migration.
- Do not commit runtime databases, Parquet segments, journals, blobs, virtual
  environments, package caches, or build output.

## Performance rules

- Never load complete run histories into memory.
- Never fetch unrequested metric columns.
- Every queue, cache, query, and response must have an explicit bound.
- Storage is lossless; response budgets must not delete source data.
- Tie long-running work to cancellation and isolate it from async executors.
- Keep media and artifact bytes outside scalar metric queries.
- Add workload-shaped benchmarks for storage or query changes.

## Compatibility rules

- Add or update the compatibility matrix when public behavior changes.
- Prefer contract tests at the public Python and HTTP boundaries.
- Unsupported compatibility arguments must fail or warn explicitly.
- Keep compatibility adapters outside storage internals.
- Record significant storage, protocol, and deployment changes as ADRs.

## Pre-alpha change policy

- Treat Runloom's internal catalog and stored pre-alpha data as disposable.
  Keep one current definition without internal schema generations, upgrade
  logic, or shape guards.
- Do not preserve backward compatibility for the pre-alpha Python API, HTTP
  payloads, spool format, or stored data unless the user explicitly asks for it.
- Prefer replacing a weak design directly over aliases or compatibility
  scaffolding.
- Keep the `/api/v1` namespace. It is the current protocol boundary, not a
  promise that every pre-alpha shape inside it is frozen.
- Tests and documentation describe only the current supported shape. Remove
  obsolete behavior instead of carrying it forward.
