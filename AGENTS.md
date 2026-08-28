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
