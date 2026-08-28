# HTTP API

The pre-alpha API is versioned under `/api/v1`. Request bodies are capped at
2 MiB. List, batch, metric-column, and history sizes are independently bounded.

## Lifecycle

- `POST /projects/{project}/runs` creates or resumes a run.
- `GET /runs/{run_id}` returns config, summary, state, and revisions.
- `POST /runs/{run_id}/finish` atomically marks a run finished.

Create bodies accept `id`, `name`, `config`, and `resume`. Resume is one of
`never`, `allow`, or `must`. A finished run cannot accept new metrics.

## Metrics

- `POST /runs/{run_id}/batches` accepts up to 1,024 consecutive points.
- `GET /runs/{run_id}/metrics` lists discovered scalar keys.
- `GET /runs/{run_id}/history?keys=loss,reward&limit=1000` returns a columnar
  page with sequence, step, timestamp, and only the requested metric columns.

Batch sequence and canonical request digest form the idempotency contract. An
identical replay succeeds as a duplicate; reusing a sequence for different
contents returns a conflict. History requests accept at most 32 columns and
5,000 points. Continue a full-resolution scan with the returned `next_after`
cursor. These response bounds are not retention quotas.

## Discovery

- `GET /projects?limit=100` returns bounded project summaries.
- `GET /projects/{project}/runs?limit=100` returns bounded run records.
- `GET /health` checks the service and SQLite catalog.

Authentication and stable external deployment guarantees are not implemented
yet. Keep the current server on a trusted interface or Tailnet.
