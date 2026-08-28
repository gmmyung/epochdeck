# HTTP API

The pre-alpha API is versioned under `/api/v1`. Request bodies are capped at
2 MiB. List, batch, metric-column, and history sizes are independently bounded.

## Lifecycle

- `POST /projects/{project}/runs` creates or resumes a run.
- `GET /runs/{run_id}` returns config, summary, state, and revisions.
- `PATCH /runs/{run_id}/config` merges `updates`; replacing an existing value
  requires `allow_val_change=true`.
- `PATCH /runs/{run_id}/summary` merges JSON `updates` while the run is active.
- `POST /runs/{run_id}/finish` atomically marks a run finished.

Create bodies accept `id`, `name`, `config`, and `resume`. Resume is one of
`never`, `allow`, or `must`. Create responses include `next_sequence` and
`next_step`; clients must use these authoritative positions when appending to a
resumed run. Config and summary documents accept JSON scalars, arrays, and
objects up to 256 KiB after merging. Mutation is shallow by top-level key. A
finished run cannot accept new metrics or document updates. Repeating finish
with the same summary is idempotent; trying to change a finished summary returns
a conflict.

## Metrics

- `POST /runs/{run_id}/batches` accepts up to 1,024 consecutive points.
- `GET /runs/{run_id}/metrics` lists discovered scalar keys.
- `GET /runs/{run_id}/history?keys=loss,reward&limit=1000` returns a columnar
  page with sequence, step, timestamp, and only the requested metric columns.
- `GET /runs/{run_id}/history?keys=loss&max_points=2000` scans the selected
  columns across the run and returns a bounded min/max representation.

Batch sequence and canonical request digest form the idempotency contract. An
identical replay succeeds as a duplicate; reusing a sequence for different
contents returns a conflict. History requests accept at most 32 columns and
5,000 points. `limit` selects full-resolution cursor pagination;
`max_points` selects spike-preserving display sampling, and the two parameters
are mutually exclusive. Sampled responses set `sampled` and `source_points`;
full-resolution pages continue with the returned `next_after` cursor. A sampled
budget must allow at least two extrema per requested metric. History responses
include `source_last_sequence`, allowing a revision-aware client to request only
new rows after a sampled snapshot. These response bounds are not retention
quotas.

## Monitoring

- `POST /runs/{run_id}/alerts` creates an alert while a run is active.
- `GET /runs/{run_id}/alerts?limit=100&before=<alert_id>` returns alerts newest
  first with a bounded UUIDv7 cursor.

Alert bodies contain an optional stable `id`, `title`, `text`, `level`, optional
`step`, and `timestamp_ms`. Levels are `info`, `warn`, or `error`; titles are at
most 256 UTF-8 bytes and text is at most 4 KiB. Repeating the same ID and body is
idempotent. Reusing an ID with different content returns a conflict.

System metrics share the scalar history API under the reserved `system/`
namespace. The SDK records CPU, memory, disk, network, process, load-average,
and NVIDIA GPU values when available. Samples are stored losslessly but excluded
from the user summary and do not advance the logical training step.

## Rich values and blobs

- `PUT /blobs/{sha256}` streams bytes into the content-addressed blob store.
- `GET /blobs/{sha256}` supports standard byte ranges. The optional `mime`
  query parameter supplies the media content type for native browser playback.
- `POST /runs/{run_id}/rich-values` creates a rich-value manifest.
- `GET /runs/{run_id}/rich-values?limit=100&before=<value_id>` returns bounded
  newest-first manifests.

Blob uploads send the digest in the path and content type in the header. The
server hashes while streaming to a staging file, rejects mismatches, syncs the
file, and atomically installs it under `RUNLOOM_BLOBS_DIR/sha256`. Repeating an
already installed digest is idempotent and does not rewrite content.

Rich manifests contain a stable UUIDv7 ID, key, `image`, `audio`, `video`,
`table`, or `histogram` kind, step, timestamp, optional blob reference, and up
to 256 KiB of preview metadata. Media and table values require an uploaded blob.
Exact manifest retries are idempotent.

## Artifacts

- `POST /runs/{run_id}/artifacts` creates the next immutable version and an
  output lineage edge.
- `POST /runs/{run_id}/artifacts/use` records an input lineage edge.
- `GET /runs/{run_id}/artifacts` lists the run's bounded input/output links.
- `GET /projects/{project}/artifacts?limit=100&before=<artifact_id>` lists
  project versions newest first.
- `GET /projects/{project}/artifacts/{name}/aliases/{alias}` resolves a movable
  alias such as `latest` or `best`.
- `GET /artifacts/{artifact_id}` returns one immutable manifest.
- `GET /artifacts/{artifact_id}/lineage` returns bounded input/output run IDs.
- `GET /artifacts/{artifact_id}/files/{path}` streams one manifest entry with
  range support.

Artifact collection names have one stable type per project. Each create request
contains up to 4,096 unique relative POSIX paths and a total 2 MiB manifest
budget; these are metadata bounds, not file-size or retention quotas. The server
verifies every referenced SHA-256 object before allocating the version in one
SQLite transaction. Stable request IDs make a lost create response replay-safe.

## Traces

- `POST /runs/{run_id}/traces` creates a structured span.
- `GET /runs/{run_id}/traces?limit=100&before=<span_id>` returns bounded
  newest-first spans.
- `GET /runs/{run_id}/traces?q=assistant+reward&limit=100` searches indexed
  names, attributes, and bounded message previews.
- `GET /traces/{span_id}` returns one span.

Span bodies contain an optional stable UUIDv7 `id`, a caller-selected
`trace_id`, optional `parent_span_id`, name, `span`, `llm`, `tool`, `chain`, or
`agent` kind, `unset`, `ok`, or `error` status, start/end times, optional run
step, bounded attributes and preview documents, and an optional uploaded JSON
payload blob. Complete inputs, outputs, and messages belong in the payload;
SQLite retains only the metadata needed to list and search. Exact span retries
are idempotent, and reusing an ID with different contents returns a conflict.

## Discovery

- `GET /projects?limit=100` returns bounded project summaries.
- `GET /projects/{project}/runs?limit=100` returns bounded run records.
- `GET /health` checks the service and SQLite catalog.

Authentication and stable external deployment guarantees are not implemented
yet. Keep the current server on a trusted interface or Tailnet.
