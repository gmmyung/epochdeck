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
- `GET /runs/{run_id}/chart-history?key=loss&max_buckets=1000` returns exact
  per-bucket minimum, maximum, and last values for chart rendering.
- `POST /projects/{project}/chart-history/query` places requested series from
  multiple runs on one bounded comparison axis.

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

Chart history is a separate aggregate contract; raw cursor pagination on
`/history` is unchanged. Repeat the percent-encoded `key` parameter to request
multiple metrics. `max_buckets` is between 1 and 2,000, and the product
of requested metric keys and buckets cannot exceed 5,000 metric-bucket cells.
When omitted, the server uses the smaller of 1,000 buckets and the per-key share
of that global limit. Optional `step_min` and `step_max` are an inclusive
viewport and must be supplied together in ascending order. Without a viewport,
the server discovers the union step extent containing a non-null requested
metric. All scans project only the requested Parquet columns and use a frozen
sequence watermark, so concurrent ingestion appears in the next snapshot.
Explicit viewports also prune Parquet row groups whose exact step statistics
prove they cannot overlap the requested range. Missing, inexact, deprecated, or
internally inconsistent statistics disable pruning for that row group rather
than risking the loss of source values.

The response is columnar and keeps sparse metrics independent:

```json
{
  "run_id": "019c...",
  "step_min": 0,
  "step_max": 9999,
  "bucket_count": 1000,
  "source_points": 10000,
  "source_last_sequence": 10000,
  "metrics": {
    "loss": {
      "source_points": 9998,
      "bucket": [0, 1],
      "last_x": [9, 19],
      "last_step": [9, 19],
      "last_timestamp_ms": [1710000000009, 1710000000019],
      "minimum": [0.82, 0.71],
      "maximum": [1.14, 0.93],
      "last": [0.88, 0.74]
    }
  }
}
```

Each metric's arrays are aligned and omit buckets in which that metric has no
value. `minimum` and `maximum` are the exact extrema of every source value for
that metric in the bucket. `last` is selected by greatest run sequence even
when steps repeat or move backward; `last_step` and `last_timestamp_ms` come
from that same point. `last_x` equals `last_step` on this absolute-step
endpoint. The bucket index is
`floor((step - step_min) * bucket_count / (step_max - step_min + 1))`.
The dashboard draws Band directly from `minimum`/`maximum` and may smooth the
`last` trend column; it must not construct a rolling envelope from sampled
points. `source_points` at the top level counts rows containing any requested
metric, while each metric reports its own non-null source count.

Multi-run charts send one project-scoped JSON request:

```json
{
  "series": [
    {"run_id": "019c...", "key": "loss"},
    {"run_id": "019d...", "key": "loss"}
  ],
  "alignment": "relative_step",
  "max_buckets": 1000,
  "viewport": {"minimum": 0, "maximum": 10000}
}
```

`alignment` is `step`, `relative_step`, or `elapsed_time`. Step uses each raw
run step. Relative step subtracts the minimum step containing any requested
metric for each run, and elapsed time subtracts the equivalent per-run minimum
timestamp in milliseconds. The optional inclusive viewport is expressed in
the selected aligned unit. Requests contain 1 to 32 unique `(run_id, key)`
series from at most 32 runs, at most 2,000 buckets, and at most 20,000 requested
series-bucket cells. Every run must belong to the path project.

The response returns one shared `x_min`, `x_max`, and `bucket_count`, fixed
per-run `source_last_sequence` watermarks, and sparse series:

```json
{
  "project": "robotics",
  "alignment": "relative_step",
  "x_min": 0,
  "x_max": 10000,
  "bucket_count": 1000,
  "runs": [
    {"run_id": "019c...", "source_last_sequence": 12000},
    {"run_id": "019d...", "source_last_sequence": 9000}
  ],
  "series": [{
    "run_id": "019c...",
    "key": "loss",
    "source_points": 9998,
    "bucket": [0, 1],
    "last_x": [9, 19],
    "last_step": [109, 119],
    "last_timestamp_ms": [1710000000009, 1710000000019],
    "minimum": [0.82, 0.71],
    "maximum": [1.14, 0.93],
    "last": [0.88, 0.74]
  }]
}
```

All series use the same bucket lattice. Empty buckets and missing metrics are
omitted rather than interpolated or zero-filled. Each occupied bucket retains
exact source minimum and maximum values and selects `last`, `last_x`, raw
`last_step`, and `last_timestamp_ms` from the greatest sequence. The server
groups requested columns by run, scans against one snapshot barrier and fixed
per-run watermarks, caches exact per-key axis extents (including missing
metrics) by watermark, and caches individual aggregate series with watermark,
origin, and lattice-aware keys. Both caches have independent explicit entry and
byte bounds; aggregate series additionally have a cell bound.

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
- `GET /runs/{run_id}/artifacts?limit=100&before=<artifact_id>` lists the run's
  bounded input/output links.
- `GET /projects/{project}/artifacts?limit=100&before=<artifact_id>` lists
  project versions newest first.
- `GET /projects/{project}/artifacts/{name}/aliases/{alias}` resolves a movable
  alias such as `latest` or `best`.
- `GET /artifacts/{artifact_id}` returns one immutable manifest.
- `GET /artifacts/{artifact_id}/lineage` returns bounded input/output run IDs.
- `GET /artifacts/{artifact_id}/download` streams every manifest entry as a ZIP
  archive.
- `GET /artifacts/{artifact_id}/files/{path}` streams one manifest entry with
  range support.

Artifact collection names have one stable type per project. Each create request
contains up to 4,096 unique relative POSIX paths and a total 2 MiB manifest
budget; these are metadata bounds, not file-size or retention quotas. The server
verifies every referenced SHA-256 object before allocating the version in one
SQLite transaction. Stable request IDs make a lost create response replay-safe.
Whole-artifact downloads preserve validated relative POSIX entry paths, use
stored ZIP entries, and stream through a fixed-size buffer rather than staging
or retaining the complete archive. Responses provide both an ASCII fallback and
UTF-8 `filename*` in `Content-Disposition`.

Project artifact and run-link responses include `next_before` cursors. The
artifact ID order is stable and allows complete lineage scans without loading a
run's link set into one response.

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

## Sweeps and agents

- `POST /projects/{project}/sweeps` creates an idempotent sweep definition.
- `GET /projects/{project}/sweeps?limit=100` lists bounded sweep metadata.
- `GET /sweeps/{sweep_id}` returns one definition and scheduler position.
- `POST /sweeps/{sweep_id}/claim` leases one configuration to an agent.
- `GET /sweeps/{sweep_id}/trials?limit=100` lists bounded trial state.
- `POST /sweep-trials/{trial_id}/complete` records `completed`, `failed`, or
  `stopped` terminal state and an optional metric.

Definitions select `grid` or `random`, a metric and minimize/maximize goal,
between 1 and 64 parameters with at most 256 typed JSON `values` each, and a
`max_runs` cap. Grid configurations are generated by mixed-radix index and
random configurations by deterministic hashing, so the server never
materializes the search space. Claims have a 60-second pre-run lease; binding a
run makes the claim exclusive. Exact terminal retries are idempotent.

Sweep and trial list responses include `next_before` when a full page may have a
continuation. Pass it back as `before` to scan definitions without a fixed total
limit.

An optional early-termination document contains `min_step` and `min_trials` for
the median rule. Ingesting the target metric records a bounded observation and
returns `stop_requested` in the normal batch acknowledgement when the run is
worse than the eligible peer median.

## Reports

- `POST /projects/{project}/reports` creates an idempotent persisted report.
- `GET /projects/{project}/reports?limit=100` lists bounded report definitions.
- `GET /reports/{report_id}` returns one definition.
- `PUT /reports/{report_id}` replaces its current definition.
- `DELETE /reports/{report_id}` removes the definition, not its referenced runs.

A report has one to four columns and at most 32 panels. Each panel has a unique
safe identifier, title, width, and height. Metric panels reference exactly one
run in the same project and one to eight metric keys. Markdown panels contain a
bounded document and cannot reference run metrics. The complete serialized
layout is capped at 256 KiB.

Reports store no metric copies or aggregate results. The dashboard lazily
requests exact min/max/last buckets for each visible metric with a fixed bucket
budget and at most four concurrent chart queries. Report creation validates all
referenced runs before committing the layout transaction.

Report list responses use the same `next_before` cursor convention.

## Discovery

- `GET /projects?limit=100` returns bounded project summaries.
- `GET /projects/{project}/runs?limit=100` returns bounded run records.
- `POST /query/runs` returns a bounded, cursor-paginated run query.
- `GET /health` checks the service and SQLite catalog.
- `GET /diagnostics` returns bounded process, queue, schema, and slow-request
  telemetry.

Run query bodies accept optional `project`, `state`, exact `name`, literal
`name_contains`, top-level `config_equals` and `summary_equals` JSON maps,
`before`, and `limit`. Document values use typed JSON equality, including null;
they are not stringified comparisons. Responses return `next_before` when a
full page may have a continuation. The current pre-alpha query contract rejects
unimplemented comparison operators and alternate sort orders explicitly.

Authentication and stable external deployment guarantees are not implemented
yet. Keep the current server on a trusted interface or Tailnet.

Diagnostics retain only the most recent 64 slow requests in memory. The default
slow threshold is 1,000 ms and `RUNLOOM_SLOW_REQUEST_MS` accepts values from 1
to 60,000. Counters reset on process restart and do not add a telemetry database.
