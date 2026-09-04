# HTTP API

The pre-alpha API is versioned under `/api/v1`. Request bodies are capped at
2 MiB. List, batch, metric-column, and history sizes are independently bounded.

Every integer nested in an arbitrary JSON document must be in JavaScript's
exact range, `-9007199254740991` through `9007199254740991`. This applies to
config and explicit-summary documents, rich/artifact/trace metadata and trace
previews, run-query document equality values, and sweep parameter values.
Values at both limits are accepted; an integer one step outside either limit is
rejected before persistence or comparison. The check walks only the already
bounded serialized document or request.

## Lifecycle

- `POST /projects/{project}/runs` creates or resumes a run.
- `GET /runs/{run_id}` returns config, separated summary layers, state, and
  revisions.
- `PATCH /runs/{run_id}/config` merges `updates`; replacing an existing value
  requires `allow_val_change=true`.
- `PATCH /runs/{run_id}/summary` merges JSON `updates` while the run is active.
- `POST /runs/{run_id}/finish` atomically marks a run finished.

Create bodies accept `id`, `name`, `config`, and `resume`. Resume is one of
`never`, `allow`, or `must`. Create responses include `next_sequence` and
`next_step`; clients must use these authoritative positions when appending to a
resumed run. Config and the explicit user summary accept JSON scalars, arrays,
and objects up to 256 KiB independently after merging. Mutation is shallow by
top-level key. A finished run cannot accept new metrics or document updates.
Repeating finish with the same explicit summary is idempotent; trying to change
a finished summary returns a conflict.

The complete run record exposes `explicit_summary`, `metric_summary`, and
`summary`. `metric_summary` is a bounded latest-value preview containing the
lexicographically smallest 256 non-system metric keys; retained keys continue
to update. `summary` overlays the explicit summary on that preview, so an
explicit value wins when both layers contain a key. `summary_truncated=true`
means only that more metric keys exist outside the preview. It never means that
metric history, the metric-key catalog, or the independently bounded explicit
summary was truncated.

`document_revision` increments once for each real config, explicit-summary, or
finish mutation and does not increment for an idempotent no-op. Metric ingest
and its derived preview use `metric_revision`. Alerts, rich values, artifact
lineage, and traces use `rich_data_revision`. These integer revisions, rather
than the one-second `updated_at` timestamp, are the cache-invalidation contract.

## Metrics

- `POST /runs/{run_id}/batches` accepts up to 1,024 consecutive points.
- `GET /runs/{run_id}/metrics?limit=200&after=<metric_key>` lists discovered
  scalar keys with a lexicographic cursor.
- `POST /projects/{project}/metrics/query` lists a bounded union or intersection
  of metric keys for selected runs without draining each run's catalog.
- `GET /runs/{run_id}/history?key=loss&key=reward&limit=1000` returns a columnar
  page with sequence, step, timestamp, and only the requested metric columns.
- `GET /runs/{run_id}/history?key=loss&max_points=2000` scans the selected
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

Metric-key pages contain `run_id`, `keys`, and `next_after`. A null cursor means
the server confirmed exhaustion; clients must continue non-null cursors rather
than treating one bounded page as the complete metric catalog. Raw history also
uses repeated, independently percent-encoded `key` parameters, so commas and
other URL-significant characters remain part of the metric name.

Project metric discovery accepts 1 to 32 unique `run_ids`, required `mode`
(`union` or `intersection`), and optional `search`, `after`, and `limit` fields.
Every selected run must belong to the path project. Results are lexicographic
pages of `{key, run_ids}` summaries; `run_ids` identifies exactly which selected
runs contain the key. Pass `next_after` back unchanged. This endpoint returns
catalog metadata only—metric values still come from the chart-history APIs.

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
  query parameter supplies an allowlisted image, audio, or video content type
  for native browser playback; other values download as an opaque attachment.
- `POST /runs/{run_id}/rich-values` creates a rich-value manifest.
- `GET /runs/{run_id}/rich-values/keys?limit=100&after=<key>` returns a bounded
  key catalog with counts and each key's latest summary.
- `GET /runs/{run_id}/rich-values?key=<key>&limit=100&before=<value_id>` returns
  bounded newest-first summaries for one key.
- `GET /rich-values/{value_id}` returns the selected complete manifest.

Blob uploads send the digest in the path and content type in the header. The
optional `x-epochdeck-file-name` header is a percent-encoded UTF-8 basename; after
one decoding pass it must contain 1 to 512 non-control UTF-8 bytes and no `/` or
`\\`. The server hashes while streaming to a staging file, rejects mismatches,
syncs the file, and atomically installs it under `EPOCHDECK_BLOBS_DIR/sha256`.
Repeating an already installed digest is idempotent and does not rewrite
content. Successful reads carry a digest-derived strong `ETag` and
`Cache-Control: public, max-age=31536000, immutable`; conditional and byte-range
requests retain the same immutable identity.

Rich manifests contain a stable UUIDv7 ID, key, `image`, `audio`, `video`,
`table`, or `histogram` kind, step, timestamp, optional blob reference, and up
to 256 KiB of preview metadata. Media and table values require an uploaded blob.
Exact manifest retries are idempotent.

## Artifacts

- `POST /runs/{run_id}/artifacts` creates an immutable version and an output
  lineage edge. Omit `version` to allocate the next version, or provide a
  nonnegative JSON-safe integer to reserve that exact version.
- `POST /runs/{run_id}/artifacts/use` records an input lineage edge.
- `GET /runs/{run_id}/artifacts?limit=100&before=<artifact_id>&before_relation=output`
  lists the run's bounded input/output links.
- `GET /projects/{project}/artifacts?limit=100&before=<artifact_id>` lists
  project versions newest first.
- `GET /projects/{project}/artifacts/{name}/aliases/{alias}` resolves a movable
  alias such as `latest` or `best`.
- `GET /artifacts/{artifact_id}` returns one immutable manifest.
- `GET /artifacts/{artifact_id}/lineage?relation=input&limit=100&before=<run_id>`
  returns a bounded page of input or output run summaries.
- `GET /artifacts/{artifact_id}/download` streams every manifest entry as a ZIP
  archive.
- `GET /artifacts/{artifact_id}/files/{path}` streams one manifest entry with
  range support.

Artifact collection names have one stable type per project. Each create request
contains up to 4,096 unique relative POSIX paths and a total 2 MiB manifest
budget; these are metadata bounds, not file-size or retention quotas. The server
verifies every referenced SHA-256 object before allocating the version in one
SQLite transaction. Stable request IDs make a lost create response replay-safe.
An occupied explicit version slot conflicts unless the request is an exact
same-ID replay. Automatic allocation remains `max(existing version) + 1`, so
explicit imports may preserve sparse source version numbers. An alias always
targets the highest artifact version that requested it; importing an older
version later cannot move an alias backward.
Whole-artifact downloads preserve validated relative POSIX entry paths, use
stored ZIP entries, and stream through a fixed-size buffer rather than staging
or retaining the complete archive. Responses provide both an ASCII fallback and
UTF-8 `filename*` in `Content-Disposition`.

Project artifact responses include `next_before`. Run-link responses use the
paired `next_before` and `next_before_relation` cursor because the same artifact
may occur once as an input and once as an output. Both values must be omitted or
supplied together. Artifact lineage is paginated separately for `input` and
`output` relations and returns lightweight run summaries plus `next_before`.

## Traces

- `POST /runs/{run_id}/traces` creates a structured span.
- `GET /runs/{run_id}/traces?limit=100&before=<span_id>` returns bounded
  newest-first span summaries.
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
- `GET /sweep-trials/{trial_id}` returns one trial, including its configuration.
- `POST /sweep-trials/{trial_id}/heartbeat` renews the current agent's claim.
- `POST /sweep-trials/{trial_id}/complete` records the owning `agent_id`, a
  `completed`, `failed`, or `stopped` terminal state, and an optional metric.

Definitions select `grid` or `random`, a metric and minimize/maximize goal,
between 1 and 64 parameters with at most 256 typed JSON `values` each, and a
`max_runs` cap. Grid configurations are generated by mixed-radix index and
random configurations by deterministic hashing, so the server never
materializes the search space. Claimed and bound-running trials have renewable
60-second leases. An expired trial can be reclaimed with its original config and
bound run ID, while heartbeat and terminal updates reject stale owners. Exact
terminal retries by the current owner are idempotent.

Sweep list items contain `id`, project identity, name, method, metric,
`parameter_count`, `max_runs`, `next_index`, state, and creation time; they omit
the complete parameter and early-termination documents. Trial list items retain
identity, run/agent binding, index, state, stop/result/lease state, and
timestamps, but omit `config`. Fetch the corresponding detail route only when
those documents are needed. Both list responses include `next_before` only when
another row exists. Pass it back as `before` to continue the scan.

An optional early-termination document contains `min_step` and `min_trials` for
the median rule. Ingesting the target metric records a bounded observation and
returns `stop_requested` in the normal batch acknowledgement when the run is
worse than the eligible peer median.

## Reports

- `POST /projects/{project}/reports` creates an idempotent persisted report.
- `GET /projects/{project}/reports?limit=100` lists bounded report summaries.
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

- `GET /projects?limit=100&before=<project_id>` returns bounded project
  summaries including transactionally maintained run counts.
- `GET /projects/{project}` returns one project summary for direct links.
- `GET /projects/{project}/runs?limit=100&before=<run_id>&q=<text>` returns
  lightweight run summaries.
- `GET /runs/{run_id}` returns the selected complete run document.
- `POST /query/runs` returns a bounded, cursor-paginated page of lightweight run
  summaries.
- `GET /dashboard/config` returns the server's immutable dashboard accent color
  and optional same-origin logo URL.
- `GET /dashboard/logo` returns the configured bounded image, or 404 when no
  logo is configured.
- `GET /health` checks the SQLite catalog and writable metric/blob roots.
- `GET /diagnostics` returns bounded process, queue, storage-capacity, and
  slow-request telemetry.

Run query bodies accept optional `project`, `state`, exact `name`, literal
`name_contains`, top-level `config_equals` and `summary_equals` JSON maps,
`run_ids`, `before`, and `limit`. `run_ids` is a unique set of at most 32 IDs for
one bounded bulk status refresh; it cannot be combined with `before`, and the
page limit must cover the complete requested set. Document values use typed JSON
equality, including null; they are not stringified comparisons. Responses
return `next_before` when a full page may have a continuation. The current
pre-alpha query contract rejects unknown fields, unimplemented comparison
operators, and alternate sort orders explicitly.

Newest-first project, run, report, artifact, rich-value, trace, sweep, and alert
lists order by their event time and ID. Although the cursor is represented by an
ID, the server resolves its full ordering tuple and rejects missing or foreign
cursors. This is stable for imported deterministic IDs that do not encode time.
List records deliberately omit large config, summary, layout, manifest, and
trace documents; fetch the detail route only after selection.

Every project summary contains `id`, `name`, `created_at`, `run_count`, and an
opaque decimal-string `mutation_token`. The token changes monotonically for
every project-visible catalog mutation, including transient create-delete
sequences, but not for physical metric compaction. Consumers that require one
coherent project snapshot compare the exact token before and after traversal
and retry when it differs; they must not parse it through a JavaScript number.

Run list items contain identity, name, state, timestamps,
`summary_truncated`, `document_revision`, `metric_revision`, and
`rich_data_revision`; config and all summary maps exist only on the detail
record. `summary_equals` tests an explicit value first and falls back to the
derived metric preview only when the explicit layer lacks that key.

Dashboard configuration has the shape
`{"accent_color":"#2766ad","logo_url":null}`. When a logo is configured,
`logo_url` is `/api/v1/dashboard/logo`; the response never exposes its server
filesystem path. Logo responses use the validated `image/png`, `image/jpeg`,
`image/webp`, or `image/svg+xml` content type and `Cache-Control: no-cache`.
SVG responses carry a route-specific sandboxed `default-src 'none'` content
security policy in addition to startup validation.

Native application authentication, multi-user authorization, and stable
external deployment guarantees are not implemented. Bind the EpochDeck HTTP
server to loopback and expose it only through an authenticated HTTPS reverse
proxy that terminates TLS and enforces access control. Never expose the
EpochDeck HTTP port directly.

Diagnostics retain only the most recent 64 slow requests in memory. They also
report the catalog, metric, and blob roots with string device IDs and total,
free, and available bytes, plus the 64-request API admission limit, its
currently available permits and rejection count, the dedicated two-request
health pool, and available metric-ingest, blob-upload, artifact-I/O,
download-stream, and query permits. When all API admission permits are occupied,
a new API request is rejected before its body is parsed with HTTP 503,
`server_busy`, and `Retry-After: 1`; it never joins an unbounded worker or
mutation-lock queue. Health checks use their own non-queuing two-request pool;
embedded static dashboard assets bypass API admission. Admission permits remain
attached to response bodies until EOF or disconnect. Raw blob and artifact-file
responses additionally use a 16-stream pool, preventing slow media clients from
occupying the general 64-request capacity. The default slow threshold is 1,000 ms and
`EPOCHDECK_SLOW_REQUEST_MS` accepts values from 1 to 60,000. Counters reset on
process restart and do not add a telemetry database.

SQLite writer acquisition is also bounded. If catalog contention outlasts that
bound, the request receives the same retryable HTTP 503 `server_busy` response
and `Retry-After: 1`; SQLite busy conditions are not reported as HTTP 500.
