# ADR 0013: Multi-run chart comparison

- Status: Accepted
- Date: 2026-08-29

## Context

Fetching one chart aggregate per run makes a comparison workspace fan out in
the browser, gives each response a different bucket lattice, and repeats
manifest and Parquet scans when several requested metrics belong to one run.
Joining raw histories in the dashboard would make browser memory proportional
to run length and would lose the exact source envelope guaranteed by the chart
API.

Runs also commonly begin at different steps and wall-clock times. A useful
comparison needs explicit absolute-step, relative-step, and elapsed-time
semantics without filling absent metrics or gaps with invented values.

## Decision

Add the project-scoped
`POST /api/v1/projects/{project}/chart-history/query` aggregate. A request names
unique `(run_id, metric key)` series, one alignment, a display bucket budget,
and an optional inclusive aligned viewport. It is limited to 32 runs, 32
series, 2,000 buckets, and 20,000 requested series-bucket cells. Every run is
loaded and checked against the path project before metric files are read.

All returned series use one inclusive axis extent and bucket lattice. `step`
uses raw steps. `relative_step` subtracts the minimum step containing any of
that run's requested values. `elapsed_time` similarly subtracts its minimum
requested-value timestamp and expresses the result in milliseconds. Empty
buckets remain absent. Every occupied bucket contains exact minimum and maximum
values and the value, aligned x, raw step, and timestamp selected by greatest
run sequence.

The query holds one metric snapshot guard and one bounded worker permit while
capturing a sequence watermark for each run. Keys are grouped by run: at most
one projected extent pass for cold keys and one projected aggregate pass are
made per run, not per series. Dropping the request sets a cancellation flag
observed between segments and record batches. Absolute-step requests with an
explicit viewport skip the extent pass, and exact step statistics prune
disjoint row groups.

Exact per-metric axis extents, including the absence of a requested metric, are
cached by run, metric key, and fixed first/last sequence watermark. A grouped
cold scan populates all missing keys in one pass; later natural-range queries
combine the cached per-key extents before looking up aggregate series, so an
unchanged replay performs neither extent nor aggregate scans. The weighted LRU
is independently limited to 2,048 entries and 2 MiB of estimated resident
storage. A watermark change cannot reuse an older extent.

Completed individual series enter an in-process weighted LRU keyed by run,
metric, fixed sequence watermark, alignment origin, viewport, and bucket
budget. The independent limits are 512 entries, 250,000 occupied cells, and 32
MiB of estimated resident response storage. A live update changes only that
run's watermark key. A changed shared extent correctly produces a new lattice
key. Cache eviction never affects source storage.

## Consequences

The dashboard can request an overlay with one bounded response, and every
series is directly comparable by bucket index. Source scans and response memory
remain bounded independently of run history length. Adding or refreshing a run
reuses cached series whenever the shared lattice remains the same; if the new
data changes the natural extent, exactness requires rebuilding aggregates on
the new lattice.

The dashboard partitions the selected runs' union metric catalog into stable
groups that obey the series and cell limits. A group is independent of search,
pagination, and visibility timing, and a zoomed chart reuses its full-range
group so relative-step and elapsed-time origins cannot shift during an
interaction. The client instantiates at most 24 charts per page, limits physical
query concurrency to four, evicts offscreen component histories, and keeps a
weighted comparison-response LRU bounded by entry, occupied-cell, and estimated
byte limits.

Relative-step and elapsed-time queries whose requested per-key extents are not
cached need an extent pass before aggregation. Elapsed-time viewports cannot
use step-statistic row group pruning, so they conservatively inspect the
projected timestamp and metric columns.

## Rejected alternatives

### Browser-side alignment of single-run responses

Independent bucket extents do not describe the same x intervals. Resampling
their extrema in the browser can hide or misplace source spikes and multiplies
HTTP fan-out.

### Interpolate missing series values

Interpolation would present measurements that were never logged. Sparse bucket
indexes are an explicit part of the response contract.

### Cache a complete comparison response

A monolithic key invalidates every series when one run advances and duplicates
the same aggregate across different run selections. Per-series keys preserve
reuse while retaining the shared-lattice correctness inputs.
