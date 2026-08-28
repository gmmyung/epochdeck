# ADR 0012: Exact bucket chart history

- Status: Accepted
- Date: 2026-08-28

## Context

Raw history pagination is lossless, but it is not a chart contract. Returning a
fixed selection of raw points can hide spikes, and deriving a rolling envelope
in the browser describes neighboring displayed samples rather than the source
values represented by each pixel bucket. Repeated and nonmonotonic steps also
make append-only updates to an existing aggregate incorrect.

## Decision

Keep `/history` as the raw paged query and add `/chart-history` as a separate,
bounded aggregate. A request repeats percent-encoded metric keys, chooses at
most 2,000 buckets, and may provide an inclusive step viewport. The product of
metric keys and buckets is capped at 5,000 cells.

The server freezes the run sequence watermark, projects only the requested
Parquet columns, discovers the relevant step extent, and aggregates each metric
independently. Every occupied bucket returns its exact minimum and maximum plus
the value, step, and timestamp from the greatest run sequence in that bucket.
Empty buckets are omitted while their bucket indexes remain explicit.

For an explicit step viewport, the reader skips a Parquet row group only when
the step column has exact, current-format minimum and maximum statistics that
prove the group is disjoint. Missing, inexact, deprecated, wrong-type, or
inconsistent statistics conservatively keep the row group in the scan.

The dashboard draws minimum and maximum as the Band envelope and smooths only
the bucket-last center line. Visible charts are loaded lazily with four requests
in flight. Settled pan and zoom viewports request a fresh aggregate; metric
revision changes replace the aggregate rather than appending raw deltas.

## Consequences

Response and dashboard memory remain independent of raw run length, while
single-point spikes remain visible. Zooming increases detail without loading a
complete metric. Aggregate scans take a frozen view of concurrent ingestion and
are cancellable, but an initial request without a viewport needs an extent pass
before its aggregation pass. Viewports still inspect each segment footer, while
their decoded row count falls with the number of overlapping row groups.

## Rejected alternatives

### Browser-generated rolling bands

A rolling min/max window depends on the already sampled response and can imply
variation that does not correspond to a server bucket.

### Append raw deltas to aggregate buckets

A later sequence can target an older step, changing a previously rendered
bucket's minimum, maximum, or last value.

### Replace raw history pagination

Exports and exact API consumers still need lossless ordered rows. Chart
aggregation is an additional read model, not a retention policy.
