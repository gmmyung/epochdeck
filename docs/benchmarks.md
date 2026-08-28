# Metric storage benchmarks

The storage benchmark reproduces the motivating wide-run shape while keeping
memory bounded to one 1,024-row input batch, one 8,192-row Parquet output group,
and one bounded query response.

```bash
just benchmark-metrics 200000 180
```

It generates 200,000 rows with 180 numeric metric columns, writes immutable
Zstd-compressed wide Parquet segments, compacts them in bounded 16-segment
groups, and then scans one projected column into a 5,000-point min/max budget.
The same workload also exercises the exact chart-history path: one projected
pass discovers the requested metric's step extent, and a second projected pass
computes exact per-bucket minimum, maximum, and last values into a 2,000-bucket
budget. The harness verifies source counts, the bucket cap, and that every last
value remains inside its exact band. It then requests a centered 512-step
viewport and records decoded rows plus selected and pruned row groups. Pruning
is conservative: only exact step statistics can exclude a group.
It also projects the final sequence and step from the last row of the latest
segment, matching the remote-resume workload. Temporary data is removed after
each run.

Reference smoke result on an Apple M5 Pro on 2026-08-28:

```text
rows=200000 metrics=180 segments_before=196 segments_after=13
compaction_seconds=1.926 compacted_mib=97.52
write_seconds=8.501 stored_mib=114.78
sampled_query_seconds=0.016 source_points=200000 returned_points=5000
chart_history_seconds=0.016 extent_seconds=0.008 aggregate_seconds=0.008 source_points=200000 returned_buckets=2000
chart_viewport_seconds=0.002 source_points=512 decoded_rows=8192 row_groups_read=1 row_groups_pruned=24
resume_tail_seconds=0.000 sequence=200000 step=199999
```

This is a development measurement, not a cross-machine performance guarantee.
Future benchmark gates will cover concurrent ingestion/query load, resident
memory, cold-cache reads, and the 2-core/2-GiB deployment target.
