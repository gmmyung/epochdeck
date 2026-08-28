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
It also projects the final sequence and step from the last row of the latest
segment, matching the remote-resume workload. Temporary data is removed after
each run.

Reference smoke result on an Apple M5 Pro on 2026-08-28:

```text
rows=200000 metrics=180 segments_before=196 segments_after=13
compaction_seconds=3.708 compacted_mib=97.52
write_seconds=15.348 stored_mib=114.78
sampled_query_seconds=0.032 source_points=200000 returned_points=5000
resume_tail_seconds=0.000 sequence=200000 step=199999
```

This is a development measurement, not a cross-machine performance guarantee.
Future benchmark gates will cover concurrent ingestion/query load, resident
memory, cold-cache reads, and the 2-core/2-GiB deployment target.
