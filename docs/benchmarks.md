# Metric storage benchmarks

The storage benchmark reproduces the motivating wide-run shape while keeping
memory bounded to one 1,024-row input batch, one 8,192-row Parquet output group,
and one bounded query response.

```bash
just benchmark-metrics 200000 180
```

The workload:

- generates 200,000 rows with 180 numeric metrics;
- writes Zstd-compressed wide Parquet segments;
- compacts bounded groups of up to 16 segments;
- samples one projected column into 5,000 points;
- builds exact min/max/last chart buckets;
- measures a 512-step viewport with row-group pruning;
- compares eight columns on a shared relative-step axis; and
- reads the final sequence and step for remote resume.

The comparison result is a cold storage-path measurement. The server separately
caches exact per-key extents by sequence watermark. Temporary data is removed
after every run.

Reference smoke result on an Apple M5 Pro on 2026-08-29:

```text
rows=200000 metrics=180 segments_before=196 segments_after=13
compaction_seconds=1.753 compacted_mib=97.52
write_seconds=8.215 stored_mib=114.78
sampled_query_seconds=0.015 source_points=200000 returned_points=5000
chart_history_seconds=0.015 extent_seconds=0.007 aggregate_seconds=0.008 source_points=200000 returned_buckets=2000
comparison_chart_seconds=0.038 series=8 cells=16000 alignment=relative_step
chart_viewport_seconds=0.002 source_points=512 decoded_rows=8192 row_groups_read=1 row_groups_pruned=24
resume_tail_seconds=0.000 sequence=200000 step=199999
```

This is a development measurement, not a cross-machine performance guarantee.
Future benchmark gates will cover concurrent ingestion/query load, resident
memory, cold-cache reads, and the 2-core/2-GiB deployment target.
