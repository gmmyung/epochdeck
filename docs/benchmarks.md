# Metric storage benchmarks

The storage benchmark reproduces the motivating wide-run shape while keeping
memory bounded to one 1,024-row ingest batch and one bounded query response.

```bash
just benchmark-metrics 200000 180
```

It generates 200,000 rows with 180 numeric metric columns, writes immutable
Zstd-compressed wide Parquet segments, and then requests 5,000 values from one
projected column. Temporary data is removed after each run.

Reference smoke result on an Apple M5 Pro on 2026-08-28:

```text
rows=200000 metrics=180 segments=196
write_seconds=6.660 stored_mib=114.78
projected_query_seconds=0.002 returned_points=5000
```

This is a development measurement, not a cross-machine performance guarantee.
Future benchmark gates will cover concurrent ingestion/query load, resident
memory, compaction, cold-cache reads, and the 2-core/2-GiB deployment target.
