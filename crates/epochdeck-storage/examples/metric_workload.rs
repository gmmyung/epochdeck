use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use epochdeck_protocol::{IngestBatchRequest, MetricPoint, ProjectId, RunId};
use epochdeck_storage::{
    ChartAxisExtentScanner, ChartCoordinate, ChartHistorySampler, ChartSamplingSpec,
    ChartStepExtentScanner, CompactionSource, MetricStore, MinMaxHistorySampler, SegmentSource,
};
use tempfile::tempdir;

const BATCH_SIZE: usize = 1_024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = argument(1, 200_000)?;
    let metric_count = argument(2, 180)?;
    if rows == 0 || metric_count == 0 {
        return Err("rows and metrics must be positive".into());
    }

    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let started = Instant::now();
    let mut written_segments = Vec::new();
    let mut stored_bytes = 0_u64;

    for (batch_sequence, first_row) in (0..rows).step_by(BATCH_SIZE).enumerate() {
        let last_row = (first_row + BATCH_SIZE).min(rows);
        let points = (first_row..last_row)
            .map(|row| MetricPoint {
                sequence: row as u64 + 1,
                step: row as u64,
                timestamp_ms: row as i64,
                metrics: (0..metric_count)
                    .map(|metric| {
                        (
                            format!("metric_{metric}"),
                            (row as f64 * 0.001) + metric as f64,
                        )
                    })
                    .collect::<BTreeMap<_, _>>(),
            })
            .collect();
        let request = IngestBatchRequest {
            batch_sequence: batch_sequence as u64,
            points,
        };
        let digest = format!("{batch_sequence:064x}");
        let segment = store.write_batch(project_id, run_id, &digest, &request)?;
        stored_bytes += segment.byte_size;
        written_segments.push(segment);
    }
    let write_elapsed = started.elapsed();

    let compaction_started = Instant::now();
    let mut segments = Vec::new();
    let mut compacted_bytes = 0_u64;
    for chunk in written_segments.chunks(16) {
        if chunk.len() == 1 {
            compacted_bytes += chunk[0].byte_size;
            segments.push(SegmentSource {
                relative_path: chunk[0].relative_path.clone(),
            });
            continue;
        }
        let sources = chunk
            .iter()
            .map(|segment| CompactionSource {
                relative_path: segment.relative_path.clone(),
                first_sequence: segment.first_sequence,
                last_sequence: segment.last_sequence,
                row_count: segment.row_count,
            })
            .collect::<Vec<_>>();
        let compacted = store.compact_segments(
            project_id,
            run_id,
            &chunk[0].signature,
            &sources,
            &AtomicBool::new(false),
        )?;
        for source in sources {
            store.remove_segment(&source.relative_path)?;
        }
        compacted_bytes += compacted.byte_size;
        segments.push(SegmentSource {
            relative_path: compacted.relative_path,
        });
    }
    let compaction_elapsed = compaction_started.elapsed();

    let query_started = Instant::now();
    let history_keys = ["metric_0".to_owned()];
    let mut history_sampler =
        MinMaxHistorySampler::new(run_id, &history_keys, 1, rows as u64, 5_000)?;
    history_sampler.read_segments_cancelable(&store, &segments, &AtomicBool::new(false))?;
    let history = history_sampler.finish();
    let query_elapsed = query_started.elapsed();

    let chart_keys = ["metric_0".to_owned()];
    let cancelled = AtomicBool::new(false);
    let chart_extent_started = Instant::now();
    let mut chart_extent = ChartStepExtentScanner::new(&chart_keys, 1, rows as u64)?;
    chart_extent.read_segments(&store, &segments, &cancelled)?;
    let chart_extent = chart_extent
        .finish()
        .ok_or("chart benchmark produced no requested metric values")?;
    let chart_extent_elapsed = chart_extent_started.elapsed();
    let chart_aggregate_started = Instant::now();
    let mut chart_sampler = ChartHistorySampler::new(
        run_id,
        &chart_keys,
        1,
        rows as u64,
        chart_extent.minimum,
        chart_extent.maximum,
        2_000,
    )?;
    chart_sampler.read_segments(&store, &segments, &cancelled)?;
    let chart = chart_sampler.finish();
    let chart_aggregate_elapsed = chart_aggregate_started.elapsed();
    let chart_series = &chart.metrics["metric_0"];
    if chart.source_points != rows as u64
        || chart_series.source_points != rows as u64
        || chart_series.bucket.len() > 2_000
        || chart_series
            .minimum
            .iter()
            .zip(&chart_series.last)
            .zip(&chart_series.maximum)
            .any(|((minimum, last), maximum)| minimum > last || last > maximum)
    {
        return Err("chart benchmark aggregate violated its bounded exactness contract".into());
    }

    let comparison_keys = (0..metric_count.min(8))
        .map(|metric| format!("metric_{metric}"))
        .collect::<Vec<_>>();
    let comparison_started = Instant::now();
    let mut comparison_extent = ChartAxisExtentScanner::new(&comparison_keys, 1, rows as u64)?;
    comparison_extent.read_segments(&store, &segments, &cancelled)?;
    let comparison_extent = comparison_extent
        .finish()
        .ok_or("comparison benchmark produced no requested metric values")?;
    let comparison_buckets = 2_000.min(20_000 / comparison_keys.len());
    let mut comparison_sampler = ChartHistorySampler::new_aligned(
        run_id,
        &comparison_keys,
        ChartSamplingSpec {
            first_sequence: 1,
            last_sequence: rows as u64,
            coordinate: ChartCoordinate::RelativeStep {
                origin: comparison_extent.step_minimum,
            },
            x_min: 0,
            x_max: comparison_extent.step_maximum - comparison_extent.step_minimum,
            max_buckets: comparison_buckets,
        },
    )?;
    comparison_sampler.read_segments(&store, &segments, &cancelled)?;
    let comparison = comparison_sampler.finish();
    let comparison_elapsed = comparison_started.elapsed();
    if comparison.metrics.len() != comparison_keys.len()
        || comparison
            .metrics
            .values()
            .any(|series| series.source_points != rows as u64)
        || comparison
            .metrics
            .values()
            .map(|series| series.bucket.len())
            .sum::<usize>()
            > 20_000
    {
        return Err("comparison benchmark violated its series or cell budget".into());
    }

    let viewport_width = rows.min(512) as u64;
    let viewport_min = (rows as u64 - viewport_width) / 2;
    let viewport_max = viewport_min + viewport_width - 1;
    let viewport_started = Instant::now();
    let mut viewport_sampler = ChartHistorySampler::new(
        run_id,
        &chart_keys,
        1,
        rows as u64,
        viewport_min,
        viewport_max,
        viewport_width.min(2_000) as usize,
    )?;
    viewport_sampler.read_segments(&store, &segments, &cancelled)?;
    let viewport_scan = viewport_sampler.scan_statistics();
    let viewport_chart = viewport_sampler.finish();
    let viewport_elapsed = viewport_started.elapsed();
    if viewport_chart.source_points != viewport_width
        || viewport_chart.metrics["metric_0"].source_points != viewport_width
        || viewport_scan.decoded_rows > rows as u64
        || (rows > 16_384
            && (viewport_scan.row_groups_pruned == 0 || viewport_scan.decoded_rows >= rows as u64))
    {
        return Err("chart viewport benchmark did not prune its disjoint row groups".into());
    }
    let resume_started = Instant::now();
    let latest_segment = segments
        .last()
        .ok_or("benchmark produced no metric segments")?;
    let tail = store.read_segment_tail(&latest_segment.relative_path)?;
    let resume_elapsed = resume_started.elapsed();
    if tail.sequence != rows as u64 || tail.step != rows as u64 - 1 {
        return Err("resume tail did not match the final metric point".into());
    }

    println!(
        "rows={rows} metrics={metric_count} segments_before={} segments_after={}",
        written_segments.len(),
        segments.len(),
    );
    println!(
        "compaction_seconds={:.3} compacted_mib={:.2}",
        compaction_elapsed.as_secs_f64(),
        compacted_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "write_seconds={:.3} stored_mib={:.2}",
        write_elapsed.as_secs_f64(),
        stored_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "sampled_query_seconds={:.3} source_points={} returned_points={}",
        query_elapsed.as_secs_f64(),
        history.source_points.unwrap_or_default(),
        history.sequence.len()
    );
    println!(
        "chart_history_seconds={:.3} extent_seconds={:.3} aggregate_seconds={:.3} source_points={} returned_buckets={}",
        (chart_extent_elapsed + chart_aggregate_elapsed).as_secs_f64(),
        chart_extent_elapsed.as_secs_f64(),
        chart_aggregate_elapsed.as_secs_f64(),
        chart.source_points,
        chart_series.bucket.len()
    );
    println!(
        "comparison_chart_seconds={:.3} series={} cells={} alignment=relative_step",
        comparison_elapsed.as_secs_f64(),
        comparison.metrics.len(),
        comparison
            .metrics
            .values()
            .map(|series| series.bucket.len())
            .sum::<usize>()
    );
    println!(
        "chart_viewport_seconds={:.3} source_points={} decoded_rows={} row_groups_read={} row_groups_pruned={}",
        viewport_elapsed.as_secs_f64(),
        viewport_chart.source_points,
        viewport_scan.decoded_rows,
        viewport_scan.row_groups_read,
        viewport_scan.row_groups_pruned,
    );
    println!(
        "resume_tail_seconds={:.3} sequence={} step={}",
        resume_elapsed.as_secs_f64(),
        tail.sequence,
        tail.step
    );
    Ok(())
}

fn argument(index: usize, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    std::env::args()
        .nth(index)
        .map(|value| value.parse().map_err(Into::into))
        .unwrap_or(Ok(default))
}
