use std::collections::BTreeMap;
use std::time::Instant;

use runloom_protocol::{IngestBatchRequest, MetricPoint, ProjectId, RunId};
use runloom_storage::{MetricStore, SegmentSource};
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
    let mut segments = Vec::new();
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
        segments.push(SegmentSource {
            relative_path: segment.relative_path,
        });
    }
    let write_elapsed = started.elapsed();

    let query_started = Instant::now();
    let history = store.read_history(run_id, &segments, &["metric_0".to_owned()], None, 5_000)?;
    let query_elapsed = query_started.elapsed();

    println!(
        "rows={rows} metrics={metric_count} segments={}",
        segments.len()
    );
    println!(
        "write_seconds={:.3} stored_mib={:.2}",
        write_elapsed.as_secs_f64(),
        stored_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "projected_query_seconds={:.3} returned_points={}",
        query_elapsed.as_secs_f64(),
        history.sequence.len()
    );
    Ok(())
}

fn argument(index: usize, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    std::env::args()
        .nth(index)
        .map(|value| value.parse().map_err(Into::into))
        .unwrap_or(Ok(default))
}
