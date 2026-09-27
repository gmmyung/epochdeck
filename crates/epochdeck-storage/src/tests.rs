use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;

use epochdeck_protocol::{IngestBatchRequest, MetricPoint, ProjectId, RunId};
use parquet::file::statistics::{Statistics, ValueStatistics};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

use super::{
    BlobInstallation, BlobStore, ChartAxisExtent, ChartAxisExtentScanner, ChartCoordinate,
    ChartHistorySampler, ChartSamplingSpec, ChartStepExtent, ChartStepExtentScanner,
    CompactionSource, MetricStore, MinMaxHistorySampler, SegmentInstallation, SegmentSource,
    StorageError, StorageLayout, row_group_may_overlap_step_range,
};

#[test]
fn creates_independent_storage_roots() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let layout = StorageLayout::new(
        directory.path().join("data"),
        directory.path().join("metrics"),
        directory.path().join("blobs"),
    );
    layout.ensure()?;

    assert!(
        layout
            .catalog_path()
            .parent()
            .is_some_and(|path| path.exists())
    );
    assert!(layout.metrics_dir().exists());
    assert!(layout.blob_staging_dir().exists());
    Ok(())
}

#[test]
fn accepts_data_as_parent_of_disjoint_metric_and_blob_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let data = directory.path().join("data");
    let layout = StorageLayout::new(&data, data.join("metrics"), data.join("blobs"));
    layout.ensure()?;
    let lock_paths = layout.ownership_lock_paths()?;
    assert_eq!(lock_paths.len(), 3);
    assert!(lock_paths.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(lock_paths.iter().all(|path| {
        path.file_name()
            .is_some_and(|name| name == super::STORAGE_LOCK_FILE_NAME)
    }));
    Ok(())
}

#[test]
fn rejects_overlapping_storage_roots() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let metric_parent = directory.path().join("metric-parent");
    let overlapping = StorageLayout::new(
        directory.path().join("data"),
        &metric_parent,
        metric_parent.join("blobs"),
    );
    assert!(matches!(
        overlapping.ensure(),
        Err(StorageError::InvalidLayout(message))
            if message.contains("metric and blob roots must be disjoint")
    ));

    let metrics = directory.path().join("contains-data");
    let nested_data = StorageLayout::new(
        metrics.join("data"),
        &metrics,
        directory.path().join("separate-blobs"),
    );
    assert!(matches!(
        nested_data.ensure(),
        Err(StorageError::InvalidLayout(message))
            if message.contains("data root may contain the metric root")
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_storage_roots_that_alias_through_symlinks() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempdir()?;
    let shared = directory.path().join("shared");
    std::fs::create_dir(&shared)?;
    let metrics = directory.path().join("metrics-link");
    let blobs = directory.path().join("blobs-link");
    symlink(&shared, &metrics)?;
    symlink(&shared, &blobs)?;
    let layout = StorageLayout::new(directory.path().join("data"), metrics, blobs);
    assert!(matches!(
        layout.ensure(),
        Err(StorageError::InvalidLayout(message))
            if message.contains("metric and blob roots must be disjoint")
    ));
    Ok(())
}

#[test]
fn installs_content_addressed_blobs_idempotently() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = BlobStore::new(directory.path());
    let digest = format!("{:x}", Sha256::digest(b"blob-content"));
    let mut first = store.staging_file()?;
    std::fs::write(first.path(), b"blob-content")?;
    let first_path = first.path().to_path_buf();
    let installed = store.install(first.path(), &digest)?;
    first.disarm();
    let mut replay = store.staging_file()?;
    std::fs::write(replay.path(), b"blob-content")?;
    let replay_path = replay.path().to_path_buf();
    let replayed = store.install(replay.path(), &digest)?;
    replay.disarm();

    assert_eq!(installed.installation, BlobInstallation::InstalledNew);
    assert_eq!(replayed.installation, BlobInstallation::AlreadyPresent);
    assert_eq!(replayed.path, installed.path);
    assert_eq!(store.size(&digest)?, Some(12));
    assert_eq!(std::fs::read(installed.path)?, b"blob-content");
    assert!(!first_path.exists());
    assert!(!replay_path.exists());
    Ok(())
}

#[test]
fn staging_files_are_owned_and_startup_cleanup_removes_orphans()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = BlobStore::new(directory.path());
    let staging = store.staging_file()?;
    let cancelled_path = staging.path().to_path_buf();
    std::fs::write(&cancelled_path, b"partial")?;
    drop(staging);
    assert!(!cancelled_path.exists());

    let mut orphan = store.staging_file()?;
    let orphan_path = orphan.path().to_path_buf();
    std::fs::write(&orphan_path, b"interrupted")?;
    orphan.disarm();
    drop(orphan);
    store.cleanup_staging()?;
    assert!(!orphan_path.exists());
    Ok(())
}

#[test]
fn metric_staging_files_are_owned_and_startup_cleanup_removes_orphans()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let staging = store.staging_file()?;
    let cancelled_path = staging.path().to_path_buf();
    std::fs::write(&cancelled_path, b"partial parquet")?;
    drop(staging);
    assert!(!cancelled_path.exists());

    let mut orphan = store.staging_file()?;
    let orphan_path = orphan.path().to_path_buf();
    std::fs::write(&orphan_path, b"interrupted parquet")?;
    orphan.disarm();
    drop(orphan);
    store.cleanup_staging()?;
    assert!(!orphan_path.exists());
    assert!(directory.path().join("staging").is_dir());
    Ok(())
}

#[test]
fn writes_wide_parquet_and_projects_requested_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let request = IngestBatchRequest {
        batch_sequence: 0,
        points: vec![
            MetricPoint {
                sequence: 1,
                step: 10,
                timestamp_ms: 100,
                metrics: BTreeMap::from([("loss".to_owned(), 2.0), ("reward".to_owned(), 4.0)]),
            },
            MetricPoint {
                sequence: 2,
                step: 11,
                timestamp_ms: 101,
                metrics: BTreeMap::from([("loss".to_owned(), 1.0)]),
            },
        ],
    };
    let segment = store.write_batch(project_id, run_id, &"a".repeat(64), &request)?;
    assert_eq!(segment.installation, SegmentInstallation::InstalledNew);
    let replay = store.write_batch(project_id, run_id, &"a".repeat(64), &request)?;
    assert_eq!(replay.installation, SegmentInstallation::AlreadyPresent);
    let history = store.read_history_cancelable(
        run_id,
        &[SegmentSource {
            relative_path: segment.relative_path,
        }],
        &["loss".to_owned()],
        None,
        10,
        &AtomicBool::new(false),
    )?;

    assert_eq!(history.sequence, vec![1, 2]);
    assert_eq!(history.metrics["loss"], vec![Some(2.0), Some(1.0)]);
    assert!(!history.metrics.contains_key("reward"));
    Ok(())
}

#[test]
fn min_max_sampling_preserves_spikes_across_segments() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let values = [
        0.0, 1.0, 2.0, 100.0, -100.0, 3.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
    ];
    let mut segments = Vec::new();
    for (batch_sequence, chunk) in values.chunks(6).enumerate() {
        let first = batch_sequence * 6;
        let request = IngestBatchRequest {
            batch_sequence: batch_sequence as u64,
            points: chunk
                .iter()
                .enumerate()
                .map(|(offset, value)| {
                    let index = first + offset;
                    MetricPoint {
                        sequence: index as u64 + 1,
                        step: index as u64,
                        timestamp_ms: index as i64,
                        metrics: BTreeMap::from([("loss".to_owned(), *value)]),
                    }
                })
                .collect(),
        };
        let segment = store.write_batch(
            project_id,
            run_id,
            &format!("{batch_sequence:064x}"),
            &request,
        )?;
        segments.push(SegmentSource {
            relative_path: segment.relative_path,
        });
    }

    let mut sampler = MinMaxHistorySampler::new(run_id, &["loss".to_owned()], 1, 12, 4)?;
    sampler.read_segments_cancelable(&store, &segments, &AtomicBool::new(false))?;
    let history = sampler.finish();

    assert_eq!(history.sequence, vec![4, 5, 7, 12]);
    assert_eq!(
        history.metrics["loss"],
        vec![Some(100.0), Some(-100.0), Some(10.0), Some(15.0)]
    );
    assert_eq!(history.source_points, Some(12));
    assert!(history.sampled);
    assert_eq!(history.next_after, None);
    Ok(())
}

#[test]
fn compacts_adjacent_segments_without_changing_history() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let mut written = Vec::new();
    for batch_sequence in 0..3u64 {
        let first_sequence = batch_sequence * 2 + 1;
        let request = IngestBatchRequest {
            batch_sequence,
            points: (0..2)
                .map(|offset| {
                    let sequence = first_sequence + offset;
                    MetricPoint {
                        sequence,
                        step: sequence - 1,
                        timestamp_ms: sequence as i64 * 10,
                        metrics: BTreeMap::from([
                            ("loss".to_owned(), 10.0 - sequence as f64),
                            ("reward".to_owned(), sequence as f64),
                        ]),
                    }
                })
                .collect(),
        };
        written.push(store.write_batch(
            project_id,
            run_id,
            &format!("{batch_sequence:064x}"),
            &request,
        )?);
    }
    let sources = written
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
        &written[0].signature,
        &sources,
        &AtomicBool::new(false),
    )?;
    let replayed = store.compact_segments(
        project_id,
        run_id,
        &written[0].signature,
        &sources,
        &AtomicBool::new(false),
    )?;
    let history = store.read_history_cancelable(
        run_id,
        &[SegmentSource {
            relative_path: compacted.relative_path.clone(),
        }],
        &["loss".to_owned(), "reward".to_owned()],
        None,
        10,
        &AtomicBool::new(false),
    )?;
    let page = store.read_history_cancelable(
        run_id,
        &[SegmentSource {
            relative_path: compacted.relative_path.clone(),
        }],
        &["loss".to_owned()],
        Some(3),
        2,
        &AtomicBool::new(false),
    )?;

    assert_eq!(compacted.first_sequence, 1);
    assert_eq!(compacted.last_sequence, 6);
    assert_eq!(compacted.row_count, 6);
    assert_eq!(compacted.installation, SegmentInstallation::InstalledNew);
    assert_eq!(
        replayed,
        super::WrittenSegment {
            installation: SegmentInstallation::AlreadyPresent,
            ..compacted.clone()
        }
    );
    assert_eq!(
        store.read_segment_tail(&compacted.relative_path)?,
        super::SegmentTail {
            sequence: 6,
            step: 5
        }
    );
    assert_eq!(history.sequence, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(page.sequence, vec![4, 5]);
    assert_eq!(page.next_after, Some(5));
    assert_eq!(
        history.metrics["reward"],
        vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(5.0),
            Some(6.0)
        ]
    );
    Ok(())
}

#[test]
fn reads_the_tail_from_the_final_parquet_row_group() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let mut written = Vec::new();
    for batch_sequence in 0..9_u64 {
        let first_sequence = batch_sequence * 1_024 + 1;
        let request = IngestBatchRequest {
            batch_sequence,
            points: (0..1_024)
                .map(|offset| {
                    let sequence = first_sequence + offset;
                    MetricPoint {
                        sequence,
                        step: sequence * 2,
                        timestamp_ms: sequence as i64,
                        metrics: BTreeMap::from([("loss".to_owned(), sequence as f64)]),
                    }
                })
                .collect(),
        };
        written.push(store.write_batch(
            project_id,
            run_id,
            &format!("{batch_sequence:064x}"),
            &request,
        )?);
    }
    let sources = written
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
        &written[0].signature,
        &sources,
        &AtomicBool::new(false),
    )?;

    assert_eq!(compacted.row_count, 9_216);
    assert_eq!(
        store.read_segment_tail(&compacted.relative_path)?,
        super::SegmentTail {
            sequence: 9_216,
            step: 18_432,
        }
    );
    Ok(())
}

#[test]
fn multi_metric_sampling_keeps_a_strict_shared_row_budget() -> Result<(), Box<dyn std::error::Error>>
{
    let run_id = RunId::new();
    let mut sampler =
        MinMaxHistorySampler::new(run_id, &["a".to_owned(), "b".to_owned()], 1, 5, 4)?;
    for (index, (a, b)) in [
        (0.0, 5.0),
        (-10.0, 4.0),
        (3.0, 100.0),
        (20.0, -100.0),
        (1.0, 0.0),
    ]
    .into_iter()
    .enumerate()
    {
        let sequence = index as u64 + 1;
        sampler.observe(sequence, sequence - 1, index as i64, &[Some(a), Some(b)]);
    }
    let history = sampler.finish();

    assert_eq!(history.sequence, vec![2, 3, 4]);
    assert!(history.sequence.len() <= 4);
    assert_eq!(
        history.metrics["a"],
        vec![Some(-10.0), Some(3.0), Some(20.0)]
    );
    assert_eq!(
        history.metrics["b"],
        vec![Some(4.0), Some(100.0), Some(-100.0)]
    );
    Ok(())
}

#[test]
fn chart_viewport_prunes_only_provably_disjoint_row_groups()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let store = MetricStore::new(directory.path());
    let project_id = ProjectId::new();
    let run_id = RunId::new();
    let mut written = Vec::new();
    for batch_sequence in 0..16_u64 {
        let first_step = batch_sequence * 1_024;
        let request = IngestBatchRequest {
            batch_sequence,
            points: (0..1_024_u64)
                .map(|offset| {
                    let step = first_step + offset;
                    MetricPoint {
                        sequence: step + 1,
                        step,
                        timestamp_ms: step as i64,
                        metrics: BTreeMap::from([("loss".to_owned(), step as f64)]),
                    }
                })
                .collect(),
        };
        written.push(store.write_batch(
            project_id,
            run_id,
            &format!("{batch_sequence:064x}"),
            &request,
        )?);
    }
    let sources = written
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
        &written[0].signature,
        &sources,
        &AtomicBool::new(false),
    )?;
    let mut sampler =
        ChartHistorySampler::new(run_id, &["loss".to_owned()], 1, 16_384, 9_000, 9_005, 6)?;
    sampler.read_segments(
        &store,
        &[SegmentSource {
            relative_path: compacted.relative_path,
        }],
        &AtomicBool::new(false),
    )?;

    assert_eq!(
        sampler.scan_statistics(),
        super::ChartScanStatistics {
            decoded_rows: 8_192,
            row_groups_read: 1,
            row_groups_pruned: 1,
        }
    );
    let response = sampler.finish();
    assert_eq!(response.source_points, 6);
    let expected = (9_000..=9_005)
        .map(|value| value as f64)
        .collect::<Vec<_>>();
    assert_eq!(response.metrics["loss"].minimum, expected);
    assert_eq!(response.metrics["loss"].maximum, expected);
    assert_eq!(response.metrics["loss"].last, expected);
    Ok(())
}

#[test]
fn chart_row_group_pruning_falls_back_for_untrusted_statistics() {
    let exact = Statistics::int64(Some(0), Some(99), None, Some(0), false);
    assert!(!row_group_may_overlap_step_range(Some(&exact), 100, 200));
    assert!(row_group_may_overlap_step_range(Some(&exact), 99, 200));
    assert!(row_group_may_overlap_step_range(None, 100, 200));

    let wrong_type = Statistics::double(Some(0.0), Some(99.0), None, Some(0), false);
    assert!(row_group_may_overlap_step_range(
        Some(&wrong_type),
        100,
        200
    ));
    let deprecated = Statistics::int64(Some(0), Some(99), None, Some(0), true);
    assert!(row_group_may_overlap_step_range(
        Some(&deprecated),
        100,
        200
    ));
    let inexact = Statistics::from(
        ValueStatistics::<i64>::new(Some(0), Some(99), None, Some(0), false)
            .with_max_is_exact(false),
    );
    assert!(row_group_may_overlap_step_range(Some(&inexact), 100, 200));
    let reversed = Statistics::int64(Some(99), Some(0), None, Some(0), false);
    assert!(row_group_may_overlap_step_range(Some(&reversed), 100, 200));
}

#[test]
fn chart_sampling_keeps_exact_sparse_buckets_and_last_sequence_order()
-> Result<(), Box<dyn std::error::Error>> {
    let run_id = RunId::new();
    let keys = ["a".to_owned(), "b".to_owned()];
    let mut extent = ChartStepExtentScanner::new(&keys, 1, 7)?;
    let mut sampler = ChartHistorySampler::new(run_id, &keys, 1, 7, 0, 9, 2)?;
    for (sequence, step, a, b) in [
        (1, 4, Some(10.0), None),
        (2, 1, Some(-5.0), Some(100.0)),
        (3, 4, Some(7.0), None),
        (4, 0, None, Some(50.0)),
        (5, 8, Some(20.0), None),
        (6, 6, None, Some(-1.0)),
        (7, 5, Some(15.0), Some(2.0)),
        (8, 9, Some(999.0), Some(999.0)),
    ] {
        let values = [a, b];
        extent.observe(sequence, step, &values);
        sampler.observe(sequence, step, sequence as i64 * 10, &values);
    }

    assert_eq!(
        extent.finish(),
        Some(ChartStepExtent {
            minimum: 0,
            maximum: 8
        })
    );
    let response = sampler.finish();
    assert_eq!(response.bucket_count, 2);
    assert_eq!(response.source_points, 7);
    assert_eq!(response.metrics["a"].source_points, 5);
    assert_eq!(response.metrics["a"].bucket, vec![0, 1]);
    assert_eq!(response.metrics["a"].last_x, vec![4, 5]);
    assert_eq!(response.metrics["a"].last_step, vec![4, 5]);
    assert_eq!(response.metrics["a"].last_timestamp_ms, vec![30, 70]);
    assert_eq!(response.metrics["a"].minimum, vec![-5.0, 15.0]);
    assert_eq!(response.metrics["a"].maximum, vec![10.0, 20.0]);
    assert_eq!(response.metrics["a"].last, vec![7.0, 15.0]);
    assert_eq!(response.metrics["b"].source_points, 4);
    assert_eq!(response.metrics["b"].bucket, vec![0, 1]);
    assert_eq!(response.metrics["b"].last_x, vec![0, 5]);
    assert_eq!(response.metrics["b"].last_step, vec![0, 5]);
    assert_eq!(response.metrics["b"].last_timestamp_ms, vec![40, 70]);
    assert_eq!(response.metrics["b"].minimum, vec![50.0, -1.0]);
    assert_eq!(response.metrics["b"].maximum, vec![100.0, 2.0]);
    assert_eq!(response.metrics["b"].last, vec![50.0, 2.0]);
    Ok(())
}

#[test]
fn chart_axis_extents_remain_exact_per_metric() -> Result<(), Box<dyn std::error::Error>> {
    let keys = ["early".to_owned(), "late".to_owned(), "missing".to_owned()];
    let mut scanner = ChartAxisExtentScanner::new(&keys, 2, 4)?;
    for (sequence, step, timestamp_ms, values) in [
        (1, 1, 10, [Some(1.0), None, None]),
        (2, 5, 50, [Some(2.0), None, None]),
        (3, 8, 80, [None, Some(3.0), None]),
        (4, 3, 100, [Some(4.0), Some(5.0), None]),
        (5, 20, 200, [None, Some(6.0), None]),
    ] {
        scanner.observe(sequence, step, timestamp_ms, &values);
    }

    let extents = scanner.finish_by_key();
    assert_eq!(
        extents["early"],
        ChartAxisExtent {
            step_minimum: 3,
            step_maximum: 5,
            timestamp_minimum_ms: 50,
            timestamp_maximum_ms: 100,
        }
    );
    assert_eq!(
        extents["late"],
        ChartAxisExtent {
            step_minimum: 3,
            step_maximum: 8,
            timestamp_minimum_ms: 80,
            timestamp_maximum_ms: 100,
        }
    );
    assert!(!extents.contains_key("missing"));
    Ok(())
}

#[test]
fn aligned_chart_sampling_uses_relative_and_elapsed_lattices()
-> Result<(), Box<dyn std::error::Error>> {
    let run_id = RunId::new();
    let keys = ["loss".to_owned()];
    for (coordinate, maximum, expected_x) in [
        (
            ChartCoordinate::RelativeStep { origin: 100 },
            20,
            vec![10, 20],
        ),
        (
            ChartCoordinate::ElapsedTime { origin_ms: 5_000 },
            2_000,
            vec![1_000, 2_000],
        ),
    ] {
        let mut sampler = ChartHistorySampler::new_aligned(
            run_id,
            &keys,
            ChartSamplingSpec {
                first_sequence: 1,
                last_sequence: 3,
                coordinate,
                x_min: 0,
                x_max: maximum,
                max_buckets: 2,
            },
        )?;
        for (sequence, step, timestamp_ms, value) in [
            (1, 100, 5_000, 5.0),
            (2, 110, 6_000, -2.0),
            (3, 120, 7_000, 9.0),
        ] {
            sampler.observe(sequence, step, timestamp_ms, &[Some(value)]);
        }
        let history = sampler.finish();
        assert_eq!(history.metrics["loss"].bucket, vec![0, 1]);
        assert_eq!(history.metrics["loss"].last_x, expected_x);
        assert_eq!(history.metrics["loss"].minimum, vec![-2.0, 9.0]);
        assert_eq!(history.metrics["loss"].maximum, vec![5.0, 9.0]);
        assert_eq!(history.metrics["loss"].last, vec![-2.0, 9.0]);
    }
    Ok(())
}
