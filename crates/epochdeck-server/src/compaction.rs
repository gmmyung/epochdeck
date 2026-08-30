use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use epochdeck_catalog::{Catalog, CatalogError, SegmentManifest};
use epochdeck_storage::{CompactionSource, MetricStore, SegmentInstallation, StorageError};
use thiserror::Error;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, watch};

const DEFAULT_TARGET_ROWS: usize = 16 * 1_024;
const DEFAULT_MAX_INPUT_SEGMENTS: usize = 16;
const DEFAULT_RETIREMENT_BATCH: usize = 64;
const DEFAULT_MAX_CONSECUTIVE_PASSES: usize = 8;
const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
const MAX_TARGET_ROWS: usize = 64 * 1_024;
const MIN_INPUT_SEGMENTS: usize = 4;
const MAX_INPUT_SEGMENTS: usize = 64;
const MAX_RETIREMENT_BATCH: usize = 1_024;
const MAX_CONSECUTIVE_PASSES: usize = 64;

#[derive(Debug, Clone)]
pub struct MetricRuntime {
    store: MetricStore,
    snapshots: Arc<RwLock<()>>,
}

impl MetricRuntime {
    #[must_use]
    pub fn new(store: MetricStore) -> Self {
        Self {
            store,
            snapshots: Arc::new(RwLock::new(())),
        }
    }

    pub(crate) fn store(&self) -> &MetricStore {
        &self.store
    }

    pub(crate) async fn read_snapshot(&self) -> OwnedRwLockReadGuard<()> {
        Arc::clone(&self.snapshots).read_owned().await
    }

    async fn write_snapshot(&self) -> OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.snapshots).write_owned().await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    pub interval: Duration,
    pub target_rows: usize,
    pub max_input_segments: usize,
    pub retirement_batch: usize,
    pub max_consecutive_passes: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_INTERVAL,
            target_rows: DEFAULT_TARGET_ROWS,
            max_input_segments: DEFAULT_MAX_INPUT_SEGMENTS,
            retirement_batch: DEFAULT_RETIREMENT_BATCH,
            max_consecutive_passes: DEFAULT_MAX_CONSECUTIVE_PASSES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionOutcome {
    Idle,
    RetiredFilesRemoved { count: usize },
    SegmentsCompacted { inputs: usize, rows: usize },
}

#[derive(Debug, Error)]
pub(crate) enum CompactionError {
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("compaction worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
    #[error("invalid compaction configuration: {0}")]
    InvalidConfig(String),
}

pub(crate) async fn compact_once(
    catalog: &Catalog,
    metrics: &MetricRuntime,
    config: CompactionConfig,
    cancelled: Arc<AtomicBool>,
) -> Result<CompactionOutcome, CompactionError> {
    validate_config(config)?;
    if cancelled.load(Ordering::Relaxed) {
        return Err(StorageError::Cancelled.into());
    }
    let retired = catalog.retired_segments(config.retirement_batch).await?;
    if !retired.is_empty() {
        let _snapshot = metrics.write_snapshot().await;
        let (removed, failures) = remove_files(metrics, &retired, Arc::clone(&cancelled)).await?;
        if !removed.is_empty() {
            catalog.acknowledge_retired_segments(&removed).await?;
        }
        for failure in failures {
            tracing::warn!(%failure, "retired metric segment could not be removed");
        }
        if !removed.is_empty() {
            return Ok(CompactionOutcome::RetiredFilesRemoved {
                count: removed.len(),
            });
        }
    }
    let Some(candidate) = catalog
        .next_compaction_candidate(config.target_rows, config.max_input_segments)
        .await?
    else {
        return Ok(CompactionOutcome::Idle);
    };
    let input_count = candidate.segments.len();
    let row_count = candidate
        .segments
        .iter()
        .map(|segment| segment.row_count)
        .sum();
    let signature = candidate.segments[0].signature.clone();
    let sources = candidate
        .segments
        .iter()
        .map(|segment| CompactionSource {
            relative_path: segment.relative_path.clone(),
            first_sequence: segment.first_sequence,
            last_sequence: segment.last_sequence,
            row_count: segment.row_count,
        })
        .collect::<Vec<_>>();
    let store = metrics.store().clone();
    let cancelled_for_write = Arc::clone(&cancelled);
    let written = tokio::task::spawn_blocking(move || {
        store.compact_segments(
            candidate.project_id,
            candidate.run_id,
            &signature,
            &sources,
            &cancelled_for_write,
        )
    })
    .await??;
    if cancelled.load(Ordering::Relaxed) {
        cleanup_unregistered_output(catalog, metrics, &written).await;
        return Err(StorageError::Cancelled.into());
    }
    let replacement = SegmentManifest {
        id: written.id.clone(),
        signature: written.signature.clone(),
        relative_path: written.relative_path.clone(),
        first_sequence: written.first_sequence,
        last_sequence: written.last_sequence,
        row_count: written.row_count,
        byte_size: written.byte_size,
    };
    let _snapshot = metrics.write_snapshot().await;
    if cancelled.load(Ordering::Relaxed) {
        cleanup_unregistered_output(catalog, metrics, &written).await;
        return Err(StorageError::Cancelled.into());
    }
    let retired = match catalog
        .replace_compacted_segments(candidate.run_id, &candidate.segments, &replacement)
        .await
    {
        Ok(retired) => retired,
        Err(error) => {
            cleanup_unregistered_output(catalog, metrics, &written).await;
            return Err(error.into());
        }
    };
    let (removed, failures) = remove_files(metrics, &retired, Arc::clone(&cancelled)).await?;
    if !removed.is_empty() {
        catalog.acknowledge_retired_segments(&removed).await?;
    }
    for failure in failures {
        tracing::warn!(%failure, "retired metric segment could not be removed");
    }
    Ok(CompactionOutcome::SegmentsCompacted {
        inputs: input_count,
        rows: row_count,
    })
}

async fn cleanup_unregistered_output(
    catalog: &Catalog,
    metrics: &MetricRuntime,
    written: &epochdeck_storage::WrittenSegment,
) {
    if written.installation != SegmentInstallation::InstalledNew {
        return;
    }
    match catalog
        .segment_path_is_registered(&written.relative_path)
        .await
    {
        Ok(false) => {
            let store = metrics.store().clone();
            let path = written.relative_path.clone();
            match tokio::task::spawn_blocking(move || store.remove_segment(&path)).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "unregistered compaction output could not be removed");
                }
                Err(error) => {
                    tracing::warn!(%error, "compaction output cleanup worker failed");
                }
            }
        }
        Ok(true) => {}
        Err(error) => {
            tracing::warn!(%error, "could not verify compaction output ownership for cleanup");
        }
    }
}

async fn remove_files(
    metrics: &MetricRuntime,
    relative_paths: &[String],
    cancelled: Arc<AtomicBool>,
) -> Result<(Vec<String>, Vec<String>), CompactionError> {
    let store = metrics.store().clone();
    let relative_paths = relative_paths.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut removed = Vec::new();
        let mut failures = Vec::new();
        for path in &relative_paths {
            if cancelled.load(Ordering::Relaxed) {
                return Err(StorageError::Cancelled);
            }
            match store.remove_segment(path) {
                Ok(()) => removed.push(path.clone()),
                Err(error) => failures.push(error.to_string()),
            }
        }
        Ok::<_, StorageError>((removed, failures))
    })
    .await?
    .map_err(Into::into)
}

pub async fn run_compaction_worker(
    catalog: Catalog,
    metrics: MetricRuntime,
    mut shutdown: watch::Receiver<bool>,
    config: CompactionConfig,
) {
    let mut consecutive_passes = 0usize;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_catalog = catalog.clone();
        let task_metrics = metrics.clone();
        let task_cancelled = Arc::clone(&cancelled);
        let mut task = tokio::spawn(async move {
            compact_once(&task_catalog, &task_metrics, config, task_cancelled).await
        });
        let sleep_before_next = tokio::select! {
            result = &mut task => match result {
                Ok(Ok(CompactionOutcome::Idle)) => true,
                Ok(Ok(outcome)) => {
                    tracing::info!(?outcome, "metric compaction pass completed");
                    consecutive_passes = consecutive_passes.saturating_add(1);
                    consecutive_passes >= config.max_consecutive_passes
                }
                Ok(Err(CompactionError::Storage(StorageError::Cancelled))) => return,
                Ok(Err(error)) => {
                    tracing::error!(%error, "metric compaction pass failed");
                    true
                }
                Err(error) => {
                    tracing::error!(%error, "metric compaction task failed");
                    true
                }
            },
            changed = shutdown.changed() => {
                cancelled.store(true, Ordering::Relaxed);
                let _ = task.await;
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
                true
            }
        };
        if !sleep_before_next {
            tokio::task::yield_now().await;
            continue;
        }
        consecutive_passes = 0;
        tokio::select! {
            () = tokio::time::sleep(config.interval) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

fn validate_config(config: CompactionConfig) -> Result<(), CompactionError> {
    if config.interval.is_zero() {
        return Err(CompactionError::InvalidConfig(
            "interval must be positive".to_owned(),
        ));
    }
    if config.target_rows == 0
        || config.target_rows > MAX_TARGET_ROWS
        || config.max_input_segments < MIN_INPUT_SEGMENTS
        || config.max_input_segments > MAX_INPUT_SEGMENTS
        || config.retirement_batch == 0
        || config.retirement_batch > MAX_RETIREMENT_BATCH
        || config.max_consecutive_passes == 0
        || config.max_consecutive_passes > MAX_CONSECUTIVE_PASSES
    {
        return Err(CompactionError::InvalidConfig(format!(
            "target_rows must be 1..={MAX_TARGET_ROWS}, max_input_segments must be \
             {MIN_INPUT_SEGMENTS}..={MAX_INPUT_SEGMENTS}, retirement_batch must be \
             1..={MAX_RETIREMENT_BATCH}, and max_consecutive_passes must be \
             1..={MAX_CONSECUTIVE_PASSES}"
        )));
    }
    Ok(())
}
