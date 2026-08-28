#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use runloom_protocol::{HistoryResponse, IngestBatchRequest, ProjectId, RunId};
use sha2::{Digest, Sha256};
use thiserror::Error;

const SEQUENCE_COLUMN: &str = "__sequence";
const STEP_COLUMN: &str = "__step";
const TIMESTAMP_COLUMN: &str = "__timestamp_ms";
const METRIC_PREFIX: &str = "metric:";
const PARQUET_BATCH_SIZE: usize = 1_024;
static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to create storage directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("storage I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("invalid metric segment: {0}")]
    InvalidSegment(String),
    #[error("invalid blob: {0}")]
    InvalidBlob(String),
    #[error("metric compaction was cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLayout {
    data_dir: PathBuf,
    metrics_dir: PathBuf,
    blobs_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct BlobStore {
    root: Arc<PathBuf>,
}

impl BlobStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
        }
    }

    pub fn ensure(&self) -> Result<(), StorageError> {
        for path in [self.root.as_path(), &self.staging_dir()] {
            std::fs::create_dir_all(path).map_err(|source| StorageError::CreateDirectory {
                path: path.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    }

    pub fn staging_path(&self) -> Result<PathBuf, StorageError> {
        self.ensure()?;
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .staging_dir()
            .join(format!("upload-{}-{sequence}.tmp", std::process::id())))
    }

    pub fn install(&self, staging_path: &Path, digest: &str) -> Result<PathBuf, StorageError> {
        validate_blob_digest(digest)?;
        let final_path = self.path(digest)?;
        let parent = final_path.parent().ok_or_else(|| {
            StorageError::InvalidBlob("blob path has no parent directory".to_owned())
        })?;
        std::fs::create_dir_all(parent).map_err(|source| StorageError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
        match std::fs::hard_link(staging_path, &final_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(StorageError::Io {
                    path: final_path,
                    source,
                });
            }
        }
        std::fs::remove_file(staging_path).map_err(|source| StorageError::Io {
            path: staging_path.to_path_buf(),
            source,
        })?;
        sync_path(&final_path)?;
        sync_path(parent)?;
        Ok(final_path)
    }

    pub fn path(&self, digest: &str) -> Result<PathBuf, StorageError> {
        validate_blob_digest(digest)?;
        Ok(self.root.join("sha256").join(&digest[..2]).join(digest))
    }

    pub fn size(&self, digest: &str) -> Result<Option<u64>, StorageError> {
        let path = self.path(digest)?;
        match std::fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            Ok(_) => Err(StorageError::InvalidBlob(format!(
                "blob path is not a regular file: {}",
                path.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(StorageError::Io { path, source }),
        }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    fn staging_dir(&self) -> PathBuf {
        self.root.join("staging")
    }
}

pub fn validate_blob_digest(digest: &str) -> Result<(), StorageError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StorageError::InvalidBlob(
            "SHA-256 digest must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

impl StorageLayout {
    #[must_use]
    pub fn from_environment() -> Self {
        let data_dir = environment_path("RUNLOOM_DATA_DIR", "./data");
        let metrics_dir = std::env::var_os("RUNLOOM_METRICS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("metrics"));
        let blobs_dir = std::env::var_os("RUNLOOM_BLOBS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("blobs"));
        Self::new(data_dir, metrics_dir, blobs_dir)
    }

    #[must_use]
    pub fn new(
        data_dir: impl Into<PathBuf>,
        metrics_dir: impl Into<PathBuf>,
        blobs_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            metrics_dir: metrics_dir.into(),
            blobs_dir: blobs_dir.into(),
        }
    }

    pub fn ensure(&self) -> Result<(), StorageError> {
        for path in [
            &self.data_dir,
            &self.metrics_dir,
            &self.blobs_dir,
            &self.journal_dir(),
            &self.blob_staging_dir(),
        ] {
            std::fs::create_dir_all(path).map_err(|source| StorageError::CreateDirectory {
                path: path.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    }

    #[must_use]
    pub fn catalog_path(&self) -> PathBuf {
        self.data_dir.join("catalog.sqlite3")
    }

    #[must_use]
    pub fn journal_dir(&self) -> PathBuf {
        self.data_dir.join("journal")
    }

    #[must_use]
    pub fn metrics_dir(&self) -> &Path {
        &self.metrics_dir
    }

    #[must_use]
    pub fn blobs_dir(&self) -> &Path {
        &self.blobs_dir
    }

    #[must_use]
    pub fn blob_staging_dir(&self) -> PathBuf {
        self.blobs_dir.join("staging")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenSegment {
    pub id: String,
    pub signature: String,
    pub relative_path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub row_count: usize,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSource {
    pub relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentTail {
    pub sequence: u64,
    pub step: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionSource {
    pub relative_path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub row_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct Extremum {
    sequence: u64,
    value: f64,
}

#[derive(Debug, Clone)]
struct MetricRange {
    minimum: Option<Extremum>,
    maximum: Option<Extremum>,
}

#[derive(Debug, Clone)]
struct SampleRow {
    step: u64,
    timestamp_ms: i64,
    metrics: Vec<Option<f64>>,
}

#[derive(Debug, Clone)]
struct SampleBucket {
    ranges: Vec<MetricRange>,
    references: BTreeMap<u64, usize>,
    rows: BTreeMap<u64, SampleRow>,
}

impl SampleBucket {
    fn new(metric_count: usize) -> Self {
        Self {
            ranges: vec![
                MetricRange {
                    minimum: None,
                    maximum: None,
                };
                metric_count
            ],
            references: BTreeMap::new(),
            rows: BTreeMap::new(),
        }
    }

    fn observe(&mut self, sequence: u64, step: u64, timestamp_ms: i64, values: &[Option<f64>]) {
        let changed = self.ranges.iter().zip(values).any(|(range, value)| {
            value.is_some_and(|value| {
                range
                    .minimum
                    .is_none_or(|candidate| value < candidate.value)
                    || range
                        .maximum
                        .is_none_or(|candidate| value >= candidate.value)
            })
        });
        if !changed {
            return;
        }
        self.rows.insert(
            sequence,
            SampleRow {
                step,
                timestamp_ms,
                metrics: values.to_vec(),
            },
        );

        for (metric_index, value) in values.iter().copied().enumerate() {
            let Some(value) = value else {
                continue;
            };
            let candidate = Extremum { sequence, value };
            let (replace_minimum, replace_maximum, old_minimum, old_maximum) = {
                let range = &self.ranges[metric_index];
                (
                    range.minimum.is_none_or(|current| value < current.value),
                    range.maximum.is_none_or(|current| value >= current.value),
                    range.minimum.map(|current| current.sequence),
                    range.maximum.map(|current| current.sequence),
                )
            };
            if replace_minimum {
                self.ranges[metric_index].minimum = Some(candidate);
                self.replace_reference(old_minimum, sequence);
            }
            if replace_maximum {
                self.ranges[metric_index].maximum = Some(candidate);
                self.replace_reference(old_maximum, sequence);
            }
        }
    }

    fn replace_reference(&mut self, previous: Option<u64>, current: u64) {
        if previous == Some(current) {
            return;
        }
        if let Some(previous) = previous {
            let remove = self.references.get_mut(&previous).is_some_and(|count| {
                *count -= 1;
                *count == 0
            });
            if remove {
                self.references.remove(&previous);
                self.rows.remove(&previous);
            }
        }
        *self.references.entry(current).or_default() += 1;
    }
}

#[derive(Debug)]
pub struct MinMaxHistorySampler {
    run_id: RunId,
    keys: Vec<String>,
    first_sequence: u64,
    last_sequence: u64,
    source_points: u64,
    buckets: Vec<SampleBucket>,
}

impl MinMaxHistorySampler {
    pub fn new(
        run_id: RunId,
        keys: &[String],
        first_sequence: u64,
        last_sequence: u64,
        max_points: usize,
    ) -> Result<Self, StorageError> {
        if keys.is_empty() || first_sequence > last_sequence {
            return Err(StorageError::InvalidSegment(
                "sampled history requires keys and a valid sequence extent".to_owned(),
            ));
        }
        let candidates_per_bucket = keys.len().checked_mul(2).ok_or_else(|| {
            StorageError::InvalidSegment("sampled history key count overflow".to_owned())
        })?;
        if max_points < candidates_per_bucket {
            return Err(StorageError::InvalidSegment(format!(
                "sampled history needs at least {candidates_per_bucket} output points"
            )));
        }
        let bucket_count = max_points / candidates_per_bucket;
        Ok(Self {
            run_id,
            keys: keys.to_vec(),
            first_sequence,
            last_sequence,
            source_points: 0,
            buckets: (0..bucket_count)
                .map(|_| SampleBucket::new(keys.len()))
                .collect(),
        })
    }

    pub fn read_segments(
        &mut self,
        store: &MetricStore,
        segments: &[SegmentSource],
    ) -> Result<(), StorageError> {
        for segment in segments {
            let path = store.resolve_segment(&segment.relative_path)?;
            read_segment_sampled(&path, self)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn finish(self) -> HistoryResponse {
        let point_count = self.buckets.iter().map(|bucket| bucket.rows.len()).sum();
        let mut sequence = Vec::with_capacity(point_count);
        let mut step = Vec::with_capacity(point_count);
        let mut timestamp_ms = Vec::with_capacity(point_count);
        let mut metric_columns = (0..self.keys.len())
            .map(|_| Vec::with_capacity(point_count))
            .collect::<Vec<_>>();
        for bucket in self.buckets {
            for (row_sequence, row) in bucket.rows {
                sequence.push(row_sequence);
                step.push(row.step);
                timestamp_ms.push(row.timestamp_ms);
                for (column, value) in metric_columns.iter_mut().zip(row.metrics) {
                    column.push(value);
                }
            }
        }
        HistoryResponse {
            run_id: self.run_id,
            sequence,
            step,
            timestamp_ms,
            metrics: self.keys.into_iter().zip(metric_columns).collect(),
            next_after: None,
            sampled: true,
            source_points: Some(self.source_points),
            source_last_sequence: Some(self.last_sequence),
        }
    }

    fn observe(&mut self, sequence: u64, step: u64, timestamp_ms: i64, values: &[Option<f64>]) {
        if sequence < self.first_sequence || sequence > self.last_sequence {
            return;
        }
        if !values.iter().any(Option::is_some) {
            return;
        }
        self.source_points = self.source_points.saturating_add(1);
        let span = u128::from(self.last_sequence - self.first_sequence) + 1;
        let offset = u128::from(sequence - self.first_sequence);
        let bucket_count = self.buckets.len() as u128;
        let bucket_index = usize::try_from((offset * bucket_count) / span)
            .unwrap_or(self.buckets.len() - 1)
            .min(self.buckets.len() - 1);
        self.buckets[bucket_index].observe(sequence, step, timestamp_ms, values);
    }
}

#[derive(Debug, Clone)]
pub struct MetricStore {
    root: PathBuf,
}

impl MetricStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_batch(
        &self,
        project_id: ProjectId,
        run_id: RunId,
        digest: &str,
        request: &IngestBatchRequest,
    ) -> Result<WrittenSegment, StorageError> {
        if digest.len() < 16 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(StorageError::InvalidSegment(
                "segment digest must be at least 16 hexadecimal characters".to_owned(),
            ));
        }
        let first_sequence = request
            .points
            .first()
            .ok_or_else(|| StorageError::InvalidSegment("metric batch is empty".to_owned()))?
            .sequence;
        let last_sequence = request
            .points
            .last()
            .ok_or_else(|| StorageError::InvalidSegment("metric batch is empty".to_owned()))?
            .sequence;
        let metric_keys: BTreeSet<String> = request
            .points
            .iter()
            .flat_map(|point| point.metrics.keys().cloned())
            .collect();
        let signature = schema_signature(&metric_keys);
        let relative_path = segment_directory(project_id, run_id).join(format!(
            "{:020}-{}.parquet",
            request.batch_sequence,
            &digest[..16]
        ));
        let final_path = self.root.join(&relative_path);
        let parent = final_path
            .parent()
            .ok_or_else(|| StorageError::InvalidSegment("segment path has no parent".to_owned()))?;
        std::fs::create_dir_all(parent).map_err(|source| StorageError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;

        if !final_path.exists() {
            let temporary_path = temporary_path(&final_path)?;
            let result = write_parquet(&temporary_path, request, &metric_keys)
                .and_then(|()| install_file(&temporary_path, &final_path));
            let _ = std::fs::remove_file(&temporary_path);
            result?;
        }

        sync_path(&final_path)?;
        sync_path(parent)?;

        let byte_size = std::fs::metadata(&final_path)
            .map_err(|source| StorageError::Io {
                path: final_path.clone(),
                source,
            })?
            .len();
        Ok(WrittenSegment {
            id: format!("{run_id}:{}:{digest}", request.batch_sequence),
            signature,
            relative_path: relative_path.to_string_lossy().into_owned(),
            first_sequence,
            last_sequence,
            row_count: request.points.len(),
            byte_size,
        })
    }

    pub fn read_history(
        &self,
        run_id: RunId,
        segments: &[SegmentSource],
        keys: &[String],
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<HistoryResponse, StorageError> {
        let mut response = HistoryResponse {
            run_id,
            sequence: Vec::with_capacity(limit),
            step: Vec::with_capacity(limit),
            timestamp_ms: Vec::with_capacity(limit),
            metrics: keys
                .iter()
                .cloned()
                .map(|key| (key, Vec::with_capacity(limit)))
                .collect(),
            next_after: None,
            sampled: false,
            source_points: None,
            source_last_sequence: None,
        };

        for segment in segments {
            if response.sequence.len() >= limit {
                break;
            }
            let path = self.resolve_segment(&segment.relative_path)?;
            read_segment(&path, keys, after_sequence, limit, &mut response)?;
        }
        if response.sequence.len() == limit {
            response.next_after = response.sequence.last().copied();
        }
        response.source_last_sequence = response.sequence.last().copied();
        Ok(response)
    }

    pub fn read_sampled_history(
        &self,
        run_id: RunId,
        segments: &[SegmentSource],
        keys: &[String],
        first_sequence: u64,
        last_sequence: u64,
        max_points: usize,
    ) -> Result<HistoryResponse, StorageError> {
        let mut sampler =
            MinMaxHistorySampler::new(run_id, keys, first_sequence, last_sequence, max_points)?;
        sampler.read_segments(self, segments)?;
        Ok(sampler.finish())
    }

    pub fn read_segment_tail(&self, relative_path: &str) -> Result<SegmentTail, StorageError> {
        let path = self.resolve_segment(relative_path)?;
        let file = File::open(&path).map_err(|source| StorageError::Io {
            path: path.clone(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let row_group_index = builder
            .metadata()
            .num_row_groups()
            .checked_sub(1)
            .ok_or_else(|| StorageError::InvalidSegment("metric segment is empty".to_owned()))?;
        let row_count = usize::try_from(builder.metadata().row_group(row_group_index).num_rows())
            .map_err(|_| {
            StorageError::InvalidSegment("metric row group is too large".to_owned())
        })?;
        if row_count == 0 {
            return Err(StorageError::InvalidSegment(
                "metric segment has an empty final row group".to_owned(),
            ));
        }
        let schema = builder.schema();
        let projection = ProjectionMask::roots(
            builder.parquet_schema(),
            vec![
                schema.index_of(SEQUENCE_COLUMN)?,
                schema.index_of(STEP_COLUMN)?,
            ],
        );
        let mut selectors = Vec::with_capacity(2);
        if row_count > 1 {
            selectors.push(RowSelector::skip(row_count - 1));
        }
        selectors.push(RowSelector::select(1));
        let mut reader = builder
            .with_projection(projection)
            .with_row_groups(vec![row_group_index])
            .with_row_selection(RowSelection::from(selectors))
            .with_batch_size(1)
            .build()?;
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| StorageError::InvalidSegment("metric segment is empty".to_owned()))?;
        if batch.num_rows() != 1 {
            return Err(StorageError::InvalidSegment(
                "metric tail selection did not return one row".to_owned(),
            ));
        }
        let sequence = required_array::<UInt64Array>(&batch, SEQUENCE_COLUMN)?;
        let step = required_array::<UInt64Array>(&batch, STEP_COLUMN)?;
        Ok(SegmentTail {
            sequence: sequence.value(0),
            step: step.value(0),
        })
    }

    pub fn compact_segments(
        &self,
        project_id: ProjectId,
        run_id: RunId,
        signature: &str,
        sources: &[CompactionSource],
        cancelled: &AtomicBool,
    ) -> Result<WrittenSegment, StorageError> {
        validate_compaction_sources(sources)?;
        if cancelled.load(Ordering::Relaxed) {
            return Err(StorageError::Cancelled);
        }
        let first_sequence = sources[0].first_sequence;
        let last_sequence = sources[sources.len() - 1].last_sequence;
        let row_count = sources.iter().try_fold(0usize, |total, source| {
            total.checked_add(source.row_count).ok_or_else(|| {
                StorageError::InvalidSegment("compaction row count overflow".to_owned())
            })
        })?;
        let mut hasher = Sha256::new();
        hasher.update(signature.as_bytes());
        for source in sources {
            hasher.update(source.relative_path.as_bytes());
            hasher.update([0]);
        }
        let digest = hex_digest(hasher.finalize().as_slice());
        let relative_path = segment_directory(project_id, run_id).join(format!(
            "compact-{first_sequence:020}-{last_sequence:020}-{}.parquet",
            &digest[..16]
        ));
        let final_path = self.root.join(&relative_path);
        let parent = final_path
            .parent()
            .ok_or_else(|| StorageError::InvalidSegment("segment path has no parent".to_owned()))?;
        std::fs::create_dir_all(parent).map_err(|source| StorageError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;

        if !final_path.exists() {
            let temporary_path = temporary_path(&final_path)?;
            let result = write_compacted_parquet(
                self,
                &temporary_path,
                sources,
                first_sequence,
                last_sequence,
                row_count,
                cancelled,
            )
            .and_then(|()| install_file(&temporary_path, &final_path));
            let _ = std::fs::remove_file(&temporary_path);
            result?;
        }
        if cancelled.load(Ordering::Relaxed) {
            return Err(StorageError::Cancelled);
        }
        sync_path(&final_path)?;
        sync_path(parent)?;
        let byte_size = std::fs::metadata(&final_path)
            .map_err(|source| StorageError::Io {
                path: final_path.clone(),
                source,
            })?
            .len();
        Ok(WrittenSegment {
            id: format!("{run_id}:compact:{first_sequence}:{last_sequence}:{digest}"),
            signature: signature.to_owned(),
            relative_path: relative_path.to_string_lossy().into_owned(),
            first_sequence,
            last_sequence,
            row_count,
            byte_size,
        })
    }

    pub fn remove_segment(&self, relative_path: &str) -> Result<(), StorageError> {
        let path = self.resolve_segment(relative_path)?;
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(StorageError::Io { path, source }),
        }
    }

    fn resolve_segment(&self, relative_path: &str) -> Result<PathBuf, StorageError> {
        let relative_path = Path::new(relative_path);
        if relative_path.is_absolute()
            || relative_path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(StorageError::InvalidSegment(
                "catalog segment path is not a safe relative path".to_owned(),
            ));
        }
        Ok(self.root.join(relative_path))
    }
}

fn write_parquet(
    path: &Path,
    request: &IngestBatchRequest,
    metric_keys: &BTreeSet<String>,
) -> Result<(), StorageError> {
    let mut fields = vec![
        Field::new(SEQUENCE_COLUMN, DataType::UInt64, false),
        Field::new(STEP_COLUMN, DataType::UInt64, false),
        Field::new(TIMESTAMP_COLUMN, DataType::Int64, false),
    ];
    fields.extend(
        metric_keys
            .iter()
            .map(|key| Field::new(metric_column(key), DataType::Float64, true)),
    );
    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            request.points.iter().map(|point| point.sequence),
        )),
        Arc::new(UInt64Array::from_iter_values(
            request.points.iter().map(|point| point.step),
        )),
        Arc::new(Int64Array::from_iter_values(
            request.points.iter().map(|point| point.timestamp_ms),
        )),
    ];
    columns.extend(metric_keys.iter().map(|key| {
        Arc::new(Float64Array::from(
            request
                .points
                .iter()
                .map(|point| point.metrics.get(key).copied())
                .collect::<Vec<_>>(),
        )) as ArrayRef
    }));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let properties = writer_properties();
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn write_compacted_parquet(
    store: &MetricStore,
    path: &Path,
    sources: &[CompactionSource],
    first_sequence: u64,
    last_sequence: u64,
    expected_rows: usize,
    cancelled: &AtomicBool,
) -> Result<(), StorageError> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let mut writer: Option<ArrowWriter<File>> = None;
    let mut expected_schema: Option<Arc<Schema>> = None;
    let mut observed_rows = 0usize;
    let mut previous_sequence = None;
    for source in sources {
        if cancelled.load(Ordering::Relaxed) {
            return Err(StorageError::Cancelled);
        }
        let source_path = store.resolve_segment(&source.relative_path)?;
        let source_file = File::open(&source_path).map_err(|source| StorageError::Io {
            path: source_path.clone(),
            source,
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(source_file)?;
        let schema = Arc::clone(builder.schema());
        if let Some(expected) = &expected_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(StorageError::InvalidSegment(
                    "compaction sources have different Arrow schemas".to_owned(),
                ));
            }
        } else {
            writer = Some(ArrowWriter::try_new(
                file.try_clone().map_err(|source| StorageError::Io {
                    path: path.to_path_buf(),
                    source,
                })?,
                Arc::clone(&schema),
                Some(writer_properties()),
            )?);
            expected_schema = Some(Arc::clone(&schema));
        }
        let reader = builder.with_batch_size(PARQUET_BATCH_SIZE).build()?;
        for batch in reader {
            if cancelled.load(Ordering::Relaxed) {
                return Err(StorageError::Cancelled);
            }
            let batch = batch?;
            let sequence = required_array::<UInt64Array>(&batch, SEQUENCE_COLUMN)?;
            for row_index in 0..batch.num_rows() {
                let current = sequence.value(row_index);
                let expected = previous_sequence
                    .and_then(|previous: u64| previous.checked_add(1))
                    .unwrap_or(first_sequence);
                if current != expected {
                    return Err(StorageError::InvalidSegment(format!(
                        "compaction sequence {current} followed {previous_sequence:?}"
                    )));
                }
                previous_sequence = Some(current);
            }
            observed_rows = observed_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                StorageError::InvalidSegment("compaction row count overflow".to_owned())
            })?;
            writer
                .as_mut()
                .ok_or_else(|| {
                    StorageError::InvalidSegment("compaction has no output writer".to_owned())
                })?
                .write(&batch)?;
        }
    }
    if observed_rows != expected_rows || previous_sequence != Some(last_sequence) {
        return Err(StorageError::InvalidSegment(format!(
            "compaction observed {observed_rows} rows ending at {previous_sequence:?}; expected {expected_rows} rows ending at {last_sequence}"
        )));
    }
    writer
        .ok_or_else(|| StorageError::InvalidSegment("compaction has no sources".to_owned()))?
        .close()?;
    file.sync_all().map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn validate_compaction_sources(sources: &[CompactionSource]) -> Result<(), StorageError> {
    if sources.len() < 2 {
        return Err(StorageError::InvalidSegment(
            "compaction requires at least two source segments".to_owned(),
        ));
    }
    let mut next_sequence = sources[0].first_sequence;
    for source in sources {
        if source.row_count == 0 || source.first_sequence != next_sequence {
            return Err(StorageError::InvalidSegment(
                "compaction sources must be non-empty and adjacent".to_owned(),
            ));
        }
        next_sequence = source
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| StorageError::InvalidSegment("run sequence overflow".to_owned()))?;
    }
    Ok(())
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_row_count(Some(8 * PARQUET_BATCH_SIZE))
        .build()
}

fn install_file(temporary_path: &Path, final_path: &Path) -> Result<(), StorageError> {
    match std::fs::rename(temporary_path, final_path) {
        Ok(()) => Ok(()),
        Err(_) if final_path.exists() => Ok(()),
        Err(source) => Err(StorageError::Io {
            path: final_path.to_path_buf(),
            source,
        }),
    }
}

fn sync_path(path: &Path) -> Result<(), StorageError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn read_segment(
    path: &Path,
    keys: &[String],
    after_sequence: Option<u64>,
    limit: usize,
    response: &mut HistoryResponse,
) -> Result<(), StorageError> {
    let reader = projected_reader(path, keys, PARQUET_BATCH_SIZE.min(limit))?;

    for batch in reader {
        let batch = batch?;
        let sequence = required_array::<UInt64Array>(&batch, SEQUENCE_COLUMN)?;
        let step = required_array::<UInt64Array>(&batch, STEP_COLUMN)?;
        let timestamp = required_array::<Int64Array>(&batch, TIMESTAMP_COLUMN)?;
        for row_index in 0..batch.num_rows() {
            if response.sequence.len() >= limit {
                return Ok(());
            }
            let row_sequence = sequence.value(row_index);
            if after_sequence.is_some_and(|cursor| row_sequence <= cursor) {
                continue;
            }
            response.sequence.push(row_sequence);
            response.step.push(step.value(row_index));
            response.timestamp_ms.push(timestamp.value(row_index));
            for key in keys {
                let value = batch
                    .column_by_name(&metric_column(key))
                    .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
                    .and_then(|column| {
                        (!column.is_null(row_index)).then(|| column.value(row_index))
                    });
                response
                    .metrics
                    .get_mut(key)
                    .ok_or_else(|| {
                        StorageError::InvalidSegment(format!(
                            "history response did not allocate metric {key}"
                        ))
                    })?
                    .push(value);
            }
        }
    }
    Ok(())
}

fn read_segment_sampled(
    path: &Path,
    sampler: &mut MinMaxHistorySampler,
) -> Result<(), StorageError> {
    let reader = projected_reader(path, &sampler.keys, PARQUET_BATCH_SIZE)?;

    for batch in reader {
        let batch = batch?;
        let sequence = required_array::<UInt64Array>(&batch, SEQUENCE_COLUMN)?;
        let step = required_array::<UInt64Array>(&batch, STEP_COLUMN)?;
        let timestamp = required_array::<Int64Array>(&batch, TIMESTAMP_COLUMN)?;
        let metric_columns = sampler
            .keys
            .iter()
            .map(|key| {
                batch
                    .column_by_name(&metric_column(key))
                    .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            })
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(sampler.keys.len());
        for row_index in 0..batch.num_rows() {
            values.clear();
            values.extend(metric_columns.iter().map(|column| {
                column.and_then(|column| {
                    (!column.is_null(row_index)).then(|| column.value(row_index))
                })
            }));
            sampler.observe(
                sequence.value(row_index),
                step.value(row_index),
                timestamp.value(row_index),
                &values,
            );
        }
    }
    Ok(())
}

fn projected_reader(
    path: &Path,
    keys: &[String],
    batch_size: usize,
) -> Result<ParquetRecordBatchReader, StorageError> {
    let file = File::open(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.schema();
    let mut projection_indices = vec![
        schema.index_of(SEQUENCE_COLUMN)?,
        schema.index_of(STEP_COLUMN)?,
        schema.index_of(TIMESTAMP_COLUMN)?,
    ];
    projection_indices.extend(
        keys.iter()
            .filter_map(|key| schema.index_of(&metric_column(key)).ok()),
    );
    projection_indices.sort_unstable();
    projection_indices.dedup();
    let projection = ProjectionMask::roots(builder.parquet_schema(), projection_indices);
    Ok(builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .build()?)
}

fn required_array<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, StorageError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            StorageError::InvalidSegment(format!("segment is missing typed column {name}"))
        })
}

fn schema_signature(metric_keys: &BTreeSet<String>) -> String {
    let mut hasher = Sha256::new();
    for key in metric_keys {
        hasher.update(key.as_bytes());
        hasher.update([0]);
    }
    hex_digest(hasher.finalize().as_slice())
}

fn segment_directory(project_id: ProjectId, run_id: RunId) -> PathBuf {
    PathBuf::from("projects")
        .join(project_id.to_string())
        .join("runs")
        .join(run_id.to_string())
        .join("segments")
}

fn metric_column(key: &str) -> String {
    format!("{METRIC_PREFIX}{key}")
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write;

    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        },
    )
}

fn temporary_path(final_path: &Path) -> Result<PathBuf, StorageError> {
    let file_name = final_path
        .file_name()
        .ok_or_else(|| StorageError::InvalidSegment("segment path has no file name".to_owned()))?;
    let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(final_path.with_file_name(format!(
        ".{}.{}.{sequence}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

fn environment_path(name: &str, default: &str) -> PathBuf {
    std::env::var_os(name)
        .unwrap_or_else(|| OsString::from(default))
        .into()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicBool;

    use runloom_protocol::{IngestBatchRequest, MetricPoint, ProjectId, RunId};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::{
        BlobStore, CompactionSource, MetricStore, MinMaxHistorySampler, SegmentSource,
        StorageLayout,
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
    fn installs_content_addressed_blobs_idempotently() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let store = BlobStore::new(directory.path());
        let digest = format!("{:x}", Sha256::digest(b"blob-content"));
        let first = store.staging_path()?;
        std::fs::write(&first, b"blob-content")?;
        let installed = store.install(&first, &digest)?;
        let replay = store.staging_path()?;
        std::fs::write(&replay, b"blob-content")?;
        assert_eq!(store.install(&replay, &digest)?, installed);

        assert_eq!(store.size(&digest)?, Some(12));
        assert_eq!(std::fs::read(installed)?, b"blob-content");
        assert!(!first.exists());
        assert!(!replay.exists());
        Ok(())
    }

    #[test]
    fn writes_wide_parquet_and_projects_requested_metrics() -> Result<(), Box<dyn std::error::Error>>
    {
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
        let history = store.read_history(
            run_id,
            &[SegmentSource {
                relative_path: segment.relative_path,
            }],
            &["loss".to_owned()],
            None,
            10,
        )?;

        assert_eq!(history.sequence, vec![1, 2]);
        assert_eq!(history.metrics["loss"], vec![Some(2.0), Some(1.0)]);
        assert!(!history.metrics.contains_key("reward"));
        Ok(())
    }

    #[test]
    fn min_max_sampling_preserves_spikes_across_segments() -> Result<(), Box<dyn std::error::Error>>
    {
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

        let history =
            store.read_sampled_history(run_id, &segments, &["loss".to_owned()], 1, 12, 4)?;

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
    fn compacts_adjacent_segments_without_changing_history()
    -> Result<(), Box<dyn std::error::Error>> {
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
        let history = store.read_history(
            run_id,
            &[SegmentSource {
                relative_path: compacted.relative_path.clone(),
            }],
            &["loss".to_owned(), "reward".to_owned()],
            None,
            10,
        )?;
        let page = store.read_history(
            run_id,
            &[SegmentSource {
                relative_path: compacted.relative_path.clone(),
            }],
            &["loss".to_owned()],
            Some(3),
            2,
        )?;

        assert_eq!(compacted.first_sequence, 1);
        assert_eq!(compacted.last_sequence, 6);
        assert_eq!(compacted.row_count, 6);
        assert_eq!(replayed, compacted);
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
    fn multi_metric_sampling_keeps_a_strict_shared_row_budget()
    -> Result<(), Box<dyn std::error::Error>> {
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
}
