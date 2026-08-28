#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLayout {
    data_dir: PathBuf,
    metrics_dir: PathBuf,
    blobs_dir: PathBuf,
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

#[derive(Debug, Clone)]
pub struct MetricStore {
    root: PathBuf,
}

impl MetricStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
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
        let relative_path = PathBuf::from("projects")
            .join(project_id.to_string())
            .join("runs")
            .join(run_id.to_string())
            .join("segments")
            .join(format!(
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
            let result = write_parquet(&temporary_path, request, &metric_keys).and_then(|()| {
                match std::fs::rename(&temporary_path, &final_path) {
                    Ok(()) => Ok(()),
                    Err(_) if final_path.exists() => Ok(()),
                    Err(source) => Err(StorageError::Io {
                        path: final_path.clone(),
                        source,
                    }),
                }
            });
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
        Ok(response)
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
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
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
    let reader = builder
        .with_projection(projection)
        .with_batch_size(PARQUET_BATCH_SIZE.min(limit))
        .build()?;

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

    use runloom_protocol::{IngestBatchRequest, MetricPoint, ProjectId, RunId};
    use tempfile::tempdir;

    use super::{MetricStore, SegmentSource, StorageLayout};

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
}
