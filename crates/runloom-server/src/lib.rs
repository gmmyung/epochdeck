#![forbid(unsafe_code)]

mod compaction;
mod diagnostics;
mod discovery;

pub use compaction::{CompactionConfig, MetricRuntime, run_compaction_worker};
#[cfg(test)]
use compaction::{CompactionError, CompactionOutcome, compact_once};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, RawQuery, State};
#[cfg(feature = "embedded-dashboard")]
use axum::http::Uri;
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post, put};
use axum::{Json, Router};
use futures_util::StreamExt;
use runloom_catalog::{
    BatchRegistration, BatchStatus, Catalog, CatalogError, MAX_SEGMENTS_PER_QUERY, SegmentManifest,
};
use runloom_protocol::{
    AlertId, AlertListResponse, ApiError, ArtifactId, ArtifactLineageResponse,
    ArtifactListResponse, ArtifactRecord, ArtifactRelation, BlobRef, BlobUploadResponse,
    ChartAlignment, ChartHistoryQueryRequest, ChartHistoryQueryResponse, ChartHistoryResponse,
    ChartMetricHistory, ChartRunWatermark, ChartSeriesHistory, ClaimSweepTrialRequest,
    ClaimSweepTrialResponse, CompleteSweepTrialRequest, ConfigUpdateRequest, CreateAlertRequest,
    CreateAlertResponse, CreateArtifactRequest, CreateArtifactResponse, CreateReportRequest,
    CreateReportResponse, CreateRichValueRequest, CreateRichValueResponse, CreateRunRequest,
    CreateRunResponse, CreateSweepRequest, CreateSweepResponse, CreateTraceSpanRequest,
    CreateTraceSpanResponse, DiagnosticsResponse, FinishRunRequest, FinishRunResponse,
    HealthResponse, HeartbeatSweepTrialRequest, HistoryResponse, IngestBatchRequest,
    IngestBatchResponse, MAX_ALERT_TEXT_BYTES, MAX_ALERT_TITLE_BYTES, MAX_ARTIFACT_ENTRIES,
    MAX_ARTIFACT_MANIFEST_BYTES, MAX_BATCH_POINTS, MAX_CHART_BUCKET_CELLS, MAX_CHART_BUCKETS,
    MAX_CHART_QUERY_CELLS, MAX_CHART_QUERY_RUNS, MAX_CHART_QUERY_SERIES, MAX_CONFIG_BYTES,
    MAX_HISTORY_KEYS, MAX_HISTORY_POINTS, MAX_JSON_SAFE_INTEGER, MAX_METRICS_PER_POINT,
    MAX_RICH_KEY_BYTES, MAX_RICH_METADATA_BYTES, MAX_SUMMARY_BYTES, MAX_TRACE_METADATA_BYTES,
    ReportId, ReportLayout, ReportListResponse, ReportPanelKind, ReportRecord, ResumePolicy,
    RichValueId, RichValueKeyListResponse, RichValueKind, RichValueListResponse,
    RunArtifactListResponse, RunId, RunQueryRequest, RunState, RunUpdateResponse,
    SlowRequestRecord, SummaryUpdateRequest, SweepId, SweepListResponse, SweepTrialId,
    SweepTrialListResponse, SweepTrialRecord, SweepTrialState, TraceSpanId, TraceSpanListResponse,
    UpdateReportRequest, UseArtifactRequest,
};
use runloom_storage::{
    BlobInstallation, BlobStore, ChartAxisExtent, ChartAxisExtentScanner, ChartCoordinate,
    ChartHistorySampler, ChartSamplingSpec, ChartStepExtent, ChartStepExtentScanner, MetricStore,
    MinMaxHistorySampler, SegmentInstallation, SegmentSource, SegmentTail, StorageError,
};
#[cfg(feature = "embedded-dashboard")]
use rust_embed::RustEmbed;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore,
    mpsc,
};
use tower::ServiceExt;
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;

use diagnostics::collect_storage_root_diagnostics;
use discovery::{
    get_project, get_run, list_projects, list_runs, metric_keys, query_project_metrics, query_runs,
};

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_PROJECT_NAME_BYTES: usize = 128;
const MAX_RUN_NAME_BYTES: usize = 256;
const MAX_METRIC_KEY_BYTES: usize = 256;
const INGEST_WORKERS: usize = 2;
const BLOB_UPLOAD_WORKERS: usize = 8;
const REQUEST_ADMISSION_LIMIT: usize = 64;
const HEALTH_ADMISSION_LIMIT: usize = 2;
const DOWNLOAD_STREAM_LIMIT: usize = 16;
const MUTATION_LOCKS: usize = 256;
const QUERY_WORKERS: usize = 4;
const ARTIFACT_IO_WORKERS: usize = 4;
const MAX_LIST_ITEMS: usize = 200;
const MAX_MIME_TYPE_BYTES: usize = 256;
const MAX_FILE_NAME_BYTES: usize = 512;
const MAX_ARTIFACT_NAME_BYTES: usize = 128;
const MAX_ARTIFACT_TYPE_BYTES: usize = 64;
const MAX_ARTIFACT_ALIAS_BYTES: usize = 128;
const MAX_ARTIFACT_PATH_BYTES: usize = 1_024;
const MAX_ARTIFACT_DESCRIPTION_BYTES: usize = 64 * 1024;
const ARTIFACT_ZIP_CHUNK_BYTES: usize = 64 * 1024;
const ARTIFACT_ZIP_CHANNEL_CAPACITY: usize = 2;
const MAX_TRACE_ID_BYTES: usize = 128;
const MAX_TRACE_NAME_BYTES: usize = 256;
const MAX_TRACE_SEARCH_BYTES: usize = 256;
const MAX_SWEEP_PARAMETERS: usize = 64;
const MAX_SWEEP_VALUES: usize = 256;
const MAX_SWEEP_RUNS: u64 = 100_000;
const MAX_AGENT_ID_BYTES: usize = 128;
const MAX_REPORT_PANELS: usize = 32;
const MAX_REPORT_METRICS: usize = 8;
const MAX_REPORT_MARKDOWN_BYTES: usize = 64 * 1024;
const DEFAULT_SLOW_REQUEST_MS: u64 = 1_000;
const MAX_RECENT_SLOW_REQUESTS: usize = 64;
const MAX_DIAGNOSTIC_PATH_BYTES: usize = 512;
const DEFAULT_CHART_BUCKETS: usize = 1_000;
const CHART_SERIES_CACHE_MAX_ENTRIES: usize = 512;
const CHART_SERIES_CACHE_MAX_CELLS: usize = 250_000;
const CHART_SERIES_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
const CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES: usize = 2_048;
const CHART_AXIS_EXTENT_CACHE_MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
struct AppState {
    catalog: Catalog,
    metrics: MetricRuntime,
    blobs: BlobStore,
    request_admission: Arc<Semaphore>,
    health_admission: Arc<Semaphore>,
    ingest_permits: Arc<Semaphore>,
    blob_upload_permits: Arc<Semaphore>,
    mutation_locks: Arc<Vec<Arc<AsyncMutex<()>>>>,
    query_permits: Arc<Semaphore>,
    artifact_io_permits: Arc<Semaphore>,
    download_stream_permits: Arc<Semaphore>,
    chart_series_cache: Arc<Mutex<ChartSeriesCache>>,
    chart_axis_extent_cache: Arc<Mutex<ChartAxisExtentCache>>,
    telemetry: Arc<RequestTelemetry>,
}

impl AppState {
    fn new(
        catalog: Catalog,
        metrics: MetricRuntime,
        blobs: BlobStore,
        chart_axis_extent_cache: Arc<Mutex<ChartAxisExtentCache>>,
        telemetry: Arc<RequestTelemetry>,
    ) -> Self {
        Self {
            catalog,
            metrics,
            blobs,
            request_admission: Arc::new(Semaphore::new(REQUEST_ADMISSION_LIMIT)),
            health_admission: Arc::new(Semaphore::new(HEALTH_ADMISSION_LIMIT)),
            ingest_permits: Arc::new(Semaphore::new(INGEST_WORKERS)),
            blob_upload_permits: Arc::new(Semaphore::new(BLOB_UPLOAD_WORKERS)),
            mutation_locks: Arc::new(
                (0..MUTATION_LOCKS)
                    .map(|_| Arc::new(AsyncMutex::new(())))
                    .collect(),
            ),
            query_permits: Arc::new(Semaphore::new(QUERY_WORKERS)),
            artifact_io_permits: Arc::new(Semaphore::new(ARTIFACT_IO_WORKERS)),
            download_stream_permits: Arc::new(Semaphore::new(DOWNLOAD_STREAM_LIMIT)),
            chart_series_cache: Arc::new(Mutex::new(ChartSeriesCache::default())),
            chart_axis_extent_cache,
            telemetry,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChartAxisExtentCacheKey {
    run_id: RunId,
    key: String,
    source_first_sequence: u64,
    source_last_sequence: u64,
}

#[derive(Debug, Clone)]
struct CachedChartAxisExtent {
    extent: Option<ChartAxisExtent>,
    bytes: usize,
    last_used: u64,
}

#[derive(Debug, Default)]
struct ChartAxisExtentCache {
    entries: HashMap<ChartAxisExtentCacheKey, CachedChartAxisExtent>,
    bytes: usize,
    clock: u64,
    #[cfg(test)]
    scans: u64,
}

impl ChartAxisExtentCache {
    fn get(&mut self, key: &ChartAxisExtentCacheKey) -> Option<Option<ChartAxisExtent>> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.last_used = self.clock;
        Some(entry.extent)
    }

    fn insert(&mut self, key: ChartAxisExtentCacheKey, extent: Option<ChartAxisExtent>) {
        let bytes = std::mem::size_of::<ChartAxisExtentCacheKey>()
            .saturating_add(std::mem::size_of::<CachedChartAxisExtent>())
            .saturating_add(key.key.capacity());
        if bytes > CHART_AXIS_EXTENT_CACHE_MAX_BYTES {
            return;
        }
        self.remove(&key);
        self.clock = self.clock.saturating_add(1);
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.insert(
            key,
            CachedChartAxisExtent {
                extent,
                bytes,
                last_used: self.clock,
            },
        );
        while self.entries.len() > CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES
            || self.bytes > CHART_AXIS_EXTENT_CACHE_MAX_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
    }

    fn remove(&mut self, key: &ChartAxisExtentCacheKey) {
        if let Some(removed) = self.entries.remove(key) {
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
    }

    #[cfg(test)]
    fn record_scan(&mut self) {
        self.scans = self.scans.saturating_add(1);
    }

    #[cfg(test)]
    fn scan_count(&self) -> u64 {
        self.scans
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CachedChartOrigin {
    Step,
    RelativeStep(u64),
    ElapsedTime(i64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChartSeriesCacheKey {
    run_id: RunId,
    key: String,
    source_last_sequence: u64,
    alignment: ChartAlignment,
    origin: CachedChartOrigin,
    x_min: u64,
    x_max: u64,
    max_buckets: usize,
}

#[derive(Debug, Clone)]
struct CachedChartSeries {
    history: ChartMetricHistory,
    cells: usize,
    bytes: usize,
    last_used: u64,
}

#[derive(Debug, Default)]
struct ChartSeriesCache {
    entries: HashMap<ChartSeriesCacheKey, CachedChartSeries>,
    cells: usize,
    bytes: usize,
    clock: u64,
}

impl ChartSeriesCache {
    fn get(&mut self, key: &ChartSeriesCacheKey) -> Option<ChartMetricHistory> {
        self.clock = self.clock.saturating_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.last_used = self.clock;
        Some(entry.history.clone())
    }

    fn insert(&mut self, key: ChartSeriesCacheKey, history: ChartMetricHistory) {
        let cells = history.bucket.len();
        let bytes = chart_series_cache_bytes(&key, &history);
        if cells > CHART_SERIES_CACHE_MAX_CELLS || bytes > CHART_SERIES_CACHE_MAX_BYTES {
            return;
        }
        self.remove(&key);
        self.clock = self.clock.saturating_add(1);
        self.cells = self.cells.saturating_add(cells);
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.insert(
            key,
            CachedChartSeries {
                history,
                cells,
                bytes,
                last_used: self.clock,
            },
        );
        while self.entries.len() > CHART_SERIES_CACHE_MAX_ENTRIES
            || self.cells > CHART_SERIES_CACHE_MAX_CELLS
            || self.bytes > CHART_SERIES_CACHE_MAX_BYTES
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
    }

    fn remove(&mut self, key: &ChartSeriesCacheKey) {
        if let Some(removed) = self.entries.remove(key) {
            self.cells = self.cells.saturating_sub(removed.cells);
            self.bytes = self.bytes.saturating_sub(removed.bytes);
        }
    }
}

fn chart_series_cache_bytes(key: &ChartSeriesCacheKey, history: &ChartMetricHistory) -> usize {
    std::mem::size_of::<ChartSeriesCacheKey>()
        .saturating_add(std::mem::size_of::<CachedChartSeries>())
        .saturating_add(key.key.capacity())
        .saturating_add(
            history
                .bucket
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            history
                .last_x
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(
            history
                .last_step
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(
            history
                .last_timestamp_ms
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>()),
        )
        .saturating_add(
            history
                .minimum
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
        .saturating_add(
            history
                .maximum
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
        .saturating_add(
            history
                .last
                .capacity()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
}

#[derive(Debug)]
struct RequestTelemetry {
    started_at: Instant,
    slow_threshold: Duration,
    requests_total: AtomicU64,
    requests_active: AtomicU64,
    requests_rejected_total: AtomicU64,
    server_errors_total: AtomicU64,
    slow_requests_total: AtomicU64,
    history_queries_total: AtomicU64,
    history_query_duration_ms_total: AtomicU64,
    history_query_duration_ms_max: AtomicU64,
    recent_slow_requests: Mutex<VecDeque<SlowRequestRecord>>,
}

impl RequestTelemetry {
    fn from_environment() -> Self {
        let threshold_ms = std::env::var("RUNLOOM_SLOW_REQUEST_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| (1..=60_000).contains(value))
            .unwrap_or(DEFAULT_SLOW_REQUEST_MS);
        Self::new(Duration::from_millis(threshold_ms))
    }

    fn new(slow_threshold: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            slow_threshold,
            requests_total: AtomicU64::new(0),
            requests_active: AtomicU64::new(0),
            requests_rejected_total: AtomicU64::new(0),
            server_errors_total: AtomicU64::new(0),
            slow_requests_total: AtomicU64::new(0),
            history_queries_total: AtomicU64::new(0),
            history_query_duration_ms_total: AtomicU64::new(0),
            history_query_duration_ms_max: AtomicU64::new(0),
            recent_slow_requests: Mutex::new(VecDeque::with_capacity(MAX_RECENT_SLOW_REQUESTS)),
        }
    }
}

pub fn app(catalog: Catalog, metrics: MetricStore) -> Router {
    let blob_root = metrics
        .root()
        .parent()
        .unwrap_or_else(|| metrics.root())
        .join("blobs");
    app_with_runtime_and_blobs(
        catalog,
        MetricRuntime::new(metrics),
        BlobStore::new(blob_root),
    )
}

pub fn app_with_runtime(catalog: Catalog, metrics: MetricRuntime) -> Router {
    let blob_root = metrics
        .store()
        .root()
        .parent()
        .unwrap_or_else(|| metrics.store().root())
        .join("blobs");
    app_with_runtime_and_blobs(catalog, metrics, BlobStore::new(blob_root))
}

pub fn app_with_runtime_and_blobs(
    catalog: Catalog,
    metrics: MetricRuntime,
    blobs: BlobStore,
) -> Router {
    app_with_axis_extent_cache(
        catalog,
        metrics,
        blobs,
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
    )
}

fn app_with_axis_extent_cache(
    catalog: Catalog,
    metrics: MetricRuntime,
    blobs: BlobStore,
    chart_axis_extent_cache: Arc<Mutex<ChartAxisExtentCache>>,
) -> Router {
    let telemetry = Arc::new(RequestTelemetry::from_environment());
    let state = AppState::new(
        catalog,
        metrics,
        blobs,
        chart_axis_extent_cache,
        Arc::clone(&telemetry),
    );
    let admission_state = state.clone();
    let router = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/diagnostics", get(diagnostics))
        .route("/api/v1/projects", get(list_projects))
        .route("/api/v1/projects/{project}", get(get_project))
        .route(
            "/api/v1/projects/{project}/metrics/query",
            post(query_project_metrics),
        )
        .route("/api/v1/query/runs", post(query_runs))
        .route(
            "/api/v1/projects/{project}/sweeps",
            post(create_sweep).get(list_sweeps),
        )
        .route(
            "/api/v1/projects/{project}/reports",
            post(create_report).get(list_reports),
        )
        .route(
            "/api/v1/reports/{report_id}",
            get(get_report).put(update_report).delete(delete_report),
        )
        .route("/api/v1/sweeps/{sweep_id}", get(get_sweep))
        .route("/api/v1/sweeps/{sweep_id}/claim", post(claim_sweep_trial))
        .route("/api/v1/sweeps/{sweep_id}/trials", get(list_sweep_trials))
        .route("/api/v1/sweep-trials/{trial_id}", get(get_sweep_trial))
        .route(
            "/api/v1/sweep-trials/{trial_id}/complete",
            post(complete_sweep_trial),
        )
        .route(
            "/api/v1/sweep-trials/{trial_id}/heartbeat",
            post(heartbeat_sweep_trial),
        )
        .route(
            "/api/v1/projects/{project}/runs",
            post(create_run).get(list_runs),
        )
        .route("/api/v1/runs/{run_id}", get(get_run))
        .route("/api/v1/runs/{run_id}/config", patch(update_config))
        .route("/api/v1/runs/{run_id}/summary", patch(update_summary))
        .route("/api/v1/runs/{run_id}/metrics", get(metric_keys))
        .route("/api/v1/runs/{run_id}/batches", post(ingest_batch))
        .route("/api/v1/runs/{run_id}/finish", post(finish_run))
        .route(
            "/api/v1/runs/{run_id}/alerts",
            post(create_alert).get(list_alerts),
        )
        .route(
            "/api/v1/runs/{run_id}/rich-values",
            post(create_rich_value).get(list_rich_values),
        )
        .route(
            "/api/v1/runs/{run_id}/rich-values/keys",
            get(list_rich_value_keys),
        )
        .route("/api/v1/rich-values/{value_id}", get(get_rich_value))
        .route(
            "/api/v1/runs/{run_id}/artifacts",
            post(create_artifact).get(list_run_artifacts),
        )
        .route("/api/v1/runs/{run_id}/artifacts/use", post(use_artifact))
        .route(
            "/api/v1/projects/{project}/artifacts",
            get(list_project_artifacts),
        )
        .route(
            "/api/v1/projects/{project}/artifacts/{name}/aliases/{alias}",
            get(resolve_artifact),
        )
        .route("/api/v1/artifacts/{artifact_id}", get(get_artifact))
        .route(
            "/api/v1/artifacts/{artifact_id}/download",
            get(download_artifact),
        )
        .route(
            "/api/v1/artifacts/{artifact_id}/lineage",
            get(get_artifact_lineage),
        )
        .route(
            "/api/v1/artifacts/{artifact_id}/files/{*artifact_path}",
            get(get_artifact_file),
        )
        .route(
            "/api/v1/runs/{run_id}/traces",
            post(create_trace_span).get(list_trace_spans),
        )
        .route("/api/v1/traces/{span_id}", get(get_trace_span))
        .route("/api/v1/blobs/{digest}", put(upload_blob).get(get_blob))
        .route("/api/v1/runs/{run_id}/history", get(history))
        .route("/api/v1/runs/{run_id}/chart-history", get(chart_history))
        .route(
            "/api/v1/projects/{project}/chart-history/query",
            post(query_chart_history),
        )
        .with_state(state);
    #[cfg(feature = "embedded-dashboard")]
    let router = router.fallback(embedded_dashboard);
    router
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(
            admission_state,
            admit_api_request,
        ))
        .layer(middleware::from_fn(add_security_headers))
        .layer(middleware::from_fn_with_state(telemetry, record_request))
}

async fn admit_api_request(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if !path.starts_with("/api/v1/") {
        return next.run(request).await;
    }
    let admission = if path == "/api/v1/health" {
        &state.health_admission
    } else {
        &state.request_admission
    };
    let Ok(permit) = Arc::clone(admission).try_acquire_owned() else {
        state
            .telemetry
            .requests_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        return HttpError::busy("server request capacity is exhausted; retry later")
            .into_response();
    };
    retain_response_permit(next.run(request).await, permit)
}

async fn add_security_headers(request: Request<Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response.headers_mut().insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; media-src 'self'; connect-src 'self'",
        ),
    );
    response
}

#[cfg(feature = "embedded-dashboard")]
#[derive(RustEmbed)]
#[folder = "../../web/dist"]
struct DashboardAssets;

#[cfg(feature = "embedded-dashboard")]
async fn embedded_dashboard(uri: Uri) -> Response {
    let requested = uri.path().trim_start_matches('/');
    if requested == "api" || requested.starts_with("api/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let asset_name = if requested.is_empty() {
        "index.html"
    } else {
        requested
    };
    let selected = DashboardAssets::get(asset_name)
        .map(|asset| (asset_name, asset))
        .or_else(|| {
            if asset_name.contains('.') {
                None
            } else {
                DashboardAssets::get("index.html").map(|asset| ("index.html", asset))
            }
        });
    let Some((name, asset)) = selected else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = Body::from(asset.data).into_response();
    let mime = mime_guess::from_path(name).first_or_octet_stream();
    if let Ok(value) = HeaderValue::from_str(mime.as_ref()) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if name == "index.html" {
            "no-cache"
        } else {
            "public, max-age=31536000, immutable"
        }),
    );
    response
}

struct ActiveRequestGuard(Arc<RequestTelemetry>);

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.0.requests_active.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn record_request(
    State(telemetry): State<Arc<RequestTelemetry>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    telemetry.requests_total.fetch_add(1, Ordering::Relaxed);
    telemetry.requests_active.fetch_add(1, Ordering::Relaxed);
    let _active_guard = ActiveRequestGuard(Arc::clone(&telemetry));
    let method = request.method().to_string();
    let path = truncate_diagnostic_path(request.uri().path());
    let history_query = path.ends_with("/history")
        || path.ends_with("/chart-history")
        || path.ends_with("/chart-history/query");
    let started_at = Instant::now();
    let response = next.run(request).await;
    let elapsed = started_at.elapsed();
    let duration_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
    let status = response.status();
    if status.is_server_error() {
        telemetry
            .server_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }
    if history_query {
        telemetry
            .history_queries_total
            .fetch_add(1, Ordering::Relaxed);
        telemetry
            .history_query_duration_ms_total
            .fetch_add(duration_ms, Ordering::Relaxed);
        telemetry
            .history_query_duration_ms_max
            .fetch_max(duration_ms, Ordering::Relaxed);
    }
    if elapsed >= telemetry.slow_threshold {
        telemetry
            .slow_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let record = SlowRequestRecord {
            method: method.clone(),
            path: path.clone(),
            status: status.as_u16(),
            duration_ms,
            timestamp_ms,
        };
        if let Ok(mut recent) = telemetry.recent_slow_requests.lock() {
            if recent.len() == MAX_RECENT_SLOW_REQUESTS {
                recent.pop_front();
            }
            recent.push_back(record);
        }
        tracing::warn!(%method, %path, status = status.as_u16(), duration_ms, "slow request");
    }
    response
}

fn truncate_diagnostic_path(value: &str) -> String {
    if value.len() <= MAX_DIAGNOSTIC_PATH_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_DIAGNOSTIC_PATH_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

async fn diagnostics(
    State(state): State<AppState>,
) -> Result<Json<DiagnosticsResponse>, HttpError> {
    let telemetry = &state.telemetry;
    let recent_slow_requests = telemetry
        .recent_slow_requests
        .lock()
        .map(|recent| recent.iter().cloned().collect())
        .unwrap_or_default();
    let catalog_path = state.catalog.path().to_path_buf();
    let metrics_path = state.metrics.store().root().to_path_buf();
    let blobs_path = state.blobs.root().to_path_buf();
    let storage_roots = tokio::task::spawn_blocking(move || {
        collect_storage_root_diagnostics(&catalog_path, &metrics_path, &blobs_path)
    })
    .await
    .map_err(|error| HttpError::internal(format!("storage diagnostics worker failed: {error}")))?
    .map_err(|error| HttpError::internal(format!("storage diagnostics failed: {error}")))?;
    Ok(Json(DiagnosticsResponse {
        service: "runloom".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        uptime_seconds: telemetry.started_at.elapsed().as_secs(),
        requests_total: telemetry.requests_total.load(Ordering::Relaxed),
        requests_active: telemetry.requests_active.load(Ordering::Relaxed),
        requests_rejected_total: telemetry.requests_rejected_total.load(Ordering::Relaxed),
        server_errors_total: telemetry.server_errors_total.load(Ordering::Relaxed),
        slow_requests_total: telemetry.slow_requests_total.load(Ordering::Relaxed),
        slow_request_threshold_ms: telemetry
            .slow_threshold
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
        history_queries_total: telemetry.history_queries_total.load(Ordering::Relaxed),
        history_query_duration_ms_total: telemetry
            .history_query_duration_ms_total
            .load(Ordering::Relaxed),
        history_query_duration_ms_max: telemetry
            .history_query_duration_ms_max
            .load(Ordering::Relaxed),
        request_admission_limit: REQUEST_ADMISSION_LIMIT,
        request_admission_permits_available: state.request_admission.available_permits(),
        health_admission_limit: HEALTH_ADMISSION_LIMIT,
        health_admission_permits_available: state.health_admission.available_permits(),
        ingest_permits_available: state.ingest_permits.available_permits(),
        blob_upload_permits_available: state.blob_upload_permits.available_permits(),
        artifact_io_permits_available: state.artifact_io_permits.available_permits(),
        download_stream_limit: DOWNLOAD_STREAM_LIMIT,
        download_stream_permits_available: state.download_stream_permits.available_permits(),
        query_permits_available: state.query_permits.available_permits(),
        storage_roots,
        recent_slow_requests,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SweepListQuery {
    before: Option<SweepId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SweepTrialListQuery {
    before: Option<SweepTrialId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportListQuery {
    before: Option<ReportId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn create_sweep(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(mut request): Json<CreateSweepRequest>,
) -> Result<(StatusCode, Json<CreateSweepResponse>), HttpError> {
    let sweep_id = request.id.unwrap_or_default();
    request.id = Some(sweep_id);
    validate_project_name(&project)?;
    validate_sweep(&request)?;
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&sweep_id)]).await;
    let (sweep, duplicate) = state.catalog.create_sweep(&project, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateSweepResponse { sweep, duplicate })))
}

async fn get_sweep(
    State(state): State<AppState>,
    Path(sweep_id): Path<SweepId>,
) -> Result<Json<runloom_protocol::SweepRecord>, HttpError> {
    Ok(Json(state.catalog.get_sweep(sweep_id).await?))
}

async fn list_sweeps(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Query(query): Query<SweepListQuery>,
) -> Result<Json<SweepListResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_list_limit(query.limit)?;
    let mut sweeps = state
        .catalog
        .list_sweeps(&project, query.before, page_limit(query.limit))
        .await?;
    let has_more = sweeps.len() > query.limit;
    sweeps.truncate(query.limit);
    let next_before = has_more
        .then(|| sweeps.last().map(|sweep| sweep.id))
        .flatten();
    Ok(Json(SweepListResponse {
        sweeps,
        next_before,
    }))
}

async fn claim_sweep_trial(
    State(state): State<AppState>,
    Path(sweep_id): Path<SweepId>,
    Json(request): Json<ClaimSweepTrialRequest>,
) -> Result<Json<ClaimSweepTrialResponse>, HttpError> {
    validate_agent_id(&request.agent_id)?;
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&sweep_id)]).await;
    let (sweep, trial) = state
        .catalog
        .claim_sweep_trial(sweep_id, &request.agent_id)
        .await?;
    Ok(Json(ClaimSweepTrialResponse { sweep, trial }))
}

async fn list_sweep_trials(
    State(state): State<AppState>,
    Path(sweep_id): Path<SweepId>,
    Query(query): Query<SweepTrialListQuery>,
) -> Result<Json<SweepTrialListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let mut trials = state
        .catalog
        .list_sweep_trials(sweep_id, query.before, page_limit(query.limit))
        .await?;
    let has_more = trials.len() > query.limit;
    trials.truncate(query.limit);
    let next_before = has_more
        .then(|| trials.last().map(|trial| trial.id))
        .flatten();
    Ok(Json(SweepTrialListResponse {
        trials,
        next_before,
    }))
}

async fn get_sweep_trial(
    State(state): State<AppState>,
    Path(trial_id): Path<SweepTrialId>,
) -> Result<Json<SweepTrialRecord>, HttpError> {
    Ok(Json(state.catalog.get_sweep_trial(trial_id).await?))
}

async fn complete_sweep_trial(
    State(state): State<AppState>,
    Path(trial_id): Path<SweepTrialId>,
    Json(request): Json<CompleteSweepTrialRequest>,
) -> Result<Json<SweepTrialRecord>, HttpError> {
    validate_agent_id(&request.agent_id)?;
    if !matches!(
        request.state,
        SweepTrialState::Completed | SweepTrialState::Failed | SweepTrialState::Stopped
    ) {
        return Err(HttpError::invalid(
            "completed sweep trial state must be completed, failed, or stopped",
        ));
    }
    if request.metric.is_some_and(|metric| !metric.is_finite()) {
        return Err(HttpError::invalid("sweep trial metric must be finite"));
    }
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&trial_id)]).await;
    Ok(Json(
        state
            .catalog
            .complete_sweep_trial(trial_id, &request)
            .await?,
    ))
}

async fn heartbeat_sweep_trial(
    State(state): State<AppState>,
    Path(trial_id): Path<SweepTrialId>,
    Json(request): Json<HeartbeatSweepTrialRequest>,
) -> Result<Json<SweepTrialRecord>, HttpError> {
    validate_agent_id(&request.agent_id)?;
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&trial_id)]).await;
    Ok(Json(
        state
            .catalog
            .heartbeat_sweep_trial(trial_id, &request.agent_id)
            .await?,
    ))
}

async fn create_report(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(mut request): Json<CreateReportRequest>,
) -> Result<(StatusCode, Json<CreateReportResponse>), HttpError> {
    let report_id = request.id.unwrap_or_default();
    request.id = Some(report_id);
    validate_project_name(&project)?;
    validate_report(
        &request.name,
        request.description.as_deref(),
        &request.layout,
    )?;
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&report_id)]).await;
    let (report, duplicate) = state.catalog.create_report(&project, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateReportResponse { report, duplicate })))
}

async fn list_reports(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Query(query): Query<ReportListQuery>,
) -> Result<Json<ReportListResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_list_limit(query.limit)?;
    let mut reports = state
        .catalog
        .list_reports(&project, query.before, page_limit(query.limit))
        .await?;
    let has_more = reports.len() > query.limit;
    reports.truncate(query.limit);
    let next_before = has_more
        .then(|| reports.last().map(|report| report.id))
        .flatten();
    Ok(Json(ReportListResponse {
        reports,
        next_before,
    }))
}

async fn get_report(
    State(state): State<AppState>,
    Path(report_id): Path<ReportId>,
) -> Result<Json<ReportRecord>, HttpError> {
    Ok(Json(state.catalog.get_report(report_id).await?))
}

async fn update_report(
    State(state): State<AppState>,
    Path(report_id): Path<ReportId>,
    Json(request): Json<UpdateReportRequest>,
) -> Result<Json<ReportRecord>, HttpError> {
    validate_report(
        &request.name,
        request.description.as_deref(),
        &request.layout,
    )?;
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&report_id)]).await;
    Ok(Json(
        state.catalog.update_report(report_id, &request).await?,
    ))
}

async fn delete_report(
    State(state): State<AppState>,
    Path(report_id): Path<ReportId>,
) -> Result<Json<ReportRecord>, HttpError> {
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&report_id)]).await;
    Ok(Json(state.catalog.delete_report(report_id).await?))
}

async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let version = env!("CARGO_PKG_VERSION");
    let catalog_healthy = match state.catalog.health_check().await {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(%error, "catalog health check failed");
            false
        }
    };
    let metrics = state.metrics.store().clone();
    let blobs = state.blobs.clone();
    let storage_healthy = match tokio::task::spawn_blocking(move || {
        metrics.health_check()?;
        blobs.health_check()
    })
    .await
    {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(%error, "storage root health check failed");
            false
        }
        Err(error) => {
            tracing::error!(%error, "storage root health worker failed");
            false
        }
    };
    if catalog_healthy && storage_healthy {
        (StatusCode::OK, Json(HealthResponse::healthy(version)))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse::unhealthy(version)),
        )
    }
}

async fn create_run(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(mut request): Json<CreateRunRequest>,
) -> Result<(StatusCode, Json<CreateRunResponse>), HttpError> {
    let run_id = request.id.unwrap_or_default();
    request.id = Some(run_id);
    validate_project_name(&project)?;
    validate_create_run(&request)?;
    let mut mutation_indices = vec![mutation_lock_index(&run_id)];
    if let Some(trial_id) = request.sweep_trial_id {
        mutation_indices.push(mutation_lock_index(&trial_id));
    }
    let _mutation = acquire_mutation_locks(&state, mutation_indices).await;
    let (run, resumed) = state
        .catalog
        .create_or_resume_run(&project, &request)
        .await?;
    let status = if resumed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let (next_sequence, next_step) = if resumed {
        next_run_position(&state, run.id).await?
    } else {
        (1, 0)
    };
    Ok((
        status,
        Json(CreateRunResponse {
            run,
            resumed,
            next_sequence,
            next_step,
        }),
    ))
}

async fn next_run_position(state: &AppState, run_id: RunId) -> Result<(u64, u64), HttpError> {
    let _permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let _snapshot = state.metrics.read_snapshot().await;
    let rich_next_step = state.catalog.rich_value_next_step(run_id).await?;
    let Some(segment) = state.catalog.last_segment(run_id).await? else {
        return Ok((1, rich_next_step));
    };
    let metrics = state.metrics.store().clone();
    let tail =
        tokio::task::spawn_blocking(move || metrics.read_segment_tail(&segment.relative_path))
            .await
            .map_err(|error| {
                HttpError::internal(format!("resume query worker failed: {error}"))
            })??;
    let (next_sequence, metric_next_step) = next_position(tail)?;
    Ok((next_sequence, metric_next_step.max(rich_next_step)))
}

fn next_position(tail: SegmentTail) -> Result<(u64, u64), HttpError> {
    let next_sequence = tail
        .sequence
        .checked_add(1)
        .ok_or_else(|| HttpError::internal("run sequence overflow"))?;
    let next_step = tail
        .step
        .checked_add(1)
        .ok_or_else(|| HttpError::internal("run step overflow"))?;
    Ok((next_sequence, next_step))
}

async fn update_config(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<ConfigUpdateRequest>,
) -> Result<Json<RunUpdateResponse>, HttpError> {
    validate_document_updates(&request.updates, "config", MAX_CONFIG_BYTES)?;
    let _run_mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&run_id)]).await;
    let run = state
        .catalog
        .update_config(run_id, &request.updates, request.allow_val_change)
        .await?;
    Ok(Json(RunUpdateResponse { run }))
}

async fn update_summary(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<SummaryUpdateRequest>,
) -> Result<Json<RunUpdateResponse>, HttpError> {
    validate_document_updates(&request.updates, "summary", MAX_SUMMARY_BYTES)?;
    let _run_mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&run_id)]).await;
    let run = state
        .catalog
        .update_summary(run_id, &request.updates)
        .await?;
    Ok(Json(RunUpdateResponse { run }))
}

async fn ingest_batch(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<IngestBatchRequest>,
) -> Result<(StatusCode, Json<IngestBatchResponse>), HttpError> {
    validate_batch(&request)?;
    let run_mutation = Arc::clone(&state.mutation_locks[mutation_lock_index(&run_id)])
        .lock_owned()
        .await;
    tokio::spawn(process_ingest_batch(state, run_id, request, run_mutation))
        .await
        .map_err(|error| HttpError::internal(format!("ingestion task failed: {error}")))?
}

async fn process_ingest_batch(
    state: AppState,
    run_id: RunId,
    request: IngestBatchRequest,
    _run_mutation: OwnedMutexGuard<()>,
) -> Result<(StatusCode, Json<IngestBatchResponse>), HttpError> {
    let digest = batch_digest(&request)?;
    if let BatchStatus::Duplicate { metric_revision } = state
        .catalog
        .batch_status(run_id, request.batch_sequence, &digest)
        .await?
    {
        let latest = latest_metrics(&request);
        let stop_requested = if let Some(point) = request.points.last() {
            state
                .catalog
                .observe_sweep_metric(run_id, point.step, &latest)
                .await?
        } else {
            false
        };
        return Ok((
            StatusCode::OK,
            Json(IngestBatchResponse {
                run_id,
                batch_sequence: request.batch_sequence,
                accepted_points: request.points.len(),
                duplicate: true,
                metric_revision,
                stop_requested,
            }),
        ));
    }

    let location = state.catalog.run_location(run_id).await?;
    if location.state != RunState::Running {
        return Err(HttpError::conflict(
            "run_finished",
            "metrics cannot be appended to a finished run",
        ));
    }
    let summary = latest_metrics(&request);
    let observation_step = request.points.last().map(|point| point.step);
    let batch_sequence = request.batch_sequence;
    let accepted_points = request.points.len();
    let metrics = state.metrics.store().clone();
    let digest_for_write = digest.clone();
    let permit = Arc::clone(&state.ingest_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("ingestion worker pool is unavailable"))?;
    let written = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        metrics.write_batch(location.project_id, run_id, &digest_for_write, &request)
    })
    .await
    .map_err(|error| HttpError::internal(format!("ingestion worker failed: {error}")))??;
    let manifest = SegmentManifest {
        id: written.id,
        signature: written.signature,
        relative_path: written.relative_path.clone(),
        first_sequence: written.first_sequence,
        last_sequence: written.last_sequence,
        row_count: written.row_count,
        byte_size: written.byte_size,
    };

    let registration = state
        .catalog
        .register_batch(run_id, batch_sequence, &digest, &manifest, &summary)
        .await;
    let (status, duplicate, metric_revision) = match registration {
        Ok(BatchRegistration::Accepted { metric_revision }) => {
            (StatusCode::CREATED, false, metric_revision)
        }
        Ok(BatchRegistration::Duplicate { metric_revision }) => {
            (StatusCode::OK, true, metric_revision)
        }
        Err(error) => {
            if written.installation == SegmentInstallation::InstalledNew {
                match state
                    .catalog
                    .segment_path_is_registered(&manifest.relative_path)
                    .await
                {
                    Ok(false) => {
                        if let Err(cleanup_error) = state
                            .metrics
                            .store()
                            .remove_segment(&manifest.relative_path)
                        {
                            tracing::error!(%cleanup_error, "failed to clean up unregistered metric segment");
                        }
                    }
                    Ok(true) => {}
                    Err(verification_error) => {
                        tracing::warn!(%verification_error, "could not verify metric segment ownership for cleanup");
                    }
                }
            }
            return Err(error.into());
        }
    };
    let stop_requested = if let Some(step) = observation_step {
        state
            .catalog
            .observe_sweep_metric(run_id, step, &summary)
            .await?
    } else {
        false
    };
    Ok((
        status,
        Json(IngestBatchResponse {
            run_id,
            batch_sequence,
            accepted_points,
            duplicate,
            metric_revision,
            stop_requested,
        }),
    ))
}

async fn finish_run(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<FinishRunRequest>,
) -> Result<Json<FinishRunResponse>, HttpError> {
    validate_document_size(&request.summary, "summary", MAX_SUMMARY_BYTES)?;
    let _run_mutation = Arc::clone(&state.mutation_locks[mutation_lock_index(&run_id)])
        .lock_owned()
        .await;
    let run = state.catalog.finish_run(run_id, &request.summary).await?;
    Ok(Json(FinishRunResponse { run }))
}

fn mutation_lock_index(value: &impl Hash) -> usize {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() as usize) % MUTATION_LOCKS
}

async fn acquire_mutation_locks(
    state: &AppState,
    mut indices: Vec<usize>,
) -> Vec<OwnedMutexGuard<()>> {
    indices.sort_unstable();
    indices.dedup();
    let mut guards = Vec::with_capacity(indices.len());
    for index in indices {
        guards.push(Arc::clone(&state.mutation_locks[index]).lock_owned().await);
    }
    guards
}

async fn create_alert(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(mut request): Json<CreateAlertRequest>,
) -> Result<(StatusCode, Json<CreateAlertResponse>), HttpError> {
    let alert_id = request.id.unwrap_or_default();
    request.id = Some(alert_id);
    validate_alert(&request)?;
    let _mutation = acquire_mutation_locks(
        &state,
        vec![mutation_lock_index(&run_id), mutation_lock_index(&alert_id)],
    )
    .await;
    let (alert, duplicate) = state.catalog.create_alert(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateAlertResponse { alert, duplicate })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertListQuery {
    before: Option<AlertId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn list_alerts(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<AlertListQuery>,
) -> Result<Json<AlertListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let mut alerts = state
        .catalog
        .list_alerts(run_id, query.before, page_limit(query.limit))
        .await?;
    let has_more = alerts.len() > query.limit;
    alerts.truncate(query.limit);
    let next_before = has_more
        .then(|| alerts.last().map(|alert| alert.id))
        .flatten();
    Ok(Json(AlertListResponse {
        alerts,
        next_before,
    }))
}

async fn create_rich_value(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(mut request): Json<CreateRichValueRequest>,
) -> Result<(StatusCode, Json<CreateRichValueResponse>), HttpError> {
    let value_id = request.id.unwrap_or_default();
    request.id = Some(value_id);
    validate_rich_value(&request)?;
    if let Some(blob) = &request.blob {
        let actual_size = state
            .blobs
            .size(&blob.digest)
            .map_err(|error| HttpError::invalid(error.to_string()))?
            .ok_or_else(|| HttpError::invalid("rich value blob has not been uploaded"))?;
        if actual_size != blob.size {
            return Err(HttpError::invalid(
                "rich value blob size does not match uploaded content",
            ));
        }
    }
    let _mutation = acquire_mutation_locks(
        &state,
        vec![mutation_lock_index(&run_id), mutation_lock_index(&value_id)],
    )
    .await;
    let (value, duplicate) = state.catalog.create_rich_value(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateRichValueResponse { value, duplicate })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RichValueListQuery {
    key: String,
    before: Option<RichValueId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RichValueKeyListQuery {
    after: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn list_rich_value_keys(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<RichValueKeyListQuery>,
) -> Result<Json<RichValueKeyListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    if let Some(after) = &query.after {
        validate_rich_key(after)?;
    }
    let mut keys = state
        .catalog
        .list_rich_value_keys(run_id, query.after.as_deref(), page_limit(query.limit))
        .await?;
    let has_more = keys.len() > query.limit;
    keys.truncate(query.limit);
    let next_after = has_more
        .then(|| keys.last().map(|summary| summary.key.clone()))
        .flatten();
    Ok(Json(RichValueKeyListResponse { keys, next_after }))
}

async fn list_rich_values(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<RichValueListQuery>,
) -> Result<Json<RichValueListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    validate_rich_key(&query.key)?;
    let mut values = state
        .catalog
        .list_rich_values(run_id, &query.key, query.before, page_limit(query.limit))
        .await?;
    let has_more = values.len() > query.limit;
    values.truncate(query.limit);
    let next_before = has_more
        .then(|| values.last().map(|value| value.id))
        .flatten();
    Ok(Json(RichValueListResponse {
        values,
        next_before,
    }))
}

async fn get_rich_value(
    State(state): State<AppState>,
    Path(value_id): Path<RichValueId>,
) -> Result<Json<runloom_protocol::RichValueRecord>, HttpError> {
    Ok(Json(state.catalog.get_rich_value(value_id).await?))
}

async fn create_artifact(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(mut request): Json<CreateArtifactRequest>,
) -> Result<(StatusCode, Json<CreateArtifactResponse>), HttpError> {
    let artifact_id = request.id.unwrap_or_default();
    request.id = Some(artifact_id);
    validate_artifact(&request)?;
    let permit = Arc::clone(&state.artifact_io_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("artifact I/O worker pool is unavailable"))?;
    let entries = request.entries.clone();
    let blobs = state.blobs.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        verify_artifact_blobs(&blobs, &entries)
    })
    .await
    .map_err(|error| {
        HttpError::internal(format!("artifact verification worker failed: {error}"))
    })??;
    let _mutation = acquire_mutation_locks(
        &state,
        vec![
            mutation_lock_index(&run_id),
            mutation_lock_index(&artifact_id),
            mutation_lock_index(&request.name),
        ],
    )
    .await;
    let (artifact, duplicate) = state.catalog.create_artifact(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((
        status,
        Json(CreateArtifactResponse {
            artifact,
            duplicate,
        }),
    ))
}

async fn use_artifact(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<UseArtifactRequest>,
) -> Result<Json<ArtifactRecord>, HttpError> {
    let _mutation = acquire_mutation_locks(
        &state,
        vec![
            mutation_lock_index(&run_id),
            mutation_lock_index(&request.artifact_id),
        ],
    )
    .await;
    Ok(Json(
        state
            .catalog
            .use_artifact(run_id, request.artifact_id)
            .await?,
    ))
}

async fn get_artifact(
    State(state): State<AppState>,
    Path(artifact_id): Path<ArtifactId>,
) -> Result<Json<ArtifactRecord>, HttpError> {
    Ok(Json(state.catalog.get_artifact(artifact_id).await?))
}

async fn resolve_artifact(
    State(state): State<AppState>,
    Path((project, name, alias)): Path<(String, String, String)>,
) -> Result<Json<ArtifactRecord>, HttpError> {
    validate_project_name(&project)?;
    validate_artifact_component(&name, "artifact name", MAX_ARTIFACT_NAME_BYTES)?;
    validate_artifact_component(&alias, "artifact alias", MAX_ARTIFACT_ALIAS_BYTES)?;
    Ok(Json(
        state
            .catalog
            .resolve_artifact(&project, &name, &alias)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactListQuery {
    before: Option<ArtifactId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunArtifactListQuery {
    before: Option<ArtifactId>,
    before_relation: Option<ArtifactRelation>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn list_project_artifacts(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Query(query): Query<ArtifactListQuery>,
) -> Result<Json<ArtifactListResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_list_limit(query.limit)?;
    let mut artifacts = state
        .catalog
        .list_project_artifacts(&project, query.before, page_limit(query.limit))
        .await?;
    let has_more = artifacts.len() > query.limit;
    artifacts.truncate(query.limit);
    let next_before = has_more
        .then(|| artifacts.last().map(|artifact| artifact.id))
        .flatten();
    Ok(Json(ArtifactListResponse {
        artifacts,
        next_before,
    }))
}

async fn list_run_artifacts(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<RunArtifactListQuery>,
) -> Result<Json<RunArtifactListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    if query.before.is_some() != query.before_relation.is_some() {
        return Err(HttpError::invalid(
            "run artifact cursors require both 'before' and 'before_relation'",
        ));
    }
    let mut artifacts = state
        .catalog
        .list_run_artifacts(
            run_id,
            query.before,
            query.before_relation,
            page_limit(query.limit),
        )
        .await?;
    let has_more = artifacts.len() > query.limit;
    artifacts.truncate(query.limit);
    let next_before = has_more
        .then(|| artifacts.last().map(|linked| linked.artifact.id))
        .flatten();
    let next_before_relation = has_more
        .then(|| artifacts.last().map(|linked| linked.relation))
        .flatten();
    Ok(Json(RunArtifactListResponse {
        artifacts,
        next_before,
        next_before_relation,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactLineageQuery {
    relation: ArtifactRelation,
    before: Option<RunId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn get_artifact_lineage(
    State(state): State<AppState>,
    Path(artifact_id): Path<ArtifactId>,
    Query(query): Query<ArtifactLineageQuery>,
) -> Result<Json<ArtifactLineageResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let mut runs = state
        .catalog
        .artifact_lineage(
            artifact_id,
            query.relation,
            query.before,
            page_limit(query.limit),
        )
        .await?;
    let has_more = runs.len() > query.limit;
    runs.truncate(query.limit);
    let next_before = has_more.then(|| runs.last().map(|run| run.id)).flatten();
    Ok(Json(ArtifactLineageResponse {
        artifact_id,
        relation: query.relation,
        runs,
        next_before,
    }))
}

#[derive(Debug)]
struct ArtifactZipEntry {
    path: String,
    blob_path: PathBuf,
    size: u64,
}

struct ArtifactZipWriter {
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
    buffer: Vec<u8>,
}

impl ArtifactZipWriter {
    fn new(sender: mpsc::Sender<Result<Bytes, io::Error>>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(ARTIFACT_ZIP_CHUNK_BYTES),
        }
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(ARTIFACT_ZIP_CHUNK_BYTES),
        ));
        self.sender
            .blocking_send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "ZIP client disconnected"))
    }

    fn finish(mut self) -> io::Result<()> {
        self.send_buffer()
    }
}

impl Write for ArtifactZipWriter {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        if source.is_empty() {
            return Ok(0);
        }
        if self.buffer.len() == ARTIFACT_ZIP_CHUNK_BYTES {
            self.send_buffer()?;
        }
        let written = source
            .len()
            .min(ARTIFACT_ZIP_CHUNK_BYTES - self.buffer.len());
        self.buffer.extend_from_slice(&source[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send_buffer()
    }
}

async fn download_artifact(
    State(state): State<AppState>,
    Path(artifact_id): Path<ArtifactId>,
) -> Result<Response, HttpError> {
    let artifact = state.catalog.get_artifact(artifact_id).await?;
    let permit = Arc::clone(&state.artifact_io_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("artifact I/O worker pool is unavailable"))?;
    let blobs = state.blobs.clone();
    let manifest_entries = artifact.entries.clone();
    let (entries, permit) = tokio::task::spawn_blocking(move || {
        prepare_artifact_zip_entries(&blobs, &manifest_entries).map(|entries| (entries, permit))
    })
    .await
    .map_err(|error| HttpError::internal(format!("artifact ZIP worker failed: {error}")))??;

    let file_name = artifact_zip_file_name(&artifact.name, artifact.version);
    let content_disposition = artifact_download_content_disposition(&file_name)?;
    let (sender, receiver) = mpsc::channel(ARTIFACT_ZIP_CHANNEL_CAPACITY);
    let error_sender = sender.clone();
    tokio::spawn(async move {
        let result =
            tokio::task::spawn_blocking(move || stream_artifact_zip(entries, sender)).await;
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(io::Error::other(format!(
                "artifact ZIP worker failed: {error}"
            ))),
        };
        if let Some(error) = error {
            tracing::error!(%error, "artifact ZIP stream failed");
            let _ = error_sender.send(Err(error)).await;
        }
        drop(permit);
    });

    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_DISPOSITION, content_disposition);
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

fn prepare_artifact_zip_entries(
    blobs: &BlobStore,
    entries: &[runloom_protocol::ArtifactEntry],
) -> Result<Vec<ArtifactZipEntry>, HttpError> {
    entries
        .iter()
        .map(|entry| {
            validate_artifact_path(&entry.path).map_err(|_| {
                HttpError::internal(format!(
                    "artifact contains an invalid ZIP entry path: {}",
                    entry.path
                ))
            })?;
            let blob_path = blobs
                .path(&entry.blob.digest)
                .map_err(|error| HttpError::internal(error.to_string()))?;
            let metadata = std::fs::metadata(&blob_path).map_err(|error| {
                HttpError::internal(format!(
                    "failed to inspect artifact blob for '{}': {error}",
                    entry.path
                ))
            })?;
            if !metadata.is_file() || metadata.len() != entry.blob.size {
                return Err(HttpError::internal(format!(
                    "artifact blob for '{}' does not match its manifest",
                    entry.path
                )));
            }
            Ok(ArtifactZipEntry {
                path: entry.path.clone(),
                blob_path,
                size: entry.blob.size,
            })
        })
        .collect()
}

fn stream_artifact_zip(
    entries: Vec<ArtifactZipEntry>,
    sender: mpsc::Sender<Result<Bytes, io::Error>>,
) -> io::Result<()> {
    let output = ArtifactZipWriter::new(sender);
    let mut archive = zip::ZipWriter::new_stream(output);
    for entry in entries {
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .large_file(entry.size >= u64::from(u32::MAX))
            .unix_permissions(0o644);
        archive
            .start_file(&entry.path, options)
            .map_err(io::Error::other)?;
        let mut source = std::fs::File::open(&entry.blob_path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to open artifact blob for '{}': {error}", entry.path),
            )
        })?;
        let copied = io::copy(&mut source, &mut archive)?;
        if copied != entry.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "artifact blob for '{}' changed size while streaming",
                    entry.path
                ),
            ));
        }
    }
    archive
        .finish()
        .map_err(io::Error::other)?
        .into_inner()
        .finish()
}

fn artifact_zip_file_name(name: &str, version: u64) -> String {
    let sanitized = name
        .chars()
        .map(|character| {
            if character.is_control() || r#"<>:\"/\|?*"#.contains(character) {
                '_'
            } else {
                character
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches(|character| character == ' ' || character == '.');
    let stem = if sanitized.is_empty() {
        "artifact"
    } else {
        sanitized
    };
    format!("{stem}-v{version}.zip")
}

fn artifact_download_content_disposition(file_name: &str) -> Result<HeaderValue, HttpError> {
    let fallback = file_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let encoded = encode_rfc8187(file_name);
    HeaderValue::from_str(&format!(
        "attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
    ))
    .map_err(|error| HttpError::internal(format!("invalid artifact download filename: {error}")))
}

fn encode_rfc8187(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            )
        {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}

async fn get_artifact_file(
    State(state): State<AppState>,
    Path((artifact_id, artifact_path)): Path<(ArtifactId, String)>,
    request: Request<Body>,
) -> Result<Response, HttpError> {
    let artifact = state.catalog.get_artifact(artifact_id).await?;
    let entry = artifact
        .entries
        .iter()
        .find(|entry| entry.path == artifact_path)
        .ok_or_else(|| HttpError::not_found(format!("artifact file {artifact_path}")))?;
    serve_blob(
        &state.blobs,
        &state.download_stream_permits,
        &entry.blob.digest,
        Some(&entry.blob.mime_type),
        request,
    )
    .await
}

async fn create_trace_span(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(mut request): Json<CreateTraceSpanRequest>,
) -> Result<(StatusCode, Json<CreateTraceSpanResponse>), HttpError> {
    let span_id = request.id.unwrap_or_default();
    request.id = Some(span_id);
    validate_trace_span(&request)?;
    if let Some(payload) = &request.payload {
        let actual_size = state
            .blobs
            .size(&payload.digest)
            .map_err(|error| HttpError::invalid(error.to_string()))?
            .ok_or_else(|| HttpError::invalid("trace payload blob has not been uploaded"))?;
        if actual_size != payload.size {
            return Err(HttpError::invalid(
                "trace payload size does not match uploaded content",
            ));
        }
    }
    let _mutation = acquire_mutation_locks(
        &state,
        vec![mutation_lock_index(&run_id), mutation_lock_index(&span_id)],
    )
    .await;
    let (span, duplicate) = state.catalog.create_trace_span(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateTraceSpanResponse { span, duplicate })))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TraceListQuery {
    before: Option<TraceSpanId>,
    q: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn list_trace_spans(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<TraceListQuery>,
) -> Result<Json<TraceSpanListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    if query.q.as_ref().is_some_and(|value| {
        value.len() > MAX_TRACE_SEARCH_BYTES || value.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid(format!(
            "trace search cannot exceed {MAX_TRACE_SEARCH_BYTES} non-control bytes"
        )));
    }
    let mut spans = state
        .catalog
        .list_trace_spans(
            run_id,
            query.before,
            query.q.as_deref(),
            page_limit(query.limit),
        )
        .await?;
    let has_more = spans.len() > query.limit;
    spans.truncate(query.limit);
    let next_before = has_more.then(|| spans.last().map(|span| span.id)).flatten();
    Ok(Json(TraceSpanListResponse { spans, next_before }))
}

async fn get_trace_span(
    State(state): State<AppState>,
    Path(span_id): Path<TraceSpanId>,
) -> Result<Json<runloom_protocol::TraceSpanRecord>, HttpError> {
    Ok(Json(state.catalog.get_trace_span(span_id).await?))
}

async fn upload_blob(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<(StatusCode, Json<BlobUploadResponse>), HttpError> {
    let mime_type = header_text(&headers, "content-type")
        .unwrap_or("application/octet-stream")
        .to_owned();
    validate_mime_type(&mime_type)?;
    let file_name = match headers.get("x-runloom-file-name") {
        None => None,
        Some(value) => {
            let encoded = value.to_str().map_err(|_| {
                HttpError::invalid("x-runloom-file-name must be percent-encoded UTF-8")
            })?;
            if encoded.is_empty() {
                None
            } else {
                Some(percent_decode_utf8(encoded, "x-runloom-file-name")?)
            }
        }
    };
    validate_file_name(file_name.as_deref())?;
    let declared_size = headers
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    let existing_size = state
        .blobs
        .size(&digest)
        .map_err(|error| HttpError::invalid(error.to_string()))?;
    if let Some(size) = existing_size {
        if declared_size.is_some_and(|declared| declared != size) {
            return Err(HttpError::conflict(
                "blob_size_conflict",
                "existing blob size differs from the request",
            ));
        }
        return Ok((
            StatusCode::OK,
            Json(BlobUploadResponse {
                blob: BlobRef {
                    digest,
                    size,
                    mime_type,
                    file_name,
                },
                duplicate: true,
            }),
        ));
    }

    let permit = Arc::clone(&state.blob_upload_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("blob upload worker pool is unavailable"))?;
    let staging = state.blobs.staging_file().map_err(HttpError::from)?;
    let (actual_digest, size) = stream_blob(staging.path(), body).await?;
    if actual_digest != digest {
        return Err(HttpError::invalid(format!(
            "blob digest mismatch: expected {digest}, received {actual_digest}"
        )));
    }
    let blobs = state.blobs.clone();
    let install_digest = digest.clone();
    let installed = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let mut staging = staging;
        let installed = blobs.install(staging.path(), &install_digest);
        if installed.is_ok() {
            staging.disarm();
        }
        installed
    })
    .await
    .map_err(|error| HttpError::internal(format!("blob install worker failed: {error}")))??;
    let duplicate = installed.installation == BlobInstallation::AlreadyPresent;
    Ok((
        if duplicate {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(BlobUploadResponse {
            blob: BlobRef {
                digest,
                size,
                mime_type,
                file_name,
            },
            duplicate,
        }),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobQuery {
    mime: Option<String>,
}

async fn get_blob(
    State(state): State<AppState>,
    Path(digest): Path<String>,
    Query(query): Query<BlobQuery>,
    request: Request<Body>,
) -> Result<Response, HttpError> {
    if let Some(mime_type) = &query.mime {
        validate_mime_type(mime_type)?;
    }
    serve_blob(
        &state.blobs,
        &state.download_stream_permits,
        &digest,
        query.mime.as_deref(),
        request,
    )
    .await
}

async fn serve_blob(
    blobs: &BlobStore,
    download_stream_permits: &Arc<Semaphore>,
    digest: &str,
    mime_type: Option<&str>,
    request: Request<Body>,
) -> Result<Response, HttpError> {
    let path = blobs
        .path(digest)
        .map_err(|error| HttpError::invalid(error.to_string()))?;
    if !path.is_file() {
        return Err(HttpError::not_found(format!("blob {digest}")));
    }
    let etag_text = format!("\"sha256:{digest}\"");
    let etag = HeaderValue::from_str(&etag_text)
        .map_err(|_| HttpError::internal("failed to construct blob ETag"))?;
    let not_modified = request
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').map(str::trim).any(|candidate| {
                candidate == "*"
                    || candidate == etag_text
                    || candidate.strip_prefix("W/") == Some(etag_text.as_str())
            })
        });
    if not_modified {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response.headers_mut().insert(header::ETAG, etag);
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=31536000, immutable"),
        );
        return Ok(response);
    }
    let permit = Arc::clone(download_stream_permits)
        .try_acquire_owned()
        .map_err(|_| HttpError::busy("download stream capacity is exhausted; retry later"))?;
    let response = match ServeFile::new(path).oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    };
    let mut response = response.map(Body::new);
    response.headers_mut().insert(header::ETAG, etag);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    if let Some(mime_type) = mime_type.filter(|value| is_safe_inline_mime_type(value)) {
        let value = mime_type
            .parse()
            .map_err(|_| HttpError::invalid("invalid blob MIME type"))?;
        response.headers_mut().insert("content-type", value);
    } else {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        response.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment"),
        );
    }
    Ok(retain_response_permit(response, permit))
}

fn retain_response_permit(response: Response, permit: OwnedSemaphorePermit) -> Response {
    let (parts, body) = response.into_parts();
    let stream = body
        .into_data_stream()
        .scan(permit, |_permit, item| std::future::ready(Some(item)));
    Response::from_parts(parts, Body::from_stream(stream))
}

fn is_safe_inline_mime_type(value: &str) -> bool {
    matches!(
        value.split(';').next().map(str::trim).unwrap_or_default(),
        "audio/aac"
            | "audio/flac"
            | "audio/mpeg"
            | "audio/ogg"
            | "audio/wav"
            | "image/avif"
            | "image/gif"
            | "image/jpeg"
            | "image/png"
            | "image/webp"
            | "video/mp4"
            | "video/ogg"
            | "video/webm"
    )
}

async fn stream_blob(path: &std::path::Path, body: Body) -> Result<(String, u64), HttpError> {
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
        .map_err(|error| {
            HttpError::internal(format!("failed to create blob staging file: {error}"))
        })?;
    let mut stream = body.into_data_stream();
    let mut digest = Sha256::new();
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|error| HttpError::invalid(format!("blob upload failed: {error}")))?;
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| HttpError::invalid("blob size overflow"))?;
        digest.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|error| HttpError::internal(format!("failed to write blob: {error}")))?;
    }
    file.sync_all()
        .await
        .map_err(|error| HttpError::internal(format!("failed to sync blob: {error}")))?;
    drop(file);
    Ok((format!("{:x}", digest.finalize()), size))
}

#[derive(Debug)]
struct HistoryQuery {
    keys: Vec<String>,
    after: Option<u64>,
    limit: Option<usize>,
    max_points: Option<usize>,
}

#[derive(Debug)]
struct SingleRunChartHistoryQuery {
    keys: Vec<String>,
    max_buckets: Option<usize>,
    step_min: Option<u64>,
    step_max: Option<u64>,
}

#[derive(Debug)]
struct ChartRunQueryPlan {
    run_id: RunId,
    keys: Vec<String>,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    axis_extent: Option<ChartAxisExtent>,
}

struct MetricQueryLease {
    _snapshot: OwnedRwLockReadGuard<()>,
    _permit: OwnedSemaphorePermit,
}

struct CancelMetricQueryOnDrop(Arc<AtomicBool>);

impl Drop for CancelMetricQueryOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn chart_history(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<ChartHistoryResponse>, HttpError> {
    let query = parse_chart_history_query(raw_query.as_deref())?;
    let keys = query.keys;
    let max_buckets = query
        .max_buckets
        .unwrap_or_else(|| DEFAULT_CHART_BUCKETS.min(MAX_CHART_BUCKET_CELLS / keys.len()));
    validate_chart_buckets(max_buckets, keys.len())?;
    let requested_viewport = validate_chart_viewport(query.step_min, query.step_max)?;
    state.catalog.get_run(run_id).await?;
    let permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let snapshot = state.metrics.read_snapshot().await;
    let source_extent = state.catalog.metric_extent(run_id, None).await?;
    let source_last_sequence = source_extent.map(|extent| extent.last_sequence);
    let Some(source_extent) = source_extent else {
        let mut response = match requested_viewport {
            Some(viewport) => empty_chart_history_in_viewport(run_id, &keys, viewport, max_buckets),
            None => empty_chart_history(run_id, &keys),
        };
        response.source_last_sequence = source_last_sequence;
        return Ok(Json(response));
    };

    let mut lease = MetricQueryLease {
        _snapshot: snapshot,
        _permit: permit,
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelMetricQueryOnDrop(Arc::clone(&cancelled));
    let viewport = match requested_viewport {
        Some(viewport) => Some(viewport),
        None => {
            let scanner = ChartStepExtentScanner::new(
                &keys,
                source_extent.first_sequence,
                source_extent.last_sequence,
            )?;
            let (scanner, returned_lease) = scan_chart_step_extent(
                &state,
                run_id,
                source_extent.last_sequence,
                scanner,
                lease,
                Arc::clone(&cancelled),
            )
            .await?;
            lease = returned_lease;
            scanner.finish()
        }
    };
    let Some(viewport) = viewport else {
        let mut response = empty_chart_history(run_id, &keys);
        response.source_last_sequence = source_last_sequence;
        return Ok(Json(response));
    };
    let sampler = ChartHistorySampler::new(
        run_id,
        &keys,
        source_extent.first_sequence,
        source_extent.last_sequence,
        viewport.minimum,
        viewport.maximum,
        max_buckets,
    )?;
    let (sampler, _lease) = sample_chart_history(
        &state,
        run_id,
        source_extent.last_sequence,
        sampler,
        lease,
        Arc::clone(&cancelled),
    )
    .await?;
    let mut response = sampler.finish();
    response.source_last_sequence = source_last_sequence;
    Ok(Json(response))
}

async fn query_chart_history(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(request): Json<ChartHistoryQueryRequest>,
) -> Result<Json<ChartHistoryQueryResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_chart_history_request(&request)?;

    let mut run_order = Vec::new();
    let mut keys_by_run = HashMap::<RunId, BTreeSet<String>>::new();
    for requested in &request.series {
        if !keys_by_run.contains_key(&requested.run_id) {
            run_order.push(requested.run_id);
        }
        keys_by_run
            .entry(requested.run_id)
            .or_default()
            .insert(requested.key.clone());
    }
    for run_id in &run_order {
        let run = state.catalog.get_run(*run_id).await?;
        if run.project != project {
            return Err(HttpError::invalid(format!(
                "run {run_id} does not belong to project '{project}'"
            )));
        }
    }

    let permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let snapshot = state.metrics.read_snapshot().await;
    let mut lease = MetricQueryLease {
        _snapshot: snapshot,
        _permit: permit,
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelMetricQueryOnDrop(Arc::clone(&cancelled));
    let needs_axis_extent = request.viewport.is_none()
        || matches!(
            request.alignment,
            ChartAlignment::RelativeStep | ChartAlignment::ElapsedTime
        );

    let mut plans = Vec::with_capacity(run_order.len());
    for run_id in run_order.iter().copied() {
        let keys = keys_by_run
            .remove(&run_id)
            .expect("run order and grouped chart keys stay aligned")
            .into_iter()
            .collect::<Vec<_>>();
        let source_extent = state.catalog.metric_extent(run_id, None).await?;
        let mut plan = ChartRunQueryPlan {
            run_id,
            keys,
            first_sequence: source_extent.map(|extent| extent.first_sequence),
            last_sequence: source_extent.map(|extent| extent.last_sequence),
            axis_extent: None,
        };
        if needs_axis_extent {
            if let Some(extent) = source_extent {
                let mut missing_keys = Vec::new();
                {
                    let mut cache = state
                        .chart_axis_extent_cache
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for key in &plan.keys {
                        let cache_key = ChartAxisExtentCacheKey {
                            run_id,
                            key: key.clone(),
                            source_first_sequence: extent.first_sequence,
                            source_last_sequence: extent.last_sequence,
                        };
                        match cache.get(&cache_key) {
                            Some(Some(cached)) => {
                                include_axis_extent(&mut plan.axis_extent, cached)
                            }
                            Some(None) => {}
                            None => missing_keys.push(key.clone()),
                        }
                    }
                }
                if !missing_keys.is_empty() {
                    let scanner = ChartAxisExtentScanner::new(
                        &missing_keys,
                        extent.first_sequence,
                        extent.last_sequence,
                    )?;
                    let (scanner, returned_lease) = scan_chart_axis_extent(
                        &state,
                        run_id,
                        extent.last_sequence,
                        scanner,
                        lease,
                        Arc::clone(&cancelled),
                    )
                    .await?;
                    lease = returned_lease;
                    let mut scanned = scanner.finish_by_key();
                    let mut cache = state
                        .chart_axis_extent_cache
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for key in missing_keys {
                        let scanned_extent = scanned.remove(&key);
                        cache.insert(
                            ChartAxisExtentCacheKey {
                                run_id,
                                key,
                                source_first_sequence: extent.first_sequence,
                                source_last_sequence: extent.last_sequence,
                            },
                            scanned_extent,
                        );
                        if let Some(scanned_extent) = scanned_extent {
                            include_axis_extent(&mut plan.axis_extent, scanned_extent);
                        }
                    }
                }
            }
        }
        plans.push(plan);
    }

    let x_extent = if let Some(viewport) = request.viewport {
        Some(ChartStepExtent {
            minimum: viewport.minimum,
            maximum: viewport.maximum,
        })
    } else {
        plans.iter().filter_map(|plan| plan.axis_extent).fold(
            None,
            |combined: Option<ChartStepExtent>, extent| {
                let current = aligned_axis_extent(request.alignment, extent);
                Some(match combined {
                    Some(combined) => ChartStepExtent {
                        minimum: combined.minimum.min(current.minimum),
                        maximum: combined.maximum.max(current.maximum),
                    },
                    None => current,
                })
            },
        )
    };
    let runs = plans
        .iter()
        .map(|plan| ChartRunWatermark {
            run_id: plan.run_id,
            source_last_sequence: plan.last_sequence,
        })
        .collect::<Vec<_>>();
    let Some(x_extent) = x_extent else {
        return Ok(Json(ChartHistoryQueryResponse {
            project,
            alignment: request.alignment,
            x_min: None,
            x_max: None,
            bucket_count: 0,
            runs,
            series: request
                .series
                .into_iter()
                .map(|requested| empty_chart_series(requested.run_id, requested.key))
                .collect(),
        }));
    };
    let x_span = u128::from(x_extent.maximum - x_extent.minimum) + 1;
    let bucket_count = usize::try_from(x_span.min(request.max_buckets as u128))
        .expect("validated chart bucket count fits usize");
    let mut sampled = HashMap::<(RunId, String), ChartMetricHistory>::new();

    for plan in &plans {
        let (Some(first_sequence), Some(last_sequence)) = (plan.first_sequence, plan.last_sequence)
        else {
            continue;
        };
        let Some((coordinate, origin)) = chart_coordinate(request.alignment, plan.axis_extent)
        else {
            continue;
        };
        let mut missing_keys = Vec::new();
        let mut cache_keys = HashMap::new();
        for key in &plan.keys {
            let cache_key = ChartSeriesCacheKey {
                run_id: plan.run_id,
                key: key.clone(),
                source_last_sequence: last_sequence,
                alignment: request.alignment,
                origin: origin.clone(),
                x_min: x_extent.minimum,
                x_max: x_extent.maximum,
                max_buckets: request.max_buckets,
            };
            let cached = state
                .chart_series_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&cache_key);
            if let Some(history) = cached {
                sampled.insert((plan.run_id, key.clone()), history);
            } else {
                missing_keys.push(key.clone());
                cache_keys.insert(key.clone(), cache_key);
            }
        }
        if missing_keys.is_empty() {
            continue;
        }
        let sampler = ChartHistorySampler::new_aligned(
            plan.run_id,
            &missing_keys,
            ChartSamplingSpec {
                first_sequence,
                last_sequence,
                coordinate,
                x_min: x_extent.minimum,
                x_max: x_extent.maximum,
                max_buckets: request.max_buckets,
            },
        )?;
        let (sampler, returned_lease) = sample_chart_history(
            &state,
            plan.run_id,
            last_sequence,
            sampler,
            lease,
            Arc::clone(&cancelled),
        )
        .await?;
        lease = returned_lease;
        for (key, history) in sampler.finish().metrics {
            let cache_key = cache_keys
                .remove(&key)
                .expect("sampler returns every requested chart metric");
            state
                .chart_series_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(cache_key, history.clone());
            sampled.insert((plan.run_id, key), history);
        }
    }
    drop(lease);

    Ok(Json(ChartHistoryQueryResponse {
        project,
        alignment: request.alignment,
        x_min: Some(x_extent.minimum),
        x_max: Some(x_extent.maximum),
        bucket_count,
        runs,
        series: request
            .series
            .into_iter()
            .map(|requested| {
                let history = sampled.remove(&(requested.run_id, requested.key.clone()));
                match history {
                    Some(history) => {
                        chart_series_from_metric(requested.run_id, requested.key, history)
                    }
                    None => empty_chart_series(requested.run_id, requested.key),
                }
            })
            .collect(),
    }))
}

fn aligned_axis_extent(alignment: ChartAlignment, extent: ChartAxisExtent) -> ChartStepExtent {
    match alignment {
        ChartAlignment::Step => ChartStepExtent {
            minimum: extent.step_minimum,
            maximum: extent.step_maximum,
        },
        ChartAlignment::RelativeStep => ChartStepExtent {
            minimum: 0,
            maximum: extent.step_maximum - extent.step_minimum,
        },
        ChartAlignment::ElapsedTime => ChartStepExtent {
            minimum: 0,
            maximum: u64::try_from(
                i128::from(extent.timestamp_maximum_ms) - i128::from(extent.timestamp_minimum_ms),
            )
            .expect("ordered i64 timestamps have a non-negative u64 difference"),
        },
    }
}

fn include_axis_extent(combined: &mut Option<ChartAxisExtent>, extent: ChartAxisExtent) {
    *combined = Some(match *combined {
        Some(combined) => ChartAxisExtent {
            step_minimum: combined.step_minimum.min(extent.step_minimum),
            step_maximum: combined.step_maximum.max(extent.step_maximum),
            timestamp_minimum_ms: combined
                .timestamp_minimum_ms
                .min(extent.timestamp_minimum_ms),
            timestamp_maximum_ms: combined
                .timestamp_maximum_ms
                .max(extent.timestamp_maximum_ms),
        },
        None => extent,
    });
}

fn chart_coordinate(
    alignment: ChartAlignment,
    extent: Option<ChartAxisExtent>,
) -> Option<(ChartCoordinate, CachedChartOrigin)> {
    match alignment {
        ChartAlignment::Step => Some((ChartCoordinate::Step, CachedChartOrigin::Step)),
        ChartAlignment::RelativeStep => extent.map(|extent| {
            (
                ChartCoordinate::RelativeStep {
                    origin: extent.step_minimum,
                },
                CachedChartOrigin::RelativeStep(extent.step_minimum),
            )
        }),
        ChartAlignment::ElapsedTime => extent.map(|extent| {
            (
                ChartCoordinate::ElapsedTime {
                    origin_ms: extent.timestamp_minimum_ms,
                },
                CachedChartOrigin::ElapsedTime(extent.timestamp_minimum_ms),
            )
        }),
    }
}

fn chart_series_from_metric(
    run_id: RunId,
    key: String,
    history: ChartMetricHistory,
) -> ChartSeriesHistory {
    ChartSeriesHistory {
        run_id,
        key,
        source_points: history.source_points,
        bucket: history.bucket,
        last_x: history.last_x,
        last_step: history.last_step,
        last_timestamp_ms: history.last_timestamp_ms,
        minimum: history.minimum,
        maximum: history.maximum,
        last: history.last,
    }
}

fn empty_chart_series(run_id: RunId, key: String) -> ChartSeriesHistory {
    chart_series_from_metric(run_id, key, ChartMetricHistory::default())
}

async fn scan_chart_axis_extent(
    state: &AppState,
    run_id: RunId,
    source_last_sequence: u64,
    mut scanner: ChartAxisExtentScanner,
    mut lease: MetricQueryLease,
    cancelled: Arc<AtomicBool>,
) -> Result<(ChartAxisExtentScanner, MetricQueryLease), HttpError> {
    #[cfg(test)]
    state
        .chart_axis_extent_cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .record_scan();
    let mut segment_cursor = None;
    loop {
        let records = state.catalog.list_segments(run_id, segment_cursor).await?;
        let Some(page_last) = records.last().map(|segment| segment.last_sequence) else {
            break;
        };
        let page_full = records.len() == MAX_SEGMENTS_PER_QUERY;
        let segments = records
            .into_iter()
            .map(|segment| SegmentSource {
                relative_path: segment.relative_path,
            })
            .collect::<Vec<_>>();
        let metrics = state.metrics.store().clone();
        let worker_cancelled = Arc::clone(&cancelled);
        (scanner, lease) = tokio::task::spawn_blocking(move || -> Result<_, StorageError> {
            scanner.read_segments(&metrics, &segments, &worker_cancelled)?;
            Ok((scanner, lease))
        })
        .await
        .map_err(|error| HttpError::internal(format!("query worker failed: {error}")))??;
        if page_last >= source_last_sequence || !page_full {
            break;
        }
        if segment_cursor == Some(page_last) {
            return Err(HttpError::internal(
                "chart axis extent cursor did not advance",
            ));
        }
        segment_cursor = Some(page_last);
    }
    Ok((scanner, lease))
}

async fn scan_chart_step_extent(
    state: &AppState,
    run_id: RunId,
    source_last_sequence: u64,
    mut scanner: ChartStepExtentScanner,
    mut lease: MetricQueryLease,
    cancelled: Arc<AtomicBool>,
) -> Result<(ChartStepExtentScanner, MetricQueryLease), HttpError> {
    let mut segment_cursor = None;
    loop {
        let records = state.catalog.list_segments(run_id, segment_cursor).await?;
        let Some(page_last) = records.last().map(|segment| segment.last_sequence) else {
            break;
        };
        let page_full = records.len() == MAX_SEGMENTS_PER_QUERY;
        let segments = records
            .into_iter()
            .map(|segment| SegmentSource {
                relative_path: segment.relative_path,
            })
            .collect::<Vec<_>>();
        let metrics = state.metrics.store().clone();
        let worker_cancelled = Arc::clone(&cancelled);
        (scanner, lease) = tokio::task::spawn_blocking(move || -> Result<_, StorageError> {
            scanner.read_segments(&metrics, &segments, &worker_cancelled)?;
            Ok((scanner, lease))
        })
        .await
        .map_err(|error| HttpError::internal(format!("query worker failed: {error}")))??;
        if page_last >= source_last_sequence || !page_full {
            break;
        }
        if segment_cursor == Some(page_last) {
            return Err(HttpError::internal(
                "chart history extent cursor did not advance",
            ));
        }
        segment_cursor = Some(page_last);
    }
    Ok((scanner, lease))
}

async fn sample_chart_history(
    state: &AppState,
    run_id: RunId,
    source_last_sequence: u64,
    mut sampler: ChartHistorySampler,
    mut lease: MetricQueryLease,
    cancelled: Arc<AtomicBool>,
) -> Result<(ChartHistorySampler, MetricQueryLease), HttpError> {
    let mut segment_cursor = None;
    loop {
        let records = state.catalog.list_segments(run_id, segment_cursor).await?;
        let Some(page_last) = records.last().map(|segment| segment.last_sequence) else {
            break;
        };
        let page_full = records.len() == MAX_SEGMENTS_PER_QUERY;
        let segments = records
            .into_iter()
            .map(|segment| SegmentSource {
                relative_path: segment.relative_path,
            })
            .collect::<Vec<_>>();
        let metrics = state.metrics.store().clone();
        let worker_cancelled = Arc::clone(&cancelled);
        (sampler, lease) = tokio::task::spawn_blocking(move || -> Result<_, StorageError> {
            sampler.read_segments(&metrics, &segments, &worker_cancelled)?;
            Ok((sampler, lease))
        })
        .await
        .map_err(|error| HttpError::internal(format!("query worker failed: {error}")))??;
        if page_last >= source_last_sequence || !page_full {
            break;
        }
        if segment_cursor == Some(page_last) {
            return Err(HttpError::internal(
                "chart history sampling cursor did not advance",
            ));
        }
        segment_cursor = Some(page_last);
    }
    Ok((sampler, lease))
}

fn empty_chart_history(run_id: RunId, keys: &[String]) -> ChartHistoryResponse {
    ChartHistoryResponse {
        run_id,
        step_min: None,
        step_max: None,
        bucket_count: 0,
        source_points: 0,
        source_last_sequence: None,
        metrics: keys
            .iter()
            .cloned()
            .map(|key| (key, ChartMetricHistory::default()))
            .collect(),
    }
}

fn empty_chart_history_in_viewport(
    run_id: RunId,
    keys: &[String],
    viewport: ChartStepExtent,
    max_buckets: usize,
) -> ChartHistoryResponse {
    let span = u128::from(viewport.maximum - viewport.minimum) + 1;
    let bucket_count = usize::try_from(span.min(max_buckets as u128))
        .expect("validated chart bucket count fits usize");
    ChartHistoryResponse {
        run_id,
        step_min: Some(viewport.minimum),
        step_max: Some(viewport.maximum),
        bucket_count,
        source_points: 0,
        source_last_sequence: None,
        metrics: keys
            .iter()
            .cloned()
            .map(|key| (key, ChartMetricHistory::default()))
            .collect(),
    }
}

async fn history(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<HistoryResponse>, HttpError> {
    let query = parse_history_query(raw_query.as_deref())?;
    let keys = query.keys;
    if query.limit.is_some() && query.max_points.is_some() {
        return Err(HttpError::invalid(
            "history queries cannot combine limit and max_points",
        ));
    }
    state.catalog.get_run(run_id).await?;
    if let Some(max_points) = query.max_points {
        validate_sample_points(max_points, keys.len())?;
        return sampled_history(&state, run_id, keys, query.after, max_points)
            .await
            .map(Json);
    }
    let limit = query.limit.unwrap_or_else(default_history_limit);
    if limit == 0 || limit > MAX_HISTORY_POINTS {
        return Err(HttpError::invalid(format!(
            "history limit must be between 1 and {MAX_HISTORY_POINTS}"
        )));
    }
    let permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let snapshot = state.metrics.read_snapshot().await;
    let segments = state
        .catalog
        .list_segments(run_id, query.after)
        .await?
        .into_iter()
        .map(|segment| SegmentSource {
            relative_path: segment.relative_path,
        })
        .collect::<Vec<_>>();
    let segment_page_full = segments.len() == MAX_SEGMENTS_PER_QUERY;
    let metrics = state.metrics.store().clone();
    let after = query.after;
    let lease = MetricQueryLease {
        _snapshot: snapshot,
        _permit: permit,
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelMetricQueryOnDrop(Arc::clone(&cancelled));
    let mut response = tokio::task::spawn_blocking(move || {
        let _lease = lease;
        metrics.read_history_cancelable(run_id, &segments, &keys, after, limit, &cancelled)
    })
    .await
    .map_err(|error| HttpError::internal(format!("query worker failed: {error}")))??;
    if response.next_after.is_none() && segment_page_full {
        response.next_after = response.sequence.last().copied();
    }
    Ok(Json(response))
}

async fn sampled_history(
    state: &AppState,
    run_id: RunId,
    keys: Vec<String>,
    after: Option<u64>,
    max_points: usize,
) -> Result<HistoryResponse, HttpError> {
    let permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let snapshot = state.metrics.read_snapshot().await;
    let Some(extent) = state.catalog.metric_extent(run_id, after).await? else {
        return Ok(HistoryResponse {
            run_id,
            sequence: Vec::new(),
            step: Vec::new(),
            timestamp_ms: Vec::new(),
            metrics: keys.into_iter().map(|key| (key, Vec::new())).collect(),
            next_after: None,
            sampled: true,
            source_points: Some(0),
            source_last_sequence: None,
        });
    };
    let mut sampler = MinMaxHistorySampler::new(
        run_id,
        &keys,
        extent.first_sequence,
        extent.last_sequence,
        max_points,
    )?;
    let mut lease = MetricQueryLease {
        _snapshot: snapshot,
        _permit: permit,
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelMetricQueryOnDrop(Arc::clone(&cancelled));
    let mut segment_cursor = after;
    loop {
        let records = state.catalog.list_segments(run_id, segment_cursor).await?;
        let Some(page_last) = records.last().map(|segment| segment.last_sequence) else {
            break;
        };
        let page_full = records.len() == MAX_SEGMENTS_PER_QUERY;
        let segments = records
            .into_iter()
            .map(|segment| SegmentSource {
                relative_path: segment.relative_path,
            })
            .collect::<Vec<_>>();
        let metrics = state.metrics.store().clone();
        let cancelled_for_read = Arc::clone(&cancelled);
        let (returned_sampler, returned_lease) = tokio::task::spawn_blocking(
            move || -> Result<(MinMaxHistorySampler, MetricQueryLease), StorageError> {
                sampler.read_segments_cancelable(&metrics, &segments, &cancelled_for_read)?;
                Ok((sampler, lease))
            },
        )
        .await
        .map_err(|error| HttpError::internal(format!("query worker failed: {error}")))??;
        sampler = returned_sampler;
        lease = returned_lease;
        if page_last >= extent.last_sequence || !page_full {
            break;
        }
        if segment_cursor == Some(page_last) {
            return Err(HttpError::internal(
                "sampled history cursor did not advance",
            ));
        }
        segment_cursor = Some(page_last);
    }
    drop(lease);
    Ok(sampler.finish())
}

fn validate_project_name(project: &str) -> Result<(), HttpError> {
    if project.is_empty()
        || project.len() > MAX_PROJECT_NAME_BYTES
        || project.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "project name must contain 1 to {MAX_PROJECT_NAME_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_alert(request: &CreateAlertRequest) -> Result<(), HttpError> {
    if request.title.is_empty()
        || request.title.len() > MAX_ALERT_TITLE_BYTES
        || request.title.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "alert title must contain 1 to {MAX_ALERT_TITLE_BYTES} non-control bytes"
        )));
    }
    if request.text.len() > MAX_ALERT_TEXT_BYTES {
        return Err(HttpError::invalid(format!(
            "alert text cannot exceed {MAX_ALERT_TEXT_BYTES} bytes"
        )));
    }
    validate_json_safe_timestamp(request.timestamp_ms, "alert timestamp")?;
    if let Some(step) = request.step {
        validate_json_safe_unsigned(step, "alert step")?;
    }
    Ok(())
}

fn validate_rich_value(request: &CreateRichValueRequest) -> Result<(), HttpError> {
    validate_rich_key(&request.key)?;
    validate_json_safe_unsigned(request.step, "rich value step")?;
    validate_json_safe_timestamp(request.timestamp_ms, "rich value timestamp")?;
    if matches!(
        request.kind,
        RichValueKind::Image | RichValueKind::Audio | RichValueKind::Video | RichValueKind::Table
    ) && request.blob.is_none()
    {
        return Err(HttpError::invalid(format!(
            "{} rich values require a content blob",
            request.kind
        )));
    }
    if let Some(blob) = &request.blob {
        validate_mime_type(&blob.mime_type)?;
        validate_file_name(blob.file_name.as_deref())?;
    }
    validate_document_size(&request.metadata, "rich metadata", MAX_RICH_METADATA_BYTES)
}

fn validate_rich_key(key: &str) -> Result<(), HttpError> {
    if key.is_empty() || key.len() > MAX_RICH_KEY_BYTES || key.chars().any(char::is_control) {
        return Err(HttpError::invalid(format!(
            "rich value keys must contain 1 to {MAX_RICH_KEY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_artifact(request: &CreateArtifactRequest) -> Result<(), HttpError> {
    validate_artifact_component(&request.name, "artifact name", MAX_ARTIFACT_NAME_BYTES)?;
    validate_artifact_component(
        &request.artifact_type,
        "artifact type",
        MAX_ARTIFACT_TYPE_BYTES,
    )?;
    if let Some(version) = request.version {
        validate_json_safe_unsigned(version, "artifact version")?;
    }
    if request
        .description
        .as_ref()
        .is_some_and(|description| description.len() > MAX_ARTIFACT_DESCRIPTION_BYTES)
    {
        return Err(HttpError::invalid(format!(
            "artifact description cannot exceed {MAX_ARTIFACT_DESCRIPTION_BYTES} bytes"
        )));
    }
    if request.aliases.len() > 256 {
        return Err(HttpError::invalid(
            "artifact cannot have more than 256 aliases",
        ));
    }
    let mut aliases = BTreeSet::new();
    for alias in &request.aliases {
        validate_artifact_component(alias, "artifact alias", MAX_ARTIFACT_ALIAS_BYTES)?;
        if !aliases.insert(alias) {
            return Err(HttpError::invalid("artifact aliases must be unique"));
        }
    }
    if request.entries.len() > MAX_ARTIFACT_ENTRIES {
        return Err(HttpError::invalid(format!(
            "artifact cannot contain more than {MAX_ARTIFACT_ENTRIES} entries"
        )));
    }
    let mut paths = BTreeSet::new();
    for entry in &request.entries {
        validate_artifact_path(&entry.path)?;
        validate_mime_type(&entry.blob.mime_type)?;
        validate_file_name(entry.blob.file_name.as_deref())?;
        if !paths.insert(&entry.path) {
            return Err(HttpError::invalid("artifact entry paths must be unique"));
        }
    }
    validate_document_size(
        &request.metadata,
        "artifact metadata",
        MAX_RICH_METADATA_BYTES,
    )?;
    let manifest_size = serde_json::to_vec(request)
        .map_err(|error| HttpError::invalid(format!("artifact is not serializable: {error}")))?
        .len();
    if manifest_size > MAX_ARTIFACT_MANIFEST_BYTES {
        return Err(HttpError::invalid(format!(
            "serialized artifact manifest exceeds {MAX_ARTIFACT_MANIFEST_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_artifact_component(value: &str, name: &str, max_bytes: usize) -> Result<(), HttpError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.chars().any(char::is_control)
        || value.contains('/')
    {
        return Err(HttpError::invalid(format!(
            "{name} must contain 1 to {max_bytes} safe bytes without '/'"
        )));
    }
    Ok(())
}

fn validate_artifact_path(value: &str) -> Result<(), HttpError> {
    if value.is_empty()
        || value.len() > MAX_ARTIFACT_PATH_BYTES
        || value.starts_with('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(HttpError::invalid(format!(
            "artifact paths must be relative POSIX paths up to {MAX_ARTIFACT_PATH_BYTES} bytes"
        )));
    }
    Ok(())
}

fn verify_artifact_blobs(
    blobs: &BlobStore,
    entries: &[runloom_protocol::ArtifactEntry],
) -> Result<(), HttpError> {
    for entry in entries {
        let actual_size = blobs
            .size(&entry.blob.digest)
            .map_err(|error| HttpError::invalid(error.to_string()))?
            .ok_or_else(|| {
                HttpError::invalid(format!(
                    "artifact blob for '{}' has not been uploaded",
                    entry.path
                ))
            })?;
        if actual_size != entry.blob.size {
            return Err(HttpError::invalid(format!(
                "artifact blob size for '{}' does not match uploaded content",
                entry.path
            )));
        }
    }
    Ok(())
}

fn validate_trace_span(request: &CreateTraceSpanRequest) -> Result<(), HttpError> {
    if request.trace_id.is_empty()
        || request.trace_id.len() > MAX_TRACE_ID_BYTES
        || request.trace_id.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "trace ID must contain 1 to {MAX_TRACE_ID_BYTES} non-control bytes"
        )));
    }
    if request.name.is_empty()
        || request.name.len() > MAX_TRACE_NAME_BYTES
        || request.name.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "trace name must contain 1 to {MAX_TRACE_NAME_BYTES} non-control bytes"
        )));
    }
    validate_json_safe_timestamp(request.start_time_ms, "trace start timestamp")?;
    validate_json_safe_timestamp(request.end_time_ms, "trace end timestamp")?;
    if request.end_time_ms < request.start_time_ms {
        return Err(HttpError::invalid(
            "trace end timestamp must be at or after start",
        ));
    }
    if let Some(step) = request.step {
        validate_json_safe_unsigned(step, "trace step")?;
    }
    validate_document_size(
        &request.attributes,
        "trace attributes",
        MAX_TRACE_METADATA_BYTES,
    )?;
    validate_document_size(&request.preview, "trace preview", MAX_TRACE_METADATA_BYTES)?;
    if let Some(payload) = &request.payload {
        validate_mime_type(&payload.mime_type)?;
        validate_file_name(payload.file_name.as_deref())?;
    }
    Ok(())
}

fn validate_mime_type(value: &str) -> Result<(), HttpError> {
    if value.is_empty()
        || value.len() > MAX_MIME_TYPE_BYTES
        || value.chars().any(char::is_control)
        || !value.contains('/')
    {
        return Err(HttpError::invalid(format!(
            "MIME type must contain 1 to {MAX_MIME_TYPE_BYTES} safe bytes"
        )));
    }
    Ok(())
}

fn validate_file_name(value: Option<&str>) -> Result<(), HttpError> {
    if value.is_some_and(|name| {
        name.is_empty()
            || name.len() > MAX_FILE_NAME_BYTES
            || name.chars().any(char::is_control)
            || name.contains(['/', '\\'])
    }) {
        return Err(HttpError::invalid(format!(
            "file name must be a 1 to {MAX_FILE_NAME_BYTES} byte non-control basename"
        )));
    }
    Ok(())
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn percent_decode_utf8(value: &str, name: &str) -> Result<String, HttpError> {
    let source = value.as_bytes();
    let mut decoded = Vec::with_capacity(source.len());
    let mut index = 0;
    while index < source.len() {
        if source[index] != b'%' {
            decoded.push(source[index]);
            index += 1;
            continue;
        }
        let Some(encoded) = source.get(index + 1..index + 3) else {
            return Err(HttpError::invalid(format!(
                "{name} contains an incomplete percent escape"
            )));
        };
        let high = decode_hex_digit(encoded[0]);
        let low = decode_hex_digit(encoded[1]);
        let (Some(high), Some(low)) = (high, low) else {
            return Err(HttpError::invalid(format!(
                "{name} contains an invalid percent escape"
            )));
        };
        decoded.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(decoded)
        .map_err(|_| HttpError::invalid(format!("{name} is not valid percent-encoded UTF-8")))
}

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn validate_create_run(request: &CreateRunRequest) -> Result<(), HttpError> {
    if request.resume == ResumePolicy::Must && request.id.is_none() {
        return Err(HttpError::invalid(
            "resume='must' requires an explicit run ID",
        ));
    }
    if request.name.as_ref().is_some_and(|name| {
        name.is_empty() || name.len() > MAX_RUN_NAME_BYTES || name.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid(format!(
            "run name must contain 1 to {MAX_RUN_NAME_BYTES} non-control bytes"
        )));
    }
    validate_document_size(&request.config, "config", MAX_CONFIG_BYTES)?;
    Ok(())
}

fn validate_document_updates(
    updates: &BTreeMap<String, serde_json::Value>,
    name: &str,
    max_bytes: usize,
) -> Result<(), HttpError> {
    if updates.is_empty() {
        return Err(HttpError::invalid(format!(
            "{name} updates cannot be empty"
        )));
    }
    validate_document_size(updates, name, max_bytes)
}

fn validate_document_size(
    document: &BTreeMap<String, serde_json::Value>,
    name: &str,
    max_bytes: usize,
) -> Result<(), HttpError> {
    let size = serde_json::to_vec(document)
        .map_err(|error| HttpError::invalid(format!("{name} is not serializable: {error}")))?
        .len();
    if size > max_bytes {
        return Err(HttpError::invalid(format!(
            "serialized {name} exceeds {max_bytes} bytes"
        )));
    }
    validate_json_safe_integers(document.values(), name)?;
    Ok(())
}

fn validate_json_safe_integers<'a>(
    values: impl IntoIterator<Item = &'a serde_json::Value>,
    name: &str,
) -> Result<(), HttpError> {
    let mut pending = values.into_iter().collect::<Vec<_>>();
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Array(values) => pending.extend(values),
            serde_json::Value::Object(values) => pending.extend(values.values()),
            serde_json::Value::Number(value) => {
                let outside_safe_range = value.as_i64().is_some_and(|value| {
                    value < -(MAX_JSON_SAFE_INTEGER as i64) || value > MAX_JSON_SAFE_INTEGER as i64
                }) || value
                    .as_u64()
                    .is_some_and(|value| value > MAX_JSON_SAFE_INTEGER);
                if outside_safe_range {
                    return Err(HttpError::invalid(format!(
                        "{name} contains an integer outside the JSON-safe range -{MAX_JSON_SAFE_INTEGER} to {MAX_JSON_SAFE_INTEGER}"
                    )));
                }
            }
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {
            }
        }
    }
    Ok(())
}

fn validate_batch(request: &IngestBatchRequest) -> Result<(), HttpError> {
    if request.points.is_empty() || request.points.len() > MAX_BATCH_POINTS {
        return Err(HttpError::invalid(format!(
            "metric batches must contain 1 to {MAX_BATCH_POINTS} points"
        )));
    }
    validate_json_safe_unsigned(request.batch_sequence, "batch sequence")?;
    let mut previous_sequence = None;
    for point in &request.points {
        validate_json_safe_unsigned(point.sequence, "metric sequence")?;
        validate_json_safe_unsigned(point.step, "metric step")?;
        validate_json_safe_timestamp(point.timestamp_ms, "metric timestamp")?;
        if previous_sequence.is_some_and(|previous| point.sequence != previous + 1) {
            return Err(HttpError::invalid(
                "point sequences must be strictly consecutive within a batch",
            ));
        }
        previous_sequence = Some(point.sequence);
        if point.metrics.is_empty() || point.metrics.len() > MAX_METRICS_PER_POINT {
            return Err(HttpError::invalid(format!(
                "each point must contain 1 to {MAX_METRICS_PER_POINT} metrics"
            )));
        }
        for (key, value) in &point.metrics {
            if key.is_empty()
                || key.len() > MAX_METRIC_KEY_BYTES
                || key.chars().any(char::is_control)
            {
                return Err(HttpError::invalid(format!(
                    "metric keys must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
                )));
            }
            if !value.is_finite() {
                return Err(HttpError::invalid(format!(
                    "metric '{key}' must be a finite number"
                )));
            }
        }
    }
    Ok(())
}

fn parse_history_query(value: Option<&str>) -> Result<HistoryQuery, HttpError> {
    let pairs = serde_urlencoded::from_str::<Vec<(String, String)>>(value.unwrap_or_default())
        .map_err(|error| HttpError::invalid(format!("invalid history query: {error}")))?;
    let mut keys = BTreeSet::new();
    let mut after = None;
    let mut limit = None;
    let mut max_points = None;
    for (name, value) in pairs {
        match name.as_str() {
            "key" => {
                if value.is_empty()
                    || value.len() > MAX_METRIC_KEY_BYTES
                    || value.chars().any(char::is_control)
                {
                    return Err(HttpError::invalid(format!(
                        "history metric keys must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
                    )));
                }
                keys.insert(value);
            }
            "after" => parse_history_unsigned_parameter(&mut after, &name, &value)?,
            "limit" => parse_history_unsigned_parameter(&mut limit, &name, &value)?,
            "max_points" => {
                parse_history_unsigned_parameter(&mut max_points, &name, &value)?;
            }
            _ => {
                return Err(HttpError::invalid(format!(
                    "unknown history query parameter '{name}'"
                )));
            }
        }
    }
    if keys.is_empty() || keys.len() > MAX_HISTORY_KEYS {
        return Err(HttpError::invalid(format!(
            "history queries must request 1 to {MAX_HISTORY_KEYS} metric keys"
        )));
    }
    if let Some(after) = after {
        validate_json_safe_unsigned(after, "history cursor")?;
    }
    Ok(HistoryQuery {
        keys: keys.into_iter().collect(),
        after,
        limit,
        max_points,
    })
}

fn parse_history_unsigned_parameter<T: FromStr>(
    target: &mut Option<T>,
    name: &str,
    value: &str,
) -> Result<(), HttpError> {
    if target.is_some() {
        return Err(HttpError::invalid(format!(
            "history query parameter '{name}' cannot be repeated"
        )));
    }
    *target = Some(value.parse().map_err(|_| {
        HttpError::invalid(format!(
            "history query parameter '{name}' must be an unsigned integer"
        ))
    })?);
    Ok(())
}

fn parse_chart_history_query(value: Option<&str>) -> Result<SingleRunChartHistoryQuery, HttpError> {
    let pairs = serde_urlencoded::from_str::<Vec<(String, String)>>(value.unwrap_or_default())
        .map_err(|error| HttpError::invalid(format!("invalid chart history query: {error}")))?;
    let mut keys = BTreeSet::new();
    let mut max_buckets = None;
    let mut step_min = None;
    let mut step_max = None;
    for (name, value) in pairs {
        match name.as_str() {
            "key" => {
                if value.is_empty()
                    || value.len() > MAX_METRIC_KEY_BYTES
                    || value.chars().any(char::is_control)
                {
                    return Err(HttpError::invalid(format!(
                        "chart metric keys must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
                    )));
                }
                keys.insert(value);
            }
            "max_buckets" => {
                parse_unique_unsigned_parameter(&mut max_buckets, &name, &value)?;
            }
            "step_min" => parse_unique_unsigned_parameter(&mut step_min, &name, &value)?,
            "step_max" => parse_unique_unsigned_parameter(&mut step_max, &name, &value)?,
            _ => {
                return Err(HttpError::invalid(format!(
                    "unknown chart history query parameter '{name}'"
                )));
            }
        }
    }
    if keys.is_empty() || keys.len() > MAX_HISTORY_KEYS {
        return Err(HttpError::invalid(format!(
            "chart history queries must request 1 to {MAX_HISTORY_KEYS} metric keys"
        )));
    }
    Ok(SingleRunChartHistoryQuery {
        keys: keys.into_iter().collect(),
        max_buckets,
        step_min,
        step_max,
    })
}

fn parse_unique_unsigned_parameter<T: FromStr>(
    target: &mut Option<T>,
    name: &str,
    value: &str,
) -> Result<(), HttpError> {
    if target.is_some() {
        return Err(HttpError::invalid(format!(
            "chart history query parameter '{name}' cannot be repeated"
        )));
    }
    *target = Some(value.parse().map_err(|_| {
        HttpError::invalid(format!(
            "chart history query parameter '{name}' must be an unsigned integer"
        ))
    })?);
    Ok(())
}

fn default_history_limit() -> usize {
    1_000
}

fn validate_sample_points(max_points: usize, key_count: usize) -> Result<(), HttpError> {
    let minimum = key_count * 2;
    if max_points < minimum || max_points > MAX_HISTORY_POINTS {
        return Err(HttpError::invalid(format!(
            "history max_points must be between {minimum} and {MAX_HISTORY_POINTS} for {key_count} requested metric key(s)"
        )));
    }
    Ok(())
}

fn validate_chart_history_request(request: &ChartHistoryQueryRequest) -> Result<(), HttpError> {
    if request.series.is_empty() || request.series.len() > MAX_CHART_QUERY_SERIES {
        return Err(HttpError::invalid(format!(
            "chart history queries must request 1 to {MAX_CHART_QUERY_SERIES} series"
        )));
    }
    let mut keys_by_run = HashMap::<RunId, BTreeSet<&str>>::new();
    for series in &request.series {
        if series.key.is_empty()
            || series.key.len() > MAX_METRIC_KEY_BYTES
            || series.key.chars().any(char::is_control)
        {
            return Err(HttpError::invalid(format!(
                "chart metric keys must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
            )));
        }
        if !keys_by_run
            .entry(series.run_id)
            .or_default()
            .insert(&series.key)
        {
            return Err(HttpError::invalid(format!(
                "chart series ({}, '{}') is repeated",
                series.run_id, series.key
            )));
        }
    }
    if keys_by_run.len() > MAX_CHART_QUERY_RUNS {
        return Err(HttpError::invalid(format!(
            "chart history queries may include at most {MAX_CHART_QUERY_RUNS} runs"
        )));
    }
    let cells = request
        .max_buckets
        .checked_mul(request.series.len())
        .ok_or_else(|| HttpError::invalid("chart history bucket budget overflow"))?;
    if request.max_buckets == 0
        || request.max_buckets > MAX_CHART_BUCKETS
        || cells > MAX_CHART_QUERY_CELLS
    {
        let maximum = MAX_CHART_BUCKETS.min(MAX_CHART_QUERY_CELLS / request.series.len());
        return Err(HttpError::invalid(format!(
            "chart history max_buckets must be between 1 and {maximum} for {} requested series",
            request.series.len()
        )));
    }
    if request
        .viewport
        .is_some_and(|viewport| viewport.minimum > viewport.maximum)
    {
        return Err(HttpError::invalid(
            "chart history viewport minimum must not exceed maximum",
        ));
    }
    if let Some(viewport) = request.viewport {
        validate_json_safe_unsigned(viewport.minimum, "chart viewport minimum")?;
        validate_json_safe_unsigned(viewport.maximum, "chart viewport maximum")?;
    }
    Ok(())
}

fn validate_chart_buckets(max_buckets: usize, key_count: usize) -> Result<(), HttpError> {
    let cells = max_buckets
        .checked_mul(key_count)
        .ok_or_else(|| HttpError::invalid("chart history bucket budget overflow"))?;
    if max_buckets == 0 || max_buckets > MAX_CHART_BUCKETS || cells > MAX_CHART_BUCKET_CELLS {
        let maximum = MAX_CHART_BUCKETS.min(MAX_CHART_BUCKET_CELLS / key_count);
        return Err(HttpError::invalid(format!(
            "chart history max_buckets must be between 1 and {maximum} for {key_count} requested metric key(s)"
        )));
    }
    Ok(())
}

fn validate_chart_viewport(
    step_min: Option<u64>,
    step_max: Option<u64>,
) -> Result<Option<ChartStepExtent>, HttpError> {
    if let Some(minimum) = step_min {
        validate_json_safe_unsigned(minimum, "chart step_min")?;
    }
    if let Some(maximum) = step_max {
        validate_json_safe_unsigned(maximum, "chart step_max")?;
    }
    match (step_min, step_max) {
        (None, None) => Ok(None),
        (Some(minimum), Some(maximum)) if minimum <= maximum => {
            Ok(Some(ChartStepExtent { minimum, maximum }))
        }
        (Some(_), Some(_)) => Err(HttpError::invalid(
            "chart history step_min must not exceed step_max",
        )),
        _ => Err(HttpError::invalid(
            "chart history step_min and step_max must be provided together",
        )),
    }
}

fn validate_json_safe_unsigned(value: u64, name: &str) -> Result<(), HttpError> {
    if value > MAX_JSON_SAFE_INTEGER {
        return Err(HttpError::invalid(format!(
            "{name} cannot exceed {MAX_JSON_SAFE_INTEGER}"
        )));
    }
    Ok(())
}

fn validate_json_safe_timestamp(value: i64, name: &str) -> Result<(), HttpError> {
    if value < 0 || value as u64 > MAX_JSON_SAFE_INTEGER {
        return Err(HttpError::invalid(format!(
            "{name} must be between 0 and {MAX_JSON_SAFE_INTEGER}"
        )));
    }
    Ok(())
}

fn default_list_limit() -> usize {
    100
}

fn validate_list_limit(limit: usize) -> Result<(), HttpError> {
    if limit == 0 || limit > MAX_LIST_ITEMS {
        return Err(HttpError::invalid(format!(
            "list limit must be between 1 and {MAX_LIST_ITEMS}"
        )));
    }
    Ok(())
}

fn page_limit(limit: usize) -> usize {
    limit.saturating_add(1)
}

fn validate_search(value: Option<&str>, name: &str) -> Result<(), HttpError> {
    if value.is_some_and(|value| {
        value.is_empty() || value.len() > MAX_RUN_NAME_BYTES || value.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid(format!(
            "{name} must contain 1 to {MAX_RUN_NAME_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_metric_catalog_text(value: Option<&str>, name: &str) -> Result<(), HttpError> {
    if value.is_some_and(|value| {
        value.is_empty()
            || value.len() > MAX_METRIC_KEY_BYTES
            || value.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid(format!(
            "{name} must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_run_query(request: &RunQueryRequest) -> Result<(), HttpError> {
    validate_list_limit(request.limit)?;
    if request.run_ids.len() > MAX_CHART_QUERY_RUNS {
        return Err(HttpError::invalid(format!(
            "run queries cannot contain more than {MAX_CHART_QUERY_RUNS} run IDs"
        )));
    }
    if request
        .run_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>()
        .len()
        != request.run_ids.len()
    {
        return Err(HttpError::invalid("run query run IDs must be unique"));
    }
    if !request.run_ids.is_empty() && request.before.is_some() {
        return Err(HttpError::invalid(
            "run_ids and before cannot be used together",
        ));
    }
    if !request.run_ids.is_empty() && request.limit < request.run_ids.len() {
        return Err(HttpError::invalid(
            "run query limit must include every requested run ID",
        ));
    }
    if let Some(project) = &request.project {
        validate_project_name(project)?;
    }
    for (value, name, maximum) in [
        (request.name.as_deref(), "run name", MAX_RUN_NAME_BYTES),
        (
            request.name_contains.as_deref(),
            "run name search",
            MAX_RUN_NAME_BYTES,
        ),
    ] {
        if value.is_some_and(|value| {
            value.is_empty() || value.len() > maximum || value.chars().any(char::is_control)
        }) {
            return Err(HttpError::invalid(format!(
                "{name} must contain 1 to {maximum} non-control bytes"
            )));
        }
    }
    if request.config_equals.len() > 32 || request.summary_equals.len() > 32 {
        return Err(HttpError::invalid(
            "run queries cannot contain more than 32 config or summary filters",
        ));
    }
    for key in request
        .config_equals
        .keys()
        .chain(request.summary_equals.keys())
    {
        if key.is_empty() || key.len() > 256 || key.chars().any(char::is_control) {
            return Err(HttpError::invalid(
                "run query document keys must contain 1 to 256 non-control bytes",
            ));
        }
    }
    validate_document_size(&request.config_equals, "config filters", MAX_CONFIG_BYTES)?;
    validate_document_size(
        &request.summary_equals,
        "summary filters",
        MAX_SUMMARY_BYTES,
    )?;
    Ok(())
}

fn validate_sweep(request: &CreateSweepRequest) -> Result<(), HttpError> {
    if request.name.as_ref().is_some_and(|name| {
        name.is_empty() || name.len() > MAX_RUN_NAME_BYTES || name.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid(format!(
            "sweep name must contain 1 to {MAX_RUN_NAME_BYTES} non-control bytes"
        )));
    }
    if request.metric.name.is_empty()
        || request.metric.name.len() > MAX_METRIC_KEY_BYTES
        || request.metric.name.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "sweep metric name must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
        )));
    }
    if request.parameters.is_empty() || request.parameters.len() > MAX_SWEEP_PARAMETERS {
        return Err(HttpError::invalid(format!(
            "sweeps require 1 to {MAX_SWEEP_PARAMETERS} parameters"
        )));
    }
    for (name, parameter) in &request.parameters {
        if name.is_empty()
            || name.len() > 256
            || name.chars().any(char::is_control)
            || parameter.values.is_empty()
            || parameter.values.len() > MAX_SWEEP_VALUES
        {
            return Err(HttpError::invalid(format!(
                "sweep parameters require a safe name and 1 to {MAX_SWEEP_VALUES} values"
            )));
        }
    }
    if request.max_runs == 0 || request.max_runs > MAX_SWEEP_RUNS {
        return Err(HttpError::invalid(format!(
            "sweep max_runs must be between 1 and {MAX_SWEEP_RUNS}"
        )));
    }
    if request
        .early_terminate
        .as_ref()
        .is_some_and(|early| early.min_trials == 0 || early.min_trials > 100)
    {
        return Err(HttpError::invalid(
            "early termination requires min_trials between 1 and 100",
        ));
    }
    let encoded = serde_json::to_vec(&request.parameters)
        .map_err(|error| HttpError::invalid(format!("sweep is not serializable: {error}")))?;
    if encoded.len() > MAX_CONFIG_BYTES {
        return Err(HttpError::invalid(format!(
            "serialized sweep parameters exceed {MAX_CONFIG_BYTES} bytes"
        )));
    }
    validate_json_safe_integers(
        request
            .parameters
            .values()
            .flat_map(|parameter| parameter.values.iter()),
        "sweep parameters",
    )?;
    Ok(())
}

fn validate_agent_id(agent_id: &str) -> Result<(), HttpError> {
    if agent_id.is_empty()
        || agent_id.len() > MAX_AGENT_ID_BYTES
        || agent_id.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "agent ID must contain 1 to {MAX_AGENT_ID_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_report(
    name: &str,
    description: Option<&str>,
    layout: &ReportLayout,
) -> Result<(), HttpError> {
    if name.is_empty() || name.len() > MAX_RUN_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(HttpError::invalid(format!(
            "report name must contain 1 to {MAX_RUN_NAME_BYTES} non-control bytes"
        )));
    }
    if description.is_some_and(|value| value.len() > MAX_REPORT_MARKDOWN_BYTES) {
        return Err(HttpError::invalid(format!(
            "report description cannot exceed {MAX_REPORT_MARKDOWN_BYTES} bytes"
        )));
    }
    if !(1..=4).contains(&layout.columns)
        || layout.panels.is_empty()
        || layout.panels.len() > MAX_REPORT_PANELS
    {
        return Err(HttpError::invalid(format!(
            "report layouts require 1 to {MAX_REPORT_PANELS} panels and 1 to 4 columns"
        )));
    }
    let mut panel_ids = BTreeSet::new();
    for panel in &layout.panels {
        if panel.id.is_empty()
            || panel.id.len() > 128
            || panel.id.chars().any(char::is_control)
            || !panel_ids.insert(&panel.id)
            || panel.title.is_empty()
            || panel.title.len() > MAX_RUN_NAME_BYTES
            || panel.title.chars().any(char::is_control)
            || panel.width == 0
            || panel.width > layout.columns
            || !(180..=800).contains(&panel.height)
        {
            return Err(HttpError::invalid(
                "report panels require unique safe IDs, titles, valid spans, and 180-800px heights",
            ));
        }
        match panel.kind {
            ReportPanelKind::Metric => {
                if panel.run_id.is_none()
                    || panel.metric_keys.is_empty()
                    || panel.metric_keys.len() > MAX_REPORT_METRICS
                    || panel.markdown.is_some()
                {
                    return Err(HttpError::invalid(format!(
                        "metric panels require a run and 1 to {MAX_REPORT_METRICS} metric keys"
                    )));
                }
                for key in &panel.metric_keys {
                    if key.is_empty()
                        || key.len() > MAX_METRIC_KEY_BYTES
                        || key.chars().any(char::is_control)
                    {
                        return Err(HttpError::invalid("report metric key is invalid"));
                    }
                }
            }
            ReportPanelKind::Markdown => {
                if panel.run_id.is_some()
                    || !panel.metric_keys.is_empty()
                    || panel
                        .markdown
                        .as_ref()
                        .is_none_or(|value| value.len() > MAX_REPORT_MARKDOWN_BYTES)
                {
                    return Err(HttpError::invalid(
                        "markdown panels require only bounded markdown text",
                    ));
                }
            }
        }
    }
    let encoded = serde_json::to_vec(layout)
        .map_err(|error| HttpError::invalid(format!("report is not serializable: {error}")))?;
    if encoded.len() > MAX_CONFIG_BYTES {
        return Err(HttpError::invalid(format!(
            "serialized report layout exceeds {MAX_CONFIG_BYTES} bytes"
        )));
    }
    Ok(())
}

fn batch_digest(request: &IngestBatchRequest) -> Result<String, HttpError> {
    let encoded = serde_json::to_vec(request)
        .map_err(|error| HttpError::invalid(format!("batch is not serializable: {error}")))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn latest_metrics(request: &IngestBatchRequest) -> BTreeMap<String, f64> {
    let mut summary = BTreeMap::new();
    for point in &request.points {
        summary.extend(
            point
                .metrics
                .iter()
                .filter(|(key, _)| !key.starts_with("system/"))
                .map(|(key, value)| (key.clone(), *value)),
        );
    }
    summary
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    body: ApiError,
}

impl HttpError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: ApiError::new("invalid_request", message),
        }
    }

    fn conflict(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            body: ApiError::new(code, message),
        }
    }

    fn not_found(resource: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            body: ApiError::new("not_found", format!("{} was not found", resource.into())),
        }
    }

    fn busy(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: ApiError::new("server_busy", message),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        let message = message.into();
        tracing::error!(%message, "request failed");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: ApiError::new("internal_error", "internal server error"),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = self.status;
        let mut response = (status, Json(self.body)).into_response();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

impl From<CatalogError> for HttpError {
    fn from(error: CatalogError) -> Self {
        match error {
            CatalogError::NotFound { .. } => Self {
                status: StatusCode::NOT_FOUND,
                body: ApiError::new("not_found", error.to_string()),
            },
            CatalogError::Conflict(_) => Self {
                status: StatusCode::CONFLICT,
                body: ApiError::new("conflict", error.to_string()),
            },
            CatalogError::Busy(_) => Self::busy(error.to_string()),
            CatalogError::Limit(_) => Self::invalid(error.to_string()),
            CatalogError::CreateDirectory { .. }
            | CatalogError::Database(_)
            | CatalogError::InvalidData(_) => Self::internal(error.to_string()),
        }
    }
}

impl From<StorageError> for HttpError {
    fn from(error: StorageError) -> Self {
        Self::internal(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Cursor, Read};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::{Body, Bytes, to_bytes};
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use runloom_catalog::{BatchStatus, Catalog, CatalogError, SegmentManifest};
    use runloom_protocol::{
        AlertId, AlertLevel, AlertListResponse, ApiError, ArtifactEntry, ArtifactListResponse,
        ArtifactRelation, BlobRef, BlobUploadResponse, ChartAlignment, ChartHistoryQueryRequest,
        ChartHistoryQueryResponse, ChartHistoryResponse, ChartMetricHistory, ChartSeriesRequest,
        ChartViewport, ClaimSweepTrialRequest, ClaimSweepTrialResponse, CompleteSweepTrialRequest,
        ConfigUpdateRequest, CreateAlertRequest, CreateAlertResponse, CreateArtifactRequest,
        CreateArtifactResponse, CreateReportRequest, CreateReportResponse, CreateRichValueRequest,
        CreateRichValueResponse, CreateRunRequest, CreateRunResponse, CreateSweepRequest,
        CreateSweepResponse, CreateTraceSpanRequest, CreateTraceSpanResponse, DiagnosticsResponse,
        EarlyTerminateConfig, FinishRunRequest, FinishRunResponse, HealthResponse, HealthStatus,
        HeartbeatSweepTrialRequest, HistoryResponse, IngestBatchRequest, IngestBatchResponse,
        MetricCatalogMode, MetricGoal, MetricKeyListResponse, MetricPoint, ProjectListResponse,
        ProjectMetricCatalogRequest, ProjectMetricCatalogResponse, ProjectSummary, ReportLayout,
        ReportListResponse, ReportPanel, ReportPanelKind, ReportRecord, ResumePolicy, RichValueId,
        RichValueKeyListResponse, RichValueKind, RichValueListResponse, RunArtifactListResponse,
        RunId, RunListResponse, RunQueryRequest, RunQueryResponse, RunState, RunUpdateResponse,
        SummaryUpdateRequest, SweepMethod, SweepMetric, SweepParameter, SweepTrialListResponse,
        SweepTrialState, TraceKind, TraceSpanId, TraceSpanListResponse, TraceSpanRecord,
        TraceStatus, UpdateReportRequest, UseArtifactRequest,
    };
    use runloom_storage::{BlobStore, ChartAxisExtent, MetricStore, StorageError};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::{
        AppState, BLOB_UPLOAD_WORKERS, CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES,
        CHART_SERIES_CACHE_MAX_ENTRIES, CachedChartOrigin, ChartAxisExtentCache,
        ChartAxisExtentCacheKey, ChartSeriesCache, ChartSeriesCacheKey, CompactionConfig,
        CompactionError, CompactionOutcome, MetricRuntime, RequestTelemetry, app,
        app_with_axis_extent_cache, app_with_runtime, compact_once, create_artifact, ingest_batch,
        mutation_lock_index, process_ingest_batch, upload_blob,
    };

    #[test]
    fn chart_axis_extent_cache_is_bounded_and_caches_missing_metrics() {
        let run_id = RunId::new();
        let keys = (0..=CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES)
            .map(|index| ChartAxisExtentCacheKey {
                run_id,
                key: format!("metric-{index}"),
                source_first_sequence: 1,
                source_last_sequence: 10,
            })
            .collect::<Vec<_>>();
        let mut cache = ChartAxisExtentCache::default();
        for (index, key) in keys
            .iter()
            .take(CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES)
            .enumerate()
        {
            cache.insert(
                key.clone(),
                (index != 0).then_some(ChartAxisExtent {
                    step_minimum: index as u64,
                    step_maximum: index as u64 + 1,
                    timestamp_minimum_ms: index as i64,
                    timestamp_maximum_ms: index as i64 + 1,
                }),
            );
        }
        assert_eq!(cache.get(&keys[0]), Some(None));
        cache.insert(
            keys[CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES].clone(),
            Some(ChartAxisExtent {
                step_minimum: 0,
                step_maximum: 1,
                timestamp_minimum_ms: 0,
                timestamp_maximum_ms: 1,
            }),
        );
        assert!(cache.get(&keys[0]).is_some());
        assert!(cache.get(&keys[1]).is_none());
        assert!(cache.entries.len() <= CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES);
    }

    #[test]
    fn chart_series_cache_is_bounded_and_updates_recency() {
        let mut cache = ChartSeriesCache::default();
        let keys = (0..=CHART_SERIES_CACHE_MAX_ENTRIES)
            .map(|index| ChartSeriesCacheKey {
                run_id: RunId::new(),
                key: format!("metric-{index}"),
                source_last_sequence: 1,
                alignment: ChartAlignment::Step,
                origin: CachedChartOrigin::Step,
                x_min: 0,
                x_max: 1,
                max_buckets: 2,
            })
            .collect::<Vec<_>>();
        for key in keys.iter().take(CHART_SERIES_CACHE_MAX_ENTRIES) {
            cache.insert(key.clone(), ChartMetricHistory::default());
        }
        assert!(cache.get(&keys[0]).is_some());
        cache.insert(
            keys[CHART_SERIES_CACHE_MAX_ENTRIES].clone(),
            ChartMetricHistory::default(),
        );

        assert_eq!(cache.entries.len(), CHART_SERIES_CACHE_MAX_ENTRIES);
        assert!(cache.entries.contains_key(&keys[0]));
        assert!(!cache.entries.contains_key(&keys[1]));
        assert!(
            cache
                .entries
                .contains_key(&keys[CHART_SERIES_CACHE_MAX_ENTRIES])
        );
    }

    #[test]
    fn json_number_contract_rejects_values_beyond_browser_precision() {
        let unsafe_value = runloom_protocol::MAX_JSON_SAFE_INTEGER + 1;
        let batch = IngestBatchRequest {
            batch_sequence: unsafe_value,
            points: vec![MetricPoint {
                sequence: 1,
                step: 0,
                timestamp_ms: 0,
                metrics: BTreeMap::from([("loss".to_owned(), 1.0)]),
            }],
        };
        assert!(super::validate_batch(&batch).is_err());
        assert!(super::validate_chart_viewport(Some(0), Some(unsafe_value)).is_err());
        assert!(super::validate_json_safe_timestamp(unsafe_value as i64, "timestamp").is_err());
        assert!(super::validate_json_safe_unsigned(unsafe_value, "step").is_err());
    }

    #[test]
    fn arbitrary_json_boundaries_share_the_safe_integer_contract() {
        let maximum = runloom_protocol::MAX_JSON_SAFE_INTEGER;
        let safe_document = BTreeMap::from([(
            "nested".to_owned(),
            serde_json::json!([-(maximum as i64), maximum]),
        )]);
        assert!(
            super::validate_document_size(
                &safe_document,
                "safe document",
                runloom_protocol::MAX_CONFIG_BYTES,
            )
            .is_ok()
        );

        for unsafe_integer in [
            serde_json::json!(maximum + 1),
            serde_json::json!(-(maximum as i64) - 1),
        ] {
            let document = BTreeMap::from([(
                "nested".to_owned(),
                serde_json::json!({"array": [unsafe_integer]}),
            )]);
            assert!(
                super::validate_create_run(&CreateRunRequest {
                    id: None,
                    name: None,
                    config: document.clone(),
                    resume: ResumePolicy::Never,
                    sweep_trial_id: None,
                })
                .is_err()
            );
            assert!(
                super::validate_document_updates(
                    &document,
                    "summary",
                    runloom_protocol::MAX_SUMMARY_BYTES,
                )
                .is_err()
            );
            assert!(
                super::validate_rich_value(&CreateRichValueRequest {
                    id: None,
                    key: "histogram".to_owned(),
                    kind: RichValueKind::Histogram,
                    step: 0,
                    timestamp_ms: 0,
                    blob: None,
                    metadata: document.clone(),
                })
                .is_err()
            );
            assert!(
                super::validate_artifact(&CreateArtifactRequest {
                    id: None,
                    name: "checkpoint".to_owned(),
                    artifact_type: "model".to_owned(),
                    version: None,
                    description: None,
                    metadata: document.clone(),
                    aliases: Vec::new(),
                    entries: Vec::new(),
                })
                .is_err()
            );
            for (attributes, preview) in [
                (document.clone(), BTreeMap::new()),
                (BTreeMap::new(), document.clone()),
            ] {
                assert!(
                    super::validate_trace_span(&CreateTraceSpanRequest {
                        id: None,
                        trace_id: "trace".to_owned(),
                        parent_span_id: None,
                        name: "span".to_owned(),
                        kind: TraceKind::Span,
                        status: TraceStatus::Ok,
                        start_time_ms: 0,
                        end_time_ms: 0,
                        step: None,
                        attributes,
                        preview,
                        payload: None,
                    })
                    .is_err()
                );
            }
            for (config_equals, summary_equals) in [
                (document.clone(), BTreeMap::new()),
                (BTreeMap::new(), document.clone()),
            ] {
                assert!(
                    super::validate_run_query(&RunQueryRequest {
                        project: None,
                        run_ids: Vec::new(),
                        state: None,
                        name: None,
                        name_contains: None,
                        config_equals,
                        summary_equals,
                        before: None,
                        limit: 1,
                    })
                    .is_err()
                );
            }
            assert!(
                super::validate_sweep(&CreateSweepRequest {
                    id: None,
                    name: None,
                    method: SweepMethod::Grid,
                    metric: SweepMetric {
                        name: "loss".to_owned(),
                        goal: MetricGoal::Minimize,
                    },
                    parameters: BTreeMap::from([(
                        "seed".to_owned(),
                        SweepParameter {
                            values: document.values().cloned().collect(),
                        },
                    )]),
                    max_runs: 1,
                    early_terminate: None,
                })
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn raw_http_json_documents_preserve_only_safe_integer_edges()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let maximum = runloom_protocol::MAX_JSON_SAFE_INTEGER;
        let safe_response = router
            .clone()
            .oneshot(
                Request::post("/api/v1/projects/json-integers/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"config":{{"nested":[-{},{}]}}}}"#,
                        maximum, maximum
                    )))?,
            )
            .await?;
        assert_eq!(safe_response.status(), StatusCode::CREATED);
        let created: CreateRunResponse = response_json(safe_response).await?;
        assert_eq!(
            created.run.config["nested"],
            serde_json::json!([-(maximum as i64), maximum])
        );

        for (project, integer) in [
            ("too-large", (maximum + 1).to_string()),
            ("too-small", format!("-{}", maximum + 1)),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::post(format!("/api/v1/projects/{project}/runs"))
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"config":{{"nested":[{integer}]}}}}"#
                        )))?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }
        Ok(())
    }

    #[test]
    fn run_and_blob_names_reject_controls_and_path_separators() {
        let request = CreateRunRequest {
            id: None,
            name: Some("invalid\nname".to_owned()),
            config: BTreeMap::new(),
            resume: ResumePolicy::Never,
            sweep_trial_id: None,
        };
        assert!(super::validate_create_run(&request).is_err());
        assert!(super::validate_file_name(Some("nested/file.mp4")).is_err());
        assert!(super::validate_file_name(Some("nested\\file.mp4")).is_err());
        assert!(super::validate_file_name(Some("file.mp4")).is_ok());
        assert_eq!(
            super::percent_decode_utf8("policy_%EC%A0%95%EC%B1%85.bin", "file name")
                .expect("valid percent-encoded UTF-8"),
            "policy_정책.bin"
        );
        for invalid in ["broken%", "%GG", "%FF"] {
            assert!(super::percent_decode_utf8(invalid, "file name").is_err());
        }
    }

    #[tokio::test]
    async fn metric_ingest_accepts_boolean_scalars_as_zero_or_one()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let created: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/boolean-metrics/runs",
                    &CreateRunRequest {
                        id: None,
                        name: None,
                        config: BTreeMap::new(),
                        resume: ResumePolicy::Never,
                        sweep_trial_id: None,
                    },
                )?)
                .await?,
        )
        .await?;
        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/runs/{}/batches", created.run.id))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&serde_json::json!({
                "batch_sequence": 1,
                "points": [{
                    "sequence": 1,
                    "step": 0,
                    "timestamp_ms": 1,
                    "metrics": {"disabled": false, "enabled": true}
                }]
            }))?))?;
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        let history: HistoryResponse = response_json(
            router
                .oneshot(
                    Request::get(format!(
                        "/api/v1/runs/{}/history?key=disabled&key=enabled&limit=10",
                        created.run.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(history.metrics["disabled"], vec![Some(0.0)]);
        assert_eq!(history.metrics["enabled"], vec![Some(1.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn health_checks_the_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics_root = directory.path().join("metrics");
        std::fs::create_dir_all(&metrics_root)?;
        std::fs::create_dir_all(directory.path().join("blobs"))?;
        let router = app(catalog, MetricStore::new(metrics_root));
        let response = router
            .clone()
            .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let health: HealthResponse = serde_json::from_slice(&body)?;
        assert_eq!(health.status, HealthStatus::Healthy);
        let diagnostics: DiagnosticsResponse = response_json(
            router
                .clone()
                .oneshot(Request::get("/api/v1/diagnostics").body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(diagnostics.requests_total, 2);
        assert_eq!(diagnostics.requests_active, 1);
        assert_eq!(diagnostics.requests_rejected_total, 0);
        assert_eq!(
            diagnostics.request_admission_limit,
            super::REQUEST_ADMISSION_LIMIT
        );
        assert_eq!(
            diagnostics.request_admission_permits_available,
            super::REQUEST_ADMISSION_LIMIT - 1
        );
        assert_eq!(
            diagnostics.health_admission_limit,
            super::HEALTH_ADMISSION_LIMIT
        );
        assert_eq!(
            diagnostics.health_admission_permits_available,
            super::HEALTH_ADMISSION_LIMIT
        );
        assert_eq!(
            diagnostics.blob_upload_permits_available,
            BLOB_UPLOAD_WORKERS
        );
        assert_eq!(
            diagnostics.artifact_io_permits_available,
            super::ARTIFACT_IO_WORKERS
        );
        assert_eq!(
            diagnostics.download_stream_limit,
            super::DOWNLOAD_STREAM_LIMIT
        );
        assert_eq!(
            diagnostics.download_stream_permits_available,
            super::DOWNLOAD_STREAM_LIMIT
        );
        assert_eq!(diagnostics.query_permits_available, super::QUERY_WORKERS);
        assert_eq!(diagnostics.storage_roots.len(), 3);
        assert!(diagnostics.storage_roots.iter().all(|root| {
            !root.path.is_empty()
                && root.total_bytes >= root.free_bytes
                && root.free_bytes >= root.available_bytes
        }));
        #[cfg(unix)]
        assert!(diagnostics.storage_roots.iter().all(|root| {
            root.device_id
                .as_ref()
                .is_some_and(|value| !value.is_empty())
        }));
        std::fs::remove_dir_all(directory.path().join("blobs"))?;
        let unhealthy = router
            .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
            .await?;
        assert_eq!(unhealthy.status(), StatusCode::SERVICE_UNAVAILABLE);
        Ok(())
    }

    #[cfg(feature = "embedded-dashboard")]
    #[tokio::test]
    async fn embedded_dashboard_serves_assets_and_spa_routes()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        for path in ["/", "/projects/demo"] {
            let response = router
                .clone()
                .oneshot(Request::get(path).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get("cache-control")
                    .and_then(|value| value.to_str().ok()),
                Some("no-cache")
            );
            assert_eq!(
                response
                    .headers()
                    .get("x-content-type-options")
                    .and_then(|value| value.to_str().ok()),
                Some("nosniff")
            );
            assert!(response.headers().contains_key("content-security-policy"));
            let body = to_bytes(response.into_body(), 1024 * 1024).await?;
            assert!(String::from_utf8_lossy(&body).contains("<title>Runloom</title>"));
        }
        let missing_asset = router
            .clone()
            .oneshot(Request::get("/missing.js").body(Body::empty())?)
            .await?;
        assert_eq!(missing_asset.status(), StatusCode::NOT_FOUND);
        let missing_api = router
            .oneshot(Request::get("/api/v1/missing").body(Body::empty())?)
            .await?;
        assert_eq!(missing_api.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn api_and_health_admission_shed_without_reading_bodies_and_exempt_static()
    -> Result<(), Box<dyn std::error::Error>> {
        async fn parse_json(Json(_): Json<serde_json::Value>) -> StatusCode {
            StatusCode::OK
        }

        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let state = AppState::new(
            catalog,
            MetricRuntime::new(MetricStore::new(directory.path().join("metrics"))),
            BlobStore::new(directory.path().join("blobs")),
            Arc::new(Mutex::new(ChartAxisExtentCache::default())),
            Arc::new(RequestTelemetry::from_environment()),
        );
        let router = Router::new()
            .route("/api/v1/heavy", post(parse_json))
            .route("/api/v1/health", get(|| async { StatusCode::OK }))
            .route("/dashboard.js", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                super::admit_api_request,
            ));
        let held = Arc::clone(&state.request_admission)
            .acquire_many_owned(super::REQUEST_ADMISSION_LIMIT as u32)
            .await?;

        let pending_body =
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
        let rejected = tokio::time::timeout(
            Duration::from_millis(100),
            router.clone().oneshot(
                Request::post("/api/v1/heavy")
                    .header("content-type", "application/json")
                    .body(pending_body)?,
            ),
        )
        .await??;
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            rejected.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        let error: runloom_protocol::ApiError = response_json(rejected).await?;
        assert_eq!(error.code, "server_busy");
        for path in ["/api/v1/health", "/dashboard.js"] {
            let response = router
                .clone()
                .oneshot(Request::get(path).body(Body::empty())?)
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
        }
        let held_health = Arc::clone(&state.health_admission)
            .acquire_many_owned(super::HEALTH_ADMISSION_LIMIT as u32)
            .await?;
        let rejected_health = router
            .clone()
            .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
            .await?;
        assert_eq!(rejected_health.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            state
                .telemetry
                .requests_rejected_total
                .load(Ordering::Relaxed),
            2
        );

        drop(held_health);
        drop(held);
        let accepted = router
            .oneshot(
                Request::post("/api/v1/heavy")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))?,
            )
            .await?;
        assert_eq!(accepted.status(), StatusCode::OK);
        assert_eq!(
            state.request_admission.available_permits(),
            super::REQUEST_ADMISSION_LIMIT - 1
        );
        drop(accepted);
        assert_eq!(
            state.request_admission.available_permits(),
            super::REQUEST_ADMISSION_LIMIT
        );
        Ok(())
    }

    #[tokio::test]
    async fn request_telemetry_keeps_bounded_slow_history_diagnostics()
    -> Result<(), Box<dyn std::error::Error>> {
        let telemetry = Arc::new(super::RequestTelemetry::new(Duration::ZERO));
        let router = axum::Router::new()
            .route(
                "/api/v1/runs/example/history",
                axum::routing::get(|| async {}),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&telemetry),
                super::record_request,
            ));
        let response = router
            .oneshot(Request::get("/api/v1/runs/example/history").body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            telemetry
                .requests_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            telemetry
                .requests_active
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            telemetry
                .slow_requests_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            telemetry
                .history_queries_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        let recent = telemetry
            .recent_slow_requests
            .lock()
            .map_err(|_| std::io::Error::other("slow request diagnostics lock was poisoned"))?;
        assert_eq!(recent.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn public_run_query_filters_documents_and_paginates()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let mut created_ids = Vec::new();
        for (name, seed) in [("alpha", 1), ("beta", 2), ("beta-eval", 2)] {
            let mut config = BTreeMap::from([
                ("seed".to_owned(), seed.into()),
                ("nullable".to_owned(), serde_json::Value::Null),
            ]);
            if name == "alpha" {
                config.extend([
                    ("literal.dot".to_owned(), "dot".into()),
                    ("literal\"quote".to_owned(), "quote".into()),
                    ("literal\\slash".to_owned(), "slash".into()),
                ]);
            }
            let created: CreateRunResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/query-demo/runs",
                        &CreateRunRequest {
                            id: None,
                            name: Some(name.to_owned()),
                            config,
                            resume: ResumePolicy::Never,
                            sweep_trial_id: None,
                        },
                    )?)
                    .await?,
            )
            .await?;
            created_ids.push(created.run.id);
        }

        let special_metrics = BTreeMap::from([
            ("summary.dot".to_owned(), 1.0),
            ("summary\"quote".to_owned(), 2.0),
            ("summary\\slash".to_owned(), 3.0),
            ("shadow\\key".to_owned(), 4.0),
        ]);
        let metric_response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/runs/{}/batches", created_ids[0]),
                &IngestBatchRequest {
                    batch_sequence: 1,
                    points: vec![MetricPoint {
                        sequence: 1,
                        step: 0,
                        timestamp_ms: 1,
                        metrics: special_metrics,
                    }],
                },
            )?)
            .await?;
        assert_eq!(metric_response.status(), StatusCode::CREATED);
        let summary_response = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &format!("/api/v1/runs/{}/summary", created_ids[0]),
                &SummaryUpdateRequest {
                    updates: BTreeMap::from([
                        ("summary.dot".to_owned(), "dot".into()),
                        ("summary\"quote".to_owned(), "quote".into()),
                        ("summary\\slash".to_owned(), "slash".into()),
                        ("shadow\\key".to_owned(), "explicit".into()),
                    ]),
                },
            )?)
            .await?;
        assert_eq!(summary_response.status(), StatusCode::OK);

        for (label, config_equals, summary_equals) in [
            (
                "config dot",
                BTreeMap::from([("literal.dot".to_owned(), "dot".into())]),
                BTreeMap::new(),
            ),
            (
                "config quote",
                BTreeMap::from([("literal\"quote".to_owned(), "quote".into())]),
                BTreeMap::new(),
            ),
            (
                "config backslash",
                BTreeMap::from([("literal\\slash".to_owned(), "slash".into())]),
                BTreeMap::new(),
            ),
            (
                "summary dot",
                BTreeMap::new(),
                BTreeMap::from([("summary.dot".to_owned(), "dot".into())]),
            ),
            (
                "summary quote",
                BTreeMap::new(),
                BTreeMap::from([("summary\"quote".to_owned(), "quote".into())]),
            ),
            (
                "summary backslash",
                BTreeMap::new(),
                BTreeMap::from([("summary\\slash".to_owned(), "slash".into())]),
            ),
            (
                "explicit precedence",
                BTreeMap::new(),
                BTreeMap::from([("shadow\\key".to_owned(), "explicit".into())]),
            ),
        ] {
            let special: RunQueryResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/query/runs",
                        &RunQueryRequest {
                            project: Some("query-demo".to_owned()),
                            run_ids: Vec::new(),
                            state: None,
                            name: None,
                            name_contains: None,
                            config_equals,
                            summary_equals,
                            before: None,
                            limit: 10,
                        },
                    )?)
                    .await?,
            )
            .await?;
            assert_eq!(special.runs.len(), 1, "failed {label}");
            assert_eq!(special.runs[0].id, created_ids[0], "failed {label}");
        }
        let shadowed: RunQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/query/runs",
                    &RunQueryRequest {
                        project: Some("query-demo".to_owned()),
                        run_ids: Vec::new(),
                        state: None,
                        name: None,
                        name_contains: None,
                        config_equals: BTreeMap::new(),
                        summary_equals: BTreeMap::from([("shadow\\key".to_owned(), 4.0.into())]),
                        before: None,
                        limit: 10,
                    },
                )?)
                .await?,
        )
        .await?;
        assert!(shadowed.runs.is_empty());

        let filtered: RunQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/query/runs",
                    &RunQueryRequest {
                        project: Some("query-demo".to_owned()),
                        run_ids: Vec::new(),
                        state: Some(RunState::Running),
                        name: None,
                        name_contains: Some("beta".to_owned()),
                        config_equals: BTreeMap::from([
                            ("seed".to_owned(), 2.into()),
                            ("nullable".to_owned(), serde_json::Value::Null),
                        ]),
                        summary_equals: BTreeMap::new(),
                        before: None,
                        limit: 10,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(filtered.runs.len(), 2);
        assert!(filtered.runs.iter().all(|run| run.name.contains("beta")));

        let first_page: RunQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/query/runs",
                    &RunQueryRequest {
                        project: Some("query-demo".to_owned()),
                        run_ids: Vec::new(),
                        state: None,
                        name: None,
                        name_contains: None,
                        config_equals: BTreeMap::new(),
                        summary_equals: BTreeMap::new(),
                        before: None,
                        limit: 2,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(first_page.runs.len(), 2);
        let cursor = first_page.next_before.expect("a full page has a cursor");
        let second_page: RunQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/query/runs",
                    &RunQueryRequest {
                        project: Some("query-demo".to_owned()),
                        run_ids: Vec::new(),
                        state: None,
                        name: None,
                        name_contains: None,
                        config_equals: BTreeMap::new(),
                        summary_equals: BTreeMap::new(),
                        before: Some(cursor),
                        limit: 2,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(second_page.runs.len(), 1);
        assert!(
            first_page
                .runs
                .iter()
                .all(|run| !second_page.runs.iter().any(|other| other.id == run.id))
        );
        assert_eq!(created_ids.len(), 3);

        let exact: RunQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/query/runs",
                    &RunQueryRequest {
                        project: Some("query-demo".to_owned()),
                        run_ids: vec![created_ids[0], created_ids[2]],
                        state: None,
                        name: None,
                        name_contains: None,
                        config_equals: BTreeMap::new(),
                        summary_equals: BTreeMap::new(),
                        before: None,
                        limit: 2,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(exact.runs.len(), 2);
        assert_eq!(exact.next_before, None);
        assert!(
            exact
                .runs
                .iter()
                .all(|run| run.id == created_ids[0] || run.id == created_ids[2])
        );

        let invalid_cursor = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/query/runs",
                &RunQueryRequest {
                    project: Some("query-demo".to_owned()),
                    run_ids: vec![created_ids[0]],
                    state: None,
                    name: None,
                    name_contains: None,
                    config_equals: BTreeMap::new(),
                    summary_equals: BTreeMap::new(),
                    before: Some(created_ids[1]),
                    limit: 1,
                },
            )?)
            .await?;
        assert_eq!(invalid_cursor.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    }

    #[tokio::test]
    async fn project_metric_catalog_is_searchable_and_limit_plus_one_paginated()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let mut runs = Vec::new();
        for name in ["first", "second"] {
            let created: CreateRunResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/metric-catalog/runs",
                        &CreateRunRequest {
                            id: None,
                            name: Some(name.to_owned()),
                            config: BTreeMap::new(),
                            resume: ResumePolicy::Never,
                            sweep_trial_id: None,
                        },
                    )?)
                    .await?,
            )
            .await?;
            runs.push(created.run.id);
        }
        for (run_id, metrics) in [
            (
                runs[0],
                BTreeMap::from([("loss".to_owned(), 1.0), ("reward".to_owned(), 2.0)]),
            ),
            (
                runs[1],
                BTreeMap::from([("loss".to_owned(), 3.0), ("throughput".to_owned(), 4.0)]),
            ),
        ] {
            let response = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/api/v1/runs/{run_id}/batches"),
                    &IngestBatchRequest {
                        batch_sequence: 1,
                        points: vec![MetricPoint {
                            sequence: 1,
                            step: 0,
                            timestamp_ms: 1,
                            metrics,
                        }],
                    },
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let first: ProjectMetricCatalogResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/metric-catalog/metrics/query",
                    &ProjectMetricCatalogRequest {
                        run_ids: runs.clone(),
                        mode: MetricCatalogMode::Union,
                        search: None,
                        after: None,
                        limit: 1,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(first.keys.len(), 1);
        assert_eq!(first.keys[0].key, "loss");
        assert_eq!(first.keys[0].run_ids.len(), 2);
        assert_eq!(first.next_after.as_deref(), Some("loss"));

        let second: ProjectMetricCatalogResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/metric-catalog/metrics/query",
                    &ProjectMetricCatalogRequest {
                        run_ids: runs.clone(),
                        mode: MetricCatalogMode::Union,
                        search: None,
                        after: first.next_after,
                        limit: 2,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(
            second
                .keys
                .iter()
                .map(|summary| summary.key.as_str())
                .collect::<Vec<_>>(),
            vec!["reward", "throughput"]
        );
        assert_eq!(second.next_after, None);

        let intersection: ProjectMetricCatalogResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/metric-catalog/metrics/query",
                    &ProjectMetricCatalogRequest {
                        run_ids: runs.clone(),
                        mode: MetricCatalogMode::Intersection,
                        search: Some("LOSS".to_owned()),
                        after: None,
                        limit: 10,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(intersection.keys.len(), 1);
        assert_eq!(intersection.keys[0].key, "loss");

        let foreign_cursor = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/projects/metric-catalog/metrics/query",
                &ProjectMetricCatalogRequest {
                    run_ids: runs,
                    mode: MetricCatalogMode::Union,
                    search: None,
                    after: Some("unknown".to_owned()),
                    limit: 10,
                },
            )?)
            .await?;
        assert_eq!(foreign_cursor.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn public_inputs_reject_unknown_fields() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let unknown_body = router
            .clone()
            .oneshot(
                Request::post("/api/v1/projects/strict/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"strict","unsupported_resume_alias":true}"#,
                    ))?,
            )
            .await?;
        assert!(unknown_body.status().is_client_error());

        let unknown_query = router
            .oneshot(Request::get("/api/v1/projects?unbounded=true").body(Body::empty())?)
            .await?;
        assert!(unknown_query.status().is_client_error());
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_stable_run_and_artifact_creates_are_deterministic()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));

        let stable_id = RunId::new();
        let stable_request = CreateRunRequest {
            id: Some(stable_id),
            name: Some("stable".to_owned()),
            config: BTreeMap::new(),
            resume: ResumePolicy::Allow,
            sweep_trial_id: None,
        };
        let create_a = router.clone().oneshot(json_request(
            "POST",
            "/api/v1/projects/concurrent-create/runs",
            &stable_request,
        )?);
        let create_b = router.clone().oneshot(json_request(
            "POST",
            "/api/v1/projects/concurrent-create/runs",
            &stable_request,
        )?);
        let (response_a, response_b) = tokio::join!(create_a, create_b);
        let response_a = response_a?;
        let response_b = response_b?;
        assert!(
            (response_a.status() == StatusCode::CREATED && response_b.status() == StatusCode::OK)
                || (response_b.status() == StatusCode::CREATED
                    && response_a.status() == StatusCode::OK)
        );
        let created_a: CreateRunResponse = response_json(response_a).await?;
        let created_b: CreateRunResponse = response_json(response_b).await?;
        assert_eq!(created_a.run.id, stable_id);
        assert_eq!(created_b.run.id, stable_id);
        let project: ProjectSummary = response_json(
            router
                .clone()
                .oneshot(Request::get("/api/v1/projects/concurrent-create").body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(project.run_count, 1);

        let mut artifact_runs = Vec::new();
        for name in ["producer-a", "producer-b"] {
            let created: CreateRunResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/artifact-race/runs",
                        &CreateRunRequest {
                            id: None,
                            name: Some(name.to_owned()),
                            config: BTreeMap::new(),
                            resume: ResumePolicy::Never,
                            sweep_trial_id: None,
                        },
                    )?)
                    .await?,
            )
            .await?;
            artifact_runs.push(created.run.id);
        }
        let artifact_a = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "policy".to_owned(),
            artifact_type: "model".to_owned(),
            version: None,
            description: None,
            metadata: BTreeMap::new(),
            aliases: Vec::new(),
            entries: Vec::new(),
        };
        let artifact_b = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            ..artifact_a.clone()
        };
        let create_a = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/artifacts", artifact_runs[0]),
            &artifact_a,
        )?);
        let create_b = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/artifacts", artifact_runs[1]),
            &artifact_b,
        )?);
        let (response_a, response_b) = tokio::join!(create_a, create_b);
        let response_a = response_a?;
        let response_b = response_b?;
        assert_eq!(response_a.status(), StatusCode::CREATED);
        assert_eq!(response_b.status(), StatusCode::CREATED);
        let created_a: CreateArtifactResponse = response_json(response_a).await?;
        let created_b: CreateArtifactResponse = response_json(response_b).await?;
        let mut versions = vec![created_a.artifact.version, created_b.artifact.version];
        versions.sort_unstable();
        assert_eq!(versions, vec![0, 1]);

        let model = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "checkpoint".to_owned(),
            artifact_type: "model".to_owned(),
            version: None,
            description: None,
            metadata: BTreeMap::new(),
            aliases: Vec::new(),
            entries: Vec::new(),
        };
        let dataset = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            artifact_type: "dataset".to_owned(),
            ..model.clone()
        };
        let create_model = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/artifacts", artifact_runs[0]),
            &model,
        )?);
        let create_dataset = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/artifacts", artifact_runs[1]),
            &dataset,
        )?);
        let (create_model, create_dataset) = tokio::join!(create_model, create_dataset);
        let mut statuses = [create_model?.status(), create_dataset?.status()];
        statuses.sort();
        assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
        Ok(())
    }

    #[tokio::test]
    async fn explicit_artifact_versions_are_exact_and_aliases_never_regress()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let mut run_ids = Vec::new();
        for name in ["producer-a", "producer-b"] {
            let (run, _) = catalog
                .create_or_resume_run(
                    "artifact-backfill",
                    &CreateRunRequest {
                        id: None,
                        name: Some(name.to_owned()),
                        config: BTreeMap::new(),
                        resume: ResumePolicy::Never,
                        sweep_trial_id: None,
                    },
                )
                .await?;
            run_ids.push(run.id);
        }
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let path_a = format!("/api/v1/runs/{}/artifacts", run_ids[0]);
        let path_b = format!("/api/v1/runs/{}/artifacts", run_ids[1]);

        let newer = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "policy".to_owned(),
            artifact_type: "model".to_owned(),
            version: Some(9),
            description: None,
            metadata: BTreeMap::new(),
            aliases: vec!["latest".to_owned()],
            entries: Vec::new(),
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &path_a, &newer)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created_newer: CreateArtifactResponse = response_json(response).await?;
        assert_eq!(created_newer.artifact.version, 9);

        let older = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            version: Some(3),
            ..newer.clone()
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &path_b, &older)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created_older: CreateArtifactResponse = response_json(response).await?;
        assert_eq!(created_older.artifact.version, 3);
        assert!(created_older.artifact.aliases.is_empty());

        let alias_path = "/api/v1/projects/artifact-backfill/artifacts/policy/aliases/latest";
        let resolved: runloom_protocol::ArtifactRecord = response_json(
            router
                .clone()
                .oneshot(Request::get(alias_path).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(resolved.id, created_newer.artifact.id);

        let replay = router
            .clone()
            .oneshot(json_request("POST", &path_b, &older)?)
            .await?;
        assert_eq!(replay.status(), StatusCode::OK);
        assert!(
            response_json::<CreateArtifactResponse>(replay)
                .await?
                .duplicate
        );

        let occupied = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            ..older.clone()
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &path_a, &occupied)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let changed_replay = CreateArtifactRequest {
            version: Some(10),
            ..newer.clone()
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &path_a, &changed_replay)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let automatic = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            version: None,
            ..newer.clone()
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &path_a, &automatic)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created_automatic: CreateArtifactResponse = response_json(response).await?;
        assert_eq!(created_automatic.artifact.version, 10);
        let resolved: runloom_protocol::ArtifactRecord = response_json(
            router
                .clone()
                .oneshot(Request::get(alias_path).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(resolved.id, created_automatic.artifact.id);

        let low = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "concurrent-policy".to_owned(),
            version: Some(2),
            ..newer.clone()
        };
        let high = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            version: Some(7),
            ..low.clone()
        };
        let create_low = router.clone().oneshot(json_request("POST", &path_a, &low)?);
        let create_high = router
            .clone()
            .oneshot(json_request("POST", &path_b, &high)?);
        let (created_low, created_high) = tokio::join!(create_low, create_high);
        assert_eq!(created_low?.status(), StatusCode::CREATED);
        assert_eq!(created_high?.status(), StatusCode::CREATED);
        let resolved: runloom_protocol::ArtifactRecord = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(
                        "/api/v1/projects/artifact-backfill/artifacts/concurrent-policy/aliases/latest",
                    )
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(resolved.id, high.id.expect("explicit artifact ID"));

        let unsafe_version = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "unsafe-version".to_owned(),
            version: Some(runloom_protocol::MAX_JSON_SAFE_INTEGER + 1),
            ..newer
        };
        let response = router
            .oneshot(json_request("POST", &path_a, &unsafe_version)?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        Ok(())
    }

    #[tokio::test]
    async fn artifact_verification_waits_on_a_bounded_cancelable_io_pool()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics = MetricRuntime::new(MetricStore::new(directory.path().join("metrics")));
        let blobs = BlobStore::new(directory.path().join("blobs"));
        blobs.ensure()?;
        let state = AppState::new(
            catalog.clone(),
            metrics,
            blobs,
            Arc::new(Mutex::new(ChartAxisExtentCache::default())),
            Arc::new(RequestTelemetry::from_environment()),
        );
        let (run, _) = catalog
            .create_or_resume_run(
                "artifact-io",
                &CreateRunRequest {
                    id: None,
                    name: Some("producer".to_owned()),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                    sweep_trial_id: None,
                },
            )
            .await?;
        let held = Arc::clone(&state.artifact_io_permits)
            .acquire_many_owned(super::ARTIFACT_IO_WORKERS as u32)
            .await?;
        let state_for_create = state.clone();
        let create = tokio::spawn(async move {
            create_artifact(
                State(state_for_create),
                Path(run.id),
                Json(CreateArtifactRequest {
                    id: Some(runloom_protocol::ArtifactId::new()),
                    name: "blocked".to_owned(),
                    artifact_type: "model".to_owned(),
                    version: None,
                    description: None,
                    metadata: BTreeMap::new(),
                    aliases: Vec::new(),
                    entries: Vec::new(),
                }),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!create.is_finished());
        create.abort();
        let _ = create.await;
        drop(held);
        assert!(
            catalog
                .list_project_artifacts("artifact-io", None, 10)
                .await?
                .is_empty()
        );
        assert_eq!(
            state.artifact_io_permits.available_permits(),
            super::ARTIFACT_IO_WORKERS
        );
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_blob_puts_report_one_idempotent_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let content = b"concurrent-content-addressed-blob";
        let digest = format!("{:x}", Sha256::digest(content));
        let path = format!("/api/v1/blobs/{digest}");
        let upload_a = router.clone().oneshot(
            Request::put(&path)
                .header("content-length", content.len())
                .header("x-runloom-file-name", "policy_%EC%A0%95%EC%B1%85.bin")
                .body(Body::from(content.as_slice()))?,
        );
        let upload_b = router.clone().oneshot(
            Request::put(&path)
                .header("content-length", content.len())
                .header("x-runloom-file-name", "policy_%EC%A0%95%EC%B1%85.bin")
                .body(Body::from(content.as_slice()))?,
        );
        let (upload_a, upload_b) = tokio::join!(upload_a, upload_b);
        let upload_a = upload_a?;
        let upload_b = upload_b?;
        assert!(
            (upload_a.status() == StatusCode::CREATED && upload_b.status() == StatusCode::OK)
                || (upload_b.status() == StatusCode::CREATED
                    && upload_a.status() == StatusCode::OK)
        );
        let uploaded_a: BlobUploadResponse = response_json(upload_a).await?;
        let uploaded_b: BlobUploadResponse = response_json(upload_b).await?;
        assert_ne!(uploaded_a.duplicate, uploaded_b.duplicate);
        assert_eq!(uploaded_a.blob.digest, digest);
        assert_eq!(uploaded_b.blob.digest, digest);
        assert_eq!(
            uploaded_a.blob.file_name.as_deref(),
            Some("policy_정책.bin")
        );
        assert_eq!(uploaded_b.blob.file_name, uploaded_a.blob.file_name);
        Ok(())
    }

    #[tokio::test]
    async fn download_stream_admission_sheds_and_holds_permits_until_body_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let blobs = BlobStore::new(directory.path().join("blobs"));
        let content = b"bounded-download-stream";
        let digest = format!("{:x}", Sha256::digest(content));
        let mut staging = blobs.staging_file()?;
        std::fs::write(staging.path(), content)?;
        blobs.install(staging.path(), &digest)?;
        staging.disarm();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));

        let response = super::serve_blob(
            &blobs,
            &permits,
            &digest,
            None,
            Request::get("/").body(Body::empty())?,
        )
        .await
        .map_err(|error| std::io::Error::other(error.body.message))?;
        assert_eq!(permits.available_permits(), 0);
        drop(response);
        assert_eq!(permits.available_permits(), 1);

        let response = super::serve_blob(
            &blobs,
            &permits,
            &digest,
            None,
            Request::get("/").body(Body::empty())?,
        )
        .await
        .map_err(|error| std::io::Error::other(error.body.message))?;
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(
            to_bytes(response.into_body(), 1024).await?,
            content.as_slice()
        );
        assert_eq!(permits.available_permits(), 1);

        let held = Arc::clone(&permits).acquire_owned().await?;
        let error = super::serve_blob(
            &blobs,
            &permits,
            &digest,
            None,
            Request::get("/").body(Body::empty())?,
        )
        .await
        .expect_err("a full download pool must shed immediately");
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        drop(held);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_writer_contention_is_retryable_http_overload()
    -> Result<(), Box<dyn std::error::Error>> {
        let response = super::HttpError::from(CatalogError::Busy(
            "(code: 517) database is locked".to_owned(),
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        let error: ApiError = response_json(response).await?;
        assert_eq!(error.code, "server_busy");
        Ok(())
    }

    #[tokio::test]
    async fn blob_upload_concurrency_is_bounded_and_cancellation_cleans_staging()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics = MetricRuntime::new(MetricStore::new(directory.path().join("metrics")));
        let blobs = BlobStore::new(directory.path().join("blobs"));
        blobs.ensure()?;
        let state = AppState::new(
            catalog,
            metrics,
            blobs.clone(),
            Arc::new(Mutex::new(ChartAxisExtentCache::default())),
            Arc::new(RequestTelemetry::from_environment()),
        );
        let held = Arc::clone(&state.blob_upload_permits)
            .acquire_many_owned((BLOB_UPLOAD_WORKERS - 1) as u32)
            .await?;
        let mut uploads = Vec::new();
        for digest in ["a".repeat(64), "b".repeat(64)] {
            let state = state.clone();
            uploads.push(tokio::spawn(async move {
                let body = Body::from_stream(futures_util::stream::pending::<
                    Result<Bytes, std::io::Error>,
                >());
                upload_blob(State(state), Path(digest), HeaderMap::new(), body).await
            }));
        }
        let staging_dir = blobs.root().join("staging");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let staged = std::fs::read_dir(&staging_dir)
                    .map(|entries| entries.filter_map(Result::ok).count())
                    .unwrap_or(0);
                if staged == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(state.blob_upload_permits.available_permits(), 0);

        for upload in &uploads {
            upload.abort();
        }
        for upload in uploads {
            let _ = upload.await;
        }
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let staged = std::fs::read_dir(&staging_dir)
                    .map(|entries| entries.filter_map(Result::ok).count())
                    .unwrap_or(0);
                if staged == 0
                    && state.blob_upload_permits.available_permits() == BLOB_UPLOAD_WORKERS
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn finish_is_serialized_with_config_and_resource_mutations()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let mut run_ids = Vec::new();
        for name in ["config-race", "resource-race"] {
            let created: CreateRunResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/mutation-races/runs",
                        &CreateRunRequest {
                            id: None,
                            name: Some(name.to_owned()),
                            config: BTreeMap::new(),
                            resume: ResumePolicy::Never,
                            sweep_trial_id: None,
                        },
                    )?)
                    .await?,
            )
            .await?;
            run_ids.push(created.run.id);
        }

        let finish = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/finish", run_ids[0]),
            &FinishRunRequest {
                summary: BTreeMap::new(),
            },
        )?);
        let config = router.clone().oneshot(json_request(
            "PATCH",
            &format!("/api/v1/runs/{}/config", run_ids[0]),
            &ConfigUpdateRequest {
                updates: BTreeMap::from([("seed".to_owned(), 1.into())]),
                allow_val_change: false,
            },
        )?);
        let (finish, config) = tokio::join!(finish, config);
        let finish = finish?;
        let config = config?;
        assert_eq!(finish.status(), StatusCode::OK);
        assert!(matches!(
            config.status(),
            StatusCode::OK | StatusCode::CONFLICT
        ));

        let finish = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/finish", run_ids[1]),
            &FinishRunRequest {
                summary: BTreeMap::new(),
            },
        )?);
        let alert = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/runs/{}/alerts", run_ids[1]),
            &CreateAlertRequest {
                id: Some(AlertId::new()),
                title: "race".to_owned(),
                text: "race".to_owned(),
                level: AlertLevel::Info,
                step: None,
                timestamp_ms: 1,
            },
        )?);
        let (finish, alert) = tokio::join!(finish, alert);
        let finish = finish?;
        let alert = alert?;
        assert_eq!(finish.status(), StatusCode::OK);
        assert!(matches!(
            alert.status(),
            StatusCode::CREATED | StatusCode::CONFLICT
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sweep_trial_binding_and_terminal_mutations_are_serialized()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let sweep: CreateSweepResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/sweep-race/sweeps",
                    &CreateSweepRequest {
                        id: None,
                        name: Some("binding".to_owned()),
                        method: SweepMethod::Grid,
                        metric: SweepMetric {
                            name: "loss".to_owned(),
                            goal: MetricGoal::Minimize,
                        },
                        parameters: BTreeMap::from([(
                            "seed".to_owned(),
                            SweepParameter {
                                values: vec![1.into()],
                            },
                        )]),
                        max_runs: 1,
                        early_terminate: None,
                    },
                )?)
                .await?,
        )
        .await?;
        let agent_id = "race-agent";
        let claim: ClaimSweepTrialResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/api/v1/sweeps/{}/claim", sweep.sweep.id),
                    &ClaimSweepTrialRequest {
                        agent_id: agent_id.to_owned(),
                    },
                )?)
                .await?,
        )
        .await?;
        let trial = claim.trial.expect("grid sweep has one trial");

        let run_request_a = CreateRunRequest {
            id: Some(RunId::new()),
            name: Some("trial-a".to_owned()),
            config: trial.config.clone(),
            resume: ResumePolicy::Never,
            sweep_trial_id: Some(trial.id),
        };
        let run_request_b = CreateRunRequest {
            id: Some(RunId::new()),
            name: Some("trial-b".to_owned()),
            ..run_request_a.clone()
        };
        let bind_a = router.clone().oneshot(json_request(
            "POST",
            "/api/v1/projects/sweep-race/runs",
            &run_request_a,
        )?);
        let bind_b = router.clone().oneshot(json_request(
            "POST",
            "/api/v1/projects/sweep-race/runs",
            &run_request_b,
        )?);
        let (bind_a, bind_b) = tokio::join!(bind_a, bind_b);
        let bind_a = bind_a?;
        let bind_b = bind_b?;
        assert!(
            (bind_a.status() == StatusCode::CREATED && bind_b.status() == StatusCode::CONFLICT)
                || (bind_b.status() == StatusCode::CREATED
                    && bind_a.status() == StatusCode::CONFLICT)
        );

        let complete = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/sweep-trials/{}/complete", trial.id),
            &CompleteSweepTrialRequest {
                agent_id: agent_id.to_owned(),
                state: SweepTrialState::Completed,
                metric: Some(0.5),
            },
        )?);
        let heartbeat = router.clone().oneshot(json_request(
            "POST",
            &format!("/api/v1/sweep-trials/{}/heartbeat", trial.id),
            &HeartbeatSweepTrialRequest {
                agent_id: agent_id.to_owned(),
            },
        )?);
        let (complete, heartbeat) = tokio::join!(complete, heartbeat);
        let complete = complete?;
        let heartbeat = heartbeat?;
        assert_eq!(complete.status(), StatusCode::OK);
        assert!(matches!(
            heartbeat.status(),
            StatusCode::OK | StatusCode::CONFLICT
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sweep_scheduler_claims_binds_and_requests_median_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let sweep_request = CreateSweepRequest {
            id: None,
            name: Some("optimizer-search".to_owned()),
            method: SweepMethod::Grid,
            metric: SweepMetric {
                name: "loss".to_owned(),
                goal: MetricGoal::Minimize,
            },
            parameters: BTreeMap::from([
                (
                    "learning_rate".to_owned(),
                    SweepParameter {
                        values: vec![0.1.into(), 0.01.into()],
                    },
                ),
                (
                    "seed".to_owned(),
                    SweepParameter {
                        values: vec![1.into(), 2.into()],
                    },
                ),
            ]),
            max_runs: 3,
            early_terminate: Some(EarlyTerminateConfig {
                min_step: 1,
                min_trials: 1,
            }),
        };
        let created: CreateSweepResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/sweep-demo/sweeps",
                    &sweep_request,
                )?)
                .await?,
        )
        .await?;
        assert!(!created.duplicate);
        let replay: CreateSweepResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/sweep-demo/sweeps",
                    &CreateSweepRequest {
                        id: Some(created.sweep.id),
                        ..sweep_request.clone()
                    },
                )?)
                .await?,
        )
        .await?;
        assert!(replay.duplicate);

        for (agent, loss, expected_stop) in [("agent-a", 0.1, false), ("agent-b", 1.0, true)] {
            let claim: ClaimSweepTrialResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        &format!("/api/v1/sweeps/{}/claim", created.sweep.id),
                        &ClaimSweepTrialRequest {
                            agent_id: agent.to_owned(),
                        },
                    )?)
                    .await?,
            )
            .await?;
            let trial = claim.trial.expect("available grid trial");
            let heartbeat: runloom_protocol::SweepTrialRecord = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        &format!("/api/v1/sweep-trials/{}/heartbeat", trial.id),
                        &HeartbeatSweepTrialRequest {
                            agent_id: agent.to_owned(),
                        },
                    )?)
                    .await?,
            )
            .await?;
            assert_eq!(heartbeat.agent_id, agent);
            let run: CreateRunResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/sweep-demo/runs",
                        &CreateRunRequest {
                            id: None,
                            name: Some(format!("run-{agent}")),
                            config: trial.config,
                            resume: ResumePolicy::Never,
                            sweep_trial_id: Some(trial.id),
                        },
                    )?)
                    .await?,
            )
            .await?;
            let accepted: IngestBatchResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        &format!("/api/v1/runs/{}/batches", run.run.id),
                        &IngestBatchRequest {
                            batch_sequence: 1,
                            points: vec![MetricPoint {
                                sequence: 1,
                                step: 1,
                                timestamp_ms: 1,
                                metrics: BTreeMap::from([("loss".to_owned(), loss)]),
                            }],
                        },
                    )?)
                    .await?,
            )
            .await?;
            assert_eq!(accepted.stop_requested, expected_stop);
            let terminal_state = if expected_stop {
                SweepTrialState::Stopped
            } else {
                SweepTrialState::Completed
            };
            let completed: runloom_protocol::SweepTrialRecord = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        &format!("/api/v1/sweep-trials/{}/complete", trial.id),
                        &CompleteSweepTrialRequest {
                            agent_id: agent.to_owned(),
                            state: terminal_state,
                            metric: Some(loss),
                        },
                    )?)
                    .await?,
            )
            .await?;
            assert_eq!(completed.state, terminal_state);
        }
        let trials: SweepTrialListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/sweeps/{}/trials?limit=1",
                        created.sweep.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(trials.trials.len(), 1);
        let encoded_trial_page = serde_json::to_value(&trials)?;
        assert!(encoded_trial_page["trials"][0].get("config").is_none());
        let trial_detail: runloom_protocol::SweepTrialRecord = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!("/api/v1/sweep-trials/{}", trials.trials[0].id))
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert!(!trial_detail.config.is_empty());
        let trial_cursor = trials.next_before.expect("full trial page has a cursor");
        let next_trials: SweepTrialListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/sweeps/{}/trials?limit=1&before={trial_cursor}",
                        created.sweep.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(next_trials.trials.len(), 1);
        assert_ne!(trials.trials[0].id, next_trials.trials[0].id);

        let sweeps: runloom_protocol::SweepListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get("/api/v1/projects/sweep-demo/sweeps?limit=10")
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(sweeps.sweeps[0].parameter_count, 2);
        let encoded_sweeps = serde_json::to_value(&sweeps)?;
        assert!(encoded_sweeps["sweeps"][0].get("parameters").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn reports_persist_bounded_dashboard_layouts() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let run: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/report-demo/runs",
                    &CreateRunRequest {
                        id: None,
                        name: Some("baseline".to_owned()),
                        config: BTreeMap::new(),
                        resume: ResumePolicy::Never,
                        sweep_trial_id: None,
                    },
                )?)
                .await?,
        )
        .await?;
        let layout = ReportLayout {
            columns: 2,
            panels: vec![
                ReportPanel {
                    id: "overview".to_owned(),
                    title: "Overview".to_owned(),
                    kind: ReportPanelKind::Markdown,
                    run_id: None,
                    metric_keys: Vec::new(),
                    markdown: Some("# Baseline\nStable rollout".to_owned()),
                    width: 2,
                    height: 180,
                },
                ReportPanel {
                    id: "loss".to_owned(),
                    title: "Training loss".to_owned(),
                    kind: ReportPanelKind::Metric,
                    run_id: Some(run.run.id),
                    metric_keys: vec!["loss".to_owned()],
                    markdown: None,
                    width: 1,
                    height: 320,
                },
            ],
        };
        let request = CreateReportRequest {
            id: None,
            name: "Training report".to_owned(),
            description: Some("Baseline dashboard".to_owned()),
            layout: layout.clone(),
        };
        let token_before = response_json::<ProjectSummary>(
            router
                .clone()
                .oneshot(Request::get("/api/v1/projects/report-demo").body(Body::empty())?)
                .await?,
        )
        .await?
        .mutation_token
        .parse::<u64>()?;
        let created: CreateReportResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/report-demo/reports",
                    &request,
                )?)
                .await?,
        )
        .await?;
        assert!(!created.duplicate);
        let token_after_create = response_json::<ProjectSummary>(
            router
                .clone()
                .oneshot(Request::get("/api/v1/projects/report-demo").body(Body::empty())?)
                .await?,
        )
        .await?
        .mutation_token
        .parse::<u64>()?;
        assert!(token_after_create > token_before);
        let replay: CreateReportResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/report-demo/reports",
                    &CreateReportRequest {
                        id: Some(created.report.id),
                        ..request
                    },
                )?)
                .await?,
        )
        .await?;
        assert!(replay.duplicate);
        let token_after_replay = response_json::<ProjectSummary>(
            router
                .clone()
                .oneshot(Request::get("/api/v1/projects/report-demo").body(Body::empty())?)
                .await?,
        )
        .await?
        .mutation_token
        .parse::<u64>()?;
        assert_eq!(token_after_replay, token_after_create);
        let reports: ReportListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get("/api/v1/projects/report-demo/reports?limit=1")
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(reports.reports.len(), 1);
        assert_eq!(reports.next_before, None);
        let updated: ReportRecord = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "PUT",
                    &format!("/api/v1/reports/{}", created.report.id),
                    &UpdateReportRequest {
                        name: "Updated report".to_owned(),
                        description: None,
                        layout,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(updated.name, "Updated report");
        let token_after_update = response_json::<ProjectSummary>(
            router
                .clone()
                .oneshot(Request::get("/api/v1/projects/report-demo").body(Body::empty())?)
                .await?,
        )
        .await?
        .mutation_token
        .parse::<u64>()?;
        assert!(token_after_update > token_after_replay);
        let deleted: ReportRecord = response_json(
            router
                .clone()
                .oneshot(
                    Request::delete(format!("/api/v1/reports/{}", created.report.id))
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(deleted.id, created.report.id);
        let token_after_delete = response_json::<ProjectSummary>(
            router
                .oneshot(Request::get("/api/v1/projects/report-demo").body(Body::empty())?)
                .await?,
        )
        .await?
        .mutation_token
        .parse::<u64>()?;
        assert!(token_after_delete > token_after_update);
        Ok(())
    }

    #[tokio::test]
    async fn chart_history_returns_exact_sparse_buckets_and_validates_viewports()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let created: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/charts/runs",
                    &CreateRunRequest {
                        id: None,
                        name: Some("nonmonotonic".to_owned()),
                        config: BTreeMap::new(),
                        resume: ResumePolicy::Never,
                        sweep_trial_id: None,
                    },
                )?)
                .await?,
        )
        .await?;
        let points = [
            (1, 4, Some(10.0), None),
            (2, 1, Some(-5.0), Some(100.0)),
            (3, 4, Some(7.0), None),
            (4, 0, None, Some(50.0)),
            (5, 8, Some(20.0), None),
            (6, 6, None, Some(-1.0)),
            (7, 5, Some(15.0), Some(2.0)),
        ]
        .into_iter()
        .map(|(sequence, step, a, b)| {
            let mut metrics = BTreeMap::new();
            if let Some(value) = a {
                metrics.insert("a".to_owned(), value);
            }
            if let Some(value) = b {
                metrics.insert("b".to_owned(), value);
            }
            MetricPoint {
                sequence,
                step,
                timestamp_ms: sequence as i64 * 10,
                metrics,
            }
        })
        .collect();
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/runs/{}/batches", created.run.id),
                &IngestBatchRequest {
                    batch_sequence: 1,
                    points,
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);

        let path = format!(
            "/api/v1/runs/{}/chart-history?key=a&key=b&key=missing&max_buckets=2",
            created.run.id
        );
        let response: ChartHistoryResponse = response_json(
            router
                .clone()
                .oneshot(Request::get(path).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(response.step_min, Some(0));
        assert_eq!(response.step_max, Some(8));
        assert_eq!(response.bucket_count, 2);
        assert_eq!(response.source_points, 7);
        assert_eq!(response.source_last_sequence, Some(7));
        assert_eq!(response.metrics["a"].minimum, vec![-5.0, 15.0]);
        assert_eq!(response.metrics["a"].maximum, vec![10.0, 20.0]);
        assert_eq!(response.metrics["a"].last, vec![7.0, 15.0]);
        assert_eq!(response.metrics["a"].last_step, vec![4, 5]);
        assert_eq!(response.metrics["b"].minimum, vec![50.0, -1.0]);
        assert_eq!(response.metrics["b"].maximum, vec![100.0, 2.0]);
        assert_eq!(response.metrics["b"].last, vec![50.0, 2.0]);
        assert_eq!(response.metrics["b"].last_step, vec![0, 5]);
        assert_eq!(response.metrics["missing"].source_points, 0);
        assert!(response.metrics["missing"].bucket.is_empty());

        let viewport_path = format!(
            "/api/v1/runs/{}/chart-history?key=a&key=b&max_buckets=1&step_min=1&step_max=4",
            created.run.id
        );
        let viewport: ChartHistoryResponse = response_json(
            router
                .clone()
                .oneshot(Request::get(viewport_path).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(viewport.source_points, 3);
        assert_eq!(viewport.metrics["a"].minimum, vec![-5.0]);
        assert_eq!(viewport.metrics["a"].maximum, vec![10.0]);
        assert_eq!(viewport.metrics["a"].last, vec![7.0]);
        assert_eq!(viewport.metrics["b"].last, vec![100.0]);

        let comma_key: ChartHistoryResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/runs/{}/chart-history?key=comma%2Ckey&step_min=0&step_max=1",
                        created.run.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert!(comma_key.metrics.contains_key("comma,key"));

        for query in [
            "key=a&step_min=1",
            "key=a&step_min=5&step_max=4",
            "key=a&max_buckets=0",
            "key=a&key=b&key=missing&max_buckets=2000",
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/runs/{}/chart-history?{query}",
                        created.run.id
                    ))
                    .body(Body::empty())?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }
        Ok(())
    }

    #[tokio::test]
    async fn project_chart_history_overlays_runs_on_a_shared_sparse_axis()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let axis_extent_cache = Arc::new(Mutex::new(ChartAxisExtentCache::default()));
        let router = app_with_axis_extent_cache(
            catalog,
            MetricRuntime::new(MetricStore::new(directory.path().join("metrics"))),
            BlobStore::new(directory.path().join("blobs")),
            Arc::clone(&axis_extent_cache),
        );
        let create_run = |name: &str| CreateRunRequest {
            id: None,
            name: Some(name.to_owned()),
            config: BTreeMap::new(),
            resume: ResumePolicy::Never,
            sweep_trial_id: None,
        };
        let first: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/runs",
                    &create_run("first"),
                )?)
                .await?,
        )
        .await?;
        let second: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/runs",
                    &create_run("second"),
                )?)
                .await?,
        )
        .await?;
        let foreign: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/other/runs",
                    &create_run("foreign"),
                )?)
                .await?,
        )
        .await?;

        for (run_id, steps, timestamps, losses) in [
            (
                first.run.id,
                [10, 20, 30],
                [1_000, 2_000, 3_000],
                [1.0, 2.0, 3.0],
            ),
            (
                second.run.id,
                [100, 110, 120],
                [5_000, 6_000, 7_000],
                [4.0, 5.0, 6.0],
            ),
        ] {
            let points = (0..3)
                .map(|index| {
                    let mut metrics = BTreeMap::from([("loss".to_owned(), losses[index])]);
                    if run_id == first.run.id && index != 1 {
                        metrics.insert("sparse".to_owned(), (index + 10) as f64);
                    }
                    MetricPoint {
                        sequence: index as u64 + 1,
                        step: steps[index],
                        timestamp_ms: timestamps[index],
                        metrics,
                    }
                })
                .collect();
            let response = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/api/v1/runs/{run_id}/batches"),
                    &IngestBatchRequest {
                        batch_sequence: 1,
                        points,
                    },
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let series = vec![
            ChartSeriesRequest {
                run_id: first.run.id,
                key: "loss".to_owned(),
            },
            ChartSeriesRequest {
                run_id: second.run.id,
                key: "loss".to_owned(),
            },
            ChartSeriesRequest {
                run_id: first.run.id,
                key: "sparse".to_owned(),
            },
        ];
        let query = ChartHistoryQueryRequest {
            series: series.clone(),
            alignment: ChartAlignment::Step,
            max_buckets: 2,
            viewport: None,
        };
        let absolute: ChartHistoryQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/chart-history/query",
                    &query,
                )?)
                .await?,
        )
        .await?;
        assert_eq!(
            axis_extent_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .scan_count(),
            2
        );
        assert_eq!((absolute.x_min, absolute.x_max), (Some(10), Some(120)));
        assert_eq!(absolute.bucket_count, 2);
        assert_eq!(absolute.runs.len(), 2);
        assert!(
            absolute
                .runs
                .iter()
                .all(|run| run.source_last_sequence == Some(3))
        );
        assert_eq!(absolute.series[0].bucket, vec![0]);
        assert_eq!(absolute.series[0].minimum, vec![1.0]);
        assert_eq!(absolute.series[0].maximum, vec![3.0]);
        assert_eq!(absolute.series[0].last, vec![3.0]);
        assert_eq!(absolute.series[0].last_x, vec![30]);
        assert_eq!(absolute.series[1].bucket, vec![1]);
        assert_eq!(absolute.series[1].last_x, vec![120]);
        assert_eq!(absolute.series[2].bucket, vec![0]);
        assert_eq!(absolute.series[2].minimum, vec![10.0]);
        assert_eq!(absolute.series[2].maximum, vec![12.0]);

        let replay: ChartHistoryQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/chart-history/query",
                    &query,
                )?)
                .await?,
        )
        .await?;
        assert_eq!(replay, absolute);
        assert_eq!(
            axis_extent_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .scan_count(),
            2,
            "a natural-range replay must reuse cached per-key axis extents"
        );

        for alignment in [ChartAlignment::RelativeStep, ChartAlignment::ElapsedTime] {
            let aligned: ChartHistoryQueryResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/compare/chart-history/query",
                        &ChartHistoryQueryRequest {
                            series: series[..2].to_vec(),
                            alignment,
                            max_buckets: 2,
                            viewport: None,
                        },
                    )?)
                    .await?,
            )
            .await?;
            if alignment == ChartAlignment::RelativeStep {
                assert_eq!((aligned.x_min, aligned.x_max), (Some(0), Some(20)));
                assert_eq!(aligned.series[0].last_x, vec![10, 20]);
                assert_eq!(aligned.series[1].last_x, vec![10, 20]);
            } else {
                assert_eq!((aligned.x_min, aligned.x_max), (Some(0), Some(2_000)));
                assert_eq!(aligned.series[0].last_x, vec![1_000, 2_000]);
                assert_eq!(aligned.series[1].last_x, vec![1_000, 2_000]);
            }
            assert_eq!(aligned.series[0].bucket, vec![0, 1]);
            assert_eq!(aligned.series[1].bucket, vec![0, 1]);
        }

        let viewport: ChartHistoryQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/chart-history/query",
                    &ChartHistoryQueryRequest {
                        series: series[..2].to_vec(),
                        alignment: ChartAlignment::RelativeStep,
                        max_buckets: 3,
                        viewport: Some(ChartViewport {
                            minimum: 5,
                            maximum: 15,
                        }),
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(viewport.bucket_count, 3);
        assert_eq!(viewport.series[0].bucket, vec![1]);
        assert_eq!(viewport.series[0].last_x, vec![10]);
        assert_eq!(viewport.series[1].last_x, vec![10]);

        let update = router
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/api/v1/runs/{}/batches", first.run.id),
                &IngestBatchRequest {
                    batch_sequence: 2,
                    points: vec![MetricPoint {
                        sequence: 4,
                        step: 40,
                        timestamp_ms: 4_000,
                        metrics: BTreeMap::from([("loss".to_owned(), -100.0)]),
                    }],
                },
            )?)
            .await?;
        assert_eq!(update.status(), StatusCode::CREATED);
        let refreshed: ChartHistoryQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/chart-history/query",
                    &query,
                )?)
                .await?,
        )
        .await?;
        assert_eq!(refreshed.series[0].minimum, vec![-100.0]);
        assert_eq!(refreshed.series[0].last, vec![-100.0]);
        assert_eq!(refreshed.series[1], absolute.series[1]);
        assert_eq!(refreshed.runs[0].source_last_sequence, Some(4));
        assert_eq!(refreshed.runs[1].source_last_sequence, Some(3));
        assert_eq!(
            axis_extent_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .scan_count(),
            3,
            "only the run with a new sequence watermark should rescan extents"
        );

        let foreign_response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/projects/compare/chart-history/query",
                &ChartHistoryQueryRequest {
                    series: vec![ChartSeriesRequest {
                        run_id: foreign.run.id,
                        key: "loss".to_owned(),
                    }],
                    alignment: ChartAlignment::Step,
                    max_buckets: 10,
                    viewport: None,
                },
            )?)
            .await?;
        assert_eq!(foreign_response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        for invalid in [
            ChartHistoryQueryRequest {
                series: vec![series[0].clone(), series[0].clone()],
                alignment: ChartAlignment::Step,
                max_buckets: 10,
                viewport: None,
            },
            ChartHistoryQueryRequest {
                series: series[..2].to_vec(),
                alignment: ChartAlignment::Step,
                max_buckets: 2_000,
                viewport: Some(ChartViewport {
                    minimum: 2,
                    maximum: 1,
                }),
            },
            ChartHistoryQueryRequest {
                series: (0..32)
                    .map(|index| ChartSeriesRequest {
                        run_id: first.run.id,
                        key: format!("metric-{index}"),
                    })
                    .collect(),
                alignment: ChartAlignment::Step,
                max_buckets: 626,
                viewport: None,
            },
        ] {
            let response = router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/compare/chart-history/query",
                    &invalid,
                )?)
                .await?;
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }
        Ok(())
    }

    #[tokio::test]
    async fn dropped_ingest_waiter_does_not_orphan_a_written_segment()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics = MetricRuntime::new(MetricStore::new(directory.path().join("metrics")));
        let blobs = BlobStore::new(directory.path().join("blobs"));
        blobs.ensure()?;
        let state = AppState::new(
            catalog.clone(),
            metrics.clone(),
            blobs,
            Arc::new(Mutex::new(ChartAxisExtentCache::default())),
            Arc::new(RequestTelemetry::from_environment()),
        );
        let (run, _) = catalog
            .create_or_resume_run(
                "cancelled-ingest",
                &CreateRunRequest {
                    id: None,
                    name: Some("detached".to_owned()),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                    sweep_trial_id: None,
                },
            )
            .await?;
        let request = IngestBatchRequest {
            batch_sequence: 1,
            points: vec![MetricPoint {
                sequence: 1,
                step: 0,
                timestamp_ms: 1,
                metrics: BTreeMap::from([("loss".to_owned(), 1.0)]),
            }],
        };
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&request)?));
        let run_mutation = Arc::clone(&state.mutation_locks[mutation_lock_index(&run.id)])
            .lock_owned()
            .await;
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let state_for_ingest = state.clone();
        let waiter = tokio::spawn(async move {
            let owned_ingest = tokio::spawn(process_ingest_batch(
                state_for_ingest,
                run.id,
                request,
                run_mutation,
            ));
            let _ = started_sender.send(());
            owned_ingest.await
        });
        started_receiver.await?;
        waiter.abort();
        let _ = waiter.await;

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(
                    catalog.batch_status(run.id, 1, &digest).await?,
                    BatchStatus::Duplicate { .. }
                ) {
                    return Ok::<_, runloom_catalog::CatalogError>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let segments = catalog.list_segments(run.id, None).await?;
        assert_eq!(segments.len(), 1);
        assert!(
            metrics
                .store()
                .root()
                .join(&segments[0].relative_path)
                .is_file()
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_ingests_waiting_for_a_run_lock_do_not_detach()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics = MetricRuntime::new(MetricStore::new(directory.path().join("metrics")));
        let blobs = BlobStore::new(directory.path().join("blobs"));
        blobs.ensure()?;
        let state = AppState::new(
            catalog.clone(),
            metrics,
            blobs,
            Arc::new(Mutex::new(ChartAxisExtentCache::default())),
            Arc::new(RequestTelemetry::from_environment()),
        );
        let (run, _) = catalog
            .create_or_resume_run(
                "cancelled-queue",
                &CreateRunRequest {
                    id: None,
                    name: Some("queued".to_owned()),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                    sweep_trial_id: None,
                },
            )
            .await?;
        let request = IngestBatchRequest {
            batch_sequence: 1,
            points: vec![MetricPoint {
                sequence: 1,
                step: 0,
                timestamp_ms: 1,
                metrics: BTreeMap::from([("loss".to_owned(), 1.0)]),
            }],
        };
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&request)?));
        let held_lock = Arc::clone(&state.mutation_locks[mutation_lock_index(&run.id)])
            .lock_owned()
            .await;
        let mut waiters = Vec::new();
        for _ in 0..64 {
            let state = state.clone();
            let request = request.clone();
            waiters.push(tokio::spawn(async move {
                ingest_batch(State(state), Path(run.id), Json(request)).await
            }));
        }
        tokio::task::yield_now().await;
        for waiter in &waiters {
            waiter.abort();
        }
        for waiter in waiters {
            let _ = waiter.await;
        }
        drop(held_lock);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            catalog.batch_status(run.id, 1, &digest).await?,
            BatchStatus::Missing
        );
        assert!(catalog.list_segments(run.id, None).await?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_duplicate_ingest_preserves_registered_segment()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics_root = directory.path().join("metrics");
        let router = app(catalog.clone(), MetricStore::new(&metrics_root));
        let created: CreateRunResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/projects/concurrent-ingest/runs",
                    &CreateRunRequest {
                        id: None,
                        name: Some("duplicate-race".to_owned()),
                        config: BTreeMap::new(),
                        resume: ResumePolicy::Never,
                        sweep_trial_id: None,
                    },
                )?)
                .await?,
        )
        .await?;
        let batch = IngestBatchRequest {
            batch_sequence: 1,
            points: (1..=runloom_protocol::MAX_BATCH_POINTS as u64)
                .map(|sequence| MetricPoint {
                    sequence,
                    step: sequence - 1,
                    timestamp_ms: sequence as i64,
                    metrics: BTreeMap::from([("loss".to_owned(), sequence as f64)]),
                })
                .collect(),
        };
        let path = format!("/api/v1/runs/{}/batches", created.run.id);
        let first = router.clone().oneshot(json_request("POST", &path, &batch)?);
        let second = router.clone().oneshot(json_request("POST", &path, &batch)?);
        let (first, second) = tokio::join!(first, second);
        let first = first?;
        let second = second?;
        let mut statuses = [first.status(), second.status()];
        statuses.sort();
        assert_eq!(statuses, [StatusCode::OK, StatusCode::CREATED]);
        let first: IngestBatchResponse = response_json(first).await?;
        let second: IngestBatchResponse = response_json(second).await?;
        assert_ne!(first.duplicate, second.duplicate);

        let segments = catalog.list_segments(created.run.id, None).await?;
        assert_eq!(segments.len(), 1);
        assert!(metrics_root.join(&segments[0].relative_path).is_file());
        let history: HistoryResponse = response_json(
            router
                .oneshot(
                    Request::get(format!(
                        "/api/v1/runs/{}/history?key=loss&limit=1024",
                        created.run.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(history.sequence.len(), runloom_protocol::MAX_BATCH_POINTS);
        assert_eq!(history.sequence.last(), Some(&1024));
        Ok(())
    }

    #[tokio::test]
    async fn lifecycle_is_idempotent_and_history_is_columnar()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
        let create = CreateRunRequest {
            id: None,
            name: Some("fast-run".to_owned()),
            config: BTreeMap::from([("seed".to_owned(), 42.into())]),
            resume: ResumePolicy::Never,
            sweep_trial_id: None,
        };
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/projects/robotics/runs",
                &create,
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created: CreateRunResponse = response_json(response).await?;
        assert!(!created.resumed);
        assert_eq!(created.next_sequence, 1);
        assert_eq!(created.next_step, 0);

        let config_path = format!("/api/v1/runs/{}/config", created.run.id);
        let response = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &config_path,
                &ConfigUpdateRequest {
                    updates: BTreeMap::from([("optimizer".to_owned(), "adam".into())]),
                    allow_val_change: false,
                },
            )?)
            .await?;
        let updated: RunUpdateResponse = response_json(response).await?;
        assert_eq!(updated.run.config["optimizer"], "adam");

        let response = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &config_path,
                &ConfigUpdateRequest {
                    updates: BTreeMap::from([("seed".to_owned(), 7.into())]),
                    allow_val_change: false,
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let response = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &config_path,
                &ConfigUpdateRequest {
                    updates: BTreeMap::from([("seed".to_owned(), 7.into())]),
                    allow_val_change: true,
                },
            )?)
            .await?;
        let updated: RunUpdateResponse = response_json(response).await?;
        assert_eq!(updated.run.config["seed"], 7);

        let summary_path = format!("/api/v1/runs/{}/summary", created.run.id);
        let response = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &summary_path,
                &SummaryUpdateRequest {
                    updates: BTreeMap::from([
                        ("status".to_owned(), "running".into()),
                        ("tags".to_owned(), serde_json::json!(["fast", null])),
                    ]),
                },
            )?)
            .await?;
        let updated: RunUpdateResponse = response_json(response).await?;
        assert_eq!(updated.run.summary["status"], "running");
        assert_eq!(
            updated.run.summary["tags"],
            serde_json::json!(["fast", null])
        );
        let response = router
            .clone()
            .oneshot(json_request(
                "PATCH",
                &summary_path,
                &SummaryUpdateRequest {
                    updates: BTreeMap::from([(
                        "oversized".to_owned(),
                        "x".repeat(256 * 1024).into(),
                    )]),
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let batch = IngestBatchRequest {
            batch_sequence: 1,
            points: vec![
                MetricPoint {
                    sequence: 1,
                    step: 10,
                    timestamp_ms: 1_000,
                    metrics: BTreeMap::from([
                        ("comma,key".to_owned(), 4.0),
                        ("loss".to_owned(), 2.0),
                        ("reward".to_owned(), 3.0),
                    ]),
                },
                MetricPoint {
                    sequence: 2,
                    step: 11,
                    timestamp_ms: 2_000,
                    metrics: BTreeMap::from([("loss".to_owned(), 1.0)]),
                },
            ],
        };
        let batch_path = format!("/api/v1/runs/{}/batches", created.run.id);
        let response = router
            .clone()
            .oneshot(json_request("POST", &batch_path, &batch)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let accepted: IngestBatchResponse = response_json(response).await?;
        assert!(!accepted.duplicate);

        let response = router
            .clone()
            .oneshot(json_request("POST", &batch_path, &batch)?)
            .await?;
        let duplicate: IngestBatchResponse = response_json(response).await?;
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.metric_revision, accepted.metric_revision);

        let alert_path = format!("/api/v1/runs/{}/alerts", created.run.id);
        let alert_request = CreateAlertRequest {
            id: Some(AlertId::new()),
            title: "reward stalled".to_owned(),
            text: "No improvement in the last window".to_owned(),
            level: AlertLevel::Warn,
            step: Some(11),
            timestamp_ms: 2_000,
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &alert_path, &alert_request)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created_alert: CreateAlertResponse = response_json(response).await?;
        assert!(!created_alert.duplicate);
        let response = router
            .clone()
            .oneshot(json_request("POST", &alert_path, &alert_request)?)
            .await?;
        let replayed_alert: CreateAlertResponse = response_json(response).await?;
        assert!(replayed_alert.duplicate);
        assert_eq!(replayed_alert.alert, created_alert.alert);
        let response = router
            .clone()
            .oneshot(Request::get(format!("{alert_path}?limit=10")).body(Body::empty())?)
            .await?;
        let alerts: AlertListResponse = response_json(response).await?;
        assert_eq!(alerts.alerts, vec![created_alert.alert]);

        let video = b"native-video-content";
        let blob_digest = format!("{:x}", Sha256::digest(video));
        let blob_path = format!("/api/v1/blobs/{blob_digest}");
        let response = router
            .clone()
            .oneshot(
                Request::put(&blob_path)
                    .header("content-type", "video/mp4")
                    .header("content-length", video.len())
                    .body(Body::from(video.as_slice()))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let uploaded: BlobUploadResponse = response_json(response).await?;
        assert_eq!(uploaded.blob.size, video.len() as u64);
        assert!(!uploaded.duplicate);

        let rich_path = format!("/api/v1/runs/{}/rich-values", created.run.id);
        let rich_request = CreateRichValueRequest {
            id: Some(RichValueId::new()),
            key: "rollout/video".to_owned(),
            kind: RichValueKind::Video,
            step: 12,
            timestamp_ms: 2_100,
            blob: Some(BlobRef {
                digest: blob_digest.clone(),
                size: video.len() as u64,
                mime_type: "video/mp4".to_owned(),
                file_name: Some("rollout.mp4".to_owned()),
            }),
            metadata: BTreeMap::from([("caption".to_owned(), "native playback".into())]),
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &rich_path, &rich_request)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created_value: CreateRichValueResponse = response_json(response).await?;
        assert!(!created_value.duplicate);
        let response = router
            .clone()
            .oneshot(json_request("POST", &rich_path, &rich_request)?)
            .await?;
        let replayed_value: CreateRichValueResponse = response_json(response).await?;
        assert!(replayed_value.duplicate);
        let values: RichValueListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!("{rich_path}?key=rollout%2Fvideo&limit=10"))
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(values.values.len(), 1);
        assert_eq!(values.values[0].id, created_value.value.id);
        assert_eq!(values.values[0].blob, created_value.value.blob);
        assert_eq!(values.next_before, None);
        let rich_keys: RichValueKeyListResponse = response_json(
            router
                .clone()
                .oneshot(Request::get(format!("{rich_path}/keys?limit=10")).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(rich_keys.keys.len(), 1);
        assert_eq!(rich_keys.keys[0].key, "rollout/video");
        assert_eq!(rich_keys.keys[0].count, 1);
        let loaded_value: runloom_protocol::RichValueRecord = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!("/api/v1/rich-values/{}", created_value.value.id))
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(loaded_value, created_value.value);

        let response = router
            .clone()
            .oneshot(
                Request::get(format!("{blob_path}?mime=video%2Fmp4"))
                    .header("range", "bytes=7-11")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["content-type"], "video/mp4");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(
            response.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
        let expected_etag = format!("\"sha256:{blob_digest}\"");
        assert_eq!(response.headers()["etag"], expected_etag.as_str());
        assert_eq!(to_bytes(response.into_body(), 64).await?, &video[7..=11]);
        let not_modified = router
            .clone()
            .oneshot(
                Request::get(format!("{blob_path}?mime=video%2Fmp4"))
                    .header("if-none-match", &expected_etag)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(not_modified.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(not_modified.headers()["etag"], expected_etag.as_str());
        assert_eq!(
            not_modified.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
        assert!(to_bytes(not_modified.into_body(), 1).await?.is_empty());
        let unsafe_mime = router
            .clone()
            .oneshot(Request::get(format!("{blob_path}?mime=text%2Fhtml")).body(Body::empty())?)
            .await?;
        assert_eq!(unsafe_mime.status(), StatusCode::OK);
        assert_eq!(
            unsafe_mime.headers()["content-type"],
            "application/octet-stream"
        );
        assert_eq!(unsafe_mime.headers()["content-disposition"], "attachment");
        assert_eq!(unsafe_mime.headers()["x-content-type-options"], "nosniff");

        let artifact_path = format!("/api/v1/runs/{}/artifacts", created.run.id);
        let artifact_request = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "policy".to_owned(),
            artifact_type: "model".to_owned(),
            version: None,
            description: Some("trained policy".to_owned()),
            metadata: BTreeMap::from([("framework".to_owned(), "jax".into())]),
            aliases: vec!["latest".to_owned()],
            entries: vec![
                ArtifactEntry {
                    path: "checkpoint.bin".to_owned(),
                    blob: uploaded.blob.clone(),
                },
                ArtifactEntry {
                    path: "metadata/정책.json".to_owned(),
                    blob: uploaded.blob.clone(),
                },
            ],
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &artifact_path, &artifact_request)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let version_zero: CreateArtifactResponse = response_json(response).await?;
        assert_eq!(version_zero.artifact.version, 0);
        let replay: CreateArtifactResponse = response_json(
            router
                .clone()
                .oneshot(json_request("POST", &artifact_path, &artifact_request)?)
                .await?,
        )
        .await?;
        assert!(replay.duplicate);

        let mut next_request = artifact_request.clone();
        next_request.id = Some(runloom_protocol::ArtifactId::new());
        next_request.aliases = vec!["latest".to_owned(), "best".to_owned()];
        let version_one: CreateArtifactResponse = response_json(
            router
                .clone()
                .oneshot(json_request("POST", &artifact_path, &next_request)?)
                .await?,
        )
        .await?;
        assert_eq!(version_one.artifact.version, 1);
        let resolved: runloom_protocol::ArtifactRecord = response_json(
            router
                .clone()
                .oneshot(
                    Request::get("/api/v1/projects/robotics/artifacts/policy/aliases/latest")
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(resolved.id, version_one.artifact.id);

        let used: runloom_protocol::ArtifactRecord = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    &format!("{artifact_path}/use"),
                    &UseArtifactRequest {
                        artifact_id: version_zero.artifact.id,
                    },
                )?)
                .await?,
        )
        .await?;
        assert_eq!(used.id, version_zero.artifact.id);
        let run_artifacts: RunArtifactListResponse = response_json(
            router
                .clone()
                .oneshot(Request::get(&artifact_path).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(run_artifacts.artifacts.len(), 3);
        let project_artifacts: ArtifactListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get("/api/v1/projects/robotics/artifacts?limit=10")
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(project_artifacts.artifacts.len(), 2);
        let input_lineage: runloom_protocol::ArtifactLineageResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/artifacts/{}/lineage?relation=input&limit=10",
                        version_zero.artifact.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(input_lineage.relation, ArtifactRelation::Input);
        assert_eq!(input_lineage.runs[0].id, created.run.id);
        let output_lineage: runloom_protocol::ArtifactLineageResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/artifacts/{}/lineage?relation=output&limit=10",
                        version_zero.artifact.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(output_lineage.relation, ArtifactRelation::Output);
        assert_eq!(output_lineage.runs[0].id, created.run.id);
        let response = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/artifacts/{}/files/checkpoint.bin",
                    version_zero.artifact.id
                ))
                .header("range", "bytes=0-5")
                .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(to_bytes(response.into_body(), 64).await?, &video[0..=5]);

        let response = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/artifacts/{}/download",
                    version_zero.artifact.id
                ))
                .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "application/zip");
        assert_eq!(
            response.headers()["content-disposition"],
            "attachment; filename=\"policy-v0.zip\"; filename*=UTF-8''policy-v0.zip"
        );
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert!(response.headers().get("content-length").is_none());
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let mut archive = zip::ZipArchive::new(Cursor::new(body))?;
        assert_eq!(archive.len(), 2);
        for path in ["checkpoint.bin", "metadata/정책.json"] {
            let mut file = archive.by_name(path)?;
            assert_eq!(file.compression(), zip::CompressionMethod::Stored);
            assert_eq!(file.unix_mode(), Some(0o100644));
            let mut contents = Vec::new();
            file.read_to_end(&mut contents)?;
            assert_eq!(contents, video);
        }

        let trace_payload = br#"{"inputs":{"prompt":"reward"},"outputs":{"text":"reward is 3"},"messages":[{"role":"assistant","content":"reward is 3"}]}"#;
        let trace_digest = format!("{:x}", Sha256::digest(trace_payload));
        let response = router
            .clone()
            .oneshot(
                Request::put(format!("/api/v1/blobs/{trace_digest}"))
                    .header("content-type", "application/vnd.runloom.trace+json")
                    .header("content-length", trace_payload.len())
                    .body(Body::from(trace_payload.as_slice()))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let trace_id = TraceSpanId::new();
        let trace_path = format!("/api/v1/runs/{}/traces", created.run.id);
        let trace_request = CreateTraceSpanRequest {
            id: Some(trace_id),
            trace_id: "trace-answer".to_owned(),
            parent_span_id: None,
            name: "generate-answer".to_owned(),
            kind: TraceKind::Llm,
            status: TraceStatus::Ok,
            start_time_ms: 2_200,
            end_time_ms: 2_250,
            step: Some(12),
            attributes: BTreeMap::from([("model".to_owned(), "local-model".into())]),
            preview: BTreeMap::from([(
                "messages".to_owned(),
                serde_json::json!([{"role": "assistant", "content": "reward is 3"}]),
            )]),
            payload: Some(BlobRef {
                digest: trace_digest,
                size: trace_payload.len() as u64,
                mime_type: "application/vnd.runloom.trace+json".to_owned(),
                file_name: None,
            }),
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &trace_path, &trace_request)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CREATED);
        let created_trace: CreateTraceSpanResponse = response_json(response).await?;
        assert!(!created_trace.duplicate);
        let replayed_trace: CreateTraceSpanResponse = response_json(
            router
                .clone()
                .oneshot(json_request("POST", &trace_path, &trace_request)?)
                .await?,
        )
        .await?;
        assert!(replayed_trace.duplicate);
        assert_eq!(replayed_trace.span, created_trace.span);
        let traces: TraceSpanListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!("{trace_path}?q=assistant%20reward&limit=10"))
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(traces.spans.len(), 1);
        assert_eq!(traces.spans[0].id, created_trace.span.id);
        let loaded_trace: TraceSpanRecord = response_json(
            router
                .clone()
                .oneshot(Request::get(format!("/api/v1/traces/{trace_id}")).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(loaded_trace, created_trace.span);

        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/projects/robotics/runs",
                &CreateRunRequest {
                    id: Some(created.run.id),
                    name: None,
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Must,
                    sweep_trial_id: None,
                },
            )?)
            .await?;
        let resumed: CreateRunResponse = response_json(response).await?;
        assert!(resumed.resumed);
        assert_eq!(resumed.next_sequence, 3);
        assert_eq!(resumed.next_step, 13);

        let history_path = format!(
            "/api/v1/runs/{}/history?key=loss&key=comma%2Ckey&limit=1",
            created.run.id
        );
        let response = router
            .clone()
            .oneshot(Request::get(history_path).body(Body::empty())?)
            .await?;
        let history: HistoryResponse = response_json(response).await?;
        assert_eq!(history.sequence, vec![1]);
        assert_eq!(history.metrics["loss"], vec![Some(2.0)]);
        assert_eq!(history.metrics["comma,key"], vec![Some(4.0)]);
        assert_eq!(history.next_after, Some(1));
        assert!(!history.sampled);
        assert_eq!(history.source_points, None);

        let sampled_path = format!(
            "/api/v1/runs/{}/history?key=loss&max_points=2",
            created.run.id
        );
        let response = router
            .clone()
            .oneshot(Request::get(sampled_path).body(Body::empty())?)
            .await?;
        let sampled: HistoryResponse = response_json(response).await?;
        assert_eq!(sampled.sequence, vec![1, 2]);
        assert_eq!(sampled.metrics["loss"], vec![Some(2.0), Some(1.0)]);
        assert_eq!(sampled.source_points, Some(2));
        assert!(sampled.sampled);

        let invalid_path = format!(
            "/api/v1/runs/{}/history?key=loss&limit=2&max_points=2",
            created.run.id
        );
        let response = router
            .clone()
            .oneshot(Request::get(invalid_path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let response = router
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/runs/{}/history?key=loss&unknown=1",
                    created.run.id
                ))
                .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        for query in [
            "key=loss&unknown=1",
            "key=loss&max_buckets=10&max_buckets=20",
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/runs/{}/chart-history?{query}",
                        created.run.id
                    ))
                    .body(Body::empty())?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }

        let response = router
            .clone()
            .oneshot(Request::get("/api/v1/projects").body(Body::empty())?)
            .await?;
        let projects: ProjectListResponse = response_json(response).await?;
        assert_eq!(projects.projects[0].run_count, 1);
        let response = router
            .clone()
            .oneshot(Request::get("/api/v1/projects/robotics/runs").body(Body::empty())?)
            .await?;
        let runs: RunListResponse = response_json(response).await?;
        assert_eq!(runs.runs[0].id, created.run.id);
        assert_eq!(runs.runs[0].metric_revision, 1);
        let encoded_runs = serde_json::to_value(&runs)?;
        assert!(encoded_runs["runs"][0].get("summary").is_none());
        assert!(encoded_runs["runs"][0].get("config").is_none());
        let response = router
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/runs/{}/metrics", created.run.id))
                    .body(Body::empty())?,
            )
            .await?;
        let keys: MetricKeyListResponse = response_json(response).await?;
        assert_eq!(keys.keys, vec!["comma,key", "loss", "reward"]);
        assert_eq!(keys.next_after, None);
        let first_keys: MetricKeyListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!("/api/v1/runs/{}/metrics?limit=2", created.run.id))
                        .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(first_keys.keys, vec!["comma,key", "loss"]);
        assert_eq!(first_keys.next_after.as_deref(), Some("loss"));
        let final_keys: MetricKeyListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/runs/{}/metrics?limit=2&after=loss",
                        created.run.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(final_keys.keys, vec!["reward"]);
        assert_eq!(final_keys.next_after, None);

        let finish_path = format!("/api/v1/runs/{}/finish", created.run.id);
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &finish_path,
                &FinishRunRequest {
                    summary: BTreeMap::from([("status".to_owned(), "complete".into())]),
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let finished: FinishRunResponse = response_json(response).await?;
        assert_eq!(finished.run.summary["status"], "complete");
        assert_eq!(
            finished.run.summary["tags"],
            serde_json::json!(["fast", null])
        );
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &finish_path,
                &FinishRunRequest {
                    summary: BTreeMap::from([("status".to_owned(), "complete".into())]),
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                &finish_path,
                &FinishRunRequest {
                    summary: BTreeMap::from([("status".to_owned(), "changed".into())]),
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let rejected_batch = IngestBatchRequest {
            batch_sequence: 2,
            points: vec![MetricPoint {
                sequence: 3,
                step: 2,
                timestamp_ms: 3_000,
                metrics: BTreeMap::from([("loss".to_owned(), 0.5)]),
            }],
        };
        let response = router
            .clone()
            .oneshot(json_request("POST", &batch_path, &rejected_batch)?)
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let response = router
            .oneshot(json_request(
                "PATCH",
                &summary_path,
                &SummaryUpdateRequest {
                    updates: BTreeMap::from([("status".to_owned(), "late".into())]),
                },
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        Ok(())
    }

    #[tokio::test]
    async fn background_compaction_preserves_http_history() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics_root = directory.path().join("metrics");
        let metrics = MetricRuntime::new(MetricStore::new(&metrics_root));
        let router = app_with_runtime(catalog.clone(), metrics.clone());
        let response = router
            .clone()
            .oneshot(json_request(
                "POST",
                "/api/v1/projects/robotics/runs",
                &CreateRunRequest {
                    id: None,
                    name: Some("compact-me".to_owned()),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                    sweep_trial_id: None,
                },
            )?)
            .await?;
        let created: CreateRunResponse = response_json(response).await?;
        let batch_path = format!("/api/v1/runs/{}/batches", created.run.id);
        for batch_index in 0..4u64 {
            let first_sequence = batch_index * 2 + 1;
            let batch = IngestBatchRequest {
                batch_sequence: first_sequence,
                points: (0..2)
                    .map(|offset| {
                        let sequence = first_sequence + offset;
                        MetricPoint {
                            sequence,
                            step: sequence - 1,
                            timestamp_ms: sequence as i64 * 10,
                            metrics: BTreeMap::from([("loss".to_owned(), sequence as f64)]),
                        }
                    })
                    .collect(),
            };
            let response = router
                .clone()
                .oneshot(json_request("POST", &batch_path, &batch)?)
                .await?;
            assert_eq!(response.status(), StatusCode::CREATED);
        }
        let history_path = format!("/api/v1/runs/{}/history?key=loss&limit=10", created.run.id);
        let before: HistoryResponse = response_json(
            router
                .clone()
                .oneshot(Request::get(&history_path).body(Body::empty())?)
                .await?,
        )
        .await?;
        let sources = catalog.list_segments(created.run.id, None).await?;
        let snapshot = metrics.read_snapshot().await;
        let task_catalog = catalog.clone();
        let task_metrics = metrics.clone();
        let mut compaction = tokio::spawn(async move {
            compact_once(
                &task_catalog,
                &task_metrics,
                CompactionConfig {
                    interval: Duration::from_secs(1),
                    target_rows: 16,
                    max_input_segments: 16,
                    retirement_batch: 16,
                    max_consecutive_passes: 8,
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut compaction)
                .await
                .is_err(),
            "manifest replacement must wait for active history snapshots"
        );
        assert_eq!(catalog.list_segments(created.run.id, None).await?.len(), 4);
        drop(snapshot);
        let outcome = compaction.await??;
        let after: HistoryResponse = response_json(
            router
                .oneshot(Request::get(&history_path).body(Body::empty())?)
                .await?,
        )
        .await?;

        assert_eq!(
            outcome,
            CompactionOutcome::SegmentsCompacted { inputs: 4, rows: 8 }
        );
        assert_eq!(after, before);
        assert_eq!(catalog.list_segments(created.run.id, None).await?.len(), 1);
        assert!(catalog.retired_segments(16).await?.is_empty());
        assert!(
            sources
                .iter()
                .all(|source| !metrics_root.join(&source.relative_path).exists())
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_compaction_removes_only_its_unregistered_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let metrics_root = directory.path().join("metrics");
        let metrics = MetricRuntime::new(MetricStore::new(&metrics_root));
        let (run, resumed) = catalog
            .create_or_resume_run(
                "compaction-cancel",
                &CreateRunRequest {
                    id: None,
                    name: Some("cancelled-output".to_owned()),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                    sweep_trial_id: None,
                },
            )
            .await?;
        assert!(!resumed);
        for batch_index in 0..4u64 {
            let first_sequence = batch_index * 2 + 1;
            let batch = IngestBatchRequest {
                batch_sequence: batch_index + 1,
                points: (0..2)
                    .map(|offset| {
                        let sequence = first_sequence + offset;
                        MetricPoint {
                            sequence,
                            step: sequence - 1,
                            timestamp_ms: sequence as i64,
                            metrics: BTreeMap::from([("loss".to_owned(), sequence as f64)]),
                        }
                    })
                    .collect(),
            };
            let digest = format!("{:064x}", batch_index + 1);
            let written = metrics
                .store()
                .write_batch(run.project_id, run.id, &digest, &batch)?;
            catalog
                .register_batch(
                    run.id,
                    batch.batch_sequence,
                    &digest,
                    &SegmentManifest {
                        id: written.id,
                        signature: written.signature,
                        relative_path: written.relative_path,
                        first_sequence: written.first_sequence,
                        last_sequence: written.last_sequence,
                        row_count: written.row_count,
                        byte_size: written.byte_size,
                    },
                    &BTreeMap::new(),
                )
                .await?;
        }
        let sources = catalog.list_segments(run.id, None).await?;
        let segment_directory = metrics_root
            .join(&sources[0].relative_path)
            .parent()
            .ok_or_else(|| std::io::Error::other("segment path has no parent"))?
            .to_path_buf();
        let snapshot = metrics.read_snapshot().await;
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_catalog = catalog.clone();
        let task_metrics = metrics.clone();
        let task_cancelled = Arc::clone(&cancelled);
        let task = tokio::spawn(async move {
            compact_once(
                &task_catalog,
                &task_metrics,
                CompactionConfig {
                    interval: Duration::from_secs(1),
                    target_rows: 16,
                    max_input_segments: 16,
                    retirement_batch: 16,
                    max_consecutive_passes: 8,
                },
                task_cancelled,
            )
            .await
        });
        let mut output_observed = false;
        for _ in 0..100 {
            output_observed = std::fs::read_dir(&segment_directory)?.any(|entry| {
                entry.is_ok_and(|entry| entry.file_name().to_string_lossy().starts_with("compact-"))
            });
            if output_observed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(output_observed, "compaction output was not installed");
        cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(snapshot);
        let result = task.await?;
        assert!(matches!(
            result,
            Err(CompactionError::Storage(StorageError::Cancelled))
        ));
        assert_eq!(catalog.list_segments(run.id, None).await?.len(), 4);
        assert!(
            sources
                .iter()
                .all(|source| { metrics_root.join(&source.relative_path).is_file() })
        );
        assert!(!std::fs::read_dir(segment_directory)?.any(|entry| {
            entry.is_ok_and(|entry| entry.file_name().to_string_lossy().starts_with("compact-"))
        }));
        Ok(())
    }

    #[test]
    fn artifact_download_filename_is_safe_and_utf8_compatible()
    -> Result<(), Box<dyn std::error::Error>> {
        let file_name = super::artifact_zip_file_name(r#" policy:"정책". "#, 7);
        assert_eq!(file_name, "policy__정책_-v7.zip");
        let disposition = super::artifact_download_content_disposition(&file_name)
            .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
        assert_eq!(
            disposition.to_str()?,
            "attachment; filename=\"policy_____-v7.zip\"; filename*=UTF-8''policy__%EC%A0%95%EC%B1%85_-v7.zip"
        );
        assert_eq!(super::artifact_zip_file_name("...", 0), "artifact-v0.zip");
        Ok(())
    }

    fn json_request<T: serde::Serialize>(
        method: &str,
        uri: &str,
        body: &T,
    ) -> Result<Request<Body>, Box<dyn std::error::Error>> {
        Ok(Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(body)?))?)
    }

    async fn response_json<T: serde::de::DeserializeOwned>(
        response: axum::response::Response,
    ) -> Result<T, Box<dyn std::error::Error>> {
        let body = to_bytes(response.into_body(), 2 * 1024 * 1024).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}
