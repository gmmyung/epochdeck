#![forbid(unsafe_code)]

mod compaction;

pub use compaction::{CompactionConfig, MetricRuntime, run_compaction_worker};
#[cfg(test)]
use compaction::{CompactionOutcome, compact_once};

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
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
    HealthResponse, HistoryResponse, IngestBatchRequest, IngestBatchResponse, MAX_ALERT_TEXT_BYTES,
    MAX_ALERT_TITLE_BYTES, MAX_ARTIFACT_ENTRIES, MAX_ARTIFACT_MANIFEST_BYTES, MAX_BATCH_POINTS,
    MAX_CHART_BUCKET_CELLS, MAX_CHART_BUCKETS, MAX_CHART_QUERY_CELLS, MAX_CHART_QUERY_RUNS,
    MAX_CHART_QUERY_SERIES, MAX_CONFIG_BYTES, MAX_HISTORY_KEYS, MAX_HISTORY_POINTS,
    MAX_METRICS_PER_POINT, MAX_RICH_KEY_BYTES, MAX_RICH_METADATA_BYTES, MAX_SUMMARY_BYTES,
    MAX_TRACE_METADATA_BYTES, MetricKeyListResponse, ProjectListResponse, ReportId, ReportLayout,
    ReportListResponse, ReportPanelKind, ReportRecord, ResumePolicy, RichValueId, RichValueKind,
    RichValueListResponse, RunArtifactListResponse, RunId, RunListResponse, RunQueryRequest,
    RunQueryResponse, RunRecord, RunState, RunUpdateResponse, SlowRequestRecord,
    SummaryUpdateRequest, SweepId, SweepListResponse, SweepTrialId, SweepTrialListResponse,
    SweepTrialRecord, SweepTrialState, TraceSpanId, TraceSpanListResponse, UpdateReportRequest,
    UseArtifactRequest,
};
use runloom_storage::{
    BlobStore, ChartAxisExtent, ChartAxisExtentScanner, ChartCoordinate, ChartHistorySampler,
    ChartSamplingSpec, ChartStepExtent, ChartStepExtentScanner, MetricStore, MinMaxHistorySampler,
    SegmentSource, SegmentTail, StorageError,
};
#[cfg(feature = "embedded-dashboard")]
use rust_embed::RustEmbed;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore, mpsc};
use tower::ServiceExt;
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_PROJECT_NAME_BYTES: usize = 128;
const MAX_RUN_NAME_BYTES: usize = 256;
const MAX_METRIC_KEY_BYTES: usize = 256;
const INGEST_WORKERS: usize = 2;
const QUERY_WORKERS: usize = 4;
const ARTIFACT_DOWNLOAD_WORKERS: usize = 4;
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
    ingest_permits: Arc<Semaphore>,
    query_permits: Arc<Semaphore>,
    artifact_download_permits: Arc<Semaphore>,
    chart_series_cache: Arc<Mutex<ChartSeriesCache>>,
    chart_axis_extent_cache: Arc<Mutex<ChartAxisExtentCache>>,
    telemetry: Arc<RequestTelemetry>,
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
    let router = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/diagnostics", get(diagnostics))
        .route("/api/v1/projects", get(list_projects))
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
        .route(
            "/api/v1/sweep-trials/{trial_id}/complete",
            post(complete_sweep_trial),
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
        .with_state(AppState {
            catalog,
            metrics,
            blobs,
            ingest_permits: Arc::new(Semaphore::new(INGEST_WORKERS)),
            query_permits: Arc::new(Semaphore::new(QUERY_WORKERS)),
            artifact_download_permits: Arc::new(Semaphore::new(ARTIFACT_DOWNLOAD_WORKERS)),
            chart_series_cache: Arc::new(Mutex::new(ChartSeriesCache::default())),
            chart_axis_extent_cache,
            telemetry: Arc::clone(&telemetry),
        })
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(telemetry, record_request));
    #[cfg(feature = "embedded-dashboard")]
    let router = router.fallback(embedded_dashboard);
    router
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
    Ok(Json(DiagnosticsResponse {
        service: "runloom".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        schema_version: state.catalog.schema_version().await?,
        uptime_seconds: telemetry.started_at.elapsed().as_secs(),
        requests_total: telemetry.requests_total.load(Ordering::Relaxed),
        requests_active: telemetry.requests_active.load(Ordering::Relaxed),
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
        ingest_permits_available: state.ingest_permits.available_permits(),
        query_permits_available: state.query_permits.available_permits(),
        recent_slow_requests,
    }))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct SweepListQuery {
    before: Option<SweepId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct SweepTrialListQuery {
    before: Option<SweepTrialId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct ReportListQuery {
    before: Option<ReportId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn list_projects(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> Result<Json<ProjectListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let projects = state.catalog.list_projects(query.limit).await?;
    Ok(Json(ProjectListResponse { projects }))
}

async fn list_runs(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<RunListResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_list_limit(query.limit)?;
    let runs = state.catalog.list_runs(&project, query.limit).await?;
    Ok(Json(RunListResponse { runs }))
}

async fn query_runs(
    State(state): State<AppState>,
    Json(request): Json<RunQueryRequest>,
) -> Result<Json<RunQueryResponse>, HttpError> {
    validate_run_query(&request)?;
    let runs = state.catalog.query_runs(&request).await?;
    let next_before = if runs.len() == request.limit {
        runs.last().map(|run| run.id)
    } else {
        None
    };
    Ok(Json(RunQueryResponse { runs, next_before }))
}

async fn create_sweep(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(request): Json<CreateSweepRequest>,
) -> Result<(StatusCode, Json<CreateSweepResponse>), HttpError> {
    validate_project_name(&project)?;
    validate_sweep(&request)?;
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
    let sweeps = state
        .catalog
        .list_sweeps(&project, query.before, query.limit)
        .await?;
    let next_before = if sweeps.len() == query.limit {
        sweeps.last().map(|sweep| sweep.id)
    } else {
        None
    };
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
    let trials = state
        .catalog
        .list_sweep_trials(sweep_id, query.before, query.limit)
        .await?;
    let next_before = if trials.len() == query.limit {
        trials.last().map(|trial| trial.id)
    } else {
        None
    };
    Ok(Json(SweepTrialListResponse {
        trials,
        next_before,
    }))
}

async fn complete_sweep_trial(
    State(state): State<AppState>,
    Path(trial_id): Path<SweepTrialId>,
    Json(request): Json<CompleteSweepTrialRequest>,
) -> Result<Json<SweepTrialRecord>, HttpError> {
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
    Ok(Json(
        state
            .catalog
            .complete_sweep_trial(trial_id, &request)
            .await?,
    ))
}

async fn create_report(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(request): Json<CreateReportRequest>,
) -> Result<(StatusCode, Json<CreateReportResponse>), HttpError> {
    validate_project_name(&project)?;
    validate_report(
        &request.name,
        request.description.as_deref(),
        &request.layout,
    )?;
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
    let reports = state
        .catalog
        .list_reports(&project, query.before, query.limit)
        .await?;
    let next_before = if reports.len() == query.limit {
        reports.last().map(|report| report.id)
    } else {
        None
    };
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
    Ok(Json(
        state.catalog.update_report(report_id, &request).await?,
    ))
}

async fn delete_report(
    State(state): State<AppState>,
    Path(report_id): Path<ReportId>,
) -> Result<Json<ReportRecord>, HttpError> {
    Ok(Json(state.catalog.delete_report(report_id).await?))
}

async fn health(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let version = env!("CARGO_PKG_VERSION");
    match state.catalog.health_check().await {
        Ok(()) => (StatusCode::OK, Json(HealthResponse::healthy(version))),
        Err(error) => {
            tracing::error!(%error, "catalog health check failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(HealthResponse::unhealthy(version)),
            )
        }
    }
}

async fn create_run(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(request): Json<CreateRunRequest>,
) -> Result<(StatusCode, Json<CreateRunResponse>), HttpError> {
    validate_project_name(&project)?;
    validate_create_run(&request)?;
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

async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, HttpError> {
    Ok(Json(state.catalog.get_run(run_id).await?))
}

async fn update_config(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<ConfigUpdateRequest>,
) -> Result<Json<RunUpdateResponse>, HttpError> {
    validate_document_updates(&request.updates, "config", MAX_CONFIG_BYTES)?;
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
    let run = state
        .catalog
        .update_summary(run_id, &request.updates)
        .await?;
    Ok(Json(RunUpdateResponse { run }))
}

async fn metric_keys(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
) -> Result<Json<MetricKeyListResponse>, HttpError> {
    let keys = state.catalog.metric_keys(run_id).await?;
    Ok(Json(MetricKeyListResponse { run_id, keys }))
}

async fn ingest_batch(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<IngestBatchRequest>,
) -> Result<(StatusCode, Json<IngestBatchResponse>), HttpError> {
    validate_batch(&request)?;
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
    let _permit = Arc::clone(&state.ingest_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("ingestion worker pool is unavailable"))?;
    let written = tokio::task::spawn_blocking(move || {
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
            if let Err(cleanup_error) = state
                .metrics
                .store()
                .remove_segment(&manifest.relative_path)
            {
                tracing::error!(%cleanup_error, "failed to clean up unregistered metric segment");
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
    let run = state.catalog.finish_run(run_id, &request.summary).await?;
    Ok(Json(FinishRunResponse { run }))
}

async fn create_alert(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<CreateAlertRequest>,
) -> Result<(StatusCode, Json<CreateAlertResponse>), HttpError> {
    validate_alert(&request)?;
    let (alert, duplicate) = state.catalog.create_alert(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateAlertResponse { alert, duplicate })))
}

#[derive(Debug, Deserialize)]
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
    let alerts = state
        .catalog
        .list_alerts(run_id, query.before, query.limit)
        .await?;
    let next_before = if alerts.len() == query.limit {
        alerts.last().map(|alert| alert.id)
    } else {
        None
    };
    Ok(Json(AlertListResponse {
        alerts,
        next_before,
    }))
}

async fn create_rich_value(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<CreateRichValueRequest>,
) -> Result<(StatusCode, Json<CreateRichValueResponse>), HttpError> {
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
    let (value, duplicate) = state.catalog.create_rich_value(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateRichValueResponse { value, duplicate })))
}

#[derive(Debug, Deserialize)]
struct RichValueListQuery {
    before: Option<RichValueId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

async fn list_rich_values(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<RichValueListQuery>,
) -> Result<Json<RichValueListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let values = state
        .catalog
        .list_rich_values(run_id, query.before, query.limit)
        .await?;
    let next_before = if values.len() == query.limit {
        values.last().map(|value| value.id)
    } else {
        None
    };
    Ok(Json(RichValueListResponse {
        values,
        next_before,
    }))
}

async fn create_artifact(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<CreateArtifactRequest>,
) -> Result<(StatusCode, Json<CreateArtifactResponse>), HttpError> {
    validate_artifact(&request)?;
    let entries = request.entries.clone();
    let blobs = state.blobs.clone();
    tokio::task::spawn_blocking(move || verify_artifact_blobs(&blobs, &entries))
        .await
        .map_err(|error| {
            HttpError::internal(format!("artifact verification worker failed: {error}"))
        })??;
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
struct ArtifactListQuery {
    before: Option<ArtifactId>,
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
    let artifacts = state
        .catalog
        .list_project_artifacts(&project, query.before, query.limit)
        .await?;
    let next_before = if artifacts.len() == query.limit {
        artifacts.last().map(|artifact| artifact.id)
    } else {
        None
    };
    Ok(Json(ArtifactListResponse {
        artifacts,
        next_before,
    }))
}

async fn list_run_artifacts(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<ArtifactListQuery>,
) -> Result<Json<RunArtifactListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let artifacts = state
        .catalog
        .list_run_artifacts(run_id, query.before, query.limit)
        .await?;
    let next_before = if artifacts.len() == query.limit {
        artifacts.last().map(|linked| linked.artifact.id)
    } else {
        None
    };
    Ok(Json(RunArtifactListResponse {
        artifacts,
        next_before,
    }))
}

async fn get_artifact_lineage(
    State(state): State<AppState>,
    Path(artifact_id): Path<ArtifactId>,
) -> Result<Json<ArtifactLineageResponse>, HttpError> {
    let artifact = state.catalog.get_artifact(artifact_id).await?;
    let (input_runs, output_runs) = tokio::try_join!(
        state
            .catalog
            .artifact_lineage(artifact_id, ArtifactRelation::Input, MAX_LIST_ITEMS),
        state
            .catalog
            .artifact_lineage(artifact_id, ArtifactRelation::Output, MAX_LIST_ITEMS),
    )?;
    Ok(Json(ArtifactLineageResponse {
        artifact,
        input_runs,
        output_runs,
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
    let permit = Arc::clone(&state.artifact_download_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
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
        &entry.blob.digest,
        Some(&entry.blob.mime_type),
        request,
    )
    .await
}

async fn create_trace_span(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Json(request): Json<CreateTraceSpanRequest>,
) -> Result<(StatusCode, Json<CreateTraceSpanResponse>), HttpError> {
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
    let (span, duplicate) = state.catalog.create_trace_span(run_id, &request).await?;
    let status = if duplicate {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(CreateTraceSpanResponse { span, duplicate })))
}

#[derive(Debug, Deserialize)]
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
    let spans = state
        .catalog
        .list_trace_spans(run_id, query.before, query.q.as_deref(), query.limit)
        .await?;
    let next_before = if spans.len() == query.limit {
        spans.last().map(|span| span.id)
    } else {
        None
    };
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
    let file_name = header_text(&headers, "x-runloom-file-name")
        .map(str::to_owned)
        .filter(|value| !value.is_empty());
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

    let staging_path = state.blobs.staging_path().map_err(HttpError::from)?;
    let result = stream_blob(&staging_path, body).await;
    let (actual_digest, size) = match result {
        Ok(value) => value,
        Err(error) => {
            let _ = tokio::fs::remove_file(&staging_path).await;
            return Err(error);
        }
    };
    if actual_digest != digest {
        let _ = tokio::fs::remove_file(&staging_path).await;
        return Err(HttpError::invalid(format!(
            "blob digest mismatch: expected {digest}, received {actual_digest}"
        )));
    }
    let blobs = state.blobs.clone();
    let install_path = staging_path.clone();
    let install_digest = digest.clone();
    tokio::task::spawn_blocking(move || blobs.install(&install_path, &install_digest))
        .await
        .map_err(|error| HttpError::internal(format!("blob install worker failed: {error}")))??;
    Ok((
        StatusCode::CREATED,
        Json(BlobUploadResponse {
            blob: BlobRef {
                digest,
                size,
                mime_type,
                file_name,
            },
            duplicate: false,
        }),
    ))
}

#[derive(Debug, Deserialize)]
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
    serve_blob(&state.blobs, &digest, query.mime.as_deref(), request).await
}

async fn serve_blob(
    blobs: &BlobStore,
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
    let response = match ServeFile::new(path).oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    };
    let mut response = response.map(Body::new);
    if let Some(mime_type) = mime_type {
        let value = mime_type
            .parse()
            .map_err(|_| HttpError::invalid("invalid blob MIME type"))?;
        response.headers_mut().insert("content-type", value);
    }
    Ok(response)
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

#[derive(Debug, Deserialize)]
struct HistoryQuery {
    keys: String,
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

struct ChartQueryLease {
    _snapshot: OwnedRwLockReadGuard<()>,
    _permit: OwnedSemaphorePermit,
}

struct CancelChartQueryOnDrop(Arc<AtomicBool>);

impl Drop for CancelChartQueryOnDrop {
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

    let mut lease = ChartQueryLease {
        _snapshot: snapshot,
        _permit: permit,
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelChartQueryOnDrop(Arc::clone(&cancelled));
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
    let mut lease = ChartQueryLease {
        _snapshot: snapshot,
        _permit: permit,
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelChartQueryOnDrop(Arc::clone(&cancelled));
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
    mut lease: ChartQueryLease,
    cancelled: Arc<AtomicBool>,
) -> Result<(ChartAxisExtentScanner, ChartQueryLease), HttpError> {
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
    mut lease: ChartQueryLease,
    cancelled: Arc<AtomicBool>,
) -> Result<(ChartStepExtentScanner, ChartQueryLease), HttpError> {
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
    mut lease: ChartQueryLease,
    cancelled: Arc<AtomicBool>,
) -> Result<(ChartHistorySampler, ChartQueryLease), HttpError> {
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
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, HttpError> {
    let keys = parse_history_keys(&query.keys)?;
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
    let _permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let _snapshot = state.metrics.read_snapshot().await;
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
    let mut response = tokio::task::spawn_blocking(move || {
        metrics.read_history(run_id, &segments, &keys, after, limit)
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
    let _permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
    let _snapshot = state.metrics.read_snapshot().await;
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
        sampler =
            tokio::task::spawn_blocking(move || -> Result<MinMaxHistorySampler, StorageError> {
                sampler.read_segments(&metrics, &segments)?;
                Ok(sampler)
            })
            .await
            .map_err(|error| HttpError::internal(format!("query worker failed: {error}")))??;
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
    if request.timestamp_ms < 0 {
        return Err(HttpError::invalid("alert timestamp cannot be negative"));
    }
    Ok(())
}

fn validate_rich_value(request: &CreateRichValueRequest) -> Result<(), HttpError> {
    if request.key.is_empty()
        || request.key.len() > MAX_RICH_KEY_BYTES
        || request.key.chars().any(char::is_control)
    {
        return Err(HttpError::invalid(format!(
            "rich value keys must contain 1 to {MAX_RICH_KEY_BYTES} non-control bytes"
        )));
    }
    if request.timestamp_ms < 0 {
        return Err(HttpError::invalid(
            "rich value timestamp cannot be negative",
        ));
    }
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

fn validate_artifact(request: &CreateArtifactRequest) -> Result<(), HttpError> {
    validate_artifact_component(&request.name, "artifact name", MAX_ARTIFACT_NAME_BYTES)?;
    validate_artifact_component(
        &request.artifact_type,
        "artifact type",
        MAX_ARTIFACT_TYPE_BYTES,
    )?;
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
    if request.start_time_ms < 0
        || request.end_time_ms < 0
        || request.end_time_ms < request.start_time_ms
    {
        return Err(HttpError::invalid(
            "trace timestamps must be non-negative and end at or after start",
        ));
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
        name.is_empty() || name.len() > MAX_FILE_NAME_BYTES || name.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid(format!(
            "file name must contain 1 to {MAX_FILE_NAME_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn validate_create_run(request: &CreateRunRequest) -> Result<(), HttpError> {
    if request.resume == ResumePolicy::Must && request.id.is_none() {
        return Err(HttpError::invalid(
            "resume='must' requires an explicit run ID",
        ));
    }
    if request
        .name
        .as_ref()
        .is_some_and(|name| name.is_empty() || name.len() > MAX_RUN_NAME_BYTES)
    {
        return Err(HttpError::invalid(format!(
            "run name must contain 1 to {MAX_RUN_NAME_BYTES} bytes"
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
    Ok(())
}

fn validate_batch(request: &IngestBatchRequest) -> Result<(), HttpError> {
    if request.points.is_empty() || request.points.len() > MAX_BATCH_POINTS {
        return Err(HttpError::invalid(format!(
            "metric batches must contain 1 to {MAX_BATCH_POINTS} points"
        )));
    }
    let mut previous_sequence = None;
    for point in &request.points {
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

fn parse_history_keys(value: &str) -> Result<Vec<String>, HttpError> {
    let keys: BTreeSet<String> = value
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .collect();
    if keys.is_empty() || keys.len() > MAX_HISTORY_KEYS {
        return Err(HttpError::invalid(format!(
            "history queries must request 1 to {MAX_HISTORY_KEYS} metric keys"
        )));
    }
    if keys.iter().any(|key| key.len() > MAX_METRIC_KEY_BYTES) {
        return Err(HttpError::invalid("history metric key is too long"));
    }
    Ok(keys.into_iter().collect())
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
            _ => {}
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

fn validate_run_query(request: &RunQueryRequest) -> Result<(), HttpError> {
    validate_list_limit(request.limit)?;
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
        (self.status, Json(self.body)).into_response()
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
            CatalogError::Limit(_) => Self::invalid(error.to_string()),
            CatalogError::CreateDirectory { .. }
            | CatalogError::Database(_)
            | CatalogError::InvalidData(_)
            | CatalogError::SchemaVersion { .. } => Self::internal(error.to_string()),
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
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use runloom_catalog::Catalog;
    use runloom_protocol::{
        AlertId, AlertLevel, AlertListResponse, ArtifactEntry, ArtifactListResponse, BlobRef,
        BlobUploadResponse, ChartAlignment, ChartHistoryQueryRequest, ChartHistoryQueryResponse,
        ChartHistoryResponse, ChartMetricHistory, ChartSeriesRequest, ChartViewport,
        ClaimSweepTrialRequest, ClaimSweepTrialResponse, CompleteSweepTrialRequest,
        ConfigUpdateRequest, CreateAlertRequest, CreateAlertResponse, CreateArtifactRequest,
        CreateArtifactResponse, CreateReportRequest, CreateReportResponse, CreateRichValueRequest,
        CreateRichValueResponse, CreateRunRequest, CreateRunResponse, CreateSweepRequest,
        CreateSweepResponse, CreateTraceSpanRequest, CreateTraceSpanResponse, DiagnosticsResponse,
        EarlyTerminateConfig, FinishRunRequest, FinishRunResponse, HealthResponse, HealthStatus,
        HistoryResponse, IngestBatchRequest, IngestBatchResponse, MetricGoal,
        MetricKeyListResponse, MetricPoint, ProjectListResponse, ReportLayout, ReportListResponse,
        ReportPanel, ReportPanelKind, ReportRecord, ResumePolicy, RichValueId, RichValueKind,
        RichValueListResponse, RunArtifactListResponse, RunId, RunListResponse, RunQueryRequest,
        RunQueryResponse, RunState, RunUpdateResponse, SummaryUpdateRequest, SweepMethod,
        SweepMetric, SweepParameter, SweepTrialListResponse, SweepTrialState, TraceKind,
        TraceSpanId, TraceSpanListResponse, TraceSpanRecord, TraceStatus, UpdateReportRequest,
        UseArtifactRequest,
    };
    use runloom_storage::{BlobStore, ChartAxisExtent, MetricStore};
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::{
        CHART_AXIS_EXTENT_CACHE_MAX_ENTRIES, CHART_SERIES_CACHE_MAX_ENTRIES, CachedChartOrigin,
        ChartAxisExtentCache, ChartAxisExtentCacheKey, ChartSeriesCache, ChartSeriesCacheKey,
        CompactionConfig, CompactionOutcome, MetricRuntime, app, app_with_axis_extent_cache,
        app_with_runtime, compact_once,
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

    #[tokio::test]
    async fn health_checks_the_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let router = app(catalog, MetricStore::new(directory.path().join("metrics")));
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
                .oneshot(Request::get("/api/v1/diagnostics").body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(diagnostics.schema_version, 1);
        assert_eq!(diagnostics.requests_total, 2);
        assert_eq!(diagnostics.requests_active, 1);
        assert_eq!(diagnostics.query_permits_available, super::QUERY_WORKERS);
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
            let created: CreateRunResponse = response_json(
                router
                    .clone()
                    .oneshot(json_request(
                        "POST",
                        "/api/v1/projects/query-demo/runs",
                        &CreateRunRequest {
                            id: None,
                            name: Some(name.to_owned()),
                            config: BTreeMap::from([
                                ("seed".to_owned(), seed.into()),
                                ("nullable".to_owned(), serde_json::Value::Null),
                            ]),
                            resume: ResumePolicy::Never,
                            sweep_trial_id: None,
                        },
                    )?)
                    .await?,
            )
            .await?;
            created_ids.push(created.run.id);
        }

        let filtered: RunQueryResponse = response_json(
            router
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/api/v1/query/runs",
                    &RunQueryRequest {
                        project: Some("query-demo".to_owned()),
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
        let report_cursor = reports.next_before.expect("full report page has a cursor");
        let next_reports: ReportListResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/projects/report-demo/reports?limit=1&before={report_cursor}"
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert!(next_reports.reports.is_empty());
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
                    metrics: BTreeMap::from([("loss".to_owned(), 2.0), ("reward".to_owned(), 3.0)]),
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
                .oneshot(Request::get(format!("{rich_path}?limit=10")).body(Body::empty())?)
                .await?,
        )
        .await?;
        assert_eq!(values.values, vec![created_value.value]);

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
        assert_eq!(to_bytes(response.into_body(), 64).await?, &video[7..=11]);

        let artifact_path = format!("/api/v1/runs/{}/artifacts", created.run.id);
        let artifact_request = CreateArtifactRequest {
            id: Some(runloom_protocol::ArtifactId::new()),
            name: "policy".to_owned(),
            artifact_type: "model".to_owned(),
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
        let lineage: runloom_protocol::ArtifactLineageResponse = response_json(
            router
                .clone()
                .oneshot(
                    Request::get(format!(
                        "/api/v1/artifacts/{}/lineage",
                        version_zero.artifact.id
                    ))
                    .body(Body::empty())?,
                )
                .await?,
        )
        .await?;
        assert_eq!(lineage.input_runs, vec![created.run.id]);
        assert_eq!(lineage.output_runs, vec![created.run.id]);
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
        assert_eq!(traces.spans, vec![created_trace.span.clone()]);
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

        let history_path = format!("/api/v1/runs/{}/history?keys=loss&limit=1", created.run.id);
        let response = router
            .clone()
            .oneshot(Request::get(history_path).body(Body::empty())?)
            .await?;
        let history: HistoryResponse = response_json(response).await?;
        assert_eq!(history.sequence, vec![1]);
        assert_eq!(history.metrics["loss"], vec![Some(2.0)]);
        assert_eq!(history.next_after, Some(1));
        assert!(!history.sampled);
        assert_eq!(history.source_points, None);

        let sampled_path = format!(
            "/api/v1/runs/{}/history?keys=loss&max_points=2",
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
            "/api/v1/runs/{}/history?keys=loss&limit=2&max_points=2",
            created.run.id
        );
        let response = router
            .clone()
            .oneshot(Request::get(invalid_path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

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
        assert_eq!(runs.runs[0].summary["loss"], 1.0);
        assert_eq!(runs.runs[0].summary["status"], "running");
        assert_eq!(runs.runs[0].config["seed"], 7);
        let response = router
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/runs/{}/metrics", created.run.id))
                    .body(Body::empty())?,
            )
            .await?;
        let keys: MetricKeyListResponse = response_json(response).await?;
        assert_eq!(keys.keys, vec!["loss", "reward"]);

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
        for batch_index in 0..3u64 {
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
        let history_path = format!("/api/v1/runs/{}/history?keys=loss&limit=10", created.run.id);
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
        assert_eq!(catalog.list_segments(created.run.id, None).await?.len(), 3);
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
            CompactionOutcome::SegmentsCompacted { inputs: 3, rows: 6 }
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
