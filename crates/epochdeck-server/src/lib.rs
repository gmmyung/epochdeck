#![forbid(unsafe_code)]

mod chart_cache;
mod compaction;
mod dashboard;
mod diagnostics;
mod discovery;

use chart_cache::{
    CachedChartOrigin, ChartAxisExtentCache, ChartAxisExtentCacheKey, ChartSeriesCache,
    ChartSeriesCacheKey,
};
pub use compaction::{CompactionConfig, MetricRuntime, run_compaction_worker};
#[cfg(test)]
use compaction::{CompactionError, CompactionOutcome, compact_once};
pub use dashboard::{DashboardConfig, DashboardConfigError};

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
use epochdeck_catalog::{
    BatchRegistration, BatchStatus, Catalog, CatalogError, MAX_SEGMENTS_PER_QUERY, SegmentManifest,
};
use epochdeck_protocol::{
    AlertId, AlertListResponse, ApiError, ArtifactId, ArtifactLineageResponse,
    ArtifactListResponse, ArtifactRecord, ArtifactRelation, BlobRef, BlobUploadResponse,
    ChartAlignment, ChartHistoryQueryRequest, ChartHistoryQueryResponse, ChartHistoryResponse,
    ChartMetricHistory, ChartRunWatermark, ChartSeriesHistory, ConfigUpdateRequest,
    CreateAlertRequest, CreateAlertResponse, CreateArtifactRequest, CreateArtifactResponse,
    CreateRichValueRequest, CreateRichValueResponse, CreateRunRequest, CreateRunResponse,
    DiagnosticsResponse, FinishRunRequest, FinishRunResponse, HealthResponse, HistoryResponse,
    IngestBatchRequest, IngestBatchResponse, MAX_ALERT_TEXT_BYTES, MAX_ALERT_TITLE_BYTES,
    MAX_ARTIFACT_ENTRIES, MAX_ARTIFACT_MANIFEST_BYTES, MAX_BATCH_POINTS, MAX_CHART_BUCKET_CELLS,
    MAX_CHART_BUCKETS, MAX_CHART_QUERY_CELLS, MAX_CHART_QUERY_RUNS, MAX_CHART_QUERY_SERIES,
    MAX_CONFIG_BYTES, MAX_HISTORY_KEYS, MAX_HISTORY_POINTS, MAX_JSON_SAFE_INTEGER,
    MAX_METRICS_PER_POINT, MAX_RICH_KEY_BYTES, MAX_RICH_METADATA_BYTES, MAX_SUMMARY_BYTES,
    ResumePolicy, RichValueId, RichValueKeyListResponse, RichValueKind, RichValueListResponse,
    RunArtifactListResponse, RunId, RunQueryRequest, RunState, RunUpdateResponse,
    SlowRequestRecord, SummaryUpdateRequest, UseArtifactRequest,
};
use epochdeck_storage::{
    BlobInstallation, BlobStore, ChartAxisExtent, ChartAxisExtentScanner, ChartCoordinate,
    ChartHistorySampler, ChartSamplingSpec, ChartStepExtent, ChartStepExtentScanner,
    MinMaxHistorySampler, SegmentInstallation, SegmentSource, SegmentTail, StorageError,
};
use futures_util::StreamExt;
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
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use dashboard::{dashboard_config, dashboard_favicon, dashboard_logo};
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
const DEFAULT_SLOW_REQUEST_MS: u64 = 1_000;
const MAX_RECENT_SLOW_REQUESTS: usize = 64;
const MAX_DIAGNOSTIC_PATH_BYTES: usize = 512;
const DEFAULT_CHART_BUCKETS: usize = 1_000;

#[derive(Debug, Clone)]
struct AppState {
    catalog: Catalog,
    metrics: MetricRuntime,
    blobs: BlobStore,
    dashboard: DashboardConfig,
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
    request_metrics: Arc<RequestMetrics>,
}

impl AppState {
    fn new(
        catalog: Catalog,
        metrics: MetricRuntime,
        blobs: BlobStore,
        dashboard: DashboardConfig,
        chart_axis_extent_cache: Arc<Mutex<ChartAxisExtentCache>>,
        request_metrics: Arc<RequestMetrics>,
    ) -> Self {
        Self {
            catalog,
            metrics,
            blobs,
            dashboard,
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
            request_metrics,
        }
    }
}

#[derive(Debug)]
struct RequestMetrics {
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

impl RequestMetrics {
    fn from_environment() -> Self {
        let threshold_ms = std::env::var("EPOCHDECK_SLOW_REQUEST_MS")
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

pub fn app(
    catalog: Catalog,
    metrics: MetricRuntime,
    blobs: BlobStore,
    dashboard: DashboardConfig,
) -> Router {
    let state = AppState::new(
        catalog,
        metrics,
        blobs,
        dashboard,
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
        Arc::new(RequestMetrics::from_environment()),
    );
    build_router(state)
}

fn build_router(state: AppState) -> Router {
    let admission_state = state.clone();
    let request_metrics = Arc::clone(&state.request_metrics);
    let router = Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/dashboard/config", get(dashboard_config))
        .route("/api/v1/dashboard/logo", get(dashboard_logo))
        .route("/api/v1/dashboard/favicon", get(dashboard_favicon))
        .route("/api/v1/diagnostics", get(diagnostics))
        .route("/api/v1/projects", get(list_projects))
        .route("/api/v1/projects/{project}", get(get_project))
        .route(
            "/api/v1/projects/{project}/metrics/query",
            post(query_project_metrics),
        )
        .route("/api/v1/query/runs", post(query_runs))
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
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_request(())
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_failure(()),
        )
        .layer(middleware::from_fn_with_state(
            admission_state,
            admit_api_request,
        ))
        .layer(middleware::from_fn(add_security_headers))
        .layer(middleware::from_fn_with_state(
            request_metrics,
            record_request_metrics,
        ))
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
            .request_metrics
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
    if !response
        .headers()
        .contains_key(header::CONTENT_SECURITY_POLICY)
    {
        response.headers_mut().insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(
                "default-src 'self'; base-uri 'none'; object-src 'none'; frame-ancestors 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; media-src 'self'; connect-src 'self'",
            ),
        );
    }
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

struct ActiveRequestGuard(Arc<RequestMetrics>);

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.0.requests_active.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn record_request_metrics(
    State(request_metrics): State<Arc<RequestMetrics>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    request_metrics
        .requests_total
        .fetch_add(1, Ordering::Relaxed);
    request_metrics
        .requests_active
        .fetch_add(1, Ordering::Relaxed);
    let _active_guard = ActiveRequestGuard(Arc::clone(&request_metrics));
    let method = request.method().to_string();
    let path = truncate_diagnostic_path(request.uri().path());
    let history_query = path.ends_with("/history")
        || path.ends_with("/chart-history")
        || path.ends_with("/chart-history/query");
    let started_at = Instant::now();
    let response = next.run(request).await;
    let elapsed = started_at.elapsed();
    let duration_ms = duration_millis(elapsed);
    let status = response.status();
    if status.is_server_error() {
        request_metrics
            .server_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }
    if history_query {
        request_metrics
            .history_queries_total
            .fetch_add(1, Ordering::Relaxed);
        request_metrics
            .history_query_duration_ms_total
            .fetch_add(duration_ms, Ordering::Relaxed);
        request_metrics
            .history_query_duration_ms_max
            .fetch_max(duration_ms, Ordering::Relaxed);
    }
    if elapsed >= request_metrics.slow_threshold {
        request_metrics
            .slow_requests_total
            .fetch_add(1, Ordering::Relaxed);
        let timestamp_ms = duration_millis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
        );
        let record = SlowRequestRecord {
            method,
            path,
            status: status.as_u16(),
            duration_ms,
            timestamp_ms,
        };
        if let Ok(mut recent) = request_metrics.recent_slow_requests.lock() {
            if recent.len() == MAX_RECENT_SLOW_REQUESTS {
                recent.pop_front();
            }
            recent.push_back(record);
        }
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

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

async fn diagnostics(
    State(state): State<AppState>,
) -> Result<Json<DiagnosticsResponse>, HttpError> {
    let request_metrics = &state.request_metrics;
    let recent_slow_requests = request_metrics
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
        service: "epochdeck".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        uptime_seconds: request_metrics.started_at.elapsed().as_secs(),
        requests_total: request_metrics.requests_total.load(Ordering::Relaxed),
        requests_active: request_metrics.requests_active.load(Ordering::Relaxed),
        requests_rejected_total: request_metrics
            .requests_rejected_total
            .load(Ordering::Relaxed),
        server_errors_total: request_metrics.server_errors_total.load(Ordering::Relaxed),
        slow_requests_total: request_metrics.slow_requests_total.load(Ordering::Relaxed),
        slow_request_threshold_ms: duration_millis(request_metrics.slow_threshold),
        history_queries_total: request_metrics
            .history_queries_total
            .load(Ordering::Relaxed),
        history_query_duration_ms_total: request_metrics
            .history_query_duration_ms_total
            .load(Ordering::Relaxed),
        history_query_duration_ms_max: request_metrics
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
    let _mutation = acquire_mutation_locks(&state, vec![mutation_lock_index(&run_id)]).await;
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
        return Ok((
            StatusCode::OK,
            Json(IngestBatchResponse {
                run_id,
                batch_sequence: request.batch_sequence,
                accepted_points: request.points.len(),
                duplicate: true,
                metric_revision,
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
    Ok((
        status,
        Json(IngestBatchResponse {
            run_id,
            batch_sequence,
            accepted_points,
            duplicate,
            metric_revision,
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
) -> Result<Json<epochdeck_protocol::RichValueRecord>, HttpError> {
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
    entries: &[epochdeck_protocol::ArtifactEntry],
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
    let file_name = match headers.get("x-epochdeck-file-name") {
        None => None,
        Some(value) => {
            let encoded = value.to_str().map_err(|_| {
                HttpError::invalid("x-epochdeck-file-name must be percent-encoded UTF-8")
            })?;
            if encoded.is_empty() {
                None
            } else {
                Some(percent_decode_utf8(encoded, "x-epochdeck-file-name")?)
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
    entries: &[epochdeck_protocol::ArtifactEntry],
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
mod tests;
