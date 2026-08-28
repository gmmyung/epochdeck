#![forbid(unsafe_code)]

mod compaction;

pub use compaction::{CompactionConfig, MetricRuntime, run_compaction_worker};
#[cfg(test)]
use compaction::{CompactionOutcome, compact_once};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, Request, StatusCode};
#[cfg(feature = "embedded-dashboard")]
use axum::http::{HeaderValue, Uri, header};
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
    ClaimSweepTrialRequest, ClaimSweepTrialResponse, CompleteSweepTrialRequest,
    ConfigUpdateRequest, CreateAlertRequest, CreateAlertResponse, CreateArtifactRequest,
    CreateArtifactResponse, CreateReportRequest, CreateReportResponse, CreateRichValueRequest,
    CreateRichValueResponse, CreateRunRequest, CreateRunResponse, CreateSweepRequest,
    CreateSweepResponse, CreateTraceSpanRequest, CreateTraceSpanResponse, DiagnosticsResponse,
    FinishRunRequest, FinishRunResponse, HealthResponse, HistoryResponse, IngestBatchRequest,
    IngestBatchResponse, MAX_ALERT_TEXT_BYTES, MAX_ALERT_TITLE_BYTES, MAX_ARTIFACT_ENTRIES,
    MAX_ARTIFACT_MANIFEST_BYTES, MAX_BATCH_POINTS, MAX_CONFIG_BYTES, MAX_HISTORY_KEYS,
    MAX_HISTORY_POINTS, MAX_METRICS_PER_POINT, MAX_RICH_KEY_BYTES, MAX_RICH_METADATA_BYTES,
    MAX_SUMMARY_BYTES, MAX_TRACE_METADATA_BYTES, MetricKeyListResponse, ProjectListResponse,
    ReportId, ReportLayout, ReportListResponse, ReportPanelKind, ReportRecord, ResumePolicy,
    RichValueId, RichValueKind, RichValueListResponse, RunArtifactListResponse, RunId,
    RunListResponse, RunQueryRequest, RunQueryResponse, RunRecord, RunState, RunUpdateResponse,
    SlowRequestRecord, SummaryUpdateRequest, SweepId, SweepListResponse, SweepTrialId,
    SweepTrialListResponse, SweepTrialRecord, SweepTrialState, TraceSpanId, TraceSpanListResponse,
    UpdateReportRequest, UseArtifactRequest,
};
use runloom_storage::{
    BlobStore, MetricStore, MinMaxHistorySampler, SegmentSource, SegmentTail, StorageError,
};
#[cfg(feature = "embedded-dashboard")]
use rust_embed::RustEmbed;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
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
const MAX_LIST_ITEMS: usize = 200;
const MAX_MIME_TYPE_BYTES: usize = 256;
const MAX_FILE_NAME_BYTES: usize = 512;
const MAX_ARTIFACT_NAME_BYTES: usize = 128;
const MAX_ARTIFACT_TYPE_BYTES: usize = 64;
const MAX_ARTIFACT_ALIAS_BYTES: usize = 128;
const MAX_ARTIFACT_PATH_BYTES: usize = 1_024;
const MAX_ARTIFACT_DESCRIPTION_BYTES: usize = 64 * 1024;
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

#[derive(Debug, Clone)]
struct AppState {
    catalog: Catalog,
    metrics: MetricRuntime,
    blobs: BlobStore,
    ingest_permits: Arc<Semaphore>,
    query_permits: Arc<Semaphore>,
    telemetry: Arc<RequestTelemetry>,
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
        .with_state(AppState {
            catalog,
            metrics,
            blobs,
            ingest_permits: Arc::new(Semaphore::new(INGEST_WORKERS)),
            query_permits: Arc::new(Semaphore::new(QUERY_WORKERS)),
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
    let history_query = path.ends_with("/history");
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
    let _snapshot = state.metrics.read_snapshot().await;
    let rich_next_step = state.catalog.rich_value_next_step(run_id).await?;
    let Some(segment) = state.catalog.last_segment(run_id).await? else {
        return Ok((1, rich_next_step));
    };
    let metrics = state.metrics.store().clone();
    let _permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
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
    let _permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
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
    let _permit = Arc::clone(&state.query_permits)
        .acquire_owned()
        .await
        .map_err(|_| HttpError::internal("query worker pool is unavailable"))?;
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
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use runloom_catalog::Catalog;
    use runloom_protocol::{
        AlertId, AlertLevel, AlertListResponse, ArtifactEntry, ArtifactListResponse, BlobRef,
        BlobUploadResponse, ClaimSweepTrialRequest, ClaimSweepTrialResponse,
        CompleteSweepTrialRequest, ConfigUpdateRequest, CreateAlertRequest, CreateAlertResponse,
        CreateArtifactRequest, CreateArtifactResponse, CreateReportRequest, CreateReportResponse,
        CreateRichValueRequest, CreateRichValueResponse, CreateRunRequest, CreateRunResponse,
        CreateSweepRequest, CreateSweepResponse, CreateTraceSpanRequest, CreateTraceSpanResponse,
        DiagnosticsResponse, EarlyTerminateConfig, FinishRunRequest, FinishRunResponse,
        HealthResponse, HealthStatus, HistoryResponse, IngestBatchRequest, IngestBatchResponse,
        MetricGoal, MetricKeyListResponse, MetricPoint, ProjectListResponse, ReportLayout,
        ReportListResponse, ReportPanel, ReportPanelKind, ReportRecord, ResumePolicy, RichValueId,
        RichValueKind, RichValueListResponse, RunArtifactListResponse, RunListResponse,
        RunQueryRequest, RunQueryResponse, RunState, RunUpdateResponse, SummaryUpdateRequest,
        SweepMethod, SweepMetric, SweepParameter, SweepTrialListResponse, SweepTrialState,
        TraceKind, TraceSpanId, TraceSpanListResponse, TraceSpanRecord, TraceStatus,
        UpdateReportRequest, UseArtifactRequest,
    };
    use runloom_storage::MetricStore;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::{
        CompactionConfig, CompactionOutcome, MetricRuntime, app, app_with_runtime, compact_once,
    };

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
            entries: vec![ArtifactEntry {
                path: "checkpoint.bin".to_owned(),
                blob: uploaded.blob.clone(),
            }],
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
