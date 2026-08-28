#![forbid(unsafe_code)]

mod compaction;

pub use compaction::{CompactionConfig, MetricRuntime, run_compaction_worker};
#[cfg(test)]
use compaction::{CompactionOutcome, compact_once};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use runloom_catalog::{
    BatchRegistration, BatchStatus, Catalog, CatalogError, MAX_SEGMENTS_PER_QUERY, SegmentManifest,
};
use runloom_protocol::{
    ApiError, ConfigUpdateRequest, CreateRunRequest, CreateRunResponse, FinishRunRequest,
    FinishRunResponse, HealthResponse, HistoryResponse, IngestBatchRequest, IngestBatchResponse,
    MAX_BATCH_POINTS, MAX_CONFIG_BYTES, MAX_HISTORY_KEYS, MAX_HISTORY_POINTS,
    MAX_METRICS_PER_POINT, MAX_SUMMARY_BYTES, MetricKeyListResponse, ProjectListResponse,
    ResumePolicy, RunId, RunListResponse, RunRecord, RunState, RunUpdateResponse,
    SummaryUpdateRequest,
};
use runloom_storage::{MetricStore, MinMaxHistorySampler, SegmentSource, StorageError};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_PROJECT_NAME_BYTES: usize = 128;
const MAX_RUN_NAME_BYTES: usize = 256;
const MAX_METRIC_KEY_BYTES: usize = 256;
const INGEST_WORKERS: usize = 2;
const QUERY_WORKERS: usize = 4;
const MAX_LIST_ITEMS: usize = 200;

#[derive(Debug, Clone)]
struct AppState {
    catalog: Catalog,
    metrics: MetricRuntime,
    ingest_permits: Arc<Semaphore>,
    query_permits: Arc<Semaphore>,
}

pub fn app(catalog: Catalog, metrics: MetricStore) -> Router {
    app_with_runtime(catalog, MetricRuntime::new(metrics))
}

pub fn app_with_runtime(catalog: Catalog, metrics: MetricRuntime) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/projects", get(list_projects))
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
        .route("/api/v1/runs/{run_id}/history", get(history))
        .with_state(AppState {
            catalog,
            metrics,
            ingest_permits: Arc::new(Semaphore::new(INGEST_WORKERS)),
            query_permits: Arc::new(Semaphore::new(QUERY_WORKERS)),
        })
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
}

#[derive(Debug, Deserialize)]
struct ListQuery {
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
    Ok((status, Json(CreateRunResponse { run, resumed })))
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
    let run = state.catalog.finish_run(run_id, &request.summary).await?;
    Ok(Json(FinishRunResponse { run }))
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

fn batch_digest(request: &IngestBatchRequest) -> Result<String, HttpError> {
    let encoded = serde_json::to_vec(request)
        .map_err(|error| HttpError::invalid(format!("batch is not serializable: {error}")))?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn latest_metrics(request: &IngestBatchRequest) -> BTreeMap<String, f64> {
    let mut summary = BTreeMap::new();
    for point in &request.points {
        summary.extend(point.metrics.clone());
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
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use runloom_catalog::Catalog;
    use runloom_protocol::{
        ConfigUpdateRequest, CreateRunRequest, CreateRunResponse, FinishRunRequest,
        FinishRunResponse, HealthResponse, HealthStatus, HistoryResponse, IngestBatchRequest,
        IngestBatchResponse, MetricKeyListResponse, MetricPoint, ProjectListResponse, ResumePolicy,
        RunListResponse, RunUpdateResponse, SummaryUpdateRequest,
    };
    use runloom_storage::MetricStore;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::{
        CompactionConfig, CompactionOutcome, MetricRuntime, app, app_with_runtime, compact_once,
    };

    #[tokio::test]
    async fn health_checks_the_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let response = app(catalog, MetricStore::new(directory.path().join("metrics")))
            .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let health: HealthResponse = serde_json::from_slice(&body)?;
        assert_eq!(health.status, HealthStatus::Healthy);
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
                    step: 0,
                    timestamp_ms: 1_000,
                    metrics: BTreeMap::from([("loss".to_owned(), 2.0), ("reward".to_owned(), 3.0)]),
                },
                MetricPoint {
                    sequence: 2,
                    step: 1,
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
