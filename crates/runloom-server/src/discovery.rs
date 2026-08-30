use std::collections::HashSet;

use axum::Json;
use axum::extract::{Path, Query, State};
use runloom_protocol::{
    MetricKeyListResponse, ProjectId, ProjectListResponse, ProjectMetricCatalogRequest,
    ProjectMetricCatalogResponse, RunId, RunListResponse, RunQueryRequest, RunQueryResponse,
    RunRecord,
};
use serde::Deserialize;

use crate::{
    AppState, HttpError, MAX_CHART_QUERY_RUNS, MAX_METRIC_KEY_BYTES, default_list_limit,
    page_limit, validate_list_limit, validate_metric_catalog_text, validate_project_name,
    validate_run_query, validate_search,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectListQuery {
    before: Option<ProjectId>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunListQuery {
    before: Option<RunId>,
    q: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetricKeyListQuery {
    after: Option<String>,
    #[serde(default = "default_list_limit")]
    limit: usize,
}

pub(super) async fn list_projects(
    State(state): State<AppState>,
    Query(query): Query<ProjectListQuery>,
) -> Result<Json<ProjectListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    let mut projects = state
        .catalog
        .list_projects(query.before, page_limit(query.limit))
        .await?;
    let has_more = projects.len() > query.limit;
    projects.truncate(query.limit);
    let next_before = has_more
        .then(|| projects.last().map(|project| project.id))
        .flatten();
    Ok(Json(ProjectListResponse {
        projects,
        next_before,
    }))
}

pub(super) async fn get_project(
    State(state): State<AppState>,
    Path(project): Path<String>,
) -> Result<Json<runloom_protocol::ProjectSummary>, HttpError> {
    validate_project_name(&project)?;
    Ok(Json(state.catalog.get_project(&project).await?))
}

pub(super) async fn query_project_metrics(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Json(request): Json<ProjectMetricCatalogRequest>,
) -> Result<Json<ProjectMetricCatalogResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_list_limit(request.limit)?;
    if request.run_ids.is_empty() || request.run_ids.len() > MAX_CHART_QUERY_RUNS {
        return Err(HttpError::invalid(format!(
            "metric catalog queries require 1 to {MAX_CHART_QUERY_RUNS} runs"
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
        return Err(HttpError::invalid(
            "metric catalog query run IDs must be unique",
        ));
    }
    validate_metric_catalog_text(request.search.as_deref(), "metric catalog search")?;
    validate_metric_catalog_text(request.after.as_deref(), "metric catalog cursor")?;
    let mut catalog_request = request.clone();
    catalog_request.limit = page_limit(request.limit);
    let mut keys = state
        .catalog
        .project_metric_catalog(&project, &catalog_request)
        .await?;
    let has_more = keys.len() > request.limit;
    keys.truncate(request.limit);
    let next_after = has_more
        .then(|| keys.last().map(|summary| summary.key.clone()))
        .flatten();
    Ok(Json(ProjectMetricCatalogResponse { keys, next_after }))
}

pub(super) async fn list_runs(
    State(state): State<AppState>,
    Path(project): Path<String>,
    Query(query): Query<RunListQuery>,
) -> Result<Json<RunListResponse>, HttpError> {
    validate_project_name(&project)?;
    validate_list_limit(query.limit)?;
    validate_search(query.q.as_deref(), "run search")?;
    let mut runs = state
        .catalog
        .list_runs(
            &project,
            query.before,
            query.q.as_deref(),
            page_limit(query.limit),
        )
        .await?;
    let has_more = runs.len() > query.limit;
    runs.truncate(query.limit);
    let next_before = has_more.then(|| runs.last().map(|run| run.id)).flatten();
    Ok(Json(RunListResponse { runs, next_before }))
}

pub(super) async fn query_runs(
    State(state): State<AppState>,
    Json(request): Json<RunQueryRequest>,
) -> Result<Json<RunQueryResponse>, HttpError> {
    validate_run_query(&request)?;
    let mut catalog_request = request.clone();
    catalog_request.limit = page_limit(request.limit);
    let mut runs = state.catalog.query_runs(&catalog_request).await?;
    let has_more = runs.len() > request.limit;
    runs.truncate(request.limit);
    let next_before = has_more.then(|| runs.last().map(|run| run.id)).flatten();
    Ok(Json(RunQueryResponse { runs, next_before }))
}

pub(super) async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, HttpError> {
    Ok(Json(state.catalog.get_run(run_id).await?))
}

pub(super) async fn metric_keys(
    State(state): State<AppState>,
    Path(run_id): Path<RunId>,
    Query(query): Query<MetricKeyListQuery>,
) -> Result<Json<MetricKeyListResponse>, HttpError> {
    validate_list_limit(query.limit)?;
    if query.after.as_ref().is_some_and(|after| {
        after.is_empty()
            || after.len() > MAX_METRIC_KEY_BYTES
            || after.chars().any(char::is_control)
    }) {
        return Err(HttpError::invalid("invalid metric key cursor"));
    }
    let mut keys = state
        .catalog
        .metric_keys(run_id, query.after.as_deref(), page_limit(query.limit))
        .await?;
    let has_more = keys.len() > query.limit;
    keys.truncate(query.limit);
    let next_after = has_more.then(|| keys.last().cloned()).flatten();
    Ok(Json(MetricKeyListResponse {
        run_id,
        keys,
        next_after,
    }))
}
