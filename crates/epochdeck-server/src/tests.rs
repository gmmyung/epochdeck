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
use epochdeck_catalog::{BatchStatus, Catalog, CatalogError, SegmentManifest};
use epochdeck_protocol::{
    AlertId, AlertLevel, AlertListResponse, ApiError, ArtifactEntry, ArtifactListResponse,
    ArtifactRelation, BlobRef, BlobUploadResponse, ChartAlignment, ChartHistoryQueryRequest,
    ChartHistoryQueryResponse, ChartHistoryResponse, ChartSeriesRequest, ChartViewport,
    ConfigUpdateRequest, CreateAlertRequest, CreateAlertResponse, CreateArtifactRequest,
    CreateArtifactResponse, CreateRichValueRequest, CreateRichValueResponse, CreateRunRequest,
    CreateRunResponse, DashboardConfigResponse, DiagnosticsResponse, FinishRunRequest,
    FinishRunResponse, HealthResponse, HealthStatus, HistoryResponse, IngestBatchRequest,
    IngestBatchResponse, MetricCatalogMode, MetricKeyListResponse, MetricPoint,
    ProjectListResponse, ProjectMetricCatalogRequest, ProjectMetricCatalogResponse, ProjectSummary,
    ResumePolicy, RichValueId, RichValueKeyListResponse, RichValueKind, RichValueListResponse,
    RunArtifactListResponse, RunId, RunListResponse, RunQueryRequest, RunQueryResponse, RunState,
    RunUpdateResponse, SummaryUpdateRequest, UseArtifactRequest,
};
use epochdeck_storage::{BlobStore, MetricStore, StorageError};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tower::ServiceExt;

use super::{
    AppState, BLOB_UPLOAD_WORKERS, ChartAxisExtentCache, CompactionConfig, CompactionError,
    CompactionOutcome, DashboardConfig, MetricRuntime, RequestMetrics, app, build_router,
    compact_once, create_artifact, ingest_batch, mutation_lock_index, process_ingest_batch,
    upload_blob,
};

fn test_app(catalog: Catalog, metrics: MetricStore) -> Router {
    let blob_root = metrics
        .root()
        .parent()
        .unwrap_or_else(|| metrics.root())
        .join("blobs");
    app(
        catalog,
        MetricRuntime::new(metrics),
        BlobStore::new(blob_root),
        DashboardConfig::default(),
    )
}

fn app_with_runtime(catalog: Catalog, metrics: MetricRuntime) -> Router {
    let blob_root = metrics
        .store()
        .root()
        .parent()
        .unwrap_or_else(|| metrics.store().root())
        .join("blobs");
    app(
        catalog,
        metrics,
        BlobStore::new(blob_root),
        DashboardConfig::default(),
    )
}

#[test]
fn json_number_contract_rejects_values_beyond_browser_precision() {
    let unsafe_value = epochdeck_protocol::MAX_JSON_SAFE_INTEGER + 1;
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
    let maximum = epochdeck_protocol::MAX_JSON_SAFE_INTEGER;
    let safe_document = BTreeMap::from([(
        "nested".to_owned(),
        serde_json::json!([-(maximum as i64), maximum]),
    )]);
    assert!(
        super::validate_document_size(
            &safe_document,
            "safe document",
            epochdeck_protocol::MAX_CONFIG_BYTES,
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
            })
            .is_err()
        );
        assert!(
            super::validate_document_updates(
                &document,
                "summary",
                epochdeck_protocol::MAX_SUMMARY_BYTES,
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
    }
}

#[tokio::test]
async fn raw_http_json_documents_preserve_only_safe_integer_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
    let maximum = epochdeck_protocol::MAX_JSON_SAFE_INTEGER;
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
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
    let router = test_app(catalog, MetricStore::new(metrics_root));
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

#[tokio::test]
async fn dashboard_config_serves_bounded_same_origin_svg_branding()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let default_router = test_app(
        Catalog::open(directory.path().join("default-catalog.sqlite3")).await?,
        MetricStore::new(directory.path().join("default-metrics")),
    );
    let default_config: DashboardConfigResponse = response_json(
        default_router
            .clone()
            .oneshot(Request::get("/api/v1/dashboard/config").body(Body::empty())?)
            .await?,
    )
    .await?;
    assert_eq!(default_config.accent_color, "#2766ad");
    assert_eq!(default_config.logo_url, None);
    assert_eq!(default_config.favicon_url, None);
    assert_eq!(
        default_router
            .clone()
            .oneshot(Request::get("/api/v1/dashboard/logo").body(Body::empty())?)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        default_router
            .oneshot(Request::get("/api/v1/dashboard/favicon").body(Body::empty())?)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );

    let logo_bytes = br##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 8 8"><path fill="#fff" d="M0 0h8v8H0z"/></svg>"##;
    let logo_path = directory.path().join("logo.svg");
    std::fs::write(&logo_path, logo_bytes)?;
    let dashboard = DashboardConfig::new("#A1b2C3", Some(&logo_path), None)?;
    let router = app(
        Catalog::open(directory.path().join("custom-catalog.sqlite3")).await?,
        MetricRuntime::new(MetricStore::new(directory.path().join("custom-metrics"))),
        BlobStore::new(directory.path().join("custom-blobs")),
        dashboard,
    );
    let config: DashboardConfigResponse = response_json(
        router
            .clone()
            .oneshot(Request::get("/api/v1/dashboard/config").body(Body::empty())?)
            .await?,
    )
    .await?;
    assert_eq!(config.accent_color, "#A1b2C3");
    assert!(
        config
            .logo_url
            .as_deref()
            .is_some_and(|url| url.starts_with("/api/v1/dashboard/logo?v="))
    );
    assert!(
        config
            .favicon_url
            .as_deref()
            .is_some_and(|url| url.starts_with("/api/v1/dashboard/favicon?v="))
    );

    let logo = router
        .clone()
        .oneshot(Request::get("/api/v1/dashboard/logo").body(Body::empty())?)
        .await?;
    assert_eq!(logo.status(), StatusCode::OK);
    assert_eq!(
        logo.headers().get(header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("image/svg+xml"))
    );
    let csp = logo
        .headers()
        .get(header::CONTENT_SECURITY_POLICY)
        .and_then(|value| value.to_str().ok())
        .expect("SVG logo response must have a CSP");
    assert!(csp.contains("default-src 'none'"));
    assert!(csp.contains("sandbox"));
    assert!(!csp.contains("default-src 'self'"));
    assert_eq!(
        to_bytes(logo.into_body(), 1024 * 1024 + 1).await?,
        logo_bytes.as_slice()
    );
    let favicon = router
        .oneshot(Request::get("/api/v1/dashboard/favicon").body(Body::empty())?)
        .await?;
    assert_eq!(favicon.status(), StatusCode::OK);
    assert_eq!(
        favicon.headers().get(header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("image/svg+xml"))
    );
    assert_eq!(
        to_bytes(favicon.into_body(), 1024 * 1024 + 1).await?,
        logo_bytes.as_slice()
    );
    Ok(())
}

#[cfg(feature = "embedded-dashboard")]
#[tokio::test]
async fn embedded_dashboard_serves_assets_and_spa_routes() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
        assert!(String::from_utf8_lossy(&body).contains("<title>EpochDeck</title>"));
    }
    for path in [
        "/favicon.ico",
        "/favicon.svg",
        "/favicon-32x32.png",
        "/apple-touch-icon.png",
        "/safari-pinned-tab.svg",
    ] {
        let response = router
            .clone()
            .oneshot(Request::get(path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK, "missing asset {path}");
        assert!(response.headers().contains_key(header::CONTENT_TYPE));
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
        DashboardConfig::default(),
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
        Arc::new(RequestMetrics::from_environment()),
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
    let error: epochdeck_protocol::ApiError = response_json(rejected).await?;
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
            .request_metrics
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
async fn request_metrics_keep_bounded_slow_history_diagnostics()
-> Result<(), Box<dyn std::error::Error>> {
    let request_metrics = Arc::new(super::RequestMetrics::new(Duration::ZERO));
    let router = axum::Router::new()
        .route(
            "/api/v1/runs/example/history",
            axum::routing::get(|| async {}),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&request_metrics),
            super::record_request_metrics,
        ));
    let response = router
        .oneshot(Request::get("/api/v1/runs/example/history").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        request_metrics
            .requests_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        request_metrics
            .requests_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        request_metrics
            .slow_requests_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        request_metrics
            .history_queries_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let recent = request_metrics
        .recent_slow_requests
        .lock()
        .map_err(|_| std::io::Error::other("slow request diagnostics lock was poisoned"))?;
    assert_eq!(recent.len(), 1);
    Ok(())
}

#[tokio::test]
async fn public_run_query_filters_documents_and_paginates() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
    assert_eq!(first.total_count, 3);

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
    assert_eq!(second.total_count, 3);

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
    assert_eq!(intersection.total_count, 1);

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
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));

    let stable_id = RunId::new();
    let stable_request = CreateRunRequest {
        id: Some(stable_id),
        name: Some("stable".to_owned()),
        config: BTreeMap::new(),
        resume: ResumePolicy::Allow,
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
                    },
                )?)
                .await?,
        )
        .await?;
        artifact_runs.push(created.run.id);
    }
    let artifact_a = CreateArtifactRequest {
        id: Some(epochdeck_protocol::ArtifactId::new()),
        name: "policy".to_owned(),
        artifact_type: "model".to_owned(),
        version: None,
        description: None,
        metadata: BTreeMap::new(),
        aliases: Vec::new(),
        entries: Vec::new(),
    };
    let artifact_b = CreateArtifactRequest {
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
        id: Some(epochdeck_protocol::ArtifactId::new()),
        name: "checkpoint".to_owned(),
        artifact_type: "model".to_owned(),
        version: None,
        description: None,
        metadata: BTreeMap::new(),
        aliases: Vec::new(),
        entries: Vec::new(),
    };
    let dataset = CreateArtifactRequest {
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
                },
            )
            .await?;
        run_ids.push(run.id);
    }
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
    let path_a = format!("/api/v1/runs/{}/artifacts", run_ids[0]);
    let path_b = format!("/api/v1/runs/{}/artifacts", run_ids[1]);

    let newer = CreateArtifactRequest {
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
    let resolved: epochdeck_protocol::ArtifactRecord = response_json(
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
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
    let resolved: epochdeck_protocol::ArtifactRecord = response_json(
        router
            .clone()
            .oneshot(Request::get(alias_path).body(Body::empty())?)
            .await?,
    )
    .await?;
    assert_eq!(resolved.id, created_automatic.artifact.id);

    let low = CreateArtifactRequest {
        id: Some(epochdeck_protocol::ArtifactId::new()),
        name: "concurrent-policy".to_owned(),
        version: Some(2),
        ..newer.clone()
    };
    let high = CreateArtifactRequest {
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
    let resolved: epochdeck_protocol::ArtifactRecord = response_json(
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
        id: Some(epochdeck_protocol::ArtifactId::new()),
        name: "unsafe-version".to_owned(),
        version: Some(epochdeck_protocol::MAX_JSON_SAFE_INTEGER + 1),
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
        DashboardConfig::default(),
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
        Arc::new(RequestMetrics::from_environment()),
    );
    let (run, _) = catalog
        .create_or_resume_run(
            "artifact-io",
            &CreateRunRequest {
                id: None,
                name: Some("producer".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
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
                id: Some(epochdeck_protocol::ArtifactId::new()),
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
async fn concurrent_blob_puts_yield_one_idempotent_replay() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
    let content = b"concurrent-content-addressed-blob";
    let digest = format!("{:x}", Sha256::digest(content));
    let path = format!("/api/v1/blobs/{digest}");
    let upload_a = router.clone().oneshot(
        Request::put(&path)
            .header("content-length", content.len())
            .header("x-epochdeck-file-name", "policy_%EC%A0%95%EC%B1%85.bin")
            .body(Body::from(content.as_slice()))?,
    );
    let upload_b = router.clone().oneshot(
        Request::put(&path)
            .header("content-length", content.len())
            .header("x-epochdeck-file-name", "policy_%EC%A0%95%EC%B1%85.bin")
            .body(Body::from(content.as_slice()))?,
    );
    let (upload_a, upload_b) = tokio::join!(upload_a, upload_b);
    let upload_a = upload_a?;
    let upload_b = upload_b?;
    assert!(
        (upload_a.status() == StatusCode::CREATED && upload_b.status() == StatusCode::OK)
            || (upload_b.status() == StatusCode::CREATED && upload_a.status() == StatusCode::OK)
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
        DashboardConfig::default(),
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
        Arc::new(RequestMetrics::from_environment()),
    );
    let held = Arc::clone(&state.blob_upload_permits)
        .acquire_many_owned((BLOB_UPLOAD_WORKERS - 1) as u32)
        .await?;
    let mut uploads = Vec::new();
    for digest in ["a".repeat(64), "b".repeat(64)] {
        let state = state.clone();
        uploads.push(tokio::spawn(async move {
            let body =
                Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>());
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
            if staged == 0 && state.blob_upload_permits.available_permits() == BLOB_UPLOAD_WORKERS {
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
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
async fn chart_history_returns_exact_sparse_buckets_and_validates_viewports()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
    let router = build_router(AppState::new(
        catalog,
        MetricRuntime::new(MetricStore::new(directory.path().join("metrics"))),
        BlobStore::new(directory.path().join("blobs")),
        DashboardConfig::default(),
        Arc::clone(&axis_extent_cache),
        Arc::new(RequestMetrics::from_environment()),
    ));
    let create_run = |name: &str| CreateRunRequest {
        id: None,
        name: Some(name.to_owned()),
        config: BTreeMap::new(),
        resume: ResumePolicy::Never,
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
        DashboardConfig::default(),
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
        Arc::new(RequestMetrics::from_environment()),
    );
    let (run, _) = catalog
        .create_or_resume_run(
            "cancelled-ingest",
            &CreateRunRequest {
                id: None,
                name: Some("detached".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
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
                return Ok::<_, epochdeck_catalog::CatalogError>(());
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
        DashboardConfig::default(),
        Arc::new(Mutex::new(ChartAxisExtentCache::default())),
        Arc::new(RequestMetrics::from_environment()),
    );
    let (run, _) = catalog
        .create_or_resume_run(
            "cancelled-queue",
            &CreateRunRequest {
                id: None,
                name: Some("queued".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
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
    let router = test_app(catalog.clone(), MetricStore::new(&metrics_root));
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
                },
            )?)
            .await?,
    )
    .await?;
    let batch = IngestBatchRequest {
        batch_sequence: 1,
        points: (1..=epochdeck_protocol::MAX_BATCH_POINTS as u64)
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
    assert_eq!(history.sequence.len(), epochdeck_protocol::MAX_BATCH_POINTS);
    assert_eq!(history.sequence.last(), Some(&1024));
    Ok(())
}

#[tokio::test]
async fn lifecycle_is_idempotent_and_history_is_columnar() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let router = test_app(catalog, MetricStore::new(directory.path().join("metrics")));
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
                updates: BTreeMap::from([("oversized".to_owned(), "x".repeat(256 * 1024).into())]),
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
    let loaded_value: epochdeck_protocol::RichValueRecord = response_json(
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
        id: Some(epochdeck_protocol::ArtifactId::new()),
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
    next_request.id = Some(epochdeck_protocol::ArtifactId::new());
    next_request.aliases = vec!["latest".to_owned(), "best".to_owned()];
    let version_one: CreateArtifactResponse = response_json(
        router
            .clone()
            .oneshot(json_request("POST", &artifact_path, &next_request)?)
            .await?,
    )
    .await?;
    assert_eq!(version_one.artifact.version, 1);
    let resolved: epochdeck_protocol::ArtifactRecord = response_json(
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

    let used: epochdeck_protocol::ArtifactRecord = response_json(
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
                Request::get("/api/v1/projects/robotics/artifacts?limit=10").body(Body::empty())?,
            )
            .await?,
    )
    .await?;
    assert_eq!(project_artifacts.artifacts.len(), 2);
    let input_lineage: epochdeck_protocol::ArtifactLineageResponse = response_json(
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
    let output_lineage: epochdeck_protocol::ArtifactLineageResponse = response_json(
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
            Request::get(format!("/api/v1/runs/{}/metrics", created.run.id)).body(Body::empty())?,
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
async fn background_compaction_preserves_http_history() -> Result<(), Box<dyn std::error::Error>> {
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
fn artifact_download_filename_is_safe_and_utf8_compatible() -> Result<(), Box<dyn std::error::Error>>
{
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
