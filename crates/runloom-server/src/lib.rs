#![forbid(unsafe_code)]

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use runloom_catalog::Catalog;
use runloom_protocol::HealthResponse;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;

#[derive(Debug, Clone)]
struct AppState {
    catalog: Catalog,
}

pub fn app(catalog: Catalog) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .with_state(AppState { catalog })
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
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

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use runloom_catalog::Catalog;
    use runloom_protocol::{HealthResponse, HealthStatus};
    use tempfile::tempdir;
    use tower::ServiceExt;

    use super::app;

    #[tokio::test]
    async fn health_checks_the_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let response = app(catalog)
            .oneshot(Request::get("/api/v1/health").body(Body::empty())?)
            .await?;

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await?;
        let health: HealthResponse = serde_json::from_slice(&body)?;
        assert_eq!(health.status, HealthStatus::Healthy);
        Ok(())
    }
}
