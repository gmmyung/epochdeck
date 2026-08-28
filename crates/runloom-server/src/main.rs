use std::net::SocketAddr;

use anyhow::{Context, Result};
use runloom_catalog::Catalog;
use runloom_server::app;
use runloom_storage::{MetricStore, StorageLayout};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    initialize_tracing();

    let layout = StorageLayout::from_environment();
    layout
        .ensure()
        .context("failed to initialize storage roots")?;
    let catalog = Catalog::open(layout.catalog_path())
        .await
        .context("failed to initialize catalog")?;

    let bind = std::env::var("RUNLOOM_BIND").unwrap_or_else(|_| "127.0.0.1:8787".to_owned());
    let address: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid RUNLOOM_BIND value: {bind}"))?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind Runloom server to {address}"))?;

    info!(%address, "Runloom server listening");
    let metric_store = MetricStore::new(layout.metrics_dir());
    axum::serve(listener, app(catalog, metric_store))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Runloom server failed")?;
    Ok(())
}

fn initialize_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("runloom=info,tower_http=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install shutdown signal handler");
    }
}
