use std::net::SocketAddr;

use anyhow::{Context, Result};
use runloom_catalog::Catalog;
use runloom_server::{
    CompactionConfig, MetricRuntime, app_with_runtime_and_blobs, run_compaction_worker,
};
use runloom_storage::{BlobStore, MetricStore, StorageLayout};
use tokio::net::TcpListener;
use tokio::sync::watch;
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

    let metrics = MetricRuntime::new(MetricStore::new(layout.metrics_dir()));
    let blobs = BlobStore::new(layout.blobs_dir());
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let compaction_task = tokio::spawn(run_compaction_worker(
        catalog.clone(),
        metrics.clone(),
        shutdown_receiver,
        CompactionConfig::default(),
    ));
    let signal_sender = shutdown_sender.clone();

    info!(%address, "Runloom server listening");
    let serve_result = axum::serve(
        listener,
        app_with_runtime_and_blobs(catalog, metrics, blobs),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = signal_sender.send(true);
    })
    .await
    .context("Runloom server failed");
    let _ = shutdown_sender.send(true);
    compaction_task
        .await
        .context("metric compaction task failed")?;
    serve_result?;
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
