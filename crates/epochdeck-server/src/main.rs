use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use epochdeck_catalog::Catalog;
use epochdeck_server::{
    CompactionConfig, DashboardConfig, MetricRuntime, app_with_runtime_and_blobs,
    run_compaction_worker,
};
use epochdeck_storage::{BlobStore, MetricStore, StorageLayout};
use fs2::FileExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    match startup_action(std::env::args_os().skip(1))? {
        StartupAction::Run => {}
        StartupAction::Help => {
            print_help();
            return Ok(());
        }
        StartupAction::Version => {
            println!("epochdeck-server {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
    }
    initialize_tracing();

    let dashboard = DashboardConfig::from_environment()
        .context("failed to initialize dashboard configuration")?;

    let layout = StorageLayout::from_environment();
    layout
        .ensure()
        .context("failed to initialize storage roots")?;
    let _storage_locks = acquire_storage_locks(&layout)?;
    let blobs = BlobStore::new(layout.blobs_dir());
    blobs
        .cleanup_staging()
        .context("failed to clean interrupted blob uploads")?;
    let metric_store = MetricStore::new(layout.metrics_dir());
    metric_store
        .cleanup_staging()
        .context("failed to clean interrupted metric writes")?;
    let catalog = Catalog::open(layout.catalog_path())
        .await
        .context("failed to initialize catalog")?;

    let bind = std::env::var("EPOCHDECK_BIND").unwrap_or_else(|_| "127.0.0.1:8787".to_owned());
    let address: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid EPOCHDECK_BIND value: {bind}"))?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("failed to bind EpochDeck server to {address}"))?;

    let metrics = MetricRuntime::new(metric_store);
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let compaction_task = tokio::spawn(run_compaction_worker(
        catalog.clone(),
        metrics.clone(),
        shutdown_receiver,
        CompactionConfig::default(),
    ));
    let signal_sender = shutdown_sender.clone();

    info!(%address, "EpochDeck server listening");
    let serve_result = axum::serve(
        listener,
        app_with_runtime_and_blobs(catalog, metrics, blobs, dashboard),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = signal_sender.send(true);
    })
    .await
    .context("EpochDeck server failed");
    let _ = shutdown_sender.send(true);
    compaction_task
        .await
        .context("metric compaction task failed")?;
    serve_result?;
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum StartupAction {
    Run,
    Help,
    Version,
}

fn startup_action(arguments: impl IntoIterator<Item = OsString>) -> Result<StartupAction> {
    let mut arguments = arguments.into_iter();
    let Some(argument) = arguments.next() else {
        return Ok(StartupAction::Run);
    };
    if arguments.next().is_some() {
        bail!("epochdeck-server accepts at most one argument; use --help for usage");
    }
    match argument.to_str() {
        Some("-h" | "--help") => Ok(StartupAction::Help),
        Some("-V" | "--version") => Ok(StartupAction::Version),
        _ => bail!("unsupported epochdeck-server argument {argument:?}; use --help for usage"),
    }
}

fn print_help() {
    println!(
        "EpochDeck experiment tracking server\n\n\
         Usage: epochdeck-server [OPTIONS]\n\n\
         Options:\n  \
           -h, --help     Print help\n  \
           -V, --version  Print version\n\n\
         Server configuration is provided through EPOCHDECK_* environment variables."
    );
}

fn acquire_storage_locks(layout: &StorageLayout) -> Result<Vec<File>> {
    let lock_paths = layout
        .ownership_lock_paths()
        .context("failed to resolve EpochDeck storage ownership locks")?;
    let mut locks = Vec::with_capacity(lock_paths.len());
    for lock_path in lock_paths {
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| {
                format!(
                    "failed to open the EpochDeck storage ownership lock: {}",
                    lock_path.display()
                )
            })?;
        FileExt::try_lock_exclusive(&lock).with_context(|| {
            format!(
                "EpochDeck storage root is already active: {}",
                lock_path.display()
            )
        })?;
        locks.push(lock);
    }
    Ok(locks)
}

fn initialize_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("epochdeck_server=info,tower_http=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::error!(%error, "failed to receive interrupt signal");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "failed to install termination signal handler");
                wait_for_interrupt().await;
            }
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows::ctrl_break;

        match ctrl_break() {
            Ok(mut break_signal) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            tracing::error!(%error, "failed to receive interrupt signal");
                        }
                    }
                    _ = break_signal.recv() => {}
                }
            }
            Err(error) => {
                tracing::error!(%error, "failed to install CTRL-BREAK signal handler");
                wait_for_interrupt().await;
            }
        }
    }
    #[cfg(not(any(unix, windows)))]
    wait_for_interrupt().await;
}

async fn wait_for_interrupt() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to receive interrupt signal");
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use epochdeck_storage::StorageLayout;
    use tempfile::tempdir;

    use super::{StartupAction, acquire_storage_locks, startup_action};

    #[test]
    fn command_line_actions_are_explicit() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(startup_action([])?, StartupAction::Run);
        assert_eq!(
            startup_action([OsString::from("--help")])?,
            StartupAction::Help
        );
        assert_eq!(
            startup_action([OsString::from("-V")])?,
            StartupAction::Version
        );
        assert!(startup_action([OsString::from("--unknown")]).is_err());
        assert!(startup_action([OsString::from("--help"), OsString::from("--version")]).is_err());
        Ok(())
    }

    #[test]
    fn distinct_instances_cannot_share_an_external_mutable_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let shared_metrics = directory.path().join("shared-metrics");
        let first = StorageLayout::new(
            directory.path().join("first-data"),
            &shared_metrics,
            directory.path().join("first-blobs"),
        );
        let second = StorageLayout::new(
            directory.path().join("second-data"),
            &shared_metrics,
            directory.path().join("second-blobs"),
        );
        first.ensure()?;
        second.ensure()?;

        let first_locks = acquire_storage_locks(&first)?;
        assert!(acquire_storage_locks(&second).is_err());
        drop(first_locks);
        assert_eq!(acquire_storage_locks(&second)?.len(), 3);
        Ok(())
    }
}
