use std::io;
use std::path::Path;

use runloom_protocol::{StorageRootDiagnostics, StorageRootKind};

pub(crate) fn collect_storage_root_diagnostics(
    catalog_path: &Path,
    metrics_path: &Path,
    blobs_path: &Path,
) -> io::Result<Vec<StorageRootDiagnostics>> {
    let catalog_root = catalog_path.parent().unwrap_or(catalog_path);
    [
        (StorageRootKind::Catalog, catalog_root),
        (StorageRootKind::Metrics, metrics_path),
        (StorageRootKind::Blobs, blobs_path),
    ]
    .into_iter()
    .map(|(kind, path)| storage_root_diagnostics(kind, path))
    .collect()
}

fn storage_root_diagnostics(
    kind: StorageRootKind,
    path: &Path,
) -> io::Result<StorageRootDiagnostics> {
    let canonical_path = path.canonicalize()?;
    let metadata = canonical_path.metadata()?;
    let stats = fs2::statvfs(&canonical_path)?;
    Ok(StorageRootDiagnostics {
        kind,
        path: canonical_path.to_string_lossy().into_owned(),
        device_id: storage_device_id(&metadata),
        total_bytes: stats.total_space(),
        free_bytes: stats.free_space(),
        available_bytes: stats.available_space(),
    })
}

#[cfg(unix)]
fn storage_device_id(metadata: &std::fs::Metadata) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    Some(metadata.dev().to_string())
}

#[cfg(not(unix))]
fn storage_device_id(_metadata: &std::fs::Metadata) -> Option<String> {
    None
}
