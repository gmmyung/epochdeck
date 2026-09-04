use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::StorageError;

static PUBLICATION_PROBE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileInstallation {
    InstalledNew,
    AlreadyPresent,
}

pub(super) fn ensure_publication_capability(
    root: &Path,
    staging_dir: &Path,
    verified: &AtomicBool,
) -> Result<(), StorageError> {
    if verified.load(Ordering::Acquire) {
        return Ok(());
    }
    probe_publication_capability(root, staging_dir)?;
    verified.store(true, Ordering::Release);
    Ok(())
}

pub(super) fn probe_publication_capability(
    root: &Path,
    staging_dir: &Path,
) -> Result<(), StorageError> {
    require_directory(root)?;
    require_directory(staging_dir)?;

    let sequence = PUBLICATION_PROBE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let probe_name = format!(
        ".epochdeck-publication-{}-{sequence}.tmp",
        std::process::id()
    );
    let staging_path = staging_dir.join(&probe_name);
    let published_path = root.join(&probe_name);

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging_path)
        .map_err(|source| StorageError::Io {
            path: staging_path.clone(),
            source,
        })?;
    let mut cleanup = ProbeCleanup::new(staging_path.clone());
    file.write_all(b"epochdeck publication probe")
        .and_then(|()| file.sync_all())
        .map_err(|source| StorageError::Io {
            path: staging_path.clone(),
            source,
        })?;
    drop(file);

    match install_no_replace(&staging_path, &published_path)? {
        FileInstallation::InstalledNew => cleanup.track_published(published_path.clone()),
        FileInstallation::AlreadyPresent => {
            return Err(StorageError::InvalidLayout(format!(
                "storage publication probe collided with {}",
                published_path.display()
            )));
        }
    }
    sync_publication(&published_path, root)?;
    remove_probe_file(&published_path)?;
    remove_probe_file(&staging_path)?;
    cleanup.disarm();
    sync_directory_after_removal(root)?;
    sync_directory_after_removal(staging_dir)
}

pub(super) fn install_no_replace(
    staging_path: &Path,
    final_path: &Path,
) -> Result<FileInstallation, StorageError> {
    match std::fs::hard_link(staging_path, final_path) {
        Ok(()) => Ok(FileInstallation::InstalledNew),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            Ok(FileInstallation::AlreadyPresent)
        }
        Err(source) => Err(StorageError::AtomicPublicationFailed {
            staging_path: staging_path.to_path_buf(),
            final_path: final_path.to_path_buf(),
            source,
        }),
    }
}

pub(super) fn sync_file(path: &Path) -> Result<(), StorageError> {
    open_file_for_sync(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

pub(super) fn sync_publication(file_path: &Path, parent: &Path) -> Result<(), StorageError> {
    sync_file(file_path)?;
    sync_publication_parent(parent)
}

#[cfg(windows)]
fn open_file_for_sync(path: &Path) -> std::io::Result<File> {
    // FlushFileBuffers requires a handle opened with GENERIC_WRITE.
    OpenOptions::new().read(true).write(true).open(path)
}

#[cfg(not(windows))]
fn open_file_for_sync(path: &Path) -> std::io::Result<File> {
    File::open(path)
}

#[cfg(unix)]
fn sync_publication_parent(path: &Path) -> Result<(), StorageError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(windows)]
fn sync_publication_parent(path: &Path) -> Result<(), StorageError> {
    // Windows has no portable directory-fsync operation. FlushFileBuffers on
    // the write-capable published-file handle above is its durability boundary
    // for the file and its metadata; still validate that the caller supplied a
    // directory so a mistaken path never becomes a silent success.
    require_directory(path)
}

#[cfg(not(any(unix, windows)))]
fn sync_publication_parent(path: &Path) -> Result<(), StorageError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StorageError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(unix)]
fn sync_directory_after_removal(path: &Path) -> Result<(), StorageError> {
    sync_publication_parent(path)
}

#[cfg(not(unix))]
fn sync_directory_after_removal(path: &Path) -> Result<(), StorageError> {
    require_directory(path)
}

fn require_directory(path: &Path) -> Result<(), StorageError> {
    let metadata = std::fs::metadata(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.is_dir() {
        return Ok(());
    }
    Err(StorageError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            "storage path is not a directory",
        ),
    })
}

fn remove_probe_file(path: &Path) -> Result<(), StorageError> {
    std::fs::remove_file(path).map_err(|source| StorageError::Io {
        path: path.to_path_buf(),
        source,
    })
}

struct ProbeCleanup {
    staging_path: Option<PathBuf>,
    published_path: Option<PathBuf>,
}

impl ProbeCleanup {
    fn new(staging_path: PathBuf) -> Self {
        Self {
            staging_path: Some(staging_path),
            published_path: None,
        }
    }

    fn track_published(&mut self, path: PathBuf) {
        self.published_path = Some(path);
    }

    fn disarm(&mut self) {
        self.published_path = None;
        self.staging_path = None;
    }
}

impl Drop for ProbeCleanup {
    fn drop(&mut self) {
        if let Some(path) = &self.published_path {
            let _ = std::fs::remove_file(path);
        }
        if let Some(path) = &self.staging_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use tempfile::tempdir;

    use super::{
        ensure_publication_capability, install_no_replace, probe_publication_capability, sync_file,
    };
    use crate::StorageError;

    #[test]
    fn publication_probe_leaves_no_files_behind() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let root = directory.path().join("store");
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging)?;

        probe_publication_capability(&root, &staging)?;

        assert_eq!(std::fs::read_dir(&root)?.count(), 1);
        assert_eq!(std::fs::read_dir(&staging)?.count(), 0);
        Ok(())
    }

    #[test]
    fn publication_capability_can_be_cached() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let root = directory.path().join("store");
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging)?;
        let verified = AtomicBool::new(false);

        ensure_publication_capability(&root, &staging, &verified)?;
        assert!(verified.load(std::sync::atomic::Ordering::Acquire));
        ensure_publication_capability(&root, &staging, &verified)?;
        assert_eq!(std::fs::read_dir(&root)?.count(), 1);
        assert_eq!(std::fs::read_dir(&staging)?.count(), 0);
        Ok(())
    }

    #[test]
    fn sync_file_flushes_a_closed_writer() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let path = directory.path().join("segment");
        std::fs::write(&path, b"durable")?;

        sync_file(&path)?;

        assert_eq!(std::fs::read(path)?, b"durable");
        Ok(())
    }

    #[test]
    fn publication_failure_preserves_the_runtime_io_cause() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let staging_path = directory.path().join("segment.tmp");
        let final_path = directory.path().join("missing").join("segment.parquet");
        std::fs::write(&staging_path, b"metric data")?;

        assert!(matches!(
            install_no_replace(&staging_path, &final_path),
            Err(StorageError::AtomicPublicationFailed {
                staging_path: failed_staging,
                final_path: failed_final,
                source,
            }) if failed_staging == staging_path
                && failed_final == final_path
                && source.kind() == std::io::ErrorKind::NotFound
        ));
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn ntfs_installs_and_syncs_a_published_file() -> Result<(), Box<dyn std::error::Error>> {
        use super::{FileInstallation, sync_publication};

        let directory = tempdir()?;
        let root = directory.path().join("store");
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging)?;
        let staging_path = staging.join("segment.tmp");
        let final_path = root.join("segment.parquet");
        std::fs::write(&staging_path, b"metric data")?;

        assert_eq!(
            install_no_replace(&staging_path, &final_path)?,
            FileInstallation::InstalledNew
        );
        sync_publication(&final_path, &root)?;

        assert_eq!(std::fs::read(final_path)?, b"metric data");
        Ok(())
    }
}
