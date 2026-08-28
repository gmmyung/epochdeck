#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("failed to create storage directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLayout {
    data_dir: PathBuf,
    metrics_dir: PathBuf,
    blobs_dir: PathBuf,
}

impl StorageLayout {
    #[must_use]
    pub fn from_environment() -> Self {
        let data_dir = environment_path("RUNLOOM_DATA_DIR", "./data");
        let metrics_dir = std::env::var_os("RUNLOOM_METRICS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("metrics"));
        let blobs_dir = std::env::var_os("RUNLOOM_BLOBS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("blobs"));
        Self::new(data_dir, metrics_dir, blobs_dir)
    }

    #[must_use]
    pub fn new(
        data_dir: impl Into<PathBuf>,
        metrics_dir: impl Into<PathBuf>,
        blobs_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            data_dir: data_dir.into(),
            metrics_dir: metrics_dir.into(),
            blobs_dir: blobs_dir.into(),
        }
    }

    pub fn ensure(&self) -> Result<(), StorageError> {
        for path in [
            &self.data_dir,
            &self.metrics_dir,
            &self.blobs_dir,
            &self.journal_dir(),
            &self.blob_staging_dir(),
        ] {
            std::fs::create_dir_all(path).map_err(|source| StorageError::CreateDirectory {
                path: path.to_path_buf(),
                source,
            })?;
        }
        Ok(())
    }

    #[must_use]
    pub fn catalog_path(&self) -> PathBuf {
        self.data_dir.join("catalog.sqlite3")
    }

    #[must_use]
    pub fn journal_dir(&self) -> PathBuf {
        self.data_dir.join("journal")
    }

    #[must_use]
    pub fn metrics_dir(&self) -> &Path {
        &self.metrics_dir
    }

    #[must_use]
    pub fn blobs_dir(&self) -> &Path {
        &self.blobs_dir
    }

    #[must_use]
    pub fn blob_staging_dir(&self) -> PathBuf {
        self.blobs_dir.join("staging")
    }
}

fn environment_path(name: &str, default: &str) -> PathBuf {
    std::env::var_os(name)
        .unwrap_or_else(|| OsString::from(default))
        .into()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::StorageLayout;

    #[test]
    fn creates_independent_storage_roots() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let layout = StorageLayout::new(
            directory.path().join("data"),
            directory.path().join("metrics"),
            directory.path().join("blobs"),
        );
        layout.ensure()?;

        assert!(
            layout
                .catalog_path()
                .parent()
                .is_some_and(|path| path.exists())
        );
        assert!(layout.metrics_dir().exists());
        assert!(layout.blob_staging_dir().exists());
        Ok(())
    }
}
