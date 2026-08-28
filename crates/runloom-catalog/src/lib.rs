#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{SqlitePool, query};
use thiserror::Error;

const CATALOG_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_project_created
    ON runs(project_id, created_at, id);

CREATE TABLE IF NOT EXISTS run_revisions (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    metric_revision INTEGER NOT NULL DEFAULT 0,
    rich_data_revision INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ingest_batches (
    run_id TEXT NOT NULL REFERENCES runs(id),
    batch_sequence INTEGER NOT NULL,
    digest TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    PRIMARY KEY(run_id, batch_sequence)
);

CREATE TABLE IF NOT EXISTS metric_segments (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    signature TEXT NOT NULL,
    relative_path TEXT NOT NULL UNIQUE,
    first_sequence INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    byte_size INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_segments_run_sequence
    ON metric_segments(run_id, first_sequence, last_sequence);
"#;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("failed to create catalog directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("catalog database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct Catalog {
    pool: SqlitePool,
}

impl Catalog {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| CatalogError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        let catalog = Self { pool };
        catalog.initialize().await?;
        Ok(catalog)
    }

    async fn initialize(&self) -> Result<(), CatalogError> {
        for statement in CATALOG_SCHEMA
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
        {
            query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn health_check(&self) -> Result<(), CatalogError> {
        query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::Catalog;

    #[tokio::test]
    async fn opens_and_initializes_a_catalog() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        catalog.health_check().await?;
        Ok(())
    }
}
