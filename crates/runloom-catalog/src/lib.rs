#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use runloom_protocol::{
    CreateRunRequest, ProjectId, ProjectSummary, ResumePolicy, RunId, RunRecord, RunState,
};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, Sqlite, SqlitePool, Transaction, query};
use thiserror::Error;

pub const MAX_SEGMENTS_PER_QUERY: usize = 256;

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

CREATE TABLE IF NOT EXISTS run_documents (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    config_json TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    finished_at TEXT
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

CREATE TABLE IF NOT EXISTS run_metric_keys (
    run_id TEXT NOT NULL REFERENCES runs(id),
    key TEXT NOT NULL,
    PRIMARY KEY(run_id, key)
);
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
    #[error("{resource} was not found")]
    NotFound { resource: String },
    #[error("catalog conflict: {0}")]
    Conflict(String),
    #[error("invalid catalog data: {0}")]
    InvalidData(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLocation {
    pub project_id: ProjectId,
    pub state: RunState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifest {
    pub id: String,
    pub signature: String,
    pub relative_path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub row_count: usize,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub relative_path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub row_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricExtent {
    pub first_sequence: u64,
    pub last_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchStatus {
    Missing,
    Duplicate { metric_revision: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchRegistration {
    Accepted { metric_revision: u64 },
    Duplicate { metric_revision: u64 },
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

    pub async fn list_projects(&self, limit: usize) -> Result<Vec<ProjectSummary>, CatalogError> {
        let rows = query(
            "SELECT p.id, p.name, p.created_at, COUNT(r.id) AS run_count \
             FROM projects p LEFT JOIN runs r ON r.project_id = p.id \
             GROUP BY p.id, p.name, p.created_at \
             ORDER BY p.created_at DESC, p.id LIMIT ?",
        )
        .bind(to_i64(limit as u64, "project limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProjectSummary {
                    id: parse_id(row.get::<String, _>("id"), "project ID")?,
                    name: row.get("name"),
                    created_at: row.get("created_at"),
                    run_count: from_i64(row.get("run_count"), "run count")?,
                })
            })
            .collect()
    }

    pub async fn list_runs(
        &self,
        project_name: &str,
        limit: usize,
    ) -> Result<Vec<RunRecord>, CatalogError> {
        let project_exists: bool = query("SELECT EXISTS(SELECT 1 FROM projects WHERE name = ?)")
            .bind(project_name)
            .fetch_one(&self.pool)
            .await?
            .get(0);
        if !project_exists {
            return Err(CatalogError::NotFound {
                resource: format!("project {project_name}"),
            });
        }
        let rows = query(
            "SELECT r.id, r.project_id, p.name AS project, r.name, r.state, \
                    r.created_at, r.updated_at, d.config_json, d.summary_json, d.finished_at, \
                    v.metric_revision \
             FROM runs r \
             JOIN projects p ON p.id = r.project_id \
             JOIN run_documents d ON d.run_id = r.id \
             JOIN run_revisions v ON v.run_id = r.id \
             WHERE p.name = ? ORDER BY r.created_at DESC, r.id LIMIT ?",
        )
        .bind(project_name)
        .bind(to_i64(limit as u64, "run limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(run_from_row).collect()
    }

    pub async fn metric_keys(&self, run_id: RunId) -> Result<Vec<String>, CatalogError> {
        self.get_run(run_id).await?;
        let rows =
            query("SELECT key FROM run_metric_keys WHERE run_id = ? ORDER BY key LIMIT 2048")
                .bind(run_id.to_string())
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|row| row.get("key")).collect())
    }

    pub async fn create_or_resume_run(
        &self,
        project_name: &str,
        request: &CreateRunRequest,
    ) -> Result<(RunRecord, bool), CatalogError> {
        let mut transaction = self.pool.begin().await?;
        let project_id = ensure_project(&mut transaction, project_name).await?;
        let run_id = request.id.unwrap_or_default();

        if let Some(existing) = load_run(&mut transaction, run_id).await? {
            if existing.project_id != project_id {
                return Err(CatalogError::Conflict(
                    "run ID already belongs to another project".to_owned(),
                ));
            }
            if request.resume == ResumePolicy::Never {
                return Err(CatalogError::Conflict(
                    "run already exists and resume policy is 'never'".to_owned(),
                ));
            }
            if existing.state == RunState::Finished {
                return Err(CatalogError::Conflict(
                    "finished runs cannot be resumed".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok((existing, true));
        }

        if request.resume == ResumePolicy::Must {
            return Err(CatalogError::NotFound {
                resource: format!("run {run_id} required by resume='must'"),
            });
        }

        let run_name = request.name.clone().unwrap_or_else(|| {
            let short_id: String = run_id.to_string().chars().take(8).collect();
            format!("run-{short_id}")
        });
        let config_json = serde_json::to_string(&request.config)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;

        query(
            "INSERT INTO runs (id, project_id, name, state, created_at, updated_at) \
             VALUES (?, ?, ?, 'running', current_timestamp, current_timestamp)",
        )
        .bind(run_id.to_string())
        .bind(project_id.to_string())
        .bind(run_name)
        .execute(&mut *transaction)
        .await?;
        query(
            "INSERT INTO run_revisions (run_id, metric_revision, rich_data_revision) \
             VALUES (?, 0, 0)",
        )
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        query("INSERT INTO run_documents (run_id, config_json, summary_json) VALUES (?, ?, '{}')")
            .bind(run_id.to_string())
            .bind(config_json)
            .execute(&mut *transaction)
            .await?;

        let run =
            load_run(&mut transaction, run_id)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!("newly created run {run_id}"),
                })?;
        transaction.commit().await?;
        Ok((run, false))
    }

    pub async fn get_run(&self, run_id: RunId) -> Result<RunRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        let run =
            load_run(&mut transaction, run_id)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!("run {run_id}"),
                })?;
        transaction.commit().await?;
        Ok(run)
    }

    pub async fn run_location(&self, run_id: RunId) -> Result<RunLocation, CatalogError> {
        let row = query("SELECT project_id, state FROM runs WHERE id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                resource: format!("run {run_id}"),
            })?;
        Ok(RunLocation {
            project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
            state: parse_state(row.get::<String, _>("state"))?,
        })
    }

    pub async fn batch_status(
        &self,
        run_id: RunId,
        batch_sequence: u64,
        digest: &str,
    ) -> Result<BatchStatus, CatalogError> {
        let batch_sequence = to_i64(batch_sequence, "batch sequence")?;
        let row =
            query("SELECT digest FROM ingest_batches WHERE run_id = ? AND batch_sequence = ?")
                .bind(run_id.to_string())
                .bind(batch_sequence)
                .fetch_optional(&self.pool)
                .await?;
        let Some(row) = row else {
            return Ok(BatchStatus::Missing);
        };
        let existing_digest: String = row.get("digest");
        if existing_digest != digest {
            return Err(CatalogError::Conflict(format!(
                "batch sequence {batch_sequence} was already used with different contents"
            )));
        }
        Ok(BatchStatus::Duplicate {
            metric_revision: self.metric_revision(run_id).await?,
        })
    }

    pub async fn register_batch(
        &self,
        run_id: RunId,
        batch_sequence: u64,
        digest: &str,
        segment: &SegmentManifest,
        summary_values: &BTreeMap<String, f64>,
    ) -> Result<BatchRegistration, CatalogError> {
        let batch_sequence = to_i64(batch_sequence, "batch sequence")?;
        let mut transaction = self.pool.begin().await?;

        if let Some(row) =
            query("SELECT digest FROM ingest_batches WHERE run_id = ? AND batch_sequence = ?")
                .bind(run_id.to_string())
                .bind(batch_sequence)
                .fetch_optional(&mut *transaction)
                .await?
        {
            let existing_digest: String = row.get("digest");
            if existing_digest != digest {
                return Err(CatalogError::Conflict(format!(
                    "batch sequence {batch_sequence} was already used with different contents"
                )));
            }
            let revision = metric_revision_in(&mut transaction, run_id).await?;
            transaction.commit().await?;
            return Ok(BatchRegistration::Duplicate {
                metric_revision: revision,
            });
        }

        let location = run_location_in(&mut transaction, run_id).await?;
        if location.state != RunState::Running {
            return Err(CatalogError::Conflict(
                "metrics cannot be appended to a finished run".to_owned(),
            ));
        }

        let previous_last: Option<i64> = query(
            "SELECT MAX(last_sequence) AS last_sequence FROM metric_segments WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .fetch_one(&mut *transaction)
        .await?
        .get("last_sequence");
        if let Some(previous_last) = previous_last {
            let expected = from_i64(previous_last, "last sequence")?
                .checked_add(1)
                .ok_or_else(|| CatalogError::InvalidData("run sequence overflow".to_owned()))?;
            if segment.first_sequence != expected {
                return Err(CatalogError::Conflict(format!(
                    "metric segment starts at sequence {}, expected {expected}",
                    segment.first_sequence
                )));
            }
        }

        query(
            "INSERT INTO metric_segments \
             (id, run_id, signature, relative_path, first_sequence, last_sequence, row_count, byte_size, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        )
        .bind(&segment.id)
        .bind(run_id.to_string())
        .bind(&segment.signature)
        .bind(&segment.relative_path)
        .bind(to_i64(segment.first_sequence, "first sequence")?)
        .bind(to_i64(segment.last_sequence, "last sequence")?)
        .bind(to_i64(segment.row_count as u64, "row count")?)
        .bind(to_i64(segment.byte_size, "byte size")?)
        .execute(&mut *transaction)
        .await?;
        query(
            "INSERT INTO ingest_batches (run_id, batch_sequence, digest, accepted_at) \
             VALUES (?, ?, ?, current_timestamp)",
        )
        .bind(run_id.to_string())
        .bind(batch_sequence)
        .bind(digest)
        .execute(&mut *transaction)
        .await?;

        merge_summary(&mut transaction, run_id, summary_values).await?;
        for key in summary_values.keys() {
            query(
                "INSERT INTO run_metric_keys (run_id, key) VALUES (?, ?) \
                 ON CONFLICT(run_id, key) DO NOTHING",
            )
            .bind(run_id.to_string())
            .bind(key)
            .execute(&mut *transaction)
            .await?;
        }
        query("UPDATE run_revisions SET metric_revision = metric_revision + 1 WHERE run_id = ?")
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;
        query("UPDATE runs SET updated_at = current_timestamp WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;

        let revision = metric_revision_in(&mut transaction, run_id).await?;
        transaction.commit().await?;
        Ok(BatchRegistration::Accepted {
            metric_revision: revision,
        })
    }

    pub async fn finish_run(
        &self,
        run_id: RunId,
        summary_values: &BTreeMap<String, Value>,
    ) -> Result<RunRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        load_run(&mut transaction, run_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                resource: format!("run {run_id}"),
            })?;
        merge_summary_document(&mut transaction, run_id, summary_values).await?;
        query("UPDATE runs SET state = 'finished', updated_at = current_timestamp WHERE id = ?")
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;
        query(
            "UPDATE run_documents SET finished_at = COALESCE(finished_at, current_timestamp) \
             WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        let run =
            load_run(&mut transaction, run_id)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!("run {run_id}"),
                })?;
        transaction.commit().await?;
        Ok(run)
    }

    pub async fn list_segments(
        &self,
        run_id: RunId,
        after_sequence: Option<u64>,
    ) -> Result<Vec<SegmentRecord>, CatalogError> {
        let after_sequence = after_sequence
            .map(|value| to_i64(value, "history cursor"))
            .transpose()?
            .unwrap_or(-1);
        let rows = query(
            "SELECT relative_path, first_sequence, last_sequence, row_count \
             FROM metric_segments \
             WHERE run_id = ? AND last_sequence > ? \
             ORDER BY first_sequence, id LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(after_sequence)
        .bind(to_i64(
            MAX_SEGMENTS_PER_QUERY as u64,
            "segment query limit",
        )?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(segment_from_row).collect()
    }

    pub async fn metric_extent(
        &self,
        run_id: RunId,
        after_sequence: Option<u64>,
    ) -> Result<Option<MetricExtent>, CatalogError> {
        let after_sequence_i64 = after_sequence
            .map(|value| to_i64(value, "history cursor"))
            .transpose()?
            .unwrap_or(-1);
        let row = query(
            "SELECT MIN(first_sequence) AS first_sequence, \
                    MAX(last_sequence) AS last_sequence \
             FROM metric_segments WHERE run_id = ? AND last_sequence > ?",
        )
        .bind(run_id.to_string())
        .bind(after_sequence_i64)
        .fetch_one(&self.pool)
        .await?;
        let Some(stored_first) = row.get::<Option<i64>, _>("first_sequence") else {
            return Ok(None);
        };
        let stored_last = row
            .get::<Option<i64>, _>("last_sequence")
            .ok_or_else(|| CatalogError::InvalidData("metric extent has no end".to_owned()))?;
        let stored_first = from_i64(stored_first, "first sequence")?;
        let last_sequence = from_i64(stored_last, "last sequence")?;
        let first_sequence = match after_sequence {
            Some(cursor) => cursor
                .checked_add(1)
                .ok_or_else(|| CatalogError::InvalidData("history cursor overflow".to_owned()))?
                .max(stored_first),
            None => stored_first,
        };
        Ok((first_sequence <= last_sequence).then_some(MetricExtent {
            first_sequence,
            last_sequence,
        }))
    }

    async fn metric_revision(&self, run_id: RunId) -> Result<u64, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        let revision = metric_revision_in(&mut transaction, run_id).await?;
        transaction.commit().await?;
        Ok(revision)
    }
}

async fn ensure_project(
    transaction: &mut Transaction<'_, Sqlite>,
    project_name: &str,
) -> Result<ProjectId, CatalogError> {
    let project_id = ProjectId::new();
    query(
        "INSERT INTO projects (id, name, created_at) VALUES (?, ?, current_timestamp) \
         ON CONFLICT(name) DO NOTHING",
    )
    .bind(project_id.to_string())
    .bind(project_name)
    .execute(&mut **transaction)
    .await?;
    let row = query("SELECT id FROM projects WHERE name = ?")
        .bind(project_name)
        .fetch_one(&mut **transaction)
        .await?;
    parse_id(row.get::<String, _>("id"), "project ID")
}

async fn load_run(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<Option<RunRecord>, CatalogError> {
    let row = query(
        "SELECT r.id, r.project_id, p.name AS project, r.name, r.state, \
                r.created_at, r.updated_at, d.config_json, d.summary_json, d.finished_at, \
                v.metric_revision \
         FROM runs r \
         JOIN projects p ON p.id = r.project_id \
         JOIN run_documents d ON d.run_id = r.id \
         JOIN run_revisions v ON v.run_id = r.id \
         WHERE r.id = ?",
    )
    .bind(run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(run_from_row).transpose()
}

fn run_from_row(row: SqliteRow) -> Result<RunRecord, CatalogError> {
    Ok(RunRecord {
        id: parse_id(row.get::<String, _>("id"), "run ID")?,
        project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
        project: row.get("project"),
        name: row.get("name"),
        state: parse_state(row.get::<String, _>("state"))?,
        config: parse_document(row.get::<String, _>("config_json"), "config")?,
        summary: parse_document(row.get::<String, _>("summary_json"), "summary")?,
        metric_revision: from_i64(row.get("metric_revision"), "metric revision")?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        finished_at: row.get("finished_at"),
    })
}

fn segment_from_row(row: SqliteRow) -> Result<SegmentRecord, CatalogError> {
    Ok(SegmentRecord {
        relative_path: row.get("relative_path"),
        first_sequence: from_i64(row.get("first_sequence"), "first sequence")?,
        last_sequence: from_i64(row.get("last_sequence"), "last sequence")?,
        row_count: usize::try_from(row.get::<i64, _>("row_count"))
            .map_err(|_| CatalogError::InvalidData("row count is out of range".to_owned()))?,
    })
}

async fn run_location_in(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<RunLocation, CatalogError> {
    let row = query("SELECT project_id, state FROM runs WHERE id = ?")
        .bind(run_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("run {run_id}"),
        })?;
    Ok(RunLocation {
        project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
        state: parse_state(row.get::<String, _>("state"))?,
    })
}

async fn metric_revision_in(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<u64, CatalogError> {
    let row = query("SELECT metric_revision FROM run_revisions WHERE run_id = ?")
        .bind(run_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("run revision for {run_id}"),
        })?;
    from_i64(row.get("metric_revision"), "metric revision")
}

async fn merge_summary(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
    values: &BTreeMap<String, f64>,
) -> Result<(), CatalogError> {
    let values = values
        .iter()
        .map(|(key, value)| (key.clone(), Value::from(*value)))
        .collect();
    merge_summary_document(transaction, run_id, &values).await
}

async fn merge_summary_document(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
    values: &BTreeMap<String, Value>,
) -> Result<(), CatalogError> {
    let row = query("SELECT summary_json FROM run_documents WHERE run_id = ?")
        .bind(run_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("run document for {run_id}"),
        })?;
    let mut summary = parse_document(row.get::<String, _>("summary_json"), "summary")?;
    summary.extend(values.clone());
    let summary_json = serde_json::to_string(&summary)
        .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
    query("UPDATE run_documents SET summary_json = ? WHERE run_id = ?")
        .bind(summary_json)
        .bind(run_id.to_string())
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn parse_document(document: String, name: &str) -> Result<BTreeMap<String, Value>, CatalogError> {
    serde_json::from_str(&document)
        .map_err(|error| CatalogError::InvalidData(format!("invalid {name} JSON: {error}")))
}

fn parse_id<T>(value: String, name: &str) -> Result<T, CatalogError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| CatalogError::InvalidData(format!("invalid {name}: {error}")))
}

fn parse_state(value: String) -> Result<RunState, CatalogError> {
    value
        .parse()
        .map_err(|error| CatalogError::InvalidData(format!("{error}: {value}")))
}

fn to_i64(value: u64, name: &str) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| CatalogError::InvalidData(format!("{name} is out of range")))
}

fn from_i64(value: i64, name: &str) -> Result<u64, CatalogError> {
    u64::try_from(value).map_err(|_| CatalogError::InvalidData(format!("{name} is negative")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runloom_protocol::{CreateRunRequest, ResumePolicy, RunState};
    use tempfile::tempdir;

    use super::{Catalog, CatalogError, SegmentManifest};

    #[tokio::test]
    async fn creates_resumes_and_finishes_a_run() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let request = CreateRunRequest {
            id: None,
            name: Some("training".to_owned()),
            config: BTreeMap::from([("seed".to_owned(), 7.into())]),
            resume: ResumePolicy::Never,
        };
        let (created, resumed) = catalog.create_or_resume_run("robotics", &request).await?;
        assert!(!resumed);
        assert_eq!(created.project, "robotics");
        assert_eq!(created.config["seed"], 7);

        let resume = CreateRunRequest {
            id: Some(created.id),
            resume: ResumePolicy::Must,
            ..request
        };
        let (_, resumed) = catalog.create_or_resume_run("robotics", &resume).await?;
        assert!(resumed);

        catalog
            .register_batch(
                created.id,
                0,
                "digest",
                &SegmentManifest {
                    id: "segment-1".to_owned(),
                    signature: "loss".to_owned(),
                    relative_path: "segment-1.parquet".to_owned(),
                    first_sequence: 1,
                    last_sequence: 10,
                    row_count: 10,
                    byte_size: 100,
                },
                &BTreeMap::new(),
            )
            .await?;
        let extent = catalog.metric_extent(created.id, Some(2)).await?;
        assert_eq!(
            extent.map(|extent| (extent.first_sequence, extent.last_sequence)),
            Some((3, 10))
        );
        assert_eq!(catalog.metric_extent(created.id, Some(10)).await?, None);

        let finished = catalog.finish_run(created.id, &BTreeMap::new()).await?;
        assert_eq!(finished.state, RunState::Finished);
        assert!(finished.finished_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn resume_must_rejects_a_missing_run() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
        let request = CreateRunRequest {
            id: None,
            name: None,
            config: BTreeMap::new(),
            resume: ResumePolicy::Must,
        };
        let result = catalog.create_or_resume_run("robotics", &request).await;
        assert!(matches!(result, Err(CatalogError::NotFound { .. })));
        Ok(())
    }
}
