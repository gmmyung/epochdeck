#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use runloom_protocol::{
    AlertId, AlertLevel, AlertRecord, ArtifactEntry, ArtifactId, ArtifactRecord, ArtifactRelation,
    BlobRef, CreateAlertRequest, CreateArtifactRequest, CreateRichValueRequest, CreateRunRequest,
    CreateTraceSpanRequest, MAX_CONFIG_BYTES, MAX_SUMMARY_BYTES, ProjectId, ProjectSummary,
    ResumePolicy, RichValueId, RichValueKind, RichValueRecord, RunArtifactRecord, RunId, RunRecord,
    RunState, TraceKind, TraceSpanId, TraceSpanRecord, TraceStatus,
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

CREATE TABLE IF NOT EXISTS retired_metric_segments (
    relative_path TEXT PRIMARY KEY,
    retired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_metric_keys (
    run_id TEXT NOT NULL REFERENCES runs(id),
    key TEXT NOT NULL,
    PRIMARY KEY(run_id, key)
);

CREATE TABLE IF NOT EXISTS run_alerts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    title TEXT NOT NULL,
    text TEXT NOT NULL,
    level TEXT NOT NULL,
    step INTEGER,
    timestamp_ms INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_alerts_run_id
    ON run_alerts(run_id, id DESC);

CREATE TABLE IF NOT EXISTS run_rich_values (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    key TEXT NOT NULL,
    kind TEXT NOT NULL,
    step INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    blob_json TEXT,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_rich_values_run_id
    ON run_rich_values(run_id, id DESC);

CREATE TABLE IF NOT EXISTS artifact_versions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    description TEXT,
    metadata_json TEXT NOT NULL,
    entries_json TEXT NOT NULL,
    request_json TEXT NOT NULL,
    created_by_run TEXT NOT NULL REFERENCES runs(id),
    created_at TEXT NOT NULL,
    UNIQUE(project_id, name, artifact_type, version)
);

CREATE INDEX IF NOT EXISTS idx_artifact_versions_project_id
    ON artifact_versions(project_id, id DESC);

CREATE TABLE IF NOT EXISTS artifact_aliases (
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    alias TEXT NOT NULL,
    artifact_id TEXT NOT NULL REFERENCES artifact_versions(id),
    PRIMARY KEY(project_id, name, artifact_type, alias)
);

CREATE INDEX IF NOT EXISTS idx_artifact_aliases_artifact_id
    ON artifact_aliases(artifact_id, alias);

CREATE TABLE IF NOT EXISTS artifact_lineage (
    artifact_id TEXT NOT NULL REFERENCES artifact_versions(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    relation TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(artifact_id, run_id, relation)
);

CREATE INDEX IF NOT EXISTS idx_artifact_lineage_run_id
    ON artifact_lineage(run_id, created_at DESC);

CREATE TABLE IF NOT EXISTS trace_spans (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    trace_id TEXT NOT NULL,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER NOT NULL,
    step INTEGER,
    attributes_json TEXT NOT NULL,
    preview_json TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trace_spans_run_id
    ON trace_spans(run_id, id DESC);

CREATE VIRTUAL TABLE IF NOT EXISTS trace_search USING fts5(
    span_id UNINDEXED,
    run_id UNINDEXED,
    search_text,
    tokenize = 'unicode61'
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
    #[error("catalog limit exceeded: {0}")]
    Limit(String),
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
    pub id: String,
    pub signature: String,
    pub relative_path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub row_count: usize,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionCandidate {
    pub project_id: ProjectId,
    pub run_id: RunId,
    pub segments: Vec<SegmentRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricExtent {
    pub first_sequence: u64,
    pub last_sequence: u64,
}

#[derive(Debug)]
struct ArtifactBase {
    id: ArtifactId,
    project_id: ProjectId,
    project: String,
    name: String,
    artifact_type: String,
    version: u64,
    description: Option<String>,
    metadata: BTreeMap<String, Value>,
    entries: Vec<ArtifactEntry>,
    request_json: String,
    created_by_run: RunId,
    created_at: String,
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

    pub async fn create_alert(
        &self,
        run_id: RunId,
        request: &CreateAlertRequest,
    ) -> Result<(AlertRecord, bool), CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        let alert_id = request.id.unwrap_or_default();
        if let Some(existing) = load_alert(&mut transaction, alert_id).await? {
            let matches = existing.run_id == run_id
                && existing.title == request.title
                && existing.text == request.text
                && existing.level == request.level
                && existing.step == request.step
                && existing.timestamp_ms == request.timestamp_ms;
            if !matches {
                return Err(CatalogError::Conflict(
                    "alert ID was reused with different contents".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok((existing, true));
        }
        let step = request
            .step
            .map(|value| to_i64(value, "alert step"))
            .transpose()?;
        query(
            "INSERT INTO run_alerts \
             (id, run_id, title, text, level, step, timestamp_ms, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        )
        .bind(alert_id.to_string())
        .bind(run_id.to_string())
        .bind(&request.title)
        .bind(&request.text)
        .bind(request.level.to_string())
        .bind(step)
        .bind(request.timestamp_ms)
        .execute(&mut *transaction)
        .await?;
        touch_run(&mut transaction, run_id).await?;
        let alert = load_required_alert(&mut transaction, alert_id).await?;
        transaction.commit().await?;
        Ok((alert, false))
    }

    pub async fn list_alerts(
        &self,
        run_id: RunId,
        before: Option<AlertId>,
        limit: usize,
    ) -> Result<Vec<AlertRecord>, CatalogError> {
        self.get_run(run_id).await?;
        let before = before.map(|value| value.to_string());
        let rows = query(
            "SELECT id, run_id, title, text, level, step, timestamp_ms, created_at \
             FROM run_alerts WHERE run_id = ? AND (? IS NULL OR id < ?) \
             ORDER BY id DESC LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(&before)
        .bind(&before)
        .bind(to_i64(limit as u64, "alert limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(alert_from_row).collect()
    }

    pub async fn create_rich_value(
        &self,
        run_id: RunId,
        request: &CreateRichValueRequest,
    ) -> Result<(RichValueRecord, bool), CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        let value_id = request.id.unwrap_or_default();
        if let Some(existing) = load_rich_value(&mut transaction, value_id).await? {
            let matches = existing.run_id == run_id
                && existing.key == request.key
                && existing.kind == request.kind
                && existing.step == request.step
                && existing.timestamp_ms == request.timestamp_ms
                && existing.blob == request.blob
                && existing.metadata == request.metadata;
            if !matches {
                return Err(CatalogError::Conflict(
                    "rich value ID was reused with different contents".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok((existing, true));
        }
        let blob_json = request
            .blob
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let metadata_json = serde_json::to_string(&request.metadata)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        query(
            "INSERT INTO run_rich_values \
             (id, run_id, key, kind, step, timestamp_ms, blob_json, metadata_json, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        )
        .bind(value_id.to_string())
        .bind(run_id.to_string())
        .bind(&request.key)
        .bind(request.kind.to_string())
        .bind(to_i64(request.step, "rich value step")?)
        .bind(request.timestamp_ms)
        .bind(blob_json)
        .bind(metadata_json)
        .execute(&mut *transaction)
        .await?;
        query(
            "UPDATE run_revisions SET rich_data_revision = rich_data_revision + 1 WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        touch_run(&mut transaction, run_id).await?;
        let value = load_required_rich_value(&mut transaction, value_id).await?;
        transaction.commit().await?;
        Ok((value, false))
    }

    pub async fn list_rich_values(
        &self,
        run_id: RunId,
        before: Option<RichValueId>,
        limit: usize,
    ) -> Result<Vec<RichValueRecord>, CatalogError> {
        self.get_run(run_id).await?;
        let before = before.map(|value| value.to_string());
        let rows = query(
            "SELECT id, run_id, key, kind, step, timestamp_ms, blob_json, metadata_json, created_at \
             FROM run_rich_values WHERE run_id = ? AND (? IS NULL OR id < ?) \
             ORDER BY id DESC LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(&before)
        .bind(&before)
        .bind(to_i64(limit as u64, "rich value limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(rich_value_from_row).collect()
    }

    pub async fn rich_value_next_step(&self, run_id: RunId) -> Result<u64, CatalogError> {
        self.get_run(run_id).await?;
        let maximum: Option<i64> =
            query("SELECT MAX(step) AS maximum_step FROM run_rich_values WHERE run_id = ?")
                .bind(run_id.to_string())
                .fetch_one(&self.pool)
                .await?
                .get("maximum_step");
        maximum.map_or(Ok(0), |value| {
            from_i64(value, "rich value step")?
                .checked_add(1)
                .ok_or_else(|| CatalogError::InvalidData("run step overflow".to_owned()))
        })
    }

    pub async fn create_artifact(
        &self,
        run_id: RunId,
        request: &CreateArtifactRequest,
    ) -> Result<(ArtifactRecord, bool), CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        let location = run_location_in(&mut transaction, run_id).await?;
        let request_json = serde_json::to_string(request)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        if let Some(artifact_id) = request.id
            && let Some(existing) = load_artifact_base(&mut transaction, artifact_id).await?
        {
            if existing.request_json != request_json || existing.created_by_run != run_id {
                return Err(CatalogError::Conflict(
                    "artifact ID was reused with different contents".to_owned(),
                ));
            }
            let artifact = finish_artifact(&mut transaction, existing).await?;
            transaction.commit().await?;
            return Ok((artifact, true));
        }
        let existing_type: Option<String> = query(
            "SELECT artifact_type FROM artifact_versions WHERE project_id = ? AND name = ? LIMIT 1",
        )
        .bind(location.project_id.to_string())
        .bind(&request.name)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|row| row.get("artifact_type"));
        if existing_type.is_some_and(|value| value != request.artifact_type) {
            return Err(CatalogError::Conflict(
                "an artifact collection name cannot change type".to_owned(),
            ));
        }
        let previous_version: Option<i64> = query(
            "SELECT MAX(version) AS version FROM artifact_versions \
             WHERE project_id = ? AND name = ? AND artifact_type = ?",
        )
        .bind(location.project_id.to_string())
        .bind(&request.name)
        .bind(&request.artifact_type)
        .fetch_one(&mut *transaction)
        .await?
        .get("version");
        let version = previous_version.map_or(Ok(0), |value| {
            from_i64(value, "artifact version")?
                .checked_add(1)
                .ok_or_else(|| CatalogError::InvalidData("artifact version overflow".to_owned()))
        })?;
        let artifact_id = request.id.unwrap_or_default();
        let metadata_json = serde_json::to_string(&request.metadata)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let entries_json = serde_json::to_string(&request.entries)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        query(
            "INSERT INTO artifact_versions \
             (id, project_id, name, artifact_type, version, description, metadata_json, \
              entries_json, request_json, created_by_run, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        )
        .bind(artifact_id.to_string())
        .bind(location.project_id.to_string())
        .bind(&request.name)
        .bind(&request.artifact_type)
        .bind(to_i64(version, "artifact version")?)
        .bind(&request.description)
        .bind(metadata_json)
        .bind(entries_json)
        .bind(request_json)
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        for alias in &request.aliases {
            query(
                "INSERT INTO artifact_aliases \
                 (project_id, name, artifact_type, alias, artifact_id) VALUES (?, ?, ?, ?, ?) \
                 ON CONFLICT(project_id, name, artifact_type, alias) \
                 DO UPDATE SET artifact_id = excluded.artifact_id",
            )
            .bind(location.project_id.to_string())
            .bind(&request.name)
            .bind(&request.artifact_type)
            .bind(alias)
            .bind(artifact_id.to_string())
            .execute(&mut *transaction)
            .await?;
        }
        insert_artifact_lineage(
            &mut transaction,
            artifact_id,
            run_id,
            ArtifactRelation::Output,
        )
        .await?;
        touch_run(&mut transaction, run_id).await?;
        let artifact = load_required_artifact(&mut transaction, artifact_id).await?;
        transaction.commit().await?;
        Ok((artifact, false))
    }

    pub async fn use_artifact(
        &self,
        run_id: RunId,
        artifact_id: ArtifactId,
    ) -> Result<ArtifactRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        let location = run_location_in(&mut transaction, run_id).await?;
        let artifact = load_required_artifact(&mut transaction, artifact_id).await?;
        if artifact.project_id != location.project_id {
            return Err(CatalogError::Conflict(
                "artifact and run must belong to the same project".to_owned(),
            ));
        }
        insert_artifact_lineage(
            &mut transaction,
            artifact_id,
            run_id,
            ArtifactRelation::Input,
        )
        .await?;
        touch_run(&mut transaction, run_id).await?;
        transaction.commit().await?;
        Ok(artifact)
    }

    pub async fn get_artifact(
        &self,
        artifact_id: ArtifactId,
    ) -> Result<ArtifactRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        let artifact = load_required_artifact(&mut transaction, artifact_id).await?;
        transaction.commit().await?;
        Ok(artifact)
    }

    pub async fn resolve_artifact(
        &self,
        project: &str,
        name: &str,
        alias: &str,
    ) -> Result<ArtifactRecord, CatalogError> {
        let row = query(
            "SELECT a.artifact_id FROM artifact_aliases a \
             JOIN projects p ON p.id = a.project_id \
             WHERE p.name = ? AND a.name = ? AND a.alias = ? LIMIT 1",
        )
        .bind(project)
        .bind(name)
        .bind(alias)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("artifact {project}/{name}:{alias}"),
        })?;
        let artifact_id = parse_id(row.get::<String, _>("artifact_id"), "artifact ID")?;
        self.get_artifact(artifact_id).await
    }

    pub async fn list_project_artifacts(
        &self,
        project: &str,
        before: Option<ArtifactId>,
        limit: usize,
    ) -> Result<Vec<ArtifactRecord>, CatalogError> {
        let before = before.map(|value| value.to_string());
        let rows = query(
            "SELECT v.id, v.project_id, p.name AS project, v.name, v.artifact_type, v.version, \
                    v.description, v.metadata_json, v.entries_json, v.request_json, \
                    v.created_by_run, v.created_at \
             FROM artifact_versions v JOIN projects p ON p.id = v.project_id \
             WHERE p.name = ? AND (? IS NULL OR v.id < ?) ORDER BY v.id DESC LIMIT ?",
        )
        .bind(project)
        .bind(&before)
        .bind(&before)
        .bind(to_i64(limit as u64, "artifact list limit")?)
        .fetch_all(&self.pool)
        .await?;
        let mut transaction = self.pool.begin().await?;
        let mut artifacts = Vec::with_capacity(rows.len());
        for row in rows {
            artifacts.push(finish_artifact(&mut transaction, artifact_base_from_row(row)?).await?);
        }
        transaction.commit().await?;
        Ok(artifacts)
    }

    pub async fn list_run_artifacts(
        &self,
        run_id: RunId,
        limit: usize,
    ) -> Result<Vec<RunArtifactRecord>, CatalogError> {
        self.get_run(run_id).await?;
        let rows = query(
            "SELECT v.id, v.project_id, p.name AS project, v.name, v.artifact_type, v.version, \
                    v.description, v.metadata_json, v.entries_json, v.request_json, \
                    v.created_by_run, v.created_at, l.relation \
             FROM artifact_lineage l \
             JOIN artifact_versions v ON v.id = l.artifact_id \
             JOIN projects p ON p.id = v.project_id \
             WHERE l.run_id = ? ORDER BY l.created_at DESC, v.id DESC LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(to_i64(limit as u64, "run artifact list limit")?)
        .fetch_all(&self.pool)
        .await?;
        let mut transaction = self.pool.begin().await?;
        let mut artifacts = Vec::with_capacity(rows.len());
        for row in rows {
            let relation = ArtifactRelation::from_str(&row.get::<String, _>("relation"))
                .map_err(|error| CatalogError::InvalidData(error.to_owned()))?;
            let artifact = finish_artifact(&mut transaction, artifact_base_from_row(row)?).await?;
            artifacts.push(RunArtifactRecord { artifact, relation });
        }
        transaction.commit().await?;
        Ok(artifacts)
    }

    pub async fn artifact_lineage(
        &self,
        artifact_id: ArtifactId,
        relation: ArtifactRelation,
        limit: usize,
    ) -> Result<Vec<RunId>, CatalogError> {
        self.get_artifact(artifact_id).await?;
        let rows = query(
            "SELECT run_id FROM artifact_lineage WHERE artifact_id = ? AND relation = ? \
             ORDER BY created_at DESC, run_id LIMIT ?",
        )
        .bind(artifact_id.to_string())
        .bind(relation.to_string())
        .bind(to_i64(limit as u64, "artifact lineage limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| parse_id(row.get::<String, _>("run_id"), "run ID"))
            .collect()
    }

    pub async fn create_trace_span(
        &self,
        run_id: RunId,
        request: &CreateTraceSpanRequest,
    ) -> Result<(TraceSpanRecord, bool), CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        let span_id = request.id.unwrap_or_default();
        if let Some(existing) = load_trace_span(&mut transaction, span_id).await? {
            let matches = existing.run_id == run_id
                && existing.trace_id == request.trace_id
                && existing.parent_span_id == request.parent_span_id
                && existing.name == request.name
                && existing.kind == request.kind
                && existing.status == request.status
                && existing.start_time_ms == request.start_time_ms
                && existing.end_time_ms == request.end_time_ms
                && existing.step == request.step
                && existing.attributes == request.attributes
                && existing.preview == request.preview
                && existing.payload == request.payload;
            if !matches {
                return Err(CatalogError::Conflict(
                    "trace span ID was reused with different contents".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok((existing, true));
        }
        let attributes_json = serde_json::to_string(&request.attributes)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let preview_json = serde_json::to_string(&request.preview)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let payload_json = request
            .payload
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let step = request
            .step
            .map(|value| to_i64(value, "trace step"))
            .transpose()?;
        query(
            "INSERT INTO trace_spans \
             (id, run_id, trace_id, parent_span_id, name, kind, status, start_time_ms, \
              end_time_ms, step, attributes_json, preview_json, payload_json, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        )
        .bind(span_id.to_string())
        .bind(run_id.to_string())
        .bind(&request.trace_id)
        .bind(request.parent_span_id.map(|value| value.to_string()))
        .bind(&request.name)
        .bind(request.kind.to_string())
        .bind(request.status.to_string())
        .bind(request.start_time_ms)
        .bind(request.end_time_ms)
        .bind(step)
        .bind(attributes_json)
        .bind(preview_json)
        .bind(payload_json)
        .execute(&mut *transaction)
        .await?;
        query("INSERT INTO trace_search (span_id, run_id, search_text) VALUES (?, ?, ?)")
            .bind(span_id.to_string())
            .bind(run_id.to_string())
            .bind(trace_search_text(request))
            .execute(&mut *transaction)
            .await?;
        query(
            "UPDATE run_revisions SET rich_data_revision = rich_data_revision + 1 WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        touch_run(&mut transaction, run_id).await?;
        let span = load_required_trace_span(&mut transaction, span_id).await?;
        transaction.commit().await?;
        Ok((span, false))
    }

    pub async fn get_trace_span(
        &self,
        span_id: TraceSpanId,
    ) -> Result<TraceSpanRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        let span = load_required_trace_span(&mut transaction, span_id).await?;
        transaction.commit().await?;
        Ok(span)
    }

    pub async fn list_trace_spans(
        &self,
        run_id: RunId,
        before: Option<TraceSpanId>,
        search: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TraceSpanRecord>, CatalogError> {
        self.get_run(run_id).await?;
        let before = before.map(|value| value.to_string());
        let rows = if let Some(search) = search.filter(|value| !value.trim().is_empty()) {
            query(
                "SELECT t.id, t.run_id, t.trace_id, t.parent_span_id, t.name, t.kind, t.status, \
                        t.start_time_ms, t.end_time_ms, t.step, t.attributes_json, t.preview_json, \
                        t.payload_json, t.created_at \
                 FROM trace_search s JOIN trace_spans t ON t.id = s.span_id \
                 WHERE s.run_id = ? AND trace_search MATCH ? AND (? IS NULL OR t.id < ?) \
                 ORDER BY t.id DESC LIMIT ?",
            )
            .bind(run_id.to_string())
            .bind(trace_match_query(search))
            .bind(&before)
            .bind(&before)
            .bind(to_i64(limit as u64, "trace list limit")?)
            .fetch_all(&self.pool)
            .await?
        } else {
            query(
                "SELECT id, run_id, trace_id, parent_span_id, name, kind, status, start_time_ms, \
                        end_time_ms, step, attributes_json, preview_json, payload_json, created_at \
                 FROM trace_spans WHERE run_id = ? AND (? IS NULL OR id < ?) \
                 ORDER BY id DESC LIMIT ?",
            )
            .bind(run_id.to_string())
            .bind(&before)
            .bind(&before)
            .bind(to_i64(limit as u64, "trace list limit")?)
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(trace_span_from_row).collect()
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
        let config_json = serialize_document(&request.config, "config", MAX_CONFIG_BYTES)?;

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

    pub async fn update_config(
        &self,
        run_id: RunId,
        updates: &BTreeMap<String, Value>,
        allow_val_change: bool,
    ) -> Result<RunRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        let mut config = load_document(&mut transaction, run_id, "config_json", "config").await?;
        if !allow_val_change {
            for (key, value) in updates {
                if config.get(key).is_some_and(|existing| existing != value) {
                    return Err(CatalogError::Conflict(format!(
                        "config key '{key}' already exists; pass allow_val_change=true to replace it"
                    )));
                }
            }
        }
        config.extend(updates.clone());
        let encoded = serialize_document(&config, "config", MAX_CONFIG_BYTES)?;
        query("UPDATE run_documents SET config_json = ? WHERE run_id = ?")
            .bind(encoded)
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;
        touch_run(&mut transaction, run_id).await?;
        let run = load_required_run(&mut transaction, run_id).await?;
        transaction.commit().await?;
        Ok(run)
    }

    pub async fn update_summary(
        &self,
        run_id: RunId,
        updates: &BTreeMap<String, Value>,
    ) -> Result<RunRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        ensure_running(&mut transaction, run_id).await?;
        merge_summary_document(&mut transaction, run_id, updates).await?;
        touch_run(&mut transaction, run_id).await?;
        let run = load_required_run(&mut transaction, run_id).await?;
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
        touch_run(&mut transaction, run_id).await?;

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
        let existing = load_required_run(&mut transaction, run_id).await?;
        if existing.state == RunState::Finished {
            if summary_values
                .iter()
                .any(|(key, value)| existing.summary.get(key) != Some(value))
            {
                return Err(CatalogError::Conflict(
                    "a finished run cannot be finished again with a different summary".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok(existing);
        }
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
        let run = load_required_run(&mut transaction, run_id).await?;
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
            "SELECT id, signature, relative_path, first_sequence, last_sequence, row_count, byte_size \
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

    pub async fn last_segment(&self, run_id: RunId) -> Result<Option<SegmentRecord>, CatalogError> {
        self.get_run(run_id).await?;
        query(
            "SELECT id, signature, relative_path, first_sequence, last_sequence, row_count, byte_size \
             FROM metric_segments WHERE run_id = ? \
             ORDER BY last_sequence DESC, id DESC LIMIT 1",
        )
        .bind(run_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .map(segment_from_row)
        .transpose()
    }

    pub async fn next_compaction_candidate(
        &self,
        target_rows: usize,
        max_segments: usize,
    ) -> Result<Option<CompactionCandidate>, CatalogError> {
        if target_rows == 0 || max_segments < 2 {
            return Err(CatalogError::InvalidData(
                "compaction requires a positive row target and at least two input segments"
                    .to_owned(),
            ));
        }
        let target_rows_i64 = to_i64(target_rows as u64, "compaction row target")?;
        let seed = query(
            "WITH ordered AS ( \
                 SELECT s.run_id, r.project_id, s.signature, s.first_sequence, \
                        s.last_sequence, s.row_count, \
                        LEAD(s.signature) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS next_signature, \
                        LEAD(s.first_sequence) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS next_first_sequence, \
                        LEAD(s.row_count) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS next_row_count \
                 FROM metric_segments s \
                 JOIN runs r ON r.id = s.run_id \
                 WHERE s.row_count < ? \
             ) \
             SELECT run_id, project_id, first_sequence \
             FROM ordered \
             WHERE signature = next_signature \
               AND last_sequence + 1 = next_first_sequence \
               AND row_count + next_row_count <= ? \
             ORDER BY first_sequence, run_id LIMIT 1",
        )
        .bind(target_rows_i64)
        .bind(target_rows_i64)
        .fetch_optional(&self.pool)
        .await?;
        let Some(seed) = seed else {
            return Ok(None);
        };
        let run_id: RunId = parse_id(seed.get::<String, _>("run_id"), "run ID")?;
        let project_id: ProjectId = parse_id(seed.get::<String, _>("project_id"), "project ID")?;
        let first_sequence = seed.get::<i64, _>("first_sequence");
        let rows = query(
            "SELECT id, signature, relative_path, first_sequence, last_sequence, row_count, byte_size \
             FROM metric_segments \
             WHERE run_id = ? AND first_sequence >= ? \
             ORDER BY first_sequence, id LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(first_sequence)
        .bind(to_i64(max_segments as u64, "compaction segment limit")?)
        .fetch_all(&self.pool)
        .await?;
        let mut segments = Vec::with_capacity(rows.len());
        let mut total_rows = 0usize;
        for row in rows {
            let segment = segment_from_row(row)?;
            let compatible = segments.last().is_none_or(|previous: &SegmentRecord| {
                previous.signature == segment.signature
                    && previous.last_sequence.checked_add(1) == Some(segment.first_sequence)
            });
            let Some(next_total) = total_rows.checked_add(segment.row_count) else {
                break;
            };
            if !compatible || next_total > target_rows {
                break;
            }
            total_rows = next_total;
            segments.push(segment);
        }
        if segments.len() < 2 {
            return Err(CatalogError::InvalidData(
                "compaction seed did not resolve to adjacent compatible segments".to_owned(),
            ));
        }
        Ok(Some(CompactionCandidate {
            project_id,
            run_id,
            segments,
        }))
    }

    pub async fn replace_compacted_segments(
        &self,
        run_id: RunId,
        sources: &[SegmentRecord],
        replacement: &SegmentManifest,
    ) -> Result<Vec<String>, CatalogError> {
        validate_compaction_replacement(sources, replacement)?;
        let mut transaction = self.pool.begin().await?;
        for source in sources {
            let row = query(
                "SELECT id, signature, relative_path, first_sequence, last_sequence, row_count, byte_size \
                 FROM metric_segments WHERE id = ? AND run_id = ?",
            )
            .bind(&source.id)
            .bind(run_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| {
                CatalogError::Conflict(format!(
                    "compaction source {} is no longer active",
                    source.id
                ))
            })?;
            if segment_from_row(row)? != *source {
                return Err(CatalogError::Conflict(format!(
                    "compaction source {} changed before replacement",
                    source.id
                )));
            }
        }
        for source in sources {
            query(
                "INSERT INTO retired_metric_segments (relative_path, retired_at) \
                 VALUES (?, current_timestamp) ON CONFLICT(relative_path) DO NOTHING",
            )
            .bind(&source.relative_path)
            .execute(&mut *transaction)
            .await?;
            query("DELETE FROM metric_segments WHERE id = ? AND run_id = ?")
                .bind(&source.id)
                .bind(run_id.to_string())
                .execute(&mut *transaction)
                .await?;
        }
        query(
            "INSERT INTO metric_segments \
             (id, run_id, signature, relative_path, first_sequence, last_sequence, row_count, byte_size, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)",
        )
        .bind(&replacement.id)
        .bind(run_id.to_string())
        .bind(&replacement.signature)
        .bind(&replacement.relative_path)
        .bind(to_i64(replacement.first_sequence, "first sequence")?)
        .bind(to_i64(replacement.last_sequence, "last sequence")?)
        .bind(to_i64(replacement.row_count as u64, "row count")?)
        .bind(to_i64(replacement.byte_size, "byte size")?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(sources
            .iter()
            .map(|source| source.relative_path.clone())
            .collect())
    }

    pub async fn retired_segments(&self, limit: usize) -> Result<Vec<String>, CatalogError> {
        let rows = query(
            "SELECT relative_path FROM retired_metric_segments \
             ORDER BY retired_at, relative_path LIMIT ?",
        )
        .bind(to_i64(limit as u64, "retired segment limit")?)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.get("relative_path"))
            .collect())
    }

    pub async fn acknowledge_retired_segments(
        &self,
        relative_paths: &[String],
    ) -> Result<(), CatalogError> {
        let mut transaction = self.pool.begin().await?;
        for relative_path in relative_paths {
            query("DELETE FROM retired_metric_segments WHERE relative_path = ?")
                .bind(relative_path)
                .execute(&mut *transaction)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
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

async fn load_required_run(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<RunRecord, CatalogError> {
    load_run(transaction, run_id)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("run {run_id}"),
        })
}

async fn load_alert(
    transaction: &mut Transaction<'_, Sqlite>,
    alert_id: AlertId,
) -> Result<Option<AlertRecord>, CatalogError> {
    let row = query(
        "SELECT id, run_id, title, text, level, step, timestamp_ms, created_at \
         FROM run_alerts WHERE id = ?",
    )
    .bind(alert_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(alert_from_row).transpose()
}

async fn load_required_alert(
    transaction: &mut Transaction<'_, Sqlite>,
    alert_id: AlertId,
) -> Result<AlertRecord, CatalogError> {
    load_alert(transaction, alert_id)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("alert {alert_id}"),
        })
}

async fn load_rich_value(
    transaction: &mut Transaction<'_, Sqlite>,
    value_id: RichValueId,
) -> Result<Option<RichValueRecord>, CatalogError> {
    let row = query(
        "SELECT id, run_id, key, kind, step, timestamp_ms, blob_json, metadata_json, created_at \
         FROM run_rich_values WHERE id = ?",
    )
    .bind(value_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(rich_value_from_row).transpose()
}

async fn load_required_rich_value(
    transaction: &mut Transaction<'_, Sqlite>,
    value_id: RichValueId,
) -> Result<RichValueRecord, CatalogError> {
    load_rich_value(transaction, value_id)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("rich value {value_id}"),
        })
}

async fn load_artifact_base(
    transaction: &mut Transaction<'_, Sqlite>,
    artifact_id: ArtifactId,
) -> Result<Option<ArtifactBase>, CatalogError> {
    let row = query(
        "SELECT v.id, v.project_id, p.name AS project, v.name, v.artifact_type, v.version, \
                v.description, v.metadata_json, v.entries_json, v.request_json, \
                v.created_by_run, v.created_at \
         FROM artifact_versions v JOIN projects p ON p.id = v.project_id WHERE v.id = ?",
    )
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(artifact_base_from_row).transpose()
}

async fn load_required_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    artifact_id: ArtifactId,
) -> Result<ArtifactRecord, CatalogError> {
    let base = load_artifact_base(transaction, artifact_id)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("artifact {artifact_id}"),
        })?;
    finish_artifact(transaction, base).await
}

async fn finish_artifact(
    transaction: &mut Transaction<'_, Sqlite>,
    base: ArtifactBase,
) -> Result<ArtifactRecord, CatalogError> {
    let aliases =
        query("SELECT alias FROM artifact_aliases WHERE artifact_id = ? ORDER BY alias LIMIT 256")
            .bind(base.id.to_string())
            .fetch_all(&mut **transaction)
            .await?
            .into_iter()
            .map(|row| row.get("alias"))
            .collect();
    Ok(ArtifactRecord {
        id: base.id,
        project_id: base.project_id,
        project: base.project,
        name: base.name,
        artifact_type: base.artifact_type,
        version: base.version,
        description: base.description,
        metadata: base.metadata,
        aliases,
        entries: base.entries,
        created_by_run: base.created_by_run,
        created_at: base.created_at,
    })
}

async fn insert_artifact_lineage(
    transaction: &mut Transaction<'_, Sqlite>,
    artifact_id: ArtifactId,
    run_id: RunId,
    relation: ArtifactRelation,
) -> Result<(), CatalogError> {
    query(
        "INSERT INTO artifact_lineage (artifact_id, run_id, relation, created_at) \
         VALUES (?, ?, ?, current_timestamp) \
         ON CONFLICT(artifact_id, run_id, relation) DO NOTHING",
    )
    .bind(artifact_id.to_string())
    .bind(run_id.to_string())
    .bind(relation.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_trace_span(
    transaction: &mut Transaction<'_, Sqlite>,
    span_id: TraceSpanId,
) -> Result<Option<TraceSpanRecord>, CatalogError> {
    let row = query(
        "SELECT id, run_id, trace_id, parent_span_id, name, kind, status, start_time_ms, \
                end_time_ms, step, attributes_json, preview_json, payload_json, created_at \
         FROM trace_spans WHERE id = ?",
    )
    .bind(span_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(trace_span_from_row).transpose()
}

async fn load_required_trace_span(
    transaction: &mut Transaction<'_, Sqlite>,
    span_id: TraceSpanId,
) -> Result<TraceSpanRecord, CatalogError> {
    load_trace_span(transaction, span_id)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("trace span {span_id}"),
        })
}

async fn load_document(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
    column: &str,
    name: &str,
) -> Result<BTreeMap<String, Value>, CatalogError> {
    let row = query("SELECT config_json, summary_json FROM run_documents WHERE run_id = ?")
        .bind(run_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("run document for {run_id}"),
        })?;
    parse_document(row.get::<String, _>(column), name)
}

async fn ensure_running(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<(), CatalogError> {
    if run_location_in(transaction, run_id).await?.state != RunState::Running {
        return Err(CatalogError::Conflict(
            "finished run documents cannot be changed".to_owned(),
        ));
    }
    Ok(())
}

async fn touch_run(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<(), CatalogError> {
    query("UPDATE runs SET updated_at = current_timestamp WHERE id = ?")
        .bind(run_id.to_string())
        .execute(&mut **transaction)
        .await?;
    Ok(())
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

fn alert_from_row(row: SqliteRow) -> Result<AlertRecord, CatalogError> {
    let step = row
        .get::<Option<i64>, _>("step")
        .map(|value| from_i64(value, "alert step"))
        .transpose()?;
    Ok(AlertRecord {
        id: parse_id(row.get::<String, _>("id"), "alert ID")?,
        run_id: parse_id(row.get::<String, _>("run_id"), "run ID")?,
        title: row.get("title"),
        text: row.get("text"),
        level: AlertLevel::from_str(&row.get::<String, _>("level"))
            .map_err(|error| CatalogError::InvalidData(error.to_owned()))?,
        step,
        timestamp_ms: row.get("timestamp_ms"),
        created_at: row.get("created_at"),
    })
}

fn rich_value_from_row(row: SqliteRow) -> Result<RichValueRecord, CatalogError> {
    let blob = row
        .get::<Option<String>, _>("blob_json")
        .map(|value| {
            serde_json::from_str::<BlobRef>(&value)
                .map_err(|error| CatalogError::InvalidData(error.to_string()))
        })
        .transpose()?;
    Ok(RichValueRecord {
        id: parse_id(row.get::<String, _>("id"), "rich value ID")?,
        run_id: parse_id(row.get::<String, _>("run_id"), "run ID")?,
        key: row.get("key"),
        kind: RichValueKind::from_str(&row.get::<String, _>("kind"))
            .map_err(|error| CatalogError::InvalidData(error.to_owned()))?,
        step: from_i64(row.get("step"), "rich value step")?,
        timestamp_ms: row.get("timestamp_ms"),
        blob,
        metadata: parse_document(row.get::<String, _>("metadata_json"), "rich metadata")?,
        created_at: row.get("created_at"),
    })
}

fn artifact_base_from_row(row: SqliteRow) -> Result<ArtifactBase, CatalogError> {
    Ok(ArtifactBase {
        id: parse_id(row.get::<String, _>("id"), "artifact ID")?,
        project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
        project: row.get("project"),
        name: row.get("name"),
        artifact_type: row.get("artifact_type"),
        version: from_i64(row.get("version"), "artifact version")?,
        description: row.get("description"),
        metadata: parse_document(row.get::<String, _>("metadata_json"), "artifact metadata")?,
        entries: serde_json::from_str(&row.get::<String, _>("entries_json"))
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?,
        request_json: row.get("request_json"),
        created_by_run: parse_id(row.get::<String, _>("created_by_run"), "run ID")?,
        created_at: row.get("created_at"),
    })
}

fn trace_span_from_row(row: SqliteRow) -> Result<TraceSpanRecord, CatalogError> {
    let parent_span_id = row
        .get::<Option<String>, _>("parent_span_id")
        .map(|value| parse_id(value, "parent span ID"))
        .transpose()?;
    let step = row
        .get::<Option<i64>, _>("step")
        .map(|value| from_i64(value, "trace step"))
        .transpose()?;
    let payload = row
        .get::<Option<String>, _>("payload_json")
        .map(|value| {
            serde_json::from_str::<BlobRef>(&value)
                .map_err(|error| CatalogError::InvalidData(error.to_string()))
        })
        .transpose()?;
    Ok(TraceSpanRecord {
        id: parse_id(row.get::<String, _>("id"), "trace span ID")?,
        run_id: parse_id(row.get::<String, _>("run_id"), "run ID")?,
        trace_id: row.get("trace_id"),
        parent_span_id,
        name: row.get("name"),
        kind: TraceKind::from_str(&row.get::<String, _>("kind"))
            .map_err(|error| CatalogError::InvalidData(error.to_owned()))?,
        status: TraceStatus::from_str(&row.get::<String, _>("status"))
            .map_err(|error| CatalogError::InvalidData(error.to_owned()))?,
        start_time_ms: row.get("start_time_ms"),
        end_time_ms: row.get("end_time_ms"),
        step,
        attributes: parse_document(row.get::<String, _>("attributes_json"), "trace attributes")?,
        preview: parse_document(row.get::<String, _>("preview_json"), "trace preview")?,
        payload,
        created_at: row.get("created_at"),
    })
}

fn trace_search_text(request: &CreateTraceSpanRequest) -> String {
    const MAX_SEARCH_TEXT_BYTES: usize = 16 * 1024;
    let mut value = format!(
        "{} {} {} {} {} {}",
        request.trace_id,
        request.name,
        request.kind,
        request.status,
        serde_json::to_string(&request.attributes).unwrap_or_default(),
        serde_json::to_string(&request.preview).unwrap_or_default(),
    );
    if value.len() > MAX_SEARCH_TEXT_BYTES {
        let mut boundary = MAX_SEARCH_TEXT_BYTES;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        value.truncate(boundary);
    }
    value
}

fn trace_match_query(value: &str) -> String {
    value
        .split_whitespace()
        .take(16)
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn segment_from_row(row: SqliteRow) -> Result<SegmentRecord, CatalogError> {
    Ok(SegmentRecord {
        id: row.get("id"),
        signature: row.get("signature"),
        relative_path: row.get("relative_path"),
        first_sequence: from_i64(row.get("first_sequence"), "first sequence")?,
        last_sequence: from_i64(row.get("last_sequence"), "last sequence")?,
        row_count: usize::try_from(row.get::<i64, _>("row_count"))
            .map_err(|_| CatalogError::InvalidData("row count is out of range".to_owned()))?,
        byte_size: from_i64(row.get("byte_size"), "byte size")?,
    })
}

fn validate_compaction_replacement(
    sources: &[SegmentRecord],
    replacement: &SegmentManifest,
) -> Result<(), CatalogError> {
    if sources.len() < 2 {
        return Err(CatalogError::InvalidData(
            "compaction requires at least two source segments".to_owned(),
        ));
    }
    let first = &sources[0];
    let last = &sources[sources.len() - 1];
    let mut expected_sequence = first.first_sequence;
    let mut row_count = 0usize;
    for source in sources {
        if source.signature != first.signature || source.first_sequence != expected_sequence {
            return Err(CatalogError::InvalidData(
                "compaction sources must be adjacent and schema-compatible".to_owned(),
            ));
        }
        expected_sequence = source
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| CatalogError::InvalidData("run sequence overflow".to_owned()))?;
        row_count = row_count
            .checked_add(source.row_count)
            .ok_or_else(|| CatalogError::InvalidData("compaction row count overflow".to_owned()))?;
    }
    if replacement.signature != first.signature
        || replacement.first_sequence != first.first_sequence
        || replacement.last_sequence != last.last_sequence
        || replacement.row_count != row_count
    {
        return Err(CatalogError::InvalidData(
            "compaction replacement does not cover its source segments exactly".to_owned(),
        ));
    }
    Ok(())
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
    let mut summary = load_document(transaction, run_id, "summary_json", "summary").await?;
    summary.extend(values.clone());
    let summary_json = serialize_document(&summary, "summary", MAX_SUMMARY_BYTES)?;
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

fn serialize_document(
    document: &BTreeMap<String, Value>,
    name: &str,
    max_bytes: usize,
) -> Result<String, CatalogError> {
    let encoded = serde_json::to_string(document)
        .map_err(|error| CatalogError::InvalidData(format!("invalid {name}: {error}")))?;
    if encoded.len() > max_bytes {
        return Err(CatalogError::Limit(format!(
            "serialized {name} exceeds {max_bytes} bytes"
        )));
    }
    Ok(encoded)
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

    use runloom_protocol::{
        AlertId, AlertLevel, CreateAlertRequest, CreateRunRequest, ResumePolicy, RunState,
    };
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

        let updated = catalog
            .update_config(
                created.id,
                &BTreeMap::from([("optimizer".to_owned(), "adam".into())]),
                false,
            )
            .await?;
        assert_eq!(updated.config["optimizer"], "adam");
        let conflict = catalog
            .update_config(
                created.id,
                &BTreeMap::from([("seed".to_owned(), 9.into())]),
                false,
            )
            .await;
        assert!(matches!(conflict, Err(CatalogError::Conflict(_))));
        let updated = catalog
            .update_config(
                created.id,
                &BTreeMap::from([("seed".to_owned(), 9.into())]),
                true,
            )
            .await?;
        assert_eq!(updated.config["seed"], 9);
        let updated = catalog
            .update_summary(
                created.id,
                &BTreeMap::from([
                    ("status".to_owned(), "running".into()),
                    ("tags".to_owned(), serde_json::json!(["fast", null])),
                ]),
            )
            .await?;
        assert_eq!(updated.summary["status"], "running");

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

        let alert_id = AlertId::new();
        let alert_request = CreateAlertRequest {
            id: Some(alert_id),
            title: "training stalled".to_owned(),
            text: "reward has not improved".to_owned(),
            level: AlertLevel::Warn,
            step: Some(9),
            timestamp_ms: 1_000,
        };
        let (alert, duplicate) = catalog.create_alert(created.id, &alert_request).await?;
        assert!(!duplicate);
        assert_eq!(alert.id, alert_id);
        let (replayed, duplicate) = catalog.create_alert(created.id, &alert_request).await?;
        assert!(duplicate);
        assert_eq!(replayed, alert);
        assert_eq!(
            catalog.list_alerts(created.id, None, 10).await?,
            vec![alert]
        );

        let finished = catalog
            .finish_run(
                created.id,
                &BTreeMap::from([("status".to_owned(), "complete".into())]),
            )
            .await?;
        assert_eq!(finished.state, RunState::Finished);
        assert_eq!(finished.summary["status"], "complete");
        assert_eq!(finished.summary["tags"], serde_json::json!(["fast", null]));
        assert!(finished.finished_at.is_some());
        let repeated = catalog
            .finish_run(
                created.id,
                &BTreeMap::from([("status".to_owned(), "complete".into())]),
            )
            .await?;
        assert_eq!(repeated, finished);
        let changed_finish = catalog
            .finish_run(
                created.id,
                &BTreeMap::from([("status".to_owned(), "changed".into())]),
            )
            .await;
        assert!(matches!(changed_finish, Err(CatalogError::Conflict(_))));
        let late_update = catalog
            .update_summary(
                created.id,
                &BTreeMap::from([("status".to_owned(), "late".into())]),
            )
            .await;
        assert!(matches!(late_update, Err(CatalogError::Conflict(_))));
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

    #[tokio::test]
    async fn compaction_replaces_manifests_and_tracks_retirement()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let catalog_path = directory.path().join("catalog.sqlite3");
        let catalog = Catalog::open(&catalog_path).await?;
        let (run, _) = catalog
            .create_or_resume_run(
                "robotics",
                &CreateRunRequest {
                    id: None,
                    name: None,
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                },
            )
            .await?;
        for batch in 0..3u64 {
            catalog
                .register_batch(
                    run.id,
                    batch,
                    &format!("digest-{batch}"),
                    &SegmentManifest {
                        id: format!("segment-{batch}"),
                        signature: "shared-schema".to_owned(),
                        relative_path: format!("segment-{batch}.parquet"),
                        first_sequence: batch * 2 + 1,
                        last_sequence: batch * 2 + 2,
                        row_count: 2,
                        byte_size: 100,
                    },
                    &BTreeMap::new(),
                )
                .await?;
        }
        let revision_before = catalog.get_run(run.id).await?.metric_revision;
        let candidate = catalog
            .next_compaction_candidate(16, 16)
            .await?
            .ok_or("missing compaction candidate")?;
        assert_eq!(candidate.run_id, run.id);
        assert_eq!(candidate.segments.len(), 3);
        let replacement = SegmentManifest {
            id: "compacted".to_owned(),
            signature: "shared-schema".to_owned(),
            relative_path: "compacted.parquet".to_owned(),
            first_sequence: 1,
            last_sequence: 6,
            row_count: 6,
            byte_size: 180,
        };
        let retired = catalog
            .replace_compacted_segments(run.id, &candidate.segments, &replacement)
            .await?;

        assert_eq!(retired.len(), 3);
        let active = catalog.list_segments(run.id, None).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, "compacted");
        assert_eq!(
            catalog.get_run(run.id).await?.metric_revision,
            revision_before
        );
        assert_eq!(catalog.retired_segments(16).await?, retired);
        drop(catalog);
        let catalog = Catalog::open(catalog_path).await?;
        assert_eq!(catalog.retired_segments(16).await?, retired);
        catalog.acknowledge_retired_segments(&retired).await?;
        assert!(catalog.retired_segments(16).await?.is_empty());
        Ok(())
    }
}
