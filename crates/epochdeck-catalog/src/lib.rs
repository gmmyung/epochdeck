#![forbid(unsafe_code)]

mod discovery;
mod project_mutations;

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use epochdeck_protocol::{
    AlertId, AlertLevel, AlertRecord, ArtifactEntry, ArtifactId, ArtifactRecord, ArtifactRelation,
    ArtifactSummary, BlobRef, CreateAlertRequest, CreateArtifactRequest, CreateRichValueRequest,
    CreateRunRequest, MAX_CONFIG_BYTES, MAX_DERIVED_SUMMARY_KEYS, MAX_JSON_SAFE_INTEGER,
    MAX_SUMMARY_BYTES, ProjectId, ProjectMetricCatalogRequest, ProjectMetricKeySummary,
    ProjectSummary, ResumePolicy, RichValueId, RichValueKeySummary, RichValueKind, RichValueRecord,
    RichValueSummary, RunArtifactRecord, RunId, RunListItem, RunQueryRequest, RunRecord, RunState,
};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction, query, raw_sql};
use thiserror::Error;

pub const MAX_SEGMENTS_PER_QUERY: usize = 256;
const MIN_COMPACTION_INPUT_SEGMENTS: usize = 4;
const MAX_COMPACTION_SIZE_RATIO: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMetricCatalogPage {
    pub keys: Vec<ProjectMetricKeySummary>,
    pub total_count: usize,
}

const CATALOG_SCHEMA: &str = include_str!("schema.sql");

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("failed to create catalog directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("catalog database error: {0}")]
    Database(sqlx::Error),
    #[error("catalog database is busy: {0}")]
    Busy(String),
    #[error("{resource} was not found")]
    NotFound { resource: String },
    #[error("catalog conflict: {0}")]
    Conflict(String),
    #[error("catalog limit exceeded: {0}")]
    Limit(String),
    #[error("invalid catalog data: {0}")]
    InvalidData(String),
}

impl From<sqlx::Error> for CatalogError {
    fn from(error: sqlx::Error) -> Self {
        if sqlite_error_has_primary_code(&error, 5) {
            Self::Busy(error.to_string())
        } else {
            Self::Database(error)
        }
    }
}

fn sqlite_error_has_primary_code(error: &sqlx::Error, primary_code: i32) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };
    database
        .code()
        .and_then(|code| code.parse::<i32>().ok())
        .is_some_and(|code| code & 0xff == primary_code)
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
    path: Arc<PathBuf>,
}

impl Catalog {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| CatalogError::CreateDirectory {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        let catalog = Self {
            pool,
            path: Arc::new(path),
        };
        catalog.initialize().await?;
        Ok(catalog)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    async fn begin_write_transaction(&self) -> Result<Transaction<'static, Sqlite>, CatalogError> {
        Ok(self.pool.begin_with("BEGIN IMMEDIATE").await?)
    }

    async fn initialize(&self) -> Result<(), CatalogError> {
        let mut transaction = self.begin_write_transaction().await?;
        for statement in CATALOG_SCHEMA
            .split(';')
            .map(str::trim)
            .filter(|statement| !statement.is_empty())
        {
            query(statement).execute(&mut *transaction).await?;
        }
        let project_mutation_triggers = project_mutations::trigger_schema();
        raw_sql(&project_mutation_triggers)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn health_check(&self) -> Result<(), CatalogError> {
        query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn list_projects(
        &self,
        before: Option<ProjectId>,
        limit: usize,
    ) -> Result<Vec<ProjectSummary>, CatalogError> {
        let cursor = if let Some(before) = before {
            Some(
                query("SELECT created_at, id FROM projects WHERE id = ?")
                    .bind(before.to_string())
                    .fetch_optional(&self.pool)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound {
                        resource: format!("project list cursor {before}"),
                    })?,
            )
        } else {
            None
        };
        let (cursor_created_at, cursor_id) = cursor.map_or((None, None), |row| {
            (
                Some(row.get::<String, _>("created_at")),
                Some(row.get::<String, _>("id")),
            )
        });
        let rows = query(
            "SELECT p.id, p.name, p.created_at, p.run_count, p.mutation_revision \
             FROM projects p \
             WHERE (? IS NULL OR p.created_at < ? \
                    OR (p.created_at = ? AND p.id < ?)) \
             ORDER BY p.created_at DESC, p.id DESC LIMIT ?",
        )
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_id)
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
                    mutation_token: from_i64(
                        row.get("mutation_revision"),
                        "project mutation revision",
                    )?
                    .to_string(),
                })
            })
            .collect()
    }

    pub async fn get_project(&self, name: &str) -> Result<ProjectSummary, CatalogError> {
        let row = query(
            "SELECT id, name, created_at, run_count, mutation_revision \
             FROM projects WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| CatalogError::NotFound {
            resource: format!("project {name}"),
        })?;
        Ok(ProjectSummary {
            id: parse_id(row.get::<String, _>("id"), "project ID")?,
            name: row.get("name"),
            created_at: row.get("created_at"),
            run_count: from_i64(row.get("run_count"), "run count")?,
            mutation_token: from_i64(row.get("mutation_revision"), "project mutation revision")?
                .to_string(),
        })
    }

    pub async fn list_runs(
        &self,
        project_name: &str,
        before: Option<RunId>,
        search: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RunListItem>, CatalogError> {
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
        let cursor = if let Some(before) = before {
            Some(
                query(
                    "SELECT r.created_at, r.id FROM runs r \
                     JOIN projects p ON p.id = r.project_id \
                     WHERE r.id = ? AND p.name = ? \
                     AND (? IS NULL OR instr(lower(r.name), lower(?)) > 0)",
                )
                .bind(before.to_string())
                .bind(project_name)
                .bind(search)
                .bind(search)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!("run list cursor {before} in project {project_name}"),
                })?,
            )
        } else {
            None
        };
        let (cursor_created_at, cursor_id) = cursor.map_or((None, None), |row| {
            (
                Some(row.get::<String, _>("created_at")),
                Some(row.get::<String, _>("id")),
            )
        });
        let rows = query(
            "SELECT r.id, r.project_id, p.name AS project, r.name, r.state, \
                    r.created_at, r.updated_at, d.finished_at, d.metric_summary_truncated, \
                    v.document_revision, v.metric_revision, v.rich_data_revision \
             FROM runs r \
             JOIN projects p ON p.id = r.project_id \
             JOIN run_documents d ON d.run_id = r.id \
             JOIN run_revisions v ON v.run_id = r.id \
             WHERE p.name = ? AND (? IS NULL OR r.created_at < ? \
                    OR (r.created_at = ? AND r.id < ?)) \
             AND (? IS NULL OR instr(lower(r.name), lower(?)) > 0) \
             ORDER BY r.created_at DESC, r.id DESC LIMIT ?",
        )
        .bind(project_name)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_id)
        .bind(search)
        .bind(search)
        .bind(to_i64(limit as u64, "run limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(run_list_item_from_row).collect()
    }

    pub async fn query_runs(
        &self,
        request: &RunQueryRequest,
    ) -> Result<Vec<RunListItem>, CatalogError> {
        if request.run_ids.len() > 32 {
            return Err(CatalogError::InvalidData(
                "run queries cannot contain more than 32 run IDs".to_owned(),
            ));
        }
        if !request.run_ids.is_empty() {
            if request.before.is_some() {
                return Err(CatalogError::InvalidData(
                    "run_ids and before cannot be used together".to_owned(),
                ));
            }
            if request.limit < request.run_ids.len() {
                return Err(CatalogError::InvalidData(
                    "run query limit must include every requested run ID".to_owned(),
                ));
            }
            if request
                .run_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len()
                != request.run_ids.len()
            {
                return Err(CatalogError::InvalidData(
                    "run query run IDs must be unique".to_owned(),
                ));
            }
            let mut ownership = QueryBuilder::<Sqlite>::new(
                "SELECT COUNT(*) AS selected FROM runs r JOIN projects p ON p.id = r.project_id \
                 WHERE r.id IN (",
            );
            {
                let mut ids = ownership.separated(", ");
                for run_id in &request.run_ids {
                    ids.push_bind(run_id.to_string());
                }
                ids.push_unseparated(")");
            }
            if let Some(project) = &request.project {
                ownership.push(" AND p.name = ").push_bind(project);
            }
            let selected: i64 = ownership
                .build()
                .fetch_one(&self.pool)
                .await?
                .get("selected");
            if usize::try_from(selected).ok() != Some(request.run_ids.len()) {
                return Err(CatalogError::NotFound {
                    resource: request.project.as_ref().map_or_else(
                        || "one or more requested runs".to_owned(),
                        |project| format!("one or more requested runs in project {project}"),
                    ),
                });
            }
        }
        let cursor = if let Some(before) = request.before {
            let mut cursor_query = QueryBuilder::<Sqlite>::new(
                "SELECT r.created_at, r.id FROM runs r \
                 JOIN projects p ON p.id = r.project_id \
                 JOIN run_documents d ON d.run_id = r.id WHERE r.id = ",
            );
            cursor_query.push_bind(before.to_string());
            if let Some(project) = &request.project {
                cursor_query.push(" AND p.name = ").push_bind(project);
            }
            if let Some(state) = request.state {
                cursor_query
                    .push(" AND r.state = ")
                    .push_bind(state.to_string());
            }
            if let Some(name) = &request.name {
                cursor_query.push(" AND r.name = ").push_bind(name);
            }
            if let Some(name) = &request.name_contains {
                cursor_query
                    .push(" AND instr(r.name, ")
                    .push_bind(name)
                    .push(") > 0");
            }
            for (key, value) in &request.config_equals {
                push_json_equality(&mut cursor_query, "d.config_json", key, value)?;
            }
            for (key, value) in &request.summary_equals {
                push_summary_equality(&mut cursor_query, key, value)?;
            }
            Some(
                cursor_query
                    .build()
                    .fetch_optional(&self.pool)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound {
                        resource: format!("run query cursor {before} for the requested filters"),
                    })?,
            )
        } else {
            None
        };
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT r.id, r.project_id, p.name AS project, r.name, r.state, \
                    r.created_at, r.updated_at, d.finished_at, d.metric_summary_truncated, \
                    v.document_revision, v.metric_revision, v.rich_data_revision \
             FROM runs r \
             JOIN projects p ON p.id = r.project_id \
             JOIN run_documents d ON d.run_id = r.id \
             JOIN run_revisions v ON v.run_id = r.id WHERE 1 = 1",
        );
        if let Some(project) = &request.project {
            query.push(" AND p.name = ").push_bind(project);
        }
        if !request.run_ids.is_empty() {
            query.push(" AND r.id IN (");
            {
                let mut ids = query.separated(", ");
                for run_id in &request.run_ids {
                    ids.push_bind(run_id.to_string());
                }
                ids.push_unseparated(")");
            }
        }
        if let Some(state) = request.state {
            query.push(" AND r.state = ").push_bind(state.to_string());
        }
        if let Some(name) = &request.name {
            query.push(" AND r.name = ").push_bind(name);
        }
        if let Some(name) = &request.name_contains {
            query
                .push(" AND instr(r.name, ")
                .push_bind(name)
                .push(") > 0");
        }
        for (key, value) in &request.config_equals {
            push_json_equality(&mut query, "d.config_json", key, value)?;
        }
        for (key, value) in &request.summary_equals {
            push_summary_equality(&mut query, key, value)?;
        }
        if let Some(cursor) = cursor {
            let created_at: String = cursor.get("created_at");
            let id: String = cursor.get("id");
            query
                .push(" AND (r.created_at < ")
                .push_bind(created_at.clone())
                .push(" OR (r.created_at = ")
                .push_bind(created_at)
                .push(" AND r.id < ")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY r.created_at DESC, r.id DESC LIMIT ")
            .push_bind(to_i64(request.limit as u64, "run query limit")?);
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(run_list_item_from_row).collect()
    }

    pub async fn metric_keys(
        &self,
        run_id: RunId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, CatalogError> {
        ensure_run_exists(&self.pool, run_id).await?;
        if let Some(after) = after {
            let cursor_matches: bool =
                query("SELECT EXISTS(SELECT 1 FROM run_metric_keys WHERE run_id = ? AND key = ?)")
                    .bind(run_id.to_string())
                    .bind(after)
                    .fetch_one(&self.pool)
                    .await?
                    .get(0);
            if !cursor_matches {
                return Err(CatalogError::NotFound {
                    resource: format!("metric key cursor {after:?} for run {run_id}"),
                });
            }
        }
        let rows = query(
            "SELECT key FROM run_metric_keys WHERE run_id = ? \
             AND (? IS NULL OR key > ?) ORDER BY key LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(after)
        .bind(after)
        .bind(to_i64(limit as u64, "metric key limit")?)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.get("key")).collect())
    }

    pub async fn project_metric_catalog(
        &self,
        project: &str,
        request: &ProjectMetricCatalogRequest,
    ) -> Result<ProjectMetricCatalogPage, CatalogError> {
        discovery::project_metric_catalog(&self.pool, project, request).await
    }

    pub async fn create_alert(
        &self,
        run_id: RunId,
        request: &CreateAlertRequest,
    ) -> Result<(AlertRecord, bool), CatalogError> {
        let mut transaction = self.begin_write_transaction().await?;
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
        increment_rich_data_revision(&mut transaction, run_id).await?;
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
        ensure_run_exists(&self.pool, run_id).await?;
        let cursor = if let Some(before) = before {
            Some(
                query("SELECT timestamp_ms, id FROM run_alerts WHERE id = ? AND run_id = ?")
                    .bind(before.to_string())
                    .bind(run_id.to_string())
                    .fetch_optional(&self.pool)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound {
                        resource: format!("alert list cursor {before} for run {run_id}"),
                    })?,
            )
        } else {
            None
        };
        let (cursor_timestamp, cursor_id) = cursor.map_or((None, None), |row| {
            (
                Some(row.get::<i64, _>("timestamp_ms")),
                Some(row.get::<String, _>("id")),
            )
        });
        let rows = query(
            "SELECT id, run_id, title, text, level, step, timestamp_ms, created_at \
             FROM run_alerts WHERE run_id = ? \
             AND (? IS NULL OR timestamp_ms < ? OR (timestamp_ms = ? AND id < ?)) \
             ORDER BY timestamp_ms DESC, id DESC LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(cursor_timestamp)
        .bind(cursor_timestamp)
        .bind(cursor_timestamp)
        .bind(&cursor_id)
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
        let mut transaction = self.begin_write_transaction().await?;
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
            "INSERT INTO run_rich_value_keys (run_id, key, value_count, latest_value_id) \
             VALUES (?, ?, 1, ?) \
             ON CONFLICT(run_id, key) DO UPDATE SET \
                 value_count = run_rich_value_keys.value_count + 1, \
                 latest_value_id = excluded.latest_value_id",
        )
        .bind(run_id.to_string())
        .bind(&request.key)
        .bind(value_id.to_string())
        .execute(&mut *transaction)
        .await?;
        increment_rich_data_revision(&mut transaction, run_id).await?;
        touch_run(&mut transaction, run_id).await?;
        let value = load_required_rich_value(&mut transaction, value_id).await?;
        transaction.commit().await?;
        Ok((value, false))
    }

    pub async fn get_rich_value(
        &self,
        value_id: RichValueId,
    ) -> Result<RichValueRecord, CatalogError> {
        let mut transaction = self.pool.begin().await?;
        let value = load_required_rich_value(&mut transaction, value_id).await?;
        transaction.commit().await?;
        Ok(value)
    }

    pub async fn list_rich_value_keys(
        &self,
        run_id: RunId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RichValueKeySummary>, CatalogError> {
        ensure_run_exists(&self.pool, run_id).await?;
        if let Some(after) = after {
            let cursor_matches: bool = query(
                "SELECT EXISTS(SELECT 1 FROM run_rich_value_keys WHERE run_id = ? AND key = ?)",
            )
            .bind(run_id.to_string())
            .bind(after)
            .fetch_one(&self.pool)
            .await?
            .get(0);
            if !cursor_matches {
                return Err(CatalogError::NotFound {
                    resource: format!("rich value key cursor {after:?} for run {run_id}"),
                });
            }
        }
        let rows = query(
            "SELECT v.id, v.run_id, v.key, v.kind, v.step, v.timestamp_ms, v.blob_json, \
                    v.created_at, k.value_count \
             FROM run_rich_value_keys k \
             JOIN run_rich_values v ON v.id = k.latest_value_id \
             WHERE k.run_id = ? AND (? IS NULL OR k.key > ?) ORDER BY k.key LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(after)
        .bind(after)
        .bind(to_i64(limit as u64, "rich value key limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(rich_value_key_from_row).collect()
    }

    pub async fn list_rich_values(
        &self,
        run_id: RunId,
        key: &str,
        before: Option<RichValueId>,
        limit: usize,
    ) -> Result<Vec<RichValueSummary>, CatalogError> {
        ensure_run_exists(&self.pool, run_id).await?;
        let cursor = if let Some(before) = before {
            Some(
                query(
                    "SELECT created_at, id FROM run_rich_values \
                     WHERE id = ? AND run_id = ? AND key = ?",
                )
                .bind(before.to_string())
                .bind(run_id.to_string())
                .bind(key)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!("rich value list cursor {before} for {run_id}/{key}"),
                })?,
            )
        } else {
            None
        };
        let (cursor_created_at, cursor_id) = cursor.map_or((None, None), |row| {
            (
                Some(row.get::<String, _>("created_at")),
                Some(row.get::<String, _>("id")),
            )
        });
        let rows = query(
            "SELECT id, run_id, key, kind, step, timestamp_ms, blob_json, created_at \
             FROM run_rich_values WHERE run_id = ? AND key = ? \
             AND (? IS NULL OR created_at < ? OR (created_at = ? AND id < ?)) \
             ORDER BY created_at DESC, id DESC LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(key)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_id)
        .bind(to_i64(limit as u64, "rich value limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(rich_value_summary_from_row).collect()
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
        let mut transaction = self.begin_write_transaction().await?;
        ensure_running(&mut transaction, run_id).await?;
        let location = run_location_in(&mut transaction, run_id).await?;
        let request_json = serde_json::to_string(request)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        if let Some(artifact_id) = request.id {
            if let Some(existing) = load_artifact_base(&mut transaction, artifact_id).await? {
                if existing.request_json != request_json || existing.created_by_run != run_id {
                    return Err(CatalogError::Conflict(
                        "artifact ID was reused with different contents".to_owned(),
                    ));
                }
                let artifact = finish_artifact(&mut transaction, existing).await?;
                transaction.commit().await?;
                return Ok((artifact, true));
            }
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
        let version = if let Some(version) = request.version {
            version
        } else {
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
            previous_version.map_or(Ok(0), |value| {
                from_i64(value, "artifact version")?
                    .checked_add(1)
                    .ok_or_else(|| {
                        CatalogError::InvalidData("artifact version overflow".to_owned())
                    })
            })?
        };
        if version > MAX_JSON_SAFE_INTEGER {
            return Err(CatalogError::Limit(format!(
                "artifact version cannot exceed {MAX_JSON_SAFE_INTEGER}"
            )));
        }
        let artifact_id = request.id.unwrap_or_default();
        let metadata_json = serde_json::to_string(&request.metadata)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let entries_json = serde_json::to_string(&request.entries)
            .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
        let inserted = query(
            "INSERT INTO artifact_versions \
             (id, project_id, name, artifact_type, version, description, metadata_json, \
              entries_json, request_json, created_by_run, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp) \
             ON CONFLICT DO NOTHING",
        )
        .bind(artifact_id.to_string())
        .bind(location.project_id.to_string())
        .bind(&request.name)
        .bind(&request.artifact_type)
        .bind(to_i64(version, "artifact version")?)
        .bind(&request.description)
        .bind(metadata_json)
        .bind(entries_json)
        .bind(&request_json)
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 0 {
            if let Some(existing) = load_artifact_base(&mut transaction, artifact_id).await? {
                if existing.request_json == request_json && existing.created_by_run == run_id {
                    let artifact = finish_artifact(&mut transaction, existing).await?;
                    transaction.commit().await?;
                    return Ok((artifact, true));
                }
                return Err(CatalogError::Conflict(
                    "artifact ID was reused with different contents".to_owned(),
                ));
            }
            return Err(CatalogError::Conflict(format!(
                "artifact version v{version} already exists in collection {}/{}",
                request.name, request.artifact_type
            )));
        }
        for alias in &request.aliases {
            query(
                "INSERT INTO artifact_aliases \
                 (project_id, name, artifact_type, alias, artifact_id) VALUES (?, ?, ?, ?, ?) \
                 ON CONFLICT(project_id, name, artifact_type, alias) \
                 DO UPDATE SET artifact_id = excluded.artifact_id \
                 WHERE (SELECT version FROM artifact_versions \
                        WHERE id = artifact_aliases.artifact_id) < ?",
            )
            .bind(location.project_id.to_string())
            .bind(&request.name)
            .bind(&request.artifact_type)
            .bind(alias)
            .bind(artifact_id.to_string())
            .bind(to_i64(version, "artifact version")?)
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
        increment_rich_data_revision(&mut transaction, run_id).await?;
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
        let mut transaction = self.begin_write_transaction().await?;
        ensure_running(&mut transaction, run_id).await?;
        let location = run_location_in(&mut transaction, run_id).await?;
        let artifact = load_required_artifact(&mut transaction, artifact_id).await?;
        if artifact.project_id != location.project_id {
            return Err(CatalogError::Conflict(
                "artifact and run must belong to the same project".to_owned(),
            ));
        }
        let linked = insert_artifact_lineage(
            &mut transaction,
            artifact_id,
            run_id,
            ArtifactRelation::Input,
        )
        .await?;
        if linked {
            increment_rich_data_revision(&mut transaction, run_id).await?;
            touch_run(&mut transaction, run_id).await?;
        }
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
    ) -> Result<Vec<ArtifactSummary>, CatalogError> {
        let project_exists: bool = query("SELECT EXISTS(SELECT 1 FROM projects WHERE name = ?)")
            .bind(project)
            .fetch_one(&self.pool)
            .await?
            .get(0);
        if !project_exists {
            return Err(CatalogError::NotFound {
                resource: format!("project {project}"),
            });
        }
        let cursor = if let Some(before) = before {
            Some(
                query(
                    "SELECT v.created_at, v.id FROM artifact_versions v \
                     JOIN projects p ON p.id = v.project_id WHERE v.id = ? AND p.name = ?",
                )
                .bind(before.to_string())
                .bind(project)
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!("artifact list cursor {before} in project {project}"),
                })?,
            )
        } else {
            None
        };
        let (cursor_created_at, cursor_id) = cursor.map_or((None, None), |row| {
            (
                Some(row.get::<String, _>("created_at")),
                Some(row.get::<String, _>("id")),
            )
        });
        let rows = query(
            "SELECT v.id, v.project_id, p.name AS project, v.name, v.artifact_type, v.version, \
                    json_array_length(v.entries_json) AS entry_count, v.created_by_run, \
                    v.created_at \
             FROM artifact_versions v JOIN projects p ON p.id = v.project_id \
             WHERE p.name = ? AND (? IS NULL OR v.created_at < ? \
                    OR (v.created_at = ? AND v.id < ?)) \
             ORDER BY v.created_at DESC, v.id DESC LIMIT ?",
        )
        .bind(project)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_id)
        .bind(to_i64(limit as u64, "artifact list limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(artifact_summary_from_row).collect()
    }

    pub async fn list_run_artifacts(
        &self,
        run_id: RunId,
        before: Option<ArtifactId>,
        before_relation: Option<ArtifactRelation>,
        limit: usize,
    ) -> Result<Vec<RunArtifactRecord>, CatalogError> {
        ensure_run_exists(&self.pool, run_id).await?;
        if before.is_some() != before_relation.is_some() {
            return Err(CatalogError::InvalidData(
                "run artifact cursors require both artifact ID and relation".to_owned(),
            ));
        }
        let cursor = if let (Some(before), Some(before_relation)) = (before, before_relation) {
            Some(
                query(
                    "SELECT created_at, artifact_id, relation FROM artifact_lineage \
                     WHERE artifact_id = ? AND run_id = ? AND relation = ?",
                )
                .bind(before.to_string())
                .bind(run_id.to_string())
                .bind(before_relation.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!(
                        "artifact list cursor {before}/{before_relation} for run {run_id}"
                    ),
                })?,
            )
        } else {
            None
        };
        let (cursor_created_at, cursor_id, cursor_relation) =
            cursor.map_or((None, None, None), |row| {
                (
                    Some(row.get::<String, _>("created_at")),
                    Some(row.get::<String, _>("artifact_id")),
                    Some(row.get::<String, _>("relation")),
                )
            });
        let rows = query(
            "SELECT v.id, v.project_id, p.name AS project, v.name, v.artifact_type, v.version, \
                    json_array_length(v.entries_json) AS entry_count, v.created_by_run, \
                    v.created_at, l.relation \
             FROM artifact_lineage l \
             JOIN artifact_versions v ON v.id = l.artifact_id \
             JOIN projects p ON p.id = v.project_id \
             WHERE l.run_id = ? AND (? IS NULL OR l.created_at < ? \
                    OR (l.created_at = ? AND v.id < ?) \
                    OR (l.created_at = ? AND v.id = ? AND l.relation < ?)) \
             ORDER BY l.created_at DESC, v.id DESC, l.relation DESC LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_id)
        .bind(&cursor_created_at)
        .bind(&cursor_id)
        .bind(&cursor_relation)
        .bind(to_i64(limit as u64, "run artifact list limit")?)
        .fetch_all(&self.pool)
        .await?;
        let mut artifacts = Vec::with_capacity(rows.len());
        for row in rows {
            let relation = ArtifactRelation::from_str(&row.get::<String, _>("relation"))
                .map_err(|error| CatalogError::InvalidData(error.to_owned()))?;
            let artifact = artifact_summary_from_row(row)?;
            artifacts.push(RunArtifactRecord { artifact, relation });
        }
        Ok(artifacts)
    }

    pub async fn artifact_lineage(
        &self,
        artifact_id: ArtifactId,
        relation: ArtifactRelation,
        before: Option<RunId>,
        limit: usize,
    ) -> Result<Vec<RunListItem>, CatalogError> {
        let artifact_exists: bool =
            query("SELECT EXISTS(SELECT 1 FROM artifact_versions WHERE id = ?)")
                .bind(artifact_id.to_string())
                .fetch_one(&self.pool)
                .await?
                .get(0);
        if !artifact_exists {
            return Err(CatalogError::NotFound {
                resource: format!("artifact {artifact_id}"),
            });
        }
        let cursor = if let Some(before) = before {
            Some(
                query(
                    "SELECT created_at, run_id FROM artifact_lineage \
                     WHERE artifact_id = ? AND relation = ? AND run_id = ?",
                )
                .bind(artifact_id.to_string())
                .bind(relation.to_string())
                .bind(before.to_string())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    resource: format!(
                        "artifact lineage cursor {before} for {artifact_id}/{relation}"
                    ),
                })?,
            )
        } else {
            None
        };
        let (cursor_created_at, cursor_run_id) = cursor.map_or((None, None), |row| {
            (
                Some(row.get::<String, _>("created_at")),
                Some(row.get::<String, _>("run_id")),
            )
        });
        let rows = query(
            "SELECT r.id, r.project_id, p.name AS project, r.name, r.state, r.created_at, \
                    r.updated_at, d.finished_at, d.metric_summary_truncated, \
                    v.document_revision, v.metric_revision, v.rich_data_revision \
             FROM artifact_lineage l JOIN runs r ON r.id = l.run_id \
             JOIN projects p ON p.id = r.project_id \
             JOIN run_documents d ON d.run_id = r.id \
             JOIN run_revisions v ON v.run_id = r.id \
             WHERE l.artifact_id = ? AND l.relation = ? \
             AND (? IS NULL OR l.created_at < ? \
                  OR (l.created_at = ? AND r.id < ?)) \
             ORDER BY l.created_at DESC, r.id DESC LIMIT ?",
        )
        .bind(artifact_id.to_string())
        .bind(relation.to_string())
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_created_at)
        .bind(&cursor_run_id)
        .bind(to_i64(limit as u64, "artifact lineage limit")?)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(run_list_item_from_row).collect()
    }

    pub async fn create_or_resume_run(
        &self,
        project_name: &str,
        request: &CreateRunRequest,
    ) -> Result<(RunRecord, bool), CatalogError> {
        let mut transaction = self.begin_write_transaction().await?;
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
            "INSERT INTO run_revisions \
             (run_id, document_revision, metric_revision, rich_data_revision) \
             VALUES (?, 0, 0, 0)",
        )
        .bind(run_id.to_string())
        .execute(&mut *transaction)
        .await?;
        query("INSERT INTO run_documents (run_id, config_json, summary_json) VALUES (?, ?, '{}')")
            .bind(run_id.to_string())
            .bind(config_json)
            .execute(&mut *transaction)
            .await?;
        query("UPDATE projects SET run_count = run_count + 1 WHERE id = ?")
            .bind(project_id.to_string())
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
        let mut transaction = self.begin_write_transaction().await?;
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
        let changed = updates
            .iter()
            .any(|(key, value)| config.get(key) != Some(value));
        if !changed {
            let run = load_required_run(&mut transaction, run_id).await?;
            transaction.commit().await?;
            return Ok(run);
        }
        config.extend(updates.clone());
        let encoded = serialize_document(&config, "config", MAX_CONFIG_BYTES)?;
        query("UPDATE run_documents SET config_json = ? WHERE run_id = ?")
            .bind(encoded)
            .bind(run_id.to_string())
            .execute(&mut *transaction)
            .await?;
        increment_document_revision(&mut transaction, run_id).await?;
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
        let mut transaction = self.begin_write_transaction().await?;
        ensure_running(&mut transaction, run_id).await?;
        let summary = load_document(&mut transaction, run_id, "summary_json", "summary").await?;
        let changed = updates
            .iter()
            .any(|(key, value)| summary.get(key) != Some(value));
        if !changed {
            let run = load_required_run(&mut transaction, run_id).await?;
            transaction.commit().await?;
            return Ok(run);
        }
        merge_summary_document(&mut transaction, run_id, updates).await?;
        increment_document_revision(&mut transaction, run_id).await?;
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

    pub async fn segment_path_is_registered(
        &self,
        relative_path: &str,
    ) -> Result<bool, CatalogError> {
        let registered: i64 = query(
            "SELECT EXISTS(\
                 SELECT relative_path FROM metric_segments WHERE relative_path = ? \
                 UNION ALL \
                 SELECT relative_path FROM retired_metric_segments WHERE relative_path = ?\
             )",
        )
        .bind(relative_path)
        .bind(relative_path)
        .fetch_one(&self.pool)
        .await?
        .get(0);
        Ok(registered != 0)
    }

    pub async fn register_batch(
        &self,
        run_id: RunId,
        batch_sequence: u64,
        digest: &str,
        segment: &SegmentManifest,
        latest_values: &BTreeMap<String, f64>,
    ) -> Result<BatchRegistration, CatalogError> {
        let batch_sequence = to_i64(batch_sequence, "batch sequence")?;
        let mut transaction = self.begin_write_transaction().await?;

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

        update_metric_summary_preview(&mut transaction, run_id, latest_values).await?;
        for (key, value) in latest_values {
            query(
                "INSERT INTO run_metric_keys (run_id, key, latest_value) VALUES (?, ?, ?) \
                 ON CONFLICT(run_id, key) DO UPDATE SET latest_value = excluded.latest_value",
            )
            .bind(run_id.to_string())
            .bind(key)
            .bind(value)
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
        let mut transaction = self.begin_write_transaction().await?;
        let existing = load_required_run(&mut transaction, run_id).await?;
        if existing.state == RunState::Finished {
            if summary_values
                .iter()
                .any(|(key, value)| existing.explicit_summary.get(key) != Some(value))
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
        increment_document_revision(&mut transaction, run_id).await?;
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
        if target_rows == 0 || max_segments < MIN_COMPACTION_INPUT_SEGMENTS {
            return Err(CatalogError::InvalidData(format!(
                "compaction requires a positive row target and at least \
                     {MIN_COMPACTION_INPUT_SEGMENTS} input segments"
            )));
        }
        let target_rows_i64 = to_i64(target_rows as u64, "compaction row target")?;
        let seed = query(
            "WITH ordered AS ( \
                 SELECT s.run_id, r.project_id, s.signature, s.first_sequence, \
                        s.last_sequence, s.row_count, \
                        LEAD(s.signature, 1) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS second_signature, \
                        LEAD(s.signature, 2) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS third_signature, \
                        LEAD(s.signature, 3) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS fourth_signature, \
                        LEAD(s.first_sequence, 1) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS second_first_sequence, \
                        LEAD(s.first_sequence, 2) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS third_first_sequence, \
                        LEAD(s.first_sequence, 3) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS fourth_first_sequence, \
                        LEAD(s.last_sequence, 1) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS second_last_sequence, \
                        LEAD(s.last_sequence, 2) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS third_last_sequence, \
                        LEAD(s.row_count, 1) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS second_row_count, \
                        LEAD(s.row_count, 2) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS third_row_count, \
                        LEAD(s.row_count, 3) OVER ( \
                            PARTITION BY s.run_id ORDER BY s.first_sequence, s.id \
                        ) AS fourth_row_count \
                 FROM metric_segments s \
                 JOIN runs r ON r.id = s.run_id \
                 WHERE s.row_count < ? \
             ) \
             SELECT run_id, project_id, first_sequence \
             FROM ordered \
             WHERE signature = second_signature \
               AND signature = third_signature \
               AND signature = fourth_signature \
               AND last_sequence + 1 = second_first_sequence \
               AND second_last_sequence + 1 = third_first_sequence \
               AND third_last_sequence + 1 = fourth_first_sequence \
               AND row_count + second_row_count + third_row_count + fourth_row_count <= ? \
               AND MAX(row_count, second_row_count, third_row_count, fourth_row_count) \
                   <= MIN(row_count, second_row_count, third_row_count, fourth_row_count) * ? \
             ORDER BY first_sequence, run_id LIMIT 1",
        )
        .bind(target_rows_i64)
        .bind(target_rows_i64)
        .bind(to_i64(
            MAX_COMPACTION_SIZE_RATIO as u64,
            "compaction size ratio",
        )?)
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
        let mut minimum_rows = usize::MAX;
        let mut maximum_rows = 0usize;
        for row in rows {
            let segment = segment_from_row(row)?;
            let compatible = segments.last().is_none_or(|previous: &SegmentRecord| {
                previous.signature == segment.signature
                    && previous.last_sequence.checked_add(1) == Some(segment.first_sequence)
            });
            let Some(next_total) = total_rows.checked_add(segment.row_count) else {
                break;
            };
            let next_minimum = minimum_rows.min(segment.row_count);
            let next_maximum = maximum_rows.max(segment.row_count);
            let comparable_size = next_minimum
                .checked_mul(MAX_COMPACTION_SIZE_RATIO)
                .is_some_and(|limit| next_maximum <= limit);
            if !compatible
                || segment.row_count >= target_rows
                || !comparable_size
                || next_total > target_rows
            {
                break;
            }
            total_rows = next_total;
            minimum_rows = next_minimum;
            maximum_rows = next_maximum;
            segments.push(segment);
        }
        if segments.len() < MIN_COMPACTION_INPUT_SEGMENTS {
            return Ok(None);
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
        let mut transaction = self.begin_write_transaction().await?;
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
        let mut transaction = self.begin_write_transaction().await?;
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
                r.created_at, r.updated_at, d.config_json, d.summary_json, \
                d.metric_summary_json, d.metric_summary_truncated, d.finished_at, \
                v.document_revision, v.metric_revision, v.rich_data_revision \
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
) -> Result<bool, CatalogError> {
    let result = query(
        "INSERT INTO artifact_lineage (artifact_id, run_id, relation, created_at) \
         VALUES (?, ?, ?, current_timestamp) \
         ON CONFLICT(artifact_id, run_id, relation) DO NOTHING",
    )
    .bind(artifact_id.to_string())
    .bind(run_id.to_string())
    .bind(relation.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
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

async fn increment_rich_data_revision(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<(), CatalogError> {
    query("UPDATE run_revisions SET rich_data_revision = rich_data_revision + 1 WHERE run_id = ?")
        .bind(run_id.to_string())
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn increment_document_revision(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
) -> Result<(), CatalogError> {
    query("UPDATE run_revisions SET document_revision = document_revision + 1 WHERE run_id = ?")
        .bind(run_id.to_string())
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn ensure_run_exists(pool: &SqlitePool, run_id: RunId) -> Result<(), CatalogError> {
    let exists: bool = query("SELECT EXISTS(SELECT 1 FROM runs WHERE id = ?)")
        .bind(run_id.to_string())
        .fetch_one(pool)
        .await?
        .get(0);
    if exists {
        Ok(())
    } else {
        Err(CatalogError::NotFound {
            resource: format!("run {run_id}"),
        })
    }
}

fn run_from_row(row: SqliteRow) -> Result<RunRecord, CatalogError> {
    let explicit_summary =
        parse_document(row.get::<String, _>("summary_json"), "explicit summary")?;
    let metric_summary = parse_document(
        row.get::<String, _>("metric_summary_json"),
        "metric summary",
    )?;
    let mut summary = metric_summary.clone();
    summary.extend(explicit_summary.clone());
    Ok(RunRecord {
        id: parse_id(row.get::<String, _>("id"), "run ID")?,
        project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
        project: row.get("project"),
        name: row.get("name"),
        state: parse_state(row.get::<String, _>("state"))?,
        config: parse_document(row.get::<String, _>("config_json"), "config")?,
        summary,
        explicit_summary,
        metric_summary,
        summary_truncated: row.get("metric_summary_truncated"),
        document_revision: from_i64(row.get("document_revision"), "document revision")?,
        metric_revision: from_i64(row.get("metric_revision"), "metric revision")?,
        rich_data_revision: from_i64(row.get("rich_data_revision"), "rich data revision")?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        finished_at: row.get("finished_at"),
    })
}

fn run_list_item_from_row(row: SqliteRow) -> Result<RunListItem, CatalogError> {
    Ok(RunListItem {
        id: parse_id(row.get::<String, _>("id"), "run ID")?,
        project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
        project: row.get("project"),
        name: row.get("name"),
        state: parse_state(row.get::<String, _>("state"))?,
        summary_truncated: row.get("metric_summary_truncated"),
        document_revision: from_i64(row.get("document_revision"), "document revision")?,
        metric_revision: from_i64(row.get("metric_revision"), "metric revision")?,
        rich_data_revision: from_i64(row.get("rich_data_revision"), "rich data revision")?,
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

fn rich_value_summary_from_row(row: SqliteRow) -> Result<RichValueSummary, CatalogError> {
    let blob = row
        .get::<Option<String>, _>("blob_json")
        .map(|value| {
            serde_json::from_str::<BlobRef>(&value)
                .map_err(|error| CatalogError::InvalidData(error.to_string()))
        })
        .transpose()?;
    Ok(RichValueSummary {
        id: parse_id(row.get::<String, _>("id"), "rich value ID")?,
        run_id: parse_id(row.get::<String, _>("run_id"), "run ID")?,
        key: row.get("key"),
        kind: RichValueKind::from_str(&row.get::<String, _>("kind"))
            .map_err(|error| CatalogError::InvalidData(error.to_owned()))?,
        step: from_i64(row.get("step"), "rich value step")?,
        timestamp_ms: row.get("timestamp_ms"),
        blob,
        created_at: row.get("created_at"),
    })
}

fn rich_value_key_from_row(row: SqliteRow) -> Result<RichValueKeySummary, CatalogError> {
    let count = from_i64(row.get("value_count"), "rich value count")?;
    let latest = rich_value_summary_from_row(row)?;
    Ok(RichValueKeySummary {
        key: latest.key.clone(),
        count,
        latest,
    })
}

fn artifact_summary_from_row(row: SqliteRow) -> Result<ArtifactSummary, CatalogError> {
    Ok(ArtifactSummary {
        id: parse_id(row.get::<String, _>("id"), "artifact ID")?,
        project_id: parse_id(row.get::<String, _>("project_id"), "project ID")?,
        project: row.get("project"),
        name: row.get("name"),
        artifact_type: row.get("artifact_type"),
        version: from_i64(row.get("version"), "artifact version")?,
        entry_count: from_i64(row.get("entry_count"), "artifact entry count")?,
        created_by_run: parse_id(row.get::<String, _>("created_by_run"), "run ID")?,
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

fn push_json_equality<'args>(
    query: &mut QueryBuilder<'args, Sqlite>,
    column: &str,
    key: &str,
    value: &Value,
) -> Result<(), CatalogError> {
    let encoded = serde_json::to_string(value)
        .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
    query
        .push(" AND EXISTS (SELECT 1 FROM json_each(")
        .push(column)
        .push(") AS document_entry WHERE document_entry.key = ")
        .push_bind(key.to_owned())
        .push(" AND document_entry.value IS json_extract(")
        .push_bind(encoded)
        .push(", '$'))");
    Ok(())
}

fn push_summary_equality<'args>(
    query: &mut QueryBuilder<'args, Sqlite>,
    key: &str,
    value: &Value,
) -> Result<(), CatalogError> {
    let encoded = serde_json::to_string(value)
        .map_err(|error| CatalogError::InvalidData(error.to_string()))?;
    query
        .push(
            " AND (EXISTS (SELECT 1 FROM json_each(d.summary_json) AS explicit_match \
               WHERE explicit_match.key = ",
        )
        .push_bind(key.to_owned())
        .push(" AND explicit_match.value IS json_extract(")
        .push_bind(encoded.clone())
        .push(
            ", '$')) OR (NOT EXISTS (SELECT 1 FROM json_each(d.summary_json) AS \
               explicit_key WHERE explicit_key.key = ",
        )
        .push_bind(key.to_owned())
        .push(
            ") AND EXISTS (SELECT 1 FROM json_each(d.metric_summary_json) AS metric_match \
               WHERE metric_match.key = ",
        )
        .push_bind(key.to_owned())
        .push(" AND metric_match.value IS json_extract(")
        .push_bind(encoded)
        .push(", '$'))))");
    Ok(())
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

async fn update_metric_summary_preview(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: RunId,
    latest_values: &BTreeMap<String, f64>,
) -> Result<(), CatalogError> {
    let row = query(
        "SELECT metric_summary_json, metric_summary_truncated \
         FROM run_documents WHERE run_id = ?",
    )
    .bind(run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| CatalogError::NotFound {
        resource: format!("run documents for {run_id}"),
    })?;
    let mut preview = parse_document(
        row.get::<String, _>("metric_summary_json"),
        "metric summary",
    )?;
    let mut truncated: bool = row.get("metric_summary_truncated");
    preview.extend(
        latest_values
            .iter()
            .map(|(key, value)| (key.clone(), Value::from(*value))),
    );
    while preview.len() > MAX_DERIVED_SUMMARY_KEYS {
        let key = preview
            .keys()
            .next_back()
            .cloned()
            .ok_or_else(|| CatalogError::InvalidData("metric summary is empty".to_owned()))?;
        preview.remove(&key);
        truncated = true;
    }
    let encoded = serialize_document(&preview, "metric summary", MAX_SUMMARY_BYTES)?;
    query(
        "UPDATE run_documents SET metric_summary_json = ?, metric_summary_truncated = ? \
         WHERE run_id = ?",
    )
    .bind(encoded)
    .bind(truncated)
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
mod tests;
