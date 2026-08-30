use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use runloom_protocol::{
    MetricCatalogMode, ProjectMetricCatalogRequest, ProjectMetricKeySummary, RunId,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, query};

use crate::CatalogError;

pub(crate) async fn project_metric_catalog(
    pool: &SqlitePool,
    project: &str,
    request: &ProjectMetricCatalogRequest,
) -> Result<Vec<ProjectMetricKeySummary>, CatalogError> {
    let project_exists: bool = query("SELECT EXISTS(SELECT 1 FROM projects WHERE name = ?)")
        .bind(project)
        .fetch_one(pool)
        .await?
        .get(0);
    if !project_exists {
        return Err(CatalogError::NotFound {
            resource: format!("project {project}"),
        });
    }
    if request.run_ids.is_empty() {
        return Err(CatalogError::InvalidData(
            "metric catalog requires at least one run ID".to_owned(),
        ));
    }
    let run_ids = request
        .run_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    if run_ids.len() != request.run_ids.len() {
        return Err(CatalogError::InvalidData(
            "metric catalog run IDs must be unique".to_owned(),
        ));
    }

    let mut ownership = QueryBuilder::<Sqlite>::new(
        "SELECT COUNT(*) AS matched FROM runs r JOIN projects p ON p.id = r.project_id \
         WHERE p.name = ",
    );
    ownership.push_bind(project).push(" AND r.id IN (");
    {
        let mut separated = ownership.separated(", ");
        for run_id in &run_ids {
            separated.push_bind(run_id);
        }
    }
    ownership.push(")");
    let matched: i64 = ownership.build().fetch_one(pool).await?.get("matched");
    if usize::try_from(matched).ok() != Some(run_ids.len()) {
        return Err(CatalogError::NotFound {
            resource: format!("one or more metric catalog runs in project {project}"),
        });
    }

    if let Some(after) = &request.after {
        let mut cursor_query =
            QueryBuilder::<Sqlite>::new("SELECT m.key FROM run_metric_keys m WHERE m.run_id IN (");
        {
            let mut separated = cursor_query.separated(", ");
            for run_id in &run_ids {
                separated.push_bind(run_id);
            }
        }
        cursor_query.push(") AND m.key = ").push_bind(after);
        push_search(&mut cursor_query, request.search.as_deref());
        cursor_query.push(" GROUP BY m.key");
        push_mode_having(&mut cursor_query, request.mode, run_ids.len())?;
        if cursor_query.build().fetch_optional(pool).await?.is_none() {
            return Err(CatalogError::NotFound {
                resource: format!("metric catalog cursor {after:?} for project {project}"),
            });
        }
    }

    let mut key_query =
        QueryBuilder::<Sqlite>::new("SELECT m.key FROM run_metric_keys m WHERE m.run_id IN (");
    {
        let mut separated = key_query.separated(", ");
        for run_id in &run_ids {
            separated.push_bind(run_id);
        }
    }
    key_query.push(")");
    push_search(&mut key_query, request.search.as_deref());
    if let Some(after) = &request.after {
        key_query.push(" AND m.key > ").push_bind(after);
    }
    key_query.push(" GROUP BY m.key");
    push_mode_having(&mut key_query, request.mode, run_ids.len())?;
    key_query
        .push(" ORDER BY m.key LIMIT ")
        .push_bind(bounded_i64(request.limit, "metric catalog limit")?);
    let keys = key_query
        .build()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("key"))
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut availability_query =
        QueryBuilder::<Sqlite>::new("SELECT key, run_id FROM run_metric_keys WHERE run_id IN (");
    {
        let mut separated = availability_query.separated(", ");
        for run_id in &run_ids {
            separated.push_bind(run_id);
        }
    }
    availability_query.push(") AND key IN (");
    {
        let mut separated = availability_query.separated(", ");
        for key in &keys {
            separated.push_bind(key);
        }
    }
    availability_query.push(") ORDER BY key, run_id");
    let mut availability = BTreeMap::<String, Vec<RunId>>::new();
    for row in availability_query.build().fetch_all(pool).await? {
        let encoded: String = row.get("run_id");
        let run_id = RunId::from_str(&encoded)
            .map_err(|error| CatalogError::InvalidData(format!("invalid run ID: {error}")))?;
        availability.entry(row.get("key")).or_default().push(run_id);
    }
    Ok(keys
        .into_iter()
        .map(|key| ProjectMetricKeySummary {
            run_ids: availability.remove(&key).unwrap_or_default(),
            key,
        })
        .collect())
}

fn push_search<'args>(query: &mut QueryBuilder<'args, Sqlite>, search: Option<&'args str>) {
    if let Some(search) = search {
        query
            .push(" AND instr(lower(m.key), lower(")
            .push_bind(search)
            .push(")) > 0");
    }
}

fn push_mode_having(
    query: &mut QueryBuilder<'_, Sqlite>,
    mode: MetricCatalogMode,
    run_count: usize,
) -> Result<(), CatalogError> {
    if mode == MetricCatalogMode::Intersection {
        query
            .push(" HAVING COUNT(DISTINCT m.run_id) = ")
            .push_bind(bounded_i64(run_count, "metric catalog run count")?);
    }
    Ok(())
}

fn bounded_i64(value: usize, name: &str) -> Result<i64, CatalogError> {
    i64::try_from(value).map_err(|_| CatalogError::InvalidData(format!("{name} is out of range")))
}
