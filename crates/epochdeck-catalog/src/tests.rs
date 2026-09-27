use std::collections::BTreeMap;
use std::time::Duration;

use epochdeck_protocol::{
    AlertId, AlertLevel, ArtifactId, CreateAlertRequest, CreateArtifactRequest,
    CreateRichValueRequest, CreateRunRequest, MAX_DERIVED_SUMMARY_KEYS, MetricCatalogMode,
    ProjectMetricCatalogRequest, ResumePolicy, RichValueId, RichValueKind, RunId, RunQueryRequest,
    RunState,
};
use sqlx::Row;
use tempfile::tempdir;

use super::{BatchRegistration, Catalog, CatalogError, SegmentManifest};

#[tokio::test]
async fn discovery_indexes_cover_newest_first_pages() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    for (statement, expected_index) in [
        (
            "EXPLAIN QUERY PLAN SELECT id FROM projects \
             ORDER BY created_at DESC, id DESC LIMIT 10",
            "idx_projects_created",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM runs \
             ORDER BY created_at DESC, id DESC LIMIT 10",
            "idx_runs_created",
        ),
        (
            "EXPLAIN QUERY PLAN SELECT id FROM runs WHERE state = 'running' \
             ORDER BY created_at DESC, id DESC LIMIT 10",
            "idx_runs_state_created",
        ),
    ] {
        let plan = sqlx::query(statement)
            .fetch_all(&catalog.pool)
            .await?
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains(expected_index),
            "expected {expected_index} in query plan:\n{plan}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn project_metric_catalog_pages_selected_runs_without_history_scans()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let run_ids = [RunId::new(), RunId::new(), RunId::new()];
    for (index, run_id) in run_ids.iter().copied().enumerate() {
        catalog
            .create_or_resume_run(
                "metrics",
                &CreateRunRequest {
                    id: Some(run_id),
                    name: Some(format!("run-{index}")),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                },
            )
            .await?;
    }
    for (run_id, keys) in [
        (run_ids[0], &["loss", "reward"][..]),
        (run_ids[1], &["loss", "throughput"][..]),
    ] {
        for key in keys {
            sqlx::query("INSERT INTO run_metric_keys (run_id, key, latest_value) VALUES (?, ?, ?)")
                .bind(run_id.to_string())
                .bind(key)
                .bind(1.0)
                .execute(&catalog.pool)
                .await?;
        }
    }

    let union = catalog
        .project_metric_catalog(
            "metrics",
            &ProjectMetricCatalogRequest {
                run_ids: run_ids[..2].to_vec(),
                mode: MetricCatalogMode::Union,
                search: None,
                after: None,
                limit: 10,
            },
        )
        .await?;
    assert_eq!(
        union
            .keys
            .iter()
            .map(|summary| summary.key.as_str())
            .collect::<Vec<_>>(),
        vec!["loss", "reward", "throughput"]
    );
    assert_eq!(union.total_count, 3);
    let mut expected_loss_runs = run_ids[..2].to_vec();
    expected_loss_runs.sort_by_key(|run_id| run_id.to_string());
    assert_eq!(union.keys[0].run_ids, expected_loss_runs);

    let intersection = catalog
        .project_metric_catalog(
            "metrics",
            &ProjectMetricCatalogRequest {
                run_ids: run_ids[..2].to_vec(),
                mode: MetricCatalogMode::Intersection,
                search: None,
                after: None,
                limit: 10,
            },
        )
        .await?;
    assert_eq!(intersection.keys.len(), 1);
    assert_eq!(intersection.keys[0].key, "loss");
    assert_eq!(intersection.total_count, 1);

    let search = catalog
        .project_metric_catalog(
            "metrics",
            &ProjectMetricCatalogRequest {
                run_ids: run_ids[..2].to_vec(),
                mode: MetricCatalogMode::Union,
                search: Some("WARD".to_owned()),
                after: None,
                limit: 10,
            },
        )
        .await?;
    assert_eq!(search.keys.len(), 1);
    assert_eq!(search.keys[0].key, "reward");
    assert_eq!(search.total_count, 1);

    let after_loss = catalog
        .project_metric_catalog(
            "metrics",
            &ProjectMetricCatalogRequest {
                run_ids: run_ids[..2].to_vec(),
                mode: MetricCatalogMode::Union,
                search: None,
                after: Some("loss".to_owned()),
                limit: 1,
            },
        )
        .await?;
    assert_eq!(after_loss.keys[0].key, "reward");
    assert_eq!(after_loss.total_count, 3);
    assert!(matches!(
        catalog
            .project_metric_catalog(
                "metrics",
                &ProjectMetricCatalogRequest {
                    run_ids: run_ids[..2].to_vec(),
                    mode: MetricCatalogMode::Union,
                    search: None,
                    after: Some("missing".to_owned()),
                    limit: 10,
                },
            )
            .await,
        Err(CatalogError::NotFound { .. })
    ));

    let empty_intersection = catalog
        .project_metric_catalog(
            "metrics",
            &ProjectMetricCatalogRequest {
                run_ids: vec![run_ids[0], run_ids[2]],
                mode: MetricCatalogMode::Intersection,
                search: None,
                after: None,
                limit: 10,
            },
        )
        .await?;
    assert!(empty_intersection.keys.is_empty());
    assert_eq!(empty_intersection.total_count, 0);
    assert!(matches!(
        catalog
            .project_metric_catalog(
                "metrics",
                &ProjectMetricCatalogRequest {
                    run_ids: vec![run_ids[0], run_ids[0]],
                    mode: MetricCatalogMode::Union,
                    search: None,
                    after: None,
                    limit: 10,
                },
            )
            .await,
        Err(CatalogError::InvalidData(_))
    ));

    let (foreign, _) = catalog
        .create_or_resume_run(
            "elsewhere",
            &CreateRunRequest {
                id: None,
                name: Some("foreign".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    assert!(matches!(
        catalog
            .project_metric_catalog(
                "metrics",
                &ProjectMetricCatalogRequest {
                    run_ids: vec![run_ids[0], foreign.id],
                    mode: MetricCatalogMode::Union,
                    search: None,
                    after: None,
                    limit: 10,
                },
            )
            .await,
        Err(CatalogError::NotFound { .. })
    ));

    let exact_runs = catalog
        .query_runs(&RunQueryRequest {
            project: Some("metrics".to_owned()),
            run_ids: vec![run_ids[0], run_ids[2]],
            state: None,
            name: None,
            name_contains: None,
            config_equals: BTreeMap::new(),
            summary_equals: BTreeMap::new(),
            before: None,
            limit: 2,
        })
        .await?;
    assert_eq!(exact_runs.len(), 2);
    assert!(
        exact_runs
            .iter()
            .all(|run| run.id == run_ids[0] || run.id == run_ids[2])
    );
    Ok(())
}

#[tokio::test]
async fn derived_metric_summary_is_bounded_without_limiting_metric_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (run, _) = catalog
        .create_or_resume_run(
            "summary-preview",
            &CreateRunRequest {
                id: None,
                name: Some("wide-metrics".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    let total_keys = MAX_DERIVED_SUMMARY_KEYS * 5;
    let suffix = "x".repeat(238);
    let metric_key = |index: usize| format!("metric/{index:04}/{suffix}");

    for batch_index in 0..5usize {
        let start = batch_index * MAX_DERIVED_SUMMARY_KEYS;
        let latest = (start..start + MAX_DERIVED_SUMMARY_KEYS)
            .map(|index| (metric_key(index), index as f64))
            .collect::<BTreeMap<_, _>>();
        let sequence = batch_index as u64 + 1;
        catalog
            .register_batch(
                run.id,
                batch_index as u64,
                &format!("digest-{batch_index}"),
                &SegmentManifest {
                    id: format!("segment-{batch_index}"),
                    signature: format!("signature-{batch_index}"),
                    relative_path: format!("segment-{batch_index}.parquet"),
                    first_sequence: sequence,
                    last_sequence: sequence,
                    row_count: 1,
                    byte_size: 1,
                },
                &latest,
            )
            .await?;
    }

    let preview = catalog.get_run(run.id).await?;
    assert!(preview.summary_truncated);
    assert_eq!(preview.metric_summary.len(), MAX_DERIVED_SUMMARY_KEYS);
    assert_eq!(preview.summary, preview.metric_summary);
    assert_eq!(preview.metric_summary[&metric_key(0)], 0.0);
    assert_eq!(
        preview.metric_summary.keys().next_back(),
        Some(&metric_key(MAX_DERIVED_SUMMARY_KEYS - 1))
    );
    assert!(
        !preview
            .metric_summary
            .contains_key(&metric_key(total_keys - 1))
    );

    let mut cataloged_keys = Vec::new();
    let mut after = None;
    loop {
        let page = catalog
            .project_metric_catalog(
                "summary-preview",
                &ProjectMetricCatalogRequest {
                    run_ids: vec![run.id],
                    mode: MetricCatalogMode::Union,
                    search: None,
                    after: after.clone(),
                    limit: 200,
                },
            )
            .await?;
        if page.keys.is_empty() {
            break;
        }
        assert_eq!(page.total_count, total_keys);
        after = page.keys.last().map(|summary| summary.key.clone());
        let exhausted = page.keys.len() < 200;
        cataloged_keys.extend(page.keys.into_iter().map(|summary| summary.key));
        if exhausted {
            break;
        }
    }
    assert_eq!(cataloged_keys.len(), total_keys);
    assert_eq!(cataloged_keys.first(), Some(&metric_key(0)));
    assert_eq!(cataloged_keys.last(), Some(&metric_key(total_keys - 1)));

    let retained_key = metric_key(0);
    let sequence = 6;
    catalog
        .register_batch(
            run.id,
            5,
            "digest-5",
            &SegmentManifest {
                id: "segment-5".to_owned(),
                signature: "signature-5".to_owned(),
                relative_path: "segment-5.parquet".to_owned(),
                first_sequence: sequence,
                last_sequence: sequence,
                row_count: 1,
                byte_size: 1,
            },
            &BTreeMap::from([(retained_key.clone(), -1.0)]),
        )
        .await?;
    let explicit_payload = "e".repeat(240 * 1024);
    let explicit = catalog
        .update_summary(
            run.id,
            &BTreeMap::from([
                (retained_key.clone(), "manual".into()),
                (
                    "large_explicit_value".to_owned(),
                    explicit_payload.clone().into(),
                ),
            ]),
        )
        .await?;
    assert_eq!(explicit.metric_summary[&retained_key], -1.0);
    assert_eq!(explicit.explicit_summary[&retained_key], "manual");
    assert_eq!(explicit.summary[&retained_key], "manual");
    assert_eq!(explicit.summary["large_explicit_value"], explicit_payload);
    assert!(explicit.summary_truncated);

    let preview_match = catalog
        .query_runs(&RunQueryRequest {
            project: Some("summary-preview".to_owned()),
            run_ids: Vec::new(),
            state: None,
            name: None,
            name_contains: None,
            config_equals: BTreeMap::new(),
            summary_equals: BTreeMap::from([(metric_key(1), 1.0.into())]),
            before: None,
            limit: 10,
        })
        .await?;
    assert_eq!(preview_match.len(), 1);
    let explicit_match = catalog
        .query_runs(&RunQueryRequest {
            project: Some("summary-preview".to_owned()),
            run_ids: Vec::new(),
            state: None,
            name: None,
            name_contains: None,
            config_equals: BTreeMap::new(),
            summary_equals: BTreeMap::from([(retained_key.clone(), "manual".into())]),
            before: None,
            limit: 10,
        })
        .await?;
    assert_eq!(explicit_match.len(), 1);
    let shadowed_metric = catalog
        .query_runs(&RunQueryRequest {
            project: Some("summary-preview".to_owned()),
            run_ids: Vec::new(),
            state: None,
            name: None,
            name_contains: None,
            config_equals: BTreeMap::new(),
            summary_equals: BTreeMap::from([(retained_key, (-1.0).into())]),
            before: None,
            limit: 10,
        })
        .await?;
    assert!(shadowed_metric.is_empty());
    Ok(())
}

#[tokio::test]
async fn document_revision_tracks_real_mutations_independently_of_timestamps()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (created, _) = catalog
        .create_or_resume_run(
            "document-revisions",
            &CreateRunRequest {
                id: None,
                name: Some("same-second".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    sqlx::query(
        "CREATE TRIGGER preserve_test_run_timestamp AFTER UPDATE OF updated_at ON runs \
         BEGIN UPDATE runs SET updated_at = OLD.updated_at WHERE id = NEW.id; END",
    )
    .execute(&catalog.pool)
    .await?;

    let config = BTreeMap::from([("seed".to_owned(), 1.into())]);
    let first = catalog.update_config(created.id, &config, false).await?;
    assert_eq!(first.document_revision, 1);
    assert_eq!(first.updated_at, created.updated_at);
    let config_no_op = catalog.update_config(created.id, &config, false).await?;
    assert_eq!(config_no_op.document_revision, 1);

    let summary = BTreeMap::from([("result".to_owned(), "pending".into())]);
    let second = catalog.update_summary(created.id, &summary).await?;
    assert_eq!(second.document_revision, 2);
    assert_eq!(second.updated_at, created.updated_at);
    let summary_no_op = catalog.update_summary(created.id, &summary).await?;
    assert_eq!(summary_no_op.document_revision, 2);

    let finished = catalog.finish_run(created.id, &BTreeMap::new()).await?;
    assert_eq!(finished.document_revision, 3);
    assert_eq!(finished.updated_at, created.updated_at);
    let repeated = catalog.finish_run(created.id, &BTreeMap::new()).await?;
    assert_eq!(repeated.document_revision, 3);
    Ok(())
}

#[tokio::test]
async fn rich_resource_revision_changes_once_per_new_resource()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (run, _) = catalog
        .create_or_resume_run(
            "revisions",
            &CreateRunRequest {
                id: Some(RunId::new()),
                name: Some("resources".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    assert_eq!(run.rich_data_revision, 0);

    let alert = CreateAlertRequest {
        id: Some(AlertId::new()),
        title: "watch".to_owned(),
        text: "threshold".to_owned(),
        level: AlertLevel::Info,
        step: Some(1),
        timestamp_ms: 1,
    };
    assert!(!catalog.create_alert(run.id, &alert).await?.1);
    assert!(catalog.create_alert(run.id, &alert).await?.1);
    assert_eq!(catalog.get_run(run.id).await?.rich_data_revision, 1);

    let rich = CreateRichValueRequest {
        id: Some(RichValueId::new()),
        key: "media/video".to_owned(),
        kind: RichValueKind::Video,
        step: 2,
        timestamp_ms: 2,
        blob: None,
        metadata: BTreeMap::new(),
    };
    assert!(!catalog.create_rich_value(run.id, &rich).await?.1);
    assert!(catalog.create_rich_value(run.id, &rich).await?.1);
    assert_eq!(catalog.get_run(run.id).await?.rich_data_revision, 2);

    let artifact = CreateArtifactRequest {
        id: Some(ArtifactId::new()),
        name: "checkpoint".to_owned(),
        artifact_type: "model".to_owned(),
        version: None,
        description: None,
        metadata: BTreeMap::new(),
        aliases: vec!["latest".to_owned()],
        entries: Vec::new(),
    };
    let (created_artifact, duplicate) = catalog.create_artifact(run.id, &artifact).await?;
    assert!(!duplicate);
    assert!(catalog.create_artifact(run.id, &artifact).await?.1);
    assert_eq!(catalog.get_run(run.id).await?.rich_data_revision, 3);
    catalog.use_artifact(run.id, created_artifact.id).await?;
    catalog.use_artifact(run.id, created_artifact.id).await?;
    assert_eq!(catalog.get_run(run.id).await?.rich_data_revision, 4);

    Ok(())
}

#[tokio::test]
async fn alert_pages_use_chronology_safe_keysets() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (run, _) = catalog
        .create_or_resume_run(
            "chronology",
            &CreateRunRequest {
                id: None,
                name: Some("alerts".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    let older_alert: AlertId = "ffffffff-ffff-4fff-bfff-ffffffffffff".parse()?;
    let newer_alert: AlertId = "00000000-0000-4000-8000-000000000001".parse()?;
    for (id, timestamp_ms) in [(older_alert, 1), (newer_alert, 2)] {
        catalog
            .create_alert(
                run.id,
                &CreateAlertRequest {
                    id: Some(id),
                    title: "ordered".to_owned(),
                    text: "ordered".to_owned(),
                    level: AlertLevel::Info,
                    step: None,
                    timestamp_ms,
                },
            )
            .await?;
    }
    assert_eq!(
        catalog.list_alerts(run.id, None, 1).await?[0].id,
        newer_alert
    );
    assert_eq!(
        catalog.list_alerts(run.id, Some(newer_alert), 1).await?[0].id,
        older_alert
    );
    Ok(())
}

#[tokio::test]
async fn discovery_keysets_follow_chronology_for_caller_supplied_ids()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let older_id: RunId = "ffffffff-ffff-4fff-bfff-ffffffffffff".parse()?;
    let newer_id: RunId = "00000000-0000-4000-8000-000000000001".parse()?;
    for (id, name) in [(older_id, "older"), (newer_id, "newer")] {
        catalog
            .create_or_resume_run(
                "chronology",
                &CreateRunRequest {
                    id: Some(id),
                    name: Some(name.to_owned()),
                    config: BTreeMap::new(),
                    resume: ResumePolicy::Never,
                },
            )
            .await?;
    }
    sqlx::query("UPDATE runs SET created_at = ? WHERE id = ?")
        .bind("2025-01-01 00:00:00")
        .bind(older_id.to_string())
        .execute(&catalog.pool)
        .await?;
    sqlx::query("UPDATE runs SET created_at = ? WHERE id = ?")
        .bind("2025-01-02 00:00:00")
        .bind(newer_id.to_string())
        .execute(&catalog.pool)
        .await?;

    let first_page = catalog.list_runs("chronology", None, None, 1).await?;
    assert_eq!(
        first_page.iter().map(|run| run.id).collect::<Vec<_>>(),
        vec![newer_id]
    );
    let second_page = catalog
        .list_runs("chronology", Some(newer_id), None, 1)
        .await?;
    assert_eq!(
        second_page.iter().map(|run| run.id).collect::<Vec<_>>(),
        vec![older_id]
    );

    let foreign_id: RunId = "11111111-1111-4111-8111-111111111111".parse()?;
    catalog
        .create_or_resume_run(
            "elsewhere",
            &CreateRunRequest {
                id: Some(foreign_id),
                name: Some("foreign".to_owned()),
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    assert!(matches!(
        catalog
            .list_runs("chronology", Some(foreign_id), None, 1)
            .await,
        Err(CatalogError::NotFound { .. })
    ));
    assert!(matches!(
        catalog
            .query_runs(&RunQueryRequest {
                project: Some("chronology".to_owned()),
                run_ids: Vec::new(),
                state: None,
                name: Some("older".to_owned()),
                name_contains: None,
                config_equals: BTreeMap::new(),
                summary_equals: BTreeMap::new(),
                before: Some(newer_id),
                limit: 1,
            })
            .await,
        Err(CatalogError::NotFound { .. })
    ));

    let older_value_id: RichValueId = "eeeeeeee-eeee-4eee-aeee-eeeeeeeeeeee".parse()?;
    let newer_value_id: RichValueId = "22222222-2222-4222-8222-222222222222".parse()?;
    for (id, step) in [(older_value_id, 1), (newer_value_id, 2)] {
        catalog
            .create_rich_value(
                newer_id,
                &CreateRichValueRequest {
                    id: Some(id),
                    key: "train/histogram".to_owned(),
                    kind: RichValueKind::Histogram,
                    step,
                    timestamp_ms: step as i64,
                    blob: None,
                    metadata: BTreeMap::new(),
                },
            )
            .await?;
    }
    sqlx::query("UPDATE run_rich_values SET created_at = ? WHERE id = ?")
        .bind("2025-01-01 00:00:00")
        .bind(older_value_id.to_string())
        .execute(&catalog.pool)
        .await?;
    sqlx::query("UPDATE run_rich_values SET created_at = ? WHERE id = ?")
        .bind("2025-01-02 00:00:00")
        .bind(newer_value_id.to_string())
        .execute(&catalog.pool)
        .await?;
    let keys = catalog.list_rich_value_keys(newer_id, None, 10).await?;
    assert_eq!(keys[0].latest.id, newer_value_id);
    assert_eq!(keys[0].count, 2);
    let values = catalog
        .list_rich_values(newer_id, "train/histogram", None, 1)
        .await?;
    assert_eq!(values[0].id, newer_value_id);
    let values = catalog
        .list_rich_values(newer_id, "train/histogram", Some(newer_value_id), 1)
        .await?;
    assert_eq!(values[0].id, older_value_id);
    Ok(())
}

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
    assert_eq!(catalog.get_project("robotics").await?.run_count, 1);

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
async fn project_mutation_token_detects_create_delete_aba() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (run, _) = catalog
        .create_or_resume_run(
            "project-generation",
            &CreateRunRequest {
                id: None,
                name: None,
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    let before = catalog
        .get_project("project-generation")
        .await?
        .mutation_token
        .parse::<u64>()?;
    let alert_id = "11111111-1111-4111-8111-111111111111";
    sqlx::query(
        "INSERT INTO run_alerts \
         (id, run_id, title, text, level, step, timestamp_ms, created_at) \
         VALUES (?, ?, 'transient', 'transient', 'info', NULL, 0, current_timestamp)",
    )
    .bind(alert_id)
    .bind(run.id.to_string())
    .execute(&catalog.pool)
    .await?;
    let after_create = catalog
        .get_project("project-generation")
        .await?
        .mutation_token
        .parse::<u64>()?;
    assert!(after_create > before);

    sqlx::query("DELETE FROM run_alerts WHERE id = ?")
        .bind(alert_id)
        .execute(&catalog.pool)
        .await?;
    let after_delete = catalog
        .get_project("project-generation")
        .await?
        .mutation_token
        .parse::<u64>()?;
    assert!(after_delete > after_create);
    assert_eq!(
        catalog.list_projects(None, 10).await?[0].mutation_token,
        after_delete.to_string()
    );
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
    for batch in 0..4u64 {
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
    let project_token_before = catalog
        .get_project("robotics")
        .await?
        .mutation_token
        .parse::<u64>()?;
    let candidate = catalog
        .next_compaction_candidate(16, 16)
        .await?
        .ok_or("missing compaction candidate")?;
    assert_eq!(candidate.run_id, run.id);
    assert_eq!(candidate.segments.len(), 4);
    let replacement = SegmentManifest {
        id: "compacted".to_owned(),
        signature: "shared-schema".to_owned(),
        relative_path: "compacted.parquet".to_owned(),
        first_sequence: 1,
        last_sequence: 8,
        row_count: 8,
        byte_size: 180,
    };
    let retired = catalog
        .replace_compacted_segments(run.id, &candidate.segments, &replacement)
        .await?;

    assert_eq!(retired.len(), 4);
    let active = catalog.list_segments(run.id, None).await?;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, "compacted");
    assert_eq!(
        catalog.get_run(run.id).await?.metric_revision,
        revision_before
    );
    assert_eq!(
        catalog
            .get_project("robotics")
            .await?
            .mutation_token
            .parse::<u64>()?,
        project_token_before
    );
    assert_eq!(catalog.retired_segments(16).await?, retired);
    drop(catalog);
    let catalog = Catalog::open(catalog_path).await?;
    assert_eq!(catalog.retired_segments(16).await?, retired);
    catalog.acknowledge_retired_segments(&retired).await?;
    assert!(catalog.retired_segments(16).await?.is_empty());
    assert_eq!(
        catalog
            .get_project("robotics")
            .await?
            .mutation_token
            .parse::<u64>()?,
        project_token_before
    );
    Ok(())
}

#[tokio::test]
async fn compaction_and_ingest_wait_for_the_bounded_sqlite_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (run, _) = catalog
        .create_or_resume_run(
            "writer-concurrency",
            &CreateRunRequest {
                id: None,
                name: None,
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    for batch in 0..2u64 {
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
    let sources = catalog.list_segments(run.id, None).await?;
    let replacement = SegmentManifest {
        id: "compacted-concurrent".to_owned(),
        signature: "shared-schema".to_owned(),
        relative_path: "compacted-concurrent.parquet".to_owned(),
        first_sequence: 1,
        last_sequence: 4,
        row_count: 4,
        byte_size: 180,
    };
    let appended = SegmentManifest {
        id: "segment-2".to_owned(),
        signature: "shared-schema".to_owned(),
        relative_path: "segment-2.parquet".to_owned(),
        first_sequence: 5,
        last_sequence: 6,
        row_count: 2,
        byte_size: 100,
    };

    let mut blocker = catalog.pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query("UPDATE projects SET mutation_revision = mutation_revision + 1 WHERE name = ?")
        .bind("writer-concurrency")
        .execute(&mut *blocker)
        .await?;

    let ingest_catalog = catalog.clone();
    let ingest = tokio::spawn(async move {
        ingest_catalog
            .register_batch(run.id, 2, "digest-2", &appended, &BTreeMap::new())
            .await
    });
    let compaction_catalog = catalog.clone();
    let compaction = tokio::spawn(async move {
        compaction_catalog
            .replace_compacted_segments(run.id, &sources, &replacement)
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if catalog.pool.size() >= 3 && catalog.pool.num_idle() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!ingest.is_finished());
    assert!(!compaction.is_finished());
    blocker.commit().await?;

    let (ingest, retired) = tokio::time::timeout(Duration::from_secs(2), async {
        let (ingest, retired) = tokio::join!(ingest, compaction);
        Ok::<_, Box<dyn std::error::Error>>((ingest??, retired??))
    })
    .await??;
    assert!(matches!(ingest, BatchRegistration::Accepted { .. }));
    assert_eq!(retired.len(), 2);
    let active = catalog.list_segments(run.id, None).await?;
    assert_eq!(active.len(), 2);
    assert_eq!(active[0].first_sequence, 1);
    assert_eq!(active[1].first_sequence, 5);
    Ok(())
}

#[tokio::test]
async fn sqlite_busy_codes_are_classified_as_retryable_catalog_load()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let blocker = catalog.pool.begin_with("BEGIN IMMEDIATE").await?;
    let mut contender = catalog.pool.acquire().await?;
    sqlx::query("PRAGMA busy_timeout = 0")
        .execute(&mut *contender)
        .await?;
    let error = sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *contender)
        .await
        .expect_err("the reserved SQLite writer must reject a competing writer");
    assert!(matches!(CatalogError::from(error), CatalogError::Busy(_)));
    blocker.rollback().await?;
    Ok(())
}

#[tokio::test]
async fn tiered_compaction_bounds_live_ingest_write_amplification()
-> Result<(), Box<dyn std::error::Error>> {
    const BATCHES: u64 = 64;
    const ROWS_PER_BATCH: u64 = 100;
    const TARGET_ROWS: usize = 16 * 1_024;
    const MAX_INPUT_SEGMENTS: usize = 16;

    let directory = tempdir()?;
    let catalog = Catalog::open(directory.path().join("catalog.sqlite3")).await?;
    let (run, _) = catalog
        .create_or_resume_run(
            "tiered-compaction",
            &CreateRunRequest {
                id: None,
                name: None,
                config: BTreeMap::new(),
                resume: ResumePolicy::Never,
            },
        )
        .await?;
    let mut compacted_rows = 0usize;
    let mut compactions = 0usize;
    let mut maximum_active_segments = 0usize;

    for batch in 0..BATCHES {
        let first_sequence = batch * ROWS_PER_BATCH + 1;
        let last_sequence = first_sequence + ROWS_PER_BATCH - 1;
        catalog
            .register_batch(
                run.id,
                batch,
                &format!("digest-{batch}"),
                &SegmentManifest {
                    id: format!("ingest-{batch}"),
                    signature: "shared-schema".to_owned(),
                    relative_path: format!("ingest-{batch}.parquet"),
                    first_sequence,
                    last_sequence,
                    row_count: ROWS_PER_BATCH as usize,
                    byte_size: ROWS_PER_BATCH,
                },
                &BTreeMap::new(),
            )
            .await?;

        while let Some(candidate) = catalog
            .next_compaction_candidate(TARGET_ROWS, MAX_INPUT_SEGMENTS)
            .await?
        {
            let row_count = candidate
                .segments
                .iter()
                .map(|segment| segment.row_count)
                .sum::<usize>();
            compacted_rows = compacted_rows
                .checked_add(row_count)
                .ok_or("compaction accounting overflow")?;
            compactions += 1;
            let first = candidate.segments.first().ok_or("missing first segment")?;
            let last = candidate.segments.last().ok_or("missing last segment")?;
            let replacement = SegmentManifest {
                id: format!("compacted-{compactions}"),
                signature: first.signature.clone(),
                relative_path: format!("compacted-{compactions}.parquet"),
                first_sequence: first.first_sequence,
                last_sequence: last.last_sequence,
                row_count,
                byte_size: row_count as u64,
            };
            catalog
                .replace_compacted_segments(run.id, &candidate.segments, &replacement)
                .await?;
        }
        maximum_active_segments =
            maximum_active_segments.max(catalog.list_segments(run.id, None).await?.len());
    }

    let total_rows = (BATCHES * ROWS_PER_BATCH) as usize;
    assert!(compacted_rows <= total_rows * 3);
    assert!(compactions <= 21);
    assert!(maximum_active_segments <= 12);
    let active = catalog.list_segments(run.id, None).await?;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].row_count, total_rows);
    Ok(())
}
