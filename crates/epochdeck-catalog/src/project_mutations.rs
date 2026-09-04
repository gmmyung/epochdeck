use std::fmt::Write;

struct MutationTable {
    name: &'static str,
    inserted_project: &'static str,
    updated_project: &'static str,
    deleted_project: &'static str,
}

const TABLES: &[MutationTable] = &[
    MutationTable {
        name: "runs",
        inserted_project: "NEW.project_id",
        updated_project: "NEW.project_id",
        deleted_project: "OLD.project_id",
    },
    MutationTable {
        name: "run_documents",
        inserted_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        updated_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        deleted_project: "(SELECT project_id FROM runs WHERE id = OLD.run_id)",
    },
    MutationTable {
        name: "ingest_batches",
        inserted_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        updated_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        deleted_project: "(SELECT project_id FROM runs WHERE id = OLD.run_id)",
    },
    MutationTable {
        name: "run_alerts",
        inserted_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        updated_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        deleted_project: "(SELECT project_id FROM runs WHERE id = OLD.run_id)",
    },
    MutationTable {
        name: "run_rich_values",
        inserted_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        updated_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        deleted_project: "(SELECT project_id FROM runs WHERE id = OLD.run_id)",
    },
    MutationTable {
        name: "artifact_versions",
        inserted_project: "NEW.project_id",
        updated_project: "NEW.project_id",
        deleted_project: "OLD.project_id",
    },
    MutationTable {
        name: "artifact_aliases",
        inserted_project: "NEW.project_id",
        updated_project: "NEW.project_id",
        deleted_project: "OLD.project_id",
    },
    MutationTable {
        name: "artifact_lineage",
        inserted_project: "(SELECT project_id FROM artifact_versions WHERE id = NEW.artifact_id)",
        updated_project: "(SELECT project_id FROM artifact_versions WHERE id = NEW.artifact_id)",
        deleted_project: "(SELECT project_id FROM artifact_versions WHERE id = OLD.artifact_id)",
    },
    MutationTable {
        name: "trace_spans",
        inserted_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        updated_project: "(SELECT project_id FROM runs WHERE id = NEW.run_id)",
        deleted_project: "(SELECT project_id FROM runs WHERE id = OLD.run_id)",
    },
    MutationTable {
        name: "sweeps",
        inserted_project: "NEW.project_id",
        updated_project: "NEW.project_id",
        deleted_project: "OLD.project_id",
    },
    MutationTable {
        name: "sweep_trials",
        inserted_project: "(SELECT project_id FROM sweeps WHERE id = NEW.sweep_id)",
        updated_project: "(SELECT project_id FROM sweeps WHERE id = NEW.sweep_id)",
        deleted_project: "(SELECT project_id FROM sweeps WHERE id = OLD.sweep_id)",
    },
    MutationTable {
        name: "reports",
        inserted_project: "NEW.project_id",
        updated_project: "NEW.project_id",
        deleted_project: "OLD.project_id",
    },
];

pub(super) fn trigger_schema() -> String {
    let mut schema = String::new();
    for table in TABLES {
        for (event, project) in [
            ("insert", table.inserted_project),
            ("update", table.updated_project),
            ("delete", table.deleted_project),
        ] {
            write!(
                schema,
                "CREATE TRIGGER IF NOT EXISTS project_mutation_{}_{event} \
                 AFTER {event} ON {} BEGIN \
                 UPDATE projects SET mutation_revision = mutation_revision + 1 \
                 WHERE id = {project}; END;",
                table.name, table.name
            )
            .expect("writing SQL into a String cannot fail");
        }
    }
    schema
}

#[cfg(test)]
mod tests {
    #[test]
    fn compaction_tables_are_not_project_mutation_sources() {
        let schema = super::trigger_schema();
        assert!(!schema.contains("metric_segments"));
        assert!(!schema.contains("retired_metric_segments"));
        for table in [
            "runs",
            "run_documents",
            "ingest_batches",
            "run_alerts",
            "run_rich_values",
            "artifact_versions",
            "artifact_aliases",
            "artifact_lineage",
            "trace_spans",
            "sweeps",
            "sweep_trials",
            "reports",
        ] {
            assert!(schema.contains(&format!("AFTER insert ON {table}")));
            assert!(schema.contains(&format!("AFTER update ON {table}")));
            assert!(schema.contains(&format!("AFTER delete ON {table}")));
        }
    }
}
