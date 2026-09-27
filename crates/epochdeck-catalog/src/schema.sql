CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    run_count INTEGER NOT NULL DEFAULT 0,
    mutation_revision INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_projects_created
    ON projects(created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_project_created
    ON runs(project_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_runs_created
    ON runs(created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_runs_state_created
    ON runs(state, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS run_revisions (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    document_revision INTEGER NOT NULL DEFAULT 0,
    metric_revision INTEGER NOT NULL DEFAULT 0,
    rich_data_revision INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS run_documents (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    config_json TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    metric_summary_json TEXT NOT NULL DEFAULT '{}',
    metric_summary_truncated INTEGER NOT NULL DEFAULT 0,
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
    latest_value REAL NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_run_alerts_run_time
    ON run_alerts(run_id, timestamp_ms DESC, id DESC);

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

CREATE INDEX IF NOT EXISTS idx_run_rich_values_run_created
    ON run_rich_values(run_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_run_rich_values_run_key_created
    ON run_rich_values(run_id, key, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS run_rich_value_keys (
    run_id TEXT NOT NULL REFERENCES runs(id),
    key TEXT NOT NULL,
    value_count INTEGER NOT NULL,
    latest_value_id TEXT NOT NULL REFERENCES run_rich_values(id),
    PRIMARY KEY(run_id, key)
);

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

CREATE INDEX IF NOT EXISTS idx_artifact_versions_project_created
    ON artifact_versions(project_id, created_at DESC, id DESC);

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

CREATE INDEX IF NOT EXISTS idx_artifact_lineage_run_created
    ON artifact_lineage(run_id, created_at DESC, artifact_id DESC, relation DESC);

CREATE INDEX IF NOT EXISTS idx_artifact_lineage_artifact_created
    ON artifact_lineage(artifact_id, relation, created_at DESC, run_id DESC);
