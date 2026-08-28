#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const API_VERSION: &str = "v1";
pub const MAX_BATCH_POINTS: usize = 1_024;
pub const MAX_METRICS_PER_POINT: usize = 256;
pub const MAX_HISTORY_KEYS: usize = 32;
pub const MAX_HISTORY_POINTS: usize = 5_000;
pub const MAX_CONFIG_BYTES: usize = 256 * 1024;
pub const MAX_SUMMARY_BYTES: usize = 256 * 1024;
pub const MAX_ALERT_TITLE_BYTES: usize = 256;
pub const MAX_ALERT_TEXT_BYTES: usize = 4 * 1024;
pub const MAX_RICH_KEY_BYTES: usize = 256;
pub const MAX_RICH_METADATA_BYTES: usize = 256 * 1024;
pub const MAX_ARTIFACT_ENTRIES: usize = 4_096;
pub const MAX_ARTIFACT_MANIFEST_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_TRACE_METADATA_BYTES: usize = 256 * 1024;

macro_rules! uuid_identifier {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

uuid_identifier!(ProjectId);
uuid_identifier!(RunId);
uuid_identifier!(AlertId);
uuid_identifier!(RichValueId);
uuid_identifier!(ArtifactId);
uuid_identifier!(TraceSpanId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub service: String,
    pub version: String,
    pub status: HealthStatus,
}

impl HealthResponse {
    #[must_use]
    pub fn healthy(version: impl Into<String>) -> Self {
        Self {
            service: "runloom".to_owned(),
            version: version.into(),
            status: HealthStatus::Healthy,
        }
    }

    #[must_use]
    pub fn unhealthy(version: impl Into<String>) -> Self {
        Self {
            service: "runloom".to_owned(),
            version: version.into(),
            status: HealthStatus::Unhealthy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumePolicy {
    #[default]
    Never,
    Allow,
    Must,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Running,
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertLevel {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RichValueKind {
    Image,
    Audio,
    Video,
    Table,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRelation {
    Input,
    Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceKind {
    Span,
    Llm,
    Tool,
    Chain,
    Agent,
}

impl fmt::Display for TraceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Span => formatter.write_str("span"),
            Self::Llm => formatter.write_str("llm"),
            Self::Tool => formatter.write_str("tool"),
            Self::Chain => formatter.write_str("chain"),
            Self::Agent => formatter.write_str("agent"),
        }
    }
}

impl FromStr for TraceKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "span" => Ok(Self::Span),
            "llm" => Ok(Self::Llm),
            "tool" => Ok(Self::Tool),
            "chain" => Ok(Self::Chain),
            "agent" => Ok(Self::Agent),
            _ => Err("unknown trace kind"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceStatus {
    Unset,
    Ok,
    Error,
}

impl fmt::Display for TraceStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unset => formatter.write_str("unset"),
            Self::Ok => formatter.write_str("ok"),
            Self::Error => formatter.write_str("error"),
        }
    }
}

impl FromStr for TraceStatus {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unset" => Ok(Self::Unset),
            "ok" => Ok(Self::Ok),
            "error" => Ok(Self::Error),
            _ => Err("unknown trace status"),
        }
    }
}

impl fmt::Display for ArtifactRelation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input => formatter.write_str("input"),
            Self::Output => formatter.write_str("output"),
        }
    }
}

impl FromStr for ArtifactRelation {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "input" => Ok(Self::Input),
            "output" => Ok(Self::Output),
            _ => Err("unknown artifact relation"),
        }
    }
}

impl fmt::Display for RichValueKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image => formatter.write_str("image"),
            Self::Audio => formatter.write_str("audio"),
            Self::Video => formatter.write_str("video"),
            Self::Table => formatter.write_str("table"),
            Self::Histogram => formatter.write_str("histogram"),
        }
    }
}

impl FromStr for RichValueKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "image" => Ok(Self::Image),
            "audio" => Ok(Self::Audio),
            "video" => Ok(Self::Video),
            "table" => Ok(Self::Table),
            "histogram" => Ok(Self::Histogram),
            _ => Err("unknown rich value kind"),
        }
    }
}

impl fmt::Display for AlertLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => formatter.write_str("info"),
            Self::Warn => formatter.write_str("warn"),
            Self::Error => formatter.write_str("error"),
        }
    }
}

impl FromStr for AlertLevel {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err("unknown alert level"),
        }
    }
}

impl fmt::Display for RunState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => formatter.write_str("running"),
            Self::Finished => formatter.write_str("finished"),
        }
    }
}

impl FromStr for RunState {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "running" => Ok(Self::Running),
            "finished" => Ok(Self::Finished),
            _ => Err("unknown run state"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub id: ProjectId,
    pub name: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: ProjectId,
    pub name: String,
    pub created_at: String,
    pub run_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectListResponse {
    pub projects: Vec<ProjectSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: RunId,
    pub project_id: ProjectId,
    pub project: String,
    pub name: String,
    pub state: RunState,
    pub config: BTreeMap<String, Value>,
    pub summary: BTreeMap<String, Value>,
    pub metric_revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateRunRequest {
    #[serde(default)]
    pub id: Option<RunId>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub config: BTreeMap<String, Value>,
    #[serde(default)]
    pub resume: ResumePolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateRunResponse {
    pub run: RunRecord,
    pub resumed: bool,
    pub next_sequence: u64,
    pub next_step: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigUpdateRequest {
    #[serde(default)]
    pub updates: BTreeMap<String, Value>,
    #[serde(default)]
    pub allow_val_change: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryUpdateRequest {
    #[serde(default)]
    pub updates: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunUpdateResponse {
    pub run: RunRecord,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunListResponse {
    pub runs: Vec<RunRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunQueryRequest {
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub state: Option<RunState>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub name_contains: Option<String>,
    #[serde(default)]
    pub config_equals: BTreeMap<String, Value>,
    #[serde(default)]
    pub summary_equals: BTreeMap<String, Value>,
    #[serde(default)]
    pub before: Option<RunId>,
    #[serde(default = "default_query_limit")]
    pub limit: usize,
}

fn default_query_limit() -> usize {
    100
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunQueryResponse {
    pub runs: Vec<RunRecord>,
    pub next_before: Option<RunId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricKeyListResponse {
    pub run_id: RunId,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricPoint {
    pub sequence: u64,
    pub step: u64,
    pub timestamp_ms: i64,
    pub metrics: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestBatchRequest {
    pub batch_sequence: u64,
    pub points: Vec<MetricPoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestBatchResponse {
    pub run_id: RunId,
    pub batch_sequence: u64,
    pub accepted_points: usize,
    pub duplicate: bool,
    pub metric_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishRunRequest {
    #[serde(default)]
    pub summary: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishRunResponse {
    pub run: RunRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateAlertRequest {
    #[serde(default)]
    pub id: Option<AlertId>,
    pub title: String,
    #[serde(default)]
    pub text: String,
    pub level: AlertLevel,
    #[serde(default)]
    pub step: Option<u64>,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertRecord {
    pub id: AlertId,
    pub run_id: RunId,
    pub title: String,
    pub text: String,
    pub level: AlertLevel,
    pub step: Option<u64>,
    pub timestamp_ms: i64,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateAlertResponse {
    pub alert: AlertRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertListResponse {
    pub alerts: Vec<AlertRecord>,
    pub next_before: Option<AlertId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub digest: String,
    pub size: u64,
    pub mime_type: String,
    #[serde(default)]
    pub file_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobUploadResponse {
    pub blob: BlobRef,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateRichValueRequest {
    #[serde(default)]
    pub id: Option<RichValueId>,
    pub key: String,
    pub kind: RichValueKind,
    pub step: u64,
    pub timestamp_ms: i64,
    #[serde(default)]
    pub blob: Option<BlobRef>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RichValueRecord {
    pub id: RichValueId,
    pub run_id: RunId,
    pub key: String,
    pub kind: RichValueKind,
    pub step: u64,
    pub timestamp_ms: i64,
    pub blob: Option<BlobRef>,
    pub metadata: BTreeMap<String, Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateRichValueResponse {
    pub value: RichValueRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RichValueListResponse {
    pub values: Vec<RichValueRecord>,
    pub next_before: Option<RichValueId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEntry {
    pub path: String,
    pub blob: BlobRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateArtifactRequest {
    #[serde(default)]
    pub id: Option<ArtifactId>,
    pub name: String,
    #[serde(rename = "type")]
    pub artifact_type: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub entries: Vec<ArtifactEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub id: ArtifactId,
    pub project_id: ProjectId,
    pub project: String,
    pub name: String,
    #[serde(rename = "type")]
    pub artifact_type: String,
    pub version: u64,
    pub description: Option<String>,
    pub metadata: BTreeMap<String, Value>,
    pub aliases: Vec<String>,
    pub entries: Vec<ArtifactEntry>,
    pub created_by_run: RunId,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateArtifactResponse {
    pub artifact: ArtifactRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactListResponse {
    pub artifacts: Vec<ArtifactRecord>,
    pub next_before: Option<ArtifactId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UseArtifactRequest {
    pub artifact_id: ArtifactId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunArtifactRecord {
    pub artifact: ArtifactRecord,
    pub relation: ArtifactRelation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunArtifactListResponse {
    pub artifacts: Vec<RunArtifactRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactLineageResponse {
    pub artifact: ArtifactRecord,
    pub input_runs: Vec<RunId>,
    pub output_runs: Vec<RunId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTraceSpanRequest {
    #[serde(default)]
    pub id: Option<TraceSpanId>,
    pub trace_id: String,
    #[serde(default)]
    pub parent_span_id: Option<TraceSpanId>,
    pub name: String,
    pub kind: TraceKind,
    pub status: TraceStatus,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    #[serde(default)]
    pub step: Option<u64>,
    #[serde(default)]
    pub attributes: BTreeMap<String, Value>,
    #[serde(default)]
    pub preview: BTreeMap<String, Value>,
    #[serde(default)]
    pub payload: Option<BlobRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceSpanRecord {
    pub id: TraceSpanId,
    pub run_id: RunId,
    pub trace_id: String,
    pub parent_span_id: Option<TraceSpanId>,
    pub name: String,
    pub kind: TraceKind,
    pub status: TraceStatus,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub step: Option<u64>,
    pub attributes: BTreeMap<String, Value>,
    pub preview: BTreeMap<String, Value>,
    pub payload: Option<BlobRef>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTraceSpanResponse {
    pub span: TraceSpanRecord,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceSpanListResponse {
    pub spans: Vec<TraceSpanRecord>,
    pub next_before: Option<TraceSpanId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub run_id: RunId,
    pub sequence: Vec<u64>,
    pub step: Vec<u64>,
    pub timestamp_ms: Vec<i64>,
    pub metrics: BTreeMap<String, Vec<Option<f64>>>,
    pub next_after: Option<u64>,
    #[serde(default)]
    pub sampled: bool,
    #[serde(default)]
    pub source_points: Option<u64>,
    #[serde(default)]
    pub source_last_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}

impl ApiError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{
        AlertLevel, HealthResponse, HealthStatus, HistoryResponse, ProjectId, ResumePolicy, RunId,
        RunState,
    };

    #[test]
    fn identifiers_are_distinct_and_round_trip() -> Result<(), uuid::Error> {
        let project_id = ProjectId::new();
        assert_ne!(project_id.to_string(), ProjectId::new().to_string());
        assert_ne!(RunId::new().to_string(), RunId::new().to_string());
        assert_eq!(ProjectId::from_str(&project_id.to_string())?, project_id);
        Ok(())
    }

    #[test]
    fn healthy_response_is_stable() {
        let response = HealthResponse::healthy("0.1.0");
        assert_eq!(response.service, "runloom");
        assert_eq!(response.status, HealthStatus::Healthy);
    }

    #[test]
    fn public_enums_use_stable_wire_names() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&ResumePolicy::Must)?, "\"must\"");
        assert_eq!(serde_json::to_string(&RunState::Finished)?, "\"finished\"");
        assert_eq!(serde_json::to_string(&AlertLevel::Warn)?, "\"warn\"");
        Ok(())
    }

    #[test]
    fn history_sampling_metadata_is_backward_compatible() -> Result<(), serde_json::Error> {
        let response: HistoryResponse = serde_json::from_value(serde_json::json!({
            "run_id": RunId::new(),
            "sequence": [],
            "step": [],
            "timestamp_ms": [],
            "metrics": {},
            "next_after": null
        }))?;

        assert!(!response.sampled);
        assert_eq!(response.source_points, None);
        assert_eq!(response.source_last_sequence, None);
        Ok(())
    }
}
