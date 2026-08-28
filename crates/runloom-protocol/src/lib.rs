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
        HealthResponse, HealthStatus, HistoryResponse, ProjectId, ResumePolicy, RunId, RunState,
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
        Ok(())
    }
}
