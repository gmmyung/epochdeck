#![forbid(unsafe_code)]

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const API_VERSION: &str = "v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectId(Uuid);

impl ProjectId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for ProjectId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(Uuid);

impl RunId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

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

#[cfg(test)]
mod tests {
    use super::{HealthResponse, HealthStatus, ProjectId, RunId};

    #[test]
    fn identifiers_are_distinct() {
        assert_ne!(ProjectId::new().to_string(), ProjectId::new().to_string());
        assert_ne!(RunId::new().to_string(), RunId::new().to_string());
    }

    #[test]
    fn healthy_response_is_stable() {
        let response = HealthResponse::healthy("0.1.0");
        assert_eq!(response.service, "runloom");
        assert_eq!(response.status, HealthStatus::Healthy);
    }
}
