//! Schedule types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScheduleId(pub Uuid);

impl ScheduleId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ScheduleId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Type of schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleType {
    Cron,
    Interval,
}

impl ScheduleType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Interval => "interval",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "cron" => Some(Self::Cron),
            "interval" => Some(Self::Interval),
            _ => None,
        }
    }
}

impl std::fmt::Display for ScheduleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Status of a workflow schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleStatus {
    Active,
    Paused,
    Deleted,
}

impl ScheduleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }
}

impl std::fmt::Display for ScheduleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A workflow schedule (recurring execution).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSchedule {
    pub id: Uuid,
    pub workflow_name: String,
    pub schedule_name: String,
    pub schedule_type: String,
    pub cron_expression: Option<String>,
    pub interval_seconds: Option<i64>,
    pub jitter_seconds: i64,
    pub input_payload: Option<Vec<u8>>,
    pub status: String,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_instance_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub priority: i32,
    pub allow_duplicate: bool,
}

impl WorkflowSchedule {
    /// Get the schedule type as an enum.
    pub fn schedule_type_enum(&self) -> Option<ScheduleType> {
        ScheduleType::parse(&self.schedule_type)
    }

    /// Get the status as an enum.
    pub fn status_enum(&self) -> Option<ScheduleStatus> {
        ScheduleStatus::parse(&self.status)
    }
}

/// Parameters for creating a schedule.
#[derive(Debug, Clone)]
pub struct CreateScheduleParams {
    pub workflow_name: String,
    pub schedule_name: String,
    pub schedule_type: ScheduleType,
    pub cron_expression: Option<String>,
    pub interval_seconds: Option<i64>,
    pub jitter_seconds: i64,
    pub input_payload: Option<Vec<u8>>,
    pub priority: i32,
    pub allow_duplicate: bool,
}
