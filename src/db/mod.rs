//! Database layer for Rappel workflow execution.
//!
//! This module is split into two main components:
//! - `worker`: High-performance operations for the worker dispatch loop
//! - `webapp`: Read-heavy operations for the dashboard/webapp
//!
//! The split ensures that webapp features never accidentally impact
//! worker performance.
//!
//! # Connection
//!
//! Set the `RAPPEL_DATABASE_URL` environment variable to your PostgreSQL connection string:
//! ```text
//! RAPPEL_DATABASE_URL=postgresql://user:password@localhost:5432/rappel
//! ```

mod webapp;
mod worker;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
use thiserror::Error;

// Worker and webapp modules extend Database with impl blocks.
// No re-exports needed - methods are automatically available on Database.

// Re-export types from worker module
pub use worker::{
    ActionExecutionArchive, ArchiveRecord, ClaimedInstance, ExecutionGraphUpdate,
    InstanceFinalization, NodePayload,
};

// ============================================================================
// Type Aliases & Newtypes
// ============================================================================

/// Unique identifier for a workflow version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkflowVersionId(pub Uuid);

impl WorkflowVersionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkflowVersionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowVersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a workflow instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkflowInstanceId(pub Uuid);

impl WorkflowInstanceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkflowInstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

use uuid::Uuid;

// ============================================================================
// Status Enums
// ============================================================================

/// Status of a workflow instance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceStatus {
    Running,
    Completed,
    Failed,
}

impl InstanceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

// ============================================================================
// Model Structs
// ============================================================================

/// A workflow version (compiled program definition)
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowVersion {
    pub id: Uuid,
    pub workflow_name: String,
    pub dag_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
    pub created_at: DateTime<Utc>,
}

/// Summary of a workflow version (without the proto payload)
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowVersionSummary {
    pub id: Uuid,
    pub workflow_name: String,
    pub dag_hash: String,
    pub concurrent: bool,
    pub created_at: DateTime<Utc>,
}

/// Pool-level worker status update (one row per pool_id).
#[derive(Debug, Clone)]
pub struct WorkerStatusUpdate {
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
    /// Median time from enqueue to dispatch (dequeue latency) in milliseconds
    pub median_dequeue_ms: Option<i64>,
    /// Median execution time from dispatch to completion in milliseconds
    pub median_handling_ms: Option<i64>,
    /// Current size of the dispatch queue
    pub dispatch_queue_size: i64,
    /// Total in-flight actions across all workers
    pub total_in_flight: i64,
    /// Number of active Python worker processes
    pub active_workers: i32,
    /// Actions per second (throughput_per_min / 60)
    pub actions_per_sec: f64,
    /// Median instance duration in seconds (p50 of recently completed instances)
    pub median_instance_duration_secs: Option<f64>,
    /// Number of workflow instances currently owned by this runner
    pub active_instance_count: i32,
    /// Total instances completed (completed + failed) since runner start
    pub total_instances_completed: i64,
    /// Instances per second (rolling window throughput)
    pub instances_per_sec: f64,
    /// Instances per minute (rolling window throughput)
    pub instances_per_min: f64,
    /// Encoded time-series ring buffer (BYTEA)
    pub time_series: Option<Vec<u8>>,
}

/// Pool-level worker status for webapp reporting.
#[derive(Debug, Clone, FromRow)]
pub struct WorkerStatus {
    pub pool_id: Uuid,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    /// Median time from enqueue to dispatch (dequeue latency) in milliseconds
    pub median_dequeue_ms: Option<i64>,
    /// Median execution time from dispatch to completion in milliseconds
    pub median_handling_ms: Option<i64>,
    /// Current size of the dispatch queue
    pub dispatch_queue_size: Option<i64>,
    /// Total in-flight actions across all workers
    pub total_in_flight: Option<i64>,
    /// Number of active Python worker processes
    pub active_workers: i32,
    /// Actions per second
    pub actions_per_sec: f64,
    /// Median instance duration in seconds (p50 of recently completed instances)
    pub median_instance_duration_secs: Option<f64>,
    /// Number of workflow instances currently owned by this runner
    pub active_instance_count: i32,
    /// Total instances completed (completed + failed) since runner start
    pub total_instances_completed: i64,
    /// Instances per second (rolling window throughput)
    pub instances_per_sec: f64,
    /// Instances per minute (rolling window throughput)
    pub instances_per_min: f64,
    /// Encoded time-series ring buffer
    pub time_series: Option<Vec<u8>>,
}

/// A workflow instance (execution)
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowInstance {
    pub id: Uuid,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<Uuid>,
    pub schedule_id: Option<Uuid>,
    pub next_action_seq: i32,
    pub input_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    /// Priority for queue ordering (higher values are processed first)
    pub priority: i32,
}

/// Action execution log entry (view model for UI display).
/// Synthesized from ExecutionGraph data.
#[derive(Debug, Clone)]
pub struct ActionLog {
    pub id: Uuid,
    pub action_id: Uuid,
    pub instance_id: Uuid,
    pub attempt_number: i32,
    pub dispatched_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub success: Option<bool>,
    pub result_payload: Option<Vec<u8>>,
    pub error_message: Option<String>,
    pub duration_ms: Option<i64>,
    pub pool_id: Option<Uuid>,
    pub worker_id: Option<i64>,
    pub enqueued_at: Option<DateTime<Utc>>,
    pub module_name: Option<String>,
    pub action_name: Option<String>,
    pub node_id: Option<String>,
    pub dispatch_payload: Option<Vec<u8>>,
    /// Node kind (matches NodeKind proto enum) - used to identify sleep nodes etc.
    pub node_kind: i32,
}

// ============================================================================
// Workflow Schedules
// ============================================================================

/// Unique identifier for a workflow schedule
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Type of schedule
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Status of a workflow schedule
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// A workflow schedule (recurring execution)
#[derive(Debug, Clone, FromRow)]
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
    /// Priority for queue ordering (higher values are processed first)
    pub priority: i32,
    /// Whether duplicate instances are allowed when schedule fires
    pub allow_duplicate: bool,
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("Not found: {0}")]
    NotFound(String),
}

pub type DbResult<T> = Result<T, DbError>;

// ============================================================================
// Database
// ============================================================================

/// Main database handle
///
/// The Database struct provides all database operations. Operations are
/// organized into two categories:
///
/// - **Worker operations** (`db/worker.rs`): High-performance operations used
///   by the dispatch loop. These are hyper-optimized and should never be
///   slowed down by webapp features.
///
/// - **Webapp operations** (`db/webapp.rs`): Read-heavy operations used by
///   the dashboard. These can be slower and more feature-rich.
#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Connect to the database and run migrations
    pub async fn connect(database_url: &str) -> DbResult<Self> {
        Self::connect_with_pool_size(database_url, 10).await
    }

    /// Connect with a custom pool size
    pub async fn connect_with_pool_size(
        database_url: &str,
        max_connections: u32,
    ) -> DbResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;

        // Run migrations
        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_status_roundtrip() {
        assert_eq!(
            InstanceStatus::parse(InstanceStatus::Running.as_str()),
            Some(InstanceStatus::Running)
        );
        assert_eq!(
            InstanceStatus::parse(InstanceStatus::Completed.as_str()),
            Some(InstanceStatus::Completed)
        );
        assert_eq!(
            InstanceStatus::parse(InstanceStatus::Failed.as_str()),
            Some(InstanceStatus::Failed)
        );
    }

    #[test]
    fn test_id_display() {
        let id = WorkflowInstanceId::new();
        let s = id.to_string();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_schedule_type_roundtrip() {
        assert_eq!(
            ScheduleType::parse(ScheduleType::Cron.as_str()),
            Some(ScheduleType::Cron)
        );
        assert_eq!(
            ScheduleType::parse(ScheduleType::Interval.as_str()),
            Some(ScheduleType::Interval)
        );
        assert_eq!(ScheduleType::parse("invalid"), None);
    }

    #[test]
    fn test_schedule_status_roundtrip() {
        assert_eq!(
            ScheduleStatus::parse(ScheduleStatus::Active.as_str()),
            Some(ScheduleStatus::Active)
        );
        assert_eq!(
            ScheduleStatus::parse(ScheduleStatus::Paused.as_str()),
            Some(ScheduleStatus::Paused)
        );
        assert_eq!(
            ScheduleStatus::parse(ScheduleStatus::Deleted.as_str()),
            Some(ScheduleStatus::Deleted)
        );
        assert_eq!(ScheduleStatus::parse("invalid"), None);
    }

    #[test]
    fn test_schedule_id_display() {
        let id = ScheduleId::new();
        let s = id.to_string();
        assert!(!s.is_empty());
        // Verify it's a valid UUID format
        assert!(uuid::Uuid::parse_str(&s).is_ok());
    }
}
