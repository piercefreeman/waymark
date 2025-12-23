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

/// Unique identifier for an action in the queue
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActionId(pub Uuid);

impl ActionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ActionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ActionId {
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

/// Status of an action in the queue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionStatus {
    Queued,
    Dispatched,
    Completed,
    Failed,
    TimedOut,
    /// Terminal status for actions that exhausted all retries due to failures
    Exhausted,
    /// Terminal status for actions whose exception was caught by a handler
    Caught,
}

impl ActionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatched => "dispatched",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Exhausted => "exhausted",
            Self::Caught => "caught",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "dispatched" => Some(Self::Dispatched),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "timed_out" => Some(Self::TimedOut),
            "exhausted" => Some(Self::Exhausted),
            "caught" => Some(Self::Caught),
            _ => None,
        }
    }
}

/// Type of retry being attempted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryKind {
    Failure,
    Timeout,
}

impl RetryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Failure => "failure",
            Self::Timeout => "timeout",
        }
    }
}

/// Backoff strategy for retries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffKind {
    None,
    Linear,
    Exponential,
}

impl BackoffKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Linear => "linear",
            Self::Exponential => "exponential",
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

#[derive(Debug, Clone)]
pub struct WorkerStatusUpdate {
    pub worker_id: i64,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
}

/// Worker throughput status for webapp reporting.
#[derive(Debug, Clone, FromRow)]
pub struct WorkerStatus {
    pub pool_id: Uuid,
    pub worker_id: i64,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
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
}

/// An action ready for dispatch (returned from dispatch_actions)
#[derive(Debug, Clone)]
pub struct QueuedAction {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub partition_id: i32,
    pub action_seq: i32,
    pub module_name: String,
    pub action_name: String,
    pub dispatch_payload: Vec<u8>,
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub attempt_number: i32,
    pub delivery_token: Uuid,
    pub timeout_retry_limit: i32,
    pub retry_kind: String,
    pub node_id: Option<String>,
    /// Type of node: "action" or "barrier"
    pub node_type: String,
    /// Result payload (set after completion)
    pub result_payload: Option<Vec<u8>>,
    /// Whether the action succeeded (set after completion)
    pub success: Option<bool>,
    /// Action status: pending, dispatched, completed, failed
    pub status: String,
    /// When the action is scheduled to run (for retries with backoff)
    pub scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Error message if the action or workflow failed
    pub last_error: Option<String>,
}

/// Record for completing an action
#[derive(Debug, Clone)]
pub struct CompletionRecord {
    pub action_id: ActionId,
    pub success: bool,
    pub result_payload: Vec<u8>,
    pub delivery_token: Uuid,
    pub error_message: Option<String>,
}

/// New action to enqueue
#[derive(Debug, Clone)]
pub struct NewAction {
    pub instance_id: WorkflowInstanceId,
    pub module_name: String,
    pub action_name: String,
    pub dispatch_payload: Vec<u8>,
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub backoff_kind: BackoffKind,
    pub backoff_base_delay_ms: i32,
    pub node_id: Option<String>,
    /// The type of node (e.g., "action", "for_loop", "barrier")
    pub node_type: Option<String>,
}

/// Loop iteration state
#[derive(Debug, Clone, FromRow)]
pub struct LoopState {
    pub instance_id: Uuid,
    pub loop_id: String,
    pub current_index: i32,
    pub accumulators: Option<Vec<u8>>,
    pub updated_at: DateTime<Utc>,
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

    #[error("Invalid delivery token")]
    InvalidDeliveryToken,
}

pub type DbResult<T> = Result<T, DbError>;

// ============================================================================
// Node Readiness Types
// ============================================================================

/// Result of incrementing node readiness.
#[derive(Debug)]
pub struct ReadinessResult {
    /// Current completed count after increment
    pub completed_count: i32,
    /// Required count to be ready
    pub required_count: i32,
    /// Whether this increment made the node ready
    pub is_now_ready: bool,
}

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
    fn test_status_roundtrip() {
        assert_eq!(
            ActionStatus::parse(ActionStatus::Queued.as_str()),
            Some(ActionStatus::Queued)
        );
        assert_eq!(
            ActionStatus::parse(ActionStatus::Dispatched.as_str()),
            Some(ActionStatus::Dispatched)
        );
        assert_eq!(
            ActionStatus::parse(ActionStatus::Completed.as_str()),
            Some(ActionStatus::Completed)
        );
        assert_eq!(
            ActionStatus::parse(ActionStatus::Failed.as_str()),
            Some(ActionStatus::Failed)
        );
        assert_eq!(
            ActionStatus::parse(ActionStatus::TimedOut.as_str()),
            Some(ActionStatus::TimedOut)
        );
        assert_eq!(
            ActionStatus::parse(ActionStatus::Exhausted.as_str()),
            Some(ActionStatus::Exhausted)
        );
    }

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
