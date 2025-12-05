//! Database layer for Rappel workflow execution.
//!
//! Uses PostgreSQL with sqlx for type-safe queries. Key features:
//! - Distributed work queue with `SKIP LOCKED` for non-blocking dispatch
//! - Workflow version caching
//! - Atomic action completion with delivery token validation
//!
//! # Connection
//!
//! Set the `DATABASE_URL` environment variable to your PostgreSQL connection string:
//! ```text
//! DATABASE_URL=postgresql://user:password@localhost:5432/rappel
//! ```

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

use crate::completion::{CompletionPlan, CompletionResult};

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
}

impl ActionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatched => "dispatched",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "dispatched" => Some(Self::Dispatched),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "timed_out" => Some(Self::TimedOut),
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

/// A workflow instance (execution)
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowInstance {
    pub id: Uuid,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<Uuid>,
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

    // ========================================================================
    // Workflow Versions
    // ========================================================================

    /// Create or update a workflow version
    /// Returns the version ID (existing if hash matches, new otherwise)
    pub async fn upsert_workflow_version(
        &self,
        workflow_name: &str,
        dag_hash: &str,
        program_proto: &[u8],
        concurrent: bool,
    ) -> DbResult<WorkflowVersionId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_versions (workflow_name, dag_hash, program_proto, concurrent)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (workflow_name, dag_hash) DO UPDATE SET workflow_name = EXCLUDED.workflow_name
            RETURNING id
            "#
        )
        .bind(workflow_name)
        .bind(dag_hash)
        .bind(program_proto)
        .bind(concurrent)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        Ok(WorkflowVersionId(id))
    }

    /// Load a workflow version by ID
    pub async fn get_workflow_version(&self, id: WorkflowVersionId) -> DbResult<WorkflowVersion> {
        let version = sqlx::query_as::<_, WorkflowVersion>(
            r#"
            SELECT id, workflow_name, dag_hash, program_proto, concurrent, created_at
            FROM workflow_versions
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("workflow version {}", id)))?;

        Ok(version)
    }

    /// List all workflow versions
    pub async fn list_workflow_versions(&self) -> DbResult<Vec<WorkflowVersionSummary>> {
        let versions = sqlx::query_as::<_, WorkflowVersionSummary>(
            r#"
            SELECT id, workflow_name, dag_hash, concurrent, created_at
            FROM workflow_versions
            ORDER BY created_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(versions)
    }

    // ========================================================================
    // Workflow Instances
    // ========================================================================

    /// Create a new workflow instance
    pub async fn create_instance(
        &self,
        workflow_name: &str,
        version_id: WorkflowVersionId,
        input_payload: Option<&[u8]>,
    ) -> DbResult<WorkflowInstanceId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_instances (workflow_name, workflow_version_id, input_payload)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(version_id.0)
        .bind(input_payload)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");

        // Initialize context
        sqlx::query(
            r#"
            INSERT INTO instance_context (instance_id)
            VALUES ($1)
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(WorkflowInstanceId(id))
    }

    /// Find instances that need to be started (running but no actions created yet)
    pub async fn find_unstarted_instances(&self, limit: i32) -> DbResult<Vec<WorkflowInstance>> {
        let instances = sqlx::query_as::<_, WorkflowInstance>(
            r#"
            SELECT i.id, i.partition_id, i.workflow_name, i.workflow_version_id,
                   i.next_action_seq, i.input_payload, i.result_payload, i.status,
                   i.created_at, i.completed_at
            FROM workflow_instances i
            WHERE i.status = 'running'
              AND i.next_action_seq = 0
              AND NOT EXISTS (
                  SELECT 1 FROM action_queue a WHERE a.instance_id = i.id
              )
            ORDER BY i.created_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(instances)
    }

    /// Get a workflow instance by ID
    pub async fn get_instance(&self, id: WorkflowInstanceId) -> DbResult<WorkflowInstance> {
        let instance = sqlx::query_as::<_, WorkflowInstance>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id,
                   next_action_seq, input_payload, result_payload, status,
                   created_at, completed_at
            FROM workflow_instances
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("workflow instance {}", id)))?;

        Ok(instance)
    }

    /// Mark an instance as completed
    pub async fn complete_instance(
        &self,
        id: WorkflowInstanceId,
        result_payload: Option<&[u8]>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'completed', result_payload = $2, completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .bind(result_payload)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark an instance as failed
    pub async fn fail_instance(&self, id: WorkflowInstanceId) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'failed', completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Action Queue
    // ========================================================================

    /// Enqueue a new action
    pub async fn enqueue_action(&self, action: NewAction) -> DbResult<ActionId> {
        // Get and increment sequence number atomically
        let row = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET next_action_seq = next_action_seq + 1
            WHERE id = $1
            RETURNING next_action_seq - 1
            "#,
        )
        .bind(action.instance_id.0)
        .fetch_one(&self.pool)
        .await?;

        let seq: i32 = row.get(0);

        let row = sqlx::query(
            r#"
            INSERT INTO action_queue (
                instance_id, action_seq, module_name, action_name,
                dispatch_payload, timeout_seconds, max_retries,
                backoff_kind, backoff_base_delay_ms, node_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
            "#,
        )
        .bind(action.instance_id.0)
        .bind(seq)
        .bind(&action.module_name)
        .bind(&action.action_name)
        .bind(&action.dispatch_payload)
        .bind(action.timeout_seconds)
        .bind(action.max_retries)
        .bind(action.backoff_kind.as_str())
        .bind(action.backoff_base_delay_ms)
        .bind(&action.node_id)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        Ok(ActionId(id))
    }

    /// Dispatch actions from the queue using SKIP LOCKED
    ///
    /// This is the core distributed queue operation. It atomically:
    /// 1. Selects up to `limit` queued actions that are ready
    /// 2. Locks them with FOR UPDATE SKIP LOCKED (non-blocking)
    /// 3. Updates their status to 'dispatched'
    /// 4. Sets deadline and delivery token
    /// 5. Returns the actions for execution
    pub async fn dispatch_actions(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let rows = sqlx::query(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM action_queue
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                ORDER BY scheduled_at, action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE action_queue aq
            SET status = 'dispatched',
                dispatched_at = NOW(),
                deadline_at = CASE
                    WHEN timeout_seconds > 0
                    THEN NOW() + (timeout_seconds || ' seconds')::interval
                    ELSE NULL
                END,
                delivery_token = gen_random_uuid()
            FROM next_actions
            WHERE aq.id = next_actions.id
            RETURNING
                aq.id,
                aq.instance_id,
                aq.partition_id,
                aq.action_seq,
                aq.module_name,
                aq.action_name,
                aq.dispatch_payload,
                aq.timeout_seconds,
                aq.max_retries,
                aq.attempt_number,
                aq.delivery_token,
                aq.timeout_retry_limit,
                aq.retry_kind,
                aq.node_id,
                COALESCE(aq.node_type, 'action') as node_type
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let actions = rows
            .into_iter()
            .map(|row| QueuedAction {
                id: row.get("id"),
                instance_id: row.get("instance_id"),
                partition_id: row.get("partition_id"),
                action_seq: row.get("action_seq"),
                module_name: row.get("module_name"),
                action_name: row.get("action_name"),
                dispatch_payload: row.get("dispatch_payload"),
                timeout_seconds: row.get("timeout_seconds"),
                max_retries: row.get("max_retries"),
                attempt_number: row.get("attempt_number"),
                delivery_token: row.get("delivery_token"),
                timeout_retry_limit: row.get("timeout_retry_limit"),
                retry_kind: row.get("retry_kind"),
                node_id: row.get("node_id"),
                node_type: row.get("node_type"),
            })
            .collect();

        Ok(actions)
    }

    /// Dispatch runnable nodes (both actions and barriers) from the queue.
    ///
    /// This is the unified dispatch operation for the readiness model.
    /// It atomically selects and marks nodes as dispatched.
    ///
    /// - Actions are dispatched to Python workers
    /// - Barriers are processed inline by the runner (aggregation)
    pub async fn dispatch_runnable_nodes(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        // Reuse dispatch_actions - it now handles both types via node_type column
        self.dispatch_actions(limit).await
    }

    /// Dispatch only action nodes (not barriers).
    /// Use this when you want to send work to external workers only.
    pub async fn dispatch_actions_only(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let rows = sqlx::query(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM action_queue
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                  AND (node_type = 'action' OR node_type IS NULL)
                ORDER BY scheduled_at, action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE action_queue aq
            SET status = 'dispatched',
                dispatched_at = NOW(),
                deadline_at = CASE
                    WHEN timeout_seconds > 0
                    THEN NOW() + (timeout_seconds || ' seconds')::interval
                    ELSE NULL
                END,
                delivery_token = gen_random_uuid()
            FROM next_actions
            WHERE aq.id = next_actions.id
            RETURNING
                aq.id,
                aq.instance_id,
                aq.partition_id,
                aq.action_seq,
                aq.module_name,
                aq.action_name,
                aq.dispatch_payload,
                aq.timeout_seconds,
                aq.max_retries,
                aq.attempt_number,
                aq.delivery_token,
                aq.timeout_retry_limit,
                aq.retry_kind,
                aq.node_id,
                COALESCE(aq.node_type, 'action') as node_type
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let actions = rows
            .into_iter()
            .map(|row| QueuedAction {
                id: row.get("id"),
                instance_id: row.get("instance_id"),
                partition_id: row.get("partition_id"),
                action_seq: row.get("action_seq"),
                module_name: row.get("module_name"),
                action_name: row.get("action_name"),
                dispatch_payload: row.get("dispatch_payload"),
                timeout_seconds: row.get("timeout_seconds"),
                max_retries: row.get("max_retries"),
                attempt_number: row.get("attempt_number"),
                delivery_token: row.get("delivery_token"),
                timeout_retry_limit: row.get("timeout_retry_limit"),
                retry_kind: row.get("retry_kind"),
                node_id: row.get("node_id"),
                node_type: row.get("node_type"),
            })
            .collect();

        Ok(actions)
    }

    /// Dispatch only barrier nodes.
    /// Use this when you want to process aggregators separately.
    pub async fn dispatch_barriers_only(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let rows = sqlx::query(
            r#"
            WITH next_barriers AS (
                SELECT id
                FROM action_queue
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                  AND node_type = 'barrier'
                ORDER BY scheduled_at, action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE action_queue aq
            SET status = 'dispatched',
                dispatched_at = NOW(),
                delivery_token = gen_random_uuid()
            FROM next_barriers
            WHERE aq.id = next_barriers.id
            RETURNING
                aq.id,
                aq.instance_id,
                aq.partition_id,
                aq.action_seq,
                aq.module_name,
                aq.action_name,
                aq.dispatch_payload,
                aq.timeout_seconds,
                aq.max_retries,
                aq.attempt_number,
                aq.delivery_token,
                aq.timeout_retry_limit,
                aq.retry_kind,
                aq.node_id,
                aq.node_type
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let actions = rows
            .into_iter()
            .map(|row| QueuedAction {
                id: row.get("id"),
                instance_id: row.get("instance_id"),
                partition_id: row.get("partition_id"),
                action_seq: row.get("action_seq"),
                module_name: row.get("module_name"),
                action_name: row.get("action_name"),
                dispatch_payload: row.get("dispatch_payload"),
                timeout_seconds: row.get("timeout_seconds"),
                max_retries: row.get("max_retries"),
                attempt_number: row.get("attempt_number"),
                delivery_token: row.get("delivery_token"),
                timeout_retry_limit: row.get("timeout_retry_limit"),
                retry_kind: row.get("retry_kind"),
                node_id: row.get("node_id"),
                node_type: row.get("node_type"),
            })
            .collect();

        Ok(actions)
    }

    /// Complete an action with its result
    ///
    /// Uses delivery_token for idempotent completion - if the token doesn't match,
    /// the action was already completed by another worker or timed out.
    pub async fn complete_action(&self, record: CompletionRecord) -> DbResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE action_queue
            SET status = CASE WHEN $2 THEN 'completed' ELSE 'failed' END,
                success = $2,
                result_payload = $3,
                last_error = $4,
                completed_at = NOW()
            WHERE id = $1 AND delivery_token = $5 AND status = 'dispatched'
            "#,
        )
        .bind(record.action_id.0)
        .bind(record.success)
        .bind(&record.result_payload)
        .bind(&record.error_message)
        .bind(record.delivery_token)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Find and mark timed-out actions for retry
    ///
    /// Returns the number of actions that were marked for retry
    pub async fn mark_timed_out_actions(&self, limit: i32) -> DbResult<i64> {
        let result = sqlx::query(
            r#"
            WITH overdue AS (
                SELECT id
                FROM action_queue
                WHERE status = 'dispatched'
                  AND deadline_at IS NOT NULL
                  AND deadline_at < NOW()
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE action_queue aq
            SET status = 'timed_out',
                retry_kind = 'timeout'
            FROM overdue
            WHERE aq.id = overdue.id
            "#,
        )
        .bind(limit)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Requeue failed or timed-out actions for retry
    ///
    /// Calculates backoff delay based on policy and schedules retry
    pub async fn requeue_failed_actions(&self, limit: i32) -> DbResult<i64> {
        let result = sqlx::query(
            r#"
            WITH retryable AS (
                SELECT id
                FROM action_queue
                WHERE (
                    (status = 'failed' AND retry_kind = 'failure' AND attempt_number < max_retries)
                    OR
                    (status = 'timed_out' AND retry_kind = 'timeout' AND attempt_number < timeout_retry_limit)
                )
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE action_queue aq
            SET status = 'queued',
                attempt_number = aq.attempt_number + 1,
                scheduled_at = NOW() + (
                    CASE aq.backoff_kind
                        WHEN 'linear' THEN (aq.backoff_base_delay_ms * (aq.attempt_number + 1))
                        WHEN 'exponential' THEN (aq.backoff_base_delay_ms * POWER(aq.backoff_multiplier, aq.attempt_number))
                        ELSE 0
                    END || ' milliseconds'
                )::interval,
                deadline_at = NULL,
                delivery_token = NULL
            FROM retryable
            WHERE aq.id = retryable.id
            "#
        )
        .bind(limit)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Get all actions for an instance
    pub async fn get_instance_actions(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> DbResult<Vec<QueuedAction>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                instance_id,
                partition_id,
                action_seq,
                module_name,
                action_name,
                dispatch_payload,
                timeout_seconds,
                max_retries,
                attempt_number,
                COALESCE(delivery_token, gen_random_uuid()) as delivery_token,
                timeout_retry_limit,
                retry_kind,
                node_id,
                COALESCE(node_type, 'action') as node_type
            FROM action_queue
            WHERE instance_id = $1
            ORDER BY action_seq
            "#,
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;

        let actions = rows
            .into_iter()
            .map(|row| QueuedAction {
                id: row.get("id"),
                instance_id: row.get("instance_id"),
                partition_id: row.get("partition_id"),
                action_seq: row.get("action_seq"),
                module_name: row.get("module_name"),
                action_name: row.get("action_name"),
                dispatch_payload: row.get("dispatch_payload"),
                timeout_seconds: row.get("timeout_seconds"),
                max_retries: row.get("max_retries"),
                attempt_number: row.get("attempt_number"),
                delivery_token: row.get("delivery_token"),
                timeout_retry_limit: row.get("timeout_retry_limit"),
                retry_kind: row.get("retry_kind"),
                node_id: row.get("node_id"),
                node_type: row.get("node_type"),
            })
            .collect();

        Ok(actions)
    }

    // ========================================================================
    // Loop State
    // ========================================================================

    /// Get or create loop state
    pub async fn get_or_create_loop_state(
        &self,
        instance_id: WorkflowInstanceId,
        loop_id: &str,
    ) -> DbResult<LoopState> {
        let state = sqlx::query_as::<_, LoopState>(
            r#"
            INSERT INTO loop_state (instance_id, loop_id)
            VALUES ($1, $2)
            ON CONFLICT (instance_id, loop_id) DO UPDATE SET loop_id = EXCLUDED.loop_id
            RETURNING instance_id, loop_id, current_index, accumulators, updated_at
            "#,
        )
        .bind(instance_id.0)
        .bind(loop_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(state)
    }

    /// Increment loop index and return new value
    pub async fn increment_loop_index(
        &self,
        instance_id: WorkflowInstanceId,
        loop_id: &str,
    ) -> DbResult<i32> {
        let row = sqlx::query(
            r#"
            UPDATE loop_state
            SET current_index = current_index + 1, updated_at = NOW()
            WHERE instance_id = $1 AND loop_id = $2
            RETURNING current_index
            "#,
        )
        .bind(instance_id.0)
        .bind(loop_id)
        .fetch_one(&self.pool)
        .await?;

        let new_index: i32 = row.get(0);
        Ok(new_index)
    }

    /// Update loop accumulators
    pub async fn update_loop_accumulators(
        &self,
        instance_id: WorkflowInstanceId,
        loop_id: &str,
        accumulators: &[u8],
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE loop_state
            SET accumulators = $3, updated_at = NOW()
            WHERE instance_id = $1 AND loop_id = $2
            "#,
        )
        .bind(instance_id.0)
        .bind(loop_id)
        .bind(accumulators)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Node Inputs (Inbox Pattern)
    // ========================================================================

    /// Append a value to a node's inbox (O(1) write, no locks).
    ///
    /// When Node A completes, it calls this for each downstream node that needs the value.
    /// This is append-only - no read-modify-write cycle.
    pub async fn append_to_inbox(
        &self,
        instance_id: WorkflowInstanceId,
        target_node_id: &str,
        variable_name: &str,
        value: serde_json::Value,
        source_node_id: &str,
        spread_index: Option<i32>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            INSERT INTO node_inputs (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(instance_id.0)
        .bind(target_node_id)
        .bind(variable_name)
        .bind(value)
        .bind(source_node_id)
        .bind(spread_index)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Read all pending inputs for a node (single query).
    ///
    /// Returns a map of variable_name -> value.
    /// For spread results, returns the values as a list ordered by spread_index.
    pub async fn read_inbox(
        &self,
        instance_id: WorkflowInstanceId,
        target_node_id: &str,
    ) -> DbResult<std::collections::HashMap<String, serde_json::Value>> {
        let rows: Vec<(String, serde_json::Value, Option<i32>)> = sqlx::query_as(
            r#"
            SELECT variable_name, value, spread_index
            FROM node_inputs
            WHERE instance_id = $1 AND target_node_id = $2
            ORDER BY spread_index NULLS FIRST, created_at
            "#,
        )
        .bind(instance_id.0)
        .bind(target_node_id)
        .fetch_all(&self.pool)
        .await?;

        let mut result = std::collections::HashMap::new();
        for (var_name, value, _spread_index) in rows {
            result.insert(var_name, value);
        }
        Ok(result)
    }

    /// Read spread/parallel results for an aggregator node.
    ///
    /// Returns list of (spread_index, value) tuples for ordering.
    pub async fn read_inbox_for_aggregator(
        &self,
        instance_id: WorkflowInstanceId,
        target_node_id: &str,
    ) -> DbResult<Vec<(i32, serde_json::Value)>> {
        let rows: Vec<(i32, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT spread_index, value
            FROM node_inputs
            WHERE instance_id = $1 AND target_node_id = $2 AND spread_index IS NOT NULL
            ORDER BY spread_index
            "#,
        )
        .bind(instance_id.0)
        .bind(target_node_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Clear inbox for an instance (used when resetting/restarting).
    pub async fn clear_inbox(&self, instance_id: WorkflowInstanceId) -> DbResult<()> {
        sqlx::query(
            r#"
            DELETE FROM node_inputs WHERE instance_id = $1
            "#,
        )
        .bind(instance_id.0)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Count completed actions for a set of node IDs within an instance.
    /// Used for parallel block synchronization.
    pub async fn count_completed_actions_for_nodes(
        &self,
        instance_id: WorkflowInstanceId,
        node_ids: &[&str],
    ) -> DbResult<usize> {
        let count: (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*)
            FROM action_queue
            WHERE instance_id = $1
              AND node_id = ANY($2)
              AND status = 'completed'
            "#,
        )
        .bind(instance_id.0)
        .bind(node_ids)
        .fetch_one(&self.pool)
        .await?;

        Ok(count.0 as usize)
    }

    // ========================================================================
    // Node Readiness Tracking (for aggregation patterns)
    // ========================================================================

    /// Initialize readiness tracking for a node that has multiple precursors.
    /// Called when we know a node needs to wait for N precursors to complete.
    /// This is idempotent - calling multiple times with the same node_id is safe.
    pub async fn init_node_readiness(
        &self,
        instance_id: WorkflowInstanceId,
        node_id: &str,
        required_count: i32,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)
            VALUES ($1, $2, $3, 0)
            ON CONFLICT (instance_id, node_id) DO NOTHING
            "#,
        )
        .bind(instance_id.0)
        .bind(node_id)
        .bind(required_count)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Atomically increment node readiness counter, initializing if needed.
    /// This is for cases where we know the required_count at completion time
    /// (e.g., parallel blocks where count is computed from DAG).
    ///
    /// Uses UPSERT: first call inserts with completed_count=1, subsequent calls increment.
    pub async fn increment_node_readiness(
        &self,
        instance_id: WorkflowInstanceId,
        node_id: &str,
        required_count: i32,
    ) -> DbResult<ReadinessResult> {
        let row = sqlx::query(
            r#"
            INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)
            VALUES ($1, $2, $3, 1)
            ON CONFLICT (instance_id, node_id)
            DO UPDATE SET completed_count = node_readiness.completed_count + 1,
                          updated_at = NOW()
            RETURNING completed_count, required_count
            "#,
        )
        .bind(instance_id.0)
        .bind(node_id)
        .bind(required_count)
        .fetch_one(&self.pool)
        .await?;

        let completed_count: i32 = row.get(0);
        let required_count: i32 = row.get(1);

        Ok(ReadinessResult {
            completed_count,
            required_count,
            is_now_ready: completed_count == required_count,
        })
    }

    /// Atomically write inbox entries and increment node readiness counter.
    /// This is for parallel blocks where multiple inbox writes need to be atomic.
    ///
    /// Returns the new counts and whether this action made the node ready.
    pub async fn write_inbox_batch_and_increment_readiness(
        &self,
        instance_id: WorkflowInstanceId,
        node_id: &str,
        required_count: i32,
        inbox_writes: &[(String, String, serde_json::Value, String, Option<i32>)],
    ) -> DbResult<ReadinessResult> {
        let mut tx = self.pool.begin().await?;

        // Write all inbox entries
        for (target_node_id, variable_name, value, source_node_id, spread_index) in inbox_writes {
            sqlx::query(
                r#"
                INSERT INTO node_inputs (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(instance_id.0)
            .bind(target_node_id)
            .bind(variable_name)
            .bind(value)
            .bind(source_node_id)
            .bind(spread_index)
            .execute(&mut *tx)
            .await?;
        }

        // Atomically increment readiness counter (init-on-first-use)
        let row = sqlx::query(
            r#"
            INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)
            VALUES ($1, $2, $3, 1)
            ON CONFLICT (instance_id, node_id)
            DO UPDATE SET completed_count = node_readiness.completed_count + 1,
                          updated_at = NOW()
            RETURNING completed_count, required_count
            "#,
        )
        .bind(instance_id.0)
        .bind(node_id)
        .bind(required_count)
        .fetch_one(&mut *tx)
        .await?;

        let completed_count: i32 = row.get(0);
        let required_count: i32 = row.get(1);

        tx.commit().await?;

        Ok(ReadinessResult {
            completed_count,
            required_count,
            is_now_ready: completed_count == required_count,
        })
    }

    /// Atomically write inbox entry and increment node readiness counter.
    /// Returns the new counts and whether this action made the node ready.
    ///
    /// This is the key atomic operation:
    /// 1. Write the inbox entry (result data)
    /// 2. Increment completed_count
    /// 3. Return whether completed_count == required_count
    ///
    /// Because this is transactional, when is_now_ready=true, the caller knows
    /// ALL precursor results are already in the inbox.
    ///
    /// NOTE: Requires node_readiness to be pre-initialized via init_node_readiness.
    pub async fn write_inbox_and_mark_precursor_complete(
        &self,
        instance_id: WorkflowInstanceId,
        downstream_node_id: &str,
        target_node_id: &str,
        variable_name: &str,
        value: serde_json::Value,
        source_node_id: &str,
        spread_index: Option<i32>,
    ) -> DbResult<ReadinessResult> {
        let mut tx = self.pool.begin().await?;

        // Write the inbox entry
        sqlx::query(
            r#"
            INSERT INTO node_inputs (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(instance_id.0)
        .bind(target_node_id)
        .bind(variable_name)
        .bind(&value)
        .bind(source_node_id)
        .bind(spread_index)
        .execute(&mut *tx)
        .await?;

        // Atomically increment completed_count and return both counts
        let row = sqlx::query(
            r#"
            UPDATE node_readiness
            SET completed_count = completed_count + 1,
                updated_at = NOW()
            WHERE instance_id = $1 AND node_id = $2
            RETURNING completed_count, required_count
            "#,
        )
        .bind(instance_id.0)
        .bind(downstream_node_id)
        .fetch_one(&mut *tx)
        .await?;

        let completed_count: i32 = row.get(0);
        let required_count: i32 = row.get(1);

        tx.commit().await?;

        Ok(ReadinessResult {
            completed_count,
            required_count,
            is_now_ready: completed_count == required_count,
        })
    }

    /// Get current readiness state for a node.
    pub async fn get_node_readiness(
        &self,
        instance_id: WorkflowInstanceId,
        node_id: &str,
    ) -> DbResult<Option<(i32, i32)>> {
        let row: Option<(i32, i32)> = sqlx::query_as(
            r#"
            SELECT completed_count, required_count
            FROM node_readiness
            WHERE instance_id = $1 AND node_id = $2
            "#,
        )
        .bind(instance_id.0)
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    /// Clean up readiness tracking for an instance.
    pub async fn clear_node_readiness(&self, instance_id: WorkflowInstanceId) -> DbResult<()> {
        sqlx::query(
            r#"
            DELETE FROM node_readiness WHERE instance_id = $1
            "#,
        )
        .bind(instance_id.0)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Unified Readiness Model - Batch Operations
    // ========================================================================

    /// Batch fetch inbox data for multiple nodes in a single query.
    ///
    /// Returns a map of target_node_id -> (variable_name -> value).
    /// This is used to fetch all inbox data needed for inline subgraph execution
    /// before any database writes occur.
    pub async fn batch_read_inbox(
        &self,
        instance_id: WorkflowInstanceId,
        node_ids: &std::collections::HashSet<String>,
    ) -> DbResult<std::collections::HashMap<String, std::collections::HashMap<String, serde_json::Value>>> {
        if node_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let node_ids_vec: Vec<&str> = node_ids.iter().map(|s| s.as_str()).collect();

        let rows: Vec<(String, String, serde_json::Value, Option<i32>)> = sqlx::query_as(
            r#"
            SELECT target_node_id, variable_name, value, spread_index
            FROM node_inputs
            WHERE instance_id = $1 AND target_node_id = ANY($2)
            ORDER BY target_node_id, spread_index NULLS FIRST, created_at
            "#,
        )
        .bind(instance_id.0)
        .bind(&node_ids_vec)
        .fetch_all(&self.pool)
        .await?;

        // Group by target_node_id, then by variable_name
        // For non-spread results, later writes overwrite earlier ones (last write wins)
        let mut result: std::collections::HashMap<String, std::collections::HashMap<String, serde_json::Value>> =
            std::collections::HashMap::new();

        for (target_node_id, var_name, value, _spread_index) in rows {
            result
                .entry(target_node_id)
                .or_default()
                .insert(var_name, value);
        }

        Ok(result)
    }

    /// Batch fetch spread results for aggregator nodes.
    ///
    /// Returns a map of aggregator_node_id -> Vec<(spread_index, value)>.
    /// Results are ordered by spread_index for correct aggregation order.
    pub async fn batch_read_inbox_for_aggregators(
        &self,
        instance_id: WorkflowInstanceId,
        aggregator_node_ids: &[&str],
    ) -> DbResult<std::collections::HashMap<String, Vec<(i32, serde_json::Value)>>> {
        if aggregator_node_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let rows: Vec<(String, i32, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT target_node_id, spread_index, value
            FROM node_inputs
            WHERE instance_id = $1
              AND target_node_id = ANY($2)
              AND spread_index IS NOT NULL
            ORDER BY target_node_id, spread_index
            "#,
        )
        .bind(instance_id.0)
        .bind(aggregator_node_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut result: std::collections::HashMap<String, Vec<(i32, serde_json::Value)>> =
            std::collections::HashMap::new();

        for (target_node_id, spread_index, value) in rows {
            result
                .entry(target_node_id)
                .or_default()
                .push((spread_index, value));
        }

        Ok(result)
    }

    /// Execute a completion plan in a single atomic transaction.
    ///
    /// This is the core operation of the unified readiness model. It atomically:
    /// 1. Marks the completed action as complete (with idempotency check)
    /// 2. Writes inbox entries for all frontier nodes
    /// 3. Increments readiness counters for frontier nodes
    /// 4. Enqueues nodes when their readiness reaches the required count
    /// 5. Completes the workflow instance if output node was reached
    ///
    /// If the completion is stale (duplicate delivery), the transaction is rolled
    /// back and `CompletionResult::stale()` is returned.
    pub async fn execute_completion_plan(
        &self,
        instance_id: WorkflowInstanceId,
        plan: CompletionPlan,
    ) -> DbResult<CompletionResult> {
        let mut tx = self.pool.begin().await?;
        let mut result = CompletionResult::default();

        // 1. Mark the completed action as complete (idempotent guard)
        if let Some(action_id) = plan.completed_action_id {
            if let Some(delivery_token) = plan.delivery_token {
                let rows = sqlx::query(
                    r#"
                    UPDATE action_queue
                    SET status = CASE WHEN $2 THEN 'completed' ELSE 'failed' END,
                        success = $2,
                        result_payload = $3,
                        last_error = $4,
                        completed_at = NOW()
                    WHERE id = $1 AND delivery_token = $5 AND status = 'dispatched'
                    "#,
                )
                .bind(action_id.0)
                .bind(plan.success)
                .bind(&plan.result_payload)
                .bind(&plan.error_message)
                .bind(delivery_token)
                .execute(&mut *tx)
                .await?;

                if rows.rows_affected() == 0 {
                    // Stale or duplicate completion - roll back
                    tx.rollback().await?;
                    return Ok(CompletionResult::stale());
                }
            }
        }

        // 2. Write inbox entries for all frontier nodes
        for write in &plan.inbox_writes {
            sqlx::query(
                r#"
                INSERT INTO node_inputs
                    (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(instance_id.0)
            .bind(&write.target_node_id)
            .bind(&write.variable_name)
            .bind(&write.value)
            .bind(&write.source_node_id)
            .bind(write.spread_index)
            .execute(&mut *tx)
            .await?;
        }

        // 3. Increment readiness for frontier nodes, enqueue if ready
        for increment in &plan.readiness_increments {
            let row = sqlx::query(
                r#"
                INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)
                VALUES ($1, $2, $3, 1)
                ON CONFLICT (instance_id, node_id)
                DO UPDATE SET completed_count = node_readiness.completed_count + 1,
                              updated_at = NOW()
                RETURNING completed_count, required_count
                "#,
            )
            .bind(instance_id.0)
            .bind(&increment.node_id)
            .bind(increment.required_count)
            .fetch_one(&mut *tx)
            .await?;

            let completed_count: i32 = row.get(0);
            let required_count: i32 = row.get(1);

            debug!(
                node_id = %increment.node_id,
                completed_count = completed_count,
                required_count = required_count,
                "incremented node readiness"
            );

            // Check for overflow (indicates a bug)
            if completed_count > required_count {
                tx.rollback().await?;
                return Err(DbError::NotFound(format!(
                    "Readiness overflow for node {}: {} > {}",
                    increment.node_id, completed_count, required_count
                )));
            }

            // If this increment made the node ready, enqueue it
            if completed_count == required_count {
                // Get the next action_seq for this instance
                let seq_row = sqlx::query(
                    r#"
                    UPDATE workflow_instances
                    SET next_action_seq = next_action_seq + 1
                    WHERE id = $1
                    RETURNING next_action_seq - 1
                    "#,
                )
                .bind(instance_id.0)
                .fetch_one(&mut *tx)
                .await?;
                let action_seq: i32 = seq_row.get(0);

                // Enqueue the node based on its type
                sqlx::query(
                    r#"
                    INSERT INTO action_queue
                        (instance_id, action_seq, module_name, action_name,
                         dispatch_payload, timeout_seconds, max_retries,
                         backoff_kind, backoff_base_delay_ms, node_id, node_type)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    "#,
                )
                .bind(instance_id.0)
                .bind(action_seq)
                .bind(increment.module_name.as_deref().unwrap_or("__internal__"))
                .bind(increment.action_name.as_deref().unwrap_or("__barrier__"))
                .bind(increment.dispatch_payload.as_deref().unwrap_or(&[]))
                .bind(300) // timeout_seconds
                .bind(3) // max_retries
                .bind("exponential")
                .bind(1000) // backoff_base_delay_ms
                .bind(&increment.node_id)
                .bind(increment.node_type.as_str())
                .execute(&mut *tx)
                .await?;

                result.newly_ready_nodes.push(increment.node_id.clone());

                debug!(
                    node_id = %increment.node_id,
                    node_type = %increment.node_type.as_str(),
                    action_seq = action_seq,
                    "enqueued ready node"
                );
            }
        }

        // 4. Complete workflow instance if output node was reached
        if let Some(completion) = &plan.instance_completion {
            let rows = sqlx::query(
                r#"
                UPDATE workflow_instances
                SET status = 'completed',
                    result_payload = $2,
                    completed_at = NOW()
                WHERE id = $1 AND status = 'running'
                "#,
            )
            .bind(instance_id.0)
            .bind(&completion.result_payload)
            .execute(&mut *tx)
            .await?;

            if rows.rows_affected() > 0 {
                result.workflow_completed = true;
                debug!(
                    instance_id = %instance_id.0,
                    "workflow instance completed"
                );
            }
            // If rows_affected == 0, another path already completed it (ok)
        }

        // 5. Commit the transaction
        tx.commit().await?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

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

    // ========================================================================
    // Integration tests - require DATABASE_URL to be set
    // Run with: cargo test --lib db::tests -- --ignored
    // ========================================================================

    async fn test_db() -> Database {
        dotenvy::dotenv().ok();
        let url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
        Database::connect(&url)
            .await
            .expect("failed to connect to database")
    }

    #[tokio::test]
    #[serial]
    async fn test_workflow_version_lifecycle() {
        let db = test_db().await;

        // Create a workflow version
        let program_proto = b"test program bytes";
        let version_id = db
            .upsert_workflow_version("test_workflow", "hash123", program_proto, false)
            .await
            .expect("failed to create version");

        // Fetch it back
        let version = db
            .get_workflow_version(version_id)
            .await
            .expect("failed to get version");

        assert_eq!(version.workflow_name, "test_workflow");
        assert_eq!(version.dag_hash, "hash123");
        assert_eq!(version.program_proto, program_proto);
        assert!(!version.concurrent);

        // Upsert with same hash should return same ID
        let version_id2 = db
            .upsert_workflow_version("test_workflow", "hash123", program_proto, false)
            .await
            .expect("failed to upsert version");

        assert_eq!(version_id.0, version_id2.0);

        // List versions should include our version
        let versions = db
            .list_workflow_versions()
            .await
            .expect("failed to list versions");
        assert!(versions.iter().any(|v| v.id == version_id.0));
    }

    #[tokio::test]
    #[serial]
    async fn test_workflow_instance_lifecycle() {
        let db = test_db().await;

        // Create a version first
        let version_id = db
            .upsert_workflow_version("instance_test", "hash_inst", b"proto", false)
            .await
            .expect("failed to create version");

        // Create an instance
        let input = b"input payload";
        let instance_id = db
            .create_instance("instance_test", version_id, Some(input))
            .await
            .expect("failed to create instance");

        // Fetch it
        let instance = db
            .get_instance(instance_id)
            .await
            .expect("failed to get instance");

        assert_eq!(instance.workflow_name, "instance_test");
        assert_eq!(instance.workflow_version_id, Some(version_id.0));
        assert_eq!(instance.status, "running");
        assert_eq!(instance.input_payload, Some(input.to_vec()));
        assert!(instance.result_payload.is_none());
        assert!(instance.completed_at.is_none());

        // Complete the instance
        let result = b"result payload";
        db.complete_instance(instance_id, Some(result))
            .await
            .expect("failed to complete instance");

        // Verify completion
        let instance = db
            .get_instance(instance_id)
            .await
            .expect("failed to get instance");
        assert_eq!(instance.status, "completed");
        assert_eq!(instance.result_payload, Some(result.to_vec()));
        assert!(instance.completed_at.is_some());
    }

    #[tokio::test]
    #[serial]
    async fn test_action_queue_enqueue_and_dispatch() {
        let db = test_db().await;

        // Setup: create version and instance
        let version_id = db
            .upsert_workflow_version("queue_test", "hash_queue", b"proto", false)
            .await
            .unwrap();

        let instance_id = db
            .create_instance("queue_test", version_id, None)
            .await
            .unwrap();

        // Enqueue an action
        let action_id = db
            .enqueue_action(NewAction {
                instance_id,
                module_name: "test.module".to_string(),
                action_name: "test_action".to_string(),
                dispatch_payload: b"dispatch data".to_vec(),
                timeout_seconds: 60,
                max_retries: 3,
                backoff_kind: BackoffKind::Exponential,
                backoff_base_delay_ms: 1000,
                node_id: Some("node_1".to_string()),
            })
            .await
            .expect("failed to enqueue action");

        // Dispatch should return our action
        let dispatched = db
            .dispatch_actions(10)
            .await
            .expect("failed to dispatch actions");

        // Find our action in the dispatched list
        let our_action = dispatched.iter().find(|a| a.id == action_id.0);
        assert!(our_action.is_some(), "our action should be dispatched");

        let action = our_action.unwrap();
        assert_eq!(action.module_name, "test.module");
        assert_eq!(action.action_name, "test_action");
        assert_eq!(action.dispatch_payload, b"dispatch data");
        assert_eq!(action.timeout_seconds, 60);
        assert_eq!(action.max_retries, 3);
        assert_eq!(action.attempt_number, 0);

        // Dispatch again should NOT return the same action (it's now 'dispatched')
        let dispatched2 = db.dispatch_actions(10).await.unwrap();
        assert!(!dispatched2.iter().any(|a| a.id == action_id.0));
    }

    #[tokio::test]
    #[serial]
    async fn test_action_completion_with_delivery_token() {
        let db = test_db().await;

        // Setup
        let version_id = db
            .upsert_workflow_version("complete_test", "hash_complete", b"proto", false)
            .await
            .unwrap();

        let instance_id = db
            .create_instance("complete_test", version_id, None)
            .await
            .unwrap();

        db.enqueue_action(NewAction {
            instance_id,
            module_name: "test.module".to_string(),
            action_name: "complete_action".to_string(),
            dispatch_payload: b"data".to_vec(),
            timeout_seconds: 60,
            max_retries: 3,
            backoff_kind: BackoffKind::None,
            backoff_base_delay_ms: 0,
            node_id: None,
        })
        .await
        .unwrap();

        // Dispatch to get the delivery token
        let dispatched = db.dispatch_actions(10).await.unwrap();
        let action = dispatched
            .iter()
            .find(|a| a.action_name == "complete_action")
            .unwrap();
        let delivery_token = action.delivery_token;

        // Complete with correct token should succeed
        let completed = db
            .complete_action(CompletionRecord {
                action_id: ActionId(action.id),
                success: true,
                result_payload: b"result".to_vec(),
                delivery_token,
                error_message: None,
            })
            .await
            .expect("failed to complete action");

        assert!(completed, "completion with correct token should succeed");

        // Complete again with same token should fail (already completed)
        let completed_again = db
            .complete_action(CompletionRecord {
                action_id: ActionId(action.id),
                success: true,
                result_payload: b"result2".to_vec(),
                delivery_token,
                error_message: None,
            })
            .await
            .expect("failed to attempt completion");

        assert!(!completed_again, "duplicate completion should return false");

        // Complete with wrong token should fail
        let wrong_token = Uuid::new_v4();
        let completed_wrong = db
            .complete_action(CompletionRecord {
                action_id: ActionId(action.id),
                success: true,
                result_payload: b"result3".to_vec(),
                delivery_token: wrong_token,
                error_message: None,
            })
            .await
            .expect("failed to attempt completion");

        assert!(
            !completed_wrong,
            "completion with wrong token should return false"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_skip_locked_concurrent_dispatch() {
        let db = test_db().await;

        // Clean up any leftover actions from previous tests
        sqlx::query("DELETE FROM action_queue")
            .execute(db.pool())
            .await
            .expect("failed to clean up actions");

        // Setup: create version and instance with multiple actions
        let version_id = db
            .upsert_workflow_version("concurrent_test", "hash_concurrent", b"proto", false)
            .await
            .unwrap();

        let instance_id = db
            .create_instance("concurrent_test", version_id, None)
            .await
            .unwrap();

        // Enqueue 5 actions
        for i in 0..5 {
            db.enqueue_action(NewAction {
                instance_id,
                module_name: "test.module".to_string(),
                action_name: format!("action_{}", i),
                dispatch_payload: vec![i as u8],
                timeout_seconds: 60,
                max_retries: 3,
                backoff_kind: BackoffKind::None,
                backoff_base_delay_ms: 0,
                node_id: None,
            })
            .await
            .unwrap();
        }

        // Dispatch 2 at a time - simulates concurrent workers
        let batch1 = db.dispatch_actions(2).await.unwrap();
        let batch2 = db.dispatch_actions(2).await.unwrap();
        let batch3 = db.dispatch_actions(2).await.unwrap();

        // Should have gotten 2, 2, 1 actions respectively
        assert_eq!(batch1.len(), 2, "first batch should have 2 actions");
        assert_eq!(batch2.len(), 2, "second batch should have 2 actions");
        assert_eq!(batch3.len(), 1, "third batch should have 1 action");

        // No duplicates between batches
        let all_ids: Vec<_> = batch1
            .iter()
            .chain(batch2.iter())
            .chain(batch3.iter())
            .map(|a| a.id)
            .collect();

        let unique_ids: std::collections::HashSet<_> = all_ids.iter().collect();
        assert_eq!(
            all_ids.len(),
            unique_ids.len(),
            "should have no duplicate dispatches"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_loop_state() {
        let db = test_db().await;

        // Setup
        let version_id = db
            .upsert_workflow_version("loop_test", "hash_loop", b"proto", false)
            .await
            .unwrap();

        let instance_id = db
            .create_instance("loop_test", version_id, None)
            .await
            .unwrap();

        // Get or create loop state
        let state = db
            .get_or_create_loop_state(instance_id, "loop_1")
            .await
            .expect("failed to create loop state");

        assert_eq!(state.loop_id, "loop_1");
        assert_eq!(state.current_index, 0);
        assert!(state.accumulators.is_none());

        // Increment index
        let new_idx = db
            .increment_loop_index(instance_id, "loop_1")
            .await
            .expect("failed to increment");

        assert_eq!(new_idx, 1);

        // Increment again
        let new_idx = db
            .increment_loop_index(instance_id, "loop_1")
            .await
            .unwrap();
        assert_eq!(new_idx, 2);

        // Update accumulators
        let accum_data = b"[1, 2, 3]";
        db.update_loop_accumulators(instance_id, "loop_1", accum_data)
            .await
            .expect("failed to update accumulators");

        // Verify
        let state = db
            .get_or_create_loop_state(instance_id, "loop_1")
            .await
            .unwrap();
        assert_eq!(state.current_index, 2);
        assert_eq!(state.accumulators, Some(accum_data.to_vec()));
    }

    #[tokio::test]
    #[serial]
    async fn test_failed_instance() {
        let db = test_db().await;

        // Setup
        let version_id = db
            .upsert_workflow_version("fail_test", "hash_fail", b"proto", false)
            .await
            .unwrap();

        let instance_id = db
            .create_instance("fail_test", version_id, None)
            .await
            .unwrap();

        // Fail the instance
        db.fail_instance(instance_id)
            .await
            .expect("failed to fail instance");

        // Verify
        let instance = db.get_instance(instance_id).await.unwrap();
        assert_eq!(instance.status, "failed");
        assert!(instance.completed_at.is_some());
    }

    #[tokio::test]
    #[serial]
    async fn test_get_instance_actions() {
        let db = test_db().await;

        // Setup
        let version_id = db
            .upsert_workflow_version("actions_list_test", "hash_list", b"proto", false)
            .await
            .unwrap();

        let instance_id = db
            .create_instance("actions_list_test", version_id, None)
            .await
            .unwrap();

        // Enqueue 3 actions
        for i in 0..3 {
            db.enqueue_action(NewAction {
                instance_id,
                module_name: "test.module".to_string(),
                action_name: format!("list_action_{}", i),
                dispatch_payload: vec![i as u8],
                timeout_seconds: 60,
                max_retries: 3,
                backoff_kind: BackoffKind::None,
                backoff_base_delay_ms: 0,
                node_id: None,
            })
            .await
            .unwrap();
        }

        // Get all actions for instance
        let actions = db
            .get_instance_actions(instance_id)
            .await
            .expect("failed to get instance actions");

        assert_eq!(actions.len(), 3);

        // Should be ordered by action_seq
        assert_eq!(actions[0].action_seq, 0);
        assert_eq!(actions[1].action_seq, 1);
        assert_eq!(actions[2].action_seq, 2);
    }

    // ========================================================================
    // Node Readiness Tests
    // ========================================================================

    /// Helper to create a test instance for readiness tests
    async fn create_test_instance(db: &Database, name: &str) -> WorkflowInstanceId {
        let version_id = db
            .upsert_workflow_version(name, &format!("hash_{}", name), b"proto", false)
            .await
            .expect("failed to create version");

        db.create_instance(name, version_id, None)
            .await
            .expect("failed to create instance")
    }

    #[tokio::test]
    #[serial]
    async fn test_init_node_readiness() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "init_readiness_test").await;

        // Initialize readiness for a node
        db.init_node_readiness(instance_id, "aggregator_1", 5)
            .await
            .expect("failed to init readiness");

        // Verify via get_node_readiness
        let state = db
            .get_node_readiness(instance_id, "aggregator_1")
            .await
            .expect("failed to get readiness");

        let (completed, required) = state.expect("readiness should exist");
        assert_eq!(completed, 0, "should start with 0 completed");
        assert_eq!(required, 5, "should have required count of 5");
    }

    #[tokio::test]
    #[serial]
    async fn test_init_node_readiness_idempotent() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "idempotent_readiness_test").await;

        // Initialize twice with different required counts
        db.init_node_readiness(instance_id, "aggregator_1", 5)
            .await
            .expect("failed to init readiness");

        // Second init should be ignored (ON CONFLICT DO NOTHING)
        db.init_node_readiness(instance_id, "aggregator_1", 10)
            .await
            .expect("failed to init readiness again");

        // Should still have the original required count
        let state = db
            .get_node_readiness(instance_id, "aggregator_1")
            .await
            .expect("failed to get readiness");

        let (_, required) = state.expect("readiness should exist");
        assert_eq!(required, 5, "should keep original required count");
    }

    #[tokio::test]
    #[serial]
    async fn test_increment_node_readiness_basic() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "increment_readiness_test").await;

        // First increment creates the entry
        let result = db
            .increment_node_readiness(instance_id, "aggregator_1", 3)
            .await
            .expect("failed to increment");

        assert_eq!(result.completed_count, 1);
        assert_eq!(result.required_count, 3);
        assert!(!result.is_now_ready);

        // Second increment
        let result = db
            .increment_node_readiness(instance_id, "aggregator_1", 3)
            .await
            .expect("failed to increment");

        assert_eq!(result.completed_count, 2);
        assert!(!result.is_now_ready);

        // Third increment should mark as ready
        let result = db
            .increment_node_readiness(instance_id, "aggregator_1", 3)
            .await
            .expect("failed to increment");

        assert_eq!(result.completed_count, 3);
        assert!(result.is_now_ready, "should be ready when completed == required");
    }

    #[tokio::test]
    #[serial]
    async fn test_increment_node_readiness_single_required() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "single_required_test").await;

        // With required_count=1, first increment should make it ready
        let result = db
            .increment_node_readiness(instance_id, "aggregator_1", 1)
            .await
            .expect("failed to increment");

        assert_eq!(result.completed_count, 1);
        assert_eq!(result.required_count, 1);
        assert!(result.is_now_ready);
    }

    #[tokio::test]
    #[serial]
    async fn test_write_inbox_and_mark_precursor_complete() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "inbox_precursor_test").await;

        // Must pre-initialize node readiness
        db.init_node_readiness(instance_id, "aggregator_1", 3)
            .await
            .expect("failed to init readiness");

        // First completion
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target_node",
                "result",
                serde_json::json!({"value": 1}),
                "source_1",
                Some(0),
            )
            .await
            .expect("failed to write inbox");

        assert_eq!(result.completed_count, 1);
        assert_eq!(result.required_count, 3);
        assert!(!result.is_now_ready);

        // Verify inbox entry was written
        let inbox = db
            .read_inbox(instance_id, "target_node")
            .await
            .expect("failed to get inbox");

        assert_eq!(inbox.len(), 1);
        assert!(inbox.contains_key("result"), "inbox should have result variable");

        // Second and third completions
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target_node",
                "result",
                serde_json::json!({"value": 2}),
                "source_2",
                Some(1),
            )
            .await
            .expect("failed to write inbox");

        assert_eq!(result.completed_count, 2);
        assert!(!result.is_now_ready);

        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target_node",
                "result",
                serde_json::json!({"value": 3}),
                "source_3",
                Some(2),
            )
            .await
            .expect("failed to write inbox");

        assert_eq!(result.completed_count, 3);
        assert!(result.is_now_ready, "should be ready after all precursors complete");

        // Verify all inbox entries - read_inbox collapses by variable name,
        // so we should still see 1 (last write wins for same variable)
        let inbox = db
            .read_inbox(instance_id, "target_node")
            .await
            .expect("failed to get inbox");

        assert_eq!(inbox.len(), 1, "read_inbox dedupes by variable name");
    }

    #[tokio::test]
    #[serial]
    async fn test_write_inbox_batch_and_increment_readiness() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "batch_inbox_test").await;

        // Prepare batch of inbox writes
        let inbox_writes = vec![
            (
                "target_1".to_string(),
                "var_a".to_string(),
                serde_json::json!(1),
                "source_1".to_string(),
                None,
            ),
            (
                "target_2".to_string(),
                "var_b".to_string(),
                serde_json::json!(2),
                "source_1".to_string(),
                None,
            ),
        ];

        // First batch write creates readiness entry
        let result = db
            .write_inbox_batch_and_increment_readiness(
                instance_id,
                "aggregator_1",
                2,
                &inbox_writes,
            )
            .await
            .expect("failed to write batch");

        assert_eq!(result.completed_count, 1);
        assert_eq!(result.required_count, 2);
        assert!(!result.is_now_ready);

        // Verify inbox entries were written
        let inbox1 = db
            .read_inbox(instance_id, "target_1")
            .await
            .expect("failed to get inbox");
        assert_eq!(inbox1.len(), 1);
        assert!(inbox1.contains_key("var_a"));

        let inbox2 = db
            .read_inbox(instance_id, "target_2")
            .await
            .expect("failed to get inbox");
        assert_eq!(inbox2.len(), 1);
        assert!(inbox2.contains_key("var_b"));

        // Second batch makes it ready
        let inbox_writes_2 = vec![(
            "target_3".to_string(),
            "var_c".to_string(),
            serde_json::json!(3),
            "source_2".to_string(),
            None,
        )];

        let result = db
            .write_inbox_batch_and_increment_readiness(
                instance_id,
                "aggregator_1",
                2,
                &inbox_writes_2,
            )
            .await
            .expect("failed to write batch");

        assert_eq!(result.completed_count, 2);
        assert!(result.is_now_ready);
    }

    #[tokio::test]
    #[serial]
    async fn test_node_readiness_multiple_nodes() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "multi_node_readiness_test").await;

        // Initialize two different aggregators
        db.init_node_readiness(instance_id, "aggregator_1", 2)
            .await
            .expect("failed to init readiness");

        db.init_node_readiness(instance_id, "aggregator_2", 3)
            .await
            .expect("failed to init readiness");

        // Complete aggregator_1
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target_1",
                "result",
                serde_json::json!(1),
                "source_1",
                None,
            )
            .await
            .expect("failed to write");

        assert_eq!(result.completed_count, 1);
        assert!(!result.is_now_ready);

        // aggregator_2 should still be at 0
        let state = db
            .get_node_readiness(instance_id, "aggregator_2")
            .await
            .expect("failed to get readiness");
        let (completed, _) = state.expect("should exist");
        assert_eq!(completed, 0);

        // Complete aggregator_1 again
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target_1",
                "result",
                serde_json::json!(2),
                "source_2",
                None,
            )
            .await
            .expect("failed to write");

        assert!(result.is_now_ready, "aggregator_1 should be ready");

        // aggregator_2 should still be at 0
        let state = db
            .get_node_readiness(instance_id, "aggregator_2")
            .await
            .expect("failed to get readiness");
        let (completed, _) = state.expect("should exist");
        assert_eq!(completed, 0, "aggregator_2 should still be at 0");
    }

    #[tokio::test]
    #[serial]
    async fn test_node_readiness_isolated_by_instance() {
        let db = test_db().await;
        let instance_1 = create_test_instance(&db, "isolated_test_1").await;
        let instance_2 = create_test_instance(&db, "isolated_test_2").await;

        // Initialize same node name in both instances
        db.init_node_readiness(instance_1, "aggregator_1", 2)
            .await
            .expect("failed to init");
        db.init_node_readiness(instance_2, "aggregator_1", 3)
            .await
            .expect("failed to init");

        // Increment instance_1
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_1,
                "aggregator_1",
                "target",
                "result",
                serde_json::json!(1),
                "source",
                None,
            )
            .await
            .expect("failed to write");

        assert_eq!(result.completed_count, 1);

        // instance_2 should still be at 0
        let state = db
            .get_node_readiness(instance_2, "aggregator_1")
            .await
            .expect("failed to get");
        let (completed, required) = state.expect("should exist");
        assert_eq!(completed, 0, "instance_2 should be isolated");
        assert_eq!(required, 3, "instance_2 should have its own required count");
    }

    #[tokio::test]
    #[serial]
    async fn test_is_now_ready_only_true_once() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "ready_once_test").await;

        // Initialize with required=2
        db.init_node_readiness(instance_id, "aggregator_1", 2)
            .await
            .expect("failed to init");

        // First completion
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target",
                "result",
                serde_json::json!(1),
                "source_1",
                None,
            )
            .await
            .expect("failed to write");
        assert!(!result.is_now_ready);

        // Second completion makes it ready
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target",
                "result",
                serde_json::json!(2),
                "source_2",
                None,
            )
            .await
            .expect("failed to write");
        assert!(result.is_now_ready, "should be ready at completed==required");

        // Third completion (over the threshold) should NOT report is_now_ready
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_1",
                "target",
                "result",
                serde_json::json!(3),
                "source_3",
                None,
            )
            .await
            .expect("failed to write");
        assert_eq!(result.completed_count, 3);
        assert!(!result.is_now_ready, "should only be ready exactly at the threshold");
    }

    #[tokio::test]
    #[serial]
    async fn test_spread_pattern_five_actions() {
        let db = test_db().await;
        let instance_id = create_test_instance(&db, "spread_five_test").await;

        // Initialize for 5 spread actions
        db.init_node_readiness(instance_id, "aggregator_11", 5)
            .await
            .expect("failed to init");

        // Complete 4 actions - should not be ready
        for i in 0..4 {
            let result = db
                .write_inbox_and_mark_precursor_complete(
                    instance_id,
                    "aggregator_11",
                    "aggregator_11",
                    "result",
                    serde_json::json!({"index": i, "value": i * 10}),
                    &format!("spread_action_{}", i),
                    Some(i),
                )
                .await
                .expect("failed to write");

            assert_eq!(result.completed_count, i + 1);
            assert!(!result.is_now_ready, "should not be ready at {} of 5", i + 1);
        }

        // 5th action makes it ready
        let result = db
            .write_inbox_and_mark_precursor_complete(
                instance_id,
                "aggregator_11",
                "aggregator_11",
                "result",
                serde_json::json!({"index": 4, "value": 40}),
                "spread_action_4",
                Some(4),
            )
            .await
            .expect("failed to write");

        assert_eq!(result.completed_count, 5);
        assert_eq!(result.required_count, 5);
        assert!(result.is_now_ready, "should be ready after 5th completion");

        // Verify all inbox entries exist with correct spread indices
        let inbox = db
            .read_inbox_for_aggregator(instance_id, "aggregator_11")
            .await
            .expect("failed to get inbox");

        assert_eq!(inbox.len(), 5, "should have all 5 results");

        // Verify spread indices are present and correct
        let mut indices: Vec<_> = inbox.iter().map(|(idx, _)| *idx).collect();
        indices.sort();
        assert_eq!(indices, vec![0, 1, 2, 3, 4], "should have spread indices 0-4");
    }
}
