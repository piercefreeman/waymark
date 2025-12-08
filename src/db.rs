//! Database layer for workflow persistence.
//!
//! Uses PostgreSQL with:
//! - Workflow instances table
//! - Action inbox/queue per instance
//! - SELECT FOR UPDATE SKIP LOCKED for distributed queue
//! - Timeout-based recovery for crashed workers

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPool;
use sqlx::FromRow;
use uuid::Uuid;

/// Action status in the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "action_status", rename_all = "snake_case")]
pub enum ActionStatus {
    Queued,
    Dispatched,
    Completed,
    Failed,
}

/// Workflow instance status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "instance_status", rename_all = "snake_case")]
pub enum InstanceStatus {
    Running,
    WaitingForActions,
    Completed,
    Failed,
}

/// A workflow instance record.
#[derive(Debug, Clone, FromRow)]
pub struct WorkflowInstance {
    pub id: Uuid,
    pub workflow_name: String,
    pub module_name: String,
    pub status: InstanceStatus,
    pub initial_args: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub actions_until_index: i32,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub dispatch_token: Option<Uuid>,
    pub instance_timeout_seconds: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// An action in the instance's inbox/queue.
#[derive(Debug, Clone, FromRow)]
pub struct ActionRecord {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub sequence: i32,
    pub action_name: String,
    pub module_name: String,
    pub kwargs: serde_json::Value,
    pub status: ActionStatus,
    pub result: Option<serde_json::Value>,
    pub error_message: Option<String>,
    pub dispatch_token: Option<Uuid>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Database operations for workflow management.
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Create a new database connection.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Database { pool })
    }

    /// Run database migrations.
    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    // =========================================================================
    // Instance Operations
    // =========================================================================

    /// Create a new workflow instance.
    /// Instance starts with status='running' and dispatched_at=NULL.
    /// The dispatcher will claim it and set dispatched_at when actually dispatching.
    pub async fn create_instance(
        &self,
        workflow_name: &str,
        module_name: &str,
        initial_args: serde_json::Value,
    ) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO workflow_instances
                (id, workflow_name, module_name, status, initial_args)
            VALUES ($1, $2, $3, 'running', $4)
            "#,
        )
        .bind(id)
        .bind(workflow_name)
        .bind(module_name)
        .bind(&initial_args)
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    /// Get an instance by ID.
    pub async fn get_instance(&self, id: Uuid) -> anyhow::Result<Option<WorkflowInstance>> {
        let row: Option<WorkflowInstance> = sqlx::query_as(
            r#"
            SELECT
                id, workflow_name, module_name, status,
                initial_args, result, actions_until_index,
                scheduled_at, dispatched_at, dispatch_token,
                instance_timeout_seconds, created_at, updated_at
            FROM workflow_instances
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row)
    }

    /// Claim instances that are ready to run.
    /// This includes:
    /// - New instances (status='running', dispatched_at IS NULL) - never dispatched yet
    /// - Resumed instances (status='waiting_for_actions') - yielded and ready to continue
    /// Sets dispatched_at, generates dispatch_token, and status='running'.
    /// The dispatch_token MUST be passed back when completing/yielding the instance.
    pub async fn claim_ready_instances(&self, limit: i32) -> anyhow::Result<Vec<WorkflowInstance>> {
        let rows: Vec<WorkflowInstance> = sqlx::query_as(
            r#"
            WITH claimed AS (
                SELECT id FROM workflow_instances
                WHERE (
                    -- New instances that haven't been dispatched yet
                    (status = 'running' AND dispatched_at IS NULL)
                    OR
                    -- Instances that yielded and are ready to resume
                    status = 'waiting_for_actions'
                )
                  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                ORDER BY scheduled_at ASC NULLS FIRST, created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE workflow_instances wi
            SET status = 'running',
                dispatched_at = NOW(),
                dispatch_token = gen_random_uuid(),
                updated_at = NOW()
            FROM claimed
            WHERE wi.id = claimed.id
            RETURNING wi.id, wi.workflow_name, wi.module_name, wi.status,
                      wi.initial_args, wi.result, wi.actions_until_index,
                      wi.scheduled_at, wi.dispatched_at, wi.dispatch_token,
                      wi.instance_timeout_seconds, wi.created_at, wi.updated_at
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Find and reset timed-out running instances.
    /// Clears dispatch_token so stale workers can't complete them.
    /// Returns count of instances reset.
    pub async fn recover_timed_out_instances(&self) -> anyhow::Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'waiting_for_actions',
                dispatched_at = NULL,
                dispatch_token = NULL,
                scheduled_at = NOW(),
                updated_at = NOW()
            WHERE status = 'running'
              AND dispatched_at IS NOT NULL
              AND dispatched_at + (instance_timeout_seconds || ' seconds')::interval < NOW()
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Mark instance as waiting for actions (after pending actions reported).
    /// Requires dispatch_token for idempotency - stale workers are rejected.
    /// Returns true if update succeeded, false if token didn't match (stale worker).
    pub async fn set_instance_waiting(
        &self,
        id: Uuid,
        dispatch_token: Uuid,
        actions_until_index: i32,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'waiting_for_actions',
                actions_until_index = $3,
                dispatched_at = NULL,
                dispatch_token = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND dispatch_token = $2
              AND status = 'running'
            "#,
        )
        .bind(id)
        .bind(dispatch_token)
        .bind(actions_until_index)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Complete an instance with a result.
    /// Requires dispatch_token for idempotency - stale workers are rejected.
    /// Returns true if update succeeded, false if token didn't match (stale worker).
    pub async fn complete_instance(
        &self,
        id: Uuid,
        dispatch_token: Uuid,
        result: serde_json::Value,
    ) -> anyhow::Result<bool> {
        let query_result = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'completed',
                result = $3,
                dispatched_at = NULL,
                dispatch_token = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND dispatch_token = $2
              AND status = 'running'
            "#,
        )
        .bind(id)
        .bind(dispatch_token)
        .bind(&result)
        .execute(&self.pool)
        .await?;

        Ok(query_result.rows_affected() > 0)
    }

    /// Fail an instance with an error message.
    /// Requires dispatch_token for idempotency - stale workers are rejected.
    /// Returns true if update succeeded, false if token didn't match (stale worker).
    pub async fn fail_instance(
        &self,
        id: Uuid,
        dispatch_token: Uuid,
        error: &str,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'failed',
                result = $3,
                dispatched_at = NULL,
                dispatch_token = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND dispatch_token = $2
              AND status = 'running'
            "#,
        )
        .bind(id)
        .bind(dispatch_token)
        .bind(serde_json::json!({ "error": error }))
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    // =========================================================================
    // Action Operations
    // =========================================================================

    /// Add actions to an instance's inbox.
    /// Uses ON CONFLICT DO NOTHING to handle re-dispatches idempotently.
    pub async fn add_actions(
        &self,
        instance_id: Uuid,
        actions: Vec<(String, String, serde_json::Value, Option<i32>)>, // (action_name, module, kwargs, timeout)
        start_sequence: i32,
    ) -> anyhow::Result<Vec<Uuid>> {
        let mut ids = Vec::with_capacity(actions.len());

        for (i, (action_name, module_name, kwargs, timeout)) in actions.into_iter().enumerate() {
            let id = Uuid::new_v4();
            let sequence = start_sequence + i as i32;
            let timeout_seconds = timeout.unwrap_or(300); // 5 min default

            // Use ON CONFLICT DO NOTHING to handle re-dispatches idempotently.
            // If this (instance_id, sequence) pair already exists, skip it.
            sqlx::query(
                r#"
                INSERT INTO actions
                    (id, instance_id, sequence, action_name, module_name, kwargs,
                     status, timeout_seconds)
                VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7)
                ON CONFLICT (instance_id, sequence) DO NOTHING
                "#,
            )
            .bind(id)
            .bind(instance_id)
            .bind(sequence)
            .bind(&action_name)
            .bind(&module_name)
            .bind(&kwargs)
            .bind(timeout_seconds)
            .execute(&self.pool)
            .await?;

            ids.push(id);
        }

        Ok(ids)
    }

    /// Claim queued actions for execution.
    /// Sets dispatched_at, increments attempt_count, generates dispatch_token.
    pub async fn claim_queued_actions(&self, limit: i32) -> anyhow::Result<Vec<ActionRecord>> {
        let rows: Vec<ActionRecord> = sqlx::query_as(
            r#"
            WITH claimed AS (
                SELECT id FROM actions
                WHERE status = 'queued'
                  AND attempt_count < max_attempts
                ORDER BY created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE actions a
            SET status = 'dispatched',
                dispatched_at = NOW(),
                attempt_count = attempt_count + 1,
                dispatch_token = gen_random_uuid(),
                updated_at = NOW()
            FROM claimed
            WHERE a.id = claimed.id
            RETURNING a.id, a.instance_id, a.sequence, a.action_name, a.module_name,
                      a.kwargs, a.status, a.result, a.error_message, a.dispatch_token,
                      a.dispatched_at, a.attempt_count, a.max_attempts, a.timeout_seconds,
                      a.created_at, a.updated_at
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Find and reset timed-out dispatched actions.
    /// Returns count of actions reset.
    pub async fn recover_timed_out_actions(&self) -> anyhow::Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE actions
            SET status = 'queued',
                dispatched_at = NULL,
                dispatch_token = NULL,
                updated_at = NOW()
            WHERE status = 'dispatched'
              AND dispatched_at IS NOT NULL
              AND dispatched_at + (timeout_seconds || ' seconds')::interval < NOW()
              AND attempt_count < max_attempts
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Fail actions that have exceeded max attempts after timeout.
    pub async fn fail_exhausted_actions(&self) -> anyhow::Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE actions
            SET status = 'failed',
                error_message = 'Max attempts exceeded after timeout',
                dispatched_at = NULL,
                updated_at = NOW()
            WHERE status = 'dispatched'
              AND dispatched_at IS NOT NULL
              AND dispatched_at + (timeout_seconds || ' seconds')::interval < NOW()
              AND attempt_count >= max_attempts
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Complete an action and check if instance should be rescheduled.
    /// This is an ATOMIC transaction that:
    /// 1. Marks the action as completed
    /// 2. Checks if all actions for the instance are done
    /// 3. If so, schedules the instance for re-dispatch
    ///
    /// Returns (action_updated, instance_rescheduled)
    pub async fn complete_action_atomic(
        &self,
        action_id: Uuid,
        dispatch_token: Uuid,
        result: serde_json::Value,
    ) -> anyhow::Result<(bool, bool)> {
        let mut tx = self.pool.begin().await?;

        // 1. Complete the action (idempotent via dispatch_token)
        let action_result = sqlx::query(
            r#"
            UPDATE actions
            SET status = 'completed',
                result = $3,
                dispatched_at = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND dispatch_token = $2
              AND status = 'dispatched'
            "#,
        )
        .bind(action_id)
        .bind(dispatch_token)
        .bind(&result)
        .execute(&mut *tx)
        .await?;

        if action_result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok((false, false));
        }

        // 2. Get the instance_id for this action
        let (instance_id,): (Uuid,) = sqlx::query_as(
            "SELECT instance_id FROM actions WHERE id = $1",
        )
        .bind(action_id)
        .fetch_one(&mut *tx)
        .await?;

        // 3. Check if all queued/dispatched actions for this instance are done
        let (pending_count,): (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*) FROM actions
            WHERE instance_id = $1
              AND status IN ('queued', 'dispatched')
            "#,
        )
        .bind(instance_id)
        .fetch_one(&mut *tx)
        .await?;

        let instance_rescheduled = if pending_count == 0 {
            // All actions done! Schedule instance for re-dispatch
            let reschedule_result = sqlx::query(
                r#"
                UPDATE workflow_instances
                SET scheduled_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                  AND status = 'waiting_for_actions'
                "#,
            )
            .bind(instance_id)
            .execute(&mut *tx)
            .await?;

            reschedule_result.rows_affected() > 0
        } else {
            false
        };

        tx.commit().await?;

        Ok((true, instance_rescheduled))
    }

    /// Fail an action (non-retryable) and check instance status.
    pub async fn fail_action_atomic(
        &self,
        action_id: Uuid,
        dispatch_token: Uuid,
        error_message: &str,
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE actions
            SET status = 'failed',
                error_message = $3,
                dispatched_at = NULL,
                updated_at = NOW()
            WHERE id = $1
              AND dispatch_token = $2
              AND status = 'dispatched'
            "#,
        )
        .bind(action_id)
        .bind(dispatch_token)
        .bind(error_message)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Get all completed actions for an instance (for replay).
    pub async fn get_completed_actions(
        &self,
        instance_id: Uuid,
    ) -> anyhow::Result<Vec<ActionRecord>> {
        let rows: Vec<ActionRecord> = sqlx::query_as(
            r#"
            SELECT
                id, instance_id, sequence, action_name, module_name, kwargs,
                status, result, error_message, dispatch_token,
                dispatched_at, attempt_count, max_attempts, timeout_seconds,
                created_at, updated_at
            FROM actions
            WHERE instance_id = $1 AND status = 'completed'
            ORDER BY sequence ASC
            "#,
        )
        .bind(instance_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    /// Get the pool for direct access.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
