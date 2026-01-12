//! High-performance database operations for the worker dispatch loop.
//!
//! These operations are hyper-optimized and should never be slowed down
//! by webapp features. All operations here are on the critical path for
//! workflow execution.

use sqlx::Row;
use tracing::debug;
use uuid::Uuid;

use crate::completion::{CompletionPlan, CompletionResult};

use chrono::{DateTime, Utc};

use super::{
    ActionId, CompletionRecord, Database, DbError, DbResult, LoopState, NewAction, QueuedAction,
    ReadinessResult, ScheduleId, ScheduleType, WorkerStatusUpdate, WorkflowInstance,
    WorkflowInstanceId, WorkflowSchedule, WorkflowVersion, WorkflowVersionId,
};

impl Database {
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
            "#,
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

    // ========================================================================
    // Workflow Instances
    // ========================================================================

    /// Create a new workflow instance
    pub async fn create_instance(
        &self,
        workflow_name: &str,
        version_id: WorkflowVersionId,
        input_payload: Option<&[u8]>,
        schedule_id: Option<ScheduleId>,
    ) -> DbResult<WorkflowInstanceId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_instances (workflow_name, workflow_version_id, input_payload, schedule_id)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(version_id.0)
        .bind(input_payload)
        .bind(schedule_id.map(|id| id.0))
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
                   i.schedule_id, i.next_action_seq, i.input_payload, i.result_payload, i.status,
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

    /// Mark an instance as failed with an optional result payload.
    pub async fn fail_instance_with_result(
        &self,
        id: WorkflowInstanceId,
        result_payload: Option<&[u8]>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_instances
            SET status = 'failed', result_payload = $2, completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .bind(result_payload)
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
                backoff_kind, backoff_base_delay_ms, node_id, node_type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11, 'action'))
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
        .bind(&action.node_type)
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
    /// 5. Creates action log entries for tracking run history
    /// 6. Returns the actions for execution
    pub async fn dispatch_actions(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let start = std::time::Instant::now();
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
            ),
            updated AS (
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
                    COALESCE(aq.node_type, 'action') as node_type,
                    aq.dispatched_at
            ),
            log_insert AS (
                INSERT INTO action_logs (action_id, instance_id, attempt_number, dispatched_at)
                SELECT id, instance_id, attempt_number, dispatched_at
                FROM updated
            )
            SELECT id, instance_id, partition_id, action_seq, module_name, action_name,
                   dispatch_payload, timeout_seconds, max_retries, attempt_number,
                   delivery_token, timeout_retry_limit, retry_kind, node_id, node_type
            FROM updated
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let actions: Vec<QueuedAction> = rows
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
                result_payload: None, // Not yet completed
                success: None,
                status: "dispatched".to_string(),
                scheduled_at: None, // Already dispatched, scheduled time passed
                last_error: None,
            })
            .collect();

        let count = actions.len();
        if count > 0 {
            tracing::info!(
                elapsed_ms = start.elapsed().as_millis() as u64,
                count = count,
                "dispatch_actions"
            );
        }
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
            ),
            updated AS (
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
                    COALESCE(aq.node_type, 'action') as node_type,
                    aq.dispatched_at
            ),
            log_insert AS (
                INSERT INTO action_logs (action_id, instance_id, attempt_number, dispatched_at)
                SELECT id, instance_id, attempt_number, dispatched_at
                FROM updated
            )
            SELECT id, instance_id, partition_id, action_seq, module_name, action_name,
                   dispatch_payload, timeout_seconds, max_retries, attempt_number,
                   delivery_token, timeout_retry_limit, retry_kind, node_id, node_type
            FROM updated
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
                result_payload: None, // Not yet completed
                success: None,
                status: "dispatched".to_string(),
                scheduled_at: None, // Already dispatched, scheduled time passed
                last_error: None,
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
            ),
            updated AS (
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
                    aq.node_type,
                    aq.dispatched_at
            ),
            log_insert AS (
                INSERT INTO action_logs (action_id, instance_id, attempt_number, dispatched_at)
                SELECT id, instance_id, attempt_number, dispatched_at
                FROM updated
            )
            SELECT id, instance_id, partition_id, action_seq, module_name, action_name,
                   dispatch_payload, timeout_seconds, max_retries, attempt_number,
                   delivery_token, timeout_retry_limit, retry_kind, node_id, node_type
            FROM updated
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
                result_payload: None, // Not yet completed
                success: None,
                status: "dispatched".to_string(),
                scheduled_at: None, // Already dispatched, scheduled time passed
                last_error: None,
            })
            .collect();

        Ok(actions)
    }

    /// Complete an action with its result
    ///
    /// Uses delivery_token for idempotent completion - if the token doesn't match,
    /// the action was already completed by another worker or timed out.
    /// Also updates the action log entry with completion information.
    pub async fn complete_action(&self, record: CompletionRecord) -> DbResult<bool> {
        let result = sqlx::query(
            r#"
            WITH updated AS (
                UPDATE action_queue
                SET status = CASE WHEN $2 THEN 'completed' ELSE 'failed' END,
                    success = $2,
                    result_payload = $3,
                    last_error = $4,
                    completed_at = NOW()
                WHERE id = $1 AND delivery_token = $5 AND status = 'dispatched'
                RETURNING id, attempt_number, dispatched_at
            ),
            log_update AS (
                UPDATE action_logs
                SET completed_at = NOW(),
                    success = $2,
                    result_payload = $3,
                    error_message = $4,
                    duration_ms = EXTRACT(EPOCH FROM (NOW() - updated.dispatched_at)) * 1000
                FROM updated
                WHERE action_logs.action_id = updated.id
                  AND action_logs.attempt_number = updated.attempt_number
            )
            SELECT COUNT(*) FROM updated
            "#,
        )
        .bind(record.action_id.0)
        .bind(record.success)
        .bind(&record.result_payload)
        .bind(&record.error_message)
        .bind(record.delivery_token)
        .fetch_one(&self.pool)
        .await?;

        let count: i64 = result.get(0);
        Ok(count > 0)
    }

    /// Mark timed-out actions as failed.
    ///
    /// This finds dispatched actions past their deadline and marks them as 'failed'
    /// with retry_kind='timeout'. The actual retry/requeue logic is handled by
    /// `requeue_failed_actions`, which processes both timeout and explicit failures.
    /// Also updates the action log entries with timeout information.
    ///
    /// Uses SKIP LOCKED for multi-host safety.
    ///
    /// Returns the number of actions marked as failed.
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
            ),
            updated AS (
                UPDATE action_queue aq
                SET status = 'failed',
                    retry_kind = 'timeout',
                    -- Clear deadline and delivery token so old workers can't complete
                    deadline_at = NULL,
                    delivery_token = NULL
                FROM overdue
                WHERE aq.id = overdue.id
                RETURNING aq.id, aq.attempt_number, aq.dispatched_at
            ),
            log_update AS (
                UPDATE action_logs
                SET completed_at = NOW(),
                    success = FALSE,
                    error_message = 'Action timed out',
                    duration_ms = EXTRACT(EPOCH FROM (NOW() - updated.dispatched_at)) * 1000
                FROM updated
                WHERE action_logs.action_id = updated.id
                  AND action_logs.attempt_number = updated.attempt_number
            )
            SELECT COUNT(*) FROM updated
            "#,
        )
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

        let count: i64 = result.get(0);
        if count > 0 {
            tracing::info!(count = count, "marked timed-out actions as failed");
        }

        Ok(count)
    }

    /// Requeue failed actions for retry (handles both explicit failures and timeouts).
    ///
    /// This is the single place where retry/backoff logic is implemented. It handles:
    /// - Actions with retry_kind='failure' (explicit errors) using max_retries limit
    /// - Actions with retry_kind='timeout' (timed out) using timeout_retry_limit
    ///
    /// Actions that have exhausted their retries are marked with terminal status
    /// ('exhausted' or 'timed_out' respectively).
    ///
    /// Uses SKIP LOCKED for multi-host safety.
    ///
    /// Returns a tuple of (requeued_count, permanently_failed_count)
    pub async fn requeue_failed_actions(&self, limit: i32) -> DbResult<(i64, i64)> {
        let result = sqlx::query(
            r#"
            WITH retryable AS (
                SELECT id, retry_kind, attempt_number, max_retries, timeout_retry_limit
                FROM action_queue
                WHERE status = 'failed'
                  AND (
                      (retry_kind = 'failure' AND attempt_number < max_retries)
                      OR
                      (retry_kind = 'timeout' AND attempt_number < timeout_retry_limit)
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
            RETURNING 1 as requeued
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let requeued_count = result.len() as i64;

        // Now mark actions that have exhausted their retries as permanently failed
        let permanent_result = sqlx::query(
            r#"
            WITH exhausted AS (
                SELECT id, retry_kind
                FROM action_queue
                WHERE status = 'failed'
                  AND (
                      (retry_kind = 'failure' AND attempt_number >= max_retries)
                      OR
                      (retry_kind = 'timeout' AND attempt_number >= timeout_retry_limit)
                  )
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE action_queue aq
            SET status = CASE
                    WHEN exhausted.retry_kind = 'timeout' THEN 'timed_out'
                    ELSE 'exhausted'
                END
            FROM exhausted
            WHERE aq.id = exhausted.id
            "#,
        )
        .bind(limit)
        .execute(&self.pool)
        .await?;

        let permanently_failed_count = permanent_result.rows_affected() as i64;

        if requeued_count > 0 || permanently_failed_count > 0 {
            tracing::info!(
                requeued = requeued_count,
                permanently_failed = permanently_failed_count,
                "requeue_failed_actions"
            );
        }

        Ok((requeued_count, permanently_failed_count))
    }

    /// Fail workflow instances that have actions which exhausted all retries.
    /// This should be called after requeue_failed_actions to propagate
    /// permanent action failures to the workflow instance level.
    ///
    /// Returns the number of workflow instances that were marked as failed.
    pub async fn fail_instances_with_exhausted_actions(&self, limit: i32) -> DbResult<i64> {
        // Find workflow instances that:
        // 1. Are still 'running'
        // 2. Have at least one action in a terminal failure state
        //    (status = 'exhausted' or status = 'timed_out')
        let result = sqlx::query(
            r#"
            WITH instances_to_fail AS (
                SELECT DISTINCT wi.id
                FROM workflow_instances wi
                JOIN action_queue aq ON aq.instance_id = wi.id
                WHERE wi.status = 'running'
                  AND aq.status IN ('exhausted', 'timed_out')
                LIMIT $1
            )
            UPDATE workflow_instances wi
            SET status = 'failed',
                completed_at = NOW()
            FROM instances_to_fail
            WHERE wi.id = instances_to_fail.id
              AND wi.status = 'running'
            "#,
        )
        .bind(limit)
        .execute(&self.pool)
        .await?;

        let failed_count = result.rows_affected() as i64;

        if failed_count > 0 {
            tracing::info!(
                failed_instances = failed_count,
                "fail_instances_with_exhausted_actions"
            );
        }

        Ok(failed_count)
    }

    /// Mark an action as caught (exception was handled by a try-except block).
    /// This prevents fail_instances_with_exhausted_actions from failing the workflow.
    pub async fn mark_action_caught(
        &self,
        instance_id: WorkflowInstanceId,
        node_id: &str,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE action_queue
            SET status = 'caught'
            WHERE instance_id = $1
              AND node_id = $2
              AND status IN ('exhausted', 'timed_out', 'failed')
            "#,
        )
        .bind(instance_id.0)
        .bind(node_id)
        .execute(&self.pool)
        .await?;

        Ok(())
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

    /// Write a value to a node's inbox (O(1) upsert, no locks).
    ///
    /// When Node A completes, it calls this for each downstream node that needs the value.
    /// Uses UPSERT to overwrite existing values for the same (target, variable, spread_index),
    /// preventing unbounded inbox growth during loops.
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
            ON CONFLICT (instance_id, target_node_id, variable_name, COALESCE(spread_index, -1))
            DO UPDATE SET value = EXCLUDED.value, source_node_id = EXCLUDED.source_node_id, created_at = NOW()
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

    /// Reset node readiness counter to 0 for loop re-triggering.
    /// This is used when a for-loop body needs to be re-dispatched for the next iteration.
    pub async fn reset_node_readiness(
        &self,
        instance_id: WorkflowInstanceId,
        node_id: &str,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE node_readiness
            SET completed_count = 0, updated_at = NOW()
            WHERE instance_id = $1 AND node_id = $2
            "#,
        )
        .bind(instance_id.0)
        .bind(node_id)
        .execute(&self.pool)
        .await?;

        Ok(())
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

        // Detect overflow (should never happen) to avoid duplicate enqueues
        if completed_count > required_count {
            tx.rollback().await?;
            return Err(DbError::NotFound(format!(
                "Readiness overflow for node {}: {} > {}",
                node_id, completed_count, required_count
            )));
        }

        // When this increment makes the node ready, enqueue a barrier in the same transaction.
        if completed_count == required_count {
            // Get the next action sequence for this instance
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

            // Enqueue the barrier; processing happens in the runner's barrier handler.
            sqlx::query(
                r#"
                INSERT INTO action_queue
                    (instance_id, action_seq, module_name, action_name,
                     dispatch_payload, timeout_seconds, max_retries,
                     backoff_kind, backoff_base_delay_ms, node_id, node_type, scheduled_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'barrier', NOW())
                "#,
            )
            .bind(instance_id.0)
            .bind(action_seq)
            .bind("__internal__")
            .bind("__barrier__")
            .bind(Vec::<u8>::new())
            .bind(300)
            .bind(3)
            .bind("exponential")
            .bind(1000)
            .bind(node_id)
            .execute(&mut *tx)
            .await?;
        }

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
    #[allow(clippy::too_many_arguments)]
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
    ) -> DbResult<
        std::collections::HashMap<String, std::collections::HashMap<String, serde_json::Value>>,
    > {
        if node_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let node_ids_vec: Vec<&str> = node_ids.iter().map(|s| s.as_str()).collect();

        // With UPSERT, each (target_node_id, variable_name, spread_index) is unique,
        // so we just need a simple SELECT. Non-spread variables (spread_index IS NULL)
        // are also unique due to the COALESCE(-1) in the unique index.
        let rows: Vec<(String, String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT target_node_id, variable_name, value
            FROM node_inputs
            WHERE instance_id = $1 AND target_node_id = ANY($2)
            "#,
        )
        .bind(instance_id.0)
        .bind(&node_ids_vec)
        .fetch_all(&self.pool)
        .await?;

        // Group by target_node_id
        let mut result: std::collections::HashMap<
            String,
            std::collections::HashMap<String, serde_json::Value>,
        > = std::collections::HashMap::new();

        for (target_node_id, var_name, value) in rows {
            if var_name == "__loop_loop_8_i" {
                debug!(
                    target_node_id = %target_node_id,
                    variable_name = %var_name,
                    value = ?value,
                    "DB reading __loop_loop_8_i from inbox"
                );
            }
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
        let start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;
        let mut result = CompletionResult::default();

        // 1. Mark the completed action as complete (idempotent guard)
        // Also update the action log entry with completion information
        if let Some(action_id) = plan.completed_action_id
            && let Some(delivery_token) = plan.delivery_token
        {
            let row = sqlx::query(
                r#"
                WITH updated AS (
                    UPDATE action_queue
                    SET status = CASE WHEN $2 THEN 'completed' ELSE 'failed' END,
                        success = $2,
                        result_payload = $3,
                        last_error = $4,
                        completed_at = NOW()
                    WHERE id = $1 AND delivery_token = $5 AND status = 'dispatched'
                    RETURNING id, attempt_number, dispatched_at
                ),
                log_update AS (
                    UPDATE action_logs
                    SET completed_at = NOW(),
                        success = $2,
                        result_payload = $3,
                        error_message = $4,
                        duration_ms = EXTRACT(EPOCH FROM (NOW() - updated.dispatched_at)) * 1000
                    FROM updated
                    WHERE action_logs.action_id = updated.id
                      AND action_logs.attempt_number = updated.attempt_number
                )
                SELECT COUNT(*) FROM updated
                "#,
            )
            .bind(action_id.0)
            .bind(plan.success)
            .bind(&plan.result_payload)
            .bind(&plan.error_message)
            .bind(delivery_token)
            .fetch_one(&mut *tx)
            .await?;

            let count: i64 = row.get(0);
            if count == 0 {
                // Stale or duplicate completion - roll back
                tx.rollback().await?;
                return Ok(CompletionResult::stale());
            }
        }

        // 2. Write inbox entries for all frontier nodes
        for write in &plan.inbox_writes {
            if write.variable_name == "__loop_loop_8_i" {
                debug!(
                    target_node_id = %write.target_node_id,
                    variable_name = %write.variable_name,
                    value = ?write.value,
                    source_node_id = %write.source_node_id,
                    "DB writing __loop_loop_8_i to inbox"
                );
            }
            sqlx::query(
                r#"
                INSERT INTO node_inputs
                    (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (instance_id, target_node_id, variable_name, COALESCE(spread_index, -1))
                DO UPDATE SET value = EXCLUDED.value, source_node_id = EXCLUDED.source_node_id, created_at = NOW()
                "#,
            )
            .bind(instance_id.0)
            .bind(&write.target_node_id)
            .bind(&write.variable_name)
            .bind(write.value.to_json())
            .bind(&write.source_node_id)
            .bind(write.spread_index)
            .execute(&mut *tx)
            .await?;
        }

        // 2.5. Reset readiness for loop re-triggering (before increments)
        for node_id in &plan.readiness_resets {
            sqlx::query(
                r#"
                UPDATE node_readiness
                SET completed_count = 0, updated_at = NOW()
                WHERE instance_id = $1 AND node_id = $2
                "#,
            )
            .bind(instance_id.0)
            .bind(node_id)
            .execute(&mut *tx)
            .await?;
        }

        // 2.75. Initialize readiness for frontier nodes (before increments)
        for init in &plan.readiness_inits {
            sqlx::query(
                r#"
                INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)
                VALUES ($1, $2, $3, 0)
                ON CONFLICT (instance_id, node_id)
                DO UPDATE SET required_count = EXCLUDED.required_count,
                              completed_count = 0,
                              updated_at = NOW()
                "#,
            )
            .bind(instance_id.0)
            .bind(&init.node_id)
            .bind(init.required_count)
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

            let is_immediately_ready = completed_count == required_count;

            // If this increment made the node ready, enqueue it
            if is_immediately_ready {
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
                         backoff_kind, backoff_base_delay_ms, node_id, node_type,
                         scheduled_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($12, NOW()))
                    "#,
                )
                .bind(instance_id.0)
                .bind(action_seq)
                .bind(increment.module_name.as_deref().unwrap_or("__internal__"))
                .bind(increment.action_name.as_deref().unwrap_or("__barrier__"))
                .bind(increment.dispatch_payload.as_deref().unwrap_or(&[]))
                .bind(increment.timeout_seconds)
                .bind(increment.max_retries)
                .bind(increment.backoff_kind.as_str())
                .bind(increment.backoff_base_delay_ms)
                .bind(&increment.node_id)
                .bind(increment.node_type.as_str())
                .bind(increment.scheduled_at)
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

        // 3.5. Enqueue barriers directly (no readiness tracking)
        for node_id in &plan.barrier_enqueues {
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

            sqlx::query(
                r#"
                INSERT INTO action_queue
                    (instance_id, action_seq, module_name, action_name,
                     dispatch_payload, timeout_seconds, max_retries,
                     backoff_kind, backoff_base_delay_ms, node_id, node_type,
                     scheduled_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'barrier', NOW())
                "#,
            )
            .bind(instance_id.0)
            .bind(action_seq)
            .bind("__internal__")
            .bind("__barrier__")
            .bind(Vec::<u8>::new())
            .bind(300)
            .bind(3)
            .bind("exponential")
            .bind(1000)
            .bind(node_id)
            .execute(&mut *tx)
            .await?;

            result.newly_ready_nodes.push(node_id.clone());
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

        tracing::info!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            inbox_writes = plan.inbox_writes.len(),
            readiness_inits = plan.readiness_inits.len(),
            readiness_increments = plan.readiness_increments.len(),
            barrier_enqueues = plan.barrier_enqueues.len(),
            "execute_completion_plan"
        );

        Ok(result)
    }

    // ========================================================================
    // Workflow Schedules
    // ========================================================================

    /// Get the latest workflow version ID for a workflow name.
    /// Used by the scheduler to find which version to run.
    pub async fn get_latest_workflow_version(
        &self,
        workflow_name: &str,
    ) -> DbResult<Option<WorkflowVersionId>> {
        let row: Option<(Uuid,)> = sqlx::query_as(
            r#"
            SELECT id
            FROM workflow_versions
            WHERE workflow_name = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(workflow_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(id,)| WorkflowVersionId(id)))
    }

    /// Upsert a workflow schedule (insert or update by workflow_name + schedule_name).
    /// Returns the schedule ID.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_schedule(
        &self,
        workflow_name: &str,
        schedule_name: &str,
        schedule_type: ScheduleType,
        cron_expression: Option<&str>,
        interval_seconds: Option<i64>,
        jitter_seconds: i64,
        input_payload: Option<&[u8]>,
        next_run_at: DateTime<Utc>,
    ) -> DbResult<ScheduleId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_schedules
                (workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds, input_payload, next_run_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (workflow_name, schedule_name)
            DO UPDATE SET
                schedule_type = EXCLUDED.schedule_type,
                cron_expression = EXCLUDED.cron_expression,
                interval_seconds = EXCLUDED.interval_seconds,
                jitter_seconds = EXCLUDED.jitter_seconds,
                input_payload = EXCLUDED.input_payload,
                next_run_at = EXCLUDED.next_run_at,
                status = 'active',
                updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(schedule_name)
        .bind(schedule_type.as_str())
        .bind(cron_expression)
        .bind(interval_seconds)
        .bind(jitter_seconds)
        .bind(input_payload)
        .bind(next_run_at)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        Ok(ScheduleId(id))
    }

    /// Get a schedule by workflow name and schedule name.
    pub async fn get_schedule_by_name(
        &self,
        workflow_name: &str,
        schedule_name: &str,
    ) -> DbResult<Option<WorkflowSchedule>> {
        let schedule = sqlx::query_as::<_, WorkflowSchedule>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds,
                   input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at
            FROM workflow_schedules
            WHERE workflow_name = $1 AND schedule_name = $2 AND status != 'deleted'
            "#,
        )
        .bind(workflow_name)
        .bind(schedule_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(schedule)
    }

    /// Find due schedules for the scheduler loop.
    /// Uses FOR UPDATE SKIP LOCKED for multi-runner safety.
    pub async fn find_due_schedules(&self, limit: i32) -> DbResult<Vec<WorkflowSchedule>> {
        let schedules = sqlx::query_as::<_, WorkflowSchedule>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds,
                   input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at
            FROM workflow_schedules
            WHERE status = 'active'
              AND next_run_at IS NOT NULL
              AND next_run_at <= NOW()
            ORDER BY next_run_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(schedules)
    }

    /// Mark a schedule as executed and update next_run_at.
    /// Called after creating an instance for a scheduled workflow.
    pub async fn mark_schedule_executed(
        &self,
        schedule_id: ScheduleId,
        instance_id: WorkflowInstanceId,
        next_run_at: DateTime<Utc>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET last_run_at = NOW(),
                last_instance_id = $2,
                next_run_at = $3,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id.0)
        .bind(instance_id.0)
        .bind(next_run_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update next_run_at without creating an instance.
    /// Used when skipping a scheduled run (e.g., no workflow version exists).
    pub async fn update_schedule_next_run(
        &self,
        schedule_id: ScheduleId,
        next_run_at: DateTime<Utc>,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET next_run_at = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id.0)
        .bind(next_run_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update schedule status (pause/resume).
    pub async fn update_schedule_status(
        &self,
        workflow_name: &str,
        schedule_name: &str,
        status: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = $3, updated_at = NOW()
            WHERE workflow_name = $1 AND schedule_name = $2 AND status != 'deleted'
            "#,
        )
        .bind(workflow_name)
        .bind(schedule_name)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete a schedule (soft delete).
    pub async fn delete_schedule(
        &self,
        workflow_name: &str,
        schedule_name: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = 'deleted', updated_at = NOW()
            WHERE workflow_name = $1 AND schedule_name = $2 AND status != 'deleted'
            "#,
        )
        .bind(workflow_name)
        .bind(schedule_name)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// List all schedules with optional status filter.
    pub async fn list_schedules(
        &self,
        status_filter: Option<&str>,
    ) -> DbResult<Vec<WorkflowSchedule>> {
        let schedules = if let Some(status) = status_filter {
            sqlx::query_as::<_, WorkflowSchedule>(
                r#"
                SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds,
                       input_payload, status, next_run_at, last_run_at, last_instance_id,
                       created_at, updated_at
                FROM workflow_schedules
                WHERE status = $1
                ORDER BY workflow_name, schedule_name
                "#,
            )
            .bind(status)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, WorkflowSchedule>(
                r#"
                SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds,
                       input_payload, status, next_run_at, last_run_at, last_instance_id,
                       created_at, updated_at
                FROM workflow_schedules
                WHERE status != 'deleted'
                ORDER BY workflow_name, schedule_name
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        };

        Ok(schedules)
    }

    // ========================================================================
    // Worker Status
    // ========================================================================

    pub async fn upsert_worker_statuses(
        &self,
        pool_id: Uuid,
        statuses: &[WorkerStatusUpdate],
    ) -> DbResult<()> {
        if statuses.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        for status in statuses {
            sqlx::query(
                r#"
                INSERT INTO worker_status (
                    pool_id,
                    worker_id,
                    throughput_per_min,
                    total_completed,
                    last_action_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (pool_id, worker_id)
                DO UPDATE SET
                    throughput_per_min = EXCLUDED.throughput_per_min,
                    total_completed = EXCLUDED.total_completed,
                    last_action_at = EXCLUDED.last_action_at,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(pool_id)
            .bind(status.worker_id)
            .bind(status.throughput_per_min)
            .bind(status.total_completed)
            .bind(status.last_action_at)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    // ========================================================================
    // Garbage Collection
    // ========================================================================

    /// Garbage collect old completed/failed workflow instances and their associated data.
    ///
    /// This function cleans up instances that have been in a terminal state (completed or failed)
    /// for longer than the specified retention period. It also cleans up all associated data:
    /// - action_queue entries
    /// - action_logs entries
    /// - instance_context entries
    /// - loop_state entries
    /// - node_readiness entries
    /// - node_inputs entries
    ///
    /// Uses FOR UPDATE SKIP LOCKED for multi-host safety - multiple runners can run GC
    /// concurrently without blocking each other.
    ///
    /// Returns the number of instances that were garbage collected.
    pub async fn garbage_collect_instances(
        &self,
        retention_seconds: i64,
        limit: i32,
    ) -> DbResult<i64> {
        // Find and lock instances to delete using SKIP LOCKED
        // This allows multiple workers to run GC concurrently without blocking
        let result = sqlx::query(
            r#"
            WITH instances_to_delete AS (
                SELECT id
                FROM workflow_instances
                WHERE status IN ('completed', 'failed')
                  AND completed_at IS NOT NULL
                  AND completed_at < NOW() - ($1 || ' seconds')::interval
                ORDER BY completed_at
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            ),
            -- Delete action_logs first (references action_queue)
            deleted_logs AS (
                DELETE FROM action_logs
                WHERE instance_id IN (SELECT id FROM instances_to_delete)
            ),
            -- Delete action_queue entries
            deleted_actions AS (
                DELETE FROM action_queue
                WHERE instance_id IN (SELECT id FROM instances_to_delete)
            ),
            -- Delete instance_context entries
            deleted_context AS (
                DELETE FROM instance_context
                WHERE instance_id IN (SELECT id FROM instances_to_delete)
            ),
            -- Delete loop_state entries
            deleted_loop_state AS (
                DELETE FROM loop_state
                WHERE instance_id IN (SELECT id FROM instances_to_delete)
            ),
            -- Delete node_readiness entries
            deleted_readiness AS (
                DELETE FROM node_readiness
                WHERE instance_id IN (SELECT id FROM instances_to_delete)
            ),
            -- Delete node_inputs entries
            deleted_inputs AS (
                DELETE FROM node_inputs
                WHERE instance_id IN (SELECT id FROM instances_to_delete)
            ),
            -- Finally delete the instances themselves
            deleted_instances AS (
                DELETE FROM workflow_instances
                WHERE id IN (SELECT id FROM instances_to_delete)
                RETURNING id
            )
            SELECT COUNT(*) FROM deleted_instances
            "#,
        )
        .bind(retention_seconds)
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

        let count: i64 = result.get(0);
        if count > 0 {
            tracing::info!(
                count = count,
                retention_seconds = retention_seconds,
                "garbage_collect_instances"
            );
        }

        Ok(count)
    }
}
