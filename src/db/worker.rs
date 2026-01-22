//! High-performance database operations for the worker dispatch loop.
//!
//! These operations are hyper-optimized and should never be slowed down
//! by webapp features. All operations here are on the critical path for
//! workflow execution.

use sqlx::{Postgres, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use tracing::debug;
use uuid::Uuid;

use crate::completion::{
    CompletionPlan, CompletionResult, InboxWrite, InstanceCompletion, NodeType, ReadinessIncrement,
    ReadinessInit,
};

use chrono::{DateTime, Utc};

use super::{
    ActionId, BackoffKind, CompletionRecord, Database, DbError, DbResult, LoopState, NewAction,
    QueuedAction, ReadinessResult, ScheduleId, ScheduleType, WorkerStatusUpdate, WorkflowInstance,
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
        ir_hash: &str,
        program_proto: &[u8],
        concurrent: bool,
    ) -> DbResult<WorkflowVersionId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_versions (workflow_name, ir_hash, program_proto, concurrent)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (workflow_name, ir_hash) DO UPDATE SET workflow_name = EXCLUDED.workflow_name
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(ir_hash)
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
            SELECT id, workflow_name, ir_hash, program_proto, concurrent, created_at
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
        self.create_instance_with_priority(workflow_name, version_id, input_payload, schedule_id, 0)
            .await
    }

    /// Create a new workflow instance with a specific priority
    pub async fn create_instance_with_priority(
        &self,
        workflow_name: &str,
        version_id: WorkflowVersionId,
        input_payload: Option<&[u8]>,
        schedule_id: Option<ScheduleId>,
        priority: i32,
    ) -> DbResult<WorkflowInstanceId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_instances (workflow_name, workflow_version_id, input_payload, schedule_id, priority)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(version_id.0)
        .bind(input_payload)
        .bind(schedule_id.map(|id| id.0))
        .bind(priority)
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

    /// Create workflow instances in bulk with a specific priority (returns instance IDs).
    pub async fn create_instances_batch_with_priority(
        &self,
        workflow_name: &str,
        version_id: WorkflowVersionId,
        input_payloads: &[Option<Vec<u8>>],
        schedule_id: Option<ScheduleId>,
        priority: i32,
    ) -> DbResult<Vec<WorkflowInstanceId>> {
        if input_payloads.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r#"
            WITH new_instances AS (
                INSERT INTO workflow_instances
                    (workflow_name, workflow_version_id, input_payload, schedule_id, priority)
                SELECT $1, $2, payload, $3, $4
                FROM UNNEST($5::bytea[]) AS payload
                RETURNING id
            )
            INSERT INTO instance_context (instance_id)
            SELECT id FROM new_instances
            RETURNING instance_id
            "#,
        )
        .bind(workflow_name)
        .bind(version_id.0)
        .bind(schedule_id.map(|id| id.0))
        .bind(priority)
        .bind(input_payloads)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| WorkflowInstanceId(row.get::<Uuid, _>("instance_id")))
            .collect())
    }

    /// Create workflow instances in bulk with a specific priority (returns count).
    pub async fn create_instances_batch_count_with_priority(
        &self,
        workflow_name: &str,
        version_id: WorkflowVersionId,
        input_payloads: &[Option<Vec<u8>>],
        schedule_id: Option<ScheduleId>,
        priority: i32,
    ) -> DbResult<i64> {
        if input_payloads.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            WITH new_instances AS (
                INSERT INTO workflow_instances
                    (workflow_name, workflow_version_id, input_payload, schedule_id, priority)
                SELECT $1, $2, payload, $3, $4
                FROM UNNEST($5::bytea[]) AS payload
                RETURNING id
            )
            INSERT INTO instance_context (instance_id)
            SELECT id FROM new_instances
            "#,
        )
        .bind(workflow_name)
        .bind(version_id.0)
        .bind(schedule_id.map(|id| id.0))
        .bind(priority)
        .bind(input_payloads)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Claim instances that need to be started (running but no actions created yet).
    /// Orders by priority DESC (higher priority first), then by created_at ASC.
    pub async fn find_unstarted_instances(
        &self,
        limit: i32,
        stale_before: DateTime<Utc>,
    ) -> DbResult<Vec<WorkflowInstance>> {
        let instances = sqlx::query_as::<_, WorkflowInstance>(
            r#"
            WITH claimed AS (
                SELECT id
                FROM workflow_instances
                WHERE status = 'running'
                  AND next_action_seq = 0
                  AND (started_at IS NULL OR started_at < $2)
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE workflow_instances i
            SET started_at = NOW()
            FROM claimed
            WHERE i.id = claimed.id
            RETURNING i.id, i.partition_id, i.workflow_name, i.workflow_version_id,
                      i.schedule_id, i.next_action_seq, i.input_payload, i.result_payload, i.status,
                      i.created_at, i.completed_at, i.priority
            "#,
        )
        .bind(limit)
        .bind(stale_before)
        .fetch_all(&self.pool)
        .await?;

        Ok(instances)
    }

    /// Clear started_at for a running instance with no actions enqueued.
    pub async fn clear_instance_started_at(&self, id: WorkflowInstanceId) -> DbResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_instances
            SET started_at = NULL
            WHERE id = $1
              AND status = 'running'
              AND next_action_seq = 0
            "#,
        )
        .bind(id.0)
        .execute(&self.pool)
        .await?;

        Ok(())
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
                backoff_kind, backoff_base_delay_ms, node_id, node_type, priority
            )
            SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11, 'action'), wi.priority
            FROM workflow_instances wi
            WHERE wi.id = $1
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
    /// 5. Returns the actions for execution
    /// 6. Creates action log entries for tracking run history (non-blocking)
    ///
    /// Actions are ordered by priority (higher first), then by scheduled_at, then by action_seq.
    pub async fn dispatch_actions(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let start = std::time::Instant::now();

        // Use a transaction to ensure atomicity of dispatch + log creation
        let mut tx = self.pool.begin().await?;

        // Step 1: Select, lock, and update actions in a single CTE
        // Note: log_insert is moved outside the CTE to reduce critical path latency
        let rows = sqlx::query(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM action_queue
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                ORDER BY priority DESC, scheduled_at, action_seq
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
                COALESCE(aq.node_type, 'action') as node_type,
                aq.dispatched_at
            "#,
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        // Parse results - logs are created on completion, not dispatch
        let actions: Vec<QueuedAction> = rows
            .iter()
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

        tx.commit().await?;

        let count = actions.len();
        if count > 0 {
            tracing::debug!(
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
    /// Orders by priority (higher first).
    pub async fn dispatch_actions_only(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let rows = sqlx::query(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM action_queue
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                  AND (node_type = 'action' OR node_type IS NULL)
                ORDER BY priority DESC, scheduled_at, action_seq
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
            )
            -- Logs are created on completion, not dispatch
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
    /// Orders by priority (higher first).
    pub async fn dispatch_barriers_only(&self, limit: i32) -> DbResult<Vec<QueuedAction>> {
        let rows = sqlx::query(
            r#"
            WITH next_barriers AS (
                SELECT id
                FROM action_queue
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                  AND node_type = 'barrier'
                ORDER BY priority DESC, scheduled_at, action_seq
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
            )
            -- Logs are created on completion, not dispatch
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
    /// Logs are persisted via background flush for successes; failures log immediately.
    ///
    /// On success: Deletes the action row and enqueues a log entry.
    /// On failure: Updates status to 'failed' so retry logic can process it
    pub async fn complete_action(&self, record: CompletionRecord) -> DbResult<bool> {
        let result = if record.success {
            // Success: delete action row and enqueue log entry.
            sqlx::query(
                r#"
                WITH deleted AS (
                    DELETE FROM action_queue
                    WHERE id = $1 AND delivery_token = $2 AND status = 'dispatched'
                    RETURNING id, instance_id, attempt_number, dispatched_at,
                              module_name, action_name, node_id, dispatch_payload, enqueued_at
                ),
                log_insert AS (
                    INSERT INTO action_log_queue (action_id, instance_id, attempt_number, dispatched_at,
                                            completed_at, success, result_payload, error_message, duration_ms,
                                            module_name, action_name, node_id, dispatch_payload,
                                            pool_id, worker_id, enqueued_at, worker_duration_ms)
                    SELECT id, instance_id, attempt_number, dispatched_at, NOW(), true, $3, NULL,
                           EXTRACT(EPOCH FROM (NOW() - dispatched_at)) * 1000,
                           module_name, action_name, node_id, dispatch_payload,
                           $4, $5, enqueued_at, $6
                    FROM deleted
                )
                SELECT COUNT(*) FROM deleted
                "#,
            )
            .bind(record.action_id.0)
            .bind(record.delivery_token)
            .bind(&record.result_payload)
            .bind(record.pool_id)
            .bind(record.worker_id)
            .bind(record.worker_duration_ms)
            .fetch_one(&self.pool)
            .await?
        } else {
            // Failure: UPDATE status so retry logic can process, INSERT log with full context
            sqlx::query(
                r#"
                WITH updated AS (
                    UPDATE action_queue
                    SET status = 'failed',
                        success = false,
                        result_payload = $3,
                        last_error = $4,
                        completed_at = NOW()
                    WHERE id = $1 AND delivery_token = $2 AND status = 'dispatched'
                    RETURNING id, instance_id, attempt_number, dispatched_at,
                              module_name, action_name, node_id, dispatch_payload, enqueued_at
                ),
                log_insert AS (
                    INSERT INTO action_log_queue (action_id, instance_id, attempt_number, dispatched_at,
                                            completed_at, success, result_payload, error_message, duration_ms,
                                            module_name, action_name, node_id, dispatch_payload,
                                            pool_id, worker_id, enqueued_at, worker_duration_ms)
                    SELECT id, instance_id, attempt_number, dispatched_at, NOW(), false, $3, $4,
                           EXTRACT(EPOCH FROM (NOW() - dispatched_at)) * 1000,
                           module_name, action_name, node_id, dispatch_payload,
                           $5, $6, enqueued_at, $7
                    FROM updated
                )
                SELECT COUNT(*) FROM updated
                "#,
            )
            .bind(record.action_id.0)
            .bind(record.delivery_token)
            .bind(&record.result_payload)
            .bind(&record.error_message)
            .bind(record.pool_id)
            .bind(record.worker_id)
            .bind(record.worker_duration_ms)
            .fetch_one(&self.pool)
            .await?
        };

        let count: i64 = result.get(0);
        Ok(count > 0)
    }

    /// Mark timed-out actions as failed.
    ///
    /// This finds dispatched actions past their deadline and marks them as 'failed'
    /// with retry_kind='timeout'. The actual retry/requeue logic is handled by
    /// `requeue_failed_actions`, which processes both timeout and explicit failures.
    /// Also inserts action log entries with timeout information.
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
                RETURNING aq.id, aq.instance_id, aq.attempt_number, aq.dispatched_at,
                          aq.module_name, aq.action_name, aq.node_id, aq.dispatch_payload
            ),
            log_insert AS (
                INSERT INTO action_log_queue (action_id, instance_id, attempt_number, dispatched_at,
                                        completed_at, success, error_message, duration_ms,
                                        module_name, action_name, node_id, dispatch_payload)
                SELECT id, instance_id, attempt_number, dispatched_at, NOW(), false, 'Action timed out',
                       EXTRACT(EPOCH FROM (NOW() - dispatched_at)) * 1000,
                       module_name, action_name, node_id, dispatch_payload
                FROM updated
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

    /// Flush buffered action logs into action_logs in bulk.
    ///
    /// Returns the number of logs flushed.
    pub async fn flush_action_log_queue(&self, limit: i64) -> DbResult<i64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            WITH moved AS (
                DELETE FROM action_log_queue
                WHERE id IN (
                    SELECT id
                    FROM action_log_queue
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                )
                RETURNING action_id, instance_id, attempt_number, dispatched_at,
                          completed_at, success, result_payload, error_message, duration_ms,
                          module_name, action_name, node_id, dispatch_payload,
                          pool_id, worker_id, enqueued_at, worker_duration_ms
            ),
            inserted AS (
                INSERT INTO action_logs (
                    action_id, instance_id, attempt_number, dispatched_at,
                    completed_at, success, result_payload, error_message, duration_ms,
                    module_name, action_name, node_id, dispatch_payload,
                    pool_id, worker_id, enqueued_at, worker_duration_ms
                )
                SELECT action_id, instance_id, attempt_number, dispatched_at,
                       completed_at, success, result_payload, error_message, duration_ms,
                       module_name, action_name, node_id, dispatch_payload,
                       pool_id, worker_id, enqueued_at, worker_duration_ms
                FROM moved
                WHERE EXISTS (
                    SELECT 1
                    FROM workflow_instances
                    WHERE id = moved.instance_id
                )
                RETURNING 1
            )
            SELECT COUNT(*) FROM inserted
            "#,
        )
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Flush completed actions from action_queue into action_logs in bulk.
    ///
    /// Returns the number of logs flushed.
    pub async fn flush_completed_actions(&self, limit: i64) -> DbResult<i64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            WITH moved AS (
                DELETE FROM action_queue
                WHERE id IN (
                    SELECT id
                    FROM action_queue
                    WHERE status = 'completed'
                    ORDER BY completed_at NULLS LAST, id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                )
                RETURNING id, instance_id, attempt_number, dispatched_at, completed_at,
                          success, result_payload, last_error, module_name, action_name,
                          node_id, dispatch_payload, enqueued_at
            ),
            inserted AS (
                INSERT INTO action_logs (
                    action_id, instance_id, attempt_number, dispatched_at,
                    completed_at, success, result_payload, error_message, duration_ms,
                    module_name, action_name, node_id, dispatch_payload,
                    enqueued_at
                )
                SELECT id, instance_id, attempt_number, dispatched_at,
                       completed_at, success, result_payload, last_error,
                       CASE
                           WHEN dispatched_at IS NULL OR completed_at IS NULL THEN NULL
                           ELSE EXTRACT(EPOCH FROM (completed_at - dispatched_at)) * 1000
                       END,
                       module_name, action_name, node_id, dispatch_payload,
                       enqueued_at
                FROM moved
                RETURNING 1
            )
            SELECT COUNT(*) FROM inserted
            "#,
        )
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

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

        // Debug: log when we check but find nothing to requeue
        if requeued_count == 0 {
            tracing::debug!(
                limit = limit,
                "requeue_failed_actions: no retryable actions found"
            );
        }

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
        //
        // Query is structured to use idx_action_queue_exhausted efficiently:
        // start from the smaller set (exhausted actions) and join to instances.
        let result = sqlx::query(
            r#"
            WITH instances_to_fail AS (
                SELECT DISTINCT aq.instance_id as id
                FROM action_queue aq
                JOIN workflow_instances wi ON wi.id = aq.instance_id
                WHERE aq.status IN ('exhausted', 'timed_out')
                  AND wi.status = 'running'
                LIMIT $1
            )
            UPDATE workflow_instances wi
            SET status = 'failed',
                completed_at = NOW()
            FROM instances_to_fail
            WHERE wi.id = instances_to_fail.id
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

    /// Write a value to a node's inbox (O(1) insert, append-only).
    ///
    /// When Node A completes, it calls this for each downstream node that needs the value.
    /// The inbox is append-only; reads pick the latest value per variable.
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

    /// Fetch the latest inbox update timestamp for cache invalidation.
    pub async fn get_inbox_updated_at(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> DbResult<DateTime<Utc>> {
        let updated_at: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"
            SELECT inbox_updated_at
            FROM workflow_instances
            WHERE id = $1
            "#,
        )
        .bind(instance_id.0)
        .fetch_optional(&self.pool)
        .await?;

        updated_at.ok_or_else(|| {
            DbError::NotFound(format!(
                "workflow instance {} (inbox updated_at)",
                instance_id.0
            ))
        })
    }

    /// Mark inbox updated_at for a set of instances after inbox writes.
    pub async fn touch_inbox_updated_at(
        &self,
        instance_ids: &[Uuid],
    ) -> DbResult<HashMap<Uuid, DateTime<Utc>>> {
        if instance_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET inbox_updated_at = NOW()
            WHERE id = ANY($1)
            RETURNING id, inbox_updated_at
            "#,
        )
        .bind(instance_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut updated = HashMap::with_capacity(rows.len());
        for row in rows {
            let id: Uuid = row.get(0);
            let updated_at: DateTime<Utc> = row.get(1);
            updated.insert(id, updated_at);
        }

        Ok(updated)
    }

    /// Read all pending inputs for a node (single query).
    ///
    /// Returns a map of variable_name -> latest value (non-spread only).
    pub async fn read_inbox(
        &self,
        instance_id: WorkflowInstanceId,
        target_node_id: &str,
    ) -> DbResult<std::collections::HashMap<String, serde_json::Value>> {
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (variable_name) variable_name, value
            FROM node_inputs
            WHERE instance_id = $1
              AND target_node_id = $2
              AND spread_index IS NULL
            ORDER BY variable_name, created_at DESC
            "#,
        )
        .bind(instance_id.0)
        .bind(target_node_id)
        .fetch_all(&self.pool)
        .await?;

        let mut result = std::collections::HashMap::new();
        for (var_name, value) in rows {
            result.insert(var_name, value);
        }
        Ok(result)
    }

    /// Read spread/parallel results for an aggregator node.
    ///
    /// Returns latest (spread_index, value) tuples for ordering.
    pub async fn read_inbox_for_aggregator(
        &self,
        instance_id: WorkflowInstanceId,
        target_node_id: &str,
    ) -> DbResult<Vec<(i32, serde_json::Value)>> {
        let rows: Vec<(i32, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (spread_index) spread_index, value
            FROM node_inputs
            WHERE instance_id = $1
              AND target_node_id = $2
              AND spread_index IS NOT NULL
            ORDER BY spread_index, created_at DESC
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

    /// Compact inbox rows by removing older duplicates per variable/spread index.
    ///
    /// Keeps the latest row per (instance_id, target_node_id, variable_name, spread_index).
    pub async fn compact_node_inputs(&self, min_age_seconds: i64, limit: i64) -> DbResult<i64> {
        if limit <= 0 {
            return Ok(0);
        }

        let count: i64 = sqlx::query_scalar(
            r#"
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY instance_id, target_node_id, variable_name, COALESCE(spread_index, -1)
                           ORDER BY created_at DESC
                       ) AS rn
                FROM node_inputs
                WHERE created_at < NOW() - ($1 || ' seconds')::interval
            ),
            deleted AS (
                DELETE FROM node_inputs
                WHERE id IN (
                    SELECT id
                    FROM ranked
                    WHERE rn > 1
                    LIMIT $2
                )
                RETURNING 1
            )
            SELECT COUNT(*) FROM deleted
            "#,
        )
        .bind(min_age_seconds)
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

        if count > 0 {
            tracing::info!(count = count, "compact_node_inputs");
        }

        Ok(count)
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

        // Write all inbox entries in a single insert.
        if !inbox_writes.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO node_inputs (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)",
            );
            builder.push_values(inbox_writes, |mut row, write| {
                row.push_bind(instance_id.0)
                    .push_bind(&write.0)
                    .push_bind(&write.1)
                    .push_bind(&write.2)
                    .push_bind(&write.3)
                    .push_bind(write.4);
            });
            builder.build().execute(&mut *tx).await?;

            sqlx::query(
                r#"
                UPDATE workflow_instances
                SET inbox_updated_at = NOW()
                WHERE id = $1
                "#,
            )
            .bind(instance_id.0)
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
                     backoff_kind, backoff_base_delay_ms, node_id, node_type, scheduled_at, priority)
                SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'barrier', NOW(), wi.priority
                FROM workflow_instances wi
                WHERE wi.id = $1
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

        sqlx::query(
            r#"
            UPDATE workflow_instances
            SET inbox_updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(instance_id.0)
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

        // Append-only inbox; pick latest non-spread value per variable.
        let rows: Vec<(String, String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (target_node_id, variable_name) target_node_id, variable_name, value
            FROM node_inputs
            WHERE instance_id = $1
              AND target_node_id = ANY($2)
              AND spread_index IS NULL
            ORDER BY target_node_id, variable_name, created_at DESC
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
            SELECT DISTINCT ON (target_node_id, spread_index) target_node_id, spread_index, value
            FROM node_inputs
            WHERE instance_id = $1
              AND target_node_id = ANY($2)
              AND spread_index IS NOT NULL
            ORDER BY target_node_id, spread_index, created_at DESC
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

    /// Execute multiple completion plans in a single transaction.
    pub async fn execute_completion_plans_batch(
        &self,
        plans: Vec<(WorkflowInstanceId, CompletionPlan)>,
    ) -> DbResult<Vec<CompletionResult>> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Debug)]
        struct BatchPlan {
            index: usize,
            instance_id: WorkflowInstanceId,
            inbox_writes: Vec<InboxWrite>,
            readiness_resets: Vec<String>,
            readiness_inits: Vec<ReadinessInit>,
            direct_increments: Vec<ReadinessIncrement>,
            tracked_increments: Vec<ReadinessIncrement>,
            barrier_enqueues: Vec<String>,
            instance_completion: Option<InstanceCompletion>,
        }

        struct BatchReadinessReset {
            instance_id: WorkflowInstanceId,
            node_id: String,
        }

        struct BatchReadinessInit {
            instance_id: WorkflowInstanceId,
            node_id: String,
            required_count: i32,
        }

        struct BatchReadinessIncrement {
            instance_id: WorkflowInstanceId,
            plan_index: usize,
            increment: ReadinessIncrement,
        }

        struct BatchBarrierEnqueue {
            instance_id: WorkflowInstanceId,
            plan_index: usize,
            node_id: String,
        }

        struct BatchInstanceCompletion {
            instance_id: WorkflowInstanceId,
            plan_index: usize,
            result_payload: Vec<u8>,
        }

        #[derive(Debug)]
        struct PendingEnqueue {
            instance_id: WorkflowInstanceId,
            plan_index: usize,
            node_id: String,
            module_name: String,
            action_name: String,
            dispatch_payload: Vec<u8>,
            timeout_seconds: i32,
            max_retries: i32,
            backoff_kind: BackoffKind,
            backoff_base_delay_ms: i32,
            node_type: NodeType,
            scheduled_at: Option<DateTime<Utc>>,
        }

        struct EnqueueRow {
            instance_id: WorkflowInstanceId,
            node_id: String,
            module_name: String,
            action_name: String,
            dispatch_payload: Vec<u8>,
            timeout_seconds: i32,
            max_retries: i32,
            backoff_kind: BackoffKind,
            backoff_base_delay_ms: i32,
            node_type: NodeType,
            scheduled_at: DateTime<Utc>,
            priority: i32,
            action_seq: i32,
        }

        #[derive(Default, Debug)]
        struct BatchDbTimings {
            action_complete_us: u64,
            inbox_insert_us: u64,
            readiness_reset_us: u64,
            readiness_init_us: u64,
            readiness_increment_us: u64,
            next_action_seq_us: u64,
            enqueue_insert_us: u64,
            instance_complete_us: u64,
            total_us: u64,
        }

        let total_start = std::time::Instant::now();
        let mut tx = self.pool.begin().await?;
        let mut results = vec![CompletionResult::default(); plans.len()];
        let mut timings = BatchDbTimings::default();

        let mut batch_plans = Vec::with_capacity(plans.len());
        let mut action_update_plan_indices = Vec::new();
        let mut action_ids = Vec::new();
        let mut delivery_tokens = Vec::new();
        let mut result_payloads = Vec::new();
        let mut pool_ids = Vec::new();
        let mut worker_ids = Vec::new();
        let mut worker_duration_ms_vec = Vec::new();

        for (index, (instance_id, plan)) in plans.into_iter().enumerate() {
            let CompletionPlan {
                completed_action_id,
                delivery_token,
                success: _,
                result_payload,
                error_message: _,
                pool_id,
                worker_id,
                worker_duration_ms,
                inbox_writes,
                readiness_resets,
                readiness_inits,
                readiness_increments,
                barrier_enqueues,
                instance_completion,
                ..
            } = plan;

            let mut direct_increments = Vec::new();
            let mut tracked_increments = Vec::new();
            for increment in readiness_increments {
                if increment.required_count == 1 && !increment.is_aggregator {
                    direct_increments.push(increment);
                } else {
                    tracked_increments.push(increment);
                }
            }

            let mut direct_nodes = HashSet::new();
            for increment in &direct_increments {
                direct_nodes.insert(increment.node_id.clone());
            }

            let readiness_resets: Vec<String> = readiness_resets
                .into_iter()
                .filter(|node_id| !direct_nodes.contains(node_id))
                .collect();
            let readiness_inits: Vec<_> = readiness_inits
                .into_iter()
                .filter(|init| !direct_nodes.contains(&init.node_id))
                .collect();

            if let (Some(action_id), Some(token)) = (completed_action_id, delivery_token) {
                action_update_plan_indices.push(index);
                action_ids.push(action_id.0);
                delivery_tokens.push(token);
                result_payloads.push(result_payload);
                pool_ids.push(pool_id);
                worker_ids.push(worker_id);
                worker_duration_ms_vec.push(worker_duration_ms);
            }

            batch_plans.push(BatchPlan {
                index,
                instance_id,
                inbox_writes,
                readiness_resets,
                readiness_inits,
                direct_increments,
                tracked_increments,
                barrier_enqueues,
                instance_completion,
            });
        }

        let mut is_fresh = vec![true; batch_plans.len()];

        if !action_ids.is_empty() {
            let step_start = std::time::Instant::now();
            let rows = sqlx::query(
                r#"
                WITH updates AS (
                    SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::bytea[], $4::uuid[], $5::bigint[], $6::bigint[])
                        WITH ORDINALITY
                        AS u(action_id, delivery_token, result_payload, pool_id, worker_id, worker_duration_ms, ord)
                ),
                deleted AS (
                    DELETE FROM action_queue aq
                    USING updates
                    WHERE aq.id = updates.action_id
                      AND aq.delivery_token = updates.delivery_token
                      AND aq.status = 'dispatched'
                    RETURNING updates.ord,
                              aq.id,
                              aq.instance_id,
                              aq.attempt_number,
                              aq.dispatched_at,
                              aq.module_name,
                              aq.action_name,
                              aq.node_id,
                              aq.dispatch_payload,
                              aq.enqueued_at,
                              updates.result_payload,
                              updates.pool_id,
                              updates.worker_id,
                              updates.worker_duration_ms
                ),
                log_insert AS (
                    INSERT INTO action_log_queue (
                        action_id, instance_id, attempt_number, dispatched_at,
                        completed_at, success, result_payload, error_message, duration_ms,
                        module_name, action_name, node_id, dispatch_payload,
                        pool_id, worker_id, enqueued_at, worker_duration_ms
                    )
                    SELECT id, instance_id, attempt_number, dispatched_at,
                           NOW(), true, result_payload, NULL,
                           EXTRACT(EPOCH FROM (NOW() - dispatched_at)) * 1000,
                           module_name, action_name, node_id, dispatch_payload,
                           pool_id, worker_id, enqueued_at, worker_duration_ms
                    FROM deleted
                )
                SELECT ord FROM deleted
                "#,
            )
            .bind(&action_ids)
            .bind(&delivery_tokens)
            .bind(&result_payloads)
            .bind(&pool_ids)
            .bind(&worker_ids)
            .bind(&worker_duration_ms_vec)
            .fetch_all(&mut *tx)
            .await?;
            timings.action_complete_us = step_start.elapsed().as_micros() as u64;

            let mut updated = vec![false; action_update_plan_indices.len()];
            for row in rows {
                let ord: i64 = row.get(0);
                if ord >= 1 {
                    let idx = (ord - 1) as usize;
                    if idx < updated.len() {
                        updated[idx] = true;
                    }
                }
            }

            for (update_idx, plan_index) in action_update_plan_indices.iter().enumerate() {
                if !updated[update_idx] {
                    is_fresh[*plan_index] = false;
                    results[*plan_index] = CompletionResult::stale();
                }
            }
        }

        let mut inbox_writes: Vec<InboxWrite> = Vec::new();
        let mut readiness_resets: Vec<BatchReadinessReset> = Vec::new();
        let mut readiness_inits: Vec<BatchReadinessInit> = Vec::new();
        let mut direct_increments: Vec<BatchReadinessIncrement> = Vec::new();
        let mut tracked_increments: Vec<BatchReadinessIncrement> = Vec::new();
        let mut barrier_enqueues: Vec<BatchBarrierEnqueue> = Vec::new();
        let mut instance_completions: Vec<BatchInstanceCompletion> = Vec::new();
        let mut inbox_updated_at: HashMap<Uuid, DateTime<Utc>> = HashMap::new();

        for plan in batch_plans.iter_mut() {
            if !is_fresh[plan.index] {
                continue;
            }

            inbox_writes.append(&mut plan.inbox_writes);
            for node_id in plan.readiness_resets.drain(..) {
                readiness_resets.push(BatchReadinessReset {
                    instance_id: plan.instance_id,
                    node_id,
                });
            }
            for init in plan.readiness_inits.drain(..) {
                readiness_inits.push(BatchReadinessInit {
                    instance_id: plan.instance_id,
                    node_id: init.node_id,
                    required_count: init.required_count,
                });
            }
            for increment in plan.direct_increments.drain(..) {
                direct_increments.push(BatchReadinessIncrement {
                    instance_id: plan.instance_id,
                    plan_index: plan.index,
                    increment,
                });
            }
            for increment in plan.tracked_increments.drain(..) {
                tracked_increments.push(BatchReadinessIncrement {
                    instance_id: plan.instance_id,
                    plan_index: plan.index,
                    increment,
                });
            }
            for node_id in plan.barrier_enqueues.drain(..) {
                barrier_enqueues.push(BatchBarrierEnqueue {
                    instance_id: plan.instance_id,
                    plan_index: plan.index,
                    node_id,
                });
            }
            if let Some(completion) = plan.instance_completion.take() {
                instance_completions.push(BatchInstanceCompletion {
                    instance_id: completion.instance_id,
                    plan_index: plan.index,
                    result_payload: completion.result_payload,
                });
            }
        }

        let inbox_write_count = inbox_writes.len();
        let readiness_reset_count = readiness_resets.len();
        let readiness_init_count = readiness_inits.len();
        let instance_completion_count = instance_completions.len();

        if !inbox_writes.is_empty() {
            let step_start = std::time::Instant::now();
            let mut touch_instance_ids: HashSet<Uuid> = HashSet::new();
            for write in &inbox_writes {
                touch_instance_ids.insert(write.instance_id.0);
            }
            let mut instance_ids = Vec::with_capacity(inbox_writes.len());
            let mut target_node_ids = Vec::with_capacity(inbox_writes.len());
            let mut variable_names = Vec::with_capacity(inbox_writes.len());
            let mut values = Vec::with_capacity(inbox_writes.len());
            let mut source_node_ids = Vec::with_capacity(inbox_writes.len());
            let mut spread_indexes: Vec<Option<i32>> = Vec::with_capacity(inbox_writes.len());

            for write in inbox_writes {
                instance_ids.push(write.instance_id.0);
                target_node_ids.push(write.target_node_id);
                variable_names.push(write.variable_name);
                values.push(write.value.to_json());
                source_node_ids.push(write.source_node_id);
                spread_indexes.push(write.spread_index);
            }

            sqlx::query(
                r#"
                INSERT INTO node_inputs (
                    instance_id,
                    target_node_id,
                    variable_name,
                    value,
                    source_node_id,
                    spread_index
                )
                SELECT *
                FROM UNNEST(
                    $1::uuid[],
                    $2::text[],
                    $3::text[],
                    $4::jsonb[],
                    $5::text[],
                    $6::int4[]
                )
                "#,
            )
            .bind(&instance_ids)
            .bind(&target_node_ids)
            .bind(&variable_names)
            .bind(&values)
            .bind(&source_node_ids)
            .bind(&spread_indexes)
            .execute(&mut *tx)
            .await?;

            let touch_ids: Vec<Uuid> = touch_instance_ids.into_iter().collect();
            let rows = sqlx::query(
                r#"
                UPDATE workflow_instances
                SET inbox_updated_at = NOW()
                WHERE id = ANY($1)
                RETURNING id, inbox_updated_at
                "#,
            )
            .bind(&touch_ids)
            .fetch_all(&mut *tx)
            .await?;
            for row in rows {
                let id: Uuid = row.get(0);
                let updated_at: DateTime<Utc> = row.get(1);
                inbox_updated_at.insert(id, updated_at);
            }
            timings.inbox_insert_us = step_start.elapsed().as_micros() as u64;
        }

        if !readiness_resets.is_empty() {
            let step_start = std::time::Instant::now();
            let instance_ids: Vec<Uuid> = readiness_resets
                .iter()
                .map(|reset| reset.instance_id.0)
                .collect();
            let node_ids: Vec<String> = readiness_resets
                .iter()
                .map(|reset| reset.node_id.clone())
                .collect();
            sqlx::query(
                r#"
                WITH resets AS (
                    SELECT * FROM UNNEST($1::uuid[], $2::text[]) AS r(instance_id, node_id)
                )
                UPDATE node_readiness nr
                SET completed_count = 0, updated_at = NOW()
                FROM resets
                WHERE nr.instance_id = resets.instance_id AND nr.node_id = resets.node_id
                "#,
            )
            .bind(&instance_ids)
            .bind(&node_ids)
            .execute(&mut *tx)
            .await?;
            timings.readiness_reset_us = step_start.elapsed().as_micros() as u64;
        }

        let readiness_inits = if readiness_inits.len() > 1 {
            let mut init_map: HashMap<(Uuid, String), i32> = HashMap::new();
            for init in readiness_inits {
                init_map.insert((init.instance_id.0, init.node_id), init.required_count);
            }
            init_map
                .into_iter()
                .map(
                    |((instance_id, node_id), required_count)| BatchReadinessInit {
                        instance_id: WorkflowInstanceId(instance_id),
                        node_id,
                        required_count,
                    },
                )
                .collect()
        } else {
            readiness_inits
        };

        if !readiness_inits.is_empty() {
            let step_start = std::time::Instant::now();
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)",
            );
            builder.push_values(&readiness_inits, |mut row, init| {
                row.push_bind(init.instance_id.0)
                    .push_bind(&init.node_id)
                    .push_bind(init.required_count)
                    .push_bind(0i32);
            });
            builder.push(
                " ON CONFLICT (instance_id, node_id) DO UPDATE SET required_count = EXCLUDED.required_count, completed_count = 0, updated_at = NOW()",
            );
            builder.build().execute(&mut *tx).await?;
            timings.readiness_init_us = step_start.elapsed().as_micros() as u64;
        }

        let direct_increment_count = direct_increments.len();
        let tracked_increment_count = tracked_increments.len();
        let barrier_enqueue_count = barrier_enqueues.len();
        let mut pending_enqueues: Vec<PendingEnqueue> = Vec::new();

        for increment in direct_increments {
            pending_enqueues.push(PendingEnqueue {
                instance_id: increment.instance_id,
                plan_index: increment.plan_index,
                node_id: increment.increment.node_id.clone(),
                module_name: increment
                    .increment
                    .module_name
                    .clone()
                    .unwrap_or_else(|| "__internal__".to_string()),
                action_name: increment
                    .increment
                    .action_name
                    .clone()
                    .unwrap_or_else(|| "__barrier__".to_string()),
                dispatch_payload: increment
                    .increment
                    .dispatch_payload
                    .clone()
                    .unwrap_or_default(),
                timeout_seconds: increment.increment.timeout_seconds,
                max_retries: increment.increment.max_retries,
                backoff_kind: increment.increment.backoff_kind,
                backoff_base_delay_ms: increment.increment.backoff_base_delay_ms,
                node_type: increment.increment.node_type,
                scheduled_at: increment.increment.scheduled_at,
            });
        }

        if !tracked_increments.is_empty() {
            let step_start = std::time::Instant::now();
            struct AggregatedIncrement {
                instance_id: WorkflowInstanceId,
                node_id: String,
                required_count: i32,
                delta: i32,
                plan_index: usize,
                increment: ReadinessIncrement,
            }

            let mut aggregated: HashMap<(Uuid, String), AggregatedIncrement> = HashMap::new();
            for increment in tracked_increments {
                let key = (increment.instance_id.0, increment.increment.node_id.clone());
                let entry = aggregated
                    .entry(key)
                    .or_insert_with(|| AggregatedIncrement {
                        instance_id: increment.instance_id,
                        node_id: increment.increment.node_id.clone(),
                        required_count: increment.increment.required_count,
                        delta: 0,
                        plan_index: increment.plan_index,
                        increment: increment.increment.clone(),
                    });
                if entry.required_count != increment.increment.required_count {
                    tx.rollback().await?;
                    return Err(DbError::NotFound(format!(
                        "Readiness required_count mismatch for node {}",
                        entry.node_id
                    )));
                }
                entry.delta += 1;
            }

            let aggregated_increments: Vec<AggregatedIncrement> =
                aggregated.into_values().collect();

            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)",
            );
            builder.push_values(&aggregated_increments, |mut row, increment| {
                row.push_bind(increment.instance_id.0)
                    .push_bind(&increment.node_id)
                    .push_bind(increment.required_count)
                    .push_bind(increment.delta);
            });
            builder.push(
                " ON CONFLICT (instance_id, node_id) DO UPDATE SET completed_count = node_readiness.completed_count + EXCLUDED.completed_count, updated_at = NOW() RETURNING instance_id, node_id, completed_count, required_count",
            );
            let rows: Vec<(Uuid, String, i32, i32)> =
                builder.build_query_as().fetch_all(&mut *tx).await?;
            timings.readiness_increment_us = step_start.elapsed().as_micros() as u64;

            let mut aggregated_by_key: HashMap<(Uuid, String), &AggregatedIncrement> =
                HashMap::new();
            for increment in &aggregated_increments {
                aggregated_by_key.insert(
                    (increment.instance_id.0, increment.node_id.clone()),
                    increment,
                );
            }

            for (instance_id, node_id, completed_count, required_count) in rows {
                if completed_count > required_count {
                    tx.rollback().await?;
                    return Err(DbError::NotFound(format!(
                        "Readiness overflow for node {}: {} > {}",
                        node_id, completed_count, required_count
                    )));
                }

                if completed_count == required_count {
                    let Some(increment) = aggregated_by_key.get(&(instance_id, node_id.clone()))
                    else {
                        tx.rollback().await?;
                        return Err(DbError::NotFound(format!(
                            "Missing readiness increment for node {}",
                            node_id
                        )));
                    };

                    pending_enqueues.push(PendingEnqueue {
                        instance_id: increment.instance_id,
                        plan_index: increment.plan_index,
                        node_id: increment.node_id.clone(),
                        module_name: increment
                            .increment
                            .module_name
                            .clone()
                            .unwrap_or_else(|| "__internal__".to_string()),
                        action_name: increment
                            .increment
                            .action_name
                            .clone()
                            .unwrap_or_else(|| "__barrier__".to_string()),
                        dispatch_payload: increment
                            .increment
                            .dispatch_payload
                            .clone()
                            .unwrap_or_default(),
                        timeout_seconds: increment.increment.timeout_seconds,
                        max_retries: increment.increment.max_retries,
                        backoff_kind: increment.increment.backoff_kind,
                        backoff_base_delay_ms: increment.increment.backoff_base_delay_ms,
                        node_type: increment.increment.node_type,
                        scheduled_at: increment.increment.scheduled_at,
                    });
                }
            }
        }

        for barrier in barrier_enqueues {
            pending_enqueues.push(PendingEnqueue {
                instance_id: barrier.instance_id,
                plan_index: barrier.plan_index,
                node_id: barrier.node_id,
                module_name: "__internal__".to_string(),
                action_name: "__barrier__".to_string(),
                dispatch_payload: Vec::new(),
                timeout_seconds: 300,
                max_retries: 3,
                backoff_kind: BackoffKind::Exponential,
                backoff_base_delay_ms: 1000,
                node_type: NodeType::Barrier,
                scheduled_at: Some(Utc::now()),
            });
        }

        let pending_enqueue_count = pending_enqueues.len();
        if !pending_enqueues.is_empty() {
            let mut per_instance_counts: HashMap<Uuid, i32> = HashMap::new();
            for enqueue in &pending_enqueues {
                *per_instance_counts
                    .entry(enqueue.instance_id.0)
                    .or_insert(0) += 1;
            }

            let mut instance_ids = Vec::with_capacity(per_instance_counts.len());
            let mut enqueue_counts = Vec::with_capacity(per_instance_counts.len());
            for (instance_id, count) in per_instance_counts {
                instance_ids.push(instance_id);
                enqueue_counts.push(count);
            }

            let step_start = std::time::Instant::now();
            let rows: Vec<(Uuid, i32, i32)> = sqlx::query_as(
                r#"
                WITH counts AS (
                    SELECT * FROM UNNEST($1::uuid[], $2::int[]) AS c(instance_id, enqueue_count)
                )
                UPDATE workflow_instances wi
                SET next_action_seq = next_action_seq + counts.enqueue_count
                FROM counts
                WHERE wi.id = counts.instance_id
                RETURNING wi.id, wi.next_action_seq - counts.enqueue_count, wi.priority
                "#,
            )
            .bind(&instance_ids)
            .bind(&enqueue_counts)
            .fetch_all(&mut *tx)
            .await?;
            timings.next_action_seq_us = step_start.elapsed().as_micros() as u64;

            let mut next_seq_by_instance: HashMap<Uuid, (i32, i32)> = HashMap::new();
            for (instance_id, start_seq, priority) in rows {
                next_seq_by_instance.insert(instance_id, (start_seq, priority));
            }

            let default_scheduled_at = Utc::now();
            let mut enqueue_rows = Vec::with_capacity(pending_enqueues.len());
            for enqueue in pending_enqueues {
                let Some((next_seq, priority)) =
                    next_seq_by_instance.get_mut(&enqueue.instance_id.0)
                else {
                    tx.rollback().await?;
                    return Err(DbError::NotFound(format!(
                        "Missing action sequence for instance {}",
                        enqueue.instance_id.0
                    )));
                };

                let action_seq = *next_seq;
                *next_seq += 1;

                results[enqueue.plan_index]
                    .newly_ready_nodes
                    .push(enqueue.node_id.clone());

                enqueue_rows.push(EnqueueRow {
                    instance_id: enqueue.instance_id,
                    node_id: enqueue.node_id,
                    module_name: enqueue.module_name,
                    action_name: enqueue.action_name,
                    dispatch_payload: enqueue.dispatch_payload,
                    timeout_seconds: enqueue.timeout_seconds,
                    max_retries: enqueue.max_retries,
                    backoff_kind: enqueue.backoff_kind,
                    backoff_base_delay_ms: enqueue.backoff_base_delay_ms,
                    node_type: enqueue.node_type,
                    scheduled_at: enqueue.scheduled_at.unwrap_or(default_scheduled_at),
                    priority: *priority,
                    action_seq,
                });
            }

            let step_start = std::time::Instant::now();
            let mut instance_ids = Vec::with_capacity(enqueue_rows.len());
            let mut action_seqs = Vec::with_capacity(enqueue_rows.len());
            let mut module_names = Vec::with_capacity(enqueue_rows.len());
            let mut action_names = Vec::with_capacity(enqueue_rows.len());
            let mut dispatch_payloads = Vec::with_capacity(enqueue_rows.len());
            let mut timeout_seconds = Vec::with_capacity(enqueue_rows.len());
            let mut max_retries = Vec::with_capacity(enqueue_rows.len());
            let mut backoff_kinds: Vec<&'static str> = Vec::with_capacity(enqueue_rows.len());
            let mut backoff_base_delays = Vec::with_capacity(enqueue_rows.len());
            let mut node_ids = Vec::with_capacity(enqueue_rows.len());
            let mut node_types: Vec<&'static str> = Vec::with_capacity(enqueue_rows.len());
            let mut scheduled_ats = Vec::with_capacity(enqueue_rows.len());
            let mut priorities = Vec::with_capacity(enqueue_rows.len());

            for enqueue in enqueue_rows {
                instance_ids.push(enqueue.instance_id.0);
                action_seqs.push(enqueue.action_seq);
                module_names.push(enqueue.module_name);
                action_names.push(enqueue.action_name);
                dispatch_payloads.push(enqueue.dispatch_payload);
                timeout_seconds.push(enqueue.timeout_seconds);
                max_retries.push(enqueue.max_retries);
                backoff_kinds.push(enqueue.backoff_kind.as_str());
                backoff_base_delays.push(enqueue.backoff_base_delay_ms);
                node_ids.push(enqueue.node_id);
                node_types.push(enqueue.node_type.as_str());
                scheduled_ats.push(enqueue.scheduled_at);
                priorities.push(enqueue.priority);
            }

            sqlx::query(
                r#"
                INSERT INTO action_queue (
                    instance_id, action_seq, module_name, action_name,
                    dispatch_payload, timeout_seconds, max_retries, backoff_kind,
                    backoff_base_delay_ms, node_id, node_type, scheduled_at, priority
                )
                SELECT *
                FROM UNNEST(
                    $1::uuid[],
                    $2::int4[],
                    $3::text[],
                    $4::text[],
                    $5::bytea[],
                    $6::int4[],
                    $7::int4[],
                    $8::text[],
                    $9::int4[],
                    $10::text[],
                    $11::text[],
                    $12::timestamptz[],
                    $13::int4[]
                )
                "#,
            )
            .bind(&instance_ids)
            .bind(&action_seqs)
            .bind(&module_names)
            .bind(&action_names)
            .bind(&dispatch_payloads)
            .bind(&timeout_seconds)
            .bind(&max_retries)
            .bind(&backoff_kinds)
            .bind(&backoff_base_delays)
            .bind(&node_ids)
            .bind(&node_types)
            .bind(&scheduled_ats)
            .bind(&priorities)
            .execute(&mut *tx)
            .await?;
            timings.enqueue_insert_us = step_start.elapsed().as_micros() as u64;
        }

        if !instance_completions.is_empty() {
            let step_start = std::time::Instant::now();
            let mut instance_ids = Vec::with_capacity(instance_completions.len());
            let mut result_payloads = Vec::with_capacity(instance_completions.len());
            let mut completion_indices: HashMap<Uuid, Vec<usize>> = HashMap::new();
            for completion in instance_completions {
                instance_ids.push(completion.instance_id.0);
                result_payloads.push(completion.result_payload);
                completion_indices
                    .entry(completion.instance_id.0)
                    .or_default()
                    .push(completion.plan_index);
            }

            let rows = sqlx::query(
                r#"
                WITH updates AS (
                    SELECT * FROM UNNEST($1::uuid[], $2::bytea[]) AS u(instance_id, result_payload)
                )
                UPDATE workflow_instances wi
                SET status = 'completed',
                    result_payload = updates.result_payload,
                    completed_at = NOW()
                FROM updates
                WHERE wi.id = updates.instance_id AND wi.status = 'running'
                RETURNING wi.id
                "#,
            )
            .bind(&instance_ids)
            .bind(&result_payloads)
            .fetch_all(&mut *tx)
            .await?;

            for row in rows {
                let instance_id: Uuid = row.get(0);
                if let Some(indices) = completion_indices.get(&instance_id) {
                    for index in indices {
                        results[*index].workflow_completed = true;
                    }
                }
            }
            timings.instance_complete_us = step_start.elapsed().as_micros() as u64;
        }

        if !inbox_updated_at.is_empty() {
            for plan in &batch_plans {
                if let Some(updated_at) = inbox_updated_at.get(&plan.instance_id.0) {
                    results[plan.index].inbox_updated_at = Some(*updated_at);
                }
            }
        }

        tx.commit().await?;
        timings.total_us = total_start.elapsed().as_micros() as u64;

        debug!(
            plans = results.len(),
            actions = action_ids.len(),
            inbox_writes = inbox_write_count,
            readiness_resets = readiness_reset_count,
            readiness_inits = readiness_init_count,
            readiness_increments = tracked_increment_count,
            direct_enqueues = direct_increment_count,
            barrier_enqueues = barrier_enqueue_count,
            pending_enqueues = pending_enqueue_count,
            instance_completions = instance_completion_count,
            action_complete_us = timings.action_complete_us,
            inbox_insert_us = timings.inbox_insert_us,
            readiness_reset_us = timings.readiness_reset_us,
            readiness_init_us = timings.readiness_init_us,
            readiness_increment_us = timings.readiness_increment_us,
            next_action_seq_us = timings.next_action_seq_us,
            enqueue_insert_us = timings.enqueue_insert_us,
            instance_complete_us = timings.instance_complete_us,
            total_us = timings.total_us,
            "execute_completion_plans_batch"
        );

        Ok(results)
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

        let CompletionPlan {
            completed_action_id,
            delivery_token,
            success,
            result_payload,
            error_message,
            pool_id,
            worker_id,
            worker_duration_ms,
            inbox_writes,
            readiness_resets,
            readiness_inits,
            readiness_increments,
            barrier_enqueues,
            instance_completion,
            ..
        } = plan;

        let mut direct_increments = Vec::new();
        let mut tracked_increments = Vec::new();
        for increment in readiness_increments {
            if increment.required_count == 1 && !increment.is_aggregator {
                direct_increments.push(increment);
            } else {
                tracked_increments.push(increment);
            }
        }

        let mut direct_nodes = HashSet::new();
        for increment in &direct_increments {
            direct_nodes.insert(increment.node_id.clone());
        }

        let readiness_resets: Vec<String> = readiness_resets
            .into_iter()
            .filter(|node_id| !direct_nodes.contains(node_id))
            .collect();
        let readiness_inits: Vec<_> = readiness_inits
            .into_iter()
            .filter(|init| !direct_nodes.contains(&init.node_id))
            .collect();

        // 1. Mark the completed action as complete (idempotent guard).
        // On success: delete action row and enqueue log entry.
        // On failure: update status to 'failed' so retry logic can process it.
        if let Some(action_id) = completed_action_id
            && let Some(delivery_token) = delivery_token
        {
            let row = if success {
                // Success: delete action row and enqueue log entry.
                sqlx::query(
                    r#"
                    WITH deleted AS (
                        DELETE FROM action_queue
                        WHERE id = $1 AND delivery_token = $2 AND status = 'dispatched'
                        RETURNING id, instance_id, attempt_number, dispatched_at,
                                  module_name, action_name, node_id, dispatch_payload, enqueued_at
                    ),
                    log_insert AS (
                        INSERT INTO action_log_queue (action_id, instance_id, attempt_number, dispatched_at,
                                                completed_at, success, result_payload, error_message, duration_ms,
                                                module_name, action_name, node_id, dispatch_payload,
                                                pool_id, worker_id, enqueued_at, worker_duration_ms)
                        SELECT id, instance_id, attempt_number, dispatched_at, NOW(), true, $3, NULL,
                               EXTRACT(EPOCH FROM (NOW() - dispatched_at)) * 1000,
                               module_name, action_name, node_id, dispatch_payload,
                               $4, $5, enqueued_at, $6
                        FROM deleted
                    )
                    SELECT COUNT(*) FROM deleted
                    "#,
                )
                .bind(action_id.0)
                .bind(delivery_token)
                .bind(&result_payload)
                .bind(pool_id)
                .bind(worker_id)
                .bind(worker_duration_ms)
                .fetch_one(&mut *tx)
                .await?
            } else {
                // Failure: UPDATE status so retry logic can process, INSERT log with full context
                sqlx::query(
                    r#"
                    WITH updated AS (
                        UPDATE action_queue
                        SET status = 'failed',
                            success = false,
                            result_payload = $3,
                            last_error = $4,
                            completed_at = NOW()
                        WHERE id = $1 AND delivery_token = $2 AND status = 'dispatched'
                        RETURNING id, instance_id, attempt_number, dispatched_at,
                                  module_name, action_name, node_id, dispatch_payload, enqueued_at
                    ),
                    log_insert AS (
                        INSERT INTO action_log_queue (action_id, instance_id, attempt_number, dispatched_at,
                                                completed_at, success, result_payload, error_message, duration_ms,
                                                module_name, action_name, node_id, dispatch_payload,
                                                pool_id, worker_id, enqueued_at, worker_duration_ms)
                        SELECT id, instance_id, attempt_number, dispatched_at, NOW(), false, $3, $4,
                               EXTRACT(EPOCH FROM (NOW() - dispatched_at)) * 1000,
                               module_name, action_name, node_id, dispatch_payload,
                               $5, $6, enqueued_at, $7
                        FROM updated
                    )
                    SELECT COUNT(*) FROM updated
                    "#,
                )
                .bind(action_id.0)
                .bind(delivery_token)
                .bind(&result_payload)
                .bind(&error_message)
                .bind(pool_id)
                .bind(worker_id)
                .bind(worker_duration_ms)
                .fetch_one(&mut *tx)
                .await?
            };

            let count: i64 = row.get(0);
            if count == 0 {
                // Stale or duplicate completion
                return Ok(CompletionResult::stale());
            }
        }

        // 2. Write inbox entries for all frontier nodes
        for write in &inbox_writes {
            if write.variable_name == "__loop_loop_8_i" {
                debug!(
                    target_node_id = %write.target_node_id,
                    variable_name = %write.variable_name,
                    value = ?write.value,
                    source_node_id = %write.source_node_id,
                    "DB writing __loop_loop_8_i to inbox"
                );
            }
        }
        if !inbox_writes.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO node_inputs (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)",
            );
            builder.push_values(&inbox_writes, |mut row, write| {
                row.push_bind(instance_id.0)
                    .push_bind(&write.target_node_id)
                    .push_bind(&write.variable_name)
                    .push_bind(write.value.to_json())
                    .push_bind(&write.source_node_id)
                    .push_bind(write.spread_index);
            });
            builder.build().execute(&mut *tx).await?;

            let row = sqlx::query(
                r#"
                UPDATE workflow_instances
                SET inbox_updated_at = NOW()
                WHERE id = $1
                RETURNING inbox_updated_at
                "#,
            )
            .bind(instance_id.0)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = row {
                let updated_at: DateTime<Utc> = row.get(0);
                result.inbox_updated_at = Some(updated_at);
            }
        }

        // 2.5. Reset readiness for loop re-triggering (before increments)
        if !readiness_resets.is_empty() {
            sqlx::query(
                r#"
                UPDATE node_readiness
                SET completed_count = 0, updated_at = NOW()
                WHERE instance_id = $1 AND node_id = ANY($2)
                "#,
            )
            .bind(instance_id.0)
            .bind(&readiness_resets)
            .execute(&mut *tx)
            .await?;
        }

        // 2.75. Initialize readiness for frontier nodes (before increments)
        if !readiness_inits.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)",
            );
            builder.push_values(&readiness_inits, |mut row, init| {
                row.push_bind(instance_id.0)
                    .push_bind(&init.node_id)
                    .push_bind(init.required_count)
                    .push_bind(0i32);
            });
            builder.push(
                " ON CONFLICT (instance_id, node_id) DO UPDATE SET required_count = EXCLUDED.required_count, completed_count = 0, updated_at = NOW()",
            );
            builder.build().execute(&mut *tx).await?;
        }

        #[derive(Debug)]
        struct PendingEnqueue {
            node_id: String,
            module_name: String,
            action_name: String,
            dispatch_payload: Vec<u8>,
            timeout_seconds: i32,
            max_retries: i32,
            backoff_kind: BackoffKind,
            backoff_base_delay_ms: i32,
            node_type: crate::completion::NodeType,
            scheduled_at: Option<DateTime<Utc>>,
        }

        let mut pending_enqueues: Vec<PendingEnqueue> = Vec::new();

        for increment in &direct_increments {
            pending_enqueues.push(PendingEnqueue {
                node_id: increment.node_id.clone(),
                module_name: increment
                    .module_name
                    .clone()
                    .unwrap_or_else(|| "__internal__".to_string()),
                action_name: increment
                    .action_name
                    .clone()
                    .unwrap_or_else(|| "__barrier__".to_string()),
                dispatch_payload: increment.dispatch_payload.clone().unwrap_or_default(),
                timeout_seconds: increment.timeout_seconds,
                max_retries: increment.max_retries,
                backoff_kind: increment.backoff_kind,
                backoff_base_delay_ms: increment.backoff_base_delay_ms,
                node_type: increment.node_type,
                scheduled_at: increment.scheduled_at,
            });
        }

        // 3. Increment readiness for frontier nodes, enqueue if ready
        if !tracked_increments.is_empty() {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)",
            );
            builder.push_values(&tracked_increments, |mut row, increment| {
                row.push_bind(instance_id.0)
                    .push_bind(&increment.node_id)
                    .push_bind(increment.required_count)
                    .push_bind(1i32);
            });
            builder.push(
                " ON CONFLICT (instance_id, node_id) DO UPDATE SET completed_count = node_readiness.completed_count + 1, updated_at = NOW() RETURNING node_id, completed_count, required_count",
            );
            let rows: Vec<(String, i32, i32)> =
                builder.build_query_as().fetch_all(&mut *tx).await?;

            let mut increment_by_node: HashMap<String, &crate::completion::ReadinessIncrement> =
                HashMap::new();
            for increment in &tracked_increments {
                increment_by_node.insert(increment.node_id.clone(), increment);
            }

            for (node_id, completed_count, required_count) in rows {
                // Check for overflow (indicates a bug)
                if completed_count > required_count {
                    tx.rollback().await?;
                    return Err(DbError::NotFound(format!(
                        "Readiness overflow for node {}: {} > {}",
                        node_id, completed_count, required_count
                    )));
                }

                if completed_count == required_count
                    && let Some(increment) = increment_by_node.get(&node_id)
                {
                    pending_enqueues.push(PendingEnqueue {
                        node_id: increment.node_id.clone(),
                        module_name: increment
                            .module_name
                            .clone()
                            .unwrap_or_else(|| "__internal__".to_string()),
                        action_name: increment
                            .action_name
                            .clone()
                            .unwrap_or_else(|| "__barrier__".to_string()),
                        dispatch_payload: increment.dispatch_payload.clone().unwrap_or_default(),
                        timeout_seconds: increment.timeout_seconds,
                        max_retries: increment.max_retries,
                        backoff_kind: increment.backoff_kind,
                        backoff_base_delay_ms: increment.backoff_base_delay_ms,
                        node_type: increment.node_type,
                        scheduled_at: increment.scheduled_at,
                    });
                }
            }
        }

        // 3.5. Enqueue barriers directly (no readiness tracking)
        for node_id in &barrier_enqueues {
            pending_enqueues.push(PendingEnqueue {
                node_id: node_id.clone(),
                module_name: "__internal__".to_string(),
                action_name: "__barrier__".to_string(),
                dispatch_payload: Vec::new(),
                timeout_seconds: 300,
                max_retries: 3,
                backoff_kind: BackoffKind::Exponential,
                backoff_base_delay_ms: 1000,
                node_type: crate::completion::NodeType::Barrier,
                scheduled_at: Some(Utc::now()),
            });
        }

        if !pending_enqueues.is_empty() {
            let row = sqlx::query(
                r#"
                UPDATE workflow_instances
                SET next_action_seq = next_action_seq + $2
                WHERE id = $1
                RETURNING next_action_seq - $2, priority
                "#,
            )
            .bind(instance_id.0)
            .bind(pending_enqueues.len() as i32)
            .fetch_one(&mut *tx)
            .await?;
            let mut next_action_seq: i32 = row.get(0);
            let priority: i32 = row.get(1);
            let default_scheduled_at = Utc::now();

            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO action_queue (instance_id, action_seq, module_name, action_name, dispatch_payload, timeout_seconds, max_retries, backoff_kind, backoff_base_delay_ms, node_id, node_type, scheduled_at, priority)",
            );
            builder.push_values(&pending_enqueues, |mut row, enqueue| {
                let action_seq = next_action_seq;
                next_action_seq += 1;
                let scheduled_at = enqueue.scheduled_at.unwrap_or(default_scheduled_at);
                row.push_bind(instance_id.0)
                    .push_bind(action_seq)
                    .push_bind(&enqueue.module_name)
                    .push_bind(&enqueue.action_name)
                    .push_bind(&enqueue.dispatch_payload)
                    .push_bind(enqueue.timeout_seconds)
                    .push_bind(enqueue.max_retries)
                    .push_bind(enqueue.backoff_kind.as_str())
                    .push_bind(enqueue.backoff_base_delay_ms)
                    .push_bind(&enqueue.node_id)
                    .push_bind(enqueue.node_type.as_str())
                    .push_bind(scheduled_at)
                    .push_bind(priority);
            });
            builder.build().execute(&mut *tx).await?;

            for enqueue in &pending_enqueues {
                result.newly_ready_nodes.push(enqueue.node_id.clone());
            }
        }

        // 4. Complete workflow instance if output node was reached
        if let Some(completion) = &instance_completion {
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

        tracing::debug!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            inbox_writes = inbox_writes.len(),
            readiness_inits = readiness_inits.len(),
            readiness_increments = tracked_increments.len(),
            direct_enqueues = direct_increments.len(),
            barrier_enqueues = barrier_enqueues.len(),
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
        priority: i32,
    ) -> DbResult<ScheduleId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_schedules
                (workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds, input_payload, next_run_at, priority)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (workflow_name, schedule_name)
            DO UPDATE SET
                schedule_type = EXCLUDED.schedule_type,
                cron_expression = EXCLUDED.cron_expression,
                interval_seconds = EXCLUDED.interval_seconds,
                jitter_seconds = EXCLUDED.jitter_seconds,
                input_payload = EXCLUDED.input_payload,
                next_run_at = EXCLUDED.next_run_at,
                priority = EXCLUDED.priority,
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
        .bind(priority)
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
                   created_at, updated_at, priority
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
                   created_at, updated_at, priority
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
                       created_at, updated_at, priority
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
                       created_at, updated_at, priority
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
                    updated_at,
                    median_dequeue_ms,
                    median_handling_ms
                )
                VALUES ($1, $2, $3, $4, $5, NOW(), $6, $7)
                ON CONFLICT (pool_id, worker_id)
                DO UPDATE SET
                    throughput_per_min = EXCLUDED.throughput_per_min,
                    total_completed = EXCLUDED.total_completed,
                    last_action_at = EXCLUDED.last_action_at,
                    updated_at = EXCLUDED.updated_at,
                    median_dequeue_ms = EXCLUDED.median_dequeue_ms,
                    median_handling_ms = EXCLUDED.median_handling_ms
                "#,
            )
            .bind(pool_id)
            .bind(status.worker_id)
            .bind(status.throughput_per_min)
            .bind(status.total_completed)
            .bind(status.last_action_at)
            .bind(status.median_dequeue_ms)
            .bind(status.median_handling_ms)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Calculate median timing metrics for workers from action_logs.
    ///
    /// Returns median dequeue time (dispatched_at - enqueued_at) and median execution time
    /// (completed_at - dispatched_at) for each worker in the pool over the given time window.
    pub async fn calculate_worker_median_times(
        &self,
        pool_id: Uuid,
        lookback_seconds: i64,
    ) -> DbResult<Vec<(i64, Option<i64>, Option<i64>)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                worker_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (dispatched_at - enqueued_at)) * 1000
                )::BIGINT as median_dequeue_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY COALESCE(
                        duration_ms::DOUBLE PRECISION,
                        EXTRACT(EPOCH FROM (completed_at - dispatched_at)) * 1000
                    )
                )::BIGINT as median_handling_ms
            FROM action_logs
            WHERE pool_id = $1
              AND worker_id IS NOT NULL
              AND enqueued_at IS NOT NULL
              AND dispatched_at IS NOT NULL
              AND completed_at IS NOT NULL
              AND completed_at >= NOW() - ($2 || ' seconds')::interval
            GROUP BY worker_id
            "#,
        )
        .bind(pool_id)
        .bind(lookback_seconds.to_string())
        .fetch_all(&self.pool)
        .await?;

        let results: Vec<(i64, Option<i64>, Option<i64>)> = rows
            .iter()
            .map(|row| {
                let worker_id: i64 = row.get("worker_id");
                let median_dequeue_ms: Option<i64> = row.get("median_dequeue_ms");
                let median_handling_ms: Option<i64> = row.get("median_handling_ms");
                (worker_id, median_dequeue_ms, median_handling_ms)
            })
            .collect();

        Ok(results)
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
        // Use a transaction with separate queries instead of a single CTE.
        // This is more efficient because:
        // 1. The IDs are collected once and passed as an array to each DELETE
        // 2. Each DELETE can use the instance_id index efficiently
        // 3. No repeated subquery materialization
        let mut tx = self.pool.begin().await?;

        // Step 1: Select and lock instances to delete, returning their IDs
        let ids: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM workflow_instances
            WHERE status IN ('completed', 'failed')
              AND completed_at IS NOT NULL
              AND completed_at < NOW() - ($1 || ' seconds')::interval
            ORDER BY completed_at
            FOR UPDATE SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(retention_seconds)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;

        if ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }

        let count = ids.len() as i64;

        // Step 2: Delete from child tables using the collected IDs
        // Delete action logs first (references action_queue)
        sqlx::query("DELETE FROM action_logs WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM action_log_queue WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM action_queue WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM instance_context WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM loop_state WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM node_readiness WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM node_inputs WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        // Step 3: Finally delete the instances themselves
        sqlx::query("DELETE FROM workflow_instances WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

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
