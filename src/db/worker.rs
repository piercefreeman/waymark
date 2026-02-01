//! High-performance database operations for the worker dispatch loop.
//!
//! This module provides database operations for the instance-local execution model.
//! Each workflow instance is owned by a single runner at a time, managed through
//! database leases. Execution state is stored as a protobuf-encoded blob.
//!
//! ## Instance-Local Operations
//! - `claim_instances_batch` - Claim instances with lease
//! - `update_execution_graphs_batch` - Persist execution state for multiple instances
//! - `heartbeat_instances` - Extend leases
//! - `complete_instances_batch` / `fail_instances_batch` - Complete/fail with final state
//! - `release_instances_batch` - Release without completing
//! - `count_orphaned_instances` - Monitor orphaned instances

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sqlx::{Row, types::Json};
use uuid::Uuid;

use super::{
    Database, DbError, DbResult, ScheduleId, ScheduleType, WorkerStatusUpdate, WorkflowInstanceId,
    WorkflowSchedule, WorkflowVersion, WorkflowVersionId,
};
use crate::messages::execution::{BackoffConfig, NodeKind, NodeStatus};
use crate::workflow_state::ActionExecutionUpdate;

/// Batch update for execution graphs: (instance_id, graph_bytes, next_wakeup_time)
pub type ExecutionGraphUpdate = (WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>);

/// Batch completion/failure: (instance_id, result_payload, graph_bytes)
pub type InstanceFinalization = (WorkflowInstanceId, Option<Vec<u8>>, Vec<u8>);

/// Record to be archived to completed_instances (used for background archiving)
#[derive(Debug, Clone)]
pub struct ArchiveRecord {
    pub id: Uuid,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<Uuid>,
    pub schedule_id: Option<Uuid>,
    pub input_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub priority: i32,
    pub execution_graph: Vec<u8>,
}

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
            INSERT INTO workflow_instances
                (workflow_name, workflow_version_id, input_payload, schedule_id, priority)
            SELECT $1, $2, payload, $3, $4
            FROM UNNEST($5::bytea[]) AS payload
            RETURNING id
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
            .map(|row| WorkflowInstanceId(row.get::<Uuid, _>("id")))
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
            INSERT INTO workflow_instances
                (workflow_name, workflow_version_id, input_payload, schedule_id, priority)
            SELECT $1, $2, payload, $3, $4
            FROM UNNEST($5::bytea[]) AS payload
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
        allow_duplicate: bool,
    ) -> DbResult<ScheduleId> {
        let row = sqlx::query(
            r#"
            INSERT INTO workflow_schedules
                (workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds, input_payload, next_run_at, priority, allow_duplicate)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (workflow_name, schedule_name)
            DO UPDATE SET
                schedule_type = EXCLUDED.schedule_type,
                cron_expression = EXCLUDED.cron_expression,
                interval_seconds = EXCLUDED.interval_seconds,
                jitter_seconds = EXCLUDED.jitter_seconds,
                input_payload = EXCLUDED.input_payload,
                next_run_at = EXCLUDED.next_run_at,
                priority = EXCLUDED.priority,
                allow_duplicate = EXCLUDED.allow_duplicate,
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
        .bind(allow_duplicate)
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
                   created_at, updated_at, priority, allow_duplicate
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
                   created_at, updated_at, priority, allow_duplicate
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

    /// Check if there is a running workflow instance for the given schedule.
    /// Uses the `idx_instances_schedule_running` partial index for efficient lookups.
    pub async fn has_running_instance_for_schedule(
        &self,
        schedule_id: ScheduleId,
    ) -> DbResult<bool> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM workflow_instances WHERE schedule_id = $1 AND status = 'running') as has_running",
        )
        .bind(schedule_id.0)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.get::<bool, _>("has_running"))
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
                       created_at, updated_at, priority, allow_duplicate
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
                       created_at, updated_at, priority, allow_duplicate
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

    /// Upsert a single pool-level worker status row.
    pub async fn upsert_worker_status(
        &self,
        pool_id: Uuid,
        status: &WorkerStatusUpdate,
    ) -> DbResult<()> {
        sqlx::query(
            r#"
            INSERT INTO worker_status (
                pool_id,
                throughput_per_min,
                total_completed,
                last_action_at,
                updated_at,
                median_dequeue_ms,
                median_handling_ms,
                dispatch_queue_size,
                total_in_flight,
                active_workers,
                actions_per_sec,
                median_instance_duration_secs,
                active_instance_count,
                total_instances_completed,
                instances_per_sec,
                instances_per_min,
                time_series
            )
            VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (pool_id)
            DO UPDATE SET
                throughput_per_min = EXCLUDED.throughput_per_min,
                total_completed = EXCLUDED.total_completed,
                last_action_at = EXCLUDED.last_action_at,
                updated_at = EXCLUDED.updated_at,
                median_dequeue_ms = EXCLUDED.median_dequeue_ms,
                median_handling_ms = EXCLUDED.median_handling_ms,
                dispatch_queue_size = EXCLUDED.dispatch_queue_size,
                total_in_flight = EXCLUDED.total_in_flight,
                active_workers = EXCLUDED.active_workers,
                actions_per_sec = EXCLUDED.actions_per_sec,
                median_instance_duration_secs = EXCLUDED.median_instance_duration_secs,
                active_instance_count = EXCLUDED.active_instance_count,
                total_instances_completed = EXCLUDED.total_instances_completed,
                instances_per_sec = EXCLUDED.instances_per_sec,
                instances_per_min = EXCLUDED.instances_per_min,
                time_series = EXCLUDED.time_series
            "#,
        )
        .bind(pool_id)
        .bind(status.throughput_per_min)
        .bind(status.total_completed)
        .bind(status.last_action_at)
        .bind(status.median_dequeue_ms)
        .bind(status.median_handling_ms)
        .bind(status.dispatch_queue_size)
        .bind(status.total_in_flight)
        .bind(status.active_workers)
        .bind(status.actions_per_sec)
        .bind(status.median_instance_duration_secs)
        .bind(status.active_instance_count)
        .bind(status.total_instances_completed)
        .bind(status.instances_per_sec)
        .bind(status.instances_per_min)
        .bind(&status.time_series)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ========================================================================
    // Garbage Collection
    // ========================================================================

    /// Garbage collect old completed/failed workflow instances.
    ///
    /// This function cleans up instances that have been in a terminal state (completed or failed)
    /// for longer than the specified retention period.
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
        let mut tx = self.pool.begin().await?;

        // Select and lock instances to delete
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

        // Delete the instances
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

    // ========================================================================
    // Execution Graph Operations (Instance-Local Model)
    // ========================================================================

    /// Claim a new or orphaned instance for execution.
    ///
    /// This uses SELECT FOR UPDATE SKIP LOCKED to safely claim instances without
    /// contention. Returns the instance with its execution graph if available.
    ///
    /// An instance is claimable if:
    /// - It's running and has no owner (new)
    /// - It's running and its lease has expired (orphaned - previous owner crashed)
    pub async fn claim_instance(
        &self,
        owner_id: &str,
        lease_duration_seconds: i64,
    ) -> DbResult<Option<ClaimedInstance>> {
        let row = sqlx::query(
            r#"
            WITH claimable AS (
                SELECT id
                FROM workflow_instances
                WHERE status = 'running'
                  AND (
                    -- New instance: no owner yet
                    owner_id IS NULL
                    -- Or orphaned: lease expired
                    OR lease_expires_at < NOW()
                  )
                  AND (
                    next_wakeup_time IS NULL
                    OR next_wakeup_time <= NOW()
                  )
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE workflow_instances i
            SET owner_id = $1,
                lease_expires_at = NOW() + ($2 || ' seconds')::interval,
                next_wakeup_time = NULL
            FROM claimable
            WHERE i.id = claimable.id
            RETURNING i.id, i.partition_id, i.workflow_name, i.workflow_version_id,
                      i.schedule_id, i.next_action_seq, i.input_payload, i.result_payload,
                      i.status, i.created_at, i.completed_at, i.priority,
                      i.execution_graph, i.owner_id, i.lease_expires_at
            "#,
        )
        .bind(owner_id)
        .bind(lease_duration_seconds.to_string())
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(ClaimedInstance {
                id: WorkflowInstanceId(row.get("id")),
                partition_id: row.get("partition_id"),
                workflow_name: row.get("workflow_name"),
                workflow_version_id: row
                    .get::<Option<Uuid>, _>("workflow_version_id")
                    .map(WorkflowVersionId),
                schedule_id: row.get::<Option<Uuid>, _>("schedule_id").map(ScheduleId),
                input_payload: row.get("input_payload"),
                execution_graph: row.get("execution_graph"),
                priority: row.get("priority"),
            })),
            None => Ok(None),
        }
    }

    /// Claim multiple instances at once for batch processing.
    pub async fn claim_instances_batch(
        &self,
        owner_id: &str,
        lease_duration_seconds: i64,
        limit: i32,
    ) -> DbResult<Vec<ClaimedInstance>> {
        let rows = sqlx::query(
            r#"
            WITH claimable AS (
                SELECT id
                FROM workflow_instances
                WHERE status = 'running'
                  AND (
                    owner_id IS NULL
                    OR lease_expires_at < NOW()
                  )
                  AND (
                    next_wakeup_time IS NULL
                    OR next_wakeup_time <= NOW()
                  )
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $3
            )
            UPDATE workflow_instances i
            SET owner_id = $1,
                lease_expires_at = NOW() + ($2 || ' seconds')::interval,
                next_wakeup_time = NULL
            FROM claimable
            WHERE i.id = claimable.id
            RETURNING i.id, i.partition_id, i.workflow_name, i.workflow_version_id,
                      i.schedule_id, i.next_action_seq, i.input_payload, i.result_payload,
                      i.status, i.created_at, i.completed_at, i.priority,
                      i.execution_graph, i.owner_id, i.lease_expires_at
            "#,
        )
        .bind(owner_id)
        .bind(lease_duration_seconds.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ClaimedInstance {
                id: WorkflowInstanceId(row.get("id")),
                partition_id: row.get("partition_id"),
                workflow_name: row.get("workflow_name"),
                workflow_version_id: row
                    .get::<Option<Uuid>, _>("workflow_version_id")
                    .map(WorkflowVersionId),
                schedule_id: row.get::<Option<Uuid>, _>("schedule_id").map(ScheduleId),
                input_payload: row.get("input_payload"),
                execution_graph: row.get("execution_graph"),
                priority: row.get("priority"),
            })
            .collect())
    }

    /// Update execution graphs for multiple instances in a single query.
    ///
    /// Only succeeds for instances where the caller still owns them (lease hasn't expired
    /// and owner_id matches). Returns the set of instance IDs that were successfully updated.
    pub async fn update_execution_graphs_batch(
        &self,
        owner_id: &str,
        updates: &[ExecutionGraphUpdate],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if updates.is_empty() {
            return Ok(HashSet::new());
        }

        let mut ordered: Vec<&ExecutionGraphUpdate> = updates.iter().collect();
        ordered.sort_by_key(|(id, _, _)| id.0);

        let ids: Vec<Uuid> = ordered.iter().map(|(id, _, _)| id.0).collect();
        let graphs: Vec<Vec<u8>> = ordered.iter().map(|(_, g, _)| g.clone()).collect();
        let wakeups: Vec<Option<DateTime<Utc>>> = ordered.iter().map(|(_, _, w)| *w).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET execution_graph = u.execution_graph,
                next_wakeup_time = u.next_wakeup_time
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as execution_graph,
                       unnest($4::timestamptz[]) as next_wakeup_time
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
              AND i.lease_expires_at > NOW()
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&graphs)
        .bind(&wakeups)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Extend the lease for all instances owned by this owner.
    ///
    /// This should be called periodically (e.g., every 10 seconds for a 60 second lease)
    /// to prevent instances from being considered orphaned.
    pub async fn heartbeat_instances(
        &self,
        owner_id: &str,
        lease_duration_seconds: i64,
    ) -> DbResult<i64> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_instances
            SET lease_expires_at = NOW() + ($2 || ' seconds')::interval
            WHERE owner_id = $1
              AND status = 'running'
            "#,
        )
        .bind(owner_id)
        .bind(lease_duration_seconds.to_string())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Complete multiple instances: delete from workflow_instances (fast path).
    ///
    /// Only succeeds for instances where the caller still owns them.
    /// Returns (succeeded_ids, archive_data) - caller should spawn background archive task.
    pub async fn complete_instances_batch(
        &self,
        owner_id: &str,
        completions: &[InstanceFinalization],
    ) -> DbResult<(HashSet<WorkflowInstanceId>, Vec<ArchiveRecord>)> {
        if completions.is_empty() {
            return Ok((HashSet::new(), Vec::new()));
        }

        let ids: Vec<Uuid> = completions.iter().map(|(id, _, _)| id.0).collect();
        let results: Vec<Option<Vec<u8>>> = completions.iter().map(|(_, r, _)| r.clone()).collect();
        let graphs: Vec<Vec<u8>> = completions.iter().map(|(_, _, g)| g.clone()).collect();

        // Fast path: verify ownership, collect data for archive, delete from queue
        let rows = sqlx::query(
            r#"
            WITH owned AS (
                SELECT id FROM workflow_instances
                WHERE id = ANY($1)
                  AND owner_id = $2
                  AND lease_expires_at > NOW()
            ),
            to_delete AS (
                SELECT i.id, i.partition_id, i.workflow_name, i.workflow_version_id, i.schedule_id,
                       i.input_payload, i.created_at, i.priority
                FROM workflow_instances i
                JOIN owned o ON i.id = o.id
            ),
            deleted_payloads AS (
                DELETE FROM node_payloads
                WHERE instance_id IN (SELECT id FROM to_delete)
            ),
            deleted AS (
                DELETE FROM workflow_instances
                WHERE id IN (SELECT id FROM to_delete)
            )
            SELECT id, partition_id, workflow_name, workflow_version_id, schedule_id,
                   input_payload, created_at, priority
            FROM to_delete
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await?;

        // Build archive records for background task
        let mut succeeded = HashSet::new();
        let mut archive_records = Vec::new();

        for row in rows {
            let id: Uuid = row.get("id");
            succeeded.insert(WorkflowInstanceId(id));

            // Find the matching result/graph from input
            if let Some(idx) = ids.iter().position(|&i| i == id) {
                archive_records.push(ArchiveRecord {
                    id,
                    partition_id: row.get("partition_id"),
                    workflow_name: row.get("workflow_name"),
                    workflow_version_id: row.get("workflow_version_id"),
                    schedule_id: row.get("schedule_id"),
                    input_payload: row.get("input_payload"),
                    result_payload: results[idx].clone(),
                    status: "completed".to_string(),
                    created_at: row.get("created_at"),
                    priority: row.get("priority"),
                    execution_graph: graphs[idx].clone(),
                });
            }
        }

        Ok((succeeded, archive_records))
    }

    /// Archive records to completed_instances (background task).
    /// This is fire-and-forget - failures are logged but don't block the main loop.
    pub async fn archive_instances_batch(&self, records: &[ArchiveRecord]) -> DbResult<()> {
        if records.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = records.iter().map(|r| r.id).collect();
        let partition_ids: Vec<i32> = records.iter().map(|r| r.partition_id).collect();
        let workflow_names: Vec<&str> = records.iter().map(|r| r.workflow_name.as_str()).collect();
        let version_ids: Vec<Option<Uuid>> =
            records.iter().map(|r| r.workflow_version_id).collect();
        let schedule_ids: Vec<Option<Uuid>> = records.iter().map(|r| r.schedule_id).collect();
        let input_payloads: Vec<Option<&[u8]>> =
            records.iter().map(|r| r.input_payload.as_deref()).collect();
        let result_payloads: Vec<Option<&[u8]>> = records
            .iter()
            .map(|r| r.result_payload.as_deref())
            .collect();
        let statuses: Vec<&str> = records.iter().map(|r| r.status.as_str()).collect();
        let created_ats: Vec<DateTime<Utc>> = records.iter().map(|r| r.created_at).collect();
        let priorities: Vec<i32> = records.iter().map(|r| r.priority).collect();
        let graphs: Vec<&[u8]> = records
            .iter()
            .map(|r| r.execution_graph.as_slice())
            .collect();

        sqlx::query(
            r#"
            INSERT INTO completed_instances (
                id, partition_id, workflow_name, workflow_version_id, schedule_id,
                input_payload, result_payload, status, created_at, completed_at,
                priority, execution_graph
            )
            SELECT id, partition_id, workflow_name, workflow_version_id, schedule_id,
                   input_payload, result_payload, status, created_at, NOW(),
                   priority, execution_graph
            FROM unnest(
                $1::uuid[], $2::int[], $3::text[], $4::uuid[], $5::uuid[],
                $6::bytea[], $7::bytea[], $8::text[], $9::timestamptz[], $10::int[], $11::bytea[]
            ) AS t(id, partition_id, workflow_name, workflow_version_id, schedule_id,
                   input_payload, result_payload, status, created_at, priority, execution_graph)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&partition_ids)
        .bind(&workflow_names)
        .bind(&version_ids)
        .bind(&schedule_ids)
        .bind(&input_payloads)
        .bind(&result_payloads)
        .bind(&statuses)
        .bind(&created_ats)
        .bind(&priorities)
        .bind(&graphs)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Fail multiple instances: archive to completed_instances, delete from workflow_instances.
    ///
    /// Only succeeds for instances where the caller still owns them.
    /// Returns the set of instance IDs that were successfully failed.
    /// Fail multiple instances: delete from workflow_instances (fast path).
    ///
    /// Only succeeds for instances where the caller still owns them.
    /// Returns (succeeded_ids, archive_data) - caller should spawn background archive task.
    pub async fn fail_instances_batch(
        &self,
        owner_id: &str,
        failures: &[InstanceFinalization],
    ) -> DbResult<(HashSet<WorkflowInstanceId>, Vec<ArchiveRecord>)> {
        if failures.is_empty() {
            return Ok((HashSet::new(), Vec::new()));
        }

        let ids: Vec<Uuid> = failures.iter().map(|(id, _, _)| id.0).collect();
        let results: Vec<Option<Vec<u8>>> = failures.iter().map(|(_, r, _)| r.clone()).collect();
        let graphs: Vec<Vec<u8>> = failures.iter().map(|(_, _, g)| g.clone()).collect();

        // Fast path: verify ownership, collect data for archive, delete from queue
        let rows = sqlx::query(
            r#"
            WITH owned AS (
                SELECT id FROM workflow_instances
                WHERE id = ANY($1)
                  AND owner_id = $2
                  AND lease_expires_at > NOW()
            ),
            to_delete AS (
                SELECT i.id, i.partition_id, i.workflow_name, i.workflow_version_id, i.schedule_id,
                       i.input_payload, i.created_at, i.priority
                FROM workflow_instances i
                JOIN owned o ON i.id = o.id
            ),
            deleted_payloads AS (
                DELETE FROM node_payloads
                WHERE instance_id IN (SELECT id FROM to_delete)
            ),
            deleted AS (
                DELETE FROM workflow_instances
                WHERE id IN (SELECT id FROM to_delete)
            )
            SELECT id, partition_id, workflow_name, workflow_version_id, schedule_id,
                   input_payload, created_at, priority
            FROM to_delete
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await?;

        // Build archive records for background task
        let mut succeeded = HashSet::new();
        let mut archive_records = Vec::new();

        for row in rows {
            let id: Uuid = row.get("id");
            succeeded.insert(WorkflowInstanceId(id));

            // Find the matching result/graph from input
            if let Some(idx) = ids.iter().position(|&i| i == id) {
                archive_records.push(ArchiveRecord {
                    id,
                    partition_id: row.get("partition_id"),
                    workflow_name: row.get("workflow_name"),
                    workflow_version_id: row.get("workflow_version_id"),
                    schedule_id: row.get("schedule_id"),
                    input_payload: row.get("input_payload"),
                    result_payload: results[idx].clone(),
                    status: "failed".to_string(),
                    created_at: row.get("created_at"),
                    priority: row.get("priority"),
                    execution_graph: graphs[idx].clone(),
                });
            }
        }

        Ok((succeeded, archive_records))
    }

    /// Release ownership of multiple instances without completing them.
    ///
    /// The instances will become available for other runners to claim.
    /// Returns the set of instance IDs that were successfully released.
    pub async fn release_instances_batch(
        &self,
        owner_id: &str,
        releases: &[ExecutionGraphUpdate],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if releases.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = releases.iter().map(|(id, _, _)| id.0).collect();
        let graphs: Vec<Vec<u8>> = releases.iter().map(|(_, g, _)| g.clone()).collect();
        let wakeups: Vec<Option<DateTime<Utc>>> = releases.iter().map(|(_, _, w)| *w).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET execution_graph = u.execution_graph,
                next_wakeup_time = u.next_wakeup_time,
                owner_id = NULL,
                lease_expires_at = NULL
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as execution_graph,
                       unnest($4::timestamptz[]) as next_wakeup_time
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&graphs)
        .bind(&wakeups)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Count orphaned instances (running with expired leases).
    ///
    /// Useful for monitoring and alerting.
    pub async fn count_orphaned_instances(&self) -> DbResult<i64> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_instances
            WHERE status = 'running'
              AND owner_id IS NOT NULL
              AND lease_expires_at < NOW()
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get("count"))
    }

    // ========================================================================
    // Action Execution Archives (metadata-only history)
    // ========================================================================

    /// Save action execution metadata for a batch of executions.
    ///
    /// Uses an upsert keyed by (instance_id, execution_id) to keep the latest
    /// metadata for each execution without rewriting payloads.
    pub async fn save_action_execution_archives_batch(
        &self,
        updates: &[ActionExecutionUpdate],
    ) -> DbResult<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let filtered: Vec<&ActionExecutionUpdate> = updates
            .iter()
            .filter(|update| update.execution_id.is_some())
            .collect();

        if filtered.is_empty() {
            return Ok(());
        }

        let instance_ids: Vec<Uuid> = filtered.iter().map(|u| u.instance_id.0).collect();
        let node_ids: Vec<&str> = filtered.iter().map(|u| u.node_id.as_str()).collect();
        let action_ids: Vec<&str> = filtered.iter().map(|u| u.action_id.as_str()).collect();
        let execution_ids: Vec<&str> = filtered
            .iter()
            .map(|u| u.execution_id.as_deref().unwrap())
            .collect();
        let statuses: Vec<i32> = filtered.iter().map(|u| u.status as i32).collect();
        let attempt_numbers: Vec<i32> = filtered.iter().map(|u| u.attempt_number).collect();
        let max_retries: Vec<i32> = filtered.iter().map(|u| u.max_retries).collect();
        let worker_ids: Vec<Option<String>> =
            filtered.iter().map(|u| u.worker_id.clone()).collect();
        let started_at_ms: Vec<Option<i64>> = filtered.iter().map(|u| u.started_at_ms).collect();
        let completed_at_ms: Vec<Option<i64>> =
            filtered.iter().map(|u| u.completed_at_ms).collect();
        let duration_ms: Vec<Option<i64>> = filtered.iter().map(|u| u.duration_ms).collect();
        let parent_execution_ids: Vec<Option<String>> = filtered
            .iter()
            .map(|u| u.parent_execution_id.clone())
            .collect();
        let spread_indices: Vec<Option<i32>> = filtered.iter().map(|u| u.spread_index).collect();
        let loop_indices: Vec<Option<i32>> = filtered.iter().map(|u| u.loop_index).collect();
        let waiting_for: Vec<Json<Vec<String>>> = filtered
            .iter()
            .map(|u| Json(u.waiting_for.clone()))
            .collect();
        let completed_counts: Vec<i32> = filtered.iter().map(|u| u.completed_count).collect();
        let node_kinds: Vec<i32> = filtered.iter().map(|u| u.node_kind as i32).collect();
        let errors: Vec<Option<String>> = filtered.iter().map(|u| u.error.clone()).collect();
        let error_types: Vec<Option<String>> =
            filtered.iter().map(|u| u.error_type.clone()).collect();
        let timeout_seconds: Vec<i32> = filtered.iter().map(|u| u.timeout_seconds).collect();
        let timeout_retry_limits: Vec<i32> =
            filtered.iter().map(|u| u.timeout_retry_limit).collect();
        let backoff_kinds: Vec<i32> = filtered.iter().map(|u| u.backoff.kind).collect();
        let backoff_base_delay_ms: Vec<i32> =
            filtered.iter().map(|u| u.backoff.base_delay_ms).collect();
        let backoff_multipliers: Vec<f64> = filtered.iter().map(|u| u.backoff.multiplier).collect();
        let sleep_wakeup_time_ms: Vec<Option<i64>> =
            filtered.iter().map(|u| u.sleep_wakeup_time_ms).collect();

        sqlx::query(
            r#"
            INSERT INTO action_execution_archives (
                instance_id,
                node_id,
                action_id,
                execution_id,
                status,
                attempt_number,
                max_retries,
                worker_id,
                started_at_ms,
                completed_at_ms,
                duration_ms,
                parent_execution_id,
                spread_index,
                loop_index,
                waiting_for,
                completed_count,
                node_kind,
                error,
                error_type,
                timeout_seconds,
                timeout_retry_limit,
                backoff_kind,
                backoff_base_delay_ms,
                backoff_multiplier,
                sleep_wakeup_time_ms
            )
            SELECT *
            FROM UNNEST(
                $1::uuid[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::int4[],
                $6::int4[],
                $7::int4[],
                $8::text[],
                $9::int8[],
                $10::int8[],
                $11::int8[],
                $12::text[],
                $13::int4[],
                $14::int4[],
                $15::jsonb[],
                $16::int4[],
                $17::int4[],
                $18::text[],
                $19::text[],
                $20::int4[],
                $21::int4[],
                $22::int4[],
                $23::int4[],
                $24::float8[],
                $25::int8[]
            )
            ON CONFLICT (instance_id, execution_id) DO UPDATE SET
                node_id = EXCLUDED.node_id,
                action_id = EXCLUDED.action_id,
                status = EXCLUDED.status,
                attempt_number = EXCLUDED.attempt_number,
                max_retries = EXCLUDED.max_retries,
                worker_id = EXCLUDED.worker_id,
                started_at_ms = EXCLUDED.started_at_ms,
                completed_at_ms = EXCLUDED.completed_at_ms,
                duration_ms = EXCLUDED.duration_ms,
                parent_execution_id = EXCLUDED.parent_execution_id,
                spread_index = EXCLUDED.spread_index,
                loop_index = EXCLUDED.loop_index,
                waiting_for = EXCLUDED.waiting_for,
                completed_count = EXCLUDED.completed_count,
                node_kind = EXCLUDED.node_kind,
                error = EXCLUDED.error,
                error_type = EXCLUDED.error_type,
                timeout_seconds = EXCLUDED.timeout_seconds,
                timeout_retry_limit = EXCLUDED.timeout_retry_limit,
                backoff_kind = EXCLUDED.backoff_kind,
                backoff_base_delay_ms = EXCLUDED.backoff_base_delay_ms,
                backoff_multiplier = EXCLUDED.backoff_multiplier,
                sleep_wakeup_time_ms = EXCLUDED.sleep_wakeup_time_ms
            "#,
        )
        .bind(&instance_ids)
        .bind(&node_ids)
        .bind(&action_ids)
        .bind(&execution_ids)
        .bind(&statuses)
        .bind(&attempt_numbers)
        .bind(&max_retries)
        .bind(&worker_ids)
        .bind(&started_at_ms)
        .bind(&completed_at_ms)
        .bind(&duration_ms)
        .bind(&parent_execution_ids)
        .bind(&spread_indices)
        .bind(&loop_indices)
        .bind(&waiting_for)
        .bind(&completed_counts)
        .bind(&node_kinds)
        .bind(&errors)
        .bind(&error_types)
        .bind(&timeout_seconds)
        .bind(&timeout_retry_limits)
        .bind(&backoff_kinds)
        .bind(&backoff_base_delay_ms)
        .bind(&backoff_multipliers)
        .bind(&sleep_wakeup_time_ms)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Load action execution metadata for multiple instances in a single query.
    pub async fn load_action_execution_archives_batch(
        &self,
        instance_ids: &[WorkflowInstanceId],
    ) -> DbResult<Vec<ActionExecutionArchive>> {
        if instance_ids.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();

        let rows = sqlx::query(
            r#"
            SELECT
                instance_id,
                node_id,
                action_id,
                execution_id,
                status,
                attempt_number,
                max_retries,
                worker_id,
                started_at_ms,
                completed_at_ms,
                duration_ms,
                parent_execution_id,
                spread_index,
                loop_index,
                waiting_for,
                completed_count,
                node_kind,
                error,
                error_type,
                timeout_seconds,
                timeout_retry_limit,
                backoff_kind,
                backoff_base_delay_ms,
                backoff_multiplier,
                sleep_wakeup_time_ms
            FROM action_execution_archives
            WHERE instance_id = ANY($1)
            "#,
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let status: i32 = row.get("status");
                let node_kind: i32 = row.get("node_kind");
                let waiting_for: Json<Vec<String>> = row.get("waiting_for");
                ActionExecutionArchive {
                    instance_id: WorkflowInstanceId(row.get("instance_id")),
                    node_id: row.get("node_id"),
                    action_id: row.get("action_id"),
                    execution_id: row.get("execution_id"),
                    status: NodeStatus::try_from(status).unwrap_or(NodeStatus::Unspecified),
                    attempt_number: row.get("attempt_number"),
                    max_retries: row.get("max_retries"),
                    worker_id: row.get("worker_id"),
                    started_at_ms: row.get("started_at_ms"),
                    completed_at_ms: row.get("completed_at_ms"),
                    duration_ms: row.get("duration_ms"),
                    parent_execution_id: row.get("parent_execution_id"),
                    spread_index: row.get("spread_index"),
                    loop_index: row.get("loop_index"),
                    waiting_for: waiting_for.0,
                    completed_count: row.get("completed_count"),
                    node_kind: NodeKind::try_from(node_kind).unwrap_or(NodeKind::Unspecified),
                    error: row.get("error"),
                    error_type: row.get("error_type"),
                    timeout_seconds: row.get("timeout_seconds"),
                    timeout_retry_limit: row.get("timeout_retry_limit"),
                    backoff: BackoffConfig {
                        kind: row.get("backoff_kind"),
                        base_delay_ms: row.get("backoff_base_delay_ms"),
                        multiplier: row.get("backoff_multiplier"),
                    },
                    sleep_wakeup_time_ms: row.get("sleep_wakeup_time_ms"),
                }
            })
            .collect())
    }

    // ========================================================================
    // Node Payloads (inputs/results stored separately for performance)
    // ========================================================================

    /// Save node payloads (inputs and/or results) for a batch of nodes.
    ///
    /// Pure INSERT (no upsert) - each payload gets a new row with generated ID.
    /// This is much faster than upsert since there's no conflict checking.
    pub async fn save_node_payloads_batch(&self, payloads: &[NodePayload]) -> DbResult<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        let instance_ids: Vec<Uuid> = payloads.iter().map(|p| p.instance_id.0).collect();
        let action_ids: Vec<&str> = payloads.iter().map(|p| p.action_id.as_str()).collect();
        let execution_ids: Vec<&str> = payloads.iter().map(|p| p.execution_id.as_str()).collect();
        let inputs: Vec<Option<&[u8]>> = payloads.iter().map(|p| p.inputs.as_deref()).collect();
        let results: Vec<Option<&[u8]>> = payloads.iter().map(|p| p.result.as_deref()).collect();

        // Pure INSERT - each execution_id is unique
        sqlx::query(
            r#"
            INSERT INTO node_payloads (instance_id, node_id, execution_id, inputs, result)
            SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bytea[], $5::bytea[])
            "#,
        )
        .bind(&instance_ids)
        .bind(&action_ids)
        .bind(&execution_ids)
        .bind(&inputs)
        .bind(&results)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Load all node payloads for an instance.
    ///
    /// Used during cold start recovery to reconstruct the in-memory WorkflowState.
    /// Payloads are keyed by execution_id (unique per action run).
    pub async fn load_node_payloads(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> DbResult<Vec<NodePayload>> {
        let rows = sqlx::query(
            r#"
            SELECT instance_id, node_id, execution_id, inputs, result
            FROM node_payloads
            WHERE instance_id = $1 AND execution_id IS NOT NULL
            "#,
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| NodePayload {
                instance_id: WorkflowInstanceId(row.get("instance_id")),
                action_id: row.get("node_id"),
                execution_id: row.get("execution_id"),
                inputs: row.get("inputs"),
                result: row.get("result"),
            })
            .collect())
    }

    /// Load node payloads for multiple instances in a single query.
    ///
    /// More efficient than calling load_node_payloads for each instance.
    /// Payloads are keyed by execution_id (unique per action run).
    pub async fn load_node_payloads_batch(
        &self,
        instance_ids: &[WorkflowInstanceId],
    ) -> DbResult<Vec<NodePayload>> {
        if instance_ids.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();

        let rows = sqlx::query(
            r#"
            SELECT instance_id, node_id, execution_id, inputs, result
            FROM node_payloads
            WHERE instance_id = ANY($1) AND execution_id IS NOT NULL
            "#,
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| NodePayload {
                instance_id: WorkflowInstanceId(row.get("instance_id")),
                action_id: row.get("node_id"),
                execution_id: row.get("execution_id"),
                inputs: row.get("inputs"),
                result: row.get("result"),
            })
            .collect())
    }
}

/// Payload data for a single node execution (inputs sent to worker, result received)
#[derive(Debug, Clone)]
pub struct NodePayload {
    pub instance_id: WorkflowInstanceId,
    pub action_id: String,
    /// Unique execution ID for this specific run (generated by mark_running)
    pub execution_id: String,
    pub inputs: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
}

/// Metadata archive for a single action execution (no payloads).
#[derive(Debug, Clone)]
pub struct ActionExecutionArchive {
    pub instance_id: WorkflowInstanceId,
    pub node_id: String,
    pub action_id: String,
    pub execution_id: String,
    pub status: NodeStatus,
    pub attempt_number: i32,
    pub max_retries: i32,
    pub worker_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
    pub duration_ms: Option<i64>,
    pub parent_execution_id: Option<String>,
    pub spread_index: Option<i32>,
    pub loop_index: Option<i32>,
    pub waiting_for: Vec<String>,
    pub completed_count: i32,
    pub node_kind: NodeKind,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub timeout_seconds: i32,
    pub timeout_retry_limit: i32,
    pub backoff: BackoffConfig,
    pub sleep_wakeup_time_ms: Option<i64>,
}

/// A claimed instance ready for local execution
#[derive(Debug, Clone)]
pub struct ClaimedInstance {
    pub id: WorkflowInstanceId,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub schedule_id: Option<ScheduleId>,
    pub input_payload: Option<Vec<u8>>,
    /// The serialized ExecutionGraph (protobuf). None for new instances.
    pub execution_graph: Option<Vec<u8>>,
    pub priority: i32,
}
