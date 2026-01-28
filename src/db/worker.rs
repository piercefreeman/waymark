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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::{
    Database, DbError, DbResult, ScheduleId, ScheduleType, WorkerStatusUpdate, WorkflowInstanceId,
    WorkflowSchedule, WorkflowVersion, WorkflowVersionId,
};

/// Batch update for execution graphs: (instance_id, graph_bytes, next_wakeup_time)
pub type ExecutionGraphUpdate = (WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>);

/// Batch completion/failure: (instance_id, result_payload, graph_bytes)
pub type InstanceFinalization = (WorkflowInstanceId, Option<Vec<u8>>, Vec<u8>);

/// Batch snapshot update: (instance_id, graph_bytes, next_wakeup_time, snapshot_event_id)
pub type ExecutionGraphSnapshotUpdate = (WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>, i64);

/// Batch completion/failure with snapshot id: (instance_id, result_payload, graph_bytes, snapshot_event_id)
pub type InstanceFinalizationWithSnapshot = (WorkflowInstanceId, Option<Vec<u8>>, Vec<u8>, i64);

/// Batch release with snapshot id: (instance_id, graph_bytes, next_wakeup_time, snapshot_event_id)
pub type ExecutionGraphReleaseSnapshotUpdate =
    (WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>, i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionPayloadKind {
    Inputs,
    Result,
}

impl ExecutionPayloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionPayloadKind::Inputs => "inputs",
            ExecutionPayloadKind::Result => "result",
        }
    }
}

impl FromStr for ExecutionPayloadKind {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "inputs" => Ok(ExecutionPayloadKind::Inputs),
            "result" => Ok(ExecutionPayloadKind::Result),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionPayloadInsert {
    pub id: Uuid,
    pub instance_id: WorkflowInstanceId,
    pub node_id: String,
    pub attempt_number: i32,
    pub kind: ExecutionPayloadKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEventType {
    Dispatch,
    Completion,
    SetNextWakeup,
}

impl ExecutionEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            ExecutionEventType::Dispatch => "dispatch",
            ExecutionEventType::Completion => "completion",
            ExecutionEventType::SetNextWakeup => "set_next_wakeup",
        }
    }
}

impl FromStr for ExecutionEventType {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "dispatch" => Ok(ExecutionEventType::Dispatch),
            "completion" => Ok(ExecutionEventType::Completion),
            "set_next_wakeup" => Ok(ExecutionEventType::SetNextWakeup),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionEventRecord {
    pub id: i64,
    pub instance_id: WorkflowInstanceId,
    pub event_type: ExecutionEventType,
    pub node_id: Option<String>,
    pub worker_id: Option<String>,
    pub success: Option<bool>,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub duration_ms: Option<i64>,
    pub worker_duration_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub next_wakeup_ms: Option<i64>,
    pub inputs_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct ExecutionEventInsert {
    pub instance_id: WorkflowInstanceId,
    pub event_type: ExecutionEventType,
    pub node_id: Option<String>,
    pub worker_id: Option<String>,
    pub success: Option<bool>,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub duration_ms: Option<i64>,
    pub worker_duration_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub next_wakeup_ms: Option<i64>,
    pub inputs_payload_id: Option<Uuid>,
    pub result_payload_id: Option<Uuid>,
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
                time_series
            )
            VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                      i.execution_graph, i.snapshot_event_id, i.owner_id, i.lease_expires_at
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
                snapshot_event_id: row.get::<i64, _>("snapshot_event_id"),
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
                      i.execution_graph, i.snapshot_event_id, i.owner_id, i.lease_expires_at
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
                snapshot_event_id: row.get::<i64, _>("snapshot_event_id"),
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

        let ids: Vec<Uuid> = updates.iter().map(|(id, _, _)| id.0).collect();
        let graphs: Vec<Vec<u8>> = updates.iter().map(|(_, g, _)| g.clone()).collect();
        let wakeups: Vec<Option<DateTime<Utc>>> = updates.iter().map(|(_, _, w)| *w).collect();

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

    /// Update execution graphs with snapshot event id in a single query.
    pub async fn update_execution_graphs_with_snapshot_batch(
        &self,
        owner_id: &str,
        updates: &[ExecutionGraphSnapshotUpdate],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if updates.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = updates.iter().map(|(id, _, _, _)| id.0).collect();
        let graphs: Vec<Vec<u8>> = updates.iter().map(|(_, g, _, _)| g.clone()).collect();
        let wakeups: Vec<Option<DateTime<Utc>>> = updates.iter().map(|(_, _, w, _)| *w).collect();
        let snapshot_ids: Vec<i64> = updates.iter().map(|(_, _, _, s)| *s).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET execution_graph = u.execution_graph,
                next_wakeup_time = u.next_wakeup_time,
                snapshot_event_id = u.snapshot_event_id
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as execution_graph,
                       unnest($4::timestamptz[]) as next_wakeup_time,
                       unnest($5::bigint[]) as snapshot_event_id
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
        .bind(&snapshot_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Update next_wakeup_time without rewriting the execution graph.
    pub async fn update_next_wakeup_times_batch(
        &self,
        owner_id: &str,
        updates: &[(WorkflowInstanceId, Option<DateTime<Utc>>)],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if updates.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = updates.iter().map(|(id, _)| id.0).collect();
        let wakeups: Vec<Option<DateTime<Utc>>> = updates.iter().map(|(_, w)| *w).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET next_wakeup_time = u.next_wakeup_time
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::timestamptz[]) as next_wakeup_time
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
              AND i.lease_expires_at > NOW()
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&wakeups)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Insert payloads for execution events.
    pub async fn insert_execution_payloads_batch(
        &self,
        payloads: &[ExecutionPayloadInsert],
    ) -> DbResult<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        let ids: Vec<Uuid> = payloads.iter().map(|p| p.id).collect();
        let instance_ids: Vec<Uuid> = payloads.iter().map(|p| p.instance_id.0).collect();
        let node_ids: Vec<String> = payloads.iter().map(|p| p.node_id.clone()).collect();
        let attempt_numbers: Vec<i32> = payloads.iter().map(|p| p.attempt_number).collect();
        let kinds: Vec<String> = payloads
            .iter()
            .map(|p| p.kind.as_str().to_string())
            .collect();
        let bytes: Vec<Vec<u8>> = payloads.iter().map(|p| p.payload.clone()).collect();

        sqlx::query(
            r#"
            INSERT INTO execution_payloads (
                id,
                instance_id,
                node_id,
                attempt_number,
                payload_kind,
                payload_bytes
            )
            SELECT *
            FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::int[], $5::text[], $6::bytea[])
            "#,
        )
        .bind(&ids)
        .bind(&instance_ids)
        .bind(&node_ids)
        .bind(&attempt_numbers)
        .bind(&kinds)
        .bind(&bytes)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Insert execution events for multiple instances.
    pub async fn insert_execution_events_batch(
        &self,
        owner_id: &str,
        events: &[ExecutionEventInsert],
    ) -> DbResult<HashMap<WorkflowInstanceId, i64>> {
        if events.is_empty() {
            return Ok(HashMap::new());
        }

        let instance_ids: Vec<Uuid> = events.iter().map(|e| e.instance_id.0).collect();
        let event_types: Vec<String> = events
            .iter()
            .map(|e| e.event_type.as_str().to_string())
            .collect();
        let node_ids: Vec<Option<String>> = events.iter().map(|e| e.node_id.clone()).collect();
        let worker_ids: Vec<Option<String>> = events.iter().map(|e| e.worker_id.clone()).collect();
        let successes: Vec<Option<bool>> = events.iter().map(|e| e.success).collect();
        let errors: Vec<Option<String>> = events.iter().map(|e| e.error.clone()).collect();
        let error_types: Vec<Option<String>> =
            events.iter().map(|e| e.error_type.clone()).collect();
        let duration_ms: Vec<Option<i64>> = events.iter().map(|e| e.duration_ms).collect();
        let worker_duration_ms: Vec<Option<i64>> =
            events.iter().map(|e| e.worker_duration_ms).collect();
        let started_at_ms: Vec<Option<i64>> = events.iter().map(|e| e.started_at_ms).collect();
        let next_wakeup_ms: Vec<Option<i64>> = events.iter().map(|e| e.next_wakeup_ms).collect();
        let inputs_payload_ids: Vec<Option<Uuid>> =
            events.iter().map(|e| e.inputs_payload_id).collect();
        let result_payload_ids: Vec<Option<Uuid>> =
            events.iter().map(|e| e.result_payload_id).collect();

        let rows = sqlx::query(
            r#"
            INSERT INTO workflow_instance_events (
                instance_id,
                event_type,
                node_id,
                worker_id,
                success,
                error,
                error_type,
                duration_ms,
                worker_duration_ms,
                started_at_ms,
                next_wakeup_ms,
                inputs_payload_id,
                result_payload_id
            )
            SELECT u.instance_id,
                   u.event_type,
                   u.node_id,
                   u.worker_id,
                   u.success,
                   u.error,
                   u.error_type,
                   u.duration_ms,
                   u.worker_duration_ms,
                   u.started_at_ms,
                   u.next_wakeup_ms,
                   u.inputs_payload_id,
                   u.result_payload_id
            FROM UNNEST(
                $1::uuid[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::bool[],
                $6::text[],
                $7::text[],
                $8::bigint[],
                $9::bigint[],
                $10::bigint[],
                $11::bigint[],
                $12::uuid[],
                $13::uuid[]
            ) AS u(
                instance_id,
                event_type,
                node_id,
                worker_id,
                success,
                error,
                error_type,
                duration_ms,
                worker_duration_ms,
                started_at_ms,
                next_wakeup_ms,
                inputs_payload_id,
                result_payload_id
            )
            JOIN workflow_instances i ON i.id = u.instance_id
            WHERE i.owner_id = $14
              AND i.lease_expires_at > NOW()
            RETURNING id, instance_id
            "#,
        )
        .bind(&instance_ids)
        .bind(&event_types)
        .bind(&node_ids)
        .bind(&worker_ids)
        .bind(&successes)
        .bind(&errors)
        .bind(&error_types)
        .bind(&duration_ms)
        .bind(&worker_duration_ms)
        .bind(&started_at_ms)
        .bind(&next_wakeup_ms)
        .bind(&inputs_payload_ids)
        .bind(&result_payload_ids)
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await?;

        let mut max_by_instance: HashMap<WorkflowInstanceId, i64> = HashMap::new();
        for row in rows {
            let id: i64 = row.get("id");
            let instance_id = WorkflowInstanceId(row.get("instance_id"));
            let entry = max_by_instance.entry(instance_id).or_insert(id);
            if id > *entry {
                *entry = id;
            }
        }

        Ok(max_by_instance)
    }

    /// Load execution events after a snapshot id (inclusive of payloads).
    pub async fn get_execution_events_since(
        &self,
        instance_id: WorkflowInstanceId,
        after_event_id: i64,
    ) -> DbResult<Vec<ExecutionEventRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                e.id,
                e.instance_id,
                e.event_type,
                e.node_id,
                e.worker_id,
                e.success,
                e.error,
                e.error_type,
                e.duration_ms,
                e.worker_duration_ms,
                e.started_at_ms,
                e.next_wakeup_ms,
                inp.payload_bytes as inputs_payload,
                res.payload_bytes as result_payload
            FROM workflow_instance_events e
            LEFT JOIN execution_payloads inp ON e.inputs_payload_id = inp.id
            LEFT JOIN execution_payloads res ON e.result_payload_id = res.id
            WHERE e.instance_id = $1
              AND e.id > $2
            ORDER BY e.id ASC
            "#,
        )
        .bind(instance_id.0)
        .bind(after_event_id)
        .fetch_all(&self.pool)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event_type_raw: String = row.get("event_type");
            let event_type = ExecutionEventType::from_str(&event_type_raw)
                .map_err(|_| DbError::NotFound(format!("unknown event type: {event_type_raw}")))?;

            events.push(ExecutionEventRecord {
                id: row.get("id"),
                instance_id: WorkflowInstanceId(row.get("instance_id")),
                event_type,
                node_id: row.get("node_id"),
                worker_id: row.get("worker_id"),
                success: row.get("success"),
                error: row.get("error"),
                error_type: row.get("error_type"),
                duration_ms: row.get("duration_ms"),
                worker_duration_ms: row.get("worker_duration_ms"),
                started_at_ms: row.get("started_at_ms"),
                next_wakeup_ms: row.get("next_wakeup_ms"),
                inputs_payload: row.get("inputs_payload"),
                result_payload: row.get("result_payload"),
            });
        }

        Ok(events)
    }

    /// Delete execution events up to a snapshot id for a set of instances.
    pub async fn delete_execution_events_up_to(
        &self,
        instance_ids: &[WorkflowInstanceId],
        snapshot_ids: &[i64],
    ) -> DbResult<u64> {
        if instance_ids.is_empty() {
            return Ok(0);
        }

        let ids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();
        let snapshots: Vec<i64> = snapshot_ids.to_vec();

        let result = sqlx::query(
            r#"
            DELETE FROM workflow_instance_events e
            USING (
                SELECT unnest($1::uuid[]) as instance_id,
                       unnest($2::bigint[]) as snapshot_id
            ) u
            WHERE e.instance_id = u.instance_id
              AND e.id <= u.snapshot_id
            "#,
        )
        .bind(&ids)
        .bind(&snapshots)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
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

    /// Complete multiple instances and update their execution graphs atomically.
    ///
    /// Only succeeds for instances where the caller still owns them.
    /// Returns the set of instance IDs that were successfully completed.
    pub async fn complete_instances_batch(
        &self,
        owner_id: &str,
        completions: &[InstanceFinalization],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if completions.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = completions.iter().map(|(id, _, _)| id.0).collect();
        let results: Vec<Option<Vec<u8>>> = completions.iter().map(|(_, r, _)| r.clone()).collect();
        let graphs: Vec<Vec<u8>> = completions.iter().map(|(_, _, g)| g.clone()).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET status = 'completed',
                result_payload = u.result_payload,
                execution_graph = u.execution_graph,
                next_wakeup_time = NULL,
                completed_at = NOW(),
                owner_id = NULL,
                lease_expires_at = NULL
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as result_payload,
                       unnest($4::bytea[]) as execution_graph
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
              AND i.lease_expires_at > NOW()
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&results)
        .bind(&graphs)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Complete multiple instances and update their execution graphs with snapshot ids.
    pub async fn complete_instances_with_snapshot_batch(
        &self,
        owner_id: &str,
        completions: &[InstanceFinalizationWithSnapshot],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if completions.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = completions.iter().map(|(id, _, _, _)| id.0).collect();
        let results: Vec<Option<Vec<u8>>> =
            completions.iter().map(|(_, r, _, _)| r.clone()).collect();
        let graphs: Vec<Vec<u8>> = completions.iter().map(|(_, _, g, _)| g.clone()).collect();
        let snapshots: Vec<i64> = completions.iter().map(|(_, _, _, s)| *s).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET status = 'completed',
                result_payload = u.result_payload,
                execution_graph = u.execution_graph,
                snapshot_event_id = u.snapshot_event_id,
                next_wakeup_time = NULL,
                completed_at = NOW(),
                owner_id = NULL,
                lease_expires_at = NULL
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as result_payload,
                       unnest($4::bytea[]) as execution_graph,
                       unnest($5::bigint[]) as snapshot_event_id
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
              AND i.lease_expires_at > NOW()
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&results)
        .bind(&graphs)
        .bind(&snapshots)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Fail multiple instances and update their execution graphs atomically.
    ///
    /// Only succeeds for instances where the caller still owns them.
    /// Returns the set of instance IDs that were successfully failed.
    pub async fn fail_instances_batch(
        &self,
        owner_id: &str,
        failures: &[InstanceFinalization],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if failures.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = failures.iter().map(|(id, _, _)| id.0).collect();
        let results: Vec<Option<Vec<u8>>> = failures.iter().map(|(_, r, _)| r.clone()).collect();
        let graphs: Vec<Vec<u8>> = failures.iter().map(|(_, _, g)| g.clone()).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET status = 'failed',
                result_payload = u.result_payload,
                execution_graph = u.execution_graph,
                next_wakeup_time = NULL,
                completed_at = NOW(),
                owner_id = NULL,
                lease_expires_at = NULL
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as result_payload,
                       unnest($4::bytea[]) as execution_graph
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
              AND i.lease_expires_at > NOW()
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&results)
        .bind(&graphs)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
    }

    /// Fail multiple instances and update their execution graphs with snapshot ids.
    pub async fn fail_instances_with_snapshot_batch(
        &self,
        owner_id: &str,
        failures: &[InstanceFinalizationWithSnapshot],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if failures.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = failures.iter().map(|(id, _, _, _)| id.0).collect();
        let results: Vec<Option<Vec<u8>>> = failures.iter().map(|(_, r, _, _)| r.clone()).collect();
        let graphs: Vec<Vec<u8>> = failures.iter().map(|(_, _, g, _)| g.clone()).collect();
        let snapshots: Vec<i64> = failures.iter().map(|(_, _, _, s)| *s).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET status = 'failed',
                result_payload = u.result_payload,
                execution_graph = u.execution_graph,
                snapshot_event_id = u.snapshot_event_id,
                next_wakeup_time = NULL,
                completed_at = NOW(),
                owner_id = NULL,
                lease_expires_at = NULL
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as result_payload,
                       unnest($4::bytea[]) as execution_graph,
                       unnest($5::bigint[]) as snapshot_event_id
            ) u
            WHERE i.id = u.id
              AND i.owner_id = $2
              AND i.lease_expires_at > NOW()
            RETURNING i.id
            "#,
        )
        .bind(&ids)
        .bind(owner_id)
        .bind(&results)
        .bind(&graphs)
        .bind(&snapshots)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| WorkflowInstanceId(row.get("id")))
            .collect())
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

    /// Release ownership of multiple instances with snapshot ids.
    pub async fn release_instances_with_snapshot_batch(
        &self,
        owner_id: &str,
        releases: &[ExecutionGraphReleaseSnapshotUpdate],
    ) -> DbResult<HashSet<WorkflowInstanceId>> {
        if releases.is_empty() {
            return Ok(HashSet::new());
        }

        let ids: Vec<Uuid> = releases.iter().map(|(id, _, _, _)| id.0).collect();
        let graphs: Vec<Vec<u8>> = releases.iter().map(|(_, g, _, _)| g.clone()).collect();
        let wakeups: Vec<Option<DateTime<Utc>>> = releases.iter().map(|(_, _, w, _)| *w).collect();
        let snapshots: Vec<i64> = releases.iter().map(|(_, _, _, s)| *s).collect();

        let rows = sqlx::query(
            r#"
            UPDATE workflow_instances i
            SET execution_graph = u.execution_graph,
                next_wakeup_time = u.next_wakeup_time,
                snapshot_event_id = u.snapshot_event_id,
                owner_id = NULL,
                lease_expires_at = NULL
            FROM (
                SELECT unnest($1::uuid[]) as id,
                       unnest($3::bytea[]) as execution_graph,
                       unnest($4::timestamptz[]) as next_wakeup_time,
                       unnest($5::bigint[]) as snapshot_event_id
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
        .bind(&snapshots)
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
    /// Snapshot event id for event-log replay.
    pub snapshot_event_id: i64,
    pub priority: i32,
}
