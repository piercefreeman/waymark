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
