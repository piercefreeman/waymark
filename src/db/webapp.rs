//! Read-heavy database operations for the dashboard/webapp.
//!
//! These operations are used by the webapp for displaying workflow information.
//! They can be slower and more feature-rich than worker operations.
//! Adding features here will NOT impact worker performance.

use sqlx::Row;

use super::{
    Database, DbError, DbResult, QueuedAction, ScheduleId, WorkflowInstance, WorkflowInstanceId,
    WorkflowSchedule, WorkflowVersionId, WorkflowVersionSummary,
};

impl Database {
    // ========================================================================
    // Webapp: Workflow Version Queries
    // ========================================================================

    /// List all workflow versions (for dashboard display)
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
    // Webapp: Workflow Instance Queries
    // ========================================================================

    /// List instances for a specific workflow version (for dashboard display)
    pub async fn list_instances_for_version(
        &self,
        version_id: WorkflowVersionId,
        limit: i64,
    ) -> DbResult<Vec<WorkflowInstance>> {
        let instances = sqlx::query_as::<_, WorkflowInstance>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id,
                   schedule_id, next_action_seq, input_payload, result_payload, status,
                   created_at, completed_at
            FROM workflow_instances
            WHERE workflow_version_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(version_id.0)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(instances)
    }

    /// Get a workflow instance by ID (for detail view)
    pub async fn get_instance(&self, id: WorkflowInstanceId) -> DbResult<WorkflowInstance> {
        let instance = sqlx::query_as::<_, WorkflowInstance>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id,
                   schedule_id, next_action_seq, input_payload, result_payload, status,
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

    // ========================================================================
    // Webapp: Action Queries
    // ========================================================================

    /// Get all actions for an instance (for execution detail view)
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
                COALESCE(node_type, 'action') as node_type,
                result_payload,
                success,
                status,
                scheduled_at,
                last_error
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
                result_payload: row.get("result_payload"),
                success: row.get("success"),
                status: row.get("status"),
                scheduled_at: row.get("scheduled_at"),
                last_error: row.get("last_error"),
            })
            .collect();

        Ok(actions)
    }

    // ========================================================================
    // Webapp: Schedule Queries
    // ========================================================================

    /// Get a schedule by ID (for schedule detail page)
    pub async fn get_schedule_by_id(&self, id: ScheduleId) -> DbResult<WorkflowSchedule> {
        let schedule = sqlx::query_as::<_, WorkflowSchedule>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds,
                   input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at
            FROM workflow_schedules
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("schedule {}", id)))?;

        Ok(schedule)
    }

    /// List workflow instances triggered by a specific schedule.
    /// Returns paginated results with offset/limit.
    pub async fn list_schedule_invocations(
        &self,
        schedule_id: ScheduleId,
        limit: i64,
        offset: i64,
    ) -> DbResult<Vec<WorkflowInstance>> {
        let instances = sqlx::query_as::<_, WorkflowInstance>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id,
                   schedule_id, next_action_seq, input_payload, result_payload, status,
                   created_at, completed_at
            FROM workflow_instances
            WHERE schedule_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(schedule_id.0)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(instances)
    }

    /// Count total instances for a schedule (for pagination)
    pub async fn count_schedule_invocations(&self, schedule_id: ScheduleId) -> DbResult<i64> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_instances
            WHERE schedule_id = $1
            "#,
        )
        .bind(schedule_id.0)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<i64, _>("count"))
    }

    /// Update schedule status by ID (for webapp pause/resume/delete)
    pub async fn update_schedule_status_by_id(
        &self,
        id: ScheduleId,
        status: &str,
    ) -> DbResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = $2, updated_at = NOW()
            WHERE id = $1 AND status != 'deleted'
            "#,
        )
        .bind(id.0)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Delete a schedule by ID (soft delete, for webapp)
    pub async fn delete_schedule_by_id(&self, id: ScheduleId) -> DbResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = 'deleted', updated_at = NOW()
            WHERE id = $1 AND status != 'deleted'
            "#,
        )
        .bind(id.0)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }
}
