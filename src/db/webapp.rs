//! Read-heavy database operations for the dashboard/webapp.
//!
//! These operations are used by the webapp for displaying workflow information.
//! They can be slower and more feature-rich than worker operations.
//! Adding features here will NOT impact worker performance.

use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::{Postgres, QueryBuilder};

use super::{
    ActionLog, Database, DbError, DbResult, QueuedAction, ScheduleId, WorkerStatus,
    WorkflowInstance, WorkflowInstanceId, WorkflowSchedule, WorkflowVersionId,
    WorkflowVersionSummary,
};
use uuid::Uuid;

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

    fn append_invocation_search(query: &mut QueryBuilder<Postgres>, search: Option<&str>) {
        let Some(value) = search.filter(|value| !value.trim().is_empty()) else {
            return;
        };

        query.push(" AND workflow_name ILIKE ");
        query.push_bind(like_pattern(value));
    }

    /// Count all workflow invocations for the invocations page.
    pub async fn count_invocations(&self, search: Option<&str>) -> DbResult<i64> {
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_instances
            WHERE 1=1
            "#,
        );

        Self::append_invocation_search(&mut query, search);

        let row = query.build().fetch_one(&self.pool).await?;

        Ok(row.get::<i64, _>("count"))
    }

    /// List workflow invocations for the invocations page with pagination and search.
    pub async fn list_invocations_page(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> DbResult<Vec<WorkflowInstance>> {
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id,
                   schedule_id, next_action_seq, input_payload, result_payload, status,
                   created_at, completed_at
            FROM workflow_instances
            WHERE 1=1
            "#,
        );

        Self::append_invocation_search(&mut query, search);

        query.push(" ORDER BY created_at DESC");
        query.push(" LIMIT ");
        query.push_bind(limit);
        query.push(" OFFSET ");
        query.push_bind(offset);

        let instances = query
            .build_query_as::<WorkflowInstance>()
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
    // Webapp: Action Log Queries
    // ========================================================================

    /// Get all execution logs for a specific action (to see retry history)
    pub async fn get_action_logs(&self, action_id: Uuid) -> DbResult<Vec<ActionLog>> {
        let logs = sqlx::query_as::<_, ActionLog>(
            r#"
            SELECT id, action_id, instance_id, attempt_number,
                   dispatched_at, completed_at, success,
                   result_payload, error_message, duration_ms
            FROM action_logs
            WHERE action_id = $1
            ORDER BY attempt_number
            "#,
        )
        .bind(action_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(logs)
    }

    /// Get all execution logs for a workflow instance (full execution history)
    pub async fn get_instance_action_logs(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> DbResult<Vec<ActionLog>> {
        let logs = sqlx::query_as::<_, ActionLog>(
            r#"
            SELECT id, action_id, instance_id, attempt_number,
                   dispatched_at, completed_at, success,
                   result_payload, error_message, duration_ms
            FROM action_logs
            WHERE instance_id = $1
            ORDER BY dispatched_at, attempt_number, id
            "#,
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;

        Ok(logs)
    }

    /// Get recent action logs across all instances (for dashboard/monitoring)
    pub async fn get_recent_action_logs(&self, limit: i64) -> DbResult<Vec<ActionLog>> {
        let logs = sqlx::query_as::<_, ActionLog>(
            r#"
            SELECT id, action_id, instance_id, attempt_number,
                   dispatched_at, completed_at, success,
                   result_payload, error_message, duration_ms
            FROM action_logs
            ORDER BY dispatched_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(logs)
    }

    // ========================================================================
    // Webapp: Schedule Queries
    // ========================================================================

    fn append_schedule_search(query: &mut QueryBuilder<Postgres>, search: Option<&str>) {
        let terms = parse_schedule_search(search.unwrap_or_default());
        if terms.is_empty() {
            return;
        }

        query.push(" AND (");
        for (idx, term) in terms.iter().enumerate() {
            if idx > 0 {
                match term.operator {
                    SearchOperator::And => query.push(" AND "),
                    SearchOperator::Or => query.push(" OR "),
                };
            }

            query.push("(");
            query.push("workflow_name ILIKE ");
            query.push_bind(like_pattern(&term.value));
            query.push(" OR schedule_name ILIKE ");
            query.push_bind(like_pattern(&term.value));
            query.push(" OR id::text ILIKE ");
            query.push_bind(like_pattern(&term.value));
            query.push(")");
        }
        query.push(")");
    }

    /// Count schedules for the scheduled workflows page.
    pub async fn count_schedules(&self, search: Option<&str>) -> DbResult<i64> {
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            SELECT COUNT(*) as count
            FROM workflow_schedules
            WHERE status != 'deleted'
            "#,
        );

        Self::append_schedule_search(&mut query, search);

        let row = query.build().fetch_one(&self.pool).await?;

        Ok(row.get::<i64, _>("count"))
    }

    /// List schedules for the scheduled workflows page with pagination and search.
    pub async fn list_schedules_page(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> DbResult<Vec<WorkflowSchedule>> {
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds, jitter_seconds,
                   input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at
            FROM workflow_schedules
            WHERE status != 'deleted'
            "#,
        );

        Self::append_schedule_search(&mut query, search);

        query.push(" ORDER BY workflow_name, schedule_name");
        query.push(" LIMIT ");
        query.push_bind(limit);
        query.push(" OFFSET ");
        query.push_bind(offset);

        let schedules = query
            .build_query_as::<WorkflowSchedule>()
            .fetch_all(&self.pool)
            .await?;

        Ok(schedules)
    }

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

    // ========================================================================
    // Webapp: Worker Status Queries
    // ========================================================================

    pub async fn list_worker_statuses_recent(
        &self,
        since: DateTime<Utc>,
    ) -> DbResult<Vec<WorkerStatus>> {
        let workers = sqlx::query_as::<_, WorkerStatus>(
            r#"
            SELECT pool_id, worker_id, throughput_per_min, total_completed, last_action_at, updated_at
            FROM worker_status
            WHERE updated_at >= $1
            ORDER BY updated_at DESC, pool_id, worker_id
            "#,
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(workers)
    }
}

#[derive(Clone, Copy)]
enum SearchOperator {
    And,
    Or,
}

struct SearchTerm {
    operator: SearchOperator,
    value: String,
}

fn parse_schedule_search(input: &str) -> Vec<SearchTerm> {
    let tokens = tokenize_search(input);
    let mut terms = Vec::new();
    let mut pending_operator = None;
    let mut saw_term = false;

    for (token, quoted) in tokens {
        if token.is_empty() {
            continue;
        }

        if !quoted {
            let token_lower = token.to_ascii_lowercase();
            if token_lower == "and" {
                pending_operator = Some(SearchOperator::And);
                continue;
            }
            if token_lower == "or" {
                pending_operator = Some(SearchOperator::Or);
                continue;
            }
        }

        let operator = if saw_term {
            pending_operator.unwrap_or(SearchOperator::And)
        } else {
            SearchOperator::And
        };
        terms.push(SearchTerm {
            operator,
            value: token,
        });
        saw_term = true;
        pending_operator = None;
    }

    terms
}

fn tokenize_search(input: &str) -> Vec<(String, bool)> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut current_quote: Option<char> = None;
    let mut current_quoted = false;

    for ch in input.chars() {
        if let Some(quote) = current_quote {
            if ch == quote {
                current_quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }

        if ch == '"' || ch == '\'' {
            current_quote = Some(ch);
            current_quoted = true;
            continue;
        }

        if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push((current.clone(), current_quoted));
                current.clear();
                current_quoted = false;
            }
            continue;
        }

        current.push(ch);
    }

    if !current.is_empty() {
        tokens.push((current, current_quoted));
    }

    tokens
}

fn like_pattern(value: &str) -> String {
    format!("%{}%", value.trim())
}
