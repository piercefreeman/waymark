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
        let input = search.unwrap_or_default();
        let tokens = tokenize_search(input);
        let tokens = normalize_search_tokens(tokens);

        if tokens.is_empty() {
            return;
        }

        // Check if we have any actual search terms
        let has_terms = tokens.iter().any(|t| matches!(t, SearchToken::Term(_)));
        if !has_terms {
            return;
        }

        query.push(" AND (");
        for token in &tokens {
            match token {
                SearchToken::Term(value) => {
                    // Each term searches across workflow_name and status
                    query.push("(");
                    query.push("workflow_name ILIKE ");
                    query.push_bind(like_pattern(value));
                    query.push(" OR status ILIKE ");
                    query.push_bind(like_pattern(value));
                    query.push(")");
                }
                SearchToken::And => {
                    query.push(" AND ");
                }
                SearchToken::Or => {
                    query.push(" OR ");
                }
                SearchToken::OpenParen => {
                    query.push("(");
                }
                SearchToken::CloseParen => {
                    query.push(")");
                }
            }
        }
        query.push(")");
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
        let input = search.unwrap_or_default();
        let tokens = tokenize_search(input);
        let tokens = normalize_search_tokens(tokens);

        if tokens.is_empty() {
            return;
        }

        // Check if we have any actual search terms
        let has_terms = tokens.iter().any(|t| matches!(t, SearchToken::Term(_)));
        if !has_terms {
            return;
        }

        query.push(" AND (");
        for token in &tokens {
            match token {
                SearchToken::Term(value) => {
                    // Each term searches across multiple columns
                    query.push("(");
                    query.push("workflow_name ILIKE ");
                    query.push_bind(like_pattern(value));
                    query.push(" OR schedule_name ILIKE ");
                    query.push_bind(like_pattern(value));
                    query.push(" OR id::text ILIKE ");
                    query.push_bind(like_pattern(value));
                    query.push(")");
                }
                SearchToken::And => {
                    query.push(" AND ");
                }
                SearchToken::Or => {
                    query.push(" OR ");
                }
                SearchToken::OpenParen => {
                    query.push("(");
                }
                SearchToken::CloseParen => {
                    query.push(")");
                }
            }
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

    // ========================================================================
    // Webapp: Filter Value Queries (for dropdown column filters)
    // ========================================================================

    /// Get distinct workflow names for invocations filter dropdown
    pub async fn get_distinct_invocation_workflows(&self) -> DbResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT workflow_name
            FROM workflow_instances
            ORDER BY workflow_name
            LIMIT 100
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("workflow_name"))
            .collect())
    }

    /// Get distinct statuses for invocations filter dropdown
    pub async fn get_distinct_invocation_statuses(&self) -> DbResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT status
            FROM workflow_instances
            ORDER BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(|r| r.get::<String, _>("status")).collect())
    }

    /// Get distinct workflow names for schedules filter dropdown
    pub async fn get_distinct_schedule_workflows(&self) -> DbResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT workflow_name
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY workflow_name
            LIMIT 100
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("workflow_name"))
            .collect())
    }

    /// Get distinct statuses for schedules filter dropdown
    pub async fn get_distinct_schedule_statuses(&self) -> DbResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT status
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(|r| r.get::<String, _>("status")).collect())
    }

    /// Get distinct schedule types for schedules filter dropdown
    pub async fn get_distinct_schedule_types(&self) -> DbResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT schedule_type
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY schedule_type
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("schedule_type"))
            .collect())
    }
}

/// Represents a token in a search expression
#[derive(Clone, Debug)]
enum SearchToken {
    /// A search term (value to search for)
    Term(String),
    /// AND operator
    And,
    /// OR operator
    Or,
    /// Opening parenthesis
    OpenParen,
    /// Closing parenthesis
    CloseParen,
}

/// Tokenize search input into a list of tokens, supporting:
/// - Quoted strings: "hello world" or 'hello world'
/// - AND/OR operators (case-insensitive)
/// - Parentheses for grouping: (term1 OR term2)
fn tokenize_search(input: &str) -> Vec<SearchToken> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut current_quote: Option<char> = None;

    let flush_current = |current: &mut String, tokens: &mut Vec<SearchToken>| {
        if current.is_empty() {
            return;
        }
        let token_lower = current.to_ascii_lowercase();
        let token = if token_lower == "and" {
            SearchToken::And
        } else if token_lower == "or" {
            SearchToken::Or
        } else {
            SearchToken::Term(std::mem::take(current))
        };
        if !matches!(token, SearchToken::Term(ref s) if s.is_empty()) {
            tokens.push(token);
        }
        current.clear();
    };

    for ch in input.chars() {
        // Handle quoted strings
        if let Some(quote) = current_quote {
            if ch == quote {
                // End of quoted string - flush as a term (don't interpret AND/OR)
                if !current.is_empty() {
                    tokens.push(SearchToken::Term(std::mem::take(&mut current)));
                }
                current_quote = None;
            } else {
                current.push(ch);
            }
            continue;
        }

        // Start of quoted string
        if ch == '"' || ch == '\'' {
            // Flush any pending unquoted content first
            flush_current(&mut current, &mut tokens);
            current_quote = Some(ch);
            continue;
        }

        // Parentheses
        if ch == '(' {
            flush_current(&mut current, &mut tokens);
            tokens.push(SearchToken::OpenParen);
            continue;
        }
        if ch == ')' {
            flush_current(&mut current, &mut tokens);
            tokens.push(SearchToken::CloseParen);
            continue;
        }

        // Whitespace separates tokens
        if ch.is_whitespace() {
            flush_current(&mut current, &mut tokens);
            continue;
        }

        current.push(ch);
    }

    // Flush remaining content
    flush_current(&mut current, &mut tokens);

    tokens
}

/// Parse search tokens into a structured format for SQL generation.
/// Returns a list of tokens with implicit AND operators inserted where needed.
fn normalize_search_tokens(tokens: Vec<SearchToken>) -> Vec<SearchToken> {
    let mut result = Vec::new();
    let mut prev_needs_operator = false;

    for token in tokens {
        match &token {
            SearchToken::Term(_) | SearchToken::OpenParen => {
                // Insert implicit AND if previous token was a term or close paren
                if prev_needs_operator {
                    result.push(SearchToken::And);
                }
                result.push(token);
                prev_needs_operator = matches!(result.last(), Some(SearchToken::Term(_)));
            }
            SearchToken::CloseParen => {
                result.push(token);
                prev_needs_operator = true;
            }
            SearchToken::And | SearchToken::Or => {
                // Only add operator if we have something before it
                if !result.is_empty() {
                    result.push(token);
                    prev_needs_operator = false;
                }
            }
        }
    }

    result
}

fn like_pattern(value: &str) -> String {
    format!("%{}%", value.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to extract term values from tokens
    fn term_values(tokens: &[SearchToken]) -> Vec<&str> {
        tokens
            .iter()
            .filter_map(|t| match t {
                SearchToken::Term(s) => Some(s.as_str()),
                _ => None,
            })
            .collect()
    }

    // Helper to format tokens for debugging
    fn format_tokens(tokens: &[SearchToken]) -> String {
        tokens
            .iter()
            .map(|t| match t {
                SearchToken::Term(s) => format!("Term({})", s),
                SearchToken::And => "AND".to_string(),
                SearchToken::Or => "OR".to_string(),
                SearchToken::OpenParen => "(".to_string(),
                SearchToken::CloseParen => ")".to_string(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    // ==================== tokenize_search tests ====================

    #[test]
    fn test_tokenize_simple_term() {
        let tokens = tokenize_search("hello");
        assert_eq!(term_values(&tokens), vec!["hello"]);
    }

    #[test]
    fn test_tokenize_multiple_terms() {
        let tokens = tokenize_search("hello world");
        assert_eq!(term_values(&tokens), vec!["hello", "world"]);
    }

    #[test]
    fn test_tokenize_and_operator() {
        let tokens = tokenize_search("hello AND world");
        assert_eq!(format_tokens(&tokens), "Term(hello) AND Term(world)");
    }

    #[test]
    fn test_tokenize_or_operator() {
        let tokens = tokenize_search("hello OR world");
        assert_eq!(format_tokens(&tokens), "Term(hello) OR Term(world)");
    }

    #[test]
    fn test_tokenize_operators_case_insensitive() {
        let tokens = tokenize_search("hello and world or foo");
        assert_eq!(
            format_tokens(&tokens),
            "Term(hello) AND Term(world) OR Term(foo)"
        );
    }

    #[test]
    fn test_tokenize_double_quoted_string() {
        let tokens = tokenize_search("\"hello world\"");
        assert_eq!(term_values(&tokens), vec!["hello world"]);
    }

    #[test]
    fn test_tokenize_single_quoted_string() {
        let tokens = tokenize_search("'hello world'");
        assert_eq!(term_values(&tokens), vec!["hello world"]);
    }

    #[test]
    fn test_tokenize_quoted_and_not_interpreted() {
        // "AND" in quotes should be treated as a term, not operator
        let tokens = tokenize_search("\"AND\"");
        assert_eq!(term_values(&tokens), vec!["AND"]);
    }

    #[test]
    fn test_tokenize_quoted_or_not_interpreted() {
        let tokens = tokenize_search("'OR'");
        assert_eq!(term_values(&tokens), vec!["OR"]);
    }

    #[test]
    fn test_tokenize_mixed_quoted_and_unquoted() {
        let tokens = tokenize_search("hello \"world foo\" bar");
        assert_eq!(term_values(&tokens), vec!["hello", "world foo", "bar"]);
    }

    #[test]
    fn test_tokenize_parentheses_simple() {
        let tokens = tokenize_search("(hello)");
        assert_eq!(format_tokens(&tokens), "( Term(hello) )");
    }

    #[test]
    fn test_tokenize_parentheses_with_or() {
        let tokens = tokenize_search("(running OR completed)");
        assert_eq!(
            format_tokens(&tokens),
            "( Term(running) OR Term(completed) )"
        );
    }

    #[test]
    fn test_tokenize_parentheses_complex() {
        let tokens = tokenize_search("(running OR completed) AND workflow");
        assert_eq!(
            format_tokens(&tokens),
            "( Term(running) OR Term(completed) ) AND Term(workflow)"
        );
    }

    #[test]
    fn test_tokenize_nested_parentheses() {
        let tokens = tokenize_search("((a OR b) AND c)");
        assert_eq!(
            format_tokens(&tokens),
            "( ( Term(a) OR Term(b) ) AND Term(c) )"
        );
    }

    #[test]
    fn test_tokenize_parentheses_no_spaces() {
        let tokens = tokenize_search("(hello)AND(world)");
        assert_eq!(
            format_tokens(&tokens),
            "( Term(hello) ) AND ( Term(world) )"
        );
    }

    #[test]
    fn test_tokenize_empty_string() {
        let tokens = tokenize_search("");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenize_whitespace_only() {
        let tokens = tokenize_search("   ");
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenize_extra_whitespace() {
        let tokens = tokenize_search("  hello   world  ");
        assert_eq!(term_values(&tokens), vec!["hello", "world"]);
    }

    // ==================== normalize_search_tokens tests ====================

    #[test]
    fn test_normalize_implicit_and() {
        // "hello world" should become "hello AND world"
        let tokens = tokenize_search("hello world");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "Term(hello) AND Term(world)");
    }

    #[test]
    fn test_normalize_explicit_and_unchanged() {
        let tokens = tokenize_search("hello AND world");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "Term(hello) AND Term(world)");
    }

    #[test]
    fn test_normalize_or_preserved() {
        let tokens = tokenize_search("hello OR world");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "Term(hello) OR Term(world)");
    }

    #[test]
    fn test_normalize_paren_followed_by_term() {
        // "(a) b" should become "(a) AND b"
        let tokens = tokenize_search("(a) b");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "( Term(a) ) AND Term(b)");
    }

    #[test]
    fn test_normalize_term_followed_by_paren() {
        // "a (b)" should become "a AND (b)"
        let tokens = tokenize_search("a (b)");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "Term(a) AND ( Term(b) )");
    }

    #[test]
    fn test_normalize_complex_expression() {
        // "(running OR completed) workflow" -> "(running OR completed) AND workflow"
        let tokens = tokenize_search("(running OR completed) workflow");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(
            format_tokens(&normalized),
            "( Term(running) OR Term(completed) ) AND Term(workflow)"
        );
    }

    #[test]
    fn test_normalize_leading_operator_ignored() {
        // "AND hello" - leading AND should be ignored
        let tokens = tokenize_search("AND hello");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "Term(hello)");
    }

    #[test]
    fn test_normalize_trailing_operator_kept() {
        // "hello AND" - trailing AND is kept (will be harmless in SQL)
        let tokens = tokenize_search("hello AND");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(format_tokens(&normalized), "Term(hello) AND");
    }

    // ==================== Integration tests for full search flow ====================

    #[test]
    fn test_search_flow_simple() {
        let tokens = tokenize_search("running");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(term_values(&normalized), vec!["running"]);
    }

    #[test]
    fn test_search_flow_or_filter() {
        // Typical dropdown selection: "running OR completed"
        let tokens = tokenize_search("running OR completed");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(
            format_tokens(&normalized),
            "Term(running) OR Term(completed)"
        );
    }

    #[test]
    fn test_search_flow_grouped_or_with_and() {
        // Typical combined filter: "(running OR completed) AND my_workflow"
        let tokens = tokenize_search("(running OR completed) AND my_workflow");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(
            format_tokens(&normalized),
            "( Term(running) OR Term(completed) ) AND Term(my_workflow)"
        );
    }

    #[test]
    fn test_search_flow_multiple_groups() {
        // "(a OR b) AND (c OR d)"
        let tokens = tokenize_search("(a OR b) AND (c OR d)");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(
            format_tokens(&normalized),
            "( Term(a) OR Term(b) ) AND ( Term(c) OR Term(d) )"
        );
    }

    #[test]
    fn test_search_flow_quoted_phrase_in_group() {
        let tokens = tokenize_search("(\"hello world\" OR foo)");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(
            format_tokens(&normalized),
            "( Term(hello world) OR Term(foo) )"
        );
    }

    #[test]
    fn test_search_flow_real_world_status_filter() {
        // User selects running and failed from dropdown, then types workflow name
        let tokens = tokenize_search("(running OR failed) AND my_important_workflow");
        let normalized = normalize_search_tokens(tokens);
        assert_eq!(
            format_tokens(&normalized),
            "( Term(running) OR Term(failed) ) AND Term(my_important_workflow)"
        );
    }
}
