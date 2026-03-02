use std::collections::HashMap;

use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Row};

use uuid::Uuid;

use waymark_backends_core::{BackendError, BackendResult};
use waymark_core_backend::GraphUpdate;
use waymark_dag::{DAGNode, EdgeType, convert_to_dag};
use waymark_proto::ast as ir;
use waymark_runner::replay_action_kwargs;
use waymark_runner_state::{
    ActionCallSpec, ExecutionNode, NodeStatus, RunnerState, format_value, value_visitor::ValueExpr,
};
use waymark_webapp_core::{
    ExecutionEdgeView, ExecutionGraphView, ExecutionNodeView, InstanceDetail, InstanceStatus,
    InstanceSummary, ScheduleDetail, ScheduleInvocationSummary, ScheduleSummary, TimelineEntry,
    WorkerActionRow, WorkerAggregateStats, WorkerStatus,
};

const INSTANCE_STATUS_FALLBACK_SQL: &str = r#"
CASE
    WHEN ri.error IS NOT NULL THEN 'failed'
    WHEN ri.result IS NOT NULL THEN 'completed'
    WHEN ri.state IS NOT NULL THEN 'running'
    ELSE 'queued'
END
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
enum InstanceSearchToken {
    Term(String),
    And,
    Or,
    LParen,
    RParen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InstanceSearchExpr {
    Term(String),
    And(Box<InstanceSearchExpr>, Box<InstanceSearchExpr>),
    Or(Box<InstanceSearchExpr>, Box<InstanceSearchExpr>),
}

struct InstanceSearchParser {
    tokens: Vec<InstanceSearchToken>,
    position: usize,
}

impl InstanceSearchParser {
    fn new(tokens: Vec<InstanceSearchToken>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn parse(mut self) -> Option<InstanceSearchExpr> {
        let expr = self.parse_or()?;
        if self.position == self.tokens.len() {
            Some(expr)
        } else {
            None
        }
    }

    fn parse_or(&mut self) -> Option<InstanceSearchExpr> {
        let mut expr = self.parse_and()?;
        while self.consume_or() {
            let rhs = self.parse_and()?;
            expr = InstanceSearchExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Some(expr)
    }

    fn parse_and(&mut self) -> Option<InstanceSearchExpr> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.consume_and() || self.peek_is_primary_start() {
                let rhs = self.parse_primary()?;
                expr = InstanceSearchExpr::And(Box::new(expr), Box::new(rhs));
                continue;
            }
            break;
        }
        Some(expr)
    }

    fn parse_primary(&mut self) -> Option<InstanceSearchExpr> {
        match self.peek()? {
            InstanceSearchToken::Term(term) => {
                let term = term.clone();
                self.position += 1;
                Some(InstanceSearchExpr::Term(term))
            }
            InstanceSearchToken::LParen => {
                self.position += 1;
                let expr = self.parse_or()?;
                if !self.consume_rparen() {
                    return None;
                }
                Some(expr)
            }
            InstanceSearchToken::And | InstanceSearchToken::Or | InstanceSearchToken::RParen => {
                None
            }
        }
    }

    fn consume_and(&mut self) -> bool {
        if matches!(self.peek(), Some(InstanceSearchToken::And)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn consume_or(&mut self) -> bool {
        if matches!(self.peek(), Some(InstanceSearchToken::Or)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn consume_rparen(&mut self) -> bool {
        if matches!(self.peek(), Some(InstanceSearchToken::RParen)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn peek_is_primary_start(&self) -> bool {
        matches!(
            self.peek(),
            Some(InstanceSearchToken::Term(_)) | Some(InstanceSearchToken::LParen)
        )
    }

    fn peek(&self) -> Option<&InstanceSearchToken> {
        self.tokens.get(self.position)
    }
}

fn tokenize_instance_search(search: &str) -> Vec<InstanceSearchToken> {
    let mut chars = search.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch == '(' {
            chars.next();
            tokens.push(InstanceSearchToken::LParen);
            continue;
        }
        if ch == ')' {
            chars.next();
            tokens.push(InstanceSearchToken::RParen);
            continue;
        }
        if ch == '"' {
            chars.next();
            let mut quoted = String::new();
            for next in chars.by_ref() {
                if next == '"' {
                    break;
                }
                quoted.push(next);
            }
            if !quoted.is_empty() {
                tokens.push(InstanceSearchToken::Term(quoted));
            }
            continue;
        }

        let mut term = String::new();
        while let Some(next) = chars.peek().copied() {
            if next.is_whitespace() || next == '(' || next == ')' {
                break;
            }
            term.push(next);
            chars.next();
        }
        if term.is_empty() {
            continue;
        }

        match term.to_ascii_uppercase().as_str() {
            "AND" => tokens.push(InstanceSearchToken::And),
            "OR" => tokens.push(InstanceSearchToken::Or),
            _ => tokens.push(InstanceSearchToken::Term(term)),
        }
    }

    tokens
}

fn parse_instance_search_expr(search: &str) -> Option<InstanceSearchExpr> {
    let trimmed = search.trim();
    if trimmed.is_empty() {
        return None;
    }

    let tokens = tokenize_instance_search(trimmed);
    if tokens.is_empty() {
        return None;
    }

    InstanceSearchParser::new(tokens)
        .parse()
        .or_else(|| Some(InstanceSearchExpr::Term(trimmed.to_string())))
}

fn push_instance_search_expr_sql(
    builder: &mut QueryBuilder<'_, Postgres>,
    expr: &InstanceSearchExpr,
) {
    match expr {
        InstanceSearchExpr::Term(term) => {
            let pattern = format!("%{term}%");
            builder.push("(");
            builder.push("COALESCE(ri.workflow_name, wv.workflow_name, '') ILIKE ");
            builder.push_bind(pattern.clone());
            builder.push(" OR COALESCE(ri.current_status, ");
            builder.push(INSTANCE_STATUS_FALLBACK_SQL);
            builder.push(", '') ILIKE ");
            builder.push_bind(pattern);
            builder.push(")");
        }
        InstanceSearchExpr::And(left, right) => {
            builder.push("(");
            push_instance_search_expr_sql(builder, left);
            builder.push(" AND ");
            push_instance_search_expr_sql(builder, right);
            builder.push(")");
        }
        InstanceSearchExpr::Or(left, right) => {
            builder.push("(");
            push_instance_search_expr_sql(builder, left);
            builder.push(" OR ");
            push_instance_search_expr_sql(builder, right);
            builder.push(")");
        }
    }
}

fn parse_instance_status(status: &str) -> Option<InstanceStatus> {
    match status {
        "queued" => Some(InstanceStatus::Queued),
        "running" => Some(InstanceStatus::Running),
        "completed" => Some(InstanceStatus::Completed),
        "failed" => Some(InstanceStatus::Failed),
        _ => None,
    }
}

#[async_trait::async_trait]
impl waymark_webapp_backend::WebappBackend for crate::PostgresBackend {
    async fn count_instances(&self, search: Option<&str>) -> BackendResult<i64> {
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM runner_instances ri
            LEFT JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
            "#,
        );

        if let Some(search_expr) = search.and_then(parse_instance_search_expr) {
            builder.push(" WHERE ");
            push_instance_search_expr_sql(&mut builder, &search_expr);
        }

        let count: i64 = builder.build_query_scalar().fetch_one(&self.pool).await?;
        Ok(count)
    }

    async fn list_instances(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<InstanceSummary>> {
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            r#"
            SELECT
                ri.instance_id,
                ri.entry_node,
                ri.created_at,
                ri.state,
                ri.result,
                ri.error,
                COALESCE(ri.workflow_name, wv.workflow_name) AS workflow_name,
                COALESCE(ri.current_status,
                    CASE
                        WHEN ri.error IS NOT NULL THEN 'failed'
                        WHEN ri.result IS NOT NULL THEN 'completed'
                        WHEN ri.state IS NOT NULL THEN 'running'
                        ELSE 'queued'
                    END
                ) AS current_status
            FROM runner_instances ri
            LEFT JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
            "#,
        );
        if let Some(search_expr) = search.and_then(parse_instance_search_expr) {
            builder.push(" WHERE ");
            push_instance_search_expr_sql(&mut builder, &search_expr);
        }
        builder.push(" ORDER BY ri.created_at DESC, ri.instance_id DESC LIMIT ");
        builder.push_bind(limit);
        builder.push(" OFFSET ");
        builder.push_bind(offset);
        let rows = builder.build().fetch_all(&self.pool).await?;

        let mut instances = Vec::new();
        for row in rows {
            let instance_id: Uuid = row.get("instance_id");
            let entry_node: Uuid = row.get("entry_node");
            let created_at: DateTime<Utc> = row.get("created_at");
            let state_bytes: Option<Vec<u8>> = row.get("state");
            let result_bytes: Option<Vec<u8>> = row.get("result");
            let error_bytes: Option<Vec<u8>> = row.get("error");
            let workflow_name: Option<String> = row.get("workflow_name");
            let current_status: Option<String> = row.get("current_status");

            let status = current_status
                .as_deref()
                .and_then(parse_instance_status)
                .unwrap_or_else(|| determine_status(&state_bytes, &result_bytes, &error_bytes));
            let input_preview = extract_input_preview(&state_bytes);

            instances.push(InstanceSummary {
                id: instance_id,
                entry_node,
                created_at,
                status,
                workflow_name,
                input_preview,
            });
        }

        Ok(instances)
    }

    async fn get_instance(&self, instance_id: Uuid) -> BackendResult<InstanceDetail> {
        let row = sqlx::query(
            r#"
            SELECT
                ri.instance_id,
                ri.entry_node,
                ri.created_at,
                ri.state,
                ri.result,
                ri.error,
                COALESCE(ri.workflow_name, wv.workflow_name) AS workflow_name,
                COALESCE(ri.current_status,
                    CASE
                        WHEN ri.error IS NOT NULL THEN 'failed'
                        WHEN ri.result IS NOT NULL THEN 'completed'
                        WHEN ri.state IS NOT NULL THEN 'running'
                        ELSE 'queued'
                    END
                ) AS current_status
            FROM runner_instances ri
            LEFT JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
            WHERE ri.instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BackendError::Message(format!("instance not found: {}", instance_id)))?;

        let instance_id: Uuid = row.get("instance_id");
        let entry_node: Uuid = row.get("entry_node");
        let created_at: DateTime<Utc> = row.get("created_at");
        let state_bytes: Option<Vec<u8>> = row.get("state");
        let result_bytes: Option<Vec<u8>> = row.get("result");
        let error_bytes: Option<Vec<u8>> = row.get("error");
        let workflow_name: Option<String> = row.get("workflow_name");
        let current_status: Option<String> = row.get("current_status");

        let status = current_status
            .as_deref()
            .and_then(parse_instance_status)
            .unwrap_or_else(|| determine_status(&state_bytes, &result_bytes, &error_bytes));
        let input_payload = format_input_payload(&state_bytes);
        let result_payload = format_instance_result_payload(status, &result_bytes, &error_bytes);
        let error_payload = format_error(&error_bytes);

        Ok(InstanceDetail {
            id: instance_id,
            entry_node,
            created_at,
            status,
            workflow_name,
            input_payload,
            result_payload,
            error_payload,
        })
    }

    async fn get_execution_graph(
        &self,
        instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>> {
        let row = sqlx::query(
            r#"
            SELECT state FROM runner_instances WHERE instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let state_bytes: Option<Vec<u8>> = row.get("state");
        let Some(state_bytes) = state_bytes else {
            return Ok(None);
        };

        let graph_update: GraphUpdate = rmp_serde::from_slice(&state_bytes)
            .map_err(|e| BackendError::Message(format!("failed to decode state: {}", e)))?;

        let nodes: Vec<ExecutionNodeView> = graph_update
            .nodes
            .values()
            .map(|node| ExecutionNodeView {
                id: node.node_id.to_string(),
                node_type: node.node_type.clone(),
                label: node.label.clone(),
                status: format_node_status(&node.status),
                action_name: node.action.as_ref().map(|a| a.action_name.clone()),
                module_name: node.action.as_ref().and_then(|a| a.module_name.clone()),
            })
            .collect();

        let edges: Vec<ExecutionEdgeView> = graph_update
            .edges
            .iter()
            .map(|edge| ExecutionEdgeView {
                source: edge.source.to_string(),
                target: edge.target.to_string(),
                edge_type: format!("{:?}", edge.edge_type),
            })
            .collect();

        Ok(Some(ExecutionGraphView { nodes, edges }))
    }

    async fn get_workflow_graph(
        &self,
        instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>> {
        let row = sqlx::query(
            r#"
            SELECT ri.state, wv.program_proto
            FROM runner_instances ri
            JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
            WHERE ri.instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let program_proto: Vec<u8> = row.get("program_proto");
        let program = ir::Program::decode(&program_proto[..])
            .map_err(|err| BackendError::Message(format!("failed to decode workflow IR: {err}")))?;
        let dag = convert_to_dag(&program).map_err(|err| {
            BackendError::Message(format!("failed to convert workflow DAG: {err}"))
        })?;

        let mut template_statuses: HashMap<String, NodeStatus> = HashMap::new();
        let state_bytes: Option<Vec<u8>> = row.get("state");
        if let Some(state_bytes) = state_bytes {
            let graph_update: GraphUpdate = rmp_serde::from_slice(&state_bytes)
                .map_err(|err| BackendError::Message(format!("failed to decode state: {err}")))?;

            for node in graph_update.nodes.values() {
                let Some(template_id) = node.template_id.as_ref() else {
                    continue;
                };
                template_statuses
                    .entry(template_id.clone())
                    .and_modify(|existing| {
                        *existing = merge_template_status(existing, &node.status);
                    })
                    .or_insert_with(|| node.status.clone());
            }
        }

        let mut node_ids: Vec<String> = dag.nodes.keys().cloned().collect();
        node_ids.sort();
        let nodes: Vec<ExecutionNodeView> = node_ids
            .into_iter()
            .filter_map(|node_id| {
                let node = dag.nodes.get(&node_id)?;
                let status = template_statuses
                    .get(&node_id)
                    .map(format_node_status)
                    .unwrap_or_else(|| "pending".to_string());
                let (action_name, module_name) = match node {
                    DAGNode::ActionCall(action) => {
                        (Some(action.action_name.clone()), action.module_name.clone())
                    }
                    _ => (None, None),
                };

                Some(ExecutionNodeView {
                    id: node_id,
                    node_type: node.node_type().to_string(),
                    label: node.label(),
                    status,
                    action_name,
                    module_name,
                })
            })
            .collect();

        let edges: Vec<ExecutionEdgeView> = dag
            .edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::StateMachine)
            .map(|edge| ExecutionEdgeView {
                source: edge.source.clone(),
                target: edge.target.clone(),
                edge_type: if edge.is_loop_back {
                    "state_machine_loop_back".to_string()
                } else {
                    "state_machine".to_string()
                },
            })
            .collect();

        Ok(Some(ExecutionGraphView { nodes, edges }))
    }

    async fn get_action_results(&self, instance_id: Uuid) -> BackendResult<Vec<TimelineEntry>> {
        let row = sqlx::query(
            r#"
            SELECT state
            FROM runner_instances
            WHERE instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(Vec::new());
        };
        let state_bytes: Option<Vec<u8>> = row.get("state");
        let Some(state_bytes) = state_bytes else {
            return Ok(Vec::new());
        };
        let graph_update: GraphUpdate = rmp_serde::from_slice(&state_bytes)
            .map_err(|e| BackendError::Message(format!("failed to decode state: {}", e)))?;

        let runner_state = RunnerState::new(
            None,
            Some(graph_update.nodes.clone()),
            Some(graph_update.edges),
            false,
        );
        let action_nodes: HashMap<Uuid, ExecutionNode> = graph_update
            .nodes
            .into_iter()
            .filter(|(_, node)| node.is_action_call())
            .collect();
        if action_nodes.is_empty() {
            return Ok(Vec::new());
        }
        let execution_ids: Vec<Uuid> = action_nodes.keys().copied().collect();

        let rows = sqlx::query(
            r#"
            SELECT created_at, execution_id, attempt, status, started_at, completed_at, duration_ms, result
            FROM runner_actions_done
            WHERE execution_id = ANY($1)
            ORDER BY created_at ASC, attempt ASC
            "#,
        )
        .bind(&execution_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut decoded_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let created_at: DateTime<Utc> = row.get("created_at");
            let execution_id: Uuid = row.get("execution_id");
            let attempt: i32 = row.get("attempt");
            let status: Option<String> = row.get("status");
            let started_at: Option<DateTime<Utc>> = row.get("started_at");
            let completed_at: Option<DateTime<Utc>> = row.get("completed_at");
            let duration_ms: Option<i64> = row.get("duration_ms");
            let result_bytes: Option<Vec<u8>> = row.get("result");
            let result = result_bytes
                .as_deref()
                .map(decode_msgpack_json)
                .transpose()?;
            decoded_rows.push(DecodedActionResultRow {
                created_at,
                execution_id,
                attempt,
                status,
                started_at,
                completed_at,
                duration_ms,
                result,
            });
        }

        // Replay needs the current known action outputs by execution id.
        let mut action_results = HashMap::new();
        for row in &decoded_rows {
            if let Some(result) = &row.result {
                action_results.insert(row.execution_id, result.clone());
            }
        }

        let mut request_preview_cache: HashMap<Uuid, String> = HashMap::new();
        let mut entries = Vec::with_capacity(decoded_rows.len());
        for row in decoded_rows {
            let node = action_nodes.get(&row.execution_id);
            let action_name = node
                .and_then(|n| n.action.as_ref().map(|a| a.action_name.clone()))
                .unwrap_or_default();
            let module_name =
                node.and_then(|n| n.action.as_ref().and_then(|a| a.module_name.clone()));

            let request_preview =
                if let Some(existing) = request_preview_cache.get(&row.execution_id) {
                    existing.clone()
                } else {
                    let rendered = render_action_request_preview(
                        node.and_then(|n| n.action.as_ref()),
                        &runner_state,
                        &action_results,
                        row.execution_id,
                    );
                    request_preview_cache.insert(row.execution_id, rendered.clone());
                    rendered
                };

            let (response_preview, error) = match &row.result {
                Some(value) => format_action_result(value),
                None => ("(no result)".to_string(), None),
            };
            let status = row.status.clone().unwrap_or_else(|| {
                if error.is_some() {
                    "failed".to_string()
                } else {
                    "completed".to_string()
                }
            });
            let (dispatched_at, completed_at, duration_ms) = if row.started_at.is_some()
                || row.completed_at.is_some()
                || row.duration_ms.is_some()
            {
                (
                    Some(row.started_at.unwrap_or(row.created_at).to_rfc3339()),
                    Some(row.completed_at.unwrap_or(row.created_at).to_rfc3339()),
                    row.duration_ms,
                )
            } else {
                action_timing_from_state(node, row.attempt, row.created_at)
            };

            entries.push(TimelineEntry {
                action_id: row.execution_id.to_string(),
                action_name,
                module_name,
                status,
                attempt_number: row.attempt,
                dispatched_at,
                completed_at,
                duration_ms,
                request_preview,
                response_preview,
                error,
            });
        }

        Ok(entries)
    }

    async fn get_distinct_workflows(&self) -> BackendResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT COALESCE(ri.workflow_name, wv.workflow_name) AS workflow_name
            FROM runner_instances ri
            LEFT JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
            WHERE COALESCE(ri.workflow_name, wv.workflow_name) IS NOT NULL
            ORDER BY workflow_name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut workflows = Vec::with_capacity(rows.len());
        for row in rows {
            let workflow_name: String = row.get("workflow_name");
            workflows.push(workflow_name);
        }
        Ok(workflows)
    }

    async fn get_distinct_statuses(&self) -> BackendResult<Vec<String>> {
        Ok(vec![
            "queued".to_string(),
            "running".to_string(),
            "completed".to_string(),
            "failed".to_string(),
        ])
    }

    async fn count_schedules(&self) -> BackendResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_schedules WHERE status != 'deleted'",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    async fn list_schedules(&self, limit: i64, offset: i64) -> BackendResult<Vec<ScheduleSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   status, next_run_at, last_run_at, created_at
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY workflow_name, schedule_name
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut schedules = Vec::new();
        for row in rows {
            schedules.push(ScheduleSummary {
                id: row.get::<Uuid, _>("id").to_string(),
                workflow_name: row.get("workflow_name"),
                schedule_name: row.get("schedule_name"),
                schedule_type: row.get("schedule_type"),
                cron_expression: row.get("cron_expression"),
                interval_seconds: row.get("interval_seconds"),
                status: row.get("status"),
                next_run_at: row
                    .get::<Option<DateTime<Utc>>, _>("next_run_at")
                    .map(|dt| dt.to_rfc3339()),
                last_run_at: row
                    .get::<Option<DateTime<Utc>>, _>("last_run_at")
                    .map(|dt| dt.to_rfc3339()),
                created_at: row.get::<DateTime<Utc>, _>("created_at").to_rfc3339(),
            });
        }

        Ok(schedules)
    }

    async fn get_schedule(&self, schedule_id: Uuid) -> BackendResult<ScheduleDetail> {
        let row = sqlx::query(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE id = $1
            "#,
        )
        .bind(schedule_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BackendError::Message(format!("schedule not found: {}", schedule_id)))?;

        let input_payload: Option<String> = row
            .get::<Option<Vec<u8>>, _>("input_payload")
            .and_then(|bytes| {
                rmp_serde::from_slice::<serde_json::Value>(&bytes)
                    .ok()
                    .map(|v| serde_json::to_string_pretty(&v).unwrap_or_default())
            });

        Ok(ScheduleDetail {
            id: row.get::<Uuid, _>("id").to_string(),
            workflow_name: row.get("workflow_name"),
            schedule_name: row.get("schedule_name"),
            schedule_type: row.get("schedule_type"),
            cron_expression: row.get("cron_expression"),
            interval_seconds: row.get("interval_seconds"),
            jitter_seconds: row.get("jitter_seconds"),
            status: row.get("status"),
            next_run_at: row
                .get::<Option<DateTime<Utc>>, _>("next_run_at")
                .map(|dt| dt.to_rfc3339()),
            last_run_at: row
                .get::<Option<DateTime<Utc>>, _>("last_run_at")
                .map(|dt| dt.to_rfc3339()),
            last_instance_id: row
                .get::<Option<Uuid>, _>("last_instance_id")
                .map(|id| id.to_string()),
            created_at: row.get::<DateTime<Utc>, _>("created_at").to_rfc3339(),
            updated_at: row.get::<DateTime<Utc>, _>("updated_at").to_rfc3339(),
            priority: row.get("priority"),
            allow_duplicate: row.get("allow_duplicate"),
            input_payload,
        })
    }

    async fn count_schedule_invocations(&self, schedule_id: Uuid) -> BackendResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM runner_instances
            WHERE schedule_id = $1
            "#,
        )
        .bind(schedule_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    async fn list_schedule_invocations(
        &self,
        schedule_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<ScheduleInvocationSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT instance_id, created_at, state, result, error
            FROM runner_instances
            WHERE schedule_id = $1
            ORDER BY created_at DESC, instance_id DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(schedule_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut invocations = Vec::with_capacity(rows.len());
        for row in rows {
            let state_bytes: Option<Vec<u8>> = row.get("state");
            let result_bytes: Option<Vec<u8>> = row.get("result");
            let error_bytes: Option<Vec<u8>> = row.get("error");

            invocations.push(ScheduleInvocationSummary {
                id: row.get("instance_id"),
                created_at: row.get("created_at"),
                status: determine_status(&state_bytes, &result_bytes, &error_bytes),
            });
        }

        Ok(invocations)
    }

    async fn update_schedule_status(&self, schedule_id: Uuid, status: &str) -> BackendResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn get_distinct_schedule_statuses(&self) -> BackendResult<Vec<String>> {
        Ok(vec!["active".to_string(), "paused".to_string()])
    }

    async fn get_distinct_schedule_types(&self) -> BackendResult<Vec<String>> {
        Ok(vec!["cron".to_string(), "interval".to_string()])
    }

    async fn get_worker_action_stats(
        &self,
        window_minutes: i64,
    ) -> BackendResult<Vec<WorkerActionRow>> {
        let rows = sqlx::query(
            r#"
            SELECT
                pool_id,
                COUNT(DISTINCT worker_id) as active_workers,
                SUM(throughput_per_min) / 60.0 as actions_per_sec,
                SUM(throughput_per_min) as throughput_per_min,
                COALESCE(SUM(total_completed), 0)::BIGINT as total_completed,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_dequeue_ms) as median_dequeue_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_handling_ms) as median_handling_ms,
                MAX(last_action_at) as last_action_at,
                MAX(updated_at) as updated_at
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            GROUP BY pool_id
            ORDER BY actions_per_sec DESC
            "#,
        )
        .bind(window_minutes)
        .fetch_all(&self.pool)
        .await?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(WorkerActionRow {
                pool_id: row.get::<Uuid, _>("pool_id").to_string(),
                active_workers: row.get::<i64, _>("active_workers"),
                actions_per_sec: format!("{:.1}", row.get::<f64, _>("actions_per_sec")),
                throughput_per_min: row.get::<f64, _>("throughput_per_min") as i64,
                total_completed: row.get::<i64, _>("total_completed"),
                median_dequeue_ms: row
                    .get::<Option<f64>, _>("median_dequeue_ms")
                    .map(|v| v as i64),
                median_handling_ms: row
                    .get::<Option<f64>, _>("median_handling_ms")
                    .map(|v| v as i64),
                last_action_at: row
                    .get::<Option<DateTime<Utc>>, _>("last_action_at")
                    .map(|dt| dt.to_rfc3339()),
                updated_at: row.get::<DateTime<Utc>, _>("updated_at").to_rfc3339(),
            });
        }

        Ok(stats)
    }

    async fn get_worker_aggregate_stats(
        &self,
        window_minutes: i64,
    ) -> BackendResult<WorkerAggregateStats> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(DISTINCT worker_id) as active_worker_count,
                COALESCE(SUM(throughput_per_min) / 60.0, 0) as actions_per_sec,
                COALESCE(SUM(total_in_flight), 0)::BIGINT as total_in_flight,
                COALESCE(SUM(dispatch_queue_size), 0)::BIGINT as total_queue_depth
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            "#,
        )
        .bind(window_minutes)
        .fetch_one(&self.pool)
        .await?;

        Ok(WorkerAggregateStats {
            active_worker_count: row.get::<i64, _>("active_worker_count"),
            actions_per_sec: format!("{:.1}", row.get::<f64, _>("actions_per_sec")),
            total_in_flight: row.get::<i64, _>("total_in_flight"),
            total_queue_depth: row.get::<i64, _>("total_queue_depth"),
        })
    }

    async fn worker_status_table_exists(&self) -> bool {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'worker_status'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false)
    }

    async fn schedules_table_exists(&self) -> bool {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'workflow_schedules'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false)
    }

    async fn get_worker_statuses(&self, window_minutes: i64) -> BackendResult<Vec<WorkerStatus>> {
        let rows = sqlx::query(
            r#"
            SELECT
                pool_id,
                MAX(active_workers) as active_workers,
                COALESCE(SUM(throughput_per_min), 0) as throughput_per_min,
                COALESCE(SUM(throughput_per_min) / 60.0, 0) as actions_per_sec,
                COALESCE(SUM(total_completed), 0)::BIGINT as total_completed,
                MAX(last_action_at) as last_action_at,
                MAX(updated_at) as updated_at,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_dequeue_ms) as median_dequeue_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_handling_ms) as median_handling_ms,
                MAX(dispatch_queue_size) as dispatch_queue_size,
                MAX(total_in_flight) as total_in_flight,
                MAX(median_instance_duration_secs) as median_instance_duration_secs,
                MAX(active_instance_count) as active_instance_count,
                (
                    SELECT COUNT(*)::BIGINT
                    FROM runner_instances ri
                    WHERE ri.result IS NOT NULL
                      AND ri.error IS NULL
                ) as total_instances_completed,
                MAX(instances_per_sec) as instances_per_sec,
                MAX(instances_per_min) as instances_per_min,
                (
                    SELECT time_series FROM worker_status ws2
                    WHERE ws2.pool_id = worker_status.pool_id
                      AND ws2.time_series IS NOT NULL
                    ORDER BY ws2.updated_at DESC LIMIT 1
                ) as time_series
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            GROUP BY pool_id
            ORDER BY actions_per_sec DESC
            "#,
        )
        .bind(window_minutes)
        .fetch_all(&self.pool)
        .await?;

        let mut statuses = Vec::new();
        for row in rows {
            statuses.push(WorkerStatus {
                pool_id: row.get::<Uuid, _>("pool_id"),
                active_workers: row.get::<Option<i32>, _>("active_workers").unwrap_or(0),
                throughput_per_min: row.get::<f64, _>("throughput_per_min"),
                actions_per_sec: row.get::<f64, _>("actions_per_sec"),
                total_completed: row.get::<i64, _>("total_completed"),
                last_action_at: row.get::<Option<DateTime<Utc>>, _>("last_action_at"),
                updated_at: row.get::<DateTime<Utc>, _>("updated_at"),
                median_dequeue_ms: row
                    .get::<Option<f64>, _>("median_dequeue_ms")
                    .map(|v| v as i64),
                median_handling_ms: row
                    .get::<Option<f64>, _>("median_handling_ms")
                    .map(|v| v as i64),
                dispatch_queue_size: row.get::<Option<i64>, _>("dispatch_queue_size"),
                total_in_flight: row.get::<Option<i64>, _>("total_in_flight"),
                median_instance_duration_secs: row
                    .get::<Option<f64>, _>("median_instance_duration_secs"),
                active_instance_count: row
                    .get::<Option<i32>, _>("active_instance_count")
                    .unwrap_or(0),
                total_instances_completed: row
                    .get::<Option<i64>, _>("total_instances_completed")
                    .unwrap_or(0),
                instances_per_sec: row
                    .get::<Option<f64>, _>("instances_per_sec")
                    .unwrap_or(0.0),
                instances_per_min: row
                    .get::<Option<f64>, _>("instances_per_min")
                    .unwrap_or(0.0),
                time_series: row.get::<Option<Vec<u8>>, _>("time_series"),
            });
        }

        Ok(statuses)
    }
}

struct DecodedActionResultRow {
    created_at: DateTime<Utc>,
    execution_id: Uuid,
    attempt: i32,
    status: Option<String>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    duration_ms: Option<i64>,
    result: Option<Value>,
}

fn decode_msgpack_json(bytes: &[u8]) -> BackendResult<Value> {
    rmp_serde::from_slice::<Value>(bytes)
        .map_err(|err| BackendError::Message(format!("failed to decode action result: {err}")))
}

fn render_action_request_preview(
    action: Option<&ActionCallSpec>,
    state: &RunnerState,
    action_results: &HashMap<Uuid, Value>,
    node_id: Uuid,
) -> String {
    let Some(action) = action else {
        return "{}".to_string();
    };

    match replay_action_kwargs(state, action_results, node_id) {
        Ok(kwargs) => {
            let rendered_map: serde_json::Map<String, Value> = kwargs.into_iter().collect();
            pretty_json(&Value::Object(rendered_map))
        }
        Err(_) => format_symbolic_kwargs(action),
    }
}

fn format_symbolic_kwargs(action: &ActionCallSpec) -> String {
    if action.kwargs.is_empty() {
        return "{}".to_string();
    }
    let rendered_map: serde_json::Map<String, Value> = action
        .kwargs
        .iter()
        .map(|(name, expr)| (name.clone(), Value::String(format_value(expr))))
        .collect();
    pretty_json(&Value::Object(rendered_map))
}

fn action_timing_from_state(
    node: Option<&ExecutionNode>,
    attempt: i32,
    fallback_completed_at: DateTime<Utc>,
) -> (Option<String>, Option<String>, Option<i64>) {
    // Node timing fields represent the latest attempt for this execution id.
    // For historical retries, fall back to row timestamps from actions_done.
    let Some(node) = node else {
        let at = fallback_completed_at.to_rfc3339();
        return (Some(at.clone()), Some(at), None);
    };
    if node.action_attempt != attempt {
        let at = fallback_completed_at.to_rfc3339();
        return (Some(at.clone()), Some(at), None);
    }

    let dispatched_at = node
        .started_at
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| fallback_completed_at.to_rfc3339());
    let completed_dt = node.completed_at.unwrap_or(fallback_completed_at);
    let completed_at = completed_dt.to_rfc3339();
    let duration_ms = node
        .started_at
        .map(|started_at| {
            completed_dt
                .signed_duration_since(started_at)
                .num_milliseconds()
        })
        .filter(|duration| *duration >= 0);

    (Some(dispatched_at), Some(completed_at), duration_ms)
}

fn format_action_result(value: &Value) -> (String, Option<String>) {
    let preview = pretty_json(value);
    let error = extract_action_error(value);
    (preview, error)
}

fn extract_action_error(value: &Value) -> Option<String> {
    let Value::Object(map) = value else {
        return None;
    };
    let message = map.get("message").and_then(Value::as_str);
    let is_exception = map.contains_key("type") && map.contains_key("message");
    if is_exception {
        return Some(message.unwrap_or("action failed").to_string());
    }
    map.get("error")
        .and_then(Value::as_str)
        .map(|msg| msg.to_string())
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string())
}

fn determine_status(
    state_bytes: &Option<Vec<u8>>,
    result_bytes: &Option<Vec<u8>>,
    error_bytes: &Option<Vec<u8>>,
) -> InstanceStatus {
    if error_bytes.is_some() {
        return InstanceStatus::Failed;
    }
    if result_bytes
        .as_deref()
        .is_some_and(result_payload_is_error_wrapper)
    {
        return InstanceStatus::Failed;
    }
    if result_bytes.is_some() {
        return InstanceStatus::Completed;
    }
    if state_bytes.is_some() {
        return InstanceStatus::Running;
    }
    InstanceStatus::Queued
}

fn extract_input_preview(state_bytes: &Option<Vec<u8>>) -> String {
    let Some(bytes) = state_bytes else {
        return "{}".to_string();
    };

    match rmp_serde::from_slice::<GraphUpdate>(bytes) {
        Ok(graph) => {
            let count = graph.nodes.len();
            format!("{{nodes: {count}}}")
        }
        Err(_) => "{}".to_string(),
    }
}

fn format_input_payload(state_bytes: &Option<Vec<u8>>) -> String {
    let Some(bytes) = state_bytes else {
        return "{}".to_string();
    };

    match rmp_serde::from_slice::<GraphUpdate>(bytes) {
        Ok(graph) => format_extracted_inputs(&graph.nodes),
        Err(_) => "{}".to_string(),
    }
}

fn format_extracted_inputs(nodes: &HashMap<Uuid, ExecutionNode>) -> String {
    let mut input_pairs: Vec<(String, Value)> = nodes
        .values()
        .filter_map(extract_input_assignment)
        .collect();
    if input_pairs.is_empty() {
        return "{}".to_string();
    }
    input_pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
    let input_map: serde_json::Map<String, Value> = input_pairs.into_iter().collect();
    pretty_json(&Value::Object(input_map))
}

fn extract_input_assignment(node: &ExecutionNode) -> Option<(String, Value)> {
    let (name, raw_value) = parse_input_assignment_label(&node.label)?;

    if let Ok(value) = serde_json::from_str::<Value>(raw_value) {
        return Some((name.to_string(), value));
    }

    if let Some(value_expr) = node.assignments.get(name) {
        return Some((name.to_string(), value_expr_to_json(value_expr)));
    }

    Some((name.to_string(), Value::String(raw_value.to_string())))
}

fn parse_input_assignment_label(label: &str) -> Option<(&str, &str)> {
    let payload = label.strip_prefix("input ")?;
    payload.split_once(" = ")
}

fn value_expr_to_json(value_expr: &ValueExpr) -> Value {
    match value_expr {
        ValueExpr::Literal(value) => value.value.clone(),
        ValueExpr::List(value) => {
            Value::Array(value.elements.iter().map(value_expr_to_json).collect())
        }
        ValueExpr::Dict(value) => {
            let mut map = serde_json::Map::new();
            for entry in &value.entries {
                let key = match value_expr_to_json(&entry.key) {
                    Value::String(key) => key,
                    other => other.to_string(),
                };
                map.insert(key, value_expr_to_json(&entry.value));
            }
            Value::Object(map)
        }
        _ => Value::String(format_value(value_expr)),
    }
}

fn format_instance_result_payload(
    status: InstanceStatus,
    result_bytes: &Option<Vec<u8>>,
    error_bytes: &Option<Vec<u8>>,
) -> String {
    match status {
        InstanceStatus::Failed => {
            let payload = error_bytes.as_deref().or(result_bytes.as_deref());
            let Some(bytes) = payload else {
                return "(failed)".to_string();
            };
            match rmp_serde::from_slice::<serde_json::Value>(bytes) {
                Ok(value) => pretty_json(&normalize_error_payload(value)),
                Err(_) => "(decode error)".to_string(),
            }
        }
        InstanceStatus::Completed => {
            let Some(bytes) = result_bytes else {
                return "(pending)".to_string();
            };
            match rmp_serde::from_slice::<serde_json::Value>(bytes) {
                Ok(value) => pretty_json(&normalize_success_payload(value)),
                Err(_) => "(decode error)".to_string(),
            }
        }
        InstanceStatus::Running | InstanceStatus::Queued => "(pending)".to_string(),
    }
}

fn normalize_success_payload(value: Value) -> Value {
    let Value::Object(mut map) = value else {
        return value;
    };
    map.remove("result").unwrap_or(Value::Object(map))
}

fn normalize_error_payload(value: Value) -> Value {
    let Value::Object(mut map) = value else {
        return value;
    };

    if let Some(error) = map.remove("error") {
        return normalize_error_payload(error);
    }
    if let Some(exception) = map.remove("__exception__") {
        return normalize_error_payload(exception);
    }
    if let Some(exception) = map.remove("exception") {
        return normalize_error_payload(exception);
    }

    Value::Object(map)
}

fn result_payload_is_error_wrapper(bytes: &[u8]) -> bool {
    let Ok(value) = rmp_serde::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    let Value::Object(map) = value else {
        return false;
    };
    map.len() == 1
        && (map.contains_key("error")
            || map.contains_key("__exception__")
            || map.contains_key("exception"))
}

fn format_error(error_bytes: &Option<Vec<u8>>) -> Option<String> {
    let bytes = error_bytes.as_ref()?;

    match rmp_serde::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => Some(pretty_json(&normalize_error_payload(value))),
        Err(_) => Some("(decode error)".to_string()),
    }
}

fn format_node_status(status: &NodeStatus) -> String {
    match status {
        NodeStatus::Queued => "queued".to_string(),
        NodeStatus::Running => "running".to_string(),
        NodeStatus::Completed => "completed".to_string(),
        NodeStatus::Failed => "failed".to_string(),
    }
}

fn merge_template_status(existing: &NodeStatus, new_status: &NodeStatus) -> NodeStatus {
    if node_status_rank(new_status) > node_status_rank(existing) {
        new_status.clone()
    } else {
        existing.clone()
    }
}

fn node_status_rank(status: &NodeStatus) -> u8 {
    match status {
        NodeStatus::Completed => 0,
        NodeStatus::Queued => 1,
        NodeStatus::Running => 2,
        NodeStatus::Failed => 3,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use chrono::{Duration as ChronoDuration, Utc};
    use prost::Message;
    use serial_test::serial;
    use uuid::Uuid;
    use waymark_scheduler_backend::SchedulerBackend;
    use waymark_webapp_backend::WebappBackend;
    use waymark_worker_status_backend::{WorkerStatusBackend, WorkerStatusUpdate};
    use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend};

    use crate::PostgresBackend;

    use super::super::test_helpers::setup_backend;
    use super::*;

    use waymark_dag::EdgeType;
    use waymark_ir_parser::parse_program;
    use waymark_runner_state::{
        ActionCallSpec, ExecutionEdge, ExecutionNode, LiteralValue, NodeStatus,
        value_visitor::ValueExpr,
    };
    use waymark_scheduler_core::{CreateScheduleParams, ScheduleType};

    #[test]
    fn format_extracted_inputs_happy_path() {
        let mut nodes = HashMap::new();
        let mut first_assignments = HashMap::new();
        first_assignments.insert(
            "iterations".to_string(),
            ValueExpr::Literal(LiteralValue {
                value: serde_json::json!(3),
            }),
        );
        nodes.insert(
            Uuid::new_v4(),
            ExecutionNode {
                node_id: Uuid::new_v4(),
                node_type: "assignment".to_string(),
                label: "input iterations = 3".to_string(),
                status: NodeStatus::Completed,
                template_id: None,
                targets: vec!["iterations".to_string()],
                action: None,
                value_expr: None,
                assignments: first_assignments,
                action_attempt: 0,
                started_at: None,
                completed_at: None,
                scheduled_at: None,
            },
        );

        let mut second_assignments = HashMap::new();
        second_assignments.insert(
            "sleep_seconds".to_string(),
            ValueExpr::Literal(LiteralValue {
                value: serde_json::json!(20),
            }),
        );
        nodes.insert(
            Uuid::new_v4(),
            ExecutionNode {
                node_id: Uuid::new_v4(),
                node_type: "assignment".to_string(),
                label: "input sleep_seconds = 20".to_string(),
                status: NodeStatus::Completed,
                template_id: None,
                targets: vec!["sleep_seconds".to_string()],
                action: None,
                value_expr: None,
                assignments: second_assignments,
                action_attempt: 0,
                started_at: None,
                completed_at: None,
                scheduled_at: None,
            },
        );

        let rendered = format_extracted_inputs(&nodes);
        let value: Value = serde_json::from_str(&rendered).expect("decode rendered input payload");
        assert_eq!(
            value,
            serde_json::json!({
                "iterations": 3,
                "sleep_seconds": 20
            })
        );
    }

    #[test]
    fn format_instance_result_payload_unwraps_success_result_wrapper() {
        let result_bytes =
            rmp_serde::to_vec_named(&serde_json::json!({"result": {"total_iterations": 3}}))
                .expect("encode result");
        let rendered =
            format_instance_result_payload(InstanceStatus::Completed, &Some(result_bytes), &None);
        let value: Value = serde_json::from_str(&rendered).expect("decode result payload");
        assert_eq!(value, serde_json::json!({"total_iterations": 3}));
    }

    #[test]
    fn format_instance_result_payload_unwraps_error_wrapper() {
        let error_bytes = rmp_serde::to_vec_named(&serde_json::json!({
            "error": {
                "__exception__": {
                    "type": "ValueError",
                    "message": "boom"
                }
            }
        }))
        .expect("encode error");
        let rendered =
            format_instance_result_payload(InstanceStatus::Failed, &None, &Some(error_bytes));
        let value: Value = serde_json::from_str(&rendered).expect("decode result payload");
        assert_eq!(
            value,
            serde_json::json!({
                "type": "ValueError",
                "message": "boom"
            })
        );
    }

    #[test]
    fn determine_status_marks_wrapped_result_errors_as_failed() {
        let result_bytes =
            rmp_serde::to_vec_named(&serde_json::json!({"error": {"message": "boom"}}))
                .expect("encode result error");
        let status = determine_status(&None, &Some(result_bytes), &None);
        assert_eq!(status, InstanceStatus::Failed);
    }

    #[test]
    fn parse_instance_search_expr_handles_boolean_operators() {
        let parsed = parse_instance_search_expr("(alpha OR beta) AND running");
        assert_eq!(
            parsed,
            Some(InstanceSearchExpr::And(
                Box::new(InstanceSearchExpr::Or(
                    Box::new(InstanceSearchExpr::Term("alpha".to_string())),
                    Box::new(InstanceSearchExpr::Term("beta".to_string())),
                )),
                Box::new(InstanceSearchExpr::Term("running".to_string())),
            ))
        );
    }

    #[test]
    fn parse_instance_search_expr_falls_back_for_unbalanced_parentheses() {
        let parsed = parse_instance_search_expr("(alpha OR beta");
        assert_eq!(
            parsed,
            Some(InstanceSearchExpr::Term("(alpha OR beta".to_string()))
        );
    }

    #[test]
    fn action_timing_from_state_uses_state_timestamps_for_latest_attempt() {
        let started_at = Utc::now() - ChronoDuration::milliseconds(1500);
        let completed_at = started_at + ChronoDuration::milliseconds(450);
        let fallback = Utc::now();
        let node = ExecutionNode {
            node_id: Uuid::new_v4(),
            node_type: "action_call".to_string(),
            label: "@tests.action()".to_string(),
            status: NodeStatus::Completed,
            template_id: Some("n0".to_string()),
            targets: Vec::new(),
            action: Some(ActionCallSpec {
                action_name: "tests.action".to_string(),
                module_name: Some("tests".to_string()),
                kwargs: HashMap::new(),
            }),
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: 2,
            started_at: Some(started_at),
            completed_at: Some(completed_at),
            scheduled_at: None,
        };

        let (dispatched_at, finished_at, duration_ms) =
            action_timing_from_state(Some(&node), 2, fallback);
        assert_eq!(dispatched_at, Some(started_at.to_rfc3339()));
        assert_eq!(finished_at, Some(completed_at.to_rfc3339()));
        assert_eq!(duration_ms, Some(450));
    }

    #[test]
    fn action_timing_from_state_falls_back_for_prior_attempt_rows() {
        let started_at = Utc::now() - ChronoDuration::milliseconds(1200);
        let completed_at = started_at + ChronoDuration::milliseconds(600);
        let fallback = Utc::now();
        let node = ExecutionNode {
            node_id: Uuid::new_v4(),
            node_type: "action_call".to_string(),
            label: "@tests.action()".to_string(),
            status: NodeStatus::Completed,
            template_id: Some("n0".to_string()),
            targets: Vec::new(),
            action: Some(ActionCallSpec {
                action_name: "tests.action".to_string(),
                module_name: Some("tests".to_string()),
                kwargs: HashMap::new(),
            }),
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: 3,
            started_at: Some(started_at),
            completed_at: Some(completed_at),
            scheduled_at: None,
        };

        let (dispatched_at, finished_at, duration_ms) =
            action_timing_from_state(Some(&node), 2, fallback);
        assert_eq!(dispatched_at, Some(fallback.to_rfc3339()));
        assert_eq!(finished_at, Some(fallback.to_rfc3339()));
        assert_eq!(duration_ms, None);
    }

    fn sample_execution_node(execution_id: Uuid) -> ExecutionNode {
        ExecutionNode {
            node_id: execution_id,
            node_type: "action_call".to_string(),
            label: "@tests.action()".to_string(),
            status: NodeStatus::Queued,
            template_id: Some("n0".to_string()),
            targets: Vec::new(),
            action: Some(ActionCallSpec {
                action_name: "tests.action".to_string(),
                module_name: Some("tests".to_string()),
                kwargs: HashMap::from([(
                    "value".to_string(),
                    ValueExpr::Literal(LiteralValue {
                        value: serde_json::json!(7),
                    }),
                )]),
            }),
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: 1,
            started_at: None,
            completed_at: None,
            scheduled_at: Some(Utc::now()),
        }
    }

    fn sample_graph(instance_id: Uuid, execution_id: Uuid) -> GraphUpdate {
        let mut nodes = HashMap::new();
        nodes.insert(execution_id, sample_execution_node(execution_id));

        GraphUpdate {
            instance_id,
            nodes,
            edges: HashSet::from([ExecutionEdge {
                source: execution_id,
                target: execution_id,
                edge_type: EdgeType::StateMachine,
            }]),
        }
    }

    async fn insert_instance_with_graph_with_workflow(
        backend: &PostgresBackend,
        workflow_name: &str,
    ) -> (Uuid, Uuid, Uuid) {
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let workflow_version_id = insert_workflow_version(backend, workflow_name).await;
        let graph = sample_graph(instance_id, execution_id);
        let state_payload = rmp_serde::to_vec_named(&graph).expect("encode graph update");

        sqlx::query(
            "INSERT INTO runner_instances (instance_id, entry_node, workflow_version_id, state) VALUES ($1, $2, $3, $4)",
        )
        .bind(instance_id)
        .bind(entry_node)
        .bind(workflow_version_id)
        .bind(state_payload)
        .execute(backend.pool())
        .await
        .expect("insert runner instance");

        (instance_id, entry_node, execution_id)
    }

    async fn insert_instance_with_graph(backend: &PostgresBackend) -> (Uuid, Uuid, Uuid) {
        insert_instance_with_graph_with_workflow(backend, "tests.workflow").await
    }

    async fn insert_action_result(backend: &PostgresBackend, execution_id: Uuid) {
        let payload = rmp_serde::to_vec_named(&serde_json::json!({"ok": true}))
            .expect("encode action result");
        sqlx::query(
            "INSERT INTO runner_actions_done (execution_id, attempt, result) VALUES ($1, $2, $3)",
        )
        .bind(execution_id)
        .bind(1_i32)
        .bind(payload)
        .execute(backend.pool())
        .await
        .expect("insert action result");
    }

    fn sample_program_proto() -> Vec<u8> {
        let source = r#"
fn main(input: [x], output: [y]):
    y = @tests.action(value=x)
    return y
"#;
        let program = parse_program(source.trim()).expect("parse program");
        program.encode_to_vec()
    }

    fn loop_program_proto() -> Vec<u8> {
        let source = r#"
fn main(input: [items], output: [total]):
    total = 0
    for item in items:
        total = total + item
    return total
"#;
        let program = parse_program(source.trim()).expect("parse loop program");
        program.encode_to_vec()
    }

    async fn insert_workflow_version(backend: &PostgresBackend, workflow_name: &str) -> Uuid {
        WorkflowRegistryBackend::upsert_workflow_version(
            backend,
            &WorkflowRegistration {
                workflow_name: workflow_name.to_string(),
                workflow_version: "v1".to_string(),
                ir_hash: format!("hash-{workflow_name}"),
                program_proto: sample_program_proto(),
                concurrent: false,
            },
        )
        .await
        .expect("insert workflow version")
    }

    async fn insert_loop_workflow_version(backend: &PostgresBackend, workflow_name: &str) -> Uuid {
        WorkflowRegistryBackend::upsert_workflow_version(
            backend,
            &WorkflowRegistration {
                workflow_name: workflow_name.to_string(),
                workflow_version: "v1-loop".to_string(),
                ir_hash: format!("hash-loop-{workflow_name}"),
                program_proto: loop_program_proto(),
                concurrent: false,
            },
        )
        .await
        .expect("insert loop workflow version")
    }

    async fn insert_schedule(backend: &PostgresBackend, schedule_name: &str) -> Uuid {
        SchedulerBackend::upsert_schedule(
            backend,
            &CreateScheduleParams {
                workflow_name: "tests.workflow".to_string(),
                schedule_name: schedule_name.to_string(),
                schedule_type: ScheduleType::Interval,
                cron_expression: None,
                interval_seconds: Some(60),
                jitter_seconds: 0,
                input_payload: Some(
                    rmp_serde::to_vec_named(&serde_json::json!({"k": "v"}))
                        .expect("encode payload"),
                ),
                priority: 0,
                allow_duplicate: false,
            },
        )
        .await
        .expect("upsert schedule")
        .0
    }

    async fn insert_scheduled_instance(
        backend: &PostgresBackend,
        schedule_id: Uuid,
        created_at: DateTime<Utc>,
        with_result: bool,
    ) -> Uuid {
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let workflow_version_id = insert_workflow_version(backend, "tests.workflow").await;
        let graph = sample_graph(instance_id, execution_id);
        let state_payload = rmp_serde::to_vec_named(&graph).expect("encode graph update");
        let result_payload = if with_result {
            Some(
                rmp_serde::to_vec_named(&serde_json::json!({"result": {"ok": true}}))
                    .expect("encode result"),
            )
        } else {
            None
        };

        sqlx::query(
            "INSERT INTO runner_instances (instance_id, entry_node, workflow_version_id, schedule_id, created_at, state, result, error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(instance_id)
        .bind(entry_node)
        .bind(workflow_version_id)
        .bind(schedule_id)
        .bind(created_at)
        .bind(state_payload)
        .bind(result_payload)
        .bind(Option::<Vec<u8>>::None)
        .execute(backend.pool())
        .await
        .expect("insert scheduled instance");

        instance_id
    }

    async fn insert_worker_status(backend: &PostgresBackend, pool_id: Uuid) {
        WorkerStatusBackend::upsert_worker_status(
            backend,
            &WorkerStatusUpdate {
                pool_id,
                throughput_per_min: 180.0,
                total_completed: 20,
                last_action_at: Some(Utc::now()),
                median_dequeue_ms: Some(5),
                median_handling_ms: Some(12),
                dispatch_queue_size: 3,
                total_in_flight: 2,
                active_workers: 4,
                actions_per_sec: 3.0,
                median_instance_duration_secs: Some(0.2),
                active_instance_count: 1,
                total_instances_completed: 8,
                instances_per_sec: 0.5,
                instances_per_min: 30.0,
                time_series: None,
            },
        )
        .await
        .expect("upsert worker status");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_count_instances_happy_path() {
        let backend = setup_backend().await;
        insert_instance_with_graph(&backend).await;

        let count = WebappBackend::count_instances(&backend, None)
            .await
            .expect("count instances");
        assert_eq!(count, 1);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_count_instances_applies_search_expression() {
        let backend = setup_backend().await;
        let (alpha_id, _, _) =
            insert_instance_with_graph_with_workflow(&backend, "tests.alpha").await;
        let (beta_id, _, _) =
            insert_instance_with_graph_with_workflow(&backend, "tests.beta").await;
        assert_ne!(alpha_id, beta_id);

        let completed_payload =
            rmp_serde::to_vec_named(&serde_json::json!({"result": {"ok": true}}))
                .expect("encode completed payload");
        sqlx::query(
            "UPDATE runner_instances SET result = $2, current_status = $3 WHERE instance_id = $1",
        )
        .bind(beta_id)
        .bind(completed_payload)
        .bind("completed")
        .execute(backend.pool())
        .await
        .expect("mark beta completed");

        let alpha_count = WebappBackend::count_instances(&backend, Some("alpha"))
            .await
            .expect("count alpha");
        assert_eq!(alpha_count, 1);

        let completed_count = WebappBackend::count_instances(&backend, Some("completed"))
            .await
            .expect("count completed");
        assert_eq!(completed_count, 1);

        let combined = WebappBackend::count_instances(&backend, Some("(alpha OR completed)"))
            .await
            .expect("count combined");
        assert_eq!(combined, 2);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_list_instances_happy_path() {
        let backend = setup_backend().await;
        let (instance_id, _, _) = insert_instance_with_graph(&backend).await;

        let instances = WebappBackend::list_instances(&backend, None, 10, 0)
            .await
            .expect("list instances");

        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].id, instance_id);
        assert_eq!(instances[0].status, InstanceStatus::Running);
        assert_eq!(
            instances[0].workflow_name,
            Some("tests.workflow".to_string())
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_list_instances_applies_search_expression() {
        let backend = setup_backend().await;
        let (alpha_id, _, _) =
            insert_instance_with_graph_with_workflow(&backend, "tests.alpha").await;
        let _ = insert_instance_with_graph_with_workflow(&backend, "tests.beta").await;

        let alpha_instances = WebappBackend::list_instances(&backend, Some("alpha"), 10, 0)
            .await
            .expect("list alpha");
        assert_eq!(alpha_instances.len(), 1);
        assert_eq!(alpha_instances[0].id, alpha_id);

        let running_instances =
            WebappBackend::list_instances(&backend, Some("(alpha OR beta) AND running"), 10, 0)
                .await
                .expect("list running instances");
        assert_eq!(running_instances.len(), 2);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_instance_happy_path() {
        let backend = setup_backend().await;
        let (instance_id, _, _) = insert_instance_with_graph(&backend).await;

        let instance = WebappBackend::get_instance(&backend, instance_id)
            .await
            .expect("get instance");

        assert_eq!(instance.id, instance_id);
        assert_eq!(instance.status, InstanceStatus::Running);
        assert_eq!(instance.workflow_name, Some("tests.workflow".to_string()));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_workflow_name_prefers_registered_workflow_name() {
        let backend = setup_backend().await;
        let (instance_id, entry_node, execution_id) =
            insert_instance_with_graph_with_workflow(&backend, "tests.workflow_name").await;

        let list = WebappBackend::list_instances(&backend, None, 10, 0)
            .await
            .expect("list instances");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, instance_id);
        assert_eq!(
            list[0].workflow_name,
            Some("tests.workflow_name".to_string())
        );

        let detail = WebappBackend::get_instance(&backend, instance_id)
            .await
            .expect("get instance");
        assert_eq!(detail.id, instance_id);
        assert_eq!(detail.entry_node, entry_node);
        assert_eq!(
            detail.workflow_name,
            Some("tests.workflow_name".to_string())
        );

        let graph = WebappBackend::get_execution_graph(&backend, instance_id)
            .await
            .expect("get graph")
            .expect("graph");
        assert!(
            graph
                .nodes
                .iter()
                .any(|node| node.id == execution_id.to_string()),
            "expected action node to remain intact"
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_execution_graph_happy_path() {
        let backend = setup_backend().await;
        let (instance_id, _, execution_id) = insert_instance_with_graph(&backend).await;

        let graph = WebappBackend::get_execution_graph(&backend, instance_id)
            .await
            .expect("get execution graph")
            .expect("expected execution graph");

        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes[0].id, execution_id.to_string());
        assert_eq!(graph.nodes[0].action_name, Some("tests.action".to_string()));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_workflow_graph_uses_template_node_ids() {
        let backend = setup_backend().await;
        let (instance_id, _, execution_id) = insert_instance_with_graph(&backend).await;

        let graph = WebappBackend::get_workflow_graph(&backend, instance_id)
            .await
            .expect("get workflow graph")
            .expect("expected workflow graph");

        assert!(!graph.nodes.is_empty(), "workflow graph should have nodes");
        assert!(
            graph
                .nodes
                .iter()
                .all(|node| node.id != execution_id.to_string()),
            "workflow graph should use template node ids, not runtime execution ids"
        );
        assert!(
            graph
                .nodes
                .iter()
                .any(|node| node.node_type == "action_call"),
            "workflow graph should include action_call template nodes"
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_workflow_graph_marks_loop_back_edges() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let workflow_version_id =
            insert_loop_workflow_version(&backend, "tests.loop_workflow").await;
        let graph = sample_graph(instance_id, execution_id);
        let state_payload = rmp_serde::to_vec_named(&graph).expect("encode graph update");

        sqlx::query(
            "INSERT INTO runner_instances (instance_id, entry_node, workflow_version_id, state) VALUES ($1, $2, $3, $4)",
        )
        .bind(instance_id)
        .bind(entry_node)
        .bind(workflow_version_id)
        .bind(state_payload)
        .execute(backend.pool())
        .await
        .expect("insert loop runner instance");

        let workflow_graph = WebappBackend::get_workflow_graph(&backend, instance_id)
            .await
            .expect("get workflow graph")
            .expect("expected workflow graph");

        assert!(
            workflow_graph
                .edges
                .iter()
                .any(|edge| edge.edge_type == "state_machine_loop_back"),
            "loop workflows should emit at least one loop_back edge"
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_action_results_happy_path() {
        let backend = setup_backend().await;
        let (instance_id, _, execution_id) = insert_instance_with_graph(&backend).await;
        insert_action_result(&backend, execution_id).await;

        let entries = WebappBackend::get_action_results(&backend, instance_id)
            .await
            .expect("get action results");

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action_id, execution_id.to_string());
        assert_eq!(entries[0].action_name, "tests.action");
        assert_eq!(entries[0].status, "completed");
        assert!(entries[0].request_preview.contains("\"value\": 7"));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_distinct_workflows_happy_path() {
        let backend = setup_backend().await;
        insert_instance_with_graph_with_workflow(&backend, "tests.workflow_a").await;
        insert_instance_with_graph_with_workflow(&backend, "tests.workflow_b").await;

        let workflows = WebappBackend::get_distinct_workflows(&backend)
            .await
            .expect("get distinct workflows");
        assert_eq!(
            workflows,
            vec![
                "tests.workflow_a".to_string(),
                "tests.workflow_b".to_string()
            ]
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_distinct_statuses_happy_path() {
        let backend = setup_backend().await;

        let statuses = WebappBackend::get_distinct_statuses(&backend)
            .await
            .expect("get distinct statuses");
        assert_eq!(statuses, vec!["queued", "running", "completed", "failed"]);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_count_schedules_happy_path() {
        let backend = setup_backend().await;
        insert_schedule(&backend, "count").await;

        let count = WebappBackend::count_schedules(&backend)
            .await
            .expect("count schedules");
        assert_eq!(count, 1);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_list_schedules_happy_path() {
        let backend = setup_backend().await;
        let schedule_id = insert_schedule(&backend, "list").await;

        let schedules = WebappBackend::list_schedules(&backend, 10, 0)
            .await
            .expect("list schedules");
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].id, schedule_id.to_string());
        assert_eq!(schedules[0].schedule_name, "list");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_schedule_happy_path() {
        let backend = setup_backend().await;
        let schedule_id = insert_schedule(&backend, "detail").await;

        let schedule = WebappBackend::get_schedule(&backend, schedule_id)
            .await
            .expect("get schedule");
        assert_eq!(schedule.id, schedule_id.to_string());
        assert_eq!(schedule.schedule_name, "detail");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_schedule_invocations_are_filtered_by_schedule_id() {
        let backend = setup_backend().await;
        let schedule_id = insert_schedule(&backend, "invocations-a").await;
        let other_schedule_id = insert_schedule(&backend, "invocations-b").await;

        let running_instance_id = insert_scheduled_instance(
            &backend,
            schedule_id,
            Utc::now() - ChronoDuration::minutes(2),
            false,
        )
        .await;
        let completed_instance_id = insert_scheduled_instance(
            &backend,
            schedule_id,
            Utc::now() - ChronoDuration::minutes(1),
            true,
        )
        .await;
        let _other_instance_id =
            insert_scheduled_instance(&backend, other_schedule_id, Utc::now(), true).await;

        let total = WebappBackend::count_schedule_invocations(&backend, schedule_id)
            .await
            .expect("count schedule invocations");
        assert_eq!(total, 2);

        let invocations = WebappBackend::list_schedule_invocations(&backend, schedule_id, 10, 0)
            .await
            .expect("list schedule invocations");
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[0].id, completed_instance_id);
        assert_eq!(invocations[0].status, InstanceStatus::Completed);
        assert_eq!(invocations[1].id, running_instance_id);
        assert_eq!(invocations[1].status, InstanceStatus::Running);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_update_schedule_status_happy_path() {
        let backend = setup_backend().await;
        let schedule_id = insert_schedule(&backend, "update").await;

        let updated = WebappBackend::update_schedule_status(&backend, schedule_id, "paused")
            .await
            .expect("update schedule status");
        assert!(updated);

        let schedule = WebappBackend::get_schedule(&backend, schedule_id)
            .await
            .expect("get schedule");
        assert_eq!(schedule.status, "paused");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_distinct_schedule_statuses_happy_path() {
        let backend = setup_backend().await;

        let statuses = WebappBackend::get_distinct_schedule_statuses(&backend)
            .await
            .expect("get distinct schedule statuses");
        assert_eq!(statuses, vec!["active", "paused"]);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_distinct_schedule_types_happy_path() {
        let backend = setup_backend().await;

        let types = WebappBackend::get_distinct_schedule_types(&backend)
            .await
            .expect("get distinct schedule types");
        assert_eq!(types, vec!["cron", "interval"]);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_worker_action_stats_happy_path() {
        let backend = setup_backend().await;
        let pool_id = Uuid::new_v4();
        insert_worker_status(&backend, pool_id).await;

        let rows = WebappBackend::get_worker_action_stats(&backend, 60)
            .await
            .expect("get worker action stats");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pool_id, pool_id.to_string());
        assert_eq!(rows[0].total_completed, 20);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_worker_aggregate_stats_happy_path() {
        let backend = setup_backend().await;
        insert_worker_status(&backend, Uuid::new_v4()).await;

        let aggregate = WebappBackend::get_worker_aggregate_stats(&backend, 60)
            .await
            .expect("get worker aggregate stats");
        assert_eq!(aggregate.active_worker_count, 1);
        assert_eq!(aggregate.total_in_flight, 2);
        assert_eq!(aggregate.total_queue_depth, 3);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_worker_status_table_exists_happy_path() {
        let backend = setup_backend().await;

        assert!(WebappBackend::worker_status_table_exists(&backend).await);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_schedules_table_exists_happy_path() {
        let backend = setup_backend().await;

        assert!(WebappBackend::schedules_table_exists(&backend).await);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn webapp_get_worker_statuses_happy_path() {
        let backend = setup_backend().await;
        let pool_id = Uuid::new_v4();
        insert_worker_status(&backend, pool_id).await;
        let (completed_instance_id, _, _) = insert_instance_with_graph(&backend).await;
        let completed_payload =
            rmp_serde::to_vec_named(&serde_json::json!({"ok": true})).expect("encode result");
        sqlx::query("UPDATE runner_instances SET result = $2 WHERE instance_id = $1")
            .bind(completed_instance_id)
            .bind(completed_payload)
            .execute(backend.pool())
            .await
            .expect("mark instance completed");

        let (failed_instance_id, _, _) = insert_instance_with_graph(&backend).await;
        let error_payload = rmp_serde::to_vec_named(&serde_json::json!({
            "type": "Exception",
            "message": "boom",
        }))
        .expect("encode error");
        sqlx::query("UPDATE runner_instances SET error = $2 WHERE instance_id = $1")
            .bind(failed_instance_id)
            .bind(error_payload)
            .execute(backend.pool())
            .await
            .expect("mark instance failed");

        let statuses = WebappBackend::get_worker_statuses(&backend, 60)
            .await
            .expect("get worker statuses");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].pool_id, pool_id);
        assert_eq!(statuses[0].total_completed, 20);
        assert_eq!(statuses[0].total_instances_completed, 1);
        assert_eq!(statuses[0].total_in_flight, Some(2));
        assert_eq!(statuses[0].dispatch_queue_size, Some(3));
    }
}
