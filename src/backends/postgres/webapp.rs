use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;
use tonic::async_trait;
use uuid::Uuid;

use super::PostgresBackend;
use crate::backends::base::{BackendError, BackendResult, GraphUpdate, WebappBackend};
use crate::rappel_core::runner::state::{ActionCallSpec, ExecutionNode, NodeStatus};
use crate::rappel_core::runner::{RunnerState, ValueExpr, format_value, replay_action_kwargs};
use crate::webapp::{
    ExecutionEdgeView, ExecutionGraphView, ExecutionNodeView, InstanceDetail, InstanceStatus,
    InstanceSummary, ScheduleDetail, ScheduleSummary, TimelineEntry, WorkerActionRow,
    WorkerAggregateStats, WorkerStatus,
};

#[async_trait]
impl WebappBackend for PostgresBackend {
    async fn count_instances(&self, search: Option<&str>) -> BackendResult<i64> {
        let count = if let Some(_search) = search {
            // For now, simple search not implemented - would need to decode state.
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runner_instances")
                .fetch_one(&self.pool)
                .await?
        } else {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runner_instances")
                .fetch_one(&self.pool)
                .await?
        };
        Ok(count)
    }

    async fn list_instances(
        &self,
        _search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<InstanceSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT
                ri.instance_id,
                ri.entry_node,
                ri.created_at,
                ri.state,
                ri.result,
                ri.error,
                wv.workflow_name
            FROM runner_instances ri
            JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
            ORDER BY ri.created_at DESC, ri.instance_id DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut instances = Vec::new();
        for row in rows {
            let instance_id: Uuid = row.get("instance_id");
            let entry_node: Uuid = row.get("entry_node");
            let created_at: DateTime<Utc> = row.get("created_at");
            let state_bytes: Option<Vec<u8>> = row.get("state");
            let result_bytes: Option<Vec<u8>> = row.get("result");
            let error_bytes: Option<Vec<u8>> = row.get("error");
            let workflow_name: String = row.get("workflow_name");

            let status = determine_status(&state_bytes, &result_bytes, &error_bytes);
            let input_preview = extract_input_preview(&state_bytes);

            instances.push(InstanceSummary {
                id: instance_id,
                entry_node,
                created_at,
                status,
                workflow_name: Some(workflow_name),
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
                wv.workflow_name
            FROM runner_instances ri
            JOIN workflow_versions wv ON wv.id = ri.workflow_version_id
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
        let workflow_name: String = row.get("workflow_name");

        let status = determine_status(&state_bytes, &result_bytes, &error_bytes);
        let input_payload = format_input_payload(&state_bytes);
        let result_payload = format_instance_result_payload(status, &result_bytes, &error_bytes);
        let error_payload = format_error(&error_bytes);

        Ok(InstanceDetail {
            id: instance_id,
            entry_node,
            created_at,
            status,
            workflow_name: Some(workflow_name),
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
            SELECT created_at, execution_id, attempt, result
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
            let result_bytes: Option<Vec<u8>> = row.get("result");
            let result = result_bytes
                .as_deref()
                .map(decode_msgpack_json)
                .transpose()?;
            decoded_rows.push(DecodedActionResultRow {
                created_at,
                execution_id,
                attempt,
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
            let status = if error.is_some() {
                "failed".to_string()
            } else {
                "completed".to_string()
            };

            entries.push(TimelineEntry {
                action_id: row.execution_id.to_string(),
                action_name,
                module_name,
                status,
                attempt_number: row.attempt,
                dispatched_at: Some(row.created_at.to_rfc3339()),
                completed_at: Some(row.created_at.to_rfc3339()),
                duration_ms: None,
                request_preview,
                response_preview,
                error,
            });
        }

        Ok(entries)
    }

    async fn get_distinct_workflows(&self) -> BackendResult<Vec<String>> {
        Ok(Vec::new())
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use chrono::Utc;
    use serial_test::serial;
    use uuid::Uuid;

    use super::super::test_helpers::setup_backend;
    use super::*;
    use crate::backends::{
        SchedulerBackend, WebappBackend, WorkerStatusBackend, WorkerStatusUpdate,
        WorkflowRegistration, WorkflowRegistryBackend,
    };
    use crate::rappel_core::dag::EdgeType;
    use crate::rappel_core::runner::ValueExpr;
    use crate::rappel_core::runner::state::{
        ActionCallSpec, ExecutionEdge, ExecutionNode, LiteralValue, NodeStatus,
    };
    use crate::scheduler::{CreateScheduleParams, ScheduleType};

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

    async fn insert_workflow_version(backend: &PostgresBackend, workflow_name: &str) -> Uuid {
        WorkflowRegistryBackend::upsert_workflow_version(
            backend,
            &WorkflowRegistration {
                workflow_name: workflow_name.to_string(),
                workflow_version: "v1".to_string(),
                ir_hash: format!("hash-{workflow_name}"),
                program_proto: vec![1, 2, 3],
                concurrent: false,
            },
        )
        .await
        .expect("insert workflow version")
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

        let workflows = WebappBackend::get_distinct_workflows(&backend)
            .await
            .expect("get distinct workflows");
        assert!(workflows.is_empty());
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
