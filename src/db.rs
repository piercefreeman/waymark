use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use prost::Message;
use sqlx::{
    FromRow, PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};
use uuid::Uuid;

use crate::{
    LedgerActionId, WorkflowInstanceId, WorkflowVersionId,
    dag_state::{DagStateMachine, InstanceDagState},
    messages::proto::{
        self, WorkflowArguments, WorkflowDagDefinition, WorkflowDagNode, WorkflowNodeContext,
        WorkflowNodeDispatch,
    },
};

const DEFAULT_ACTION_TIMEOUT_SECS: i32 = 300;
const DEFAULT_ACTION_MAX_RETRIES: i32 = 1;
const DEFAULT_TIMEOUT_RETRY_LIMIT: i32 = i32::MAX;
const EXHAUSTED_EXCEPTION_TYPE: &str = "ExhaustedRetries";
const EXHAUSTED_EXCEPTION_MODULE: &str = "carabiner.exceptions";
const RETRY_KIND_FAILURE: &str = "failure";
const RETRY_KIND_TIMEOUT: &str = "timeout";

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

async fn store_instance_result_if_complete(
    tx: &mut Transaction<'_, Postgres>,
    instance: &WorkflowInstanceRow,
    dag: &WorkflowDagDefinition,
    context_values: &HashMap<String, Vec<(String, Vec<u8>)>>,
    completed_count: usize,
) -> Result<()> {
    if dag.nodes.len() != completed_count {
        return Ok(());
    }
    if dag.return_variable.is_empty() {
        return Ok(());
    }
    if instance.result_payload.is_some() {
        return Ok(());
    }
    let mut payload_opt: Option<Vec<u8>> = None;
    for entries in context_values.values() {
        for (variable, payload) in entries {
            if variable == &dag.return_variable {
                payload_opt = Some(payload.clone());
                break;
            }
        }
        if payload_opt.is_some() {
            break;
        }
    }
    let Some(payload) = payload_opt else {
        return Ok(());
    };
    sqlx::query("UPDATE workflow_instances SET result_payload = $2 WHERE id = $1")
        .bind(instance.id)
        .bind(payload)
        .execute(tx.as_mut())
        .await?;
    Ok(())
}

async fn load_instance_node_state(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
) -> Result<(HashSet<String>, HashSet<String>)> {
    let rows = sqlx::query(
        r#"
        SELECT workflow_node_id, status
        FROM daemon_action_ledger
        WHERE instance_id = $1
        "#,
    )
    .bind(instance_id)
    .map(|row: PgRow| (row.get::<Option<String>, _>(0), row.get::<String, _>(1)))
    .fetch_all(tx.as_mut())
    .await?;
    let mut known_nodes = HashSet::new();
    let mut completed_nodes = HashSet::new();
    for (node_id_opt, status) in rows {
        if let Some(node_id) = node_id_opt {
            if status.eq_ignore_ascii_case("completed") {
                completed_nodes.insert(node_id.clone());
            }
            known_nodes.insert(node_id);
        }
    }
    Ok((known_nodes, completed_nodes))
}

async fn build_variable_context(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
    dag_nodes: &HashMap<String, WorkflowDagNode>,
) -> Result<HashMap<String, Vec<(String, Vec<u8>)>>> {
    let rows = sqlx::query(
        r#"
        SELECT workflow_node_id, result_payload
        FROM daemon_action_ledger
        WHERE instance_id = $1 AND status = 'completed' AND workflow_node_id IS NOT NULL AND result_payload IS NOT NULL
        "#,
    )
    .bind(instance_id)
    .map(|row: PgRow| (row.get::<Option<String>, _>(0), row.get::<Option<Vec<u8>>, _>(1)))
    .fetch_all(tx.as_mut())
    .await?;
    let mut context: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
    for (node_id_opt, payload_opt) in rows {
        let Some(node_id) = node_id_opt else {
            continue;
        };
        let Some(payload) = payload_opt else {
            continue;
        };
        if let Some(node) = dag_nodes.get(&node_id) {
            let entries = context.entry(node_id.clone()).or_default();
            if node.produces.is_empty() {
                entries.push((String::new(), payload.clone()));
            } else {
                for variable in &node.produces {
                    entries.push((variable.clone(), payload.clone()));
                }
            }
        }
    }
    Ok(context)
}

fn build_workflow_node_dispatch(
    node: &WorkflowDagNode,
    workflow_input: &[u8],
    context: &HashMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<QueuedWorkflowNode> {
    let workflow_args = decode_arguments(workflow_input, "workflow input")?;
    let mut dispatch = WorkflowNodeDispatch {
        node: Some(node.clone()),
        workflow_input: Some(workflow_args),
        context: Vec::new(),
    };
    let mut required: Vec<&str> = node.depends_on.iter().map(|s| s.as_str()).collect();
    required.extend(node.wait_for_sync.iter().map(|s| s.as_str()));
    let mut seen: HashSet<&str> = HashSet::new();
    for dep in required {
        if !seen.insert(dep) {
            continue;
        }
        if let Some(entries) = context.get(dep) {
            for (variable, payload) in entries {
                let args = decode_arguments(payload, "context payload")?;
                dispatch.context.push(WorkflowNodeContext {
                    variable: variable.clone(),
                    payload: Some(args),
                    workflow_node_id: dep.to_string(),
                });
            }
        }
    }
    let module = if node.module.is_empty() {
        "workflow".to_string()
    } else {
        node.module.clone()
    };
    let function_name = if node.action.is_empty() {
        "action".to_string()
    } else {
        node.action.clone()
    };
    let timeout_seconds = node
        .timeout_seconds
        .unwrap_or(DEFAULT_ACTION_TIMEOUT_SECS as u32) as i32;
    let max_retries = node
        .max_retries
        .unwrap_or(DEFAULT_ACTION_MAX_RETRIES as u32) as i32;
    let timeout_retry_limit = node
        .timeout_retry_limit
        .unwrap_or(DEFAULT_TIMEOUT_RETRY_LIMIT as u32) as i32;

    Ok(QueuedWorkflowNode {
        module,
        function_name,
        dispatch,
        timeout_seconds,
        max_retries,
        timeout_retry_limit,
    })
}

fn decode_arguments(bytes: &[u8], label: &str) -> Result<WorkflowArguments> {
    if bytes.is_empty() {
        return Ok(WorkflowArguments {
            arguments: Vec::new(),
        });
    }
    WorkflowArguments::decode(bytes)
        .with_context(|| format!("failed to decode {label} workflow arguments"))
}

fn extract_error_message(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    let arguments = proto::WorkflowArguments::decode(payload).ok()?;
    for argument in arguments.arguments {
        if argument.key == "error"
            && let Some(value) = argument.value
            && let Some(kind) = value.kind
            && let proto::workflow_argument_value::Kind::Exception(exc) = kind
        {
            return Some(exc.message);
        }
    }
    None
}

fn encode_exception_payload(type_name: &str, module: &str, message: &str) -> Vec<u8> {
    let mut arguments = proto::WorkflowArguments {
        arguments: Vec::new(),
    };
    let error_value = proto::WorkflowArgumentValue {
        kind: Some(proto::workflow_argument_value::Kind::Exception(
            proto::WorkflowErrorValue {
                message: message.to_string(),
                module: module.to_string(),
                r#type: type_name.to_string(),
                traceback: String::new(),
            },
        )),
    };
    arguments.arguments.push(proto::WorkflowArgument {
        key: "error".to_string(),
        value: Some(error_value),
    });
    arguments.encode_to_vec()
}

fn encode_exhausted_retries_payload(
    action: &str,
    attempts: i32,
    last_error: Option<&str>,
) -> Vec<u8> {
    let message = match last_error {
        Some(reason) if !reason.is_empty() => {
            format!("action {action} exhausted retries after {attempts} attempts: {reason}")
        }
        _ => format!("action {action} exhausted retries after {attempts} attempts"),
    };
    encode_exception_payload(
        EXHAUSTED_EXCEPTION_TYPE,
        EXHAUSTED_EXCEPTION_MODULE,
        &message,
    )
}

#[derive(Debug, Clone)]
pub struct CompletionRecord {
    pub action_id: LedgerActionId,
    pub success: bool,
    pub delivery_id: u64,
    pub result_payload: Vec<u8>,
    pub dispatch_token: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct QueuedWorkflowNode {
    module: String,
    function_name: String,
    dispatch: proto::WorkflowNodeDispatch,
    timeout_seconds: i32,
    max_retries: i32,
    timeout_retry_limit: i32,
}

#[derive(Debug, Clone, FromRow)]
struct ExhaustedActionRow {
    action_id: LedgerActionId,
    instance_id: WorkflowInstanceId,
    workflow_node_id: Option<String>,
    function_name: String,
    attempt_number: i32,
    last_error: Option<String>,
    status: String,
    max_retries: i32,
    timeout_retry_limit: i32,
}

#[derive(Debug, Clone, FromRow)]
pub struct LedgerAction {
    pub id: LedgerActionId,
    pub instance_id: WorkflowInstanceId,
    pub partition_id: i32,
    pub action_seq: i32,
    pub module: String,
    pub function_name: String,
    pub dispatch_payload: Vec<u8>,
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub attempt_number: i32,
    pub delivery_token: Uuid,
    pub timeout_retry_limit: i32,
    pub retry_kind: String,
}

#[derive(Debug, Clone, FromRow)]
struct WorkflowInstanceRow {
    pub id: WorkflowInstanceId,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub next_action_seq: i32,
    pub input_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
struct WorkflowVersionRow {
    pub concurrent: bool,
    pub dag_proto: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WorkflowVersionSummary {
    pub id: WorkflowVersionId,
    pub workflow_name: String,
    pub dag_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WorkflowVersionDetail {
    pub id: WorkflowVersionId,
    pub workflow_name: String,
    pub dag_hash: String,
    pub concurrent: bool,
    pub created_at: DateTime<Utc>,
    pub dag: WorkflowDagDefinition,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceSummary {
    pub id: WorkflowInstanceId,
    pub created_at: DateTime<Utc>,
    pub completed_actions: i64,
    pub total_actions: i64,
    pub has_result: bool,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceDetail {
    pub instance: WorkflowInstanceRecord,
    pub actions: Vec<WorkflowInstanceActionDetail>,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceRecord {
    pub id: WorkflowInstanceId,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub workflow_name: String,
    pub created_at: DateTime<Utc>,
    pub input_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceActionDetail {
    pub action_seq: i32,
    pub workflow_node_id: Option<String>,
    pub status: String,
    pub success: Option<bool>,
    pub module: String,
    pub function_name: String,
    pub dispatch_payload: Vec<u8>,
    pub result_payload: Option<Vec<u8>>,
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run migrations")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn list_workflow_versions(&self) -> Result<Vec<WorkflowVersionSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_name, dag_hash, created_at
            FROM workflow_versions
            ORDER BY created_at DESC
            "#,
        )
        .map(|row: PgRow| WorkflowVersionSummary {
            id: row.get("id"),
            workflow_name: row.get("workflow_name"),
            dag_hash: row.get("dag_hash"),
            created_at: row.get("created_at"),
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn load_workflow_version(
        &self,
        version_id: WorkflowVersionId,
    ) -> Result<Option<WorkflowVersionDetail>> {
        let row = sqlx::query(
            r#"
            SELECT id, workflow_name, dag_hash, dag_proto, concurrent, created_at
            FROM workflow_versions
            WHERE id = $1
            "#,
        )
        .bind(version_id)
        .map(|row: PgRow| {
            (
                row.get::<WorkflowVersionId, _>("id"),
                row.get::<String, _>("workflow_name"),
                row.get::<String, _>("dag_hash"),
                row.get::<Vec<u8>, _>("dag_proto"),
                row.get::<bool, _>("concurrent"),
                row.get::<DateTime<Utc>, _>("created_at"),
            )
        })
        .fetch_optional(&self.pool)
        .await?;
        let Some((id, workflow_name, dag_hash, dag_proto, concurrent, created_at)) = row else {
            return Ok(None);
        };
        let dag = WorkflowDagDefinition::decode(dag_proto.as_slice())
            .context("failed to decode workflow dag")?;
        Ok(Some(WorkflowVersionDetail {
            id,
            workflow_name,
            dag_hash,
            concurrent,
            created_at,
            dag,
        }))
    }

    pub async fn recent_instances_for_version(
        &self,
        version_id: WorkflowVersionId,
        limit: i64,
    ) -> Result<Vec<WorkflowInstanceSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT wi.id,
                   wi.created_at,
                   wi.result_payload IS NOT NULL AS has_result,
                   COUNT(dal.id) AS total_actions,
                   COUNT(*) FILTER (WHERE dal.status = 'completed') AS completed_actions
            FROM workflow_instances wi
            LEFT JOIN daemon_action_ledger dal ON dal.instance_id = wi.id
            WHERE wi.workflow_version_id = $1
            GROUP BY wi.id
            ORDER BY wi.created_at DESC
            LIMIT $2
            "#,
        )
        .bind(version_id)
        .bind(limit.max(1))
        .map(|row: PgRow| WorkflowInstanceSummary {
            id: row.get("id"),
            created_at: row.get("created_at"),
            completed_actions: row.get::<i64, _>("completed_actions"),
            total_actions: row.get::<i64, _>("total_actions"),
            has_result: row.get::<bool, _>("has_result"),
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn load_instance_with_actions(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> Result<Option<WorkflowInstanceDetail>> {
        let row = sqlx::query(
            r#"
            SELECT id, workflow_name, workflow_version_id, created_at, input_payload, result_payload
            FROM workflow_instances
            WHERE id = $1
            "#,
        )
        .bind(instance_id)
        .map(|row: PgRow| WorkflowInstanceRecord {
            id: row.get("id"),
            workflow_name: row.get("workflow_name"),
            workflow_version_id: row.get("workflow_version_id"),
            created_at: row.get("created_at"),
            input_payload: row.get("input_payload"),
            result_payload: row.get("result_payload"),
        })
        .fetch_optional(&self.pool)
        .await?;
        let Some(instance) = row else {
            return Ok(None);
        };
        let actions = sqlx::query(
            r#"
            SELECT action_seq,
                   workflow_node_id,
                   status,
                   success,
                   module,
                   function_name,
                   dispatch_payload,
                   result_payload
            FROM daemon_action_ledger
            WHERE instance_id = $1
            ORDER BY action_seq
            "#,
        )
        .bind(instance_id)
        .map(|row: PgRow| WorkflowInstanceActionDetail {
            action_seq: row.get("action_seq"),
            workflow_node_id: row.get("workflow_node_id"),
            status: row.get("status"),
            success: row.get("success"),
            module: row.get("module"),
            function_name: row.get("function_name"),
            dispatch_payload: row.get("dispatch_payload"),
            result_payload: row.get("result_payload"),
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(Some(WorkflowInstanceDetail { instance, actions }))
    }

    pub async fn reset_workflow_state(&self) -> Result<()> {
        let span = tracing::info_span!("db.reset_workflow_state");
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM daemon_action_ledger")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM workflow_instances")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn promote_exhausted_actions_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        action_ids: &[LedgerActionId],
    ) -> Result<Vec<(WorkflowInstanceId, Option<String>)>> {
        if action_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, ExhaustedActionRow>(
            r#"
            SELECT
                id AS action_id,
                instance_id,
                workflow_node_id,
                function_name,
                attempt_number,
                last_error
            FROM daemon_action_ledger
            WHERE id = ANY($1)
              AND status IN ('failed', 'timed_out')
              AND attempt_number + 1 >= max_retries
            "#,
        )
        .bind(action_ids)
        .fetch_all(tx.as_mut())
        .await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut scheduled: Vec<(WorkflowInstanceId, Option<String>)> = Vec::new();
        for row in rows {
            let attempts = row.attempt_number + 1;
            let payload = encode_exhausted_retries_payload(
                &row.function_name,
                attempts,
                row.last_error.as_deref(),
            );
            sqlx::query(
                r#"
                UPDATE daemon_action_ledger
                SET status = 'completed',
                    success = false,
                    completed_at = NOW(),
                    acked_at = COALESCE(acked_at, NOW()),
                    result_payload = $2,
                    deadline_at = NULL,
                    delivery_token = NULL
                WHERE id = $1
                "#,
            )
            .bind(row.action_id)
            .bind(&payload)
            .execute(tx.as_mut())
            .await?;
            scheduled.push((row.instance_id, row.workflow_node_id));
        }
        Ok(scheduled)
    }

    async fn requeue_failed_actions_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        action_ids: &[LedgerActionId],
    ) -> Result<usize> {
        if action_ids.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query(
            r#"
            UPDATE daemon_action_ledger
            SET status = 'queued',
                attempt_number = attempt_number + 1,
                dispatched_at = NULL,
                acked_at = NULL,
                deadline_at = NULL,
                delivery_id = NULL,
                scheduled_at = NOW(),
                result_payload = NULL,
                success = NULL,
                delivery_token = NULL
            WHERE id = ANY($1)
              AND status IN ('failed', 'timed_out')
              AND attempt_number + 1 < max_retries
            "#,
        )
        .bind(action_ids)
        .execute(tx.as_mut())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    pub async fn seed_actions(
        &self,
        action_count: usize,
        module: &str,
        function_name: &str,
        dispatch_payload: &[u8],
    ) -> Result<()> {
        let span = tracing::info_span!("db.seed_actions", action_count);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        let instance_id: WorkflowInstanceId = sqlx::query_scalar(
            r#"
            INSERT INTO workflow_instances (partition_id, workflow_name, next_action_seq)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(0i32)
        .bind("benchmark")
        .bind(action_count as i32)
        .fetch_one(&mut *tx)
        .await?;

        for seq in 0..action_count {
            sqlx::query(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id,
                    partition_id,
                    action_seq,
                    status,
                    module,
                    function_name,
                    dispatch_payload,
                    timeout_seconds,
                    max_retries,
                    attempt_number,
                    scheduled_at
                ) VALUES ($1, $2, $3, 'queued', $4, $5, $6, $7, $8, 0, NOW())
                "#,
            )
            .bind(instance_id)
            .bind(0i32)
            .bind(seq as i32)
            .bind(module)
            .bind(function_name)
            .bind(dispatch_payload)
            .bind(DEFAULT_ACTION_TIMEOUT_SECS)
            .bind(DEFAULT_ACTION_MAX_RETRIES)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_workflow_instance(
        &self,
        workflow_name: &str,
        workflow_version_id: WorkflowVersionId,
        input_payload: Option<&[u8]>,
    ) -> Result<WorkflowInstanceId> {
        let mut tx = self.pool.begin().await?;
        let instance_id = sqlx::query_scalar::<_, WorkflowInstanceId>(
            r#"
            INSERT INTO workflow_instances (
                partition_id,
                workflow_name,
                workflow_version_id,
                input_payload
            )
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(0i32)
        .bind(workflow_name)
        .bind(workflow_version_id)
        .bind(input_payload)
        .fetch_one(&mut *tx)
        .await?;
        self.schedule_workflow_instance_tx(&mut tx, instance_id)
            .await?;
        tx.commit().await?;
        Ok(instance_id)
    }

    pub async fn schedule_workflow_instance(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let count = self
            .schedule_workflow_instance_tx(&mut tx, instance_id)
            .await?;
        tx.commit().await?;
        Ok(count)
    }

    pub async fn dispatch_actions(&self, limit: i64) -> Result<Vec<LedgerAction>> {
        let span = tracing::info_span!("db.dispatch_actions", limit);
        let _guard = span.enter();
        let result = sqlx::query_as::<_, LedgerAction>(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM daemon_action_ledger
                WHERE status = 'queued'
                  AND scheduled_at <= NOW()
                  AND attempt_number < max_retries
                ORDER BY scheduled_at, action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'dispatched',
                dispatched_at = NOW(),
                deadline_at = CASE
                    WHEN dal.timeout_seconds > 0 THEN NOW() + dal.timeout_seconds * INTERVAL '1 second'
                    ELSE NULL
                END,
                delivery_token = gen_random_uuid()
            FROM next_actions
            WHERE dal.id = next_actions.id
            RETURNING dal.id,
                     dal.instance_id,
                     dal.partition_id,
                     dal.action_seq,
                     dal.module,
                     dal.function_name,
                     dal.dispatch_payload,
                     dal.timeout_seconds,
                     dal.max_retries,
                     dal.attempt_number,
                     dal.delivery_token
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        match result {
            Ok(records) => {
                if !records.is_empty() {
                    metrics::counter!("carabiner_actions_dispatched_total")
                        .increment(records.len() as u64);
                }
                Ok(records)
            }
            Err(err) => {
                metrics::counter!("carabiner_dispatch_errors_total").increment(1);
                Err(err.into())
            }
        }
    }

    pub async fn requeue_action(&self, action_id: LedgerActionId) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE daemon_action_ledger
            SET status = 'queued',
                dispatched_at = NULL,
                acked_at = NULL,
                delivery_id = NULL,
                deadline_at = NULL,
                scheduled_at = NOW(),
                result_payload = NULL,
                success = NULL,
                delivery_token = NULL
            WHERE id = $1
            "#,
        )
        .bind(action_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_timed_out_actions(&self, limit: i64) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let timed_out_ids: Vec<LedgerActionId> = sqlx::query_scalar(
            r#"
            WITH overdue AS (
                SELECT id
                FROM daemon_action_ledger
                WHERE status = 'dispatched'
                  AND deadline_at IS NOT NULL
                  AND deadline_at < NOW()
                ORDER BY deadline_at
                FOR UPDATE SKIP LOCKED
                LIMIT $1
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'timed_out',
                last_error = COALESCE(dal.last_error, 'action timed out'),
                deadline_at = NULL
            FROM overdue
            WHERE dal.id = overdue.id
            RETURNING dal.id
            "#,
        )
        .bind(limit.max(1))
        .fetch_all(&mut *tx)
        .await?;
        if timed_out_ids.is_empty() {
            tx.commit().await?;
            return Ok(0);
        }
        let exhausted = self
            .promote_exhausted_actions_tx(&mut tx, &timed_out_ids)
            .await?;
        let requeued = self
            .requeue_failed_actions_tx(&mut tx, &timed_out_ids)
            .await?;
        if requeued > 0 {
            tracing::debug!(count = requeued, "requeued timed out actions");
        }
        let mut workflow_instances: HashSet<WorkflowInstanceId> = HashSet::new();
        for (instance_id, node_id) in exhausted {
            if node_id.is_some() {
                workflow_instances.insert(instance_id);
            }
        }
        for instance_id in workflow_instances {
            let inserted = self
                .schedule_workflow_instance_tx(&mut tx, instance_id)
                .await?;
            if inserted > 0 {
                tracing::debug!(
                    instance_id = %instance_id,
                    inserted,
                    "workflow actions unlocked after exhausting retries"
                );
            }
        }
        tx.commit().await?;
        Ok(timed_out_ids.len())
    }

    pub async fn mark_actions_batch(&self, records: &[CompletionRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let span = tracing::debug_span!("db.mark_actions_batch", count = records.len());
        let _guard = span.enter();
        let successes_with_tokens: Vec<(&CompletionRecord, Uuid)> = records
            .iter()
            .filter(|r| r.success)
            .filter_map(|record| record.dispatch_token.map(|token| (record, token)))
            .collect();
        let dropped_successes =
            records.iter().filter(|r| r.success).count() - successes_with_tokens.len();
        if dropped_successes > 0 {
            tracing::debug!(
                count = dropped_successes,
                "dropping successful completions missing dispatch tokens"
            );
        }
        let failures_with_tokens: Vec<(&CompletionRecord, Uuid)> = records
            .iter()
            .filter(|r| !r.success)
            .filter_map(|record| record.dispatch_token.map(|token| (record, token)))
            .collect();
        let dropped_failures =
            records.iter().filter(|r| !r.success).count() - failures_with_tokens.len();
        if dropped_failures > 0 {
            tracing::debug!(
                count = dropped_failures,
                "dropping failed completions missing dispatch tokens"
            );
        }
        let mut tx = self.pool.begin().await?;
        let mut workflow_instances: HashSet<WorkflowInstanceId> = HashSet::new();
        if !successes_with_tokens.is_empty() {
            let ids: Vec<LedgerActionId> = successes_with_tokens
                .iter()
                .map(|(record, _)| record.action_id)
                .collect();
            let deliveries: Vec<i64> = successes_with_tokens
                .iter()
                .map(|(record, _)| record.delivery_id as i64)
                .collect();
            let payloads: Vec<Vec<u8>> = successes_with_tokens
                .iter()
                .map(|(record, _)| record.result_payload.clone())
                .collect();
            let tokens: Vec<Uuid> = successes_with_tokens
                .iter()
                .map(|(_, token)| *token)
                .collect();
            let updated = sqlx::query(
                r#"
                WITH data AS (
                    SELECT *
                    FROM UNNEST($1::UUID[], $2::UUID[], $3::BIGINT[], $4::BYTEA[])
                        AS t(action_id, dispatch_token, delivery_id, result_payload)
                )
                UPDATE daemon_action_ledger AS dal
                SET status = 'completed',
                    success = true,
                    completed_at = NOW(),
                    acked_at = COALESCE(dal.acked_at, NOW()),
                    delivery_id = data.delivery_id,
                    result_payload = data.result_payload,
                    deadline_at = NULL,
                    delivery_token = NULL
                FROM data
                WHERE dal.id = data.action_id
                  AND dal.delivery_token = data.dispatch_token
                RETURNING dal.instance_id, dal.workflow_node_id
                "#,
            )
            .bind(&ids)
            .bind(&tokens)
            .bind(&deliveries)
            .bind(&payloads)
            .map(|row: PgRow| {
                (
                    row.get::<WorkflowInstanceId, _>(0),
                    row.get::<Option<String>, _>(1),
                )
            })
            .fetch_all(&mut *tx)
            .await?;
            for row in updated {
                if row.1.is_some() {
                    workflow_instances.insert(row.0);
                }
            }
        }

        if !failures_with_tokens.is_empty() {
            let failure_ids: Vec<LedgerActionId> = failures_with_tokens
                .iter()
                .map(|(record, _)| record.action_id)
                .collect();
            let deliveries: Vec<i64> = failures_with_tokens
                .iter()
                .map(|(record, _)| record.delivery_id as i64)
                .collect();
            let payloads: Vec<Vec<u8>> = failures_with_tokens
                .iter()
                .map(|(record, _)| record.result_payload.clone())
                .collect();
            let errors: Vec<String> = failures_with_tokens
                .iter()
                .map(|(record, _)| {
                    extract_error_message(&record.result_payload)
                        .unwrap_or_else(|| "action failed".to_string())
                })
                .collect();
            let tokens: Vec<Uuid> = failures_with_tokens
                .iter()
                .map(|(_, token)| *token)
                .collect();
            sqlx::query(
                r#"
                WITH data AS (
                    SELECT *
                    FROM UNNEST($1::UUID[], $2::UUID[], $3::BIGINT[], $4::BYTEA[], $5::TEXT[])
                        AS t(action_id, dispatch_token, delivery_id, result_payload, last_error)
                )
                UPDATE daemon_action_ledger AS dal
                SET status = 'failed',
                    success = false,
                    acked_at = COALESCE(dal.acked_at, NOW()),
                    delivery_id = data.delivery_id,
                    result_payload = data.result_payload,
                    last_error = NULLIF(data.last_error, ''),
                    deadline_at = NULL,
                    delivery_token = NULL
                FROM data
                WHERE dal.id = data.action_id
                  AND dal.delivery_token = data.dispatch_token
                "#,
            )
            .bind(&failure_ids)
            .bind(&tokens)
            .bind(&deliveries)
            .bind(&payloads)
            .bind(&errors)
            .execute(&mut *tx)
            .await?;
            let exhausted = self
                .promote_exhausted_actions_tx(&mut tx, &failure_ids)
                .await?;
            for (instance_id, node_id) in exhausted {
                if node_id.is_some() {
                    workflow_instances.insert(instance_id);
                }
            }
            let requeued = self
                .requeue_failed_actions_tx(&mut tx, &failure_ids)
                .await?;
            if requeued > 0 {
                tracing::debug!(count = requeued, "requeued failed actions");
            }
        }

        for instance_id in workflow_instances {
            let inserted = self
                .schedule_workflow_instance_tx(&mut tx, instance_id)
                .await?;
            if inserted > 0 {
                tracing::debug!(
                    instance_id = %instance_id,
                    inserted,
                    "workflow actions unlocked after completion"
                );
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_workflow_version(
        &self,
        workflow_name: &str,
        dag_hash: &str,
        dag_bytes: &[u8],
        concurrent: bool,
    ) -> Result<WorkflowVersionId> {
        let mut tx = self.pool.begin().await?;
        if let Some(existing_id) = sqlx::query_scalar::<_, WorkflowVersionId>(
            "SELECT id FROM workflow_versions WHERE workflow_name = $1 AND dag_hash = $2",
        )
        .bind(workflow_name)
        .bind(dag_hash)
        .fetch_optional(tx.as_mut())
        .await?
        {
            tx.commit().await?;
            return Ok(existing_id);
        }

        let id = sqlx::query_scalar::<_, WorkflowVersionId>(
            r#"
            INSERT INTO workflow_versions (workflow_name, dag_hash, dag_proto, concurrent)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(dag_hash)
        .bind(dag_bytes)
        .bind(concurrent)
        .fetch_one(tx.as_mut())
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn schedule_workflow_instance_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instance_id: WorkflowInstanceId,
    ) -> Result<usize> {
        let instance = sqlx::query_as::<_, WorkflowInstanceRow>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id, next_action_seq, input_payload, result_payload
            FROM workflow_instances
            WHERE id = $1
            FOR UPDATE
            "#,
        )
        .bind(instance_id)
        .fetch_optional(tx.as_mut())
        .await?;
        let Some(instance) = instance else {
            tracing::warn!(instance_id = %instance_id, "workflow instance not found during scheduling");
            return Ok(0);
        };
        let Some(version_id) = instance.workflow_version_id else {
            tracing::debug!(
                instance_id = %instance.id,
                "workflow instance missing version; skipping scheduling"
            );
            return Ok(0);
        };
        let version = sqlx::query_as::<_, WorkflowVersionRow>(
            r#"
            SELECT concurrent, dag_proto
            FROM workflow_versions
            WHERE id = $1
            "#,
        )
        .bind(version_id)
        .fetch_one(tx.as_mut())
        .await?;
        let dag = WorkflowDagDefinition::decode(version.dag_proto.as_slice())
            .context("failed to decode workflow dag for scheduling")?;
        let mut node_map = HashMap::new();
        for node in &dag.nodes {
            node_map.insert(node.id.clone(), node.clone());
        }
        let (known_nodes, completed_nodes) = load_instance_node_state(tx, instance.id).await?;
        let dag_state = InstanceDagState::new(known_nodes, completed_nodes);
        let dag_machine = DagStateMachine::new(&dag, version.concurrent);
        let context_values = build_variable_context(tx, instance.id, &node_map).await?;
        let completed_count = dag_state.completed().len();
        let unlocked = dag_machine.ready_nodes(&dag_state);
        if unlocked.is_empty() {
            store_instance_result_if_complete(
                tx,
                &instance,
                &dag,
                &context_values,
                completed_count,
            )
            .await?;
            return Ok(0);
        }
        let workflow_input = instance.input_payload.clone().unwrap_or_default();
        let mut next_sequence = instance.next_action_seq;
        let mut inserted = 0usize;
        for node in unlocked {
            let node_id = node.id.clone();
            let node_dispatch =
                build_workflow_node_dispatch(&node, &workflow_input, &context_values)?;
            let dispatch_bytes = node_dispatch.dispatch.encode_to_vec();
            let _action_id: LedgerActionId = sqlx::query_scalar::<_, LedgerActionId>(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id,
                    partition_id,
                    action_seq,
                    status,
                    module,
                    function_name,
                    dispatch_payload,
                    workflow_node_id,
                    timeout_seconds,
                    max_retries,
                    attempt_number,
                    scheduled_at
                ) VALUES ($1, $2, $3, 'queued', $4, $5, $6, $7, $8, $9, 0, NOW())
                RETURNING id
                "#,
            )
            .bind(instance.id)
            .bind(instance.partition_id)
            .bind(next_sequence)
            .bind(&node_dispatch.module)
            .bind(&node_dispatch.function_name)
            .bind(&dispatch_bytes)
            .bind(node_id)
            .bind(node_dispatch.timeout_seconds)
            .bind(node_dispatch.max_retries)
            .fetch_one(tx.as_mut())
            .await?;
            inserted += 1;
            next_sequence += 1;
        }
        if next_sequence != instance.next_action_seq {
            sqlx::query("UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1")
                .bind(instance.id)
                .bind(next_sequence)
                .execute(tx.as_mut())
                .await?;
        }
        tracing::debug!(
            instance_id = %instance.id,
            workflow = instance.workflow_name,
            inserted,
            "queued workflow actions"
        );
        Ok(inserted)
    }
}
