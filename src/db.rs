use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::Value;
use sqlx::{
    FromRow, PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};
use uuid::Uuid;

use crate::{
    LedgerActionId, WorkflowInstanceId, WorkflowVersionId,
    ast_eval::{EvalContext, eval_expr, eval_stmt},
    context::{
        DecodedPayload, argument_value_to_json, arguments_to_json, decode_payload,
        values_to_arguments,
    },
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
const EXHAUSTED_EXCEPTION_MODULE: &str = "rappel.exceptions";
const LOOP_INDEX_VAR: &str = "__loop_index";

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

#[derive(Debug, Clone)]
struct NodeContextEntry {
    variable: String,
    payload: proto::WorkflowArguments,
}

#[derive(Debug, Clone, Default)]
struct SchedulingContext {
    eval_ctx: EvalContext,
    payloads: HashMap<String, Vec<NodeContextEntry>>,
    exceptions: HashMap<String, Value>,
}

fn encode_value_result(value: &Value) -> Result<WorkflowArguments> {
    let mut map = HashMap::new();
    map.insert("result".to_string(), value.clone());
    values_to_arguments(&map)
}

fn is_truthy_value(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(f) = n.as_f64() {
                f != 0.0
            } else {
                true
            }
        }
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn variable_map_from_value(value: &Value) -> Option<HashMap<String, Value>> {
    if let Value::Object(map) = value {
        if let Some(Value::Object(data)) = map.get("data")
            && let Some(Value::Object(vars)) = data.get("variables")
        {
            let mut out = HashMap::new();
            for (k, v) in vars {
                out.insert(k.clone(), v.clone());
            }
            return Some(out);
        }
        let mut out = HashMap::new();
        for (k, v) in map {
            out.insert(k.clone(), v.clone());
        }
        return Some(out);
    }
    None
}

fn extract_variables_from_result(
    node: &WorkflowDagNode,
    decoded: &DecodedPayload,
) -> HashMap<String, Value> {
    let mut variables: HashMap<String, Value> = HashMap::new();
    let Some(result) = decoded.result.as_ref() else {
        return variables;
    };
    if node.produces.len() == 1 {
        let name = &node.produces[0];
        if let Some(mapped) = variable_map_from_value(result).and_then(|m| m.get(name).cloned()) {
            variables.insert(name.clone(), mapped);
            return variables;
        }
        if let Value::Object(map) = result
            && let Some(inner) = map.get(name)
        {
            variables.insert(name.clone(), inner.clone());
            return variables;
        }
        variables.insert(name.clone(), result.clone());
        return variables;
    }
    if node.produces.is_empty() {
        return variables;
    }
    if let Some(nested) = variable_map_from_value(result) {
        for name in &node.produces {
            if let Some(value) = nested.get(name) {
                variables.insert(name.clone(), value.clone());
            }
        }
    }
    variables
}

fn guard_allows(node: &WorkflowDagNode, ctx: &EvalContext) -> Result<bool> {
    let Some(ast) = node.ast.as_ref() else {
        return Ok(true);
    };
    let Some(guard_expr) = ast.guard.as_ref() else {
        return Ok(true);
    };
    let value = eval_expr(guard_expr, ctx)?;
    Ok(is_truthy_value(&value))
}

/// Evaluate the sleep duration expression and return the number of seconds.
fn evaluate_sleep_duration(node: &WorkflowDagNode, ctx: &EvalContext) -> Result<Option<f64>> {
    let Some(ast) = node.ast.as_ref() else {
        return Ok(None);
    };
    let Some(sleep_expr) = ast.sleep_duration.as_ref() else {
        return Ok(None);
    };
    let value = eval_expr(sleep_expr, ctx)?;
    match value {
        Value::Number(n) => {
            let secs = n.as_f64().context("sleep duration must be a number")?;
            if secs < 0.0 {
                anyhow::bail!("sleep duration must be non-negative, got {}", secs);
            }
            Ok(Some(secs))
        }
        _ => anyhow::bail!("sleep duration must be a number, got {:?}", value),
    }
}

fn exception_matches(edge: &proto::WorkflowExceptionEdge, error: &Value) -> bool {
    let Value::Object(map) = error else {
        return false;
    };
    if !edge.exception_type.is_empty()
        && map
            .get("type")
            .and_then(|v| v.as_str())
            .map(|v| v != edge.exception_type)
            .unwrap_or(true)
    {
        return false;
    }
    if !edge.exception_module.is_empty()
        && map
            .get("module")
            .and_then(|v| v.as_str())
            .map(|v| v != edge.exception_module)
            .unwrap_or(true)
    {
        return false;
    }
    true
}

fn validate_exception_context(
    node: &WorkflowDagNode,
    exceptions: &HashMap<String, Value>,
) -> Result<()> {
    if exceptions.is_empty() {
        return Ok(());
    }
    let mut matched: HashSet<String> = HashSet::new();
    for edge in &node.exception_edges {
        if let Some(error) = exceptions.get(&edge.source_node_id)
            && exception_matches(edge, error)
        {
            matched.insert(edge.source_node_id.clone());
        }
    }
    for key in exceptions.keys() {
        if !matched.contains(key) {
            return Err(anyhow!("dependency {key} failed"));
        }
    }
    Ok(())
}

fn evaluate_kwargs(node: &WorkflowDagNode, ctx: &EvalContext) -> Result<HashMap<String, Value>> {
    let Some(ast) = node.ast.as_ref() else {
        return Ok(HashMap::new());
    };
    let mut evaluated: HashMap<String, Value> = HashMap::new();
    for (key, expr) in &ast.kwargs {
        let value = eval_expr(expr, ctx)?;
        evaluated.insert(key.clone(), value);
    }
    Ok(evaluated)
}

/// Compute scheduled_at for a node. Sleep nodes are scheduled in the future.
fn compute_scheduled_at(node: &WorkflowDagNode, eval_ctx: &EvalContext) -> Result<DateTime<Utc>> {
    if node.action == "sleep"
        && let Some(secs) = evaluate_sleep_duration(node, eval_ctx)?
    {
        let scheduled_at = Utc::now() + chrono::Duration::milliseconds((secs * 1000.0) as i64);
        return Ok(scheduled_at);
    }
    Ok(Utc::now())
}

fn build_dependency_contexts(
    node: &WorkflowDagNode,
    payloads: &HashMap<String, Vec<NodeContextEntry>>,
) -> Vec<WorkflowNodeContext> {
    let mut required: Vec<&str> = node.depends_on.iter().map(|s| s.as_str()).collect();
    required.extend(node.wait_for_sync.iter().map(|s| s.as_str()));
    let mut seen: HashSet<&str> = HashSet::new();
    let mut contexts = Vec::new();
    for dep in required {
        if !seen.insert(dep) {
            continue;
        }
        if let Some(entries) = payloads.get(dep) {
            for entry in entries {
                contexts.push(WorkflowNodeContext {
                    variable: entry.variable.clone(),
                    payload: Some(entry.payload.clone()),
                    workflow_node_id: dep.to_string(),
                });
            }
        }
    }
    contexts
}

async fn build_scheduling_context(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
    dag_nodes: &HashMap<String, WorkflowDagNode>,
    workflow_input: &WorkflowArguments,
) -> Result<SchedulingContext> {
    let rows = sqlx::query(
        r#"
        SELECT workflow_node_id, result_payload, success
        FROM daemon_action_ledger
        WHERE instance_id = $1 AND status = 'completed' AND workflow_node_id IS NOT NULL
        "#,
    )
    .bind(instance_id)
    .map(|row: PgRow| {
        (
            row.get::<Option<String>, _>(0),
            row.get::<Option<Vec<u8>>, _>(1),
            row.get::<Option<bool>, _>(2),
        )
    })
    .fetch_all(tx.as_mut())
    .await?;
    let mut eval_ctx: EvalContext = arguments_to_json(workflow_input)?;
    let mut payloads: HashMap<String, Vec<NodeContextEntry>> = HashMap::new();
    let mut exceptions: HashMap<String, Value> = HashMap::new();
    for (node_id_opt, payload_opt, success_opt) in rows {
        let Some(node_id) = node_id_opt else {
            continue;
        };
        let Some(payload_bytes) = payload_opt else {
            continue;
        };
        let Some(success) = success_opt else {
            continue;
        };
        let Some(node) = dag_nodes.get(&node_id) else {
            continue;
        };
        let arguments = decode_arguments(&payload_bytes, "context payload")?;
        let decoded = decode_payload(&arguments)?;
        if success {
            let vars = extract_variables_from_result(node, &decoded);
            for (name, value) in vars {
                eval_ctx.insert(name, value);
            }
        } else if let Some(error) = decoded.error.as_ref() {
            exceptions.insert(node_id.clone(), error.clone());
        }
        let produced_vars = if node.produces.is_empty() {
            vec![String::new()]
        } else {
            node.produces.clone()
        };
        let entries = payloads.entry(node_id).or_default();
        for variable in produced_vars {
            entries.push(NodeContextEntry {
                variable,
                payload: arguments.clone(),
            });
        }
    }
    eval_ctx.insert(
        "__workflow_exceptions".to_string(),
        Value::Object(
            exceptions
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
    );
    Ok(SchedulingContext {
        eval_ctx,
        payloads,
        exceptions,
    })
}

#[derive(Debug, Clone)]
struct LoopEvaluation {
    kwargs: HashMap<String, Value>,
    accumulator: Vec<Value>,
    current_index: usize,
}

fn extract_loop_index(ctx: &EvalContext) -> usize {
    let Some(value) = ctx.get(LOOP_INDEX_VAR) else {
        return 0;
    };
    match value {
        Value::Number(num) => num.as_u64().unwrap_or(0) as usize,
        _ => 0,
    }
}

fn extract_accumulator(ctx: &EvalContext, name: &str) -> Vec<Value> {
    let Some(value) = ctx.get(name) else {
        return Vec::new();
    };
    match value {
        Value::Array(items) => items.clone(),
        Value::String(text) => text.chars().map(|c| Value::String(c.to_string())).collect(),
        Value::Null => Vec::new(),
        other => vec![other.clone()],
    }
}

fn evaluate_loop_iteration(
    node: &WorkflowDagNode,
    ctx: &EvalContext,
) -> Result<Option<LoopEvaluation>> {
    let Some(ast) = node.ast.as_ref() else {
        return Ok(None);
    };
    let Some(loop_ast) = ast.r#loop.as_ref() else {
        return Ok(None);
    };
    let iterable = loop_ast
        .iterable
        .as_ref()
        .context("loop missing iterable expression")?;
    let iterable_value = eval_expr(iterable, ctx)?;
    let Value::Array(items) = iterable_value else {
        return Err(anyhow!("loop iterable must resolve to a list"));
    };
    let loop_index = extract_loop_index(ctx);
    if loop_index >= items.len() {
        return Ok(None);
    }
    let accumulator = extract_accumulator(ctx, &loop_ast.accumulator);
    let mut local_ctx = ctx.clone();
    local_ctx.insert(loop_ast.loop_var.clone(), items[loop_index].clone());
    local_ctx.insert(
        loop_ast.accumulator.clone(),
        Value::Array(accumulator.clone()),
    );
    for stmt in &loop_ast.preamble {
        eval_stmt(stmt, &mut local_ctx)?;
    }
    let Some(body_action) = loop_ast.body_action.as_ref() else {
        return Err(anyhow!("loop missing body action metadata"));
    };
    let mut kwargs: HashMap<String, Value> = HashMap::new();
    for keyword in &body_action.keywords {
        let value = eval_expr(
            keyword
                .value
                .as_ref()
                .context("loop body keyword missing value expression")?,
            &local_ctx,
        )?;
        kwargs.insert(keyword.arg.clone(), value);
    }
    Ok(Some(LoopEvaluation {
        kwargs,
        accumulator,
        current_index: loop_index,
    }))
}

fn build_action_dispatch(
    node: WorkflowDagNode,
    workflow_input: &WorkflowArguments,
    dependency_contexts: Vec<WorkflowNodeContext>,
    resolved_kwargs: HashMap<String, Value>,
) -> Result<QueuedWorkflowNode> {
    let resolved = values_to_arguments(&resolved_kwargs)?;
    let dispatch = WorkflowNodeDispatch {
        node: Some(node.clone()),
        workflow_input: Some(workflow_input.clone()),
        context: dependency_contexts,
        resolved_kwargs: Some(resolved),
    };
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

fn update_scheduling_context(
    node: &WorkflowDagNode,
    payload: Option<WorkflowArguments>,
    success: bool,
    scheduling_ctx: &mut SchedulingContext,
) -> Result<()> {
    let Some(arguments) = payload else {
        return Ok(());
    };
    let decoded = decode_payload(&arguments)?;
    if success {
        let vars = extract_variables_from_result(node, &decoded);
        for (name, value) in vars {
            scheduling_ctx.eval_ctx.insert(name, value);
        }
    } else if let Some(error) = decoded.error.as_ref() {
        scheduling_ctx
            .exceptions
            .insert(node.id.clone(), error.clone());
        scheduling_ctx.eval_ctx.insert(
            "__workflow_exceptions".to_string(),
            Value::Object(
                scheduling_ctx
                    .exceptions
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        );
    }
    let produced_vars = if node.produces.is_empty() {
        vec![String::new()]
    } else {
        node.produces.clone()
    };
    let entries = scheduling_ctx.payloads.entry(node.id.clone()).or_default();
    for variable in produced_vars {
        entries.push(NodeContextEntry {
            variable,
            payload: arguments.clone(),
        });
    }
    Ok(())
}

async fn insert_synthetic_completion_tx(
    tx: &mut Transaction<'_, Postgres>,
    instance: &WorkflowInstanceRow,
    node: &WorkflowDagNode,
    workflow_input: &WorkflowArguments,
    action_seq: i32,
    payload: Option<WorkflowArguments>,
    success: bool,
) -> Result<()> {
    let dispatch = WorkflowNodeDispatch {
        node: Some(node.clone()),
        workflow_input: Some(workflow_input.clone()),
        context: Vec::new(),
        resolved_kwargs: None,
    };
    let dispatch_bytes = dispatch.encode_to_vec();
    let payload_bytes = payload.as_ref().map(|p| p.encode_to_vec());
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
            workflow_node_id,
            timeout_seconds,
            max_retries,
            timeout_retry_limit,
            attempt_number,
            scheduled_at,
            dispatched_at,
            completed_at,
            acked_at,
            result_payload,
            success,
            retry_kind
        ) VALUES (
            $1, $2, $3,
            'completed',
            $4, $5, $6, $7,
            $8, $9, $10,
            0,
            NOW(), NOW(), NOW(), NOW(),
            $11, $12,
            'failure'
        )
        "#,
    )
    .bind(instance.id)
    .bind(instance.partition_id)
    .bind(action_seq)
    .bind(module)
    .bind(function_name)
    .bind(&dispatch_bytes)
    .bind(&node.id)
    .bind(
        node.timeout_seconds
            .unwrap_or(DEFAULT_ACTION_TIMEOUT_SECS as u32) as i32,
    )
    .bind(
        node.max_retries
            .unwrap_or(DEFAULT_ACTION_MAX_RETRIES as u32) as i32,
    )
    .bind(
        node.timeout_retry_limit
            .unwrap_or(DEFAULT_TIMEOUT_RETRY_LIMIT as u32) as i32,
    )
    .bind(payload_bytes)
    .bind(success)
    .execute(tx.as_mut())
    .await?;
    Ok(())
}

fn build_null_payload(node: &WorkflowDagNode) -> Result<Option<WorkflowArguments>> {
    if node.produces.is_empty() {
        return Ok(None);
    }
    let mut outer = HashMap::new();
    let mut object = serde_json::Map::new();
    for name in &node.produces {
        object.insert(name.clone(), Value::Null);
    }
    outer.insert("result".to_string(), Value::Object(object));
    values_to_arguments(&outer).map(Some)
}

fn eval_context_from_dispatch(dispatch: &WorkflowNodeDispatch) -> Result<EvalContext> {
    let mut ctx = EvalContext::new();
    if let Some(input) = dispatch.workflow_input.as_ref() {
        ctx.extend(arguments_to_json(input)?);
    }
    let mut exceptions: HashMap<String, Value> = HashMap::new();
    for entry in &dispatch.context {
        let Some(payload) = entry.payload.as_ref() else {
            continue;
        };
        let decoded = decode_payload(payload)?;
        if let Some(error) = decoded.error {
            if !entry.workflow_node_id.is_empty() {
                exceptions.insert(entry.workflow_node_id.clone(), error);
            }
            continue;
        }
        if entry.variable.is_empty() {
            continue;
        }
        if let Some(value) = decoded.result {
            if let Some(mapped) =
                variable_map_from_value(&value).and_then(|map| map.get(&entry.variable).cloned())
            {
                ctx.insert(entry.variable.clone(), mapped);
            } else {
                ctx.insert(entry.variable.clone(), value);
            }
        }
    }
    ctx.insert(
        "__workflow_exceptions".to_string(),
        Value::Object(
            exceptions
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
    );
    Ok(ctx)
}

async fn store_instance_result_if_complete(
    tx: &mut Transaction<'_, Postgres>,
    instance: &WorkflowInstanceRow,
    dag: &WorkflowDagDefinition,
    context_values: &HashMap<String, Vec<NodeContextEntry>>,
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
        for entry in entries {
            if entry.variable == dag.return_variable {
                payload_opt = Some(entry.payload.encode_to_vec());
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
    pub control: Option<proto::WorkflowNodeControl>,
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
    result_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
struct LoopActionRow {
    instance_id: WorkflowInstanceId,
    dispatch_payload: Vec<u8>,
    next_action_seq: i32,
    workflow_node_id: Option<String>,
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
    async fn process_loop_completion_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        record: &CompletionRecord,
    ) -> Result<Option<CompletionRecord>> {
        if !record.success {
            return Ok(Some(record.clone()));
        }
        let row = if let Some(dispatch_token) = record.dispatch_token {
            sqlx::query_as::<_, LoopActionRow>(
                r#"
                SELECT dal.instance_id,
                       dal.dispatch_payload,
                       wi.next_action_seq,
                       dal.workflow_node_id
                FROM daemon_action_ledger AS dal
                JOIN workflow_instances wi ON wi.id = dal.instance_id
                WHERE dal.id = $1
                  AND dal.delivery_token = $2
                  AND dal.status = 'dispatched'
                FOR UPDATE OF dal, wi
                "#,
            )
            .bind(record.action_id)
            .bind(dispatch_token)
            .fetch_optional(tx.as_mut())
            .await?
        } else {
            sqlx::query_as::<_, LoopActionRow>(
                r#"
                SELECT dal.instance_id,
                       dal.dispatch_payload,
                       wi.next_action_seq,
                       dal.workflow_node_id
                FROM daemon_action_ledger AS dal
                JOIN workflow_instances wi ON wi.id = dal.instance_id
                WHERE dal.id = $1
                  AND dal.status = 'dispatched'
                FOR UPDATE OF dal, wi
                "#,
            )
            .bind(record.action_id)
            .fetch_optional(tx.as_mut())
            .await?
        };
        let Some(row) = row else {
            return Ok(Some(record.clone()));
        };
        let mut dispatch = proto::WorkflowNodeDispatch::decode(row.dispatch_payload.as_slice())
            .context("failed to decode stored loop dispatch")?;
        let node = match dispatch.node.as_ref().cloned() {
            Some(node) => node,
            None => return Ok(Some(record.clone())),
        };
        let node_id = node.id.clone();
        if let Some(row_node_id) = row.workflow_node_id.as_ref()
            && row_node_id != &node_id
        {
            return Ok(Some(record.clone()));
        }
        let Some(loop_ast) = node
            .ast
            .as_ref()
            .and_then(|ast| ast.r#loop.as_ref())
            .cloned()
        else {
            return Ok(Some(record.clone()));
        };
        let control = match record.control.as_ref().and_then(|ctrl| ctrl.kind.as_ref()) {
            Some(proto::workflow_node_control::Kind::Loop(ctrl)) => Some(ctrl),
            _ => None,
        };
        let ctx = eval_context_from_dispatch(&dispatch)?;
        let mut accumulator = extract_accumulator(&ctx, &loop_ast.accumulator);
        let mut next_ctx = ctx.clone();
        if let Some(ctrl) = control {
            if !ctrl.node_id.is_empty() && ctrl.node_id != node_id {
                return Ok(Some(record.clone()));
            }
            if let Some(value) = ctrl
                .accumulator_value
                .as_ref()
                .and_then(|v| argument_value_to_json(v).ok())
            {
                match value {
                    Value::Array(items) => accumulator = items,
                    other => accumulator = vec![other],
                }
            }
            let index = ctrl.next_index as usize;
            next_ctx.insert(
                LOOP_INDEX_VAR.to_string(),
                Value::Number(serde_json::Number::from(index as u64)),
            );
            next_ctx.insert(
                loop_ast.accumulator.clone(),
                Value::Array(accumulator.clone()),
            );
            let next_eval = evaluate_loop_iteration(&node, &next_ctx)?;
            if ctrl.has_next
                && let Some(next_eval) = next_eval
            {
                let accumulator_payload = encode_value_result(&Value::Array(accumulator))?;
                let index_payload = encode_value_result(&Value::Number(serde_json::Number::from(
                    next_eval.current_index as u64,
                )))?;
                dispatch.context.retain(|ctx| {
                    ctx.variable != loop_ast.accumulator && ctx.variable != LOOP_INDEX_VAR
                });
                dispatch.context.push(WorkflowNodeContext {
                    variable: loop_ast.accumulator.clone(),
                    payload: Some(accumulator_payload),
                    workflow_node_id: node_id.clone(),
                });
                dispatch.context.push(WorkflowNodeContext {
                    variable: LOOP_INDEX_VAR.to_string(),
                    payload: Some(index_payload),
                    workflow_node_id: node_id.clone(),
                });
                dispatch.resolved_kwargs = Some(values_to_arguments(&next_eval.kwargs)?);
                if let Some(node_mut) = dispatch.node.as_mut()
                    && let Some(body_action) = loop_ast.body_action.as_ref()
                {
                    if !body_action.action_name.is_empty() {
                        node_mut.action = body_action.action_name.clone();
                    }
                    if !body_action.module_name.is_empty() {
                        node_mut.module = body_action.module_name.clone();
                    }
                }
                let dispatch_bytes = dispatch.encode_to_vec();
                let next_seq = row.next_action_seq;
                sqlx::query(
                    "UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1 AND next_action_seq <= $2",
                )
                .bind(row.instance_id)
                .bind(next_seq + 1)
                .execute(tx.as_mut())
                .await?;
                sqlx::query(
                    r#"
                    UPDATE daemon_action_ledger
                    SET status = 'queued',
                        scheduled_at = NOW(),
                        dispatched_at = NULL,
                        acked_at = COALESCE(acked_at, NOW()),
                        deadline_at = NULL,
                        delivery_id = NULL,
                        delivery_token = NULL,
                        action_seq = $2,
                        dispatch_payload = $3,
                        result_payload = $4,
                        success = NULL,
                        retry_kind = 'failure'
                    WHERE id = $1
                    "#,
                )
                .bind(record.action_id)
                .bind(next_seq)
                .bind(&dispatch_bytes)
                .bind(&record.result_payload)
                .execute(tx.as_mut())
                .await?;
                return Ok(None);
            }
            let final_payload = encode_value_result(&Value::Array(accumulator))?;
            let mut updated = record.clone();
            updated.result_payload = final_payload.encode_to_vec();
            return Ok(Some(updated));
        }
        let iteration_args = decode_arguments(&record.result_payload, "loop iteration result")?;
        let decoded = decode_payload(&iteration_args)?;
        if let Some(value) = decoded.result {
            accumulator.push(value);
        }
        let next_index = extract_loop_index(&ctx) + 1;
        next_ctx.insert(
            LOOP_INDEX_VAR.to_string(),
            Value::Number(serde_json::Number::from(next_index as u64)),
        );
        next_ctx.insert(
            loop_ast.accumulator.clone(),
            Value::Array(accumulator.clone()),
        );
        let next_eval = evaluate_loop_iteration(&node, &next_ctx)?;
        if let Some(next_eval) = next_eval {
            let accumulator_payload = encode_value_result(&Value::Array(accumulator))?;
            let index_payload = encode_value_result(&Value::Number(serde_json::Number::from(
                next_eval.current_index as u64,
            )))?;
            dispatch.context.retain(|ctx| {
                ctx.variable != loop_ast.accumulator && ctx.variable != LOOP_INDEX_VAR
            });
            dispatch.context.push(WorkflowNodeContext {
                variable: loop_ast.accumulator.clone(),
                payload: Some(accumulator_payload),
                workflow_node_id: node_id.clone(),
            });
            dispatch.context.push(WorkflowNodeContext {
                variable: LOOP_INDEX_VAR.to_string(),
                payload: Some(index_payload),
                workflow_node_id: node_id.clone(),
            });
            dispatch.resolved_kwargs = Some(values_to_arguments(&next_eval.kwargs)?);
            if let Some(node_mut) = dispatch.node.as_mut()
                && let Some(body_action) = loop_ast.body_action.as_ref()
            {
                if !body_action.action_name.is_empty() {
                    node_mut.action = body_action.action_name.clone();
                }
                if !body_action.module_name.is_empty() {
                    node_mut.module = body_action.module_name.clone();
                }
            }
            let dispatch_bytes = dispatch.encode_to_vec();
            let next_seq = row.next_action_seq;
            sqlx::query(
                "UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1 AND next_action_seq <= $2",
            )
            .bind(row.instance_id)
            .bind(next_seq + 1)
            .execute(tx.as_mut())
            .await?;
            sqlx::query(
                r#"
                UPDATE daemon_action_ledger
                SET status = 'queued',
                    scheduled_at = NOW(),
                    dispatched_at = NULL,
                    acked_at = COALESCE(acked_at, NOW()),
                    deadline_at = NULL,
                    delivery_id = NULL,
                    delivery_token = NULL,
                    action_seq = $2,
                    dispatch_payload = $3,
                    result_payload = $4,
                    success = NULL,
                    retry_kind = 'failure'
                WHERE id = $1
                "#,
            )
            .bind(record.action_id)
            .bind(next_seq)
            .bind(&dispatch_bytes)
            .bind(&record.result_payload)
            .execute(tx.as_mut())
            .await?;
            return Ok(None);
        }
        let final_payload = encode_value_result(&Value::Array(accumulator))?;
        let mut updated = record.clone();
        updated.result_payload = final_payload.encode_to_vec();
        Ok(Some(updated))
    }

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
                last_error,
                status,
                max_retries,
                timeout_retry_limit,
                result_payload
            FROM daemon_action_ledger
            WHERE id = ANY($1)
              AND (
                  (status = 'failed' AND attempt_number + 1 >= max_retries)
                  OR (status = 'timed_out' AND attempt_number + 1 >= timeout_retry_limit)
              )
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
            let limit = if row.status == "timed_out" {
                row.timeout_retry_limit
            } else {
                row.max_retries
            };
            let attempts = row.attempt_number + 1;
            let capped_attempts = attempts.min(limit.max(1));
            let payload = row.result_payload.clone().unwrap_or_else(|| {
                encode_exhausted_retries_payload(
                    &row.function_name,
                    capped_attempts,
                    row.last_error.as_deref(),
                )
            });
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
                delivery_token = NULL,
                retry_kind = 'failure'
            WHERE id = ANY($1)
              AND status = 'failed'
              AND attempt_number + 1 < max_retries
            "#,
        )
        .bind(action_ids)
        .execute(tx.as_mut())
        .await?;
        Ok(result.rows_affected() as usize)
    }

    async fn requeue_timed_out_actions_tx(
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
                delivery_token = NULL,
                retry_kind = 'timeout'
            WHERE id = ANY($1)
              AND status = 'timed_out'
              AND attempt_number + 1 < timeout_retry_limit
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
                    timeout_retry_limit,
                    attempt_number,
                    scheduled_at,
                    retry_kind
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    'queued',
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    0,
                    NOW(),
                    'failure'
                )
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
            .bind(DEFAULT_TIMEOUT_RETRY_LIMIT)
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
                  AND (
                      (retry_kind = 'timeout' AND attempt_number < timeout_retry_limit)
                      OR (retry_kind <> 'timeout' AND attempt_number < max_retries)
                  )
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
                     dal.delivery_token,
                     dal.timeout_retry_limit,
                     dal.retry_kind
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        match result {
            Ok(records) => {
                if !records.is_empty() {
                    metrics::counter!("rappel_actions_dispatched_total")
                        .increment(records.len() as u64);
                }
                Ok(records)
            }
            Err(err) => {
                metrics::counter!("rappel_dispatch_errors_total").increment(1);
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
                delivery_token = NULL,
                retry_kind = 'failure'
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
            .requeue_timed_out_actions_tx(&mut tx, &timed_out_ids)
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
        let mut tx = self.pool.begin().await?;
        let mut processed: Vec<CompletionRecord> = Vec::new();
        for record in records {
            if record.success {
                if let Some(updated) = self.process_loop_completion_tx(&mut tx, record).await? {
                    processed.push(updated);
                }
            } else {
                processed.push(record.clone());
            }
        }
        let mut workflow_instances: HashSet<WorkflowInstanceId> = HashSet::new();
        let mut successes_with_tokens: Vec<(&CompletionRecord, Uuid)> = Vec::new();
        let mut failures_with_tokens: Vec<(&CompletionRecord, Uuid)> = Vec::new();
        let mut dropped_successes = 0usize;
        let mut dropped_failures = 0usize;
        for record in &processed {
            if record.success {
                if let Some(token) = record.dispatch_token {
                    successes_with_tokens.push((record, token));
                } else {
                    dropped_successes += 1;
                }
            } else if let Some(token) = record.dispatch_token {
                failures_with_tokens.push((record, token));
            } else {
                dropped_failures += 1;
            }
        }
        if dropped_successes > 0 {
            tracing::debug!(
                count = dropped_successes,
                "dropping successful completions missing dispatch tokens"
            );
        }
        if dropped_failures > 0 {
            tracing::debug!(
                count = dropped_failures,
                "dropping failed completions missing dispatch tokens"
            );
        }
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
        let mut dag_state = InstanceDagState::new(known_nodes, completed_nodes);
        let dag_machine = DagStateMachine::new(&dag, version.concurrent);
        let workflow_input_bytes = instance.input_payload.clone().unwrap_or_default();
        let workflow_input = decode_arguments(&workflow_input_bytes, "workflow input")?;
        let mut scheduling_ctx =
            build_scheduling_context(tx, instance.id, &node_map, &workflow_input).await?;
        let mut completed_count = dag_state.completed().len();
        let mut next_sequence = instance.next_action_seq;
        let mut inserted = 0usize;
        loop {
            let unlocked = dag_machine.ready_nodes(&dag_state);
            if unlocked.is_empty() {
                break;
            }
            let mut progressed = false;
            for node in unlocked {
                let node_id = node.id.clone();
                let loop_ast = node
                    .ast
                    .as_ref()
                    .and_then(|ast| ast.r#loop.as_ref())
                    .cloned();
                let guard_passes = guard_allows(&node, &scheduling_ctx.eval_ctx)?;
                if !guard_passes {
                    let payload = build_null_payload(&node)?;
                    insert_synthetic_completion_tx(
                        tx,
                        &instance,
                        &node,
                        &workflow_input,
                        next_sequence,
                        payload.clone(),
                        true,
                    )
                    .await?;
                    if let Some(payload) = payload {
                        update_scheduling_context(&node, Some(payload), true, &mut scheduling_ctx)?;
                    }
                    dag_state.record_completion(node_id.clone());
                    completed_count += 1;
                    progressed = true;
                    next_sequence += 1;
                    continue;
                }
                if let Err(err) = validate_exception_context(&node, &scheduling_ctx.exceptions) {
                    let payload_bytes = encode_exception_payload(
                        "RuntimeError",
                        "rappel.workflow",
                        &err.to_string(),
                    );
                    let arguments = decode_arguments(&payload_bytes, "exception payload")?;
                    insert_synthetic_completion_tx(
                        tx,
                        &instance,
                        &node,
                        &workflow_input,
                        next_sequence,
                        Some(arguments.clone()),
                        false,
                    )
                    .await?;
                    update_scheduling_context(&node, Some(arguments), false, &mut scheduling_ctx)?;
                    dag_state.record_completion(node_id.clone());
                    completed_count += 1;
                    progressed = true;
                    next_sequence += 1;
                    continue;
                }
                if let Some(loop_ast) = loop_ast {
                    let loop_eval = evaluate_loop_iteration(&node, &scheduling_ctx.eval_ctx)?;
                    if let Some(loop_eval) = loop_eval {
                        let mut contexts =
                            build_dependency_contexts(&node, &scheduling_ctx.payloads);
                        let accumulator_value = Value::Array(loop_eval.accumulator.clone());
                        let accumulator_payload = encode_value_result(&accumulator_value)?;
                        contexts.push(WorkflowNodeContext {
                            variable: loop_ast.accumulator.clone(),
                            payload: Some(accumulator_payload),
                            workflow_node_id: node_id.clone(),
                        });
                        let index_value =
                            Value::Number(serde_json::Number::from(loop_eval.current_index as u64));
                        let index_payload = encode_value_result(&index_value)?;
                        contexts.push(WorkflowNodeContext {
                            variable: LOOP_INDEX_VAR.to_string(),
                            payload: Some(index_payload),
                            workflow_node_id: node_id.clone(),
                        });
                        let mut dispatch_node = node.clone();
                        if let Some(body_action) = loop_ast.body_action.as_ref() {
                            if !body_action.action_name.is_empty() {
                                dispatch_node.action = body_action.action_name.clone();
                            }
                            if !body_action.module_name.is_empty() {
                                dispatch_node.module = body_action.module_name.clone();
                            }
                        }
                        let queued = build_action_dispatch(
                            dispatch_node,
                            &workflow_input,
                            contexts,
                            loop_eval.kwargs,
                        )?;
                        let dispatch_bytes = queued.dispatch.encode_to_vec();
                        let scheduled_at = compute_scheduled_at(&node, &scheduling_ctx.eval_ctx)?;
                        sqlx::query_scalar::<_, LedgerActionId>(
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
                                timeout_retry_limit,
                                attempt_number,
                                scheduled_at,
                                retry_kind
                            ) VALUES (
                                $1,
                                $2,
                                $3,
                                'queued',
                                $4,
                                $5,
                                $6,
                                $7,
                                $8,
                                $9,
                                $10,
                                0,
                                $11,
                                'failure'
                            )
                            RETURNING id
                            "#,
                        )
                        .bind(instance.id)
                        .bind(instance.partition_id)
                        .bind(next_sequence)
                        .bind(&queued.module)
                        .bind(&queued.function_name)
                        .bind(&dispatch_bytes)
                        .bind(node_id.clone())
                        .bind(queued.timeout_seconds)
                        .bind(queued.max_retries)
                        .bind(queued.timeout_retry_limit)
                        .bind(scheduled_at)
                        .fetch_one(tx.as_mut())
                        .await?;
                        dag_state.record_known(node_id.clone());
                        inserted += 1;
                        next_sequence += 1;
                        progressed = true;
                        continue;
                    } else {
                        let accumulator_value = Value::Array(extract_accumulator(
                            &scheduling_ctx.eval_ctx,
                            &loop_ast.accumulator,
                        ));
                        let payload = encode_value_result(&accumulator_value)?;
                        insert_synthetic_completion_tx(
                            tx,
                            &instance,
                            &node,
                            &workflow_input,
                            next_sequence,
                            Some(payload.clone()),
                            true,
                        )
                        .await?;
                        update_scheduling_context(&node, Some(payload), true, &mut scheduling_ctx)?;
                        dag_state.record_completion(node_id.clone());
                        completed_count += 1;
                        progressed = true;
                        next_sequence += 1;
                        continue;
                    }
                }
                let kwargs = evaluate_kwargs(&node, &scheduling_ctx.eval_ctx)?;
                let contexts = build_dependency_contexts(&node, &scheduling_ctx.payloads);
                let queued =
                    build_action_dispatch(node.clone(), &workflow_input, contexts, kwargs)?;
                let dispatch_bytes = queued.dispatch.encode_to_vec();
                let scheduled_at = compute_scheduled_at(&node, &scheduling_ctx.eval_ctx)?;
                sqlx::query_scalar::<_, LedgerActionId>(
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
                        timeout_retry_limit,
                        attempt_number,
                        scheduled_at,
                        retry_kind
                    ) VALUES (
                        $1,
                        $2,
                        $3,
                        'queued',
                        $4,
                        $5,
                        $6,
                        $7,
                        $8,
                        $9,
                        $10,
                        0,
                        $11,
                        'failure'
                    )
                    RETURNING id
                    "#,
                )
                .bind(instance.id)
                .bind(instance.partition_id)
                .bind(next_sequence)
                .bind(&queued.module)
                .bind(&queued.function_name)
                .bind(&dispatch_bytes)
                .bind(&node_id)
                .bind(queued.timeout_seconds)
                .bind(queued.max_retries)
                .bind(queued.timeout_retry_limit)
                .bind(scheduled_at)
                .fetch_one(tx.as_mut())
                .await?;
                dag_state.record_known(node_id);
                inserted += 1;
                next_sequence += 1;
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        if next_sequence != instance.next_action_seq {
            sqlx::query("UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1")
                .bind(instance.id)
                .bind(next_sequence)
                .execute(tx.as_mut())
                .await?;
        }
        store_instance_result_if_complete(
            tx,
            &instance,
            &dag,
            &scheduling_ctx.payloads,
            completed_count,
        )
        .await?;
        tracing::debug!(
            instance_id = %instance.id,
            workflow = instance.workflow_name,
            inserted,
            "queued workflow actions"
        );
        Ok(inserted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use sqlx::{PgPool, postgres::PgPoolOptions};

    async fn setup_test_pool() -> Result<Option<PgPool>> {
        let _ = dotenvy::dotenv();
        let database_url = match std::env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!("skipping db tests: DATABASE_URL not set");
                return Ok(None);
            }
        };
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Some(pool))
    }

    async fn reset_tables(pool: &PgPool) -> Result<()> {
        sqlx::query("TRUNCATE daemon_action_ledger, workflow_instances, workflow_versions CASCADE")
            .execute(pool)
            .await?;
        Ok(())
    }

    fn encode_error_payload(message: &str) -> Vec<u8> {
        encode_exception_payload("RuntimeError", "tests", message)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn explicit_failure_does_not_retry_by_default() -> Result<()> {
        let Some(pool) = setup_test_pool().await? else {
            return Ok(());
        };
        reset_tables(&pool).await?;
        let database = Database { pool: pool.clone() };
        let dispatch = proto::WorkflowNodeDispatch {
            node: None,
            workflow_input: None,
            context: Vec::new(),
            resolved_kwargs: None,
        };
        let payload = dispatch.encode_to_vec();
        database
            .seed_actions(1, "tests", "action", &payload)
            .await?;
        let mut actions = database.dispatch_actions(1).await?;
        let action = actions.pop().expect("dispatched action");
        let record = CompletionRecord {
            action_id: action.id,
            success: false,
            delivery_id: 1,
            result_payload: encode_error_payload("boom"),
            dispatch_token: Some(action.delivery_token),
            control: None,
        };
        database.mark_actions_batch(&[record]).await?;
        let queued: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM daemon_action_ledger WHERE status = 'queued'")
                .fetch_one(database.pool())
                .await?;
        assert_eq!(queued, 0);
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn timeout_retries_are_unbounded_by_default() -> Result<()> {
        let Some(pool) = setup_test_pool().await? else {
            return Ok(());
        };
        reset_tables(&pool).await?;
        let database = Database { pool: pool.clone() };
        let dispatch = proto::WorkflowNodeDispatch {
            node: None,
            workflow_input: None,
            context: Vec::new(),
            resolved_kwargs: None,
        };
        let payload = dispatch.encode_to_vec();
        database
            .seed_actions(1, "tests", "action", &payload)
            .await?;
        let mut actions = database.dispatch_actions(1).await?;
        let action = actions.pop().expect("dispatched action");
        sqlx::query(
            "UPDATE daemon_action_ledger SET deadline_at = NOW() - INTERVAL '5 seconds' WHERE id = $1",
        )
        .bind(action.id)
        .execute(database.pool())
        .await?;
        let timed_out = database.mark_timed_out_actions(10).await?;
        assert_eq!(timed_out, 1);
        let redispatched = database.dispatch_actions(1).await?;
        assert_eq!(redispatched.len(), 1);
        Ok(())
    }
}
