use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::Value;
use sqlx::{
    FromRow, PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};
use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

use crate::{
    LedgerActionId, WorkflowInstanceId, WorkflowVersionId,
    ast_eval::{EvalContext, eval_expr, eval_stmt, eval_string_expr},
    context::{
        DecodedPayload, argument_value_to_json, arguments_to_json, decode_payload,
        values_to_arguments,
    },
    dag_state::{DagStateMachine, InstanceDagState},
    messages::proto::{
        self, WorkflowArguments, WorkflowDagDefinition, WorkflowDagNode, WorkflowNodeContext,
        WorkflowNodeDispatch,
    },
    retry::{BackoffConfig, DEFAULT_EXPONENTIAL_MULTIPLIER},
};

const DEFAULT_ACTION_TIMEOUT_SECS: i32 = 300;
const DEFAULT_ACTION_MAX_RETRIES: i32 = 1;
const DEFAULT_TIMEOUT_RETRY_LIMIT: i32 = i32::MAX;
const EXHAUSTED_EXCEPTION_TYPE: &str = "ExhaustedRetries";
const EXHAUSTED_EXCEPTION_MODULE: &str = "rappel.exceptions";
const LOOP_INDEX_VAR: &str = "__loop_index";
const LOOP_PHASE_VAR: &str = "__loop_phase";
const LOOP_PHASE_RESULTS_VAR: &str = "__loop_phase_results";
const LOOP_PREAMBLE_RESULTS_VAR: &str = "__loop_preamble_results";

/// Compute the number of dependencies a node requires based on DAG structure.
/// Counts data dependencies (depends_on) and exception edge sources.
/// Sync dependencies (wait_for_sync) are handled through guard evaluation.
fn compute_deps_required(node: &WorkflowDagNode, _concurrent: bool) -> i16 {
    // Count unique exception edge sources (a node might have multiple exception types from same source)
    let exception_sources: std::collections::HashSet<&str> = node
        .exception_edges
        .iter()
        .map(|e| e.source_node_id.as_str())
        .collect();

    // __conditional_join needs ALL branches to complete (even skipped ones)
    // so it can properly merge non-null values from the executed branch

    (node.depends_on.len() + exception_sources.len()) as i16
}

use crate::messages::proto::{EdgeType, NodeType};

/// Get all downstream node IDs that depend on a given node.
/// Returns nodes that have this node in their `depends_on` list (data dependencies)
/// or have an exception_edge referencing this node (exception handlers).
/// Sync dependencies (wait_for_sync) are handled separately through guard evaluation.
///
/// For cycle-based loops: this excludes back edges (those are handled specially).
/// Also excludes continue/break edges (those are handled by loop_head evaluation).
fn get_downstream_nodes<'a>(
    node_id: &str,
    dag: &'a WorkflowDagDefinition,
    _concurrent: bool,
) -> Vec<&'a str> {
    use std::collections::HashSet;
    let mut result: HashSet<&str> = HashSet::new();

    // Always check depends_on for regular dependency edges
    for n in &dag.nodes {
        // Check if this node depends on the completed node
        if n.depends_on.iter().any(|d| d == node_id) {
            result.insert(n.id.as_str());
        }
        // Or if this node has an exception edge from the completed node
        if n.exception_edges
            .iter()
            .any(|e| e.source_node_id == node_id)
        {
            result.insert(n.id.as_str());
        }
    }

    // If explicit edges exist, also add forward edges (but exclude back/continue/break edges)
    // Continue/break edges are handled specially by loop_head evaluation
    // Back edges are handled by the back edge handler
    for e in &dag.edges {
        if e.from_node == node_id && e.edge_type == EdgeType::Forward as i32 {
            result.insert(e.to_node.as_str());
        }
    }

    result.into_iter().collect()
}

/// Get downstream nodes filtered by edge type.
fn get_downstream_nodes_by_edge_type<'a>(
    node_id: &str,
    dag: &'a WorkflowDagDefinition,
    edge_type: EdgeType,
) -> Vec<&'a str> {
    dag.edges
        .iter()
        .filter(|e| e.from_node == node_id && e.edge_type == edge_type as i32)
        .map(|e| e.to_node.as_str())
        .collect()
}

/// Get the target of a back edge from a node.
fn get_back_edge_target<'a>(node_id: &str, dag: &'a WorkflowDagDefinition) -> Option<&'a str> {
    dag.edges
        .iter()
        .find(|e| e.from_node == node_id && e.edge_type == EdgeType::Back as i32)
        .map(|e| e.to_node.as_str())
}

/// Cached workflow version data - avoids repeated DB lookups for the same version
#[derive(Debug, Clone)]
struct CachedWorkflowVersion {
    concurrent: bool,
    dag: WorkflowDagDefinition,
    node_map: HashMap<String, WorkflowDagNode>,
}

/// LRU-style cache for workflow versions. Uses a simple HashMap with a max size.
/// When full, clears the entire cache (simple but effective for workflow orchestration
/// where versions are relatively stable during a run).
#[derive(Debug, Default)]
struct WorkflowVersionCache {
    entries: HashMap<WorkflowVersionId, CachedWorkflowVersion>,
    max_size: usize,
}

impl WorkflowVersionCache {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(max_size),
            max_size,
        }
    }

    fn get(&self, version_id: &WorkflowVersionId) -> Option<&CachedWorkflowVersion> {
        self.entries.get(version_id)
    }

    fn insert(&mut self, version_id: WorkflowVersionId, version: CachedWorkflowVersion) {
        // Simple eviction: if at capacity, clear everything
        // This is fine because in practice we have very few workflow versions active at once
        if self.entries.len() >= self.max_size && !self.entries.contains_key(&version_id) {
            self.entries.clear();
        }
        self.entries.insert(version_id, version);
    }
}

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
    version_cache: Arc<RwLock<WorkflowVersionCache>>,
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

/// Result of extracting a variable map from a value
#[derive(Debug)]
enum VariableMapSource {
    /// Variables from a WorkflowNodeResult (data.variables structure)
    WorkflowNodeResult(HashMap<String, Value>),
    /// Variables from a plain dict - only extract for internal temp variables
    PlainDict(HashMap<String, Value>),
}

fn variable_map_from_value(value: &Value) -> Option<VariableMapSource> {
    if let Value::Object(map) = value {
        if let Some(Value::Object(data)) = map.get("data")
            && let Some(Value::Object(vars)) = data.get("variables")
        {
            let mut out = HashMap::new();
            for (k, v) in vars {
                out.insert(k.clone(), v.clone());
            }
            return Some(VariableMapSource::WorkflowNodeResult(out));
        }
        let mut out = HashMap::new();
        for (k, v) in map {
            out.insert(k.clone(), v.clone());
        }
        return Some(VariableMapSource::PlainDict(out));
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
        // Check if result is a WorkflowNodeResult or plain dict
        match variable_map_from_value(result) {
            Some(VariableMapSource::WorkflowNodeResult(map)) => {
                // Always extract from WorkflowNodeResult regardless of variable name
                if let Some(value) = map.get(name) {
                    variables.insert(name.clone(), value.clone());
                    return variables;
                }
            }
            Some(VariableMapSource::PlainDict(map)) => {
                // Only extract from plain dict for internal temp variables (like __branch_*)
                // For user-defined variables, we want to assign the entire dict even if it
                // happens to contain a key matching the variable name.
                if name.starts_with("__")
                    && let Some(value) = map.get(name)
                {
                    variables.insert(name.clone(), value.clone());
                    return variables;
                }
            }
            None => {}
        }
        variables.insert(name.clone(), result.clone());
        return variables;
    }
    if node.produces.is_empty() {
        return variables;
    }
    // For multiple produces
    match variable_map_from_value(result) {
        Some(VariableMapSource::WorkflowNodeResult(nested)) => {
            // Always extract from WorkflowNodeResult
            for name in &node.produces {
                if let Some(value) = nested.get(name) {
                    variables.insert(name.clone(), value.clone());
                }
            }
        }
        Some(VariableMapSource::PlainDict(nested)) => {
            // Only extract internal temp variables from plain dicts
            for name in &node.produces {
                if name.starts_with("__") {
                    if let Some(value) = nested.get(name) {
                        variables.insert(name.clone(), value.clone());
                    }
                } else {
                    // For user-defined variables, assign the entire result
                    variables.insert(name.clone(), result.clone());
                }
            }
        }
        None => {}
    }
    variables
}

fn guard_allows(node: &WorkflowDagNode, ctx: &EvalContext) -> Result<bool> {
    // Check string guard field first (used by IR-to-DAG converter)
    if !node.guard.is_empty() {
        let value = eval_string_expr(&node.guard, ctx)?;
        return Ok(is_truthy_value(&value));
    }
    // Fallback to legacy AST guard
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
    // Collect all direct dependencies of this node
    let dependencies: HashSet<&str> = node
        .depends_on
        .iter()
        .chain(node.wait_for_sync.iter())
        .map(|s| s.as_str())
        .collect();
    // Build set of matched exceptions from exception_edges
    let mut matched: HashSet<String> = HashSet::new();
    for edge in &node.exception_edges {
        if let Some(error) = exceptions.get(&edge.source_node_id)
            && exception_matches(edge, error)
        {
            matched.insert(edge.source_node_id.clone());
        }
    }
    // Only check exceptions from direct dependencies, not all exceptions in workflow.
    // Exceptions from nodes that are not direct dependencies have either been:
    // 1. Handled by intermediate nodes with exception_edges
    // 2. Caused intermediate nodes to be skipped (via guards)
    // Either way, they should not cause this node to fail if this node doesn't
    // directly depend on the failed node.
    for key in exceptions.keys() {
        if dependencies.contains(key.as_str()) && !matched.contains(key) {
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

/// Combined result of loading instance state and building scheduling context.
/// This combines two queries into one to reduce DB round-trips.
struct InstanceStateAndContext {
    known_nodes: HashSet<String>,
    completed_nodes: HashSet<String>,
    scheduling_ctx: SchedulingContext,
}

/// Load instance node state and build scheduling context in a single query.
/// This replaces the separate `load_instance_node_state` and `build_scheduling_context` functions.
async fn load_instance_state_and_context(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
    dag_nodes: &HashMap<String, WorkflowDagNode>,
    workflow_input: &WorkflowArguments,
) -> Result<InstanceStateAndContext> {
    // Single query fetches all action ledger data for this instance
    let rows = sqlx::query(
        r#"
        SELECT workflow_node_id, status, result_payload, success
        FROM daemon_action_ledger
        WHERE instance_id = $1
        "#,
    )
    .bind(instance_id)
    .map(|row: PgRow| {
        (
            row.get::<Option<String>, _>(0),
            row.get::<String, _>(1),
            row.get::<Option<Vec<u8>>, _>(2),
            row.get::<Option<bool>, _>(3),
        )
    })
    .fetch_all(tx.as_mut())
    .await?;

    // Build node state (known + completed)
    let mut known_nodes = HashSet::new();
    let mut completed_nodes = HashSet::new();

    // Build scheduling context
    let mut eval_ctx: EvalContext = arguments_to_json(workflow_input)?;
    let mut payloads: HashMap<String, Vec<NodeContextEntry>> = HashMap::new();
    let mut exceptions: HashMap<String, Value> = HashMap::new();

    for (node_id_opt, status, payload_opt, success_opt) in rows {
        // Handle node state tracking
        if let Some(ref node_id) = node_id_opt {
            if status.eq_ignore_ascii_case("completed") {
                completed_nodes.insert(node_id.clone());
            }
            known_nodes.insert(node_id.clone());
        }

        // Handle scheduling context (only for completed actions with node_id)
        if status.eq_ignore_ascii_case("completed") {
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

    Ok(InstanceStateAndContext {
        known_nodes,
        completed_nodes,
        scheduling_ctx: SchedulingContext {
            eval_ctx,
            payloads,
            exceptions,
        },
    })
}

#[derive(Debug, Clone)]
struct LoopEvaluation {
    kwargs: HashMap<String, Value>,
    accumulator: Vec<Value>,
    current_index: usize,
}

/// Evaluation result for a single phase within a multi-action loop iteration
#[derive(Debug, Clone)]
struct MultiActionPhaseEvaluation {
    phase_id: String,
    action: String,
    module: String,
    kwargs: HashMap<String, Value>,
    output_var: String,
    /// Variables created by preamble execution (only set for first phase)
    preamble_results: Option<HashMap<String, Value>>,
}

/// State for tracking progress within a multi-action loop iteration
#[derive(Debug, Clone)]
struct MultiActionLoopState {
    current_index: usize,
    accumulators: HashMap<String, Vec<Value>>,
    completed_phases: HashSet<String>,
    phase_results: HashMap<String, Value>,
    preamble_results: HashMap<String, Value>,
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

/// Evaluate a source expression like "var_name['key']" or just "var_name"
/// against an eval context. Supports patterns:
///   - "var_name" -> look up var_name in context
///   - "var_name['key']" -> look up var_name in context, then subscript with 'key'
///   - "var_name[\"key\"]" -> same with double quotes
fn evaluate_source_expr(expr: &str, ctx: &EvalContext) -> Option<Value> {
    // Try to parse "var['key']" or "var[\"key\"]" pattern
    if let Some(bracket_pos) = expr.find('[') {
        let var_name = &expr[..bracket_pos];
        let rest = &expr[bracket_pos..];

        // Get the variable value
        let var_value = ctx.get(var_name)?;

        // Parse the key from ['key'] or ["key"]
        // rest looks like "['key']" or "[\"key\"]"
        if (rest.starts_with("['") && rest.ends_with("']"))
            || (rest.starts_with("[\"") && rest.ends_with("\"]"))
        {
            let key = &rest[2..rest.len() - 2];
            if let Value::Object(obj) = var_value {
                return obj.get(key).cloned();
            }
        }
    }

    // Simple variable lookup
    ctx.get(expr).cloned()
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

/// Find the next ready phase in a multi-action loop body graph
fn find_next_ready_phase(
    node: &WorkflowDagNode,
    ctx: &EvalContext,
    state: &MultiActionLoopState,
) -> Result<Option<MultiActionPhaseEvaluation>> {
    let Some(ast) = node.ast.as_ref() else {
        return Ok(None);
    };
    let Some(loop_ast) = ast.r#loop.as_ref() else {
        return Ok(None);
    };
    let Some(body_graph) = loop_ast.body_graph.as_ref() else {
        return Ok(None);
    };

    // Build local context with loop variable and phase results
    let iterable = loop_ast
        .iterable
        .as_ref()
        .context("loop missing iterable expression")?;
    let iterable_value = eval_expr(iterable, ctx)?;
    let Value::Array(items) = iterable_value else {
        return Err(anyhow!("loop iterable must resolve to a list"));
    };

    if state.current_index >= items.len() {
        return Ok(None);
    }

    let mut local_ctx = ctx.clone();
    local_ctx.insert(
        loop_ast.loop_var.clone(),
        items[state.current_index].clone(),
    );

    // Insert all accumulators into context
    for (acc_name, acc_values) in &state.accumulators {
        local_ctx.insert(acc_name.clone(), Value::Array(acc_values.clone()));
    }

    // Add phase results to context
    for (var_name, value) in &state.phase_results {
        local_ctx.insert(var_name.clone(), value.clone());
    }

    // Track variables before preamble execution to capture new ones
    let pre_preamble_keys: HashSet<String> = local_ctx.keys().cloned().collect();
    let mut preamble_results: Option<HashMap<String, Value>> = None;

    // Execute preamble if this is the first phase, otherwise load saved preamble results
    if state.completed_phases.is_empty() {
        // Debug: log context keys and processed_list length
        if let Some(pl) = local_ctx.get("processed_list") {
            if let Value::Array(arr) = pl {
                tracing::debug!("preamble: processed_list has {} items", arr.len());
            } else {
                tracing::debug!("preamble: processed_list is not array: {:?}", pl);
            }
        } else {
            tracing::debug!(
                "preamble: processed_list NOT in context. Keys: {:?}",
                pre_preamble_keys
            );
        }
        for stmt in &loop_ast.preamble {
            eval_stmt(stmt, &mut local_ctx)?;
        }
        // Capture variables created by preamble
        let new_vars: HashMap<String, Value> = local_ctx
            .iter()
            .filter(|(k, _)| !pre_preamble_keys.contains(*k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !new_vars.is_empty() {
            preamble_results = Some(new_vars);
        }
    } else {
        // Load preamble results from previous phase
        for (var_name, value) in &state.preamble_results {
            local_ctx.insert(var_name.clone(), value.clone());
        }
    }

    // Find a phase that is ready (all dependencies completed)
    for phase_node in &body_graph.nodes {
        if state.completed_phases.contains(&phase_node.id) {
            continue;
        }

        // Check if all dependencies are completed
        let deps_satisfied = phase_node
            .depends_on
            .iter()
            .all(|dep| state.completed_phases.contains(dep));

        if !deps_satisfied {
            continue;
        }

        // This phase is ready - evaluate its kwargs
        let mut kwargs: HashMap<String, Value> = HashMap::new();
        for keyword in &phase_node.kwargs {
            let value = eval_expr(
                keyword
                    .value
                    .as_ref()
                    .context("phase keyword missing value expression")?,
                &local_ctx,
            )?;
            kwargs.insert(keyword.arg.clone(), value);
        }

        return Ok(Some(MultiActionPhaseEvaluation {
            phase_id: phase_node.id.clone(),
            action: phase_node.action.clone(),
            module: phase_node.module.clone(),
            kwargs,
            output_var: phase_node.output_var.clone(),
            preamble_results,
        }));
    }

    Ok(None)
}

/// Check if all phases in the body graph are completed
fn is_iteration_complete(node: &WorkflowDagNode, state: &MultiActionLoopState) -> bool {
    let Some(ast) = node.ast.as_ref() else {
        return true;
    };
    let Some(loop_ast) = ast.r#loop.as_ref() else {
        return true;
    };
    let Some(body_graph) = loop_ast.body_graph.as_ref() else {
        return true;
    };

    body_graph
        .nodes
        .iter()
        .all(|phase| state.completed_phases.contains(&phase.id))
}

/// Extract the result variable from the completed iteration
fn extract_iteration_result(node: &WorkflowDagNode, state: &MultiActionLoopState) -> Option<Value> {
    let ast = node.ast.as_ref()?;
    let loop_ast = ast.r#loop.as_ref()?;
    let body_graph = loop_ast.body_graph.as_ref()?;

    state
        .phase_results
        .get(&body_graph.result_variable)
        .cloned()
}

/// Check if this node has a multi-action loop body graph
fn has_multi_action_body_graph(node: &WorkflowDagNode) -> bool {
    node.ast
        .as_ref()
        .and_then(|ast| ast.r#loop.as_ref())
        .and_then(|loop_ast| loop_ast.body_graph.as_ref())
        .map(|bg| !bg.nodes.is_empty())
        .unwrap_or(false)
}

/// Extract multi-action loop state from context
fn extract_multi_action_loop_state(
    ctx: &EvalContext,
    accumulator_names: &[String],
) -> MultiActionLoopState {
    let current_index = extract_loop_index(ctx);
    let accumulators: HashMap<String, Vec<Value>> = accumulator_names
        .iter()
        .map(|name| (name.clone(), extract_accumulator(ctx, name)))
        .collect();

    let completed_phases = ctx
        .get(LOOP_PHASE_VAR)
        .and_then(|v| match v {
            Value::Array(arr) => Some(
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default();

    let phase_results = ctx
        .get(LOOP_PHASE_RESULTS_VAR)
        .and_then(|v| match v {
            Value::Object(obj) => Some(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            _ => None,
        })
        .unwrap_or_default();

    let preamble_results = ctx
        .get(LOOP_PREAMBLE_RESULTS_VAR)
        .and_then(|v| match v {
            Value::Object(obj) => Some(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            _ => None,
        })
        .unwrap_or_default();

    MultiActionLoopState {
        current_index,
        accumulators,
        completed_phases,
        phase_results,
        preamble_results,
    }
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
    let backoff = BackoffConfig::from_proto(node.backoff.as_ref());

    Ok(QueuedWorkflowNode {
        module,
        function_name,
        dispatch,
        timeout_seconds,
        max_retries,
        timeout_retry_limit,
        backoff,
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
    let backoff = BackoffConfig::from_proto(node.backoff.as_ref());
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
            retry_kind,
            backoff_kind,
            backoff_base_delay_ms,
            backoff_multiplier
        ) VALUES (
            $1, $2, $3,
            'completed',
            $4, $5, $6, $7,
            $8, $9, $10,
            0,
            NOW(), NOW(), NOW(), NOW(),
            $11, $12,
            'failure',
            $13, $14, $15
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
    .bind(backoff.kind_str())
    .bind(backoff.base_delay_ms())
    .bind(backoff.multiplier())
    .execute(tx.as_mut())
    .await?;
    Ok(())
}

/// Insert a synthetic completion record directly using primitive values.
/// This is a simpler variant of insert_synthetic_completion_tx for the push-based scheduler.
#[allow(clippy::too_many_arguments)]
async fn insert_synthetic_completion_simple_tx(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
    partition_id: i32,
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
    let backoff = BackoffConfig::from_proto(node.backoff.as_ref());
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
            retry_kind,
            backoff_kind,
            backoff_base_delay_ms,
            backoff_multiplier
        ) VALUES (
            $1, $2, $3,
            'completed',
            $4, $5, $6, $7,
            $8, $9, $10,
            0,
            NOW(), NOW(), NOW(), NOW(),
            $11, $12,
            'failure',
            $13, $14, $15
        )
        "#,
    )
    .bind(instance_id)
    .bind(partition_id)
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
    .bind(backoff.kind_str())
    .bind(backoff.base_delay_ms())
    .bind(backoff.multiplier())
    .execute(tx.as_mut())
    .await?;
    Ok(())
}

fn build_null_payload(node: &WorkflowDagNode) -> Result<Option<WorkflowArguments>> {
    if node.produces.is_empty() {
        return Ok(None);
    }
    // Create a WorkflowNodeResult-style payload with data.variables structure
    // This ensures proper extraction for scheduler-generated null values
    let mut variables = serde_json::Map::new();
    for name in &node.produces {
        variables.insert(name.clone(), Value::Null);
    }
    let data = serde_json::json!({
        "variables": Value::Object(variables)
    });
    let result = serde_json::json!({
        "data": data
    });
    let mut outer = HashMap::new();
    outer.insert("result".to_string(), result);
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
            // Extract value based on source type:
            // - WorkflowNodeResult: always extract the specific variable
            // - PlainDict: only extract for internal temp variables (like __branch_*)
            // For user-defined variables with plain dicts, we want to assign the entire
            // dict even if it happens to contain a key matching the variable name.
            let final_value = match variable_map_from_value(&value) {
                Some(VariableMapSource::WorkflowNodeResult(map)) => {
                    // Always extract from WorkflowNodeResult
                    map.get(&entry.variable).cloned().unwrap_or(value)
                }
                Some(VariableMapSource::PlainDict(map)) => {
                    // Only extract from plain dict for internal temp variables
                    if entry.variable.starts_with("__") {
                        map.get(&entry.variable).cloned().unwrap_or(value)
                    } else {
                        value
                    }
                }
                None => value,
            };
            // Don't overwrite existing non-null values with null - this handles convergent
            // branches (try/except, if/else) where skipped nodes produce null values that
            // shouldn't replace real values from the branch that actually executed.
            if final_value.is_null()
                && let Some(existing) = ctx.get(&entry.variable)
                && !existing.is_null()
            {
                continue;
            }
            ctx.insert(entry.variable.clone(), final_value);
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
    backoff: BackoffConfig,
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
struct BatchedLoopActionRow {
    action_id: LedgerActionId,
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

/// Pending completion to process in the work queue for O(1) push-based scheduling.
struct PendingCompletion {
    node_id: String,
    payload: Option<WorkflowArguments>,
    success: bool,
}

impl Database {
    /// Batch fetch loop action data for multiple records.
    /// Uses UNNEST to query all action_ids at once, reducing N queries to 1.
    async fn batch_fetch_loop_action_data(
        tx: &mut Transaction<'_, Postgres>,
        records: &[&CompletionRecord],
    ) -> Result<HashMap<LedgerActionId, BatchedLoopActionRow>> {
        if records.is_empty() {
            return Ok(HashMap::new());
        }

        let action_ids: Vec<LedgerActionId> = records.iter().map(|r| r.action_id).collect();
        let dispatch_tokens: Vec<Option<Uuid>> = records.iter().map(|r| r.dispatch_token).collect();

        let rows = sqlx::query_as::<_, BatchedLoopActionRow>(
            r#"
            WITH inputs AS (
                SELECT * FROM UNNEST($1::UUID[], $2::UUID[]) AS t(action_id, dispatch_token)
            )
            SELECT dal.id AS action_id,
                   dal.instance_id,
                   dal.dispatch_payload,
                   wi.next_action_seq,
                   dal.workflow_node_id
            FROM inputs
            JOIN daemon_action_ledger AS dal ON dal.id = inputs.action_id
            JOIN workflow_instances wi ON wi.id = dal.instance_id
            WHERE dal.status = 'dispatched'
              AND (inputs.dispatch_token IS NULL OR dal.delivery_token = inputs.dispatch_token)
            FOR UPDATE OF dal, wi
            "#,
        )
        .bind(&action_ids)
        .bind(&dispatch_tokens)
        .fetch_all(tx.as_mut())
        .await?;

        let map: HashMap<LedgerActionId, BatchedLoopActionRow> =
            rows.into_iter().map(|row| (row.action_id, row)).collect();

        Ok(map)
    }

    /// Process loop completion using pre-fetched row data.
    /// This version skips the initial query since data was already fetched in batch.
    async fn process_loop_completion_with_row_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        record: &CompletionRecord,
        batched_row: &BatchedLoopActionRow,
    ) -> Result<Option<CompletionRecord>> {
        let fn_start = Instant::now();

        // Convert BatchedLoopActionRow to LoopActionRow for existing processing
        let row = LoopActionRow {
            instance_id: batched_row.instance_id,
            dispatch_payload: batched_row.dispatch_payload.clone(),
            next_action_seq: batched_row.next_action_seq,
            workflow_node_id: batched_row.workflow_node_id.clone(),
        };

        let decode_start = Instant::now();
        let mut dispatch = proto::WorkflowNodeDispatch::decode(row.dispatch_payload.as_slice())
            .context("failed to decode stored loop dispatch")?;
        let decode_ms = decode_start.elapsed().as_secs_f64() * 1000.0;
        let node = match dispatch.node.as_ref().cloned() {
            Some(node) => node,
            None => {
                debug!(
                    target: "db_timing",
                    r#"{{"fn":"process_loop_completion_with_row_tx","total_ms":{:.3},"decode_ms":{:.3},"outcome":"no_node"}}"#,
                    fn_start.elapsed().as_secs_f64() * 1000.0, decode_ms
                );
                return Ok(Some(record.clone()));
            }
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
            debug!(
                target: "db_timing",
                r#"{{"fn":"process_loop_completion_with_row_tx","total_ms":{:.3},"decode_ms":{:.3},"outcome":"no_loop"}}"#,
                fn_start.elapsed().as_secs_f64() * 1000.0, decode_ms
            );
            return Ok(Some(record.clone()));
        };

        // Handle multi-action loop completion
        if has_multi_action_body_graph(&node) {
            let multi_start = Instant::now();
            let result = self
                .process_multi_action_loop_completion_tx(
                    tx, record, &node, &loop_ast, &dispatch, &row,
                )
                .await;
            let multi_ms = multi_start.elapsed().as_secs_f64() * 1000.0;
            debug!(
                target: "db_timing",
                r#"{{"fn":"process_loop_completion_with_row_tx","total_ms":{:.3},"decode_ms":{:.3},"multi_action_ms":{:.3},"outcome":"multi_action"}}"#,
                fn_start.elapsed().as_secs_f64() * 1000.0, decode_ms, multi_ms
            );
            return result;
        }

        // For simple loops, delegate to existing processing
        // (copy the rest of the simple loop logic here or call existing method)
        self.process_simple_loop_completion_tx(tx, record, &node, &loop_ast, &mut dispatch, &row)
            .await
    }

    /// Process simple (non-multi-action) loop completion
    async fn process_simple_loop_completion_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        record: &CompletionRecord,
        node: &WorkflowDagNode,
        loop_ast: &proto::LoopAst,
        dispatch: &mut proto::WorkflowNodeDispatch,
        row: &LoopActionRow,
    ) -> Result<Option<CompletionRecord>> {
        let node_id = node.id.clone();
        let control = match record.control.as_ref().and_then(|ctrl| ctrl.kind.as_ref()) {
            Some(proto::workflow_node_control::Kind::Loop(ctrl)) => Some(ctrl),
            _ => None,
        };
        let ctx = eval_context_from_dispatch(dispatch)?;
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
            let next_eval = evaluate_loop_iteration(node, &next_ctx)?;
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
        let next_eval = evaluate_loop_iteration(node, &next_ctx)?;
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

    /// Handle completion of a phase within a multi-action loop iteration
    async fn process_multi_action_loop_completion_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        record: &CompletionRecord,
        node: &WorkflowDagNode,
        loop_ast: &proto::LoopAst,
        dispatch: &proto::WorkflowNodeDispatch,
        row: &LoopActionRow,
    ) -> Result<Option<CompletionRecord>> {
        let node_id = node.id.clone();
        let ctx = eval_context_from_dispatch(dispatch)?;

        // Get current multi-action loop state
        // For now, use the single accumulator from loop_ast (will be deprecated)
        // TODO: Switch to loop_head_meta.accumulators when available
        let accumulator_names = vec![loop_ast.accumulator.clone()];
        let mut state = extract_multi_action_loop_state(&ctx, &accumulator_names);

        // Extract current phase info from context
        let current_phase_id = ctx
            .get("__current_phase_id")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_default();
        let current_output_var = ctx
            .get("__current_phase_output_var")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_default();

        // Record phase completion and extract result
        let iteration_args = decode_arguments(&record.result_payload, "phase result")?;
        let decoded = decode_payload(&iteration_args)?;
        if let Some(result_value) = decoded.result {
            state
                .phase_results
                .insert(current_output_var.clone(), result_value);
        }
        state.completed_phases.insert(current_phase_id.clone());

        // Check if there's another phase ready
        let next_phase = find_next_ready_phase(node, &ctx, &state)?;

        if let Some(next_phase) = next_phase {
            // Dispatch next phase
            let mut new_dispatch = dispatch.clone();

            // Update phase tracking in context
            let phases_value = Value::Array(
                state
                    .completed_phases
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            );
            let phases_payload = encode_value_result(&phases_value)?;

            let results_value = Value::Object(
                state
                    .phase_results
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            let results_payload = encode_value_result(&results_value)?;

            let phase_id_payload =
                encode_value_result(&Value::String(next_phase.phase_id.clone()))?;
            let output_var_payload =
                encode_value_result(&Value::String(next_phase.output_var.clone()))?;

            // Remove old phase context entries
            new_dispatch.context.retain(|ctx| {
                ctx.variable != LOOP_PHASE_VAR
                    && ctx.variable != LOOP_PHASE_RESULTS_VAR
                    && ctx.variable != LOOP_PREAMBLE_RESULTS_VAR
                    && ctx.variable != "__current_phase_id"
                    && ctx.variable != "__current_phase_output_var"
            });

            // Get preamble results - either from the phase evaluation (first phase) or state (subsequent)
            let preamble_results_value = if let Some(preamble_vars) = &next_phase.preamble_results {
                Value::Object(
                    preamble_vars
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                )
            } else {
                Value::Object(
                    state
                        .preamble_results
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                )
            };
            let preamble_results_payload = encode_value_result(&preamble_results_value)?;

            // Add updated phase context
            new_dispatch.context.push(WorkflowNodeContext {
                variable: LOOP_PHASE_VAR.to_string(),
                payload: Some(phases_payload),
                workflow_node_id: node_id.clone(),
            });
            new_dispatch.context.push(WorkflowNodeContext {
                variable: LOOP_PHASE_RESULTS_VAR.to_string(),
                payload: Some(results_payload),
                workflow_node_id: node_id.clone(),
            });
            new_dispatch.context.push(WorkflowNodeContext {
                variable: LOOP_PREAMBLE_RESULTS_VAR.to_string(),
                payload: Some(preamble_results_payload),
                workflow_node_id: node_id.clone(),
            });
            new_dispatch.context.push(WorkflowNodeContext {
                variable: "__current_phase_id".to_string(),
                payload: Some(phase_id_payload),
                workflow_node_id: node_id.clone(),
            });
            new_dispatch.context.push(WorkflowNodeContext {
                variable: "__current_phase_output_var".to_string(),
                payload: Some(output_var_payload),
                workflow_node_id: node_id.clone(),
            });

            // Update action/module for next phase
            if let Some(node_mut) = new_dispatch.node.as_mut() {
                node_mut.action = next_phase.action.clone();
                if !next_phase.module.is_empty() {
                    node_mut.module = next_phase.module.clone();
                }
            }

            // Update resolved kwargs
            new_dispatch.resolved_kwargs = Some(values_to_arguments(&next_phase.kwargs)?);

            let dispatch_bytes = new_dispatch.encode_to_vec();
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

        // No more phases - iteration is complete
        // Extract result and add to accumulator(s)
        let body_graph = loop_ast.body_graph.as_ref().context("missing body graph")?;

        // For now, we use the single result_variable and single accumulator
        // In the future, this should handle multiple accumulators with source expressions
        let result_value = state
            .phase_results
            .get(&body_graph.result_variable)
            .cloned();
        if let Some(val) = result_value {
            // Push to the first (and currently only) accumulator
            if let Some(acc_name) = accumulator_names.first() {
                state
                    .accumulators
                    .entry(acc_name.clone())
                    .or_default()
                    .push(val);
            }
        }

        // Check if there are more iterations
        let iterable = loop_ast.iterable.as_ref().context("missing iterable")?;
        let iterable_value = eval_expr(iterable, &ctx)?;
        let items_len = match iterable_value {
            Value::Array(arr) => arr.len(),
            _ => 0,
        };

        if state.current_index + 1 < items_len {
            // More iterations - reset phase state and dispatch first phase of next iteration
            let next_index = state.current_index + 1;
            let next_state = MultiActionLoopState {
                current_index: next_index,
                accumulators: state.accumulators.clone(),
                completed_phases: HashSet::new(),
                phase_results: HashMap::new(),
                preamble_results: HashMap::new(),
            };

            let mut next_ctx = ctx.clone();
            next_ctx.insert(
                LOOP_INDEX_VAR.to_string(),
                Value::Number(serde_json::Number::from(next_index as u64)),
            );

            // Insert all accumulators into context
            for (acc_name, acc_values) in &state.accumulators {
                next_ctx.insert(acc_name.clone(), Value::Array(acc_values.clone()));
            }
            next_ctx.remove(LOOP_PHASE_VAR);
            next_ctx.remove(LOOP_PHASE_RESULTS_VAR);
            next_ctx.remove(LOOP_PREAMBLE_RESULTS_VAR);

            let next_phase = find_next_ready_phase(node, &next_ctx, &next_state)?;
            if let Some(next_phase) = next_phase {
                let mut new_dispatch = dispatch.clone();

                // Update all context entries for new iteration
                let index_payload = encode_value_result(&Value::Number(serde_json::Number::from(
                    next_index as u64,
                )))?;
                let phases_payload = encode_value_result(&Value::Array(vec![]))?;
                let results_payload = encode_value_result(&Value::Object(serde_json::Map::new()))?;
                let phase_id_payload =
                    encode_value_result(&Value::String(next_phase.phase_id.clone()))?;
                let output_var_payload =
                    encode_value_result(&Value::String(next_phase.output_var.clone()))?;

                // Remove old accumulator and phase context entries
                let acc_names_set: std::collections::HashSet<String> =
                    accumulator_names.iter().cloned().collect();
                new_dispatch.context.retain(|ctx| {
                    !acc_names_set.contains(&ctx.variable)
                        && ctx.variable != LOOP_INDEX_VAR
                        && ctx.variable != LOOP_PHASE_VAR
                        && ctx.variable != LOOP_PHASE_RESULTS_VAR
                        && ctx.variable != LOOP_PREAMBLE_RESULTS_VAR
                        && ctx.variable != "__current_phase_id"
                        && ctx.variable != "__current_phase_output_var"
                });

                // Get preamble results from the new iteration's first phase
                let preamble_results_value =
                    if let Some(preamble_vars) = &next_phase.preamble_results {
                        Value::Object(
                            preamble_vars
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        )
                    } else {
                        Value::Object(serde_json::Map::new())
                    };
                let preamble_results_payload = encode_value_result(&preamble_results_value)?;

                // Push all accumulators to context
                for (acc_name, acc_values) in &state.accumulators {
                    let accumulator_payload =
                        encode_value_result(&Value::Array(acc_values.clone()))?;
                    new_dispatch.context.push(WorkflowNodeContext {
                        variable: acc_name.clone(),
                        payload: Some(accumulator_payload),
                        workflow_node_id: node_id.clone(),
                    });
                }

                new_dispatch.context.push(WorkflowNodeContext {
                    variable: LOOP_INDEX_VAR.to_string(),
                    payload: Some(index_payload),
                    workflow_node_id: node_id.clone(),
                });
                new_dispatch.context.push(WorkflowNodeContext {
                    variable: LOOP_PHASE_VAR.to_string(),
                    payload: Some(phases_payload),
                    workflow_node_id: node_id.clone(),
                });
                new_dispatch.context.push(WorkflowNodeContext {
                    variable: LOOP_PHASE_RESULTS_VAR.to_string(),
                    payload: Some(results_payload),
                    workflow_node_id: node_id.clone(),
                });
                new_dispatch.context.push(WorkflowNodeContext {
                    variable: LOOP_PREAMBLE_RESULTS_VAR.to_string(),
                    payload: Some(preamble_results_payload),
                    workflow_node_id: node_id.clone(),
                });
                new_dispatch.context.push(WorkflowNodeContext {
                    variable: "__current_phase_id".to_string(),
                    payload: Some(phase_id_payload),
                    workflow_node_id: node_id.clone(),
                });
                new_dispatch.context.push(WorkflowNodeContext {
                    variable: "__current_phase_output_var".to_string(),
                    payload: Some(output_var_payload),
                    workflow_node_id: node_id.clone(),
                });

                if let Some(node_mut) = new_dispatch.node.as_mut() {
                    node_mut.action = next_phase.action.clone();
                    if !next_phase.module.is_empty() {
                        node_mut.module = next_phase.module.clone();
                    }
                }

                new_dispatch.resolved_kwargs = Some(values_to_arguments(&next_phase.kwargs)?);

                let dispatch_bytes = new_dispatch.encode_to_vec();
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
        }

        // All iterations done - return final result
        // For now, return the first (and currently only) accumulator
        // TODO: Return all accumulators when multi-accumulator support is fully implemented
        let final_accumulator = accumulator_names
            .first()
            .and_then(|name| state.accumulators.get(name))
            .cloned()
            .unwrap_or_default();
        let final_payload = encode_value_result(&Value::Array(final_accumulator))?;
        let mut updated = record.clone();
        updated.result_payload = final_payload.encode_to_vec();
        Ok(Some(updated))
    }

    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::connect_with_pool_size(database_url, 10).await
    }

    pub async fn connect_with_pool_size(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run migrations")?;
        Ok(Self {
            pool,
            version_cache: Arc::new(RwLock::new(WorkflowVersionCache::new(64))),
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get a cached workflow version, or fetch from DB and cache it.
    /// This avoids repeated DB lookups for the same workflow version during scheduling.
    async fn get_cached_version(
        &self,
        version_id: WorkflowVersionId,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<CachedWorkflowVersion> {
        // Try to get from cache first (read lock)
        {
            let cache = self.version_cache.read().await;
            if let Some(cached) = cache.get(&version_id) {
                return Ok(cached.clone());
            }
        }

        // Not in cache, fetch from DB
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

        let cached = CachedWorkflowVersion {
            concurrent: version.concurrent,
            dag,
            node_map,
        };

        // Store in cache (write lock)
        {
            let mut cache = self.version_cache.write().await;
            cache.insert(version_id, cached.clone());
        }

        Ok(cached)
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
        // Calculate backoff delay based on backoff_kind:
        // - 'none': no delay (immediate retry)
        // - 'linear': delay = base_delay_ms * (attempt_number + 1)
        // - 'exponential': delay = base_delay_ms * multiplier^attempt_number
        // Note: attempt_number is the current attempt (0-indexed), so after increment
        // it becomes the next attempt number which we use for backoff calculation.
        let result = sqlx::query(
            r#"
            UPDATE daemon_action_ledger
            SET status = 'queued',
                attempt_number = attempt_number + 1,
                dispatched_at = NULL,
                acked_at = NULL,
                deadline_at = NULL,
                delivery_id = NULL,
                scheduled_at = NOW() + (
                    CASE backoff_kind
                        WHEN 'linear' THEN
                            make_interval(secs => (backoff_base_delay_ms * (attempt_number + 1))::double precision / 1000.0)
                        WHEN 'exponential' THEN
                            make_interval(secs => (backoff_base_delay_ms * power(backoff_multiplier, LEAST(attempt_number, 30)))::double precision / 1000.0)
                        ELSE
                            interval '0 seconds'
                    END
                ),
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
        // Calculate backoff delay based on backoff_kind (same as failure retries)
        let result = sqlx::query(
            r#"
            UPDATE daemon_action_ledger
            SET status = 'queued',
                attempt_number = attempt_number + 1,
                dispatched_at = NULL,
                acked_at = NULL,
                deadline_at = NULL,
                delivery_id = NULL,
                scheduled_at = NOW() + (
                    CASE backoff_kind
                        WHEN 'linear' THEN
                            make_interval(secs => (backoff_base_delay_ms * (attempt_number + 1))::double precision / 1000.0)
                        WHEN 'exponential' THEN
                            make_interval(secs => (backoff_base_delay_ms * power(backoff_multiplier, LEAST(attempt_number, 30)))::double precision / 1000.0)
                        ELSE
                            interval '0 seconds'
                    END
                ),
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
                    retry_kind,
                    backoff_kind,
                    backoff_base_delay_ms,
                    backoff_multiplier
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
                    'failure',
                    $10,
                    $11,
                    $12
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
            .bind(BackoffConfig::None.kind_str())
            .bind(0i32) // no backoff delay
            .bind(DEFAULT_EXPONENTIAL_MULTIPLIER) // default multiplier
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
        let partition_id = 0i32;
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
        .bind(partition_id)
        .bind(workflow_name)
        .bind(workflow_version_id)
        .bind(input_payload)
        .fetch_one(&mut *tx)
        .await?;

        // Parse workflow input for scheduling
        let workflow_input_bytes = input_payload.map(|b| b.to_vec()).unwrap_or_default();
        let workflow_input = decode_arguments(&workflow_input_bytes, "workflow input")?;

        // Use push-based scheduling initialization
        self.initialize_push_scheduling_tx(
            &mut tx,
            instance_id,
            partition_id,
            workflow_version_id,
            &workflow_input,
        )
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

    /// Initialize push-based scheduling state for a new workflow instance.
    /// This populates node_ready_state with dependency counts and queues root nodes.
    /// O(n) at creation time where n = number of DAG nodes, but O(1) per completion.
    async fn initialize_push_scheduling_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instance_id: WorkflowInstanceId,
        partition_id: i32,
        version_id: WorkflowVersionId,
        workflow_input: &WorkflowArguments,
    ) -> Result<usize> {
        // Get cached workflow version
        let cached_version = self.get_cached_version(version_id, tx).await?;
        let dag = &cached_version.dag;
        let concurrent = cached_version.concurrent;

        // Initialize instance_eval_context with workflow input
        let input_json = arguments_to_json(workflow_input)?;
        let mut initial_context = input_json.clone();
        initial_context.insert(
            "__workflow_exceptions".to_string(),
            Value::Object(serde_json::Map::new()),
        );
        sqlx::query(
            r#"
            INSERT INTO instance_eval_context (instance_id, context_json, exceptions_json)
            VALUES ($1, $2, '{}')
            "#,
        )
        .bind(instance_id)
        .bind(serde_json::to_value(&initial_context)?)
        .execute(tx.as_mut())
        .await?;

        // Initialize node_ready_state for all nodes
        let mut node_ids: Vec<String> = Vec::with_capacity(dag.nodes.len());
        let mut deps_required_list: Vec<i16> = Vec::with_capacity(dag.nodes.len());
        for node in &dag.nodes {
            node_ids.push(node.id.clone());
            deps_required_list.push(compute_deps_required(node, concurrent));
        }

        // Batch insert all node ready states
        sqlx::query(
            r#"
            INSERT INTO node_ready_state (instance_id, node_id, deps_required, deps_satisfied, is_queued, is_completed)
            SELECT $1, node_id, deps_required, 0, FALSE, FALSE
            FROM UNNEST($2::TEXT[], $3::SMALLINT[]) AS t(node_id, deps_required)
            "#,
        )
        .bind(instance_id)
        .bind(&node_ids)
        .bind(&deps_required_list)
        .execute(tx.as_mut())
        .await?;

        // Find and queue root nodes (nodes with deps_required = 0)
        let root_nodes: Vec<&WorkflowDagNode> = dag
            .nodes
            .iter()
            .filter(|n| compute_deps_required(n, concurrent) == 0)
            .collect();

        let mut queued_count = 0;
        let mut next_seq = 0i32;

        for node in root_nodes {
            // Build dispatch for root node - use initial_context which includes __workflow_exceptions
            let eval_ctx = initial_context.clone();

            // Check guard (root nodes have no dependencies to fail)
            let guard_passes = guard_allows(node, &eval_ctx)?;
            if !guard_passes {
                // Insert synthetic completion into action ledger
                let payload = build_null_payload(node)?;
                insert_synthetic_completion_simple_tx(
                    tx,
                    instance_id,
                    partition_id,
                    node,
                    workflow_input,
                    next_seq,
                    payload.clone(),
                    true,
                )
                .await?;

                // Mark as queued in node_ready_state
                sqlx::query(
                    "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                )
                .bind(instance_id)
                .bind(&node.id)
                .execute(tx.as_mut())
                .await?;

                // Process downstream nodes via complete_node_push_tx
                self.complete_node_push_tx(
                    tx,
                    instance_id,
                    partition_id,
                    &node.id,
                    next_seq,
                    payload,
                    true,
                    dag,
                    concurrent,
                    workflow_input,
                )
                .await?;
                next_seq += 1;
                continue;
            }

            // Check if this is a loop node (but not a multi-action loop - those are handled by legacy scheduler)
            let loop_ast = node.ast.as_ref().and_then(|ast| ast.r#loop.as_ref());

            if let Some(loop_ast) = loop_ast {
                // Handle multi-action loops
                if has_multi_action_body_graph(node) {
                    // Multi-action loop - dispatch first phase
                    let accumulator_names = vec![loop_ast.accumulator.clone()];
                    let state = extract_multi_action_loop_state(&eval_ctx, &accumulator_names);
                    let phase_eval = find_next_ready_phase(node, &eval_ctx, &state)?;

                    if let Some(phase_eval) = phase_eval {
                        // Build contexts for the phase
                        let mut contexts = Vec::new();

                        // Add all accumulators to context
                        for (acc_name, acc_values) in &state.accumulators {
                            let accumulator_value = Value::Array(acc_values.clone());
                            let accumulator_payload = encode_value_result(&accumulator_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: acc_name.clone(),
                                payload: Some(accumulator_payload),
                                workflow_node_id: node.id.clone(),
                            });
                        }

                        // Add loop index to context
                        let index_value =
                            Value::Number(serde_json::Number::from(state.current_index as u64));
                        let index_payload = encode_value_result(&index_value)?;
                        contexts.push(WorkflowNodeContext {
                            variable: LOOP_INDEX_VAR.to_string(),
                            payload: Some(index_payload),
                            workflow_node_id: node.id.clone(),
                        });

                        // Add completed phases to context
                        let phases_value = Value::Array(
                            state
                                .completed_phases
                                .iter()
                                .map(|s| Value::String(s.clone()))
                                .collect(),
                        );
                        let phases_payload = encode_value_result(&phases_value)?;
                        contexts.push(WorkflowNodeContext {
                            variable: LOOP_PHASE_VAR.to_string(),
                            payload: Some(phases_payload),
                            workflow_node_id: node.id.clone(),
                        });

                        // Add phase results to context
                        let results_value = Value::Object(
                            state
                                .phase_results
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        );
                        let results_payload = encode_value_result(&results_value)?;
                        contexts.push(WorkflowNodeContext {
                            variable: LOOP_PHASE_RESULTS_VAR.to_string(),
                            payload: Some(results_payload),
                            workflow_node_id: node.id.clone(),
                        });

                        // Add preamble results to context
                        let preamble_results_value =
                            if let Some(preamble_vars) = &phase_eval.preamble_results {
                                Value::Object(
                                    preamble_vars
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                )
                            } else {
                                Value::Object(
                                    state
                                        .preamble_results
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                )
                            };
                        let preamble_results_payload =
                            encode_value_result(&preamble_results_value)?;
                        contexts.push(WorkflowNodeContext {
                            variable: LOOP_PREAMBLE_RESULTS_VAR.to_string(),
                            payload: Some(preamble_results_payload),
                            workflow_node_id: node.id.clone(),
                        });

                        // Add current phase info
                        let phase_id_payload =
                            encode_value_result(&Value::String(phase_eval.phase_id.clone()))?;
                        contexts.push(WorkflowNodeContext {
                            variable: "__current_phase_id".to_string(),
                            payload: Some(phase_id_payload),
                            workflow_node_id: node.id.clone(),
                        });
                        let output_var_payload =
                            encode_value_result(&Value::String(phase_eval.output_var.clone()))?;
                        contexts.push(WorkflowNodeContext {
                            variable: "__current_phase_output_var".to_string(),
                            payload: Some(output_var_payload),
                            workflow_node_id: node.id.clone(),
                        });

                        // Create dispatch node with phase action
                        let mut dispatch_node = node.clone();
                        dispatch_node.action = phase_eval.action.clone();
                        if !phase_eval.module.is_empty() {
                            dispatch_node.module = phase_eval.module.clone();
                        }

                        let queued = build_action_dispatch(
                            dispatch_node,
                            workflow_input,
                            contexts,
                            phase_eval.kwargs,
                        )?;
                        let dispatch_bytes = queued.dispatch.encode_to_vec();
                        let scheduled_at = compute_scheduled_at(node, &eval_ctx)?;

                        sqlx::query(
                            r#"
                            INSERT INTO daemon_action_ledger (
                                instance_id, partition_id, action_seq, status,
                                module, function_name, dispatch_payload, workflow_node_id,
                                timeout_seconds, max_retries, timeout_retry_limit,
                                attempt_number, scheduled_at, retry_kind,
                                backoff_kind, backoff_base_delay_ms, backoff_multiplier
                            ) VALUES (
                                $1, $2, $3, 'queued',
                                $4, $5, $6, $7,
                                $8, $9, $10,
                                0, $11, 'failure',
                                $12, $13, $14
                            )
                            "#,
                        )
                        .bind(instance_id)
                        .bind(partition_id)
                        .bind(next_seq)
                        .bind(&queued.module)
                        .bind(&queued.function_name)
                        .bind(&dispatch_bytes)
                        .bind(&node.id)
                        .bind(queued.timeout_seconds)
                        .bind(queued.max_retries)
                        .bind(queued.timeout_retry_limit)
                        .bind(scheduled_at)
                        .bind(queued.backoff.kind_str())
                        .bind(queued.backoff.base_delay_ms())
                        .bind(queued.backoff.multiplier())
                        .execute(tx.as_mut())
                        .await?;

                        // Mark as queued (but NOT completed - loop may have more iterations)
                        sqlx::query(
                            "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                        )
                        .bind(instance_id)
                        .bind(&node.id)
                        .execute(tx.as_mut())
                        .await?;

                        queued_count += 1;
                        next_seq += 1;
                    } else {
                        // Multi-action loop has no iterations (empty iterable) - insert synthetic completion
                        let accumulator_value =
                            Value::Array(extract_accumulator(&eval_ctx, &loop_ast.accumulator));
                        let payload = encode_value_result(&accumulator_value)?;

                        insert_synthetic_completion_simple_tx(
                            tx,
                            instance_id,
                            partition_id,
                            node,
                            workflow_input,
                            next_seq,
                            Some(payload.clone()),
                            true,
                        )
                        .await?;

                        // Mark as queued and process downstream
                        sqlx::query(
                            "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                        )
                        .bind(instance_id)
                        .bind(&node.id)
                        .execute(tx.as_mut())
                        .await?;

                        // Process downstream nodes via complete_node_push_tx
                        self.complete_node_push_tx(
                            tx,
                            instance_id,
                            partition_id,
                            &node.id,
                            next_seq,
                            Some(payload),
                            true,
                            dag,
                            concurrent,
                            workflow_input,
                        )
                        .await?;
                        next_seq += 1;
                    }
                    continue;
                }

                // Handle simple loop node - dispatch body action, not the loop itself
                let loop_eval = evaluate_loop_iteration(node, &eval_ctx)?;

                if let Some(loop_eval) = loop_eval {
                    // There's an iteration - dispatch the body action
                    let mut contexts = Vec::new();
                    let accumulator_value = Value::Array(loop_eval.accumulator.clone());
                    let accumulator_payload = encode_value_result(&accumulator_value)?;
                    contexts.push(WorkflowNodeContext {
                        variable: loop_ast.accumulator.clone(),
                        payload: Some(accumulator_payload),
                        workflow_node_id: node.id.clone(),
                    });

                    let index_value =
                        Value::Number(serde_json::Number::from(loop_eval.current_index as u64));
                    let index_payload = encode_value_result(&index_value)?;
                    contexts.push(WorkflowNodeContext {
                        variable: LOOP_INDEX_VAR.to_string(),
                        payload: Some(index_payload),
                        workflow_node_id: node.id.clone(),
                    });

                    // Create dispatch node with body action
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
                        workflow_input,
                        contexts,
                        loop_eval.kwargs,
                    )?;
                    let dispatch_bytes = queued.dispatch.encode_to_vec();
                    let scheduled_at = compute_scheduled_at(node, &eval_ctx)?;

                    sqlx::query(
                        r#"
                        INSERT INTO daemon_action_ledger (
                            instance_id, partition_id, action_seq, status,
                            module, function_name, dispatch_payload, workflow_node_id,
                            timeout_seconds, max_retries, timeout_retry_limit,
                            attempt_number, scheduled_at, retry_kind,
                            backoff_kind, backoff_base_delay_ms, backoff_multiplier
                        ) VALUES (
                            $1, $2, $3, 'queued',
                            $4, $5, $6, $7,
                            $8, $9, $10,
                            0, $11, 'failure',
                            $12, $13, $14
                        )
                        "#,
                    )
                    .bind(instance_id)
                    .bind(partition_id)
                    .bind(next_seq)
                    .bind(&queued.module)
                    .bind(&queued.function_name)
                    .bind(&dispatch_bytes)
                    .bind(&node.id)
                    .bind(queued.timeout_seconds)
                    .bind(queued.max_retries)
                    .bind(queued.timeout_retry_limit)
                    .bind(scheduled_at)
                    .bind(queued.backoff.kind_str())
                    .bind(queued.backoff.base_delay_ms())
                    .bind(queued.backoff.multiplier())
                    .execute(tx.as_mut())
                    .await?;

                    // Mark as queued (but NOT completed - loop may have more iterations)
                    sqlx::query(
                        "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                    )
                    .bind(instance_id)
                    .bind(&node.id)
                    .execute(tx.as_mut())
                    .await?;

                    queued_count += 1;
                    next_seq += 1;
                } else {
                    // Loop has no iterations (empty iterable) - insert synthetic completion
                    let accumulator_value =
                        Value::Array(extract_accumulator(&eval_ctx, &loop_ast.accumulator));
                    let payload = encode_value_result(&accumulator_value)?;

                    insert_synthetic_completion_simple_tx(
                        tx,
                        instance_id,
                        partition_id,
                        node,
                        workflow_input,
                        next_seq,
                        Some(payload.clone()),
                        true,
                    )
                    .await?;

                    // Mark as queued and process downstream
                    sqlx::query(
                        "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                    )
                    .bind(instance_id)
                    .bind(&node.id)
                    .execute(tx.as_mut())
                    .await?;

                    // Process downstream nodes via complete_node_push_tx
                    self.complete_node_push_tx(
                        tx,
                        instance_id,
                        partition_id,
                        &node.id,
                        next_seq,
                        Some(payload),
                        true,
                        dag,
                        concurrent,
                        workflow_input,
                    )
                    .await?;
                    next_seq += 1;
                }
                continue;
            }

            // Queue the root node action (non-loop nodes)
            let contexts = Vec::new(); // Root nodes have no dependencies
            let resolved_kwargs = evaluate_kwargs(node, &eval_ctx)?;
            let queued =
                build_action_dispatch(node.clone(), workflow_input, contexts, resolved_kwargs)?;
            let dispatch_bytes = queued.dispatch.encode_to_vec();
            let scheduled_at = compute_scheduled_at(node, &eval_ctx)?;

            sqlx::query(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id, partition_id, action_seq, status,
                    module, function_name, dispatch_payload, workflow_node_id,
                    timeout_seconds, max_retries, timeout_retry_limit,
                    attempt_number, scheduled_at, retry_kind,
                    backoff_kind, backoff_base_delay_ms, backoff_multiplier
                ) VALUES (
                    $1, $2, $3, 'queued',
                    $4, $5, $6, $7,
                    $8, $9, $10,
                    0, $11, 'failure',
                    $12, $13, $14
                )
                "#,
            )
            .bind(instance_id)
            .bind(partition_id)
            .bind(next_seq)
            .bind(&queued.module)
            .bind(&queued.function_name)
            .bind(&dispatch_bytes)
            .bind(&node.id)
            .bind(queued.timeout_seconds)
            .bind(queued.max_retries)
            .bind(queued.timeout_retry_limit)
            .bind(scheduled_at)
            .bind(queued.backoff.kind_str())
            .bind(queued.backoff.base_delay_ms())
            .bind(queued.backoff.multiplier())
            .execute(tx.as_mut())
            .await?;

            // Mark as queued in node_ready_state
            sqlx::query(
                "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
            )
            .bind(instance_id)
            .bind(&node.id)
            .execute(tx.as_mut())
            .await?;

            queued_count += 1;
            next_seq += 1;
        }

        // Update next_action_seq
        if next_seq > 0 {
            sqlx::query("UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1")
                .bind(instance_id)
                .bind(next_seq)
                .execute(tx.as_mut())
                .await?;
        }

        debug!(
            instance_id = %instance_id,
            total_nodes = dag.nodes.len(),
            root_nodes_queued = queued_count,
            "initialized push-based scheduling"
        );

        Ok(queued_count)
    }

    /// Complete a node and propagate to downstream nodes using O(1) push logic.
    /// Uses a work queue to avoid recursion when synthetic completions trigger more completions.
    #[allow(clippy::too_many_arguments)]
    async fn complete_node_push_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instance_id: WorkflowInstanceId,
        partition_id: i32,
        initial_node_id: &str,
        _action_seq: i32,
        initial_payload: Option<WorkflowArguments>,
        initial_success: bool,
        dag: &WorkflowDagDefinition,
        concurrent: bool,
        workflow_input: &WorkflowArguments,
    ) -> Result<usize> {
        // Work queue to avoid recursion
        let mut work_queue: Vec<PendingCompletion> = vec![PendingCompletion {
            node_id: initial_node_id.to_string(),
            payload: initial_payload,
            success: initial_success,
        }];

        let mut total_queued = 0;

        // Get current next_action_seq
        let mut next_seq: i32 = sqlx::query_scalar(
            "SELECT next_action_seq FROM workflow_instances WHERE id = $1 FOR UPDATE",
        )
        .bind(instance_id)
        .fetch_one(tx.as_mut())
        .await?;

        while let Some(completion) = work_queue.pop() {
            let node = dag
                .nodes
                .iter()
                .find(|n| n.id == completion.node_id)
                .context("node not found in dag")?;

            // Mark node as completed in node_ready_state
            sqlx::query(
                "UPDATE node_ready_state SET is_completed = TRUE WHERE instance_id = $1 AND node_id = $2",
            )
            .bind(instance_id)
            .bind(&completion.node_id)
            .execute(tx.as_mut())
            .await?;

            // Update instance_eval_context with results
            if let Some(ref arguments) = completion.payload {
                let decoded = decode_payload(arguments)?;
                if completion.success {
                    let vars = extract_variables_from_result(node, &decoded);
                    if !vars.is_empty() {
                        // Update context_json with new variables
                        let vars_json = serde_json::to_value(&vars)?;
                        sqlx::query(
                            r#"
                            UPDATE instance_eval_context
                            SET context_json = context_json || $2
                            WHERE instance_id = $1
                            "#,
                        )
                        .bind(instance_id)
                        .bind(vars_json)
                        .execute(tx.as_mut())
                        .await?;
                    }
                } else if let Some(error) = decoded.error.as_ref() {
                    // Update exceptions_json
                    let exception_update = serde_json::json!({ &completion.node_id: error });
                    sqlx::query(
                        r#"
                        UPDATE instance_eval_context
                        SET exceptions_json = exceptions_json || $2,
                            context_json = jsonb_set(context_json, '{__workflow_exceptions}', COALESCE(context_json->'__workflow_exceptions', '{}'::jsonb) || $2)
                        WHERE instance_id = $1
                        "#,
                    )
                    .bind(instance_id)
                    .bind(exception_update)
                    .execute(tx.as_mut())
                    .await?;
                }
            }

            // Handle __conditional_join nodes: forward ALL values from the branch that executed
            // The join depends on all branch ends but only one executes. We need to forward
            // all variables from whichever branch completed (stored in pending_context).
            // This includes both the target variable (like `result`) and any other variables
            // set in the branch postamble (like `branch`).
            if node.action == "__conditional_join" {
                // Get the pending context for this join node - it should have all variables from the executed branch
                let pending_ctx: Vec<(String, Option<Vec<u8>>)> = sqlx::query_as(
                    "SELECT variable, payload FROM node_pending_context WHERE instance_id = $1 AND node_id = $2",
                )
                .bind(instance_id)
                .bind(&completion.node_id)
                .fetch_all(tx.as_mut())
                .await?;

                // Process ALL variables in the pending context
                for (variable, payload_opt) in pending_ctx {
                    if variable.is_empty() {
                        continue;
                    }
                    if let Some(payload_bytes) = payload_opt {
                        if let Ok(arguments) = decode_arguments(&payload_bytes, "conditional join context") {
                            if let Ok(decoded) = decode_payload(&arguments) {
                                // Extract the actual value from the branch result
                                if let Some(result) = decoded.result.as_ref() {
                                    let value = match variable_map_from_value(result) {
                                        Some(VariableMapSource::WorkflowNodeResult(map)) => {
                                            map.get(&variable).cloned()
                                        }
                                        _ => Some(result.clone())
                                    };
                                    if let Some(v) = value {
                                        let update_json = serde_json::json!({ &variable: v });
                                        sqlx::query(
                                            r#"
                                            UPDATE instance_eval_context
                                            SET context_json = context_json || $2
                                            WHERE instance_id = $1
                                            "#,
                                        )
                                        .bind(instance_id)
                                        .bind(update_json)
                                        .execute(tx.as_mut())
                                        .await?;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Handle __gather_sync nodes: collect all item* variables and combine into array
            if node.action == "__gather_sync" && !node.produces.is_empty() {
                let target_var = &node.produces[0];
                // Get current context to find all {target}__item* variables
                let ctx_json: Option<Value> = sqlx::query_scalar(
                    "SELECT context_json FROM instance_eval_context WHERE instance_id = $1",
                )
                .bind(instance_id)
                .fetch_optional(tx.as_mut())
                .await?;

                if let Some(ctx) = ctx_json {
                    if let Some(ctx_obj) = ctx.as_object() {
                        // Find all {target}__item* variables and collect them in order
                        let prefix = format!("{}__item", target_var);
                        let mut items: Vec<(usize, &Value)> = ctx_obj
                            .iter()
                            .filter_map(|(k, v)| {
                                if k.starts_with(&prefix) {
                                    // Extract the index from {target}__item{index}
                                    k[prefix.len()..].parse::<usize>().ok().map(|idx| (idx, v))
                                } else {
                                    None
                                }
                            })
                            .collect();

                        // Sort by index to maintain order
                        items.sort_by_key(|(idx, _)| *idx);

                        // Create array from values
                        let array: Vec<Value> = items.into_iter().map(|(_, v)| v.clone()).collect();

                        if !array.is_empty() {
                            // Store the combined array in eval_context
                            let update_json = serde_json::json!({ target_var: array });
                            sqlx::query(
                                r#"
                                UPDATE instance_eval_context
                                SET context_json = context_json || $2
                                WHERE instance_id = $1
                                "#,
                            )
                            .bind(instance_id)
                            .bind(update_json)
                            .execute(tx.as_mut())
                            .await?;
                        }
                    }
                }
            }

            // Handle back edges: if this node is a back edge source (loop body tail),
            // we need to update loop state and re-enable the loop_head for next iteration
            if let Some(loop_head_id) = get_back_edge_target(&completion.node_id, dag)
                && let Some(loop_head_node) = dag.nodes.iter().find(|n| n.id == loop_head_id)
                && let Some(loop_meta) = loop_head_node.loop_head_meta.as_ref()
            {
                // Update loop state: increment index, append to accumulators
                // First, get the result from which to extract accumulator values
                let body_result = completion
                    .payload
                    .as_ref()
                    .and_then(|p| decode_payload(p).ok())
                    .and_then(|d| d.result)
                    .unwrap_or(Value::Null);

                // Get current loop state
                let loop_state: Option<(i32, Option<Vec<u8>>)> = sqlx::query_as(
                    r#"
                    SELECT current_index, accumulators
                    FROM loop_iteration_state
                    WHERE instance_id = $1 AND node_id = $2
                    "#,
                )
                .bind(instance_id)
                .bind(loop_head_id)
                .fetch_optional(tx.as_mut())
                .await?;

                let (current_index, accumulators_bytes) = loop_state.unwrap_or((0, None));

                // Parse accumulators map: { "var_name" -> [values...] }
                let mut accumulators: HashMap<String, Vec<Value>> = accumulators_bytes
                    .as_ref()
                    .and_then(|b| serde_json::from_slice(b).ok())
                    .unwrap_or_default();

                // Create a context with the body result for evaluating source expressions
                let mut eval_ctx = EvalContext::new();
                // The body action produces output in a variable - for multi-action loops,
                // we store intermediate phase results in phase_results. The last phase's
                // result is available under the variable name it produces.
                // For single-action multi-accumulator loops, the result is from the single action.

                // Get current eval context to have access to phase results
                let ctx_value: Option<Value> = sqlx::query_scalar(
                    "SELECT context_json FROM instance_eval_context WHERE instance_id = $1",
                )
                .bind(instance_id)
                .fetch_optional(tx.as_mut())
                .await?;

                if let Some(Value::Object(map)) = ctx_value {
                    for (k, v) in map {
                        eval_ctx.insert(k, v);
                    }
                }

                // For each accumulator, evaluate its source expression to get the value
                for acc in &loop_meta.accumulators {
                    let value = if let Some(source_expr) = &acc.source_expr {
                        // Evaluate expression like "processed['result']" or just "result"
                        // Handle patterns:
                        //   "var_name" -> look up var_name in context
                        //   "var_name['key']" -> look up var_name in context, then subscript
                        evaluate_source_expr(source_expr, &eval_ctx).unwrap_or(body_result.clone())
                    } else {
                        // No source expression - use body result directly
                        body_result.clone()
                    };

                    // Get or create the accumulator array and append
                    let acc_vec = accumulators.entry(acc.var.clone()).or_default();
                    acc_vec.push(value);
                }

                // Update loop state with incremented index and updated accumulators
                sqlx::query(
                    r#"
                    UPDATE loop_iteration_state
                    SET current_index = $3, accumulators = $4
                    WHERE instance_id = $1 AND node_id = $2
                    "#,
                )
                .bind(instance_id)
                .bind(loop_head_id)
                .bind(current_index + 1)
                .bind(serde_json::to_vec(&accumulators)?)
                .execute(tx.as_mut())
                .await?;

                // Update each accumulator in eval_context so loop_head can see them
                for acc in &loop_meta.accumulators {
                    if let Some(acc_values) = accumulators.get(&acc.var) {
                        let acc_json =
                            serde_json::json!({ &acc.var: Value::Array(acc_values.clone()) });
                        sqlx::query(
                            r#"
                            UPDATE instance_eval_context
                            SET context_json = context_json || $2
                            WHERE instance_id = $1
                            "#,
                        )
                        .bind(instance_id)
                        .bind(acc_json)
                        .execute(tx.as_mut())
                        .await?;
                    }
                }

                // Re-enable the loop_head by resetting it to ready state
                // This allows it to be picked up again in the ready nodes query
                sqlx::query(
                    r#"
                    UPDATE node_ready_state
                    SET is_queued = FALSE, is_completed = FALSE, deps_satisfied = deps_required
                    WHERE instance_id = $1 AND node_id = $2
                    "#,
                )
                .bind(instance_id)
                .bind(loop_head_id)
                .execute(tx.as_mut())
                .await?;

                // Reset ALL body nodes so they can be unlocked again by the next continue edge
                // Use body_nodes field which contains all nodes in the loop body
                let body_nodes: Vec<&str> =
                    loop_meta.body_nodes.iter().map(|s| s.as_str()).collect();
                if !body_nodes.is_empty() {
                    sqlx::query(
                        r#"
                        UPDATE node_ready_state
                        SET is_queued = FALSE, is_completed = FALSE, deps_satisfied = 0
                        WHERE instance_id = $1 AND node_id = ANY($2)
                        "#,
                    )
                    .bind(instance_id)
                    .bind(&body_nodes)
                    .execute(tx.as_mut())
                    .await?;
                }

                // Don't push context to downstream via normal mechanism for back edges
                // The loop_head will handle distributing context when it evaluates
                // But DO continue to the ready nodes query below
            }

            // Skip downstream context pushing for back edge sources
            // The back edge handling already took care of re-enabling loop_head
            let is_back_edge = get_back_edge_target(&completion.node_id, dag).is_some();
            if !is_back_edge {
                // Push context to downstream nodes
                let payload_bytes = completion.payload.as_ref().map(|p| p.encode_to_vec());
                let downstream = get_downstream_nodes(&completion.node_id, dag, concurrent);

                for downstream_node_id in &downstream {
                    // Insert context for this dependency
                    let produced_vars = if node.produces.is_empty() {
                        vec![String::new()]
                    } else {
                        node.produces.clone()
                    };

                    for variable in &produced_vars {
                        sqlx::query(
                            r#"
                            INSERT INTO node_pending_context (instance_id, node_id, source_node_id, variable, payload)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (instance_id, node_id, source_node_id, variable) DO UPDATE SET payload = $5
                            "#,
                        )
                        .bind(instance_id)
                        .bind(*downstream_node_id)
                        .bind(&completion.node_id)
                        .bind(variable)
                        .bind(&payload_bytes)
                        .execute(tx.as_mut())
                        .await?;
                    }
                }

                // Increment deps_satisfied for all downstream nodes
                if !downstream.is_empty() {
                    let downstream_ids: Vec<&str> = downstream.to_vec();
                    sqlx::query(
                        r#"
                        UPDATE node_ready_state
                        SET deps_satisfied = deps_satisfied + 1
                        WHERE instance_id = $1 AND node_id = ANY($2)
                        "#,
                    )
                    .bind(instance_id)
                    .bind(&downstream_ids)
                    .execute(tx.as_mut())
                    .await?;
                }
            }

            // Loop until no more nodes become ready
            // This is needed because processing synthetic nodes (like loop_head) can make other nodes ready
            loop {
                // Find newly ready nodes (deps_satisfied >= deps_required, not yet queued/completed)
                let ready_nodes: Vec<String> = sqlx::query_scalar(
                    r#"
                    SELECT node_id
                    FROM node_ready_state
                    WHERE instance_id = $1
                      AND deps_satisfied >= deps_required
                      AND is_queued = FALSE
                      AND is_completed = FALSE
                    "#,
                )
                .bind(instance_id)
                .fetch_all(tx.as_mut())
                .await?;

                debug!(
                    "ready_nodes after completion of {:?}: {:?}",
                    completion.node_id, ready_nodes
                );

                // Process ready nodes - queue real actions, add synthetic completions to work queue
                if ready_nodes.is_empty() {
                    break;
                }
                // Load current eval context
                let (context_json, exceptions_json): (Value, Value) = sqlx::query_as(
                    r#"
                    SELECT context_json, exceptions_json
                    FROM instance_eval_context
                    WHERE instance_id = $1
                    "#,
                )
                .bind(instance_id)
                .fetch_one(tx.as_mut())
                .await?;

                let eval_ctx: EvalContext = serde_json::from_value(context_json)?;
                let exceptions: HashMap<String, Value> = serde_json::from_value(exceptions_json)?;

                for ready_node_id in ready_nodes {
                    let ready_node = match dag.nodes.iter().find(|n| n.id == ready_node_id) {
                        Some(n) => n,
                        None => continue,
                    };

                    // Check guard
                    let guard_passes = guard_allows(ready_node, &eval_ctx)?;
                    if !guard_passes {
                        // Insert synthetic completion into action ledger
                        let payload = build_null_payload(ready_node)?;
                        insert_synthetic_completion_simple_tx(
                            tx,
                            instance_id,
                            partition_id,
                            ready_node,
                            workflow_input,
                            next_seq,
                            payload.clone(),
                            true,
                        )
                        .await?;

                        // Mark as queued (will be marked completed when work queue processes it)
                        sqlx::query(
                            "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                        )
                        .bind(instance_id)
                        .bind(&ready_node_id)
                        .execute(tx.as_mut())
                        .await?;

                        // Add synthetic completion to work queue
                        work_queue.push(PendingCompletion {
                            node_id: ready_node_id,
                            payload,
                            success: true,
                        });
                        next_seq += 1;
                        continue;
                    }

                    // Check for unhandled exceptions from dependencies
                    if let Err(err) = validate_exception_context(ready_node, &exceptions) {
                        let payload_bytes = encode_exception_payload(
                            "RuntimeError",
                            "rappel.workflow",
                            &err.to_string(),
                        );
                        let arguments = decode_arguments(&payload_bytes, "exception payload")?;

                        // Insert synthetic completion into action ledger
                        insert_synthetic_completion_simple_tx(
                            tx,
                            instance_id,
                            partition_id,
                            ready_node,
                            workflow_input,
                            next_seq,
                            Some(arguments.clone()),
                            false,
                        )
                        .await?;

                        // Mark as queued
                        sqlx::query(
                            "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                        )
                        .bind(instance_id)
                        .bind(&ready_node_id)
                        .execute(tx.as_mut())
                        .await?;

                        work_queue.push(PendingCompletion {
                            node_id: ready_node_id,
                            payload: Some(arguments),
                            success: false,
                        });
                        next_seq += 1;
                        continue;
                    }

                    // Handle cycle-based loop_head nodes (synthetic, scheduler-evaluated)
                    if ready_node.node_type == NodeType::LoopHead as i32
                        && let Some(loop_meta) = ready_node.loop_head_meta.as_ref()
                    {
                        // Get or initialize loop state from database
                        let loop_state: Option<(i32, Option<Vec<u8>>)> = sqlx::query_as(
                            r#"
                                SELECT current_index, accumulators
                                FROM loop_iteration_state
                                WHERE instance_id = $1 AND node_id = $2
                                "#,
                        )
                        .bind(instance_id)
                        .bind(&ready_node_id)
                        .fetch_optional(tx.as_mut())
                        .await?;

                        let needs_init = loop_state.is_none();
                        let (current_index, accumulators_bytes) = loop_state.unwrap_or((0, None));

                        // Get iterator length from eval_ctx
                        let iterator_source_var = dag
                            .nodes
                            .iter()
                            .find(|n| n.id == loop_meta.iterator_source)
                            .and_then(|n| n.produces.first())
                            .map(|s| s.as_str())
                            .unwrap_or("");

                        let iterator_len = eval_ctx
                            .get(iterator_source_var)
                            .and_then(|v| v.as_array())
                            .map(|a| a.len())
                            .unwrap_or(0);

                        debug!(
                            "loop_head {}: iterator_source_var={}, iterator_len={}, current_index={}",
                            ready_node_id, iterator_source_var, iterator_len, current_index
                        );

                        // Parse accumulators from bytes or default to empty map
                        let accumulators: HashMap<String, Vec<Value>> = accumulators_bytes
                            .as_ref()
                            .and_then(|b| serde_json::from_slice(b).ok())
                            .unwrap_or_default();

                        if (current_index as usize) < iterator_len {
                            // Continue edge: dispatch body nodes
                            // First, initialize loop state if not exists
                            if needs_init {
                                // Initialize empty accumulators for each accumulator var
                                let init_accumulators: HashMap<String, Vec<Value>> = loop_meta
                                    .accumulators
                                    .iter()
                                    .map(|acc| (acc.var.clone(), Vec::new()))
                                    .collect();
                                sqlx::query(
                                        r#"
                                        INSERT INTO loop_iteration_state (instance_id, node_id, current_index, accumulators)
                                        VALUES ($1, $2, $3, $4)
                                        ON CONFLICT (instance_id, node_id) DO NOTHING
                                        "#,
                                    )
                                    .bind(instance_id)
                                    .bind(&ready_node_id)
                                    .bind(0i32)
                                    .bind(serde_json::to_vec(&init_accumulators)?)
                                    .execute(tx.as_mut())
                                    .await?;
                            }

                            // Get the current item from the iterator
                            let current_item = eval_ctx
                                .get(iterator_source_var)
                                .and_then(|v| v.as_array())
                                .and_then(|a| a.get(current_index as usize))
                                .cloned()
                                .unwrap_or(Value::Null);

                            // Evaluate preamble operations and store in eval_context
                            let mut preamble_vars: HashMap<String, Value> = HashMap::new();
                            for op in &loop_meta.preamble {
                                if let Some(preamble_op) = &op.op {
                                    match preamble_op {
                                            crate::messages::proto::preamble_op::Op::SetIteratorIndex(set_idx) => {
                                                preamble_vars.insert(set_idx.var.clone(), Value::Number(serde_json::Number::from(current_index)));
                                            }
                                            crate::messages::proto::preamble_op::Op::SetIteratorValue(set_val) => {
                                                preamble_vars.insert(set_val.var.clone(), current_item.clone());
                                            }
                                            crate::messages::proto::preamble_op::Op::SetAccumulatorLen(set_len) => {
                                                // Use the first accumulator's length (all should be same length in sync)
                                                let acc_len = accumulators.values().next().map(|v| v.len()).unwrap_or(0) as i64;
                                                preamble_vars.insert(set_len.var.clone(), Value::Number(serde_json::Number::from(acc_len)));
                                            }
                                        }
                                }
                            }

                            // Update eval_context with preamble results
                            if !preamble_vars.is_empty() {
                                let preamble_json = serde_json::to_value(&preamble_vars)?;
                                sqlx::query(
                                    r#"
                                        UPDATE instance_eval_context
                                        SET context_json = context_json || $2
                                        WHERE instance_id = $1
                                        "#,
                                )
                                .bind(instance_id)
                                .bind(preamble_json)
                                .execute(tx.as_mut())
                                .await?;
                            }

                            // Unlock body_entry nodes via continue edges
                            let body_entries = get_downstream_nodes_by_edge_type(
                                &ready_node_id,
                                dag,
                                EdgeType::Continue,
                            );
                            debug!(
                                "loop_head {} continue edge targets: {:?}",
                                ready_node_id, body_entries
                            );
                            if !body_entries.is_empty() {
                                sqlx::query(
                                    r#"
                                        UPDATE node_ready_state
                                        SET deps_satisfied = deps_satisfied + 1
                                        WHERE instance_id = $1 AND node_id = ANY($2)
                                        "#,
                                )
                                .bind(instance_id)
                                .bind(&body_entries)
                                .execute(tx.as_mut())
                                .await?;
                            }

                            // Mark loop_head as queued (NOT completed - it will be re-evaluated on back edge)
                            sqlx::query(
                                    "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                                )
                                .bind(instance_id)
                                .bind(&ready_node_id)
                                .execute(tx.as_mut())
                                .await?;

                            // Create synthetic completion to propagate context to body nodes
                            // Encode each variable separately to avoid extraction issues
                            let loop_var_payload = encode_value_result(&current_item)?;

                            // Push context to ALL body nodes (not just body_entries)
                            // This ensures that chained preamble nodes all have access to the loop variable and accumulator
                            let all_body_nodes = &loop_meta.body_nodes;
                            for body_node in all_body_nodes {
                                // Push loop variable
                                sqlx::query(
                                        r#"
                                        INSERT INTO node_pending_context (instance_id, node_id, source_node_id, variable, payload)
                                        VALUES ($1, $2, $3, $4, $5)
                                        ON CONFLICT (instance_id, node_id, source_node_id, variable) DO UPDATE SET payload = $5
                                        "#,
                                    )
                                    .bind(instance_id)
                                    .bind(body_node)
                                    .bind(&ready_node_id)
                                    .bind(&loop_meta.loop_var)
                                    .bind(Some(loop_var_payload.encode_to_vec()))
                                    .execute(tx.as_mut())
                                    .await?;

                                // Push each accumulator with its own values
                                for acc in &loop_meta.accumulators {
                                    let acc_values =
                                        accumulators.get(&acc.var).cloned().unwrap_or_default();
                                    let acc_payload =
                                        encode_value_result(&Value::Array(acc_values))?;
                                    sqlx::query(
                                            r#"
                                            INSERT INTO node_pending_context (instance_id, node_id, source_node_id, variable, payload)
                                            VALUES ($1, $2, $3, $4, $5)
                                            ON CONFLICT (instance_id, node_id, source_node_id, variable) DO UPDATE SET payload = $5
                                            "#,
                                        )
                                        .bind(instance_id)
                                        .bind(body_node)
                                        .bind(&ready_node_id)
                                        .bind(&acc.var)
                                        .bind(Some(acc_payload.encode_to_vec()))
                                        .execute(tx.as_mut())
                                        .await?;
                                }
                            }
                        } else {
                            // Loop finished: unlock downstream nodes via Break edges OR regular depends_on
                            let break_targets = get_downstream_nodes_by_edge_type(
                                &ready_node_id,
                                dag,
                                EdgeType::Break,
                            );
                            if !break_targets.is_empty() {
                                sqlx::query(
                                    r#"
                                        UPDATE node_ready_state
                                        SET deps_satisfied = deps_satisfied + 1
                                        WHERE instance_id = $1 AND node_id = ANY($2)
                                        "#,
                                )
                                .bind(instance_id)
                                .bind(&break_targets)
                                .execute(tx.as_mut())
                                .await?;
                            }

                            // Also unlock nodes that depend on loop_head via regular depends_on
                            // (IR-based loops use depends_on rather than Break edges for downstream nodes)
                            let downstream_nodes: Vec<&str> = dag
                                .nodes
                                .iter()
                                .filter(|n| n.depends_on.contains(&ready_node_id))
                                .map(|n| n.id.as_str())
                                .collect();
                            if !downstream_nodes.is_empty() {
                                sqlx::query(
                                    r#"
                                        UPDATE node_ready_state
                                        SET deps_satisfied = deps_satisfied + 1
                                        WHERE instance_id = $1 AND node_id = ANY($2)
                                        "#,
                                )
                                .bind(instance_id)
                                .bind(&downstream_nodes)
                                .execute(tx.as_mut())
                                .await?;
                            }

                            // Store final accumulator results in eval_context
                            for acc in &loop_meta.accumulators {
                                let acc_values =
                                    accumulators.get(&acc.var).cloned().unwrap_or_default();
                                let acc_json =
                                    serde_json::json!({ &acc.var: Value::Array(acc_values) });
                                sqlx::query(
                                    r#"
                                        UPDATE instance_eval_context
                                        SET context_json = context_json || $2
                                        WHERE instance_id = $1
                                        "#,
                                )
                                .bind(instance_id)
                                .bind(acc_json)
                                .execute(tx.as_mut())
                                .await?;
                            }

                            // Mark loop_head as completed
                            sqlx::query(
                                    "UPDATE node_ready_state SET is_queued = TRUE, is_completed = TRUE WHERE instance_id = $1 AND node_id = $2",
                                )
                                .bind(instance_id)
                                .bind(&ready_node_id)
                                .execute(tx.as_mut())
                                .await?;

                            // Mark all body nodes as completed too since the loop is done
                            // (They were reset by the last back edge, but won't execute again)
                            let body_nodes: Vec<&str> =
                                loop_meta.body_nodes.iter().map(|s| s.as_str()).collect();
                            if !body_nodes.is_empty() {
                                sqlx::query(
                                    r#"
                                        UPDATE node_ready_state
                                        SET is_queued = TRUE, is_completed = TRUE
                                        WHERE instance_id = $1 AND node_id = ANY($2)
                                        "#,
                                )
                                .bind(instance_id)
                                .bind(&body_nodes)
                                .execute(tx.as_mut())
                                .await?;
                            }

                            // Insert synthetic completion for the loop_head with the final accumulator
                            let final_accumulator = loop_meta
                                .accumulators
                                .first()
                                .and_then(|acc_spec| accumulators.get(&acc_spec.var))
                                .cloned()
                                .unwrap_or_default();
                            let final_payload =
                                encode_value_result(&Value::Array(final_accumulator))?;
                            insert_synthetic_completion_simple_tx(
                                tx,
                                instance_id,
                                partition_id,
                                ready_node,
                                workflow_input,
                                next_seq,
                                Some(final_payload.clone()),
                                true,
                            )
                            .await?;

                            // Push each accumulator to exit targets (Break edges)
                            for exit_target in &break_targets {
                                for acc in &loop_meta.accumulators {
                                    let acc_values =
                                        accumulators.get(&acc.var).cloned().unwrap_or_default();
                                    let acc_payload =
                                        encode_value_result(&Value::Array(acc_values))?;
                                    let payload_bytes = Some(acc_payload.encode_to_vec());
                                    sqlx::query(
                                            r#"
                                            INSERT INTO node_pending_context (instance_id, node_id, source_node_id, variable, payload)
                                            VALUES ($1, $2, $3, $4, $5)
                                            ON CONFLICT (instance_id, node_id, source_node_id, variable) DO UPDATE SET payload = $5
                                            "#,
                                        )
                                        .bind(instance_id)
                                        .bind(*exit_target)
                                        .bind(&ready_node_id)
                                        .bind(&acc.var)
                                        .bind(&payload_bytes)
                                        .execute(tx.as_mut())
                                        .await?;
                                }
                            }

                            // Push accumulators to downstream nodes (depends_on edges)
                            for downstream_node in &downstream_nodes {
                                for acc in &loop_meta.accumulators {
                                    let acc_values =
                                        accumulators.get(&acc.var).cloned().unwrap_or_default();
                                    let acc_payload =
                                        encode_value_result(&Value::Array(acc_values))?;
                                    let payload_bytes = Some(acc_payload.encode_to_vec());
                                    sqlx::query(
                                            r#"
                                            INSERT INTO node_pending_context (instance_id, node_id, source_node_id, variable, payload)
                                            VALUES ($1, $2, $3, $4, $5)
                                            ON CONFLICT (instance_id, node_id, source_node_id, variable) DO UPDATE SET payload = $5
                                            "#,
                                        )
                                        .bind(instance_id)
                                        .bind(*downstream_node)
                                        .bind(&ready_node_id)
                                        .bind(&acc.var)
                                        .bind(&payload_bytes)
                                        .execute(tx.as_mut())
                                        .await?;
                                }
                            }

                            next_seq += 1;
                        }
                        continue;
                    }

                    // Load pending context for this node
                    let pending_contexts: Vec<(String, String, Option<Vec<u8>>)> = sqlx::query_as(
                        r#"
                        SELECT source_node_id, variable, payload
                        FROM node_pending_context
                        WHERE instance_id = $1 AND node_id = $2
                        "#,
                    )
                    .bind(instance_id)
                    .bind(&ready_node_id)
                    .fetch_all(tx.as_mut())
                    .await?;

                    // Build dependency contexts
                    let mut contexts: Vec<WorkflowNodeContext> = Vec::new();
                    for (source_node_id, variable, payload_opt) in pending_contexts {
                        if let Some(payload_bytes) = payload_opt {
                            let arguments = decode_arguments(&payload_bytes, "pending context")?;
                            contexts.push(WorkflowNodeContext {
                                variable,
                                payload: Some(arguments),
                                workflow_node_id: source_node_id,
                            });
                        }
                    }

                    // Check if this is a loop node (but not a multi-action loop - those are handled by legacy scheduler)
                    let loop_ast = ready_node.ast.as_ref().and_then(|ast| ast.r#loop.as_ref());

                    if let Some(loop_ast) = loop_ast {
                        // Handle multi-action loops
                        if has_multi_action_body_graph(ready_node) {
                            // Multi-action loop - dispatch first phase
                            let accumulator_names = vec![loop_ast.accumulator.clone()];
                            let state =
                                extract_multi_action_loop_state(&eval_ctx, &accumulator_names);
                            let phase_eval = find_next_ready_phase(ready_node, &eval_ctx, &state)?;

                            if let Some(phase_eval) = phase_eval {
                                // Add all accumulators to context
                                for (acc_name, acc_values) in &state.accumulators {
                                    let accumulator_value = Value::Array(acc_values.clone());
                                    let accumulator_payload =
                                        encode_value_result(&accumulator_value)?;
                                    contexts.push(WorkflowNodeContext {
                                        variable: acc_name.clone(),
                                        payload: Some(accumulator_payload),
                                        workflow_node_id: ready_node_id.clone(),
                                    });
                                }

                                // Add loop index to context
                                let index_value = Value::Number(serde_json::Number::from(
                                    state.current_index as u64,
                                ));
                                let index_payload = encode_value_result(&index_value)?;
                                contexts.push(WorkflowNodeContext {
                                    variable: LOOP_INDEX_VAR.to_string(),
                                    payload: Some(index_payload),
                                    workflow_node_id: ready_node_id.clone(),
                                });

                                // Add completed phases to context
                                let phases_value = Value::Array(
                                    state
                                        .completed_phases
                                        .iter()
                                        .map(|s| Value::String(s.clone()))
                                        .collect(),
                                );
                                let phases_payload = encode_value_result(&phases_value)?;
                                contexts.push(WorkflowNodeContext {
                                    variable: LOOP_PHASE_VAR.to_string(),
                                    payload: Some(phases_payload),
                                    workflow_node_id: ready_node_id.clone(),
                                });

                                // Add phase results to context
                                let results_value = Value::Object(
                                    state
                                        .phase_results
                                        .iter()
                                        .map(|(k, v)| (k.clone(), v.clone()))
                                        .collect(),
                                );
                                let results_payload = encode_value_result(&results_value)?;
                                contexts.push(WorkflowNodeContext {
                                    variable: LOOP_PHASE_RESULTS_VAR.to_string(),
                                    payload: Some(results_payload),
                                    workflow_node_id: ready_node_id.clone(),
                                });

                                // Add preamble results to context
                                let preamble_results_value =
                                    if let Some(preamble_vars) = &phase_eval.preamble_results {
                                        Value::Object(
                                            preamble_vars
                                                .iter()
                                                .map(|(k, v)| (k.clone(), v.clone()))
                                                .collect(),
                                        )
                                    } else {
                                        Value::Object(
                                            state
                                                .preamble_results
                                                .iter()
                                                .map(|(k, v)| (k.clone(), v.clone()))
                                                .collect(),
                                        )
                                    };
                                let preamble_results_payload =
                                    encode_value_result(&preamble_results_value)?;
                                contexts.push(WorkflowNodeContext {
                                    variable: LOOP_PREAMBLE_RESULTS_VAR.to_string(),
                                    payload: Some(preamble_results_payload),
                                    workflow_node_id: ready_node_id.clone(),
                                });

                                // Add current phase info
                                let phase_id_payload = encode_value_result(&Value::String(
                                    phase_eval.phase_id.clone(),
                                ))?;
                                contexts.push(WorkflowNodeContext {
                                    variable: "__current_phase_id".to_string(),
                                    payload: Some(phase_id_payload),
                                    workflow_node_id: ready_node_id.clone(),
                                });
                                let output_var_payload = encode_value_result(&Value::String(
                                    phase_eval.output_var.clone(),
                                ))?;
                                contexts.push(WorkflowNodeContext {
                                    variable: "__current_phase_output_var".to_string(),
                                    payload: Some(output_var_payload),
                                    workflow_node_id: ready_node_id.clone(),
                                });

                                // Create dispatch node with phase action
                                let mut dispatch_node = ready_node.clone();
                                dispatch_node.action = phase_eval.action.clone();
                                if !phase_eval.module.is_empty() {
                                    dispatch_node.module = phase_eval.module.clone();
                                }

                                let queued = build_action_dispatch(
                                    dispatch_node,
                                    workflow_input,
                                    contexts,
                                    phase_eval.kwargs,
                                )?;
                                let dispatch_bytes = queued.dispatch.encode_to_vec();
                                let scheduled_at = compute_scheduled_at(ready_node, &eval_ctx)?;

                                sqlx::query(
                                    r#"
                                    INSERT INTO daemon_action_ledger (
                                        instance_id, partition_id, action_seq, status,
                                        module, function_name, dispatch_payload, workflow_node_id,
                                        timeout_seconds, max_retries, timeout_retry_limit,
                                        attempt_number, scheduled_at, retry_kind,
                                        backoff_kind, backoff_base_delay_ms, backoff_multiplier
                                    ) VALUES (
                                        $1, $2, $3, 'queued',
                                        $4, $5, $6, $7,
                                        $8, $9, $10,
                                        0, $11, 'failure',
                                        $12, $13, $14
                                    )
                                    "#,
                                )
                                .bind(instance_id)
                                .bind(partition_id)
                                .bind(next_seq)
                                .bind(&queued.module)
                                .bind(&queued.function_name)
                                .bind(&dispatch_bytes)
                                .bind(&ready_node_id)
                                .bind(queued.timeout_seconds)
                                .bind(queued.max_retries)
                                .bind(queued.timeout_retry_limit)
                                .bind(scheduled_at)
                                .bind(queued.backoff.kind_str())
                                .bind(queued.backoff.base_delay_ms())
                                .bind(queued.backoff.multiplier())
                                .execute(tx.as_mut())
                                .await?;

                                // Mark as queued (but NOT completed - loop may have more iterations)
                                sqlx::query(
                                    "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                                )
                                .bind(instance_id)
                                .bind(&ready_node_id)
                                .execute(tx.as_mut())
                                .await?;

                                total_queued += 1;
                                next_seq += 1;
                            } else {
                                // Multi-action loop finished - insert synthetic completion with final accumulator
                                let accumulator_value = Value::Array(extract_accumulator(
                                    &eval_ctx,
                                    &loop_ast.accumulator,
                                ));
                                let payload = encode_value_result(&accumulator_value)?;

                                insert_synthetic_completion_simple_tx(
                                    tx,
                                    instance_id,
                                    partition_id,
                                    ready_node,
                                    workflow_input,
                                    next_seq,
                                    Some(payload.clone()),
                                    true,
                                )
                                .await?;

                                // Mark as queued
                                sqlx::query(
                                    "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                                )
                                .bind(instance_id)
                                .bind(&ready_node_id)
                                .execute(tx.as_mut())
                                .await?;

                                // Add to work queue so downstream nodes are scheduled
                                work_queue.push(PendingCompletion {
                                    node_id: ready_node_id,
                                    payload: Some(payload),
                                    success: true,
                                });
                                next_seq += 1;
                            }
                            continue;
                        }

                        // Handle simple loop node - don't dispatch the loop itself, dispatch the body action
                        let loop_eval = evaluate_loop_iteration(ready_node, &eval_ctx)?;

                        if let Some(loop_eval) = loop_eval {
                            // There's another iteration - dispatch the body action
                            let accumulator_value = Value::Array(loop_eval.accumulator.clone());
                            let accumulator_payload = encode_value_result(&accumulator_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: loop_ast.accumulator.clone(),
                                payload: Some(accumulator_payload),
                                workflow_node_id: ready_node_id.clone(),
                            });

                            let index_value = Value::Number(serde_json::Number::from(
                                loop_eval.current_index as u64,
                            ));
                            let index_payload = encode_value_result(&index_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: LOOP_INDEX_VAR.to_string(),
                                payload: Some(index_payload),
                                workflow_node_id: ready_node_id.clone(),
                            });

                            // Create dispatch node with body action
                            let mut dispatch_node = ready_node.clone();
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
                                workflow_input,
                                contexts,
                                loop_eval.kwargs,
                            )?;
                            let dispatch_bytes = queued.dispatch.encode_to_vec();
                            let scheduled_at = compute_scheduled_at(ready_node, &eval_ctx)?;

                            sqlx::query(
                                r#"
                                INSERT INTO daemon_action_ledger (
                                    instance_id, partition_id, action_seq, status,
                                    module, function_name, dispatch_payload, workflow_node_id,
                                    timeout_seconds, max_retries, timeout_retry_limit,
                                    attempt_number, scheduled_at, retry_kind,
                                    backoff_kind, backoff_base_delay_ms, backoff_multiplier
                                ) VALUES (
                                    $1, $2, $3, 'queued',
                                    $4, $5, $6, $7,
                                    $8, $9, $10,
                                    0, $11, 'failure',
                                    $12, $13, $14
                                )
                                "#,
                            )
                            .bind(instance_id)
                            .bind(partition_id)
                            .bind(next_seq)
                            .bind(&queued.module)
                            .bind(&queued.function_name)
                            .bind(&dispatch_bytes)
                            .bind(&ready_node_id)
                            .bind(queued.timeout_seconds)
                            .bind(queued.max_retries)
                            .bind(queued.timeout_retry_limit)
                            .bind(scheduled_at)
                            .bind(queued.backoff.kind_str())
                            .bind(queued.backoff.base_delay_ms())
                            .bind(queued.backoff.multiplier())
                            .execute(tx.as_mut())
                            .await?;

                            // Mark as queued (but NOT completed - loop may have more iterations)
                            sqlx::query(
                                "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                            )
                            .bind(instance_id)
                            .bind(&ready_node_id)
                            .execute(tx.as_mut())
                            .await?;

                            total_queued += 1;
                            next_seq += 1;
                        } else {
                            // Loop finished - insert synthetic completion with final accumulator
                            let accumulator_value =
                                Value::Array(extract_accumulator(&eval_ctx, &loop_ast.accumulator));
                            let payload = encode_value_result(&accumulator_value)?;

                            insert_synthetic_completion_simple_tx(
                                tx,
                                instance_id,
                                partition_id,
                                ready_node,
                                workflow_input,
                                next_seq,
                                Some(payload.clone()),
                                true,
                            )
                            .await?;

                            // Mark as queued
                            sqlx::query(
                                "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                            )
                            .bind(instance_id)
                            .bind(&ready_node_id)
                            .execute(tx.as_mut())
                            .await?;

                            // Add to work queue so downstream nodes are scheduled
                            work_queue.push(PendingCompletion {
                                node_id: ready_node_id,
                                payload: Some(payload),
                                success: true,
                            });
                            next_seq += 1;
                        }
                        continue;
                    }

                    // Queue real action (non-loop nodes)
                    let resolved_kwargs = evaluate_kwargs(ready_node, &eval_ctx)?;

                    // For python_block nodes, include the full eval_context so they can access
                    // all variables set by previous nodes (not just their direct dependencies)
                    let mut dispatch_contexts = contexts;
                    if ready_node.action == "python_block" {
                        // Add all variables from eval_context that aren't already in contexts
                        for (key, value) in &eval_ctx {
                            // Skip internal variables and ones already in context
                            if key.starts_with("__") {
                                continue;
                            }
                            if dispatch_contexts.iter().any(|c| c.variable == *key) {
                                continue;
                            }
                            // Encode the value and add to contexts
                            if let Ok(payload) = encode_value_result(value) {
                                dispatch_contexts.push(WorkflowNodeContext {
                                    variable: key.clone(),
                                    payload: Some(payload),
                                    workflow_node_id: String::new(),
                                });
                            }
                        }
                    }

                    let queued = build_action_dispatch(
                        ready_node.clone(),
                        workflow_input,
                        dispatch_contexts,
                        resolved_kwargs,
                    )?;
                    let dispatch_bytes = queued.dispatch.encode_to_vec();
                    let scheduled_at = compute_scheduled_at(ready_node, &eval_ctx)?;

                    sqlx::query(
                        r#"
                        INSERT INTO daemon_action_ledger (
                            instance_id, partition_id, action_seq, status,
                            module, function_name, dispatch_payload, workflow_node_id,
                            timeout_seconds, max_retries, timeout_retry_limit,
                            attempt_number, scheduled_at, retry_kind,
                            backoff_kind, backoff_base_delay_ms, backoff_multiplier
                        ) VALUES (
                            $1, $2, $3, 'queued',
                            $4, $5, $6, $7,
                            $8, $9, $10,
                            0, $11, 'failure',
                            $12, $13, $14
                        )
                        "#,
                    )
                    .bind(instance_id)
                    .bind(partition_id)
                    .bind(next_seq)
                    .bind(&queued.module)
                    .bind(&queued.function_name)
                    .bind(&dispatch_bytes)
                    .bind(&ready_node_id)
                    .bind(queued.timeout_seconds)
                    .bind(queued.max_retries)
                    .bind(queued.timeout_retry_limit)
                    .bind(scheduled_at)
                    .bind(queued.backoff.kind_str())
                    .bind(queued.backoff.base_delay_ms())
                    .bind(queued.backoff.multiplier())
                    .execute(tx.as_mut())
                    .await?;

                    // Mark as queued
                    sqlx::query(
                        "UPDATE node_ready_state SET is_queued = TRUE WHERE instance_id = $1 AND node_id = $2",
                    )
                    .bind(instance_id)
                    .bind(&ready_node_id)
                    .execute(tx.as_mut())
                    .await?;

                    total_queued += 1;
                    next_seq += 1;
                } // end for ready_node_id
            } // end loop for re-querying ready nodes
        } // end while work_queue

        // Update next_action_seq
        sqlx::query("UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1")
            .bind(instance_id)
            .bind(next_seq)
            .execute(tx.as_mut())
            .await?;

        // Check if all nodes are completed and store workflow result if so
        let (completed_count, total_count): (i64, i64) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE is_completed = TRUE),
                COUNT(*)
            FROM node_ready_state
            WHERE instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_one(tx.as_mut())
        .await?;

        if completed_count == total_count && total_count > 0 {
            // All nodes completed - store the workflow result
            let return_variable = &dag.return_variable;
            if !return_variable.is_empty() {
                // Find the node that produces the return variable and get its result payload
                // from the action ledger
                let result_payload: Option<Vec<u8>> = sqlx::query_scalar(
                    r#"
                    SELECT dal.result_payload
                    FROM daemon_action_ledger dal
                    JOIN node_ready_state nrs ON nrs.instance_id = dal.instance_id
                        AND nrs.node_id = dal.workflow_node_id
                    WHERE dal.instance_id = $1
                      AND dal.workflow_node_id = ANY($2)
                      AND dal.success = TRUE
                    ORDER BY dal.completed_at DESC
                    LIMIT 1
                    "#,
                )
                .bind(instance_id)
                .bind(
                    dag.nodes
                        .iter()
                        .filter(|n| n.produces.contains(return_variable))
                        .map(|n| n.id.as_str())
                        .collect::<Vec<_>>(),
                )
                .fetch_optional(tx.as_mut())
                .await?
                .flatten();

                if let Some(payload) = result_payload {
                    sqlx::query("UPDATE workflow_instances SET result_payload = $2 WHERE id = $1")
                        .bind(instance_id)
                        .bind(&payload)
                        .execute(tx.as_mut())
                        .await?;
                }
            }
        }

        Ok(total_queued)
    }

    pub async fn dispatch_actions(&self, limit: i64) -> Result<Vec<LedgerAction>> {
        let fn_start = Instant::now();
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
        let query_ms = fn_start.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(records) => {
                let count = records.len();
                if count > 0 {
                    metrics::counter!("rappel_actions_dispatched_total").increment(count as u64);
                }
                // JSON timing output for analysis
                debug!(
                    target: "db_timing",
                    r#"{{"fn":"dispatch_actions","total_ms":{:.3},"limit":{},"returned":{}}}"#,
                    query_ms, limit, count
                );
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
        let batch_start = Instant::now();
        let record_count = records.len();
        let span = tracing::debug_span!("db.mark_actions_batch", count = record_count);
        let _guard = span.enter();

        let tx_start = Instant::now();
        let mut tx = self.pool.begin().await?;
        let tx_acquire_ms = tx_start.elapsed().as_secs_f64() * 1000.0;

        let loop_start = Instant::now();

        // Collect success records for batch processing
        let success_records: Vec<&CompletionRecord> =
            records.iter().filter(|r| r.success).collect();

        // Batch fetch loop action data for all success records at once
        let batch_fetch_start = Instant::now();
        let loop_data = Self::batch_fetch_loop_action_data(&mut tx, &success_records).await?;
        let batch_fetch_ms = batch_fetch_start.elapsed().as_secs_f64() * 1000.0;

        // Process each record using pre-fetched data
        let process_start = Instant::now();
        let mut processed: Vec<CompletionRecord> = Vec::new();
        for record in records {
            if record.success {
                // Use pre-fetched data if available
                if let Some(batched_row) = loop_data.get(&record.action_id) {
                    if let Some(updated) = self
                        .process_loop_completion_with_row_tx(&mut tx, record, batched_row)
                        .await?
                    {
                        processed.push(updated);
                    }
                } else {
                    // No row found in batch - record likely already processed or token mismatch
                    processed.push(record.clone());
                }
            } else {
                processed.push(record.clone());
            }
        }
        let process_ms = process_start.elapsed().as_secs_f64() * 1000.0;
        let loop_processing_ms = loop_start.elapsed().as_secs_f64() * 1000.0;

        debug!(
            target: "db_timing",
            r#"{{"fn":"mark_actions_batch_loop","batch_fetch_ms":{:.3},"process_ms":{:.3},"success_count":{},"fetched_count":{}}}"#,
            batch_fetch_ms, process_ms, success_records.len(), loop_data.len()
        );
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
        let success_update_start = Instant::now();
        let success_count = successes_with_tokens.len();
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
        let success_update_ms = success_update_start.elapsed().as_secs_f64() * 1000.0;

        let failure_update_start = Instant::now();
        let failure_count = failures_with_tokens.len();
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
        let failure_update_ms = failure_update_start.elapsed().as_secs_f64() * 1000.0;

        // O(1) push-based scheduling: call complete_node_push_tx for each completed action
        let schedule_start = Instant::now();
        let instance_count = workflow_instances.len();
        let mut total_scheduled = 0usize;

        // Build a map of action_id -> (instance_id, node_id, partition_id, action_seq, result_payload, success)
        // We already have the updated rows, we just need more data
        #[derive(Debug)]
        struct CompletionData {
            instance_id: WorkflowInstanceId,
            partition_id: i32,
            node_id: String,
            action_seq: i32,
            workflow_version_id: WorkflowVersionId,
            input_payload: Vec<u8>,
            result_payload: Vec<u8>,
            success: bool,
        }

        // Fetch all needed data for completions in a single batch query
        let all_action_ids: Vec<LedgerActionId> = records.iter().map(|r| r.action_id).collect();
        let completion_data: Vec<CompletionData> = sqlx::query_as::<
            _,
            (
                LedgerActionId,
                WorkflowInstanceId,
                i32,
                Option<String>,
                i32,
                Option<WorkflowVersionId>,
                Option<Vec<u8>>,
                Option<Vec<u8>>,
                Option<bool>,
            ),
        >(
            r#"
            SELECT
                dal.id,
                dal.instance_id,
                dal.partition_id,
                dal.workflow_node_id,
                dal.action_seq,
                wi.workflow_version_id,
                wi.input_payload,
                dal.result_payload,
                dal.success
            FROM daemon_action_ledger dal
            JOIN workflow_instances wi ON wi.id = dal.instance_id
            WHERE dal.id = ANY($1)
              AND dal.status = 'completed'
              AND dal.workflow_node_id IS NOT NULL
            "#,
        )
        .bind(&all_action_ids)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .filter_map(|row| {
            let node_id = row.3?;
            let version_id = row.5?;
            Some(CompletionData {
                instance_id: row.1,
                partition_id: row.2,
                node_id,
                action_seq: row.4,
                workflow_version_id: version_id,
                input_payload: row.6.unwrap_or_default(),
                result_payload: row.7.unwrap_or_default(),
                success: row.8.unwrap_or(true),
            })
        })
        .collect();

        // Process completions using O(1) push logic
        for data in completion_data {
            let workflow_input = decode_arguments(&data.input_payload, "workflow input")?;
            let result_payload = if data.result_payload.is_empty() {
                None
            } else {
                Some(decode_arguments(&data.result_payload, "result payload")?)
            };

            let cached_version = self
                .get_cached_version(data.workflow_version_id, &mut tx)
                .await?;

            let scheduled = self
                .complete_node_push_tx(
                    &mut tx,
                    data.instance_id,
                    data.partition_id,
                    &data.node_id,
                    data.action_seq,
                    result_payload,
                    data.success,
                    &cached_version.dag,
                    cached_version.concurrent,
                    &workflow_input,
                )
                .await?;
            total_scheduled += scheduled;
        }

        let schedule_ms = schedule_start.elapsed().as_secs_f64() * 1000.0;

        let commit_start = Instant::now();
        tx.commit().await?;
        let commit_ms = commit_start.elapsed().as_secs_f64() * 1000.0;

        let total_ms = batch_start.elapsed().as_secs_f64() * 1000.0;

        // JSON timing output for analysis
        debug!(
            target: "db_timing",
            r#"{{"fn":"mark_actions_batch","total_ms":{:.3},"tx_acquire_ms":{:.3},"loop_processing_ms":{:.3},"success_update_ms":{:.3},"failure_update_ms":{:.3},"schedule_ms":{:.3},"commit_ms":{:.3},"record_count":{},"success_count":{},"failure_count":{},"instance_count":{},"scheduled_actions":{}}}"#,
            total_ms, tx_acquire_ms, loop_processing_ms, success_update_ms, failure_update_ms, schedule_ms, commit_ms, record_count, success_count, failure_count, instance_count, total_scheduled
        );

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
        let fn_start = Instant::now();

        let instance_lookup_start = Instant::now();
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
        let instance_lookup_ms = instance_lookup_start.elapsed().as_secs_f64() * 1000.0;

        // Use cached workflow version to avoid repeated DB lookups
        let cache_start = Instant::now();
        let cached_version = self.get_cached_version(version_id, tx).await?;
        let cache_ms = cache_start.elapsed().as_secs_f64() * 1000.0;
        let workflow_input_bytes = instance.input_payload.clone().unwrap_or_default();
        let workflow_input = decode_arguments(&workflow_input_bytes, "workflow input")?;
        // Load instance state and build scheduling context in a single query
        let state_load_start = Instant::now();
        let state = load_instance_state_and_context(
            tx,
            instance.id,
            &cached_version.node_map,
            &workflow_input,
        )
        .await?;
        let state_load_ms = state_load_start.elapsed().as_secs_f64() * 1000.0;

        let dag_start = Instant::now();
        let mut dag_state = InstanceDagState::new(state.known_nodes, state.completed_nodes);
        let dag_machine = DagStateMachine::new(&cached_version.dag, cached_version.concurrent);
        let mut scheduling_ctx = state.scheduling_ctx;
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
                    // Check if this is a multi-action loop
                    if has_multi_action_body_graph(&node) {
                        let accumulator_names = vec![loop_ast.accumulator.clone()];
                        let state = extract_multi_action_loop_state(
                            &scheduling_ctx.eval_ctx,
                            &accumulator_names,
                        );
                        let phase_eval =
                            find_next_ready_phase(&node, &scheduling_ctx.eval_ctx, &state)?;

                        if let Some(phase_eval) = phase_eval {
                            // Dispatch the next ready phase
                            let mut contexts =
                                build_dependency_contexts(&node, &scheduling_ctx.payloads);

                            // Add all accumulators to context
                            for (acc_name, acc_values) in &state.accumulators {
                                let accumulator_value = Value::Array(acc_values.clone());
                                let accumulator_payload = encode_value_result(&accumulator_value)?;
                                contexts.push(WorkflowNodeContext {
                                    variable: acc_name.clone(),
                                    payload: Some(accumulator_payload),
                                    workflow_node_id: node_id.clone(),
                                });
                            }

                            // Add loop index to context
                            let index_value =
                                Value::Number(serde_json::Number::from(state.current_index as u64));
                            let index_payload = encode_value_result(&index_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: LOOP_INDEX_VAR.to_string(),
                                payload: Some(index_payload),
                                workflow_node_id: node_id.clone(),
                            });

                            // Add completed phases to context
                            let phases_value = Value::Array(
                                state
                                    .completed_phases
                                    .iter()
                                    .map(|s| Value::String(s.clone()))
                                    .collect(),
                            );
                            let phases_payload = encode_value_result(&phases_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: LOOP_PHASE_VAR.to_string(),
                                payload: Some(phases_payload),
                                workflow_node_id: node_id.clone(),
                            });

                            // Add phase results to context
                            let results_value = Value::Object(
                                state
                                    .phase_results
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                            );
                            let results_payload = encode_value_result(&results_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: LOOP_PHASE_RESULTS_VAR.to_string(),
                                payload: Some(results_payload),
                                workflow_node_id: node_id.clone(),
                            });

                            // Add preamble results to context (from first phase evaluation)
                            let preamble_results_value =
                                if let Some(preamble_vars) = &phase_eval.preamble_results {
                                    Value::Object(
                                        preamble_vars
                                            .iter()
                                            .map(|(k, v)| (k.clone(), v.clone()))
                                            .collect(),
                                    )
                                } else {
                                    Value::Object(
                                        state
                                            .preamble_results
                                            .iter()
                                            .map(|(k, v)| (k.clone(), v.clone()))
                                            .collect(),
                                    )
                                };
                            let preamble_results_payload =
                                encode_value_result(&preamble_results_value)?;
                            contexts.push(WorkflowNodeContext {
                                variable: LOOP_PREAMBLE_RESULTS_VAR.to_string(),
                                payload: Some(preamble_results_payload),
                                workflow_node_id: node_id.clone(),
                            });

                            // Add current phase info
                            let phase_id_payload =
                                encode_value_result(&Value::String(phase_eval.phase_id.clone()))?;
                            contexts.push(WorkflowNodeContext {
                                variable: "__current_phase_id".to_string(),
                                payload: Some(phase_id_payload),
                                workflow_node_id: node_id.clone(),
                            });
                            let output_var_payload =
                                encode_value_result(&Value::String(phase_eval.output_var.clone()))?;
                            contexts.push(WorkflowNodeContext {
                                variable: "__current_phase_output_var".to_string(),
                                payload: Some(output_var_payload),
                                workflow_node_id: node_id.clone(),
                            });

                            // Create dispatch node with phase action
                            let mut dispatch_node = node.clone();
                            dispatch_node.action = phase_eval.action.clone();
                            if !phase_eval.module.is_empty() {
                                dispatch_node.module = phase_eval.module.clone();
                            }

                            let queued = build_action_dispatch(
                                dispatch_node,
                                &workflow_input,
                                contexts,
                                phase_eval.kwargs,
                            )?;
                            let dispatch_bytes = queued.dispatch.encode_to_vec();
                            let scheduled_at =
                                compute_scheduled_at(&node, &scheduling_ctx.eval_ctx)?;
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
                                    retry_kind,
                                    backoff_kind,
                                    backoff_base_delay_ms,
                                    backoff_multiplier
                                ) VALUES (
                                    $1, $2, $3, 'queued', $4, $5, $6, $7, $8, $9, $10, 0, $11, 'failure', $12, $13, $14
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
                            .bind(queued.backoff.kind_str())
                            .bind(queued.backoff.base_delay_ms())
                            .bind(queued.backoff.multiplier())
                            .fetch_one(tx.as_mut())
                            .await?;
                            dag_state.record_known(node_id.clone());
                            inserted += 1;
                            next_sequence += 1;
                            progressed = true;
                            continue;
                        } else {
                            // No more phases - check if iteration complete
                            if is_iteration_complete(&node, &state) {
                                // Extract result and add to accumulator(s)
                                let result = extract_iteration_result(&node, &state);
                                let mut new_accumulators = state.accumulators.clone();
                                if let Some(result_value) = result {
                                    // Push to the first (and currently only) accumulator
                                    if let Some(acc_name) = accumulator_names.first() {
                                        new_accumulators
                                            .entry(acc_name.clone())
                                            .or_default()
                                            .push(result_value);
                                    }
                                }

                                // Check if there are more iterations
                                let iterable =
                                    loop_ast.iterable.as_ref().context("missing iterable")?;
                                let iterable_value = eval_expr(iterable, &scheduling_ctx.eval_ctx)?;
                                let items_len = match iterable_value {
                                    Value::Array(arr) => arr.len(),
                                    _ => 0,
                                };

                                if state.current_index + 1 < items_len {
                                    // More iterations - update context and re-schedule
                                    for (acc_name, acc_values) in &new_accumulators {
                                        scheduling_ctx.eval_ctx.insert(
                                            acc_name.clone(),
                                            Value::Array(acc_values.clone()),
                                        );
                                    }
                                    scheduling_ctx.eval_ctx.insert(
                                        LOOP_INDEX_VAR.to_string(),
                                        Value::Number(serde_json::Number::from(
                                            (state.current_index + 1) as u64,
                                        )),
                                    );
                                    // Clear phase tracking for new iteration
                                    scheduling_ctx.eval_ctx.remove(LOOP_PHASE_VAR);
                                    scheduling_ctx.eval_ctx.remove(LOOP_PHASE_RESULTS_VAR);
                                    // Don't mark as complete - will re-enter this branch on next iteration
                                    progressed = true;
                                    continue;
                                } else {
                                    // All iterations done - mark node complete
                                    // Return the first (and currently only) accumulator
                                    let final_accumulator = accumulator_names
                                        .first()
                                        .and_then(|name| new_accumulators.get(name))
                                        .cloned()
                                        .unwrap_or_default();
                                    let payload =
                                        encode_value_result(&Value::Array(final_accumulator))?;
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
                                    update_scheduling_context(
                                        &node,
                                        Some(payload),
                                        true,
                                        &mut scheduling_ctx,
                                    )?;
                                    dag_state.record_completion(node_id.clone());
                                    completed_count += 1;
                                    progressed = true;
                                    next_sequence += 1;
                                    continue;
                                }
                            }
                        }
                    }

                    // Fall through to legacy single-action loop handling
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
                                retry_kind,
                                backoff_kind,
                                backoff_base_delay_ms,
                                backoff_multiplier
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
                                'failure',
                                $12,
                                $13,
                                $14
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
                        .bind(queued.backoff.kind_str())
                        .bind(queued.backoff.base_delay_ms())
                        .bind(queued.backoff.multiplier())
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
                        retry_kind,
                        backoff_kind,
                        backoff_base_delay_ms,
                        backoff_multiplier
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
                        'failure',
                        $12,
                        $13,
                        $14
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
                .bind(queued.backoff.kind_str())
                .bind(queued.backoff.base_delay_ms())
                .bind(queued.backoff.multiplier())
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
        let dag_ms = dag_start.elapsed().as_secs_f64() * 1000.0;

        let finalize_start = Instant::now();
        store_instance_result_if_complete(
            tx,
            &instance,
            &cached_version.dag,
            &scheduling_ctx.payloads,
            completed_count,
        )
        .await?;
        let finalize_ms = finalize_start.elapsed().as_secs_f64() * 1000.0;

        let total_ms = fn_start.elapsed().as_secs_f64() * 1000.0;

        // JSON timing output for analysis
        debug!(
            target: "db_timing",
            r#"{{"fn":"schedule_workflow_instance_tx","total_ms":{:.3},"instance_lookup_ms":{:.3},"cache_ms":{:.3},"state_load_ms":{:.3},"dag_ms":{:.3},"finalize_ms":{:.3},"inserted":{}}}"#,
            total_ms, instance_lookup_ms, cache_ms, state_load_ms, dag_ms, finalize_ms, inserted
        );

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
        let database = Database {
            pool: pool.clone(),
            version_cache: Arc::new(RwLock::new(WorkflowVersionCache::new(64))),
        };
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
        let database = Database {
            pool: pool.clone(),
            version_cache: Arc::new(RwLock::new(WorkflowVersionCache::new(64))),
        };
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
