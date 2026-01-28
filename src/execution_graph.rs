//! Execution graph management for instance-local workflow execution.
//!
//! The execution graph is the single source of truth for a workflow instance's
//! state. It replaces the previous multi-table model (action_queue, node_inputs,
//! node_readiness, instance_context) with a single protobuf-encoded blob.
//!
//! Key concepts:
//! - **Reference DAG**: The immutable workflow definition (from workflow_versions)
//! - **Execution Graph**: The mutable execution state (this module)
//! - **Pending Graph**: Nodes expand as spreads fan out (e.g., spread_1[0], spread_1[1], ...)

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use prost::Message;
use tracing::{debug, trace, warn};

use crate::ast_evaluator::{ExpressionEvaluator, Scope};
use crate::dag::{DAG, DAGEdge, EXCEPTION_SCOPE_VAR, EdgeType};

/// Maximum number of loop iterations before terminating execution.
/// This prevents infinite loops (e.g., `while True:`) from running forever.
pub const MAX_LOOP_ITERATIONS: i32 = 50_000;
use crate::dag_state::DAGHelper;
use crate::messages::execution::{
    AttemptRecord, BackoffConfig, BackoffKind, ExceptionInfo, ExecutionGraph, ExecutionNode,
    NodeStatus,
};
use crate::messages::proto::WorkflowArguments;
use crate::messages::{decode_message, encode_message};
use crate::parser::ast;
use crate::value::{WorkflowValue, workflow_value_from_proto_bytes, workflow_value_to_proto_bytes};

/// Result of applying a batch of completions
#[derive(Debug, Default)]
pub struct BatchCompletionResult {
    /// Nodes that became ready after applying completions
    pub newly_ready: Vec<String>,
    /// Whether the workflow completed (output node reached)
    pub workflow_completed: bool,
    /// Result payload if workflow completed
    pub result_payload: Option<Vec<u8>>,
    /// Whether the workflow failed
    pub workflow_failed: bool,
    /// Error message if workflow failed
    pub error_message: Option<String>,
}

/// A completion to apply to the execution graph
#[derive(Debug, Clone)]
pub struct Completion {
    pub node_id: String,
    pub success: bool,
    pub result: Option<Vec<u8>>,
    pub error: Option<String>,
    pub error_type: Option<String>,
    pub worker_id: String,
    /// Total round-trip time from dispatch to result (for metrics)
    pub duration_ms: i64,
    /// Actual time spent executing on the worker (for accurate started_at calculation)
    pub worker_duration_ms: Option<i64>,
}

/// Wrapper around ExecutionGraph with high-level operations
#[derive(Debug, Clone)]
pub struct ExecutionState {
    pub graph: ExecutionGraph,
}

/// Worker identifier used for durable sleep nodes.
pub const SLEEP_WORKER_ID: &str = "sleep";
const SCOPE_DELIM: &str = "::";

impl ExecutionState {
    /// Create a new execution state from protobuf bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        let graph = ExecutionGraph::decode(bytes)?;
        Ok(Self { graph })
    }

    /// Serialize the execution state to protobuf bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.graph.encode_to_vec()
    }

    /// Create an empty execution state
    pub fn new() -> Self {
        Self {
            graph: ExecutionGraph {
                nodes: HashMap::new(),
                variables: HashMap::new(),
                ready_queue: Vec::new(),
                exceptions: HashMap::new(),
                next_wakeup_time: None,
            },
        }
    }

    /// Initialize the execution graph from a DAG and initial inputs
    pub fn initialize_from_dag(&mut self, dag: &DAG, inputs: &WorkflowArguments) {
        let helper = DAGHelper::new(dag);

        // Store initial inputs as variables
        for arg in &inputs.arguments {
            if let Some(value) = &arg.value {
                let encoded = encode_message(value);
                self.graph.variables.insert(arg.key.clone(), encoded);
            }
        }

        // Create execution nodes for all DAG nodes
        for (node_id, node) in &dag.nodes {
            let (max_retries, timeout_seconds, backoff) = Self::extract_policies(&node.policies);

            let exec_node = ExecutionNode {
                template_id: node_id.clone(),
                spread_index: None,
                status: NodeStatus::Blocked as i32,
                worker_id: None,
                started_at_ms: None,
                completed_at_ms: None,
                duration_ms: None,
                inputs: None,
                result: None,
                error: None,
                error_type: None,
                waiting_for: Vec::new(),
                completed_count: 0,
                attempt_number: 0,
                max_retries,
                attempts: Vec::new(),
                timeout_seconds,
                timeout_retry_limit: 3,
                backoff: Some(backoff),
                loop_index: None,
                loop_accumulators: None,
                targets: node.target.clone().map(|t| vec![t]).unwrap_or_default(),
            };

            self.graph.nodes.insert(node_id.clone(), exec_node);
        }

        // Find entry points (nodes with no predecessors) and mark them ready
        for (node_id, node) in &dag.nodes {
            let incoming_count = helper
                .get_incoming_edges(node_id)
                .iter()
                .filter(|edge| edge.edge_type == EdgeType::StateMachine)
                .count();
            if ((incoming_count == 0) || node.is_input)
                && let Some(exec_node) = self.graph.nodes.get_mut(node_id)
            {
                exec_node.status = NodeStatus::Pending as i32;
                self.graph.ready_queue.push(node_id.clone());
            }
        }
    }

    /// Extract retry/timeout/backoff from policy brackets
    fn extract_policies(policies: &[ast::PolicyBracket]) -> (i32, i32, BackoffConfig) {
        let mut max_retries = 3;
        let mut timeout_seconds = 300;
        let mut backoff = BackoffConfig {
            kind: BackoffKind::None as i32,
            base_delay_ms: 0,
            multiplier: 1.0,
        };

        for policy in policies {
            if let Some(kind) = &policy.kind {
                match kind {
                    ast::policy_bracket::Kind::Retry(retry) => {
                        max_retries = retry.max_retries as i32;
                        if let Some(backoff_duration) = &retry.backoff {
                            backoff = BackoffConfig {
                                kind: BackoffKind::Exponential as i32,
                                base_delay_ms: (backoff_duration.seconds * 1000) as i32,
                                multiplier: 2.0,
                            };
                        }
                    }
                    ast::policy_bracket::Kind::Timeout(timeout_policy) => {
                        if let Some(duration) = &timeout_policy.timeout {
                            timeout_seconds = duration.seconds as i32;
                        }
                    }
                }
            }
        }

        (max_retries, timeout_seconds, backoff)
    }

    /// Mark a node as running (dispatched to a worker).
    ///
    /// Note: `started_at_ms` is NOT set here - it will be set when the completion
    /// arrives, computed from `completed_at_ms - worker_duration_ms`. This ensures
    /// the recorded start time reflects when the worker actually started executing,
    /// not when we dispatched the action.
    pub fn mark_running(&mut self, node_id: &str, worker_id: &str, inputs: Option<Vec<u8>>) {
        self.mark_running_inner(node_id, worker_id, inputs, true);
    }

    /// Mark a node as running without removing it from the ready queue.
    /// This allows callers to batch ready_queue removals for performance.
    pub fn mark_running_no_queue_removal(
        &mut self,
        node_id: &str,
        worker_id: &str,
        inputs: Option<Vec<u8>>,
    ) {
        self.mark_running_inner(node_id, worker_id, inputs, false);
    }

    fn mark_running_inner(
        &mut self,
        node_id: &str,
        worker_id: &str,
        inputs: Option<Vec<u8>>,
        remove_ready: bool,
    ) {
        if let Some(node) = self.graph.nodes.get_mut(node_id) {
            node.status = NodeStatus::Running as i32;
            node.worker_id = Some(worker_id.to_string());
            // started_at_ms is set on completion with accurate worker timing
            node.inputs = inputs;
        }

        if remove_ready {
            self.graph.ready_queue.retain(|id| id != node_id);
        }
    }

    /// Drain the ready queue, returning all node IDs that are ready to execute
    pub fn drain_ready_queue(&mut self) -> Vec<String> {
        std::mem::take(&mut self.graph.ready_queue)
    }

    /// Get a copy of the ready queue without draining it.
    /// Used when we need to iterate but only remove specific items.
    pub fn peek_ready_queue(&self) -> Vec<String> {
        self.graph.ready_queue.clone()
    }

    /// Remove a specific node from the ready queue.
    pub fn remove_from_ready_queue(&mut self, node_id: &str) {
        self.graph.ready_queue.retain(|id| id != node_id);
    }

    /// Remove a batch of nodes from the ready queue in one pass.
    pub fn remove_from_ready_queue_batch(&mut self, node_ids: &[String]) {
        if node_ids.is_empty() {
            return;
        }
        let remove_set: HashSet<&str> = node_ids.iter().map(String::as_str).collect();
        self.graph
            .ready_queue
            .retain(|id| !remove_set.contains(id.as_str()));
    }

    /// Check if a node exists and get its status
    pub fn get_node_status(&self, node_id: &str) -> Option<NodeStatus> {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| NodeStatus::try_from(n.status).unwrap_or(NodeStatus::Unspecified))
    }

    /// Get the current attempt number for a node
    pub fn get_attempt_number(&self, node_id: &str) -> u32 {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| n.attempt_number as u32)
            .unwrap_or(0)
    }

    /// Get the max retries for a node
    pub fn get_max_retries(&self, node_id: &str) -> u32 {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| n.max_retries as u32)
            .unwrap_or(0)
    }

    /// Get the timeout in seconds for a node
    pub fn get_timeout_seconds(&self, node_id: &str) -> u32 {
        self.graph
            .nodes
            .get(node_id)
            .map(|n| n.timeout_seconds as u32)
            .unwrap_or(0)
    }

    /// Build a scope from the execution graph's variables for expression evaluation
    pub fn build_scope(&self) -> Scope {
        let mut scope = Scope::new();
        for (name, bytes) in &self.graph.variables {
            if name.contains(SCOPE_DELIM) {
                continue;
            }
            if let Some(value) = workflow_value_from_proto_bytes(bytes) {
                scope.insert(name.clone(), value);
            }
        }
        scope
    }

    pub fn build_scope_for_node(&self, node_id: &str) -> Scope {
        let mut scope = self.build_scope();

        let Some(prefix) = Self::scope_prefix(node_id) else {
            return scope;
        };

        if Self::is_bind_node(node_id) {
            if let Some(parent_prefix) = Self::parent_prefix(prefix) {
                self.merge_scoped_vars(&mut scope, parent_prefix);
            }
        } else {
            self.merge_scoped_vars(&mut scope, prefix);
        }

        scope
    }

    fn build_minimal_scope_for_node(
        &self,
        node_id: &str,
        exec_node: Option<&ExecutionNode>,
        loop_var: Option<&str>,
        required_vars: &HashSet<String>,
    ) -> Scope {
        let mut scope = Scope::new();
        if required_vars.is_empty() {
            return scope;
        }

        let mut prefix = Self::scope_prefix(node_id);
        if Self::is_bind_node(node_id) {
            prefix = prefix.and_then(Self::parent_prefix);
        }

        let template_id = exec_node
            .map(|node| node.template_id.as_str())
            .unwrap_or(node_id);
        let spread_index = exec_node.and_then(|node| node.spread_index);

        for var_name in required_vars {
            let mut value_bytes = None;

            // IMPORTANT: For spread actions, the loop variable must be looked up from the
            // spread-specific storage FIRST, before checking scoped variables. This handles
            // the case where the same variable name was used in an earlier for loop - we
            // don't want the stale value from that loop, we want the spread iteration value.
            if loop_var == Some(var_name.as_str())
                && let Some(index) = spread_index
            {
                let scoped_var = format!("{}[{}].{}", template_id, index, var_name);
                value_bytes = self.graph.variables.get(&scoped_var);
            }

            // Then try scoped variables (from function scope, loops, etc.)
            if value_bytes.is_none()
                && let Some(prefix) = prefix
            {
                let scoped_key = Self::scoped_var_key(prefix, var_name);
                value_bytes = self.graph.variables.get(&scoped_key);
            }

            // Finally try global variables
            if value_bytes.is_none() {
                value_bytes = self.graph.variables.get(var_name);
            }

            if let Some(bytes) = value_bytes
                && let Some(value) = workflow_value_from_proto_bytes(bytes)
            {
                scope.insert(var_name.clone(), value);
            }
        }

        scope
    }

    fn collect_expr_vars(expr: &ast::Expr, vars: &mut HashSet<String>) {
        use ast::expr;

        match &expr.kind {
            Some(expr::Kind::Variable(v)) => {
                vars.insert(v.name.clone());
            }
            Some(expr::Kind::BinaryOp(bin)) => {
                if let Some(left) = bin.left.as_ref() {
                    Self::collect_expr_vars(left, vars);
                }
                if let Some(right) = bin.right.as_ref() {
                    Self::collect_expr_vars(right, vars);
                }
            }
            Some(expr::Kind::UnaryOp(unary)) => {
                if let Some(op) = unary.operand.as_ref() {
                    Self::collect_expr_vars(op, vars);
                }
            }
            Some(expr::Kind::List(list)) => {
                for elem in &list.elements {
                    Self::collect_expr_vars(elem, vars);
                }
            }
            Some(expr::Kind::Dict(dict)) => {
                for entry in &dict.entries {
                    if let Some(key) = entry.key.as_ref() {
                        Self::collect_expr_vars(key, vars);
                    }
                    if let Some(value) = entry.value.as_ref() {
                        Self::collect_expr_vars(value, vars);
                    }
                }
            }
            Some(expr::Kind::Index(index)) => {
                if let Some(obj) = index.object.as_ref() {
                    Self::collect_expr_vars(obj, vars);
                }
                if let Some(idx) = index.index.as_ref() {
                    Self::collect_expr_vars(idx, vars);
                }
            }
            Some(expr::Kind::Dot(dot)) => {
                if let Some(obj) = dot.object.as_ref() {
                    Self::collect_expr_vars(obj, vars);
                }
            }
            Some(expr::Kind::FunctionCall(call)) => {
                for arg in &call.args {
                    Self::collect_expr_vars(arg, vars);
                }
                for kw in &call.kwargs {
                    if let Some(value) = kw.value.as_ref() {
                        Self::collect_expr_vars(value, vars);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn store_variable_for_node(
        &mut self,
        node_id: &str,
        node_type: &str,
        name: &str,
        value: &WorkflowValue,
    ) {
        if node_type == "return" {
            self.store_variable(name, value);
            return;
        }

        if let Some(prefix) = Self::scope_prefix(node_id) {
            let scoped_key = Self::scoped_var_key(prefix, name);
            let bytes = workflow_value_to_proto_bytes(value);
            self.graph.variables.insert(scoped_key, bytes);
            return;
        }

        self.store_variable(name, value);
    }

    pub fn store_raw_variable_for_node(
        &mut self,
        node_id: &str,
        node_type: &str,
        name: &str,
        bytes: Vec<u8>,
    ) {
        if node_type == "return" {
            self.graph.variables.insert(name.to_string(), bytes);
            return;
        }

        if let Some(prefix) = Self::scope_prefix(node_id) {
            let scoped_key = Self::scoped_var_key(prefix, name);
            self.graph.variables.insert(scoped_key, bytes);
            return;
        }

        self.graph.variables.insert(name.to_string(), bytes);
    }

    fn scope_prefix(node_id: &str) -> Option<&str> {
        let (prefix, _) = node_id.rsplit_once(':')?;
        if prefix.contains("fn_call") {
            Some(prefix)
        } else {
            None
        }
    }

    fn parent_prefix(prefix: &str) -> Option<&str> {
        let (parent, _) = prefix.rsplit_once(':')?;
        if parent.contains("fn_call") {
            Some(parent)
        } else {
            None
        }
    }

    fn scoped_var_key(prefix: &str, name: &str) -> String {
        format!("{}{}{}", prefix, SCOPE_DELIM, name)
    }

    fn is_bind_node(node_id: &str) -> bool {
        node_id
            .rsplit(':')
            .next()
            .map(|segment| segment.starts_with("bind_"))
            .unwrap_or(false)
    }

    fn merge_scoped_vars(&self, scope: &mut Scope, prefix: &str) {
        for (name, bytes) in &self.graph.variables {
            let Some((key_prefix, var_name)) = name.split_once(SCOPE_DELIM) else {
                continue;
            };
            if key_prefix != prefix {
                continue;
            }
            if let Some(value) = workflow_value_from_proto_bytes(bytes) {
                scope.insert(var_name.to_string(), value);
            }
        }
    }

    /// Store a value in the variables map
    pub fn store_variable(&mut self, name: &str, value: &WorkflowValue) {
        let bytes = workflow_value_to_proto_bytes(value);
        self.graph.variables.insert(name.to_string(), bytes);
    }

    fn build_error_payload(message: &str) -> Vec<u8> {
        let mut values = HashMap::new();
        values.insert(
            "args".to_string(),
            WorkflowValue::Tuple(vec![WorkflowValue::String(message.to_string())]),
        );
        let error = WorkflowValue::Exception {
            exc_type: "RuntimeError".to_string(),
            module: "builtins".to_string(),
            message: message.to_string(),
            traceback: String::new(),
            values,
            type_hierarchy: vec![
                "RuntimeError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        };
        let args = WorkflowArguments {
            arguments: vec![crate::messages::proto::WorkflowArgument {
                key: "error".to_string(),
                value: Some(error.to_proto()),
            }],
        };
        encode_message(&args)
    }

    /// Evaluate a guard expression against current variables.
    fn evaluate_guard(&self, guard_expr: &ast::Expr, scope: &Scope) -> Result<bool, String> {
        match ExpressionEvaluator::evaluate(guard_expr, scope) {
            Ok(value) => Ok(ExpressionEvaluator::is_truthy(&value)),
            Err(e) => {
                warn!(error = %e, "Failed to evaluate guard expression");
                Err(e.to_string())
            }
        }
    }

    fn extract_exception_info(
        &mut self,
        node_id: &str,
        completion: &Completion,
    ) -> Option<ExceptionInfo> {
        let Some(result_bytes) = &completion.result else {
            return None;
        };

        let Ok(args) = decode_message::<WorkflowArguments>(result_bytes) else {
            return None;
        };

        for arg in args.arguments {
            if arg.key != "error" {
                continue;
            }

            let Some(value) = arg.value else {
                continue;
            };

            let value = WorkflowValue::from_proto(&value);
            if let WorkflowValue::Exception {
                exc_type,
                module,
                message,
                traceback,
                values,
                type_hierarchy,
            } = value
            {
                self.store_variable(
                    EXCEPTION_SCOPE_VAR,
                    &WorkflowValue::Exception {
                        exc_type: exc_type.clone(),
                        module: module.clone(),
                        message: message.clone(),
                        traceback: traceback.clone(),
                        values,
                        type_hierarchy: type_hierarchy.clone(),
                    },
                );

                return Some(ExceptionInfo {
                    error_type: exc_type,
                    error_module: module,
                    error_message: message,
                    traceback,
                    type_hierarchy,
                    source_node_id: node_id.to_string(),
                });
            }
        }

        None
    }

    /// Filter edges by evaluating their guards, returning only enabled edges.
    /// For guarded branches, only edges whose guards evaluate to true are enabled.
    /// For else edges, they're enabled only when no guarded edge was enabled.
    /// For edges without guards, they're always enabled.
    fn filter_edges_by_guards<'a>(
        &self,
        edges: &'a [DAGEdge],
        scope: &Scope,
        dag: &DAG,
    ) -> (Vec<&'a DAGEdge>, Vec<String>) {
        // Log all incoming edges for debugging
        trace!(
            edge_count = edges.len(),
            scope_vars_len = scope.len(),
            "filter_edges_by_guards called"
        );
        if tracing::enabled!(tracing::Level::TRACE) {
            let scope_vars = scope.keys().collect::<Vec<_>>();
            tracing::trace!(
                edge_count = edges.len(),
                scope_vars = ?scope_vars,
                "filter_edges_by_guards called"
            );
            for edge in edges {
                tracing::trace!(
                    source = %edge.source,
                    target = %edge.target,
                    has_guard_expr = edge.guard_expr.is_some(),
                    is_else = edge.is_else,
                    is_loop_back = edge.is_loop_back,
                    guard_string = ?edge.guard_string,
                    edge_type = ?edge.edge_type,
                    "Processing edge in filter_edges_by_guards"
                );
            }
        }

        // Separate edges by type
        let mut guarded_edges: Vec<&DAGEdge> = Vec::new();
        let mut else_edge: Option<&DAGEdge> = None;
        let mut unguarded_edges: Vec<&DAGEdge> = Vec::new();
        let mut guard_errors: Vec<String> = Vec::new();

        for edge in edges {
            if edge.edge_type != EdgeType::StateMachine {
                continue;
            }

            if edge.is_else {
                else_edge = Some(edge);
            } else if edge.guard_expr.is_some() {
                guarded_edges.push(edge);
            } else {
                // No guard - always enabled (but check for guard_string as fallback)
                if let Some(guard_str) = &edge.guard_string {
                    // Guard string like "__loop_i < len(items)" - try to evaluate
                    // This is a fallback for edges that have string-based guards
                    if let Some(dag_node) = dag.nodes.get(&edge.source)
                        && let Some(guard_expr) = &dag_node.guard_expr
                    {
                        match self.evaluate_guard(guard_expr, scope) {
                            Ok(true) => guarded_edges.push(edge),
                            Ok(false) => {}
                            Err(e) => guard_errors.push(format!("Guard evaluation failed: {}", e)),
                        }
                        continue;
                    }
                    // Can't evaluate string guard, treat as enabled
                    trace!(guard_string = %guard_str, "Edge has string guard, treating as enabled");
                    unguarded_edges.push(edge);
                } else {
                    unguarded_edges.push(edge);
                }
            }
        }

        let mut enabled: Vec<&DAGEdge> = Vec::new();

        // Evaluate guarded edges
        let mut any_guard_passed = false;
        for edge in &guarded_edges {
            if let Some(guard_expr) = &edge.guard_expr {
                match self.evaluate_guard(guard_expr, scope) {
                    Ok(true) => {
                        trace!(
                            source = %edge.source,
                            target = %edge.target,
                            "Guard expression passed"
                        );
                        enabled.push(edge);
                        any_guard_passed = true;
                    }
                    Ok(false) => {
                        trace!(
                            source = %edge.source,
                            target = %edge.target,
                            "Guard expression failed"
                        );
                    }
                    Err(e) => guard_errors.push(format!("Guard evaluation failed: {}", e)),
                }
            }
        }

        // If no guarded edge passed and there's an else edge, enable it
        if !any_guard_passed && let Some(else_e) = else_edge {
            trace!(
                source = %else_e.source,
                target = %else_e.target,
                "Enabling else edge (no guards passed)"
            );
            enabled.push(else_e);
        }

        // Always include unguarded edges (normal control flow)
        enabled.extend(unguarded_edges);

        (enabled, guard_errors)
    }

    /// Get the inputs for a node (gathered from variables based on DAG dependencies)
    pub fn get_inputs_for_node(&self, node_id: &str, dag: &DAG) -> Option<Vec<u8>> {
        let exec_node = self.graph.nodes.get(node_id);
        let template_id = exec_node.map(|n| n.template_id.as_str()).unwrap_or(node_id);
        let dag_node = dag.nodes.get(template_id)?;

        // Build kwargs from the DAG node's kwarg expressions when available.
        let mut kwargs = WorkflowArguments { arguments: vec![] };
        let mut required_vars = HashSet::new();
        if let Some(kwarg_exprs) = &dag_node.kwarg_exprs {
            for expr in kwarg_exprs.values() {
                Self::collect_expr_vars(expr, &mut required_vars);
            }
        } else if let Some(kwarg_map) = &dag_node.kwargs {
            for var_ref in kwarg_map.values() {
                required_vars.insert(var_ref.trim_start_matches('$').to_string());
            }
        }

        let scope = self.build_minimal_scope_for_node(
            node_id,
            exec_node,
            dag_node.spread_loop_var.as_deref(),
            &required_vars,
        );

        if let Some(kwarg_exprs) = &dag_node.kwarg_exprs {
            for (key, expr) in kwarg_exprs {
                match ExpressionEvaluator::evaluate(expr, &scope) {
                    Ok(value) => {
                        kwargs
                            .arguments
                            .push(crate::messages::proto::WorkflowArgument {
                                key: key.clone(),
                                value: Some(value.to_proto()),
                            });
                    }
                    Err(e) => {
                        warn!(
                            node_id = %node_id,
                            key = %key,
                            error = %e,
                            "Failed to evaluate kwarg expression"
                        );
                    }
                }
            }
        } else if let Some(kwarg_map) = &dag_node.kwargs {
            for (key, var_ref) in kwarg_map {
                // var_ref is like "$variable_name"
                let var_name = var_ref.trim_start_matches('$');
                if let Some(value) = scope.get(var_name) {
                    kwargs
                        .arguments
                        .push(crate::messages::proto::WorkflowArgument {
                            key: key.clone(),
                            value: Some(value.to_proto()),
                        });
                }
            }
        }

        Some(encode_message(&kwargs))
    }

    /// Apply a batch of completions and compute newly ready nodes
    pub fn apply_completions_batch(
        &mut self,
        completions: Vec<Completion>,
        dag: &DAG,
    ) -> BatchCompletionResult {
        // Log entry for debugging
        debug!(
            completion_count = completions.len(),
            "apply_completions_batch called"
        );
        if tracing::enabled!(tracing::Level::TRACE) {
            let completion_ids: Vec<_> = completions.iter().map(|c| c.node_id.as_str()).collect();
            tracing::trace!(
                completion_count = completions.len(),
                node_ids = ?completion_ids,
                "apply_completions_batch called"
            );
        }

        let mut result = BatchCompletionResult::default();
        let helper = DAGHelper::new(dag);
        let now_ms = Utc::now().timestamp_millis();
        let mut exception_routes: HashMap<String, Vec<DAGEdge>> = HashMap::new();

        // 1. Mark all completed nodes and record attempts
        for completion in &completions {
            let edge_source_id = self
                .graph
                .nodes
                .get(&completion.node_id)
                .map(|n| n.template_id.clone())
                .unwrap_or_else(|| completion.node_id.clone());
            let can_retry = self
                .graph
                .nodes
                .get(&completion.node_id)
                .map(|n| n.attempt_number < n.max_retries)
                .unwrap_or(false);
            let mut exception_info = None;
            if !completion.success && !can_retry {
                exception_info = self.extract_exception_info(&completion.node_id, completion);
            }

            let node_type = self
                .graph
                .nodes
                .get(&completion.node_id)
                .and_then(|exec| dag.nodes.get(&exec.template_id))
                .map(|n| n.node_type.clone())
                .unwrap_or_default();

            if let Some(node) = self.graph.nodes.get_mut(&completion.node_id) {
                // Compute started_at from worker_duration if available (most accurate),
                // otherwise fall back to previously set value or current time
                let started_at = completion
                    .worker_duration_ms
                    .map(|wd| now_ms - wd)
                    .or(node.started_at_ms)
                    .unwrap_or(now_ms);

                // Set started_at_ms now that we have accurate timing
                if node.started_at_ms.is_none() {
                    node.started_at_ms = Some(started_at);
                }

                let attempt = AttemptRecord {
                    attempt_number: node.attempt_number,
                    worker_id: completion.worker_id.clone(),
                    started_at_ms: started_at,
                    completed_at_ms: now_ms,
                    duration_ms: completion.duration_ms,
                    success: completion.success,
                    result: completion.result.clone(),
                    error: completion.error.clone(),
                    error_type: completion.error_type.clone(),
                };
                node.attempts.push(attempt);

                if completion.success {
                    node.status = NodeStatus::Completed as i32;
                    node.result = completion.result.clone();
                    node.completed_at_ms = Some(now_ms);
                    node.duration_ms = Some(completion.duration_ms);
                    node.worker_id = None;

                    // Store result in variables if node has targets
                    // The result_bytes is a WorkflowArguments containing {key: "result", value: <actual_value>}
                    // We need to extract the actual WorkflowArgumentValue and store that
                    if !node.targets.is_empty()
                        && let Some(result_bytes) = &completion.result
                    {
                        // Decode as WorkflowArguments and extract the "result" entry's value
                        if let Ok(args) = decode_message::<WorkflowArguments>(result_bytes) {
                            for arg in &args.arguments {
                                if arg.key == "result" {
                                    if let Some(value) = &arg.value {
                                        let workflow_value = WorkflowValue::from_proto(value);
                                        let targets = node.targets.clone();

                                        // Unpack tuples/lists when there are multiple targets
                                        if targets.len() > 1 {
                                            match &workflow_value {
                                                WorkflowValue::Tuple(items)
                                                | WorkflowValue::List(items) => {
                                                    for (target, item) in
                                                        targets.iter().zip(items.iter())
                                                    {
                                                        self.store_variable_for_node(
                                                            &completion.node_id,
                                                            &node_type,
                                                            target,
                                                            item,
                                                        );
                                                    }
                                                }
                                                _ => {
                                                    // Non-iterable value with multiple targets - store as-is
                                                    warn!(
                                                        node_id = %completion.node_id,
                                                        targets = ?targets,
                                                        "Value is not iterable for tuple unpacking"
                                                    );
                                                    for target in &targets {
                                                        self.store_variable_for_node(
                                                            &completion.node_id,
                                                            &node_type,
                                                            target,
                                                            &workflow_value,
                                                        );
                                                    }
                                                }
                                            }
                                        } else {
                                            // Single target - store entire value
                                            for target in &targets {
                                                self.store_variable_for_node(
                                                    &completion.node_id,
                                                    &node_type,
                                                    target,
                                                    &workflow_value,
                                                );
                                            }
                                        }
                                    }
                                    break;
                                }
                            }
                        } else {
                            // Fallback: store raw bytes if decode fails
                            warn!(
                                node_id = %completion.node_id,
                                "Failed to decode result as WorkflowArguments, storing raw bytes"
                            );
                            for target in &node.targets.clone() {
                                self.store_raw_variable_for_node(
                                    &completion.node_id,
                                    &node_type,
                                    target,
                                    result_bytes.clone(),
                                );
                            }
                        }
                    }
                } else {
                    // Check if we can retry
                    if can_retry {
                        node.attempt_number += 1;
                        node.status = NodeStatus::Pending as i32;
                        node.worker_id = None;
                        self.graph.ready_queue.push(completion.node_id.clone());
                    } else {
                        if exception_info.is_none()
                            && let Some(error_type) = &completion.error_type
                        {
                            exception_info = Some(ExceptionInfo {
                                error_type: error_type.clone(),
                                error_module: String::new(),
                                error_message: completion.error.clone().unwrap_or_default(),
                                traceback: String::new(),
                                type_hierarchy: vec![error_type.clone()],
                                source_node_id: completion.node_id.clone(),
                            });
                        }

                        if let Some(info) = exception_info.clone() {
                            self.graph
                                .exceptions
                                .insert(completion.node_id.clone(), info);
                        }

                        let exception_edges: Vec<DAGEdge> = helper
                            .get_outgoing_edges(&edge_source_id)
                            .iter()
                            .filter(|edge| {
                                edge.edge_type == EdgeType::StateMachine
                                    && edge.exception_types.is_some()
                            })
                            .map(|edge| (*edge).clone())
                            .collect();

                        let matching_exception_edges: Vec<DAGEdge> = exception_edges
                            .into_iter()
                            .filter(|edge| {
                                let Some(types) = edge.exception_types.as_ref() else {
                                    return false;
                                };

                                if types.is_empty() {
                                    return true;
                                }

                                if let Some(info) = &exception_info {
                                    if info.type_hierarchy.iter().any(|t| types.contains(t)) {
                                        return true;
                                    }
                                    return types.contains(&info.error_type);
                                }

                                completion
                                    .error_type
                                    .as_ref()
                                    .map(|t| types.contains(t))
                                    .unwrap_or(false)
                            })
                            .collect();

                        let matching_exception_edges = if !matching_exception_edges.is_empty() {
                            let max_depth = matching_exception_edges
                                .iter()
                                .map(|edge| edge.exception_depth.unwrap_or(0))
                                .max()
                                .unwrap_or(0);
                            let edges_at_max_depth: Vec<_> = matching_exception_edges
                                .into_iter()
                                .filter(|edge| edge.exception_depth.unwrap_or(0) == max_depth)
                                .collect();
                            // Select only the first matching handler - Python semantics require
                            // that only the first matching except handler executes
                            if let Some(first) = edges_at_max_depth.into_iter().next() {
                                vec![first]
                            } else {
                                vec![]
                            }
                        } else {
                            matching_exception_edges
                        };

                        if !matching_exception_edges.is_empty() {
                            node.status = NodeStatus::Caught as i32;
                            node.error = completion.error.clone();
                            node.error_type = completion.error_type.clone();
                            node.completed_at_ms = Some(now_ms);
                            exception_routes
                                .insert(completion.node_id.clone(), matching_exception_edges);
                        } else {
                            node.status = NodeStatus::Exhausted as i32;
                            node.error = completion.error.clone();
                            node.error_type = completion.error_type.clone();
                            node.completed_at_ms = Some(now_ms);

                            result.workflow_failed = true;
                            result.error_message = completion.error.clone();
                        }
                    }
                }
            }
        }

        // 2. Build scope for guard evaluation
        let is_completed = |status: i32| {
            matches!(
                NodeStatus::try_from(status).unwrap_or(NodeStatus::Unspecified),
                NodeStatus::Completed | NodeStatus::Caught
            )
        };

        // 3. Find successors that should become ready, evaluating guards
        let mut ready_successors: Vec<(String, Option<Vec<Vec<u8>>>)> = Vec::new();

        for completion in &completions {
            let edge_source_id = self
                .graph
                .nodes
                .get(&completion.node_id)
                .map(|n| n.template_id.as_str())
                .unwrap_or(completion.node_id.as_str());
            let outgoing_edges: Vec<DAGEdge> = if completion.success {
                // Get all outgoing state machine edges from the completed node
                helper
                    .get_state_machine_successors(edge_source_id)
                    .into_iter()
                    .cloned()
                    .collect()
            } else if let Some(edges) = exception_routes.get(&completion.node_id) {
                edges.clone()
            } else {
                continue;
            };

            trace!(
                source_node = %completion.node_id,
                outgoing_edge_count = outgoing_edges.len(),
                "Found outgoing edges for completed node"
            );

            // Evaluate guards to determine which edges are enabled
            let mut guard_vars = HashSet::new();
            for edge in &outgoing_edges {
                if let Some(expr) = edge.guard_expr.as_ref() {
                    Self::collect_expr_vars(expr, &mut guard_vars);
                }
            }
            let exec_node = self.graph.nodes.get(&completion.node_id);
            let loop_var = exec_node
                .and_then(|node| dag.nodes.get(&node.template_id))
                .and_then(|node| node.spread_loop_var.as_deref());
            let scope = self.build_minimal_scope_for_node(
                &completion.node_id,
                exec_node,
                loop_var,
                &guard_vars,
            );
            let (enabled_edges, guard_errors) =
                self.filter_edges_by_guards(&outgoing_edges, &scope, dag);
            if !guard_errors.is_empty() {
                let message = format!(
                    "Guard evaluation failed during startup: {}",
                    guard_errors.join("; ")
                );
                result.workflow_failed = true;
                result.error_message = Some(message.clone());
                result.result_payload = Some(Self::build_error_payload(&message));
                return result;
            }

            if tracing::enabled!(tracing::Level::TRACE) {
                let enabled_targets = enabled_edges
                    .iter()
                    .map(|e| e.target.as_str())
                    .collect::<Vec<_>>();
                tracing::trace!(
                    source_node = %completion.node_id,
                    enabled_edge_count = enabled_edges.len(),
                    enabled_targets = ?enabled_targets,
                    "Filtered enabled edges"
                );
            }

            for edge in enabled_edges {
                let successor_id = &edge.target;

                // Handle loop-back edges: reset node status to allow re-visitation
                if edge.is_loop_back {
                    self.reset_nodes_for_loop(successor_id, &edge.source, dag);

                    if let Some(successor) = self.graph.nodes.get_mut(successor_id) {
                        // Increment loop_index for tracking
                        let current_idx = successor.loop_index.unwrap_or(0);
                        let new_idx = current_idx + 1;
                        successor.loop_index = Some(new_idx);
                        trace!(
                            node_id = %successor_id,
                            loop_index = new_idx,
                            "Reset node for loop iteration"
                        );

                        // Check for infinite loop
                        if new_idx > MAX_LOOP_ITERATIONS {
                            let message = format!(
                                "Loop exceeded maximum iterations ({}) at node '{}'. \
                                 This may indicate an infinite loop (e.g., `while True:`).",
                                MAX_LOOP_ITERATIONS, successor_id
                            );
                            warn!(
                                node_id = %successor_id,
                                iterations = new_idx,
                                max = MAX_LOOP_ITERATIONS,
                                "Loop exceeded max iterations, terminating workflow"
                            );
                            result.workflow_failed = true;
                            result.error_message = Some(message.clone());
                            result.result_payload = Some(Self::build_error_payload(&message));
                            return result;
                        }
                    }
                }

                let successor = match self.graph.nodes.get(successor_id) {
                    Some(n) => n,
                    None => {
                        debug!(successor_id = %successor_id, "Successor node not found in execution graph");
                        continue;
                    }
                };

                // Check current status
                let status =
                    NodeStatus::try_from(successor.status).unwrap_or(NodeStatus::Unspecified);
                trace!(
                    successor_id = %successor_id,
                    status = ?status,
                    "Checking successor status"
                );
                if status != NodeStatus::Blocked {
                    trace!(
                        successor_id = %successor_id,
                        status = ?status,
                        "Skipping successor - not blocked"
                    );
                    continue;
                }

                // Exception handler: if the edge has exception_types, the handler is ready
                // immediately because the exception has already occurred and been matched.
                // We don't need to wait for other potential exception sources.
                if edge.exception_types.is_some() {
                    trace!(
                        successor_id = %successor_id,
                        exception_types = ?edge.exception_types,
                        "Exception handler ready via exception edge"
                    );
                    if !ready_successors.iter().any(|(id, _)| id == successor_id) {
                        ready_successors.push((successor_id.clone(), None));
                        trace!(successor_id = %successor_id, "Added exception handler to ready_successors");
                    }
                    continue;
                }

                // Check if this is a barrier (has waiting_for)
                if !successor.waiting_for.is_empty() {
                    // Count completed predecessors
                    let completed = successor
                        .waiting_for
                        .iter()
                        .filter(|id| {
                            self.graph
                                .nodes
                                .get(*id)
                                .map(|n| is_completed(n.status))
                                .unwrap_or(false)
                        })
                        .count();

                    if completed == successor.waiting_for.len() {
                        // All predecessors done - barrier is ready
                        let aggregated: Vec<Vec<u8>> = successor
                            .waiting_for
                            .iter()
                            .filter_map(|id| {
                                self.graph.nodes.get(id).and_then(|n| n.result.clone())
                            })
                            .collect();

                        if !ready_successors.iter().any(|(id, _)| id == successor_id) {
                            ready_successors.push((successor_id.clone(), Some(aggregated)));
                        }
                    }
                } else {
                    // Regular node - check if predecessors are ready
                    // Some nodes (joins after conditionals) only require N predecessors, not all
                    let template_id = &successor.template_id;
                    let incoming: Vec<&DAGEdge> = helper
                        .get_incoming_edges(template_id)
                        .iter()
                        .filter(|edge| {
                            edge.edge_type == EdgeType::StateMachine
                                && edge.exception_types.is_none()
                        })
                        .copied()
                        .collect();

                    trace!(
                        successor_id = %successor_id,
                        template_id = %template_id,
                        incoming_edge_count = incoming.len(),
                        "Checking incoming edges for successor"
                    );

                    // Check if this node has a join_required_count override
                    let join_required = dag
                        .nodes
                        .get(template_id)
                        .and_then(|n| n.join_required_count)
                        .map(|c| c as usize);

                    // Count completed predecessors and total non-loop-back edges
                    let mut completed_count = 0usize;
                    let mut total_non_loopback = 0usize;

                    for inc_edge in incoming {
                        // Skip loop-back edges when checking readiness (they don't block forward progress)
                        if inc_edge.is_loop_back {
                            trace!(
                                successor_id = %successor_id,
                                edge_source = %inc_edge.source,
                                "Skipping loop-back edge"
                            );
                            continue;
                        }

                        total_non_loopback += 1;

                        let source_status = self
                            .graph
                            .nodes
                            .get(&inc_edge.source)
                            .map(|n| is_completed(n.status))
                            .unwrap_or(false);
                        trace!(
                            successor_id = %successor_id,
                            edge_source = %inc_edge.source,
                            source_completed = source_status,
                            "Checking incoming edge source status"
                        );

                        if source_status {
                            completed_count += 1;
                        }
                    }

                    // Determine if the node is ready based on join_required_count or all-edges logic
                    let is_ready = if let Some(required) = join_required {
                        // Join node with required count - ready when at least N predecessors complete
                        completed_count >= required
                    } else {
                        // Regular node - all non-loop-back edges must be done
                        completed_count == total_non_loopback
                    };

                    trace!(
                        successor_id = %successor_id,
                        completed = completed_count,
                        total = total_non_loopback,
                        join_required = ?join_required,
                        is_ready = is_ready,
                        "Incoming edges check result"
                    );

                    if is_ready && !ready_successors.iter().any(|(id, _)| id == successor_id) {
                        ready_successors.push((successor_id.clone(), None));
                        trace!(successor_id = %successor_id, "Added successor to ready_successors");
                    }
                }
            }
        }

        // 4. Update completed_count for barrier nodes
        for (successor_id, _) in &ready_successors {
            if let Some(successor) = self.graph.nodes.get(successor_id)
                && !successor.waiting_for.is_empty()
            {
                let completed = successor
                    .waiting_for
                    .iter()
                    .filter(|id| {
                        self.graph
                            .nodes
                            .get(*id)
                            .map(|n| is_completed(n.status))
                            .unwrap_or(false)
                    })
                    .count() as i32;

                if let Some(successor_mut) = self.graph.nodes.get_mut(successor_id) {
                    successor_mut.completed_count = completed;
                }
            }
        }

        // 5. Mark nodes as ready and store aggregated results
        for (successor_id, _aggregated) in ready_successors {
            if let Some(successor) = self.graph.nodes.get_mut(&successor_id) {
                successor.status = NodeStatus::Pending as i32;
            }

            result.newly_ready.push(successor_id.clone());
            self.graph.ready_queue.push(successor_id);
        }

        // 4. Check for workflow completion (entry function output node only)
        let entry_function = dag
            .nodes
            .values()
            .find(|n| n.is_input)
            .and_then(|n| n.function_name.clone());

        for node in dag.nodes.values() {
            let is_entry_output = node.is_output
                && entry_function
                    .as_deref()
                    .map(|name| node.function_name.as_deref() == Some(name))
                    .unwrap_or(true);

            if is_entry_output
                && let Some(exec_node) = self.graph.nodes.get(&node.id)
                && exec_node.status == NodeStatus::Completed as i32
            {
                result.workflow_completed = true;

                // Build result payload from the "result" variable
                // The Python side expects WorkflowArguments with key "result"
                if let Some(result_value_bytes) = self.graph.variables.get("result") {
                    // result_value_bytes is a WorkflowArgumentValue
                    // Wrap it in WorkflowArguments with key "result"
                    if let Ok(value) = decode_message::<crate::messages::proto::WorkflowArgumentValue>(
                        result_value_bytes,
                    ) {
                        let args = WorkflowArguments {
                            arguments: vec![crate::messages::proto::WorkflowArgument {
                                key: "result".to_string(),
                                value: Some(value),
                            }],
                        };
                        result.result_payload = Some(encode_message(&args));
                    }
                } else {
                    let args = WorkflowArguments {
                        arguments: vec![crate::messages::proto::WorkflowArgument {
                            key: "result".to_string(),
                            value: Some(WorkflowValue::Null.to_proto()),
                        }],
                    };
                    result.result_payload = Some(encode_message(&args));
                }
            }
        }

        result
    }

    fn reset_nodes_for_loop(&mut self, loop_head_id: &str, loop_back_source: &str, dag: &DAG) {
        let nodes_in_loop = Self::collect_loop_nodes(loop_head_id, loop_back_source, dag);
        if nodes_in_loop.is_empty() {
            return;
        }

        for node_id in &nodes_in_loop {
            if let Some(node) = self.graph.nodes.get_mut(node_id) {
                Self::reset_node_for_loop(node);
            }
        }

        self.graph
            .ready_queue
            .retain(|id| !nodes_in_loop.contains(id));
    }

    fn collect_loop_nodes(
        loop_head_id: &str,
        loop_back_source: &str,
        dag: &DAG,
    ) -> HashSet<String> {
        let forward = Self::reachable_from(loop_head_id, dag);
        let backward = Self::reachable_to(loop_back_source, dag);
        forward
            .intersection(&backward)
            .cloned()
            .collect::<HashSet<String>>()
    }

    fn reachable_from(start: &str, dag: &DAG) -> HashSet<String> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut stack = vec![start.to_string()];
        visited.insert(start.to_string());

        while let Some(current) = stack.pop() {
            for edge in dag.edges.iter().filter(|edge| {
                edge.edge_type == EdgeType::StateMachine
                    && !edge.is_loop_back
                    && edge.source == current
            }) {
                if visited.insert(edge.target.clone()) {
                    stack.push(edge.target.clone());
                }
            }
        }

        visited
    }

    fn reachable_to(target: &str, dag: &DAG) -> HashSet<String> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut stack = vec![target.to_string()];
        visited.insert(target.to_string());

        while let Some(current) = stack.pop() {
            for edge in dag.edges.iter().filter(|edge| {
                edge.edge_type == EdgeType::StateMachine
                    && !edge.is_loop_back
                    && edge.target == current
            }) {
                if visited.insert(edge.source.clone()) {
                    stack.push(edge.source.clone());
                }
            }
        }

        visited
    }

    fn reset_node_for_loop(node: &mut ExecutionNode) {
        node.status = NodeStatus::Blocked as i32;
        node.worker_id = None;
        node.started_at_ms = None;
        node.completed_at_ms = None;
        node.duration_ms = None;
        node.inputs = None;
        node.result = None;
        node.error = None;
        node.error_type = None;
        node.waiting_for.clear();
        node.completed_count = 0;
        node.attempt_number = 0;
        node.attempts.clear();
    }

    /// Expand a spread node into N concrete execution nodes
    pub fn expand_spread(&mut self, spread_node_id: &str, items: Vec<WorkflowValue>, dag: &DAG) {
        let dag_node = match dag.nodes.get(spread_node_id) {
            Some(n) => n,
            None => {
                warn!("Spread node {} not found in DAG", spread_node_id);
                return;
            }
        };

        let barrier_id = match &dag_node.aggregates_to {
            Some(id) => id.clone(),
            None => {
                warn!("Spread node {} has no aggregates_to", spread_node_id);
                return;
            }
        };

        let loop_var = dag_node.spread_loop_var.as_deref().unwrap_or("item");

        // Create N concrete execution nodes
        let expanded_ids: Vec<String> = items
            .into_iter()
            .enumerate()
            .map(|(i, item)| {
                let node_id = format!("{}[{}]", spread_node_id, i);

                // Get template node's config
                let template = self.graph.nodes.get(spread_node_id).cloned();

                let exec_node = ExecutionNode {
                    template_id: spread_node_id.to_string(),
                    spread_index: Some(i as i32),
                    status: NodeStatus::Pending as i32,
                    worker_id: None,
                    started_at_ms: None,
                    completed_at_ms: None,
                    duration_ms: None,
                    inputs: None,
                    result: None,
                    error: None,
                    error_type: None,
                    waiting_for: Vec::new(),
                    completed_count: 0,
                    attempt_number: 0,
                    max_retries: template.as_ref().map(|t| t.max_retries).unwrap_or(3),
                    attempts: Vec::new(),
                    timeout_seconds: template.as_ref().map(|t| t.timeout_seconds).unwrap_or(300),
                    timeout_retry_limit: 3,
                    backoff: template.and_then(|t| t.backoff),
                    loop_index: None,
                    loop_accumulators: None,
                    targets: vec![],
                };

                self.graph.nodes.insert(node_id.clone(), exec_node);
                self.graph.ready_queue.push(node_id.clone());

                // Store the spread item as a variable for this iteration
                let var_name = format!("{}[{}].{}", spread_node_id, i, loop_var);
                let item_bytes = workflow_value_to_proto_bytes(&item);
                self.graph.variables.insert(var_name, item_bytes);

                node_id
            })
            .collect();

        // Update barrier to wait for all expanded nodes
        if let Some(barrier) = self.graph.nodes.get_mut(&barrier_id) {
            barrier.waiting_for = expanded_ids.clone();
            barrier.completed_count = 0;
        }

        debug!(
            spread_node = %spread_node_id,
            barrier = %barrier_id,
            count = expanded_ids.len(),
            "Expanded spread into concrete nodes"
        );
    }

    /// Check if there are any nodes still running or pending
    pub fn has_pending_work(&self) -> bool {
        self.graph.nodes.values().any(|n| {
            let status = NodeStatus::try_from(n.status).unwrap_or(NodeStatus::Unspecified);
            matches!(status, NodeStatus::Pending | NodeStatus::Running)
        }) || !self.graph.ready_queue.is_empty()
    }

    /// Get all nodes currently marked as running (for recovery after crash)
    pub fn get_running_nodes(&self) -> Vec<String> {
        self.graph
            .nodes
            .iter()
            .filter(|(_, n)| n.status == NodeStatus::Running as i32)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Reset running nodes to pending (for crash recovery)
    pub fn recover_running_nodes(&mut self) {
        let running_ids: Vec<String> = self.get_running_nodes();

        for node_id in running_ids {
            if let Some(node) = self.graph.nodes.get_mut(&node_id) {
                if node.worker_id.as_deref() == Some(SLEEP_WORKER_ID) {
                    debug!(
                        node_id = %node_id,
                        "Preserving durable sleep node during recovery"
                    );
                    continue;
                }
                // Check if we can retry
                if node.attempt_number < node.max_retries {
                    node.attempt_number += 1;
                    node.status = NodeStatus::Pending as i32;
                    node.worker_id = None;
                    node.started_at_ms = None;
                    self.graph.ready_queue.push(node_id.clone());
                    debug!(node_id = %node_id, "Recovered running node to pending");
                } else {
                    node.status = NodeStatus::Exhausted as i32;
                    node.error = Some("Owner crashed, max retries exceeded".to_string());
                    debug!(node_id = %node_id, "Running node exhausted retries");
                }
            }
        }
    }

    /// Find completed nodes whose successors haven't been advanced.
    ///
    /// This detects the "limbo state" where a node completed successfully but
    /// the runner crashed before `apply_completions_batch` could determine the
    /// next nodes. Returns synthetic [`Completion`] objects that can be
    /// re-processed through the normal completion flow to advance the workflow.
    pub fn find_stalled_completions(&self, dag: &DAG) -> Vec<Completion> {
        let helper = DAGHelper::new(dag);
        let ready_set: HashSet<&str> = self.graph.ready_queue.iter().map(|s| s.as_str()).collect();
        let mut stalled = Vec::new();

        for (node_id, node) in &self.graph.nodes {
            if node.status != NodeStatus::Completed as i32 {
                continue;
            }

            // Check if any outgoing state-machine edge leads to a BLOCKED
            // successor that isn't already in the ready_queue.
            let has_stalled_successor = helper
                .get_state_machine_successors(&node.template_id)
                .iter()
                .any(|edge| {
                    if ready_set.contains(edge.target.as_str()) {
                        return false;
                    }
                    self.graph
                        .nodes
                        .get(&edge.target)
                        .map(|n| n.status == NodeStatus::Blocked as i32)
                        .unwrap_or(false)
                });

            if has_stalled_successor {
                debug!(
                    node_id = %node_id,
                    "Found stalled completion: node completed but successor still blocked"
                );
                stalled.push(Completion {
                    node_id: node_id.clone(),
                    success: true,
                    result: node.result.clone(),
                    error: None,
                    error_type: None,
                    worker_id: "recovery".to_string(),
                    duration_ms: 0,
                    worker_duration_ms: Some(0),
                });
            }
        }

        if !stalled.is_empty() {
            debug!(
                stalled_count = stalled.len(),
                "Found stalled completions for recovery"
            );
        }

        stalled
    }
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_state_roundtrip() {
        let mut state = ExecutionState::new();
        state.graph.ready_queue.push("test_node".to_string());
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.graph.ready_queue, vec!["test_node".to_string()]);
        assert!(recovered.graph.nodes.contains_key("test_node"));
    }

    #[test]
    fn test_mark_running() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.ready_queue.push("test_node".to_string());

        state.mark_running("test_node", "worker-1", None);

        let node = state.graph.nodes.get("test_node").unwrap();
        assert_eq!(node.status, NodeStatus::Running as i32);
        assert_eq!(node.worker_id, Some("worker-1".to_string()));
        assert!(state.graph.ready_queue.is_empty());
    }

    #[test]
    fn test_drain_ready_queue() {
        let mut state = ExecutionState::new();
        state.graph.ready_queue.push("node_a".to_string());
        state.graph.ready_queue.push("node_b".to_string());

        let ready = state.drain_ready_queue();

        assert_eq!(ready, vec!["node_a".to_string(), "node_b".to_string()]);
        assert!(state.graph.ready_queue.is_empty());
    }

    #[test]
    fn test_has_pending_work() {
        let mut state = ExecutionState::new();
        assert!(!state.has_pending_work());

        // Add a pending node
        state.graph.nodes.insert(
            "pending_node".to_string(),
            ExecutionNode {
                template_id: "pending_node".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        assert!(state.has_pending_work());

        // Mark as completed
        state.graph.nodes.get_mut("pending_node").unwrap().status = NodeStatus::Completed as i32;
        assert!(!state.has_pending_work());

        // Add to ready queue
        state.graph.ready_queue.push("another".to_string());
        assert!(state.has_pending_work());
    }

    #[test]
    fn test_get_running_nodes() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "running_1".to_string(),
            ExecutionNode {
                template_id: "running_1".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "completed_1".to_string(),
            ExecutionNode {
                template_id: "completed_1".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "running_2".to_string(),
            ExecutionNode {
                template_id: "running_2".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );

        let running = state.get_running_nodes();
        assert_eq!(running.len(), 2);
        assert!(running.contains(&"running_1".to_string()));
        assert!(running.contains(&"running_2".to_string()));
    }

    #[test]
    fn test_recover_running_nodes_with_retries() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "running_node".to_string(),
            ExecutionNode {
                template_id: "running_node".to_string(),
                status: NodeStatus::Running as i32,
                attempt_number: 0,
                max_retries: 3,
                worker_id: Some("old-worker".to_string()),
                ..Default::default()
            },
        );

        state.recover_running_nodes();

        let node = state.graph.nodes.get("running_node").unwrap();
        assert_eq!(node.status, NodeStatus::Pending as i32);
        assert_eq!(node.attempt_number, 1);
        assert!(node.worker_id.is_none());
        assert!(
            state
                .graph
                .ready_queue
                .contains(&"running_node".to_string())
        );
    }

    #[test]
    fn test_recover_running_nodes_exhausted() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "exhausted_node".to_string(),
            ExecutionNode {
                template_id: "exhausted_node".to_string(),
                status: NodeStatus::Running as i32,
                attempt_number: 3, // Already at max
                max_retries: 3,
                ..Default::default()
            },
        );

        state.recover_running_nodes();

        let node = state.graph.nodes.get("exhausted_node").unwrap();
        assert_eq!(node.status, NodeStatus::Exhausted as i32);
        assert!(node.error.is_some());
        assert!(
            !state
                .graph
                .ready_queue
                .contains(&"exhausted_node".to_string())
        );
    }

    #[test]
    fn test_get_node_status() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );

        assert_eq!(
            state.get_node_status("test_node"),
            Some(NodeStatus::Running)
        );
        assert_eq!(state.get_node_status("nonexistent"), None);
    }

    #[test]
    fn test_variables_storage() {
        let mut state = ExecutionState::new();

        // Store a variable
        let value_bytes = vec![1, 2, 3, 4];
        state
            .graph
            .variables
            .insert("my_var".to_string(), value_bytes.clone());

        // Verify it's stored
        assert_eq!(state.graph.variables.get("my_var"), Some(&value_bytes));

        // Roundtrip through serialization
        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.graph.variables.get("my_var"), Some(&value_bytes));
    }

    #[test]
    fn test_exceptions_storage() {
        let mut state = ExecutionState::new();

        state.graph.exceptions.insert(
            "node_1".to_string(),
            ExceptionInfo {
                error_type: "ValueError".to_string(),
                error_module: "builtins".to_string(),
                error_message: "invalid value".to_string(),
                traceback: "line 1".to_string(),
                type_hierarchy: vec!["ValueError".to_string(), "Exception".to_string()],
                source_node_id: "node_1".to_string(),
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let exc = recovered.graph.exceptions.get("node_1").unwrap();
        assert_eq!(exc.error_type, "ValueError");
        assert_eq!(exc.error_message, "invalid value");
    }

    #[test]
    fn test_build_error_payload_serializes_exception() {
        let payload = ExecutionState::build_error_payload("boom");
        let args: WorkflowArguments = decode_message(&payload).expect("decode payload");
        let entry = args
            .arguments
            .iter()
            .find(|arg| arg.key == "error")
            .expect("error entry");
        let value = entry.value.as_ref().expect("error value");
        let value = WorkflowValue::from_proto(value);

        match value {
            WorkflowValue::Exception {
                exc_type,
                module,
                message,
                values,
                type_hierarchy,
                ..
            } => {
                assert_eq!(exc_type, "RuntimeError");
                assert_eq!(module, "builtins");
                assert_eq!(message, "boom");
                assert_eq!(
                    type_hierarchy,
                    vec![
                        "RuntimeError".to_string(),
                        "Exception".to_string(),
                        "BaseException".to_string(),
                    ]
                );
                match values.get("args") {
                    Some(WorkflowValue::Tuple(items)) => {
                        assert_eq!(items, &vec![WorkflowValue::String("boom".to_string())]);
                    }
                    other => panic!("expected args tuple, got {other:?}"),
                }
            }
            other => panic!("expected exception payload, got {other:?}"),
        }
    }

    #[test]
    fn test_attempt_record_storage() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Completed as i32,
                attempts: vec![
                    AttemptRecord {
                        attempt_number: 0,
                        worker_id: "worker-1".to_string(),
                        started_at_ms: 1000,
                        completed_at_ms: 2000,
                        duration_ms: 1000,
                        success: false,
                        result: None,
                        error: Some("timeout".to_string()),
                        error_type: Some("TimeoutError".to_string()),
                    },
                    AttemptRecord {
                        attempt_number: 1,
                        worker_id: "worker-2".to_string(),
                        started_at_ms: 3000,
                        completed_at_ms: 4000,
                        duration_ms: 1000,
                        success: true,
                        result: Some(vec![42]),
                        error: None,
                        error_type: None,
                    },
                ],
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node = recovered.graph.nodes.get("test_node").unwrap();
        assert_eq!(node.attempts.len(), 2);
        assert!(!node.attempts[0].success);
        assert!(node.attempts[1].success);
    }

    #[test]
    fn test_spread_index_storage() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "spread_1[0]".to_string(),
            ExecutionNode {
                template_id: "spread_1".to_string(),
                spread_index: Some(0),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "spread_1[1]".to_string(),
            ExecutionNode {
                template_id: "spread_1".to_string(),
                spread_index: Some(1),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node0 = recovered.graph.nodes.get("spread_1[0]").unwrap();
        let node1 = recovered.graph.nodes.get("spread_1[1]").unwrap();
        assert_eq!(node0.spread_index, Some(0));
        assert_eq!(node1.spread_index, Some(1));
        assert_eq!(node0.template_id, "spread_1");
        assert_eq!(node1.template_id, "spread_1");
    }

    #[test]
    fn test_waiting_for_tracking() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "barrier".to_string(),
            ExecutionNode {
                template_id: "barrier".to_string(),
                status: NodeStatus::Blocked as i32,
                waiting_for: vec![
                    "predecessor_1".to_string(),
                    "predecessor_2".to_string(),
                    "predecessor_3".to_string(),
                ],
                completed_count: 1,
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let barrier = recovered.graph.nodes.get("barrier").unwrap();
        assert_eq!(barrier.waiting_for.len(), 3);
        assert_eq!(barrier.completed_count, 1);
    }

    #[test]
    fn test_backoff_config_storage() {
        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "test_node".to_string(),
            ExecutionNode {
                template_id: "test_node".to_string(),
                status: NodeStatus::Pending as i32,
                max_retries: 5,
                timeout_seconds: 120,
                backoff: Some(BackoffConfig {
                    kind: BackoffKind::Exponential as i32,
                    base_delay_ms: 1000,
                    multiplier: 2.0,
                }),
                ..Default::default()
            },
        );

        let bytes = state.to_bytes();
        let recovered = ExecutionState::from_bytes(&bytes).unwrap();

        let node = recovered.graph.nodes.get("test_node").unwrap();
        assert_eq!(node.max_retries, 5);
        assert_eq!(node.timeout_seconds, 120);
        let backoff = node.backoff.as_ref().unwrap();
        assert_eq!(backoff.kind, BackoffKind::Exponential as i32);
        assert_eq!(backoff.base_delay_ms, 1000);
        assert_eq!(backoff.multiplier, 2.0);
    }

    /// Test crash recovery with partial dispatch.
    ///
    /// Simulates: 10 nodes ready, dispatch 2, crash, recover.
    /// All 10 nodes should be recoverable because:
    /// - We peek (not drain) the ready_queue
    /// - mark_running removes dispatched nodes from ready_queue
    /// - Undispatched nodes remain in ready_queue
    /// - Running nodes are recovered to Pending on crash recovery
    #[test]
    fn test_crash_recovery_partial_dispatch() {
        let mut state = ExecutionState::new();

        // Create 10 pending nodes and add them to ready_queue
        for i in 0..10 {
            let node_id = format!("action_{}", i);
            state.graph.nodes.insert(
                node_id.clone(),
                ExecutionNode {
                    template_id: node_id.clone(),
                    status: NodeStatus::Pending as i32,
                    max_retries: 3,
                    ..Default::default()
                },
            );
            state.graph.ready_queue.push(node_id);
        }

        assert_eq!(state.graph.ready_queue.len(), 10);

        // CORRECT PATTERN: Peek, don't drain
        let ready_nodes = state.peek_ready_queue();
        assert_eq!(ready_nodes.len(), 10);
        // ready_queue is STILL intact
        assert_eq!(state.graph.ready_queue.len(), 10);

        // Dispatch only 2 nodes - mark_running removes them from ready_queue
        state.mark_running("action_0", "worker-0", None);
        state.mark_running("action_1", "worker-1", None);

        // Now ready_queue has 8 (the 2 dispatched were removed by mark_running)
        assert_eq!(state.graph.ready_queue.len(), 8);

        // Sync to DB
        let bytes = state.to_bytes();

        // CRASH and recover
        let mut recovered = ExecutionState::from_bytes(&bytes).unwrap();

        // Before recovery: 8 in ready_queue, 2 Running
        assert_eq!(recovered.graph.ready_queue.len(), 8);

        // Run recovery - resets Running nodes to Pending
        recovered.recover_running_nodes();

        // After recovery: ALL 10 should be in ready_queue
        // - 8 that were never dispatched (still in ready_queue)
        // - 2 that were Running -> recovered to Pending -> added to ready_queue
        assert_eq!(
            recovered.graph.ready_queue.len(),
            10,
            "All nodes should be recoverable"
        );

        // No orphaned Pending nodes
        let pending_not_in_queue: Vec<_> = recovered
            .graph
            .nodes
            .iter()
            .filter(|(node_id, node)| {
                node.status == NodeStatus::Pending as i32
                    && !recovered.graph.ready_queue.contains(*node_id)
            })
            .collect();

        assert!(
            pending_not_in_queue.is_empty(),
            "No Pending nodes should be orphaned"
        );
    }

    /// Helper to build a minimal DAG with sequential nodes connected by state-machine edges.
    fn build_sequential_dag(node_ids: &[&str]) -> DAG {
        use crate::dag::{DAGEdge, DAGNode};

        let mut nodes = HashMap::new();
        let mut edges = Vec::new();

        for (i, id) in node_ids.iter().enumerate() {
            let is_first = i == 0;
            let is_last = i == node_ids.len() - 1;
            let node_type = if is_first {
                "input"
            } else if is_last {
                "output"
            } else {
                "action_call"
            };

            let mut node = DAGNode::new(id.to_string(), node_type.to_string(), id.to_string());
            node.function_name = Some("run".to_string());
            node.is_input = is_first;
            node.is_output = is_last;
            if !is_first && !is_last {
                node.action_name = Some(format!("action_{}", id));
                node.module_name = Some("test_module".to_string());
            }
            node.target = Some("result".to_string());
            nodes.insert(id.to_string(), node);

            if i > 0 {
                edges.push(DAGEdge::state_machine(
                    node_ids[i - 1].to_string(),
                    id.to_string(),
                ));
            }
        }

        DAG {
            nodes,
            edges,
            entry_node: Some(node_ids[0].to_string()),
        }
    }

    #[test]
    fn test_find_stalled_completions_detects_limbo() {
        // Simulate the limbo state: node_b completed but successor node_c is still BLOCKED
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );
        // Ready queue is empty - simulating crash after completion but before successor computation
        assert!(state.graph.ready_queue.is_empty());

        let stalled = state.find_stalled_completions(&dag);

        // node_b should be identified as stalled (completed with blocked successor)
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].node_id, "node_b");
        assert!(stalled[0].success);
        assert_eq!(stalled[0].result, Some(vec![1, 2, 3]));
        assert_eq!(stalled[0].worker_id, "recovery");
    }

    #[test]
    fn test_find_stalled_completions_no_stall_when_successor_already_ready() {
        // node_b completed and node_c is already PENDING in the ready_queue - no stall
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Pending as i32,
                ..Default::default()
            },
        );
        state.graph.ready_queue.push("node_c".to_string());

        let stalled = state.find_stalled_completions(&dag);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_find_stalled_completions_ignores_running_nodes() {
        // node_b is RUNNING - this should be handled by recover_running_nodes, not us
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Running as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        let stalled = state.find_stalled_completions(&dag);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_find_stalled_completions_no_stall_when_all_successors_advanced() {
        // Both node_b and node_c completed - no blocked successors
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );

        let stalled = state.find_stalled_completions(&dag);
        assert!(stalled.is_empty());
    }

    #[test]
    fn test_stalled_completions_are_processable() {
        // Verify that stalled completions can be processed through apply_completions_batch
        // to advance the workflow
        let dag = build_sequential_dag(&["node_a", "node_b", "node_c"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "node_a".to_string(),
            ExecutionNode {
                template_id: "node_a".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_b".to_string(),
            ExecutionNode {
                template_id: "node_b".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![1, 2, 3]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "node_c".to_string(),
            ExecutionNode {
                template_id: "node_c".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        let stalled = state.find_stalled_completions(&dag);
        assert_eq!(stalled.len(), 1);

        // Process the stalled completions
        let result = state.apply_completions_batch(stalled, &dag);

        // node_c should now be PENDING in the ready_queue
        assert!(
            result.newly_ready.contains(&"node_c".to_string()),
            "node_c should be newly ready after recovery"
        );
        let node_c = state.graph.nodes.get("node_c").unwrap();
        assert_eq!(node_c.status, NodeStatus::Pending as i32);
        assert!(state.graph.ready_queue.contains(&"node_c".to_string()));
    }

    #[test]
    fn test_stalled_completions_roundtrip_recovery() {
        // Full roundtrip: create limbo state, serialize, deserialize, recover
        let dag = build_sequential_dag(&["input", "action", "output"]);

        let mut state = ExecutionState::new();
        state.graph.nodes.insert(
            "input".to_string(),
            ExecutionNode {
                template_id: "input".to_string(),
                status: NodeStatus::Completed as i32,
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "action".to_string(),
            ExecutionNode {
                template_id: "action".to_string(),
                status: NodeStatus::Completed as i32,
                result: Some(vec![42]),
                ..Default::default()
            },
        );
        state.graph.nodes.insert(
            "output".to_string(),
            ExecutionNode {
                template_id: "output".to_string(),
                status: NodeStatus::Blocked as i32,
                ..Default::default()
            },
        );

        // Simulate crash: serialize to bytes (as if persisted to DB)
        let bytes = state.to_bytes();

        // Recovery: deserialize
        let mut recovered = ExecutionState::from_bytes(&bytes).unwrap();

        // recover_running_nodes finds nothing (no RUNNING nodes)
        recovered.recover_running_nodes();
        assert!(recovered.graph.ready_queue.is_empty());

        // find_stalled_completions detects the limbo
        let stalled = recovered.find_stalled_completions(&dag);
        assert_eq!(stalled.len(), 1);
        assert_eq!(stalled[0].node_id, "action");

        // Processing the stalled completions advances the workflow
        let result = recovered.apply_completions_batch(stalled, &dag);
        assert!(
            result.newly_ready.contains(&"output".to_string()),
            "output node should be ready after recovery"
        );
    }
}
