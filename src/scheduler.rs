//! Workflow scheduler - manages DAG execution state and node dispatch.
//!
//! This module implements a push-based scheduler where completing nodes
//! unlock their dependents. The core state is:
//!
//! - `eval_context`: Single source of truth for all workflow variables
//! - `node_states`: Ready/running/completed status for each node
//! - `loop_indices`: Current iteration index for each loop
//!
//! Key design principles:
//! - eval_context is the ONLY place variables live (no separate accumulator tracking)
//! - Loops are just sub-graphs with back edges; loop_head is scheduler-evaluated
//! - Push-based: completing a node immediately unlocks dependents

use crate::dag::{Dag, EdgeKind, Node, NodeKind};
use crate::ir_parser::{self as ir, Expression};
use crate::messages::proto::{
    NodeContext, NodeDispatch, NodeInfo, WorkflowArguments,
};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::debug;

/// Node execution state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Waiting for dependencies
    Pending,
    /// Dependencies satisfied, ready to execute
    Ready,
    /// Currently being executed by a worker
    Running,
    /// Execution completed successfully
    Completed,
    /// Execution failed
    Failed,
    /// Skipped (e.g., branch not taken)
    Skipped,
}

/// Persistent state for a workflow instance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceState {
    /// All workflow variables (single source of truth)
    pub eval_context: HashMap<String, Value>,
    /// State of each node
    pub node_states: HashMap<String, NodeState>,
    /// Dependencies satisfied count for each node
    pub deps_satisfied: HashMap<String, u32>,
    /// Required dependency count for each node
    pub deps_required: HashMap<String, u32>,
    /// Current iteration index for each loop_head
    pub loop_indices: HashMap<String, usize>,
}

impl InstanceState {
    /// Create initial state for a workflow
    pub fn new(dag: &Dag, initial_context: HashMap<String, Value>) -> Self {
        let mut node_states = HashMap::new();
        let mut deps_satisfied = HashMap::new();
        let mut deps_required = HashMap::new();

        // Calculate required dependencies for each node
        for (node_id, _node) in &dag.nodes {
            node_states.insert(node_id.clone(), NodeState::Pending);
            deps_satisfied.insert(node_id.clone(), 0);

            // Count incoming edges (excluding back edges and guarded edges)
            let required = count_required_deps(node_id, dag);
            deps_required.insert(node_id.clone(), required);
        }

        // Mark entry nodes as ready (nodes with 0 required deps)
        for (node_id, &required) in &deps_required {
            if required == 0 {
                node_states.insert(node_id.clone(), NodeState::Ready);
            }
        }

        Self {
            eval_context: initial_context,
            node_states,
            deps_satisfied,
            deps_required,
            loop_indices: HashMap::new(),
        }
    }

    /// Get all nodes that are ready to execute
    pub fn ready_nodes(&self) -> Vec<String> {
        self.node_states
            .iter()
            .filter(|(_, state)| **state == NodeState::Ready)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Mark a node as running
    pub fn mark_running(&mut self, node_id: &str) {
        self.node_states.insert(node_id.to_string(), NodeState::Running);
    }

    /// Get a variable from eval_context
    pub fn get_var(&self, name: &str) -> Option<&Value> {
        self.eval_context.get(name)
    }

    /// Set a variable in eval_context
    pub fn set_var(&mut self, name: impl Into<String>, value: Value) {
        let name = name.into();
        debug!(
            variable = %name,
            value_type = ?value_type_name(&value),
            value_preview = %value_preview(&value),
            "Setting variable in eval_context"
        );
        self.eval_context.insert(name, value);
    }

    /// Get a summary of the current eval_context for debugging
    pub fn context_summary(&self) -> String {
        let vars: Vec<String> = self.eval_context
            .iter()
            .map(|(k, v)| format!("{}={}", k, value_preview(v)))
            .collect();
        vars.join(", ")
    }
}

/// Get a short type name for a Value
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Get a short preview of a Value for logging
fn value_preview(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) if s.len() <= 50 => format!("\"{}\"", s),
        Value::String(s) => format!("\"{}...\"", &s[..47]),
        Value::Array(arr) => format!("[{} items]", arr.len()),
        Value::Object(obj) => format!("{{{} keys}}", obj.len()),
    }
}

/// Count required dependencies for a node (excluding back/guarded edges)
fn count_required_deps(node_id: &str, dag: &Dag) -> u32 {
    let mut count = 0;
    for (_source_id, source_node) in &dag.nodes {
        for edge in &source_node.edges {
            if edge.target == node_id {
                match edge.kind {
                    // These edges don't count as dependencies
                    EdgeKind::Back | EdgeKind::GuardTrue | EdgeKind::GuardFalse | EdgeKind::Exception => {}
                    // These edges are real dependencies
                    EdgeKind::Data | EdgeKind::Continue | EdgeKind::Exit => {
                        count += 1;
                    }
                }
            }
        }
    }
    count
}

/// Actions returned by the scheduler for the executor to perform
#[derive(Debug)]
pub enum SchedulerAction {
    /// Dispatch a node to a worker for execution
    Dispatch {
        node_id: String,
        dispatch: NodeDispatch,
    },
    /// Node completed, here are the newly ready nodes
    NodesReady {
        node_ids: Vec<String>,
    },
    /// Workflow completed with result
    WorkflowComplete {
        result: Option<Value>,
    },
    /// Sleep for a duration (durable sleep)
    Sleep {
        node_id: String,
        duration_seconds: f64,
    },
    /// Nothing to do right now
    Idle,
}

/// The scheduler processes DAG execution
pub struct Scheduler<'a> {
    dag: &'a Dag,
}

impl<'a> Scheduler<'a> {
    pub fn new(dag: &'a Dag) -> Self {
        Self { dag }
    }

    /// Process a ready node - either dispatch to worker or evaluate locally
    pub fn process_ready_node(
        &self,
        state: &mut InstanceState,
        node_id: &str,
        workflow_input: &WorkflowArguments,
    ) -> Result<SchedulerAction> {
        let node = self.dag.get_node(node_id)
            .ok_or_else(|| anyhow!("Node {} not found", node_id))?;

        debug!(
            node_id = %node_id,
            node_kind = ?node.kind,
            action = ?node.action,
            produces = ?node.produces,
            eval_context = %state.context_summary(),
            "Processing ready node"
        );

        match node.kind {
            NodeKind::LoopHead => {
                debug!(node_id = %node_id, "Evaluating loop head locally");
                self.evaluate_loop_head(state, node)
            }
            NodeKind::Branch => {
                debug!(node_id = %node_id, "Evaluating branch condition locally");
                self.evaluate_branch(state, node)
            }
            NodeKind::GatherJoin => {
                debug!(node_id = %node_id, "Evaluating gather join locally");
                self.evaluate_gather_join(state, node)
            }
            NodeKind::Action | NodeKind::Computed => {
                debug!(
                    node_id = %node_id,
                    kwargs = ?node.kwargs,
                    "Building dispatch for worker"
                );
                let dispatch = self.build_dispatch(state, node, workflow_input)?;
                state.mark_running(node_id);
                debug!(
                    node_id = %node_id,
                    context_vars = dispatch.context.len(),
                    "Dispatching to worker"
                );
                Ok(SchedulerAction::Dispatch {
                    node_id: node_id.to_string(),
                    dispatch,
                })
            }
            NodeKind::Sleep => {
                debug!(node_id = %node_id, "Evaluating sleep node");
                self.evaluate_sleep(state, node)
            }
            NodeKind::TryHead => {
                debug!(node_id = %node_id, "Try head - passing through");
                self.mark_complete_and_unlock(state, node, None)
            }
            NodeKind::Return => {
                let result = self.dag.return_variable.as_ref()
                    .and_then(|var| state.get_var(var).cloned());
                debug!(
                    node_id = %node_id,
                    return_var = ?self.dag.return_variable,
                    has_result = result.is_some(),
                    "Workflow complete"
                );
                Ok(SchedulerAction::WorkflowComplete { result })
            }
        }
    }

    /// Handle node completion with result
    pub fn handle_completion(
        &self,
        state: &mut InstanceState,
        node_id: &str,
        result: Option<Value>,
        success: bool,
    ) -> Result<SchedulerAction> {
        let node = self.dag.get_node(node_id)
            .ok_or_else(|| anyhow!("Node {} not found", node_id))?;

        debug!(
            node_id = %node_id,
            success = success,
            has_result = result.is_some(),
            result_preview = %result.as_ref().map(value_preview).unwrap_or_else(|| "none".to_string()),
            produces = ?node.produces,
            "Handling node completion"
        );

        if !success {
            debug!(node_id = %node_id, "Node failed - checking exception handlers");
            return self.handle_failure(state, node, result);
        }

        // Store result in eval_context
        if let Some(value) = result {
            // Check if this is a WorkflowNodeResult with variables dict
            // (returned by python_block nodes)
            if let Some(variables) = value.get("variables").and_then(|v| v.as_object()) {
                debug!(
                    node_id = %node_id,
                    variables_count = variables.len(),
                    "Storing WorkflowNodeResult variables"
                );
                for var in &node.produces {
                    if let Some(var_value) = variables.get(var) {
                        state.set_var(var.clone(), var_value.clone());
                    }
                }
            } else {
                // Simple result - assign to all produces (typically just one)
                debug!(
                    node_id = %node_id,
                    produces = ?node.produces,
                    "Storing simple result to produces"
                );
                for var in &node.produces {
                    state.set_var(var.clone(), value.clone());
                }
            }
        }

        debug!(
            node_id = %node_id,
            eval_context_after = %state.context_summary(),
            "State after storing result"
        );

        // Check for back edge (loop iteration complete)
        if let Some(loop_head_id) = self.get_back_edge_target(node) {
            debug!(node_id = %node_id, loop_head = %loop_head_id, "Found back edge to loop head");
            return self.handle_back_edge(state, node, &loop_head_id);
        }

        // Normal completion - unlock downstream nodes
        self.mark_complete_and_unlock(state, node, None)
    }

    /// Evaluate a loop head node (scheduler-local, not dispatched)
    fn evaluate_loop_head(
        &self,
        state: &mut InstanceState,
        node: &Node,
    ) -> Result<SchedulerAction> {
        let meta = node.loop_meta.as_ref()
            .ok_or_else(|| anyhow!("Loop head {} missing metadata", node.id))?;

        // Get iterator from eval_context
        let iterator = state.get_var(&meta.iterator_var)
            .cloned()
            .unwrap_or(Value::Array(vec![]));

        let items = iterator.as_array()
            .ok_or_else(|| anyhow!("Loop iterator {} is not an array", meta.iterator_var))?;

        let current_idx = *state.loop_indices.get(&node.id).unwrap_or(&0);

        // Initialize accumulator on first iteration
        if current_idx == 0 {
            for var in &node.produces {
                if state.get_var(var).is_none() {
                    state.set_var(var.clone(), Value::Array(vec![]));
                }
            }
        }

        if current_idx >= items.len() {
            // Loop complete - mark loop head and all body nodes as completed
            state.node_states.insert(node.id.clone(), NodeState::Completed);

            // Mark body nodes as completed so they don't block workflow completion
            for body_node_id in &meta.body_nodes {
                state.node_states.insert(body_node_id.clone(), NodeState::Completed);
            }

            // Find and unlock nodes via Exit edges
            let mut newly_ready = Vec::new();
            for edge in &node.edges {
                if edge.kind == EdgeKind::Exit {
                    if self.try_unlock_node(state, &edge.target) {
                        newly_ready.push(edge.target.clone());
                    }
                }
            }

            // Also unlock any nodes that depend on the loop's output
            for edge in &node.edges {
                if edge.kind == EdgeKind::Data && !meta.body_nodes.contains(&edge.target) {
                    if self.try_unlock_node(state, &edge.target) {
                        newly_ready.push(edge.target.clone());
                    }
                }
            }

            Ok(SchedulerAction::NodesReady { node_ids: newly_ready })
        } else {
            // More iterations - bind loop var and unlock body
            let current_item = items[current_idx].clone();
            state.set_var(meta.loop_var.clone(), current_item);

            // Reset body nodes for this iteration
            for body_node_id in &meta.body_nodes {
                state.node_states.insert(body_node_id.clone(), NodeState::Pending);
                state.deps_satisfied.insert(body_node_id.clone(), 0);
            }

            // Mark loop head as "running" (waiting for body)
            state.node_states.insert(node.id.clone(), NodeState::Running);

            // Unlock body entry via Continue edge
            let mut newly_ready = Vec::new();
            for edge in &node.edges {
                if edge.kind == EdgeKind::Continue {
                    if self.try_unlock_node(state, &edge.target) {
                        newly_ready.push(edge.target.clone());
                    }
                }
            }

            Ok(SchedulerAction::NodesReady { node_ids: newly_ready })
        }
    }

    /// Handle back edge (loop iteration complete)
    fn handle_back_edge(
        &self,
        state: &mut InstanceState,
        _completed_node: &Node,
        loop_head_id: &str,
    ) -> Result<SchedulerAction> {
        // Increment loop index
        let idx = state.loop_indices.entry(loop_head_id.to_string()).or_insert(0);
        *idx += 1;

        // Re-queue loop head for next iteration
        state.node_states.insert(loop_head_id.to_string(), NodeState::Ready);

        Ok(SchedulerAction::NodesReady {
            node_ids: vec![loop_head_id.to_string()],
        })
    }

    /// Evaluate a branch (conditional) node
    fn evaluate_branch(
        &self,
        state: &mut InstanceState,
        node: &Node,
    ) -> Result<SchedulerAction> {
        let meta = node.branch_meta.as_ref()
            .ok_or_else(|| anyhow!("Branch {} missing metadata", node.id))?;

        debug!(
            node_id = %node.id,
            guard_expr = ?meta.guard,
            true_entry = ?meta.true_entry,
            false_entry = ?meta.false_entry,
            true_nodes_count = meta.true_nodes.len(),
            false_nodes_count = meta.false_nodes.len(),
            eval_context = %state.context_summary(),
            "Evaluating branch condition"
        );

        // Evaluate guard expression
        let guard_result = match &meta.guard {
            Some(expr) => {
                let result = eval_expression(expr, &state.eval_context)?;
                value_to_bool(&result)
            }
            None => true, // No guard = unconditional (else branch)
        };

        debug!(
            node_id = %node.id,
            guard_expr = ?meta.guard,
            guard_result = guard_result,
            "Guard evaluation result"
        );

        state.node_states.insert(node.id.clone(), NodeState::Completed);

        let mut newly_ready = Vec::new();

        if guard_result {
            // Unlock true branch, skip false branch
            if let Some(true_entry) = &meta.true_entry {
                debug!(
                    node_id = %node.id,
                    unlocking = %true_entry,
                    "Taking true branch"
                );
                if self.try_unlock_node(state, true_entry) {
                    newly_ready.push(true_entry.clone());
                }
            }
            // First mark ALL false branch nodes as skipped
            for skipped_id in &meta.false_nodes {
                state.node_states.insert(skipped_id.clone(), NodeState::Skipped);
            }
            // Then unlock downstream nodes of skipped nodes (but only if target is NOT also skipped)
            for skipped_id in &meta.false_nodes {
                if let Some(skipped_node) = self.dag.get_node(skipped_id) {
                    for edge in &skipped_node.edges {
                        if edge.kind == EdgeKind::Data && !meta.false_nodes.contains(&edge.target) {
                            if self.try_unlock_node(state, &edge.target) {
                                newly_ready.push(edge.target.clone());
                            }
                        }
                    }
                }
            }
            debug!(
                node_id = %node.id,
                skipped_count = meta.false_nodes.len(),
                "Skipped false branch nodes"
            );
        } else {
            // Unlock false branch, skip true branch
            if let Some(false_entry) = &meta.false_entry {
                debug!(
                    node_id = %node.id,
                    unlocking = %false_entry,
                    "Taking false branch"
                );
                if self.try_unlock_node(state, false_entry) {
                    newly_ready.push(false_entry.clone());
                }
            }
            // First mark ALL true branch nodes as skipped
            for skipped_id in &meta.true_nodes {
                state.node_states.insert(skipped_id.clone(), NodeState::Skipped);
            }
            // Then unlock downstream nodes of skipped nodes (but only if target is NOT also skipped)
            for skipped_id in &meta.true_nodes {
                if let Some(skipped_node) = self.dag.get_node(skipped_id) {
                    for edge in &skipped_node.edges {
                        if edge.kind == EdgeKind::Data && !meta.true_nodes.contains(&edge.target) {
                            if self.try_unlock_node(state, &edge.target) {
                                newly_ready.push(edge.target.clone());
                            }
                        }
                    }
                }
            }
            debug!(
                node_id = %node.id,
                skipped_count = meta.true_nodes.len(),
                "Skipped true branch nodes"
            );
        }

        debug!(
            node_id = %node.id,
            newly_ready = ?newly_ready,
            "Branch evaluation complete"
        );

        Ok(SchedulerAction::NodesReady { node_ids: newly_ready })
    }

    /// Evaluate a gather join node
    fn evaluate_gather_join(
        &self,
        state: &mut InstanceState,
        node: &Node,
    ) -> Result<SchedulerAction> {
        // Collect individual item results into the target array
        // The parallel actions produce: {target}__item0, {target}__item1, etc.
        // We need to collect them into: {target} = [item0_value, item1_value, ...]
        for target in &node.produces {
            let mut items = Vec::new();
            let mut i = 0;
            loop {
                let item_var = format!("{}__item{}", target, i);
                if let Some(value) = state.get_var(&item_var) {
                    items.push(value.clone());
                    i += 1;
                } else {
                    break;
                }
            }
            if !items.is_empty() {
                state.set_var(target.clone(), Value::Array(items));
            }
        }
        self.mark_complete_and_unlock(state, node, None)
    }

    /// Evaluate a sleep node - returns Sleep action for executor to handle
    fn evaluate_sleep(
        &self,
        state: &mut InstanceState,
        node: &Node,
    ) -> Result<SchedulerAction> {
        // Parse duration from the expression
        let duration_str = node.sleep_duration_expr.as_deref().unwrap_or("0");
        let duration_seconds: f64 = duration_str.parse().unwrap_or(0.0);

        debug!(
            node_id = %node.id,
            duration_seconds = duration_seconds,
            "Sleep node - returning sleep action"
        );

        // Mark as running while sleeping
        state.mark_running(&node.id);

        Ok(SchedulerAction::Sleep {
            node_id: node.id.clone(),
            duration_seconds,
        })
    }

    /// Handle node failure - check for exception handlers
    fn handle_failure(
        &self,
        state: &mut InstanceState,
        node: &Node,
        _error: Option<Value>,
    ) -> Result<SchedulerAction> {
        state.node_states.insert(node.id.clone(), NodeState::Failed);

        // Look for exception edges pointing to handlers
        // For now, just propagate failure
        // TODO: Implement proper exception handling

        Ok(SchedulerAction::NodesReady { node_ids: vec![] })
    }

    /// Mark a node complete and unlock downstream nodes
    fn mark_complete_and_unlock(
        &self,
        state: &mut InstanceState,
        node: &Node,
        _result: Option<Value>,
    ) -> Result<SchedulerAction> {
        state.node_states.insert(node.id.clone(), NodeState::Completed);

        let mut newly_ready = Vec::new();

        // Unlock downstream nodes via Data edges
        for edge in &node.edges {
            match edge.kind {
                EdgeKind::Data | EdgeKind::Exit => {
                    if self.try_unlock_node(state, &edge.target) {
                        newly_ready.push(edge.target.clone());
                    }
                }
                _ => {}
            }
        }

        // If no more ready nodes, check if workflow is complete
        if newly_ready.is_empty() && self.is_workflow_complete(state) {
            let result = self.dag.return_variable.as_ref()
                .and_then(|var| state.get_var(var).cloned());
            return Ok(SchedulerAction::WorkflowComplete { result });
        }

        Ok(SchedulerAction::NodesReady { node_ids: newly_ready })
    }

    /// Check if all nodes in the workflow are completed (or skipped)
    fn is_workflow_complete(&self, state: &InstanceState) -> bool {
        // All nodes must be in Completed or Skipped state
        self.dag.nodes.keys().all(|node_id| {
            matches!(state.node_states.get(node_id), Some(NodeState::Completed) | Some(NodeState::Skipped))
        })
    }

    /// Try to unlock a node (increment deps_satisfied, mark ready if all satisfied)
    fn try_unlock_node(&self, state: &mut InstanceState, node_id: &str) -> bool {
        let satisfied = state.deps_satisfied.entry(node_id.to_string()).or_insert(0);
        *satisfied += 1;

        let required = state.deps_required.get(node_id).copied().unwrap_or(0);

        if *satisfied >= required {
            // Check current state - only unlock if pending
            if let Some(current_state) = state.node_states.get(node_id) {
                if *current_state == NodeState::Pending {
                    state.node_states.insert(node_id.to_string(), NodeState::Ready);
                    return true;
                }
            }
        }
        false
    }

    /// Get back edge target for a node, if any
    fn get_back_edge_target(&self, node: &Node) -> Option<String> {
        node.edges.iter()
            .find(|e| e.kind == EdgeKind::Back)
            .map(|e| e.target.clone())
    }

    /// Build dispatch payload for a node
    fn build_dispatch(
        &self,
        state: &InstanceState,
        node: &Node,
        workflow_input: &WorkflowArguments,
    ) -> Result<NodeDispatch> {
        // Build node info
        let node_info = NodeInfo {
            id: node.id.clone(),
            action: node.action.clone().unwrap_or_default(),
            module: node.module.clone().unwrap_or_default(),
            kwargs: node.kwargs.clone(),
            produces: node.produces.clone(),
            exception_edges: vec![], // TODO: Convert exception edges
        };

        // Build context from eval_context
        // Include all variables the node might need
        let context = build_context_for_node(state, node);

        Ok(NodeDispatch {
            node: Some(node_info),
            workflow_input: Some(workflow_input.clone()),
            context,
            resolved_kwargs: None, // Worker will evaluate kwargs
        })
    }
}

/// Build context entries for a node from eval_context
fn build_context_for_node(state: &InstanceState, node: &Node) -> Vec<NodeContext> {
    // For now, include all variables in context
    // TODO: Optimize to only include variables referenced in kwargs
    let context: Vec<NodeContext> = state.eval_context.iter()
        .map(|(var, value)| {
            NodeContext {
                variable: var.clone(),
                payload: Some(value_to_workflow_args(var, value)),
                source_node_id: String::new(),
            }
        })
        .collect();

    let var_names: Vec<&str> = context.iter().map(|c| c.variable.as_str()).collect();
    debug!(
        node_id = %node.id,
        context_vars = ?var_names,
        context_count = context.len(),
        "Built context for node dispatch"
    );

    context
}

/// Convert a Value to WorkflowArguments with "result" key (matches Python's serialize_result_payload)
fn value_to_workflow_args(_key: &str, value: &Value) -> WorkflowArguments {
    use crate::messages::proto::{
        WorkflowArgument, WorkflowArgumentValue, PrimitiveWorkflowArgument,
        WorkflowListArgument, WorkflowDictArgument,
        primitive_workflow_argument::Kind as PrimitiveKind,
        workflow_argument_value::Kind as ValueKind,
    };

    fn convert_value(v: &Value) -> WorkflowArgumentValue {
        match v {
            Value::Null => WorkflowArgumentValue {
                kind: Some(ValueKind::Primitive(PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::NullValue(0)),
                })),
            },
            Value::Bool(b) => WorkflowArgumentValue {
                kind: Some(ValueKind::Primitive(PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::BoolValue(*b)),
                })),
            },
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    WorkflowArgumentValue {
                        kind: Some(ValueKind::Primitive(PrimitiveWorkflowArgument {
                            kind: Some(PrimitiveKind::IntValue(i)),
                        })),
                    }
                } else {
                    WorkflowArgumentValue {
                        kind: Some(ValueKind::Primitive(PrimitiveWorkflowArgument {
                            kind: Some(PrimitiveKind::DoubleValue(n.as_f64().unwrap_or(0.0))),
                        })),
                    }
                }
            }
            Value::String(s) => WorkflowArgumentValue {
                kind: Some(ValueKind::Primitive(PrimitiveWorkflowArgument {
                    kind: Some(PrimitiveKind::StringValue(s.clone())),
                })),
            },
            Value::Array(arr) => WorkflowArgumentValue {
                kind: Some(ValueKind::ListValue(WorkflowListArgument {
                    items: arr.iter().map(convert_value).collect(),
                })),
            },
            Value::Object(obj) => WorkflowArgumentValue {
                kind: Some(ValueKind::DictValue(WorkflowDictArgument {
                    entries: obj.iter()
                        .map(|(k, v)| WorkflowArgument {
                            key: k.clone(),
                            value: Some(convert_value(v)),
                        })
                        .collect(),
                })),
            },
        }
    }

    // Use "result" as key to match Python's serialize_result_payload format
    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: "result".to_string(),
            value: Some(convert_value(value)),
        }],
    }
}

/// Convert a JSON Value to bool
fn value_to_bool(value: &Value) -> bool {
    match value {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Evaluate a proto Expression against a context
fn eval_expression(expr: &Expression, context: &HashMap<String, Value>) -> Result<Value> {
    use ir::expression::Kind;
    use ir::binary_op::Op as BinaryOp;
    use ir::unary_op::Op as UnaryOp;
    use ir::literal::Value as LiteralValue;

    match &expr.kind {
        None => Err(anyhow!("Expression has no kind")),

        Some(Kind::Literal(lit)) => {
            match &lit.value {
                None => Ok(Value::Null),
                Some(LiteralValue::NullValue(_)) => Ok(Value::Null),
                Some(LiteralValue::BoolValue(b)) => Ok(Value::Bool(*b)),
                Some(LiteralValue::IntValue(i)) => Ok(Value::Number((*i).into())),
                Some(LiteralValue::FloatValue(d)) => {
                    serde_json::Number::from_f64(*d)
                        .map(Value::Number)
                        .ok_or_else(|| anyhow!("Invalid float: {}", d))
                }
                Some(LiteralValue::StringValue(s)) => Ok(Value::String(s.clone())),
            }
        }

        Some(Kind::Variable(var_name)) => {
            context.get(var_name)
                .cloned()
                .ok_or_else(|| anyhow!("Variable '{}' not found in context", var_name))
        }

        Some(Kind::Subscript(sub)) => {
            let base = sub.base.as_ref()
                .ok_or_else(|| anyhow!("Subscript missing base"))?;
            let key = sub.key.as_ref()
                .ok_or_else(|| anyhow!("Subscript missing key"))?;

            let base_val = eval_expression(base, context)?;
            let key_val = eval_expression(key, context)?;

            match (&base_val, &key_val) {
                (Value::Array(arr), Value::Number(n)) => {
                    let idx = n.as_i64().ok_or_else(|| anyhow!("Array index must be integer"))? as usize;
                    arr.get(idx).cloned().ok_or_else(|| anyhow!("Array index {} out of bounds", idx))
                }
                (Value::Object(obj), Value::String(k)) => {
                    obj.get(k).cloned().ok_or_else(|| anyhow!("Key '{}' not found in object", k))
                }
                _ => Err(anyhow!("Invalid subscript: {:?}[{:?}]", base_val, key_val))
            }
        }

        Some(Kind::Array(arr)) => {
            let items: Result<Vec<Value>> = arr.elements.iter()
                .map(|e| eval_expression(e, context))
                .collect();
            Ok(Value::Array(items?))
        }

        Some(Kind::Dict(dict)) => {
            let mut map = serde_json::Map::new();
            for entry in &dict.entries {
                // Dict keys are strings directly in the proto
                let key_str = entry.key.clone();
                let val = entry.value.as_ref()
                    .ok_or_else(|| anyhow!("Dict entry missing value"))?;

                map.insert(key_str, eval_expression(val, context)?);
            }
            Ok(Value::Object(map))
        }

        Some(Kind::BinaryOp(binop)) => {
            let left = binop.left.as_ref()
                .ok_or_else(|| anyhow!("BinaryOp missing left operand"))?;
            let right = binop.right.as_ref()
                .ok_or_else(|| anyhow!("BinaryOp missing right operand"))?;

            let left_val = eval_expression(left, context)?;

            // Short-circuit for and/or
            match BinaryOp::try_from(binop.op) {
                Ok(BinaryOp::And) => {
                    if !value_to_bool(&left_val) {
                        return Ok(Value::Bool(false));
                    }
                    let right_val = eval_expression(right, context)?;
                    return Ok(Value::Bool(value_to_bool(&right_val)));
                }
                Ok(BinaryOp::Or) => {
                    if value_to_bool(&left_val) {
                        return Ok(Value::Bool(true));
                    }
                    let right_val = eval_expression(right, context)?;
                    return Ok(Value::Bool(value_to_bool(&right_val)));
                }
                _ => {}
            }

            let right_val = eval_expression(right, context)?;

            match BinaryOp::try_from(binop.op) {
                Ok(BinaryOp::Add) => numeric_binop(&left_val, &right_val, |a, b| a + b),
                Ok(BinaryOp::Sub) => numeric_binop(&left_val, &right_val, |a, b| a - b),
                Ok(BinaryOp::Mul) => numeric_binop(&left_val, &right_val, |a, b| a * b),
                Ok(BinaryOp::Div) => numeric_binop(&left_val, &right_val, |a, b| a / b),
                Ok(BinaryOp::Mod) => numeric_binop(&left_val, &right_val, |a, b| a % b),
                Ok(BinaryOp::Eq) => Ok(Value::Bool(left_val == right_val)),
                Ok(BinaryOp::Ne) => Ok(Value::Bool(left_val != right_val)),
                Ok(BinaryOp::Lt) => Ok(Value::Bool(compare_numeric(&left_val, &right_val, |a, b| a < b))),
                Ok(BinaryOp::Le) => Ok(Value::Bool(compare_numeric(&left_val, &right_val, |a, b| a <= b))),
                Ok(BinaryOp::Gt) => Ok(Value::Bool(compare_numeric(&left_val, &right_val, |a, b| a > b))),
                Ok(BinaryOp::Ge) => Ok(Value::Bool(compare_numeric(&left_val, &right_val, |a, b| a >= b))),
                Ok(BinaryOp::And) | Ok(BinaryOp::Or) => unreachable!(), // handled above
                Ok(BinaryOp::In) => {
                    // Check if left is in right (array/object)
                    match &right_val {
                        Value::Array(arr) => Ok(Value::Bool(arr.contains(&left_val))),
                        Value::Object(obj) => {
                            match &left_val {
                                Value::String(s) => Ok(Value::Bool(obj.contains_key(s))),
                                _ => Ok(Value::Bool(false)),
                            }
                        }
                        _ => Err(anyhow!("'in' operator requires array or object on right side"))
                    }
                }
                Ok(BinaryOp::NotIn) => {
                    // Check if left is NOT in right
                    match &right_val {
                        Value::Array(arr) => Ok(Value::Bool(!arr.contains(&left_val))),
                        Value::Object(obj) => {
                            match &left_val {
                                Value::String(s) => Ok(Value::Bool(!obj.contains_key(s))),
                                _ => Ok(Value::Bool(true)),
                            }
                        }
                        _ => Err(anyhow!("'not in' operator requires array or object on right side"))
                    }
                }
                Ok(BinaryOp::Unspecified) | Err(_) => Err(anyhow!("Unknown binary operator: {}", binop.op))
            }
        }

        Some(Kind::UnaryOp(unop)) => {
            let operand = unop.operand.as_ref()
                .ok_or_else(|| anyhow!("UnaryOp missing operand"))?;
            let val = eval_expression(operand, context)?;

            match UnaryOp::try_from(unop.op) {
                Ok(UnaryOp::Not) => Ok(Value::Bool(!value_to_bool(&val))),
                Ok(UnaryOp::Neg) => {
                    match val.as_f64() {
                        Some(n) => serde_json::Number::from_f64(-n)
                            .map(Value::Number)
                            .ok_or_else(|| anyhow!("Invalid negation result")),
                        None => Err(anyhow!("Cannot negate non-number: {:?}", val))
                    }
                }
                Ok(UnaryOp::Unspecified) | Err(_) => Err(anyhow!("Unknown unary operator: {}", unop.op))
            }
        }

        Some(Kind::Call(call)) => {
            // Handle built-in functions
            let args: Result<Vec<Value>> = call.args.iter()
                .map(|e| eval_expression(e, context))
                .collect();
            let args = args?;

            match call.function.as_str() {
                "len" => {
                    if args.len() != 1 {
                        return Err(anyhow!("len() takes 1 argument, got {}", args.len()));
                    }
                    match &args[0] {
                        Value::Array(a) => Ok(Value::Number((a.len() as i64).into())),
                        Value::String(s) => Ok(Value::Number((s.len() as i64).into())),
                        Value::Object(o) => Ok(Value::Number((o.len() as i64).into())),
                        _ => Err(anyhow!("len() not supported for {:?}", args[0]))
                    }
                }
                "str" => {
                    if args.len() != 1 {
                        return Err(anyhow!("str() takes 1 argument, got {}", args.len()));
                    }
                    Ok(Value::String(format!("{}", args[0])))
                }
                "int" => {
                    if args.len() != 1 {
                        return Err(anyhow!("int() takes 1 argument, got {}", args.len()));
                    }
                    match &args[0] {
                        Value::Number(n) => Ok(Value::Number((n.as_i64().unwrap_or(0)).into())),
                        Value::String(s) => s.parse::<i64>()
                            .map(|n| Value::Number(n.into()))
                            .map_err(|_| anyhow!("Cannot convert '{}' to int", s)),
                        _ => Err(anyhow!("int() not supported for {:?}", args[0]))
                    }
                }
                _ => Err(anyhow!("Unknown function: {}", call.function))
            }
        }

        Some(Kind::Attribute(attr)) => {
            let base = attr.base.as_ref()
                .ok_or_else(|| anyhow!("Attribute access missing base"))?;
            let base_val = eval_expression(base, context)?;

            match base_val {
                Value::Object(obj) => {
                    obj.get(&attr.attribute)
                        .cloned()
                        .ok_or_else(|| anyhow!("Attribute '{}' not found", attr.attribute))
                }
                _ => Err(anyhow!("Cannot access attribute on {:?}", base_val))
            }
        }
    }
}

fn numeric_binop<F>(left: &Value, right: &Value, op: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => {
            let result = op(l, r);
            // Try to preserve integer type if both inputs were integers
            if left.as_i64().is_some() && right.as_i64().is_some() && result.fract() == 0.0 {
                Ok(Value::Number((result as i64).into()))
            } else {
                serde_json::Number::from_f64(result)
                    .map(Value::Number)
                    .ok_or_else(|| anyhow!("Invalid arithmetic result"))
            }
        }
        _ => Err(anyhow!("Cannot perform arithmetic on non-numbers: {:?} and {:?}", left, right))
    }
}

fn compare_numeric<F>(left: &Value, right: &Value, cmp: F) -> bool
where
    F: Fn(f64, f64) -> bool,
{
    match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => cmp(l, r),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{LoopHeadMeta, Node};

    #[test]
    fn test_simple_sequence() {
        let mut dag = Dag::new(false);

        let mut node_a = Node::action("a", "action_a");
        node_a.produces = vec!["result_a".to_string()];
        node_a.add_edge("b", EdgeKind::Data);
        dag.add_node(node_a);

        let mut node_b = Node::action("b", "action_b");
        node_b.produces = vec!["result_b".to_string()];
        dag.add_node(node_b);

        let mut state = InstanceState::new(&dag, HashMap::new());
        let scheduler = Scheduler::new(&dag);

        // Initially only A should be ready
        let ready = state.ready_nodes();
        assert_eq!(ready, vec!["a"]);

        // Complete A with result
        let result = scheduler.handle_completion(
            &mut state,
            "a",
            Some(Value::String("hello".to_string())),
            true,
        ).unwrap();

        // B should now be ready
        if let SchedulerAction::NodesReady { node_ids } = result {
            assert_eq!(node_ids, vec!["b"]);
        } else {
            panic!("Expected NodesReady");
        }

        // Result should be in eval_context
        assert_eq!(
            state.get_var("result_a"),
            Some(&Value::String("hello".to_string()))
        );
    }

    #[test]
    fn test_loop_execution() {
        let mut dag = Dag::new(false);

        // Create: loop items -> body action -> back edge
        let loop_meta = LoopHeadMeta {
            iterator_var: "items".to_string(),
            loop_var: "item".to_string(),
            body_entry: "body".to_string(),
            body_tail: "body".to_string(),
            body_nodes: ["body".to_string()].into_iter().collect(),
        };

        let mut loop_head = Node::loop_head("loop", loop_meta);
        loop_head.add_edge("body", EdgeKind::Continue);
        loop_head.produces = vec!["results".to_string()];
        dag.add_node(loop_head);

        let mut body = Node::action("body", "process");
        body.add_edge("loop", EdgeKind::Back);
        body.loop_id = Some("loop".to_string());
        dag.add_node(body);

        // Initialize with iterator
        let mut initial = HashMap::new();
        initial.insert("items".to_string(), Value::Array(vec![
            Value::Number(1.into()),
            Value::Number(2.into()),
        ]));

        let mut state = InstanceState::new(&dag, initial);
        let scheduler = Scheduler::new(&dag);

        // Loop head should be ready
        let ready = state.ready_nodes();
        assert!(ready.contains(&"loop".to_string()));

        // Process loop head - should unlock body with first item
        let action = scheduler.process_ready_node(
            &mut state,
            "loop",
            &WorkflowArguments::default(),
        ).unwrap();

        if let SchedulerAction::NodesReady { node_ids } = action {
            assert!(node_ids.contains(&"body".to_string()));
        } else {
            panic!("Expected NodesReady");
        }

        // Check loop var is bound
        assert_eq!(state.get_var("item"), Some(&Value::Number(1.into())));
    }
}
