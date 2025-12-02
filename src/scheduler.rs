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
use crate::messages::proto::{
    NodeContext, NodeDispatch, NodeInfo, WorkflowArguments,
};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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
        self.eval_context.insert(name.into(), value);
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

        match node.kind {
            NodeKind::LoopHead => {
                // Evaluate loop head locally (not dispatched to worker)
                self.evaluate_loop_head(state, node)
            }
            NodeKind::Branch => {
                // Evaluate branch condition locally
                self.evaluate_branch(state, node)
            }
            NodeKind::GatherJoin => {
                // Gather results from parallel branches
                self.evaluate_gather_join(state, node)
            }
            NodeKind::Action | NodeKind::Computed | NodeKind::Sleep => {
                // Dispatch to worker
                let dispatch = self.build_dispatch(state, node, workflow_input)?;
                state.mark_running(node_id);
                Ok(SchedulerAction::Dispatch {
                    node_id: node_id.to_string(),
                    dispatch,
                })
            }
            NodeKind::TryHead => {
                // Try head just passes through to try body
                self.mark_complete_and_unlock(state, node, None)
            }
            NodeKind::Return => {
                // Return node - workflow complete
                let result = self.dag.return_variable.as_ref()
                    .and_then(|var| state.get_var(var).cloned());
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

        if !success {
            // Handle failure - check for exception handlers
            return self.handle_failure(state, node, result);
        }

        // Store result in eval_context
        if let Some(value) = result {
            for var in &node.produces {
                state.set_var(var.clone(), value.clone());
            }
        }

        // Check for back edge (loop iteration complete)
        if let Some(loop_head_id) = self.get_back_edge_target(node) {
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

        if current_idx >= items.len() {
            // Loop complete - unlock exit edges
            state.node_states.insert(node.id.clone(), NodeState::Completed);

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

        // Evaluate guard expression
        let guard_result = self.evaluate_guard(&meta.guard_expr, state)?;

        state.node_states.insert(node.id.clone(), NodeState::Completed);

        let mut newly_ready = Vec::new();

        if guard_result {
            // Unlock true branch, skip false branch
            if let Some(true_entry) = &meta.true_entry {
                if self.try_unlock_node(state, true_entry) {
                    newly_ready.push(true_entry.clone());
                }
            }
            // Mark false branch nodes as skipped
            for node_id in &meta.false_nodes {
                state.node_states.insert(node_id.clone(), NodeState::Skipped);
            }
        } else {
            // Unlock false branch, skip true branch
            if let Some(false_entry) = &meta.false_entry {
                if self.try_unlock_node(state, false_entry) {
                    newly_ready.push(false_entry.clone());
                }
            }
            // Mark true branch nodes as skipped
            for node_id in &meta.true_nodes {
                state.node_states.insert(node_id.clone(), NodeState::Skipped);
            }
        }

        Ok(SchedulerAction::NodesReady { node_ids: newly_ready })
    }

    /// Evaluate a gather join node
    fn evaluate_gather_join(
        &self,
        state: &mut InstanceState,
        node: &Node,
    ) -> Result<SchedulerAction> {
        // Gather join just passes through - the results are already in eval_context
        // from the parallel nodes that completed
        self.mark_complete_and_unlock(state, node, None)
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

        Ok(SchedulerAction::NodesReady { node_ids: newly_ready })
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

    /// Evaluate a guard expression against eval_context
    fn evaluate_guard(&self, expr: &str, state: &InstanceState) -> Result<bool> {
        // Simple evaluation for common patterns
        // TODO: Use ast_eval for full expression support

        if expr.is_empty() || expr == "True" {
            return Ok(true);
        }
        if expr == "False" {
            return Ok(false);
        }

        // Check for simple variable reference
        if let Some(value) = state.get_var(expr) {
            return Ok(value_to_bool(value));
        }

        // For now, default to true for complex expressions
        // TODO: Full expression evaluation
        Ok(true)
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
fn build_context_for_node(state: &InstanceState, _node: &Node) -> Vec<NodeContext> {
    // For now, include all variables in context
    // TODO: Optimize to only include variables referenced in kwargs
    state.eval_context.iter()
        .map(|(var, value)| {
            NodeContext {
                variable: var.clone(),
                payload: Some(value_to_workflow_args(var, value)),
                source_node_id: String::new(),
            }
        })
        .collect()
}

/// Convert a Value to WorkflowArguments
fn value_to_workflow_args(key: &str, value: &Value) -> WorkflowArguments {
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

    WorkflowArguments {
        arguments: vec![WorkflowArgument {
            key: key.to_string(),
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
