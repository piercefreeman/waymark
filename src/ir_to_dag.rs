//! Convert IR (Intermediate Representation) to DAG (Directed Acyclic Graph).
//!
//! This module transforms the hierarchical IR workflow structure into a flat
//! DAG representation suitable for execution scheduling.

use crate::ir_parser::proto as ir;
use crate::messages::proto::{self as msg, WorkflowDagDefinition, WorkflowDagNode};
use std::collections::HashMap;

/// Error during IR-to-DAG conversion.
#[derive(Debug, Clone)]
pub struct ConversionError {
    pub message: String,
    pub location: Option<ir::SourceLocation>,
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(loc) = &self.location {
            write!(f, "{} (line {}, col {})", self.message, loc.lineno, loc.col_offset)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for ConversionError {}

/// State for tracking node generation during conversion.
struct ConversionState {
    nodes: Vec<WorkflowDagNode>,
    edges: Vec<msg::DagEdge>,
    node_counter: u32,
    /// Maps variable names to the node that produces them
    variable_producers: HashMap<String, String>,
    /// The last completed node ID (for sequencing)
    last_node_id: Option<String>,
    /// Return variable name
    return_variable: Option<String>,
}

impl ConversionState {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            node_counter: 0,
            variable_producers: HashMap::new(),
            last_node_id: None,
            return_variable: None,
        }
    }

    fn next_node_id(&mut self, prefix: &str) -> String {
        let id = format!("{}_{}", prefix, self.node_counter);
        self.node_counter += 1;
        id
    }

    fn add_node(&mut self, mut node: WorkflowDagNode) {
        // Add dependency on last node for sequential execution
        if let Some(last_id) = &self.last_node_id {
            if !node.depends_on.contains(last_id) {
                node.depends_on.push(last_id.clone());
            }
        }

        // Track what this node produces
        for var in &node.produces {
            self.variable_producers.insert(var.clone(), node.id.clone());
        }

        self.last_node_id = Some(node.id.clone());
        self.nodes.push(node);
    }

    #[allow(dead_code)]
    fn add_edge(&mut self, from: &str, to: &str, edge_type: msg::EdgeType) {
        self.edges.push(msg::DagEdge {
            from_node: from.to_string(),
            to_node: to.to_string(),
            edge_type: edge_type as i32,
        });
    }
}

/// Convert an IR Workflow to a DAG Definition.
pub fn convert_workflow(workflow: &ir::Workflow, concurrent: bool) -> Result<WorkflowDagDefinition, ConversionError> {
    let mut state = ConversionState::new();

    // Process each statement in the workflow body
    for stmt in &workflow.body {
        convert_statement(&mut state, stmt)?;
    }

    Ok(WorkflowDagDefinition {
        concurrent,
        nodes: state.nodes,
        return_variable: state.return_variable.unwrap_or_default(),
        edges: state.edges,
    })
}

fn convert_statement(state: &mut ConversionState, stmt: &ir::Statement) -> Result<(), ConversionError> {
    let kind = stmt.kind.as_ref().ok_or_else(|| ConversionError {
        message: "Statement has no kind".to_string(),
        location: None,
    })?;

    match kind {
        ir::statement::Kind::ActionCall(action) => {
            convert_action_call(state, action)?;
        }
        ir::statement::Kind::Gather(gather) => {
            convert_gather(state, gather)?;
        }
        ir::statement::Kind::PythonBlock(block) => {
            convert_python_block(state, block)?;
        }
        ir::statement::Kind::Loop(loop_) => {
            convert_loop(state, loop_)?;
        }
        ir::statement::Kind::Conditional(cond) => {
            convert_conditional(state, cond)?;
        }
        ir::statement::Kind::TryExcept(te) => {
            convert_try_except(state, te)?;
        }
        ir::statement::Kind::Sleep(sleep) => {
            convert_sleep(state, sleep)?;
        }
        ir::statement::Kind::ReturnStmt(ret) => {
            convert_return(state, ret)?;
        }
        ir::statement::Kind::Spread(spread) => {
            convert_spread(state, spread)?;
        }
    }

    Ok(())
}

fn convert_action_call(state: &mut ConversionState, action: &ir::ActionCall) -> Result<String, ConversionError> {
    let node_id = state.next_node_id("action");

    let mut node = WorkflowDagNode {
        id: node_id.clone(),
        action: action.action.clone(),
        module: action.module.clone().unwrap_or_default(),
        ..Default::default()
    };

    // Copy kwargs
    for (k, v) in &action.kwargs {
        node.kwargs.insert(k.clone(), v.clone());
    }

    // Set produces if target is specified
    if let Some(target) = &action.target {
        node.produces.push(target.clone());
    }

    // Add dependencies based on variables used in kwargs
    for v in action.kwargs.values() {
        if let Some(producer) = state.variable_producers.get(v) {
            if !node.depends_on.contains(producer) {
                node.depends_on.push(producer.clone());
            }
        }
    }

    // Handle config (timeout, retries, backoff)
    if let Some(config) = &action.config {
        if let Some(timeout) = config.timeout_seconds {
            node.timeout_seconds = Some(timeout);
        }
        if let Some(retries) = config.max_retries {
            node.max_retries = Some(retries);
        }
        if let Some(backoff) = &config.backoff {
            node.backoff = Some(convert_backoff(backoff));
        }
    }

    state.add_node(node);
    Ok(node_id)
}

fn convert_backoff(ir_backoff: &ir::BackoffConfig) -> msg::BackoffPolicy {
    let kind = ir::backoff_config::Kind::try_from(ir_backoff.kind).unwrap_or(ir::backoff_config::Kind::Linear);
    match kind {
        ir::backoff_config::Kind::Linear | ir::backoff_config::Kind::Unspecified => {
            msg::BackoffPolicy {
                policy: Some(msg::backoff_policy::Policy::Linear(msg::LinearBackoff {
                    base_delay_ms: ir_backoff.base_delay_ms,
                })),
            }
        }
        ir::backoff_config::Kind::Exponential => {
            msg::BackoffPolicy {
                policy: Some(msg::backoff_policy::Policy::Exponential(msg::ExponentialBackoff {
                    base_delay_ms: ir_backoff.base_delay_ms,
                    multiplier: ir_backoff.multiplier.unwrap_or(2.0),
                })),
            }
        }
    }
}

fn convert_gather(state: &mut ConversionState, gather: &ir::Gather) -> Result<(), ConversionError> {
    // For gather, we create multiple action nodes that can run in parallel
    // They all depend on the same previous node but not on each other

    let prev_last = state.last_node_id.clone();
    let mut parallel_node_ids = Vec::new();

    for (i, call) in gather.calls.iter().enumerate() {
        let call_kind = call.kind.as_ref().ok_or_else(|| ConversionError {
            message: "Gather call has no kind".to_string(),
            location: gather.location.clone(),
        })?;

        // Temporarily reset last_node_id so parallel calls don't depend on each other
        state.last_node_id = prev_last.clone();

        match call_kind {
            ir::gather_call::Kind::Action(action) => {
                // Create a modified action with indexed target if gather has a target
                let mut action_with_target = action.clone();
                if let Some(target) = &gather.target {
                    action_with_target.target = Some(format!("{}__item{}", target, i));
                }
                let node_id = convert_action_call(state, &action_with_target)?;
                parallel_node_ids.push(node_id);
            }
            ir::gather_call::Kind::Subgraph(subgraph) => {
                // For subgraphs, we'd need to inline or reference the subgraph
                // For now, create a placeholder node
                let node_id = state.next_node_id("subgraph");
                let mut node = WorkflowDagNode {
                    id: node_id.clone(),
                    action: format!("__subgraph_{}", subgraph.method_name),
                    ..Default::default()
                };
                if let Some(target) = &gather.target {
                    node.produces.push(format!("{}__item{}", target, i));
                }
                if let Some(prev) = &prev_last {
                    node.depends_on.push(prev.clone());
                }
                state.nodes.push(node);
                parallel_node_ids.push(node_id);
            }
        }
    }

    // Create a sync node if there's a target to collect results
    if let Some(target) = &gather.target {
        let sync_id = state.next_node_id("gather_sync");
        let sync_node = WorkflowDagNode {
            id: sync_id.clone(),
            action: "__gather_sync".to_string(),
            produces: vec![target.clone()],
            depends_on: parallel_node_ids,
            ..Default::default()
        };
        state.nodes.push(sync_node);
        state.last_node_id = Some(sync_id.clone());
        state.variable_producers.insert(target.clone(), sync_id);
    } else if !parallel_node_ids.is_empty() {
        // No target, but we need to track that all parallel nodes completed
        state.last_node_id = Some(parallel_node_ids.last().unwrap().clone());
    }

    Ok(())
}

fn convert_python_block(state: &mut ConversionState, block: &ir::PythonBlock) -> Result<(), ConversionError> {
    let node_id = state.next_node_id("python");

    let mut node = WorkflowDagNode {
        id: node_id.clone(),
        action: "__python_block".to_string(),
        ..Default::default()
    };

    // Store the code in kwargs
    node.kwargs.insert("__code".to_string(), block.code.clone());

    // Track inputs/outputs
    for input in &block.inputs {
        if let Some(producer) = state.variable_producers.get(input) {
            if !node.depends_on.contains(producer) {
                node.depends_on.push(producer.clone());
            }
        }
    }

    for output in &block.outputs {
        node.produces.push(output.clone());
    }

    state.add_node(node);

    // Update variable producers for outputs
    for output in &block.outputs {
        state.variable_producers.insert(output.clone(), node_id.clone());
    }

    Ok(())
}

fn convert_loop(state: &mut ConversionState, loop_: &ir::Loop) -> Result<(), ConversionError> {
    // Create a loop head node
    let loop_id = state.next_node_id("loop");
    let loop_head_id = format!("{}_head", loop_id);

    // Create the loop head node
    let mut loop_head = WorkflowDagNode {
        id: loop_head_id.clone(),
        action: "__loop_head".to_string(),
        node_type: msg::NodeType::LoopHead as i32,
        loop_id: Some(loop_id.clone()),
        ..Default::default()
    };

    // Add loop metadata
    let mut accumulators = Vec::new();
    for acc in &loop_.accumulators {
        accumulators.push(msg::AccumulatorSpec {
            var: acc.clone(),
            source_node: None,
            source_expr: None,
        });
    }

    loop_head.loop_head_meta = Some(msg::LoopHeadMeta {
        iterator_source: loop_.iterator_expr.clone(),
        loop_var: loop_.loop_var.clone(),
        body_entry: vec![],  // Will be filled after processing body
        body_tail: String::new(),
        exit_target: String::new(),
        accumulators,
        preamble: vec![],
        body_nodes: vec![],
    });

    if let Some(last) = &state.last_node_id {
        loop_head.depends_on.push(last.clone());
    }

    state.nodes.push(loop_head);
    state.last_node_id = Some(loop_head_id.clone());

    // Process loop body actions
    let mut body_node_ids = Vec::new();
    for action in &loop_.body {
        let node_id = convert_action_call(state, action)?;
        // Mark this node as part of the loop
        if let Some(node) = state.nodes.iter_mut().find(|n| n.id == node_id) {
            node.loop_id = Some(loop_id.clone());
        }
        body_node_ids.push(node_id);
    }

    // Update loop head meta with body info
    if let Some(head) = state.nodes.iter_mut().find(|n| n.id == loop_head_id) {
        if let Some(meta) = &mut head.loop_head_meta {
            meta.body_entry = body_node_ids.clone();
            if let Some(last) = body_node_ids.last() {
                meta.body_tail = last.clone();
            }
            meta.body_nodes = body_node_ids;
        }
    }

    // Track accumulators as produced by the loop
    for acc in &loop_.accumulators {
        state.variable_producers.insert(acc.clone(), loop_head_id.clone());
    }

    Ok(())
}

fn convert_conditional(state: &mut ConversionState, cond: &ir::Conditional) -> Result<(), ConversionError> {
    // For conditionals, we create guarded action nodes for each branch
    let prev_last = state.last_node_id.clone();
    let mut branch_end_nodes = Vec::new();

    for branch in &cond.branches {
        // Reset to branch from the same point
        state.last_node_id = prev_last.clone();

        // Process preamble if any
        for pre in &branch.preamble {
            convert_python_block(state, pre)?;
        }

        // Process actions with guard
        for action in &branch.actions {
            let node_id = convert_action_call(state, action)?;
            // Add guard to the node
            if let Some(node) = state.nodes.iter_mut().find(|n| n.id == node_id) {
                node.guard = branch.guard.clone();
            }
        }

        // Process postamble if any
        for post in &branch.postamble {
            convert_python_block(state, post)?;
        }

        if let Some(last) = &state.last_node_id {
            branch_end_nodes.push(last.clone());
        }
    }

    // Create a join node if there's a target
    if let Some(target) = &cond.target {
        let join_id = state.next_node_id("cond_join");
        let join_node = WorkflowDagNode {
            id: join_id.clone(),
            action: "__conditional_join".to_string(),
            depends_on: branch_end_nodes,
            produces: vec![target.clone()],
            ..Default::default()
        };
        state.nodes.push(join_node);
        state.last_node_id = Some(join_id.clone());
        state.variable_producers.insert(target.clone(), join_id);
    } else if !branch_end_nodes.is_empty() {
        // No target, just track last node
        state.last_node_id = Some(branch_end_nodes.last().unwrap().clone());
    }

    Ok(())
}

fn convert_try_except(state: &mut ConversionState, te: &ir::TryExcept) -> Result<(), ConversionError> {
    let try_start = state.last_node_id.clone();

    // Convert try body actions
    let mut try_node_ids = Vec::new();
    for action in &te.try_body {
        let node_id = convert_action_call(state, action)?;
        try_node_ids.push(node_id);
    }

    // Convert handlers
    for handler in &te.handlers {
        // Reset to branch from try start
        state.last_node_id = try_start.clone();

        for action in &handler.body {
            let _node_id = convert_action_call(state, action)?;
            // Add exception edge from try nodes to this handler
            for try_id in &try_node_ids {
                if let Some(try_node) = state.nodes.iter_mut().find(|n| n.id == *try_id) {
                    for exc_type in &handler.exception_types {
                        let edge = msg::WorkflowExceptionEdge {
                            source_node_id: try_id.clone(),
                            exception_type: exc_type.name.clone().unwrap_or_default(),
                            exception_module: exc_type.module.clone().unwrap_or_default(),
                        };
                        try_node.exception_edges.push(edge);
                    }
                }
            }
        }
    }

    Ok(())
}

fn convert_sleep(state: &mut ConversionState, sleep: &ir::Sleep) -> Result<(), ConversionError> {
    let node_id = state.next_node_id("sleep");

    let mut node = WorkflowDagNode {
        id: node_id.clone(),
        action: "__sleep".to_string(),
        ..Default::default()
    };

    // Store duration expression
    node.kwargs.insert("__duration".to_string(), sleep.duration_expr.clone());

    state.add_node(node);
    Ok(())
}

fn convert_return(state: &mut ConversionState, ret: &ir::Return) -> Result<(), ConversionError> {
    let value = ret.value.as_ref();

    match value {
        Some(ir::r#return::Value::Expr(expr)) => {
            // Simple expression return - track as the return variable
            state.return_variable = Some(expr.clone());
        }
        Some(ir::r#return::Value::Action(action)) => {
            // Return with action - the action produces __workflow_return
            convert_action_call(state, action)?;
            state.return_variable = Some("__workflow_return".to_string());
        }
        Some(ir::r#return::Value::Gather(gather)) => {
            // Return with gather - the gather produces __workflow_return
            convert_gather(state, gather)?;
            state.return_variable = Some("__workflow_return".to_string());
        }
        None => {
            // No return value
        }
    }

    Ok(())
}

fn convert_spread(state: &mut ConversionState, spread: &ir::Spread) -> Result<(), ConversionError> {
    // Spread creates parallel actions over a collection
    let node_id = state.next_node_id("spread");

    let mut node = WorkflowDagNode {
        id: node_id.clone(),
        action: spread.action.as_ref().map(|a| a.action.clone()).unwrap_or_default(),
        module: spread.action.as_ref().and_then(|a| a.module.clone()).unwrap_or_default(),
        ..Default::default()
    };

    // Copy kwargs from inner action
    if let Some(action) = &spread.action {
        for (k, v) in &action.kwargs {
            node.kwargs.insert(k.clone(), v.clone());
        }
    }

    // Store spread metadata
    node.kwargs.insert("__spread_loop_var".to_string(), spread.loop_var.clone());
    node.kwargs.insert("__spread_iterable".to_string(), spread.iterable.clone());

    if let Some(target) = &spread.target {
        node.produces.push(target.clone());
    }

    state.add_node(node);

    if let Some(target) = &spread.target {
        state.variable_producers.insert(target.clone(), node_id);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_action(name: &str, target: Option<&str>) -> ir::ActionCall {
        ir::ActionCall {
            action: name.to_string(),
            module: Some("test".to_string()),
            kwargs: HashMap::new(),
            target: target.map(|s| s.to_string()),
            config: None,
            location: None,
        }
    }

    fn make_statement(kind: ir::statement::Kind) -> ir::Statement {
        ir::Statement { kind: Some(kind) }
    }

    #[test]
    fn test_simple_workflow() {
        let workflow = ir::Workflow {
            name: "test".to_string(),
            params: vec![],
            body: vec![
                make_statement(ir::statement::Kind::ActionCall(make_action("fetch", Some("data")))),
                make_statement(ir::statement::Kind::ActionCall(make_action("process", Some("result")))),
            ],
            return_type: None,
        };

        let dag = convert_workflow(&workflow, false).unwrap();

        assert_eq!(dag.nodes.len(), 2);
        assert_eq!(dag.nodes[0].action, "fetch");
        assert_eq!(dag.nodes[0].produces, vec!["data"]);
        assert_eq!(dag.nodes[1].action, "process");
        assert!(dag.nodes[1].depends_on.contains(&dag.nodes[0].id));
    }

    #[test]
    fn test_gather() {
        let workflow = ir::Workflow {
            name: "test".to_string(),
            params: vec![],
            body: vec![
                make_statement(ir::statement::Kind::Gather(ir::Gather {
                    calls: vec![
                        ir::GatherCall {
                            kind: Some(ir::gather_call::Kind::Action(make_action("fetch_a", None))),
                        },
                        ir::GatherCall {
                            kind: Some(ir::gather_call::Kind::Action(make_action("fetch_b", None))),
                        },
                    ],
                    target: Some("results".to_string()),
                    location: None,
                })),
            ],
            return_type: None,
        };

        let dag = convert_workflow(&workflow, true).unwrap();

        // Should have 2 parallel action nodes + 1 sync node
        assert_eq!(dag.nodes.len(), 3);

        // The two action nodes should not depend on each other
        let action_a = dag.nodes.iter().find(|n| n.action == "fetch_a").unwrap();
        let action_b = dag.nodes.iter().find(|n| n.action == "fetch_b").unwrap();
        assert!(!action_a.depends_on.contains(&action_b.id));
        assert!(!action_b.depends_on.contains(&action_a.id));

        // The sync node should depend on both
        let sync = dag.nodes.iter().find(|n| n.action == "__gather_sync").unwrap();
        assert!(sync.depends_on.contains(&action_a.id));
        assert!(sync.depends_on.contains(&action_b.id));
    }

    #[test]
    fn test_return_expression() {
        let workflow = ir::Workflow {
            name: "test".to_string(),
            params: vec![],
            body: vec![
                make_statement(ir::statement::Kind::ActionCall(make_action("fetch", Some("data")))),
                make_statement(ir::statement::Kind::ReturnStmt(ir::Return {
                    value: Some(ir::r#return::Value::Expr("data".to_string())),
                    location: None,
                })),
            ],
            return_type: None,
        };

        let dag = convert_workflow(&workflow, false).unwrap();
        assert_eq!(dag.return_variable, "data");
    }
}
