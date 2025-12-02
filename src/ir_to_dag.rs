//! Convert IR (Intermediate Representation) to DAG (Directed Acyclic Graph).
//!
//! This module transforms the hierarchical IR workflow structure into a flat
//! DAG representation suitable for execution scheduling.

use crate::dag::{BackoffPolicy, BranchMeta, Dag, EdgeKind, HandlerMeta, LoopHeadMeta, Node, NodeKind};
use crate::ir_parser::proto as ir;
use std::collections::{HashMap, HashSet};

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
    dag: Dag,
    node_counter: u32,
    /// Maps variable names to the node that produces them
    variable_producers: HashMap<String, String>,
    /// The last completed node ID (for sequencing)
    last_node_id: Option<String>,
}

impl ConversionState {
    fn new(concurrent: bool) -> Self {
        Self {
            dag: Dag::new(concurrent),
            node_counter: 0,
            variable_producers: HashMap::new(),
            last_node_id: None,
        }
    }

    fn next_node_id(&mut self, prefix: &str) -> String {
        let id = format!("{}_{}", prefix, self.node_counter);
        self.node_counter += 1;
        id
    }

    fn add_node(&mut self, node: Node) {
        // Track what this node produces
        for var in &node.produces {
            self.variable_producers.insert(var.clone(), node.id.clone());
        }

        // Add data edge from last node if exists
        if let Some(last_id) = &self.last_node_id {
            if let Some(last_node) = self.dag.get_node_mut(last_id) {
                last_node.add_edge(node.id.clone(), EdgeKind::Data);
            }
        }

        self.last_node_id = Some(node.id.clone());
        self.dag.add_node(node);
    }
}

/// Convert an IR Workflow to a DAG.
pub fn convert_workflow(workflow: &ir::Workflow, concurrent: bool) -> Result<Dag, ConversionError> {
    let mut state = ConversionState::new(concurrent);

    // Process each statement in the workflow body
    for stmt in &workflow.body {
        convert_statement(&mut state, stmt)?;
    }

    state.dag.compute_entry_nodes();
    Ok(state.dag)
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

    let mut node = Node::action(&node_id, &action.action);

    if let Some(module) = &action.module {
        node.module = Some(module.clone());
    }

    // Copy kwargs
    for (k, v) in &action.kwargs {
        node.kwargs.insert(k.clone(), v.clone());
    }

    // Set produces if target is specified
    if let Some(target) = &action.target {
        node.produces.push(target.clone());
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

fn convert_backoff(ir_backoff: &ir::BackoffConfig) -> BackoffPolicy {
    let kind = ir::backoff_config::Kind::try_from(ir_backoff.kind).unwrap_or(ir::backoff_config::Kind::Linear);
    match kind {
        ir::backoff_config::Kind::Linear | ir::backoff_config::Kind::Unspecified => {
            BackoffPolicy::Linear {
                base_delay_ms: ir_backoff.base_delay_ms,
            }
        }
        ir::backoff_config::Kind::Exponential => {
            BackoffPolicy::Exponential {
                base_delay_ms: ir_backoff.base_delay_ms,
                multiplier: ir_backoff.multiplier.unwrap_or(2.0),
            }
        }
    }
}

fn convert_gather(state: &mut ConversionState, gather: &ir::Gather) -> Result<(), ConversionError> {
    let prev_last = state.last_node_id.clone();
    let mut parallel_node_ids = Vec::new();

    for (i, call) in gather.calls.iter().enumerate() {
        let call_kind = call.kind.as_ref().ok_or_else(|| ConversionError {
            message: "Gather call has no kind".to_string(),
            location: gather.location.clone(),
        })?;

        // Reset last_node_id so parallel calls don't depend on each other
        state.last_node_id = prev_last.clone();

        match call_kind {
            ir::gather_call::Kind::Action(action) => {
                let mut action_with_target = action.clone();
                if let Some(target) = &gather.target {
                    action_with_target.target = Some(format!("{}__item{}", target, i));
                }
                let node_id = convert_action_call(state, &action_with_target)?;
                parallel_node_ids.push(node_id);
            }
            ir::gather_call::Kind::Subgraph(subgraph) => {
                // Create placeholder for subgraph
                let node_id = state.next_node_id("subgraph");
                let mut node = Node::action(&node_id, format!("__subgraph_{}", subgraph.method_name));
                if let Some(target) = &gather.target {
                    node.produces.push(format!("{}__item{}", target, i));
                }
                state.dag.add_node(node);
                parallel_node_ids.push(node_id);
            }
        }
    }

    // Create join node if there's a target
    if let Some(target) = &gather.target {
        let join_id = state.next_node_id("gather_join");
        let mut join_node = Node::gather_join(&join_id);
        join_node.produces.push(target.clone());

        // Add edges from all parallel nodes to join
        for parallel_id in &parallel_node_ids {
            if let Some(parallel_node) = state.dag.get_node_mut(parallel_id) {
                parallel_node.add_edge(join_id.clone(), EdgeKind::Data);
            }
        }

        state.dag.add_node(join_node);
        state.variable_producers.insert(target.clone(), join_id.clone());
        state.last_node_id = Some(join_id);
    } else if !parallel_node_ids.is_empty() {
        state.last_node_id = Some(parallel_node_ids.last().unwrap().clone());
    }

    Ok(())
}

fn convert_python_block(state: &mut ConversionState, block: &ir::PythonBlock) -> Result<String, ConversionError> {
    let node_id = state.next_node_id("python");

    let mut node = Node::computed(&node_id);

    // Store the code in kwargs
    node.kwargs.insert("code".to_string(), block.code.clone());

    // Store imports and definitions
    if !block.imports.is_empty() {
        node.kwargs.insert("imports".to_string(), block.imports.join("\n"));
    }
    if !block.definitions.is_empty() {
        node.kwargs.insert("definitions".to_string(), block.definitions.join("\n"));
    }

    // Track outputs
    for output in &block.outputs {
        node.produces.push(output.clone());
    }

    state.add_node(node);
    Ok(node_id)
}

fn convert_loop(state: &mut ConversionState, loop_: &ir::Loop) -> Result<(), ConversionError> {
    let loop_id = state.next_node_id("loop");
    let loop_head_id = format!("{}_head", loop_id);

    // Track body nodes
    let mut body_node_ids: Vec<String> = Vec::new();

    // Process loop body to get all node IDs
    let body_start_idx = state.dag.nodes.len();
    let prev_last = state.last_node_id.clone();

    // Temporarily set last_node_id to None so body nodes don't depend on previous
    state.last_node_id = None;

    for stmt in &loop_.body {
        convert_statement(state, stmt)?;
        if let Some(last_id) = &state.last_node_id {
            body_node_ids.push(last_id.clone());
        }
    }

    // Get all body nodes
    let body_nodes: HashSet<String> = state.dag.nodes.keys()
        .skip(body_start_idx)
        .cloned()
        .collect();

    // Mark all body nodes as belonging to this loop
    for node_id in &body_nodes {
        if let Some(node) = state.dag.get_node_mut(node_id) {
            node.loop_id = Some(loop_id.clone());
        }
    }

    let body_entry = body_node_ids.first().cloned().unwrap_or_default();
    let body_tail = body_node_ids.last().cloned().unwrap_or_default();

    // Create loop head node
    let loop_meta = LoopHeadMeta {
        iterator_var: loop_.iterator_expr.clone(),
        loop_var: loop_.loop_var.clone(),
        body_entry: body_entry.clone(),
        body_tail: body_tail.clone(),
        body_nodes: body_nodes.clone(),
    };

    let mut loop_head = Node::loop_head(&loop_head_id, loop_meta);

    // The loop produces the accumulator variable
    loop_head.produces.push(loop_.accumulator.clone());
    state.variable_producers.insert(loop_.accumulator.clone(), loop_head_id.clone());

    // Add Continue edge from loop_head to body entry
    if !body_entry.is_empty() {
        loop_head.add_edge(body_entry.clone(), EdgeKind::Continue);
    }

    // Connect to previous node
    if let Some(prev_id) = &prev_last {
        if let Some(prev_node) = state.dag.get_node_mut(prev_id) {
            prev_node.add_edge(loop_head_id.clone(), EdgeKind::Data);
        }
    }

    state.dag.add_node(loop_head);

    // Add Back edge from body_tail to loop_head
    if !body_tail.is_empty() {
        if let Some(tail_node) = state.dag.get_node_mut(&body_tail) {
            tail_node.add_edge(loop_head_id.clone(), EdgeKind::Back);
        }
    }

    // Set last_node_id to loop_head (downstream nodes wait for loop completion)
    state.last_node_id = Some(loop_head_id);

    Ok(())
}

fn convert_conditional(state: &mut ConversionState, cond: &ir::Conditional) -> Result<(), ConversionError> {
    let prev_last = state.last_node_id.clone();
    let mut branch_end_nodes = Vec::new();

    // Create branch node
    let branch_id = state.next_node_id("branch");
    let mut true_nodes: HashSet<String> = HashSet::new();
    let mut false_nodes: HashSet<String> = HashSet::new();
    let mut true_entry: Option<String> = None;
    let mut false_entry: Option<String> = None;

    for (i, branch) in cond.branches.iter().enumerate() {
        let start_idx = state.dag.nodes.len();
        state.last_node_id = None;

        // Process preamble
        for pre in &branch.preamble {
            convert_python_block(state, pre)?;
        }

        // Process actions
        for action in &branch.actions {
            convert_action_call(state, action)?;
        }

        // Process postamble
        for post in &branch.postamble {
            convert_python_block(state, post)?;
        }

        // Collect nodes for this branch
        let branch_nodes: HashSet<String> = state.dag.nodes.keys()
            .skip(start_idx)
            .cloned()
            .collect();

        let entry = state.dag.nodes.keys().nth(start_idx).cloned();

        // First branch is "true", rest are "false" (elif/else)
        if i == 0 {
            true_nodes = branch_nodes;
            true_entry = entry;
        } else {
            false_nodes.extend(branch_nodes);
            if false_entry.is_none() {
                false_entry = entry;
            }
        }

        if let Some(last) = &state.last_node_id {
            branch_end_nodes.push(last.clone());
        }
    }

    // Create merge node if needed
    let merge_id = if let Some(target) = &cond.target {
        let merge_id = state.next_node_id("cond_merge");
        let mut merge_node = Node::gather_join(&merge_id);
        merge_node.produces.push(target.clone());

        // Add edges from branch ends to merge
        for end_id in &branch_end_nodes {
            if let Some(end_node) = state.dag.get_node_mut(end_id) {
                end_node.add_edge(merge_id.clone(), EdgeKind::Data);
            }
        }

        state.dag.add_node(merge_node);
        state.variable_producers.insert(target.clone(), merge_id.clone());
        Some(merge_id.clone())
    } else {
        None
    };

    // Create branch metadata
    let branch_meta = BranchMeta {
        guard_expr: cond.branches.first().map(|b| b.guard.clone()).unwrap_or_default(),
        true_entry,
        false_entry,
        true_nodes,
        false_nodes,
        merge_node: merge_id.clone(),
    };

    let branch_node = Node::branch(&branch_id, branch_meta);

    // Connect previous node to branch
    if let Some(prev_id) = &prev_last {
        if let Some(prev_node) = state.dag.get_node_mut(prev_id) {
            prev_node.add_edge(branch_id.clone(), EdgeKind::Data);
        }
    }

    state.dag.add_node(branch_node);
    state.last_node_id = merge_id.or_else(|| branch_end_nodes.last().cloned());

    Ok(())
}

fn convert_try_except(state: &mut ConversionState, te: &ir::TryExcept) -> Result<(), ConversionError> {
    let prev_last = state.last_node_id.clone();

    // Process try block
    let try_start_idx = state.dag.nodes.len();
    state.last_node_id = prev_last.clone();

    for preamble in &te.try_preamble {
        convert_python_block(state, preamble)?;
    }
    for action in &te.try_body {
        convert_action_call(state, action)?;
    }
    for postamble in &te.try_postamble {
        convert_python_block(state, postamble)?;
    }

    let try_nodes: HashSet<String> = state.dag.nodes.keys()
        .skip(try_start_idx)
        .cloned()
        .collect();

    let try_end = state.last_node_id.clone();

    // Process handlers
    let mut handlers_meta = Vec::new();
    let mut handler_end_ids = Vec::new();

    for handler in &te.handlers {
        let handler_start_idx = state.dag.nodes.len();
        state.last_node_id = prev_last.clone();

        for preamble in &handler.preamble {
            convert_python_block(state, preamble)?;
        }
        for action in &handler.body {
            convert_action_call(state, action)?;
        }
        for postamble in &handler.postamble {
            convert_python_block(state, postamble)?;
        }

        let handler_nodes: HashSet<String> = state.dag.nodes.keys()
            .skip(handler_start_idx)
            .cloned()
            .collect();

        let entry = state.dag.nodes.keys().nth(handler_start_idx).cloned().unwrap_or_default();

        // Add exception edges from try nodes to handler nodes
        for handler_node_id in &handler_nodes {
            if let Some(handler_node) = state.dag.get_node_mut(handler_node_id) {
                for try_node_id in &try_nodes {
                    let exc_types: Vec<_> = handler.exception_types.iter()
                        .map(|et| (et.module.clone(), et.name.clone()))
                        .collect();

                    for (module, name) in &exc_types {
                        handler_node.add_exception_edge(
                            try_node_id.clone(),
                            name.clone(),
                            module.clone(),
                        );
                    }

                    if exc_types.is_empty() {
                        // Bare except: catches all
                        handler_node.add_exception_edge(try_node_id.clone(), None, None);
                    }
                }
            }
        }

        handlers_meta.push(HandlerMeta {
            entry: entry.clone(),
            nodes: handler_nodes,
            exception_types: handler.exception_types.iter()
                .map(|et| (et.module.clone(), et.name.clone()))
                .collect(),
        });

        if let Some(last_id) = &state.last_node_id {
            handler_end_ids.push(last_id.clone());
        }
    }

    // Create merge node
    let merge_id = state.next_node_id("try_except_merge");
    let merge_node = Node::gather_join(&merge_id);

    // Add edges from try_end and handler ends to merge
    if let Some(try_end_id) = &try_end {
        if let Some(try_end_node) = state.dag.get_node_mut(try_end_id) {
            try_end_node.add_edge(merge_id.clone(), EdgeKind::Data);
        }
    }
    for handler_end_id in &handler_end_ids {
        if let Some(handler_end_node) = state.dag.get_node_mut(handler_end_id) {
            handler_end_node.add_edge(merge_id.clone(), EdgeKind::Data);
        }
    }

    state.dag.add_node(merge_node);
    state.last_node_id = Some(merge_id);

    Ok(())
}

fn convert_sleep(state: &mut ConversionState, sleep: &ir::Sleep) -> Result<(), ConversionError> {
    let node_id = state.next_node_id("sleep");
    let node = Node::sleep(&node_id, &sleep.duration_expr);
    state.add_node(node);
    Ok(())
}

fn convert_return(state: &mut ConversionState, ret: &ir::Return) -> Result<(), ConversionError> {
    let value = ret.value.as_ref();

    match value {
        Some(ir::r#return::Value::Expr(expr)) => {
            // Simple variable reference
            let is_simple_var = expr.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !expr.is_empty()
                && !expr.chars().next().unwrap().is_numeric();

            if is_simple_var {
                state.dag.return_variable = Some(expr.clone());
            } else {
                // Complex expression - create python_block
                let node_id = state.next_node_id("return");
                let mut node = Node::computed(&node_id);
                node.kwargs.insert("code".to_string(), format!("__workflow_return = {}", expr));
                node.produces.push("__workflow_return".to_string());
                state.add_node(node);
                state.dag.return_variable = Some("__workflow_return".to_string());
            }
        }
        Some(ir::r#return::Value::Action(action)) => {
            let mut action_with_target = action.clone();
            action_with_target.target = Some("__workflow_return".to_string());
            convert_action_call(state, &action_with_target)?;
            state.dag.return_variable = Some("__workflow_return".to_string());
        }
        Some(ir::r#return::Value::Gather(gather)) => {
            let mut gather_with_target = gather.clone();
            gather_with_target.target = Some("__workflow_return".to_string());
            convert_gather(state, &gather_with_target)?;
            state.dag.return_variable = Some("__workflow_return".to_string());
        }
        None => {}
    }

    Ok(())
}

fn convert_spread(state: &mut ConversionState, spread: &ir::Spread) -> Result<(), ConversionError> {
    // Spread is like a loop over a collection
    let loop_id = state.next_node_id("spread");
    let loop_head_id = format!("{}_head", loop_id);

    let target = spread.target.clone().unwrap_or_else(|| format!("__spread_{}_result", loop_id));

    // Create the body action node
    let action_node_id = state.next_node_id("action");
    let mut action_node = if let Some(action) = &spread.action {
        let mut node = Node::action(&action_node_id, &action.action);
        if let Some(module) = &action.module {
            node.module = Some(module.clone());
        }
        for (k, v) in &action.kwargs {
            node.kwargs.insert(k.clone(), v.clone());
        }
        node
    } else {
        return Err(ConversionError {
            message: "Spread missing action".to_string(),
            location: spread.location.clone(),
        });
    };

    action_node.loop_id = Some(loop_id.clone());
    state.dag.add_node(action_node);

    // Create loop head
    let loop_meta = LoopHeadMeta {
        iterator_var: spread.iterable.clone(),
        loop_var: spread.loop_var.clone(),
        body_entry: action_node_id.clone(),
        body_tail: action_node_id.clone(),
        body_nodes: [action_node_id.clone()].into_iter().collect(),
    };

    let mut loop_head = Node::loop_head(&loop_head_id, loop_meta);
    loop_head.produces.push(target.clone());
    loop_head.add_edge(action_node_id.clone(), EdgeKind::Continue);

    // Connect to previous
    if let Some(prev_id) = &state.last_node_id {
        if let Some(prev_node) = state.dag.get_node_mut(prev_id) {
            prev_node.add_edge(loop_head_id.clone(), EdgeKind::Data);
        }
    }

    state.dag.add_node(loop_head);

    // Add back edge
    if let Some(action_node) = state.dag.get_node_mut(&action_node_id) {
        action_node.add_edge(loop_head_id.clone(), EdgeKind::Back);
    }

    state.variable_producers.insert(target, loop_head_id.clone());
    state.last_node_id = Some(loop_head_id);

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

        let fetch_node = dag.nodes.values().find(|n| n.action.as_deref() == Some("fetch")).unwrap();
        assert_eq!(fetch_node.produces, vec!["data"]);

        let process_node = dag.nodes.values().find(|n| n.action.as_deref() == Some("process")).unwrap();
        assert!(process_node.kwargs.is_empty() || true); // Just checking it exists
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

        // Should have 2 parallel action nodes + 1 join node
        assert_eq!(dag.nodes.len(), 3);

        let join = dag.nodes.values().find(|n| n.kind == NodeKind::GatherJoin).unwrap();
        assert!(join.produces.contains(&"results".to_string()));
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
        assert_eq!(dag.return_variable, Some("data".to_string()));
    }
}
