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
        action: "python_block".to_string(),
        ..Default::default()
    };

    // Store the code in kwargs
    node.kwargs.insert("code".to_string(), block.code.clone());

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

    // Find the node ID that produces the iterator variable
    let iterator_source_node = state.variable_producers.get(&loop_.iterator_expr).cloned()
        .unwrap_or_else(|| loop_.iterator_expr.clone());

    // Add preamble op to set the loop variable to current iterator value
    let preamble_ops = vec![
        msg::PreambleOp {
            op: Some(msg::preamble_op::Op::SetIteratorValue(msg::SetIteratorValue {
                var: loop_.loop_var.clone(),
            })),
        },
    ];

    loop_head.loop_head_meta = Some(msg::LoopHeadMeta {
        iterator_source: iterator_source_node,
        loop_var: loop_.loop_var.clone(),
        body_entry: vec![],  // Will be filled after processing body
        body_tail: String::new(),
        exit_target: String::new(),
        accumulators,
        preamble: preamble_ops,
        body_nodes: vec![],
    });

    if let Some(last) = &state.last_node_id {
        loop_head.depends_on.push(last.clone());
    }

    state.nodes.push(loop_head);

    // Track all body nodes (preamble blocks + actions)
    let mut body_node_ids: Vec<String> = Vec::new();

    // Create python_block nodes for each preamble statement
    // These execute after loop_head sets the loop variable, before the action
    for (i, preamble_block) in loop_.preamble.iter().enumerate() {
        let preamble_node_id = format!("{}_preamble_{}", loop_id, i);

        let mut preamble_node = WorkflowDagNode {
            id: preamble_node_id.clone(),
            action: "python_block".to_string(),
            loop_id: Some(loop_id.clone()),
            ..Default::default()
        };

        // Store the Python code to execute
        preamble_node.kwargs.insert("code".to_string(), preamble_block.code.clone());

        // Track inputs and outputs for the python block
        for input in &preamble_block.inputs {
            preamble_node.kwargs.insert(format!("__input_{}", input), input.clone());
        }
        for output in &preamble_block.outputs {
            preamble_node.produces.push(output.clone());
            // Track that this preamble produces the output variable
            state.variable_producers.insert(output.clone(), preamble_node_id.clone());
        }

        // Preamble depends on loop_head (which sets the loop variable)
        if body_node_ids.is_empty() {
            preamble_node.depends_on.push(loop_head_id.clone());
        } else {
            // Chain preambles together
            preamble_node.depends_on.push(body_node_ids.last().unwrap().clone());
        }

        state.nodes.push(preamble_node);
        body_node_ids.push(preamble_node_id);
    }

    // Process loop body actions
    // Track the last node before processing actions so actions chain properly
    let preamble_last = body_node_ids.last().cloned();
    state.last_node_id = preamble_last.clone().or(Some(loop_head_id.clone()));

    // Track action node IDs separately - we need to make yields depend on all actions
    let mut action_node_ids: Vec<String> = Vec::new();

    for action in &loop_.body {
        let node_id = convert_action_call(state, action)?;
        // Mark this node as part of the loop
        if let Some(node) = state.nodes.iter_mut().find(|n| n.id == node_id) {
            node.loop_id = Some(loop_id.clone());
            // If there were preambles, action depends on last preamble, not loop_head
            if let Some(preamble_last) = &preamble_last {
                // Remove loop_head from depends_on if present, add preamble instead
                node.depends_on.retain(|d| d != &loop_head_id);
                if !node.depends_on.contains(preamble_last) {
                    node.depends_on.push(preamble_last.clone());
                }
            }
        }
        action_node_ids.push(node_id.clone());
        body_node_ids.push(node_id);
    }

    // Process yield expressions - these extract values from action results for accumulators
    // Each yield becomes a python_block that evaluates the expression
    for (i, yield_expr) in loop_.yields.iter().enumerate() {
        let yield_node_id = format!("{}_yield_{}", loop_id, i);

        let mut yield_node = WorkflowDagNode {
            id: yield_node_id.clone(),
            action: "python_block".to_string(),
            loop_id: Some(loop_id.clone()),
            ..Default::default()
        };

        // Generate code to evaluate the yield expression and store it
        // The expression might be something like `processed["result"]`
        // We create a special variable __yield_result_{accumulator} to hold the value
        let yield_var = format!("__yield_{}", yield_expr.accumulator);
        let code = format!("{} = {}", yield_var, yield_expr.source_expr);
        yield_node.kwargs.insert("code".to_string(), code);

        // The yield node produces a variable that will be accumulated
        yield_node.produces.push(yield_var.clone());

        // Yield nodes depend on:
        // 1. All action nodes (to get their output variables)
        // 2. Previous yield nodes (to maintain ordering)
        for action_id in &action_node_ids {
            if !yield_node.depends_on.contains(action_id) {
                yield_node.depends_on.push(action_id.clone());
            }
        }
        // Also chain to previous yield if any
        if let Some(prev_yield) = body_node_ids.last() {
            if !action_node_ids.contains(prev_yield) && !yield_node.depends_on.contains(prev_yield) {
                yield_node.depends_on.push(prev_yield.clone());
            }
        }

        state.nodes.push(yield_node);
        body_node_ids.push(yield_node_id.clone());

        // Update the accumulator to use this yield's output
        // We'll update the source_expr to reference our yield variable
    }

    // Determine the first body entry (could be preamble or action)
    let first_body_entry = if !loop_.preamble.is_empty() {
        format!("{}_preamble_0", loop_id)
    } else if let Some(first) = body_node_ids.first() {
        first.clone()
    } else {
        loop_head_id.clone() // No body - shouldn't happen
    };

    // Build a map of accumulator -> yield variable
    let yield_vars: std::collections::HashMap<String, String> = loop_.yields.iter()
        .map(|y| (y.accumulator.clone(), format!("__yield_{}", y.accumulator)))
        .collect();

    // Update loop head meta with body info
    if let Some(head) = state.nodes.iter_mut().find(|n| n.id == loop_head_id) {
        if let Some(meta) = &mut head.loop_head_meta {
            meta.body_entry = vec![first_body_entry.clone()];
            if let Some(last) = body_node_ids.last() {
                meta.body_tail = last.clone();
            }
            meta.body_nodes = body_node_ids.clone();

            // Update accumulator sources based on yields
            for acc in &mut meta.accumulators {
                if let Some(yield_var) = yield_vars.get(&acc.var) {
                    // Accumulator gets its value from the yield variable
                    acc.source_expr = Some(yield_var.clone());
                }
                // Also set source_node to the last body node for context propagation
                if let Some(last_node) = body_node_ids.last() {
                    acc.source_node = Some(last_node.clone());
                }
            }
        }
    }

    // Add Continue edge from loop_head to first body node (preamble or action)
    state.edges.push(msg::DagEdge {
        from_node: loop_head_id.clone(),
        to_node: first_body_entry,
        edge_type: msg::EdgeType::Continue as i32,
        ..Default::default()
    });

    // Add Back edge from last body node to loop_head
    if let Some(last_body) = body_node_ids.last() {
        state.edges.push(msg::DagEdge {
            from_node: last_body.clone(),
            to_node: loop_head_id.clone(),
            edge_type: msg::EdgeType::Back as i32,
            ..Default::default()
        });
    }

    // Track accumulators as produced by the loop
    for acc in &loop_.accumulators {
        state.variable_producers.insert(acc.clone(), loop_head_id.clone());
    }

    // Reset last_node_id to loop_head so subsequent nodes depend on loop completion
    state.last_node_id = Some(loop_head_id);

    Ok(())
}

fn convert_conditional(state: &mut ConversionState, cond: &ir::Conditional) -> Result<(), ConversionError> {
    // For conditionals, we create guarded action nodes for each branch
    let prev_last = state.last_node_id.clone();
    let mut branch_end_nodes = Vec::new();

    for branch in &cond.branches {
        // Reset to branch from the same point
        state.last_node_id = prev_last.clone();

        // Process preamble if any - preambles also need the branch guard
        for pre in &branch.preamble {
            convert_python_block(state, pre)?;
            // Add guard to the preamble node we just created
            if let Some(last_id) = &state.last_node_id {
                if let Some(node) = state.nodes.iter_mut().find(|n| n.id == *last_id) {
                    node.guard = branch.guard.clone();
                }
            }
        }

        // Process actions with guard
        for action in &branch.actions {
            let node_id = convert_action_call(state, action)?;
            // Add guard to the node
            if let Some(node) = state.nodes.iter_mut().find(|n| n.id == node_id) {
                node.guard = branch.guard.clone();
            }
        }

        // Process postamble if any - postambles also need the branch guard
        for post in &branch.postamble {
            convert_python_block(state, post)?;
            // Add guard to the postamble node we just created
            if let Some(last_id) = &state.last_node_id {
                if let Some(node) = state.nodes.iter_mut().find(|n| n.id == *last_id) {
                    node.guard = branch.guard.clone();
                }
            }
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

    // Track the last node of the try block for convergence after handlers
    let try_end = state.last_node_id.clone();

    // Convert handlers
    let mut handler_end_ids = Vec::new();
    for handler in &te.handlers {
        // Reset to branch from try start - handlers don't depend on try body success
        state.last_node_id = try_start.clone();

        for action in &handler.body {
            let handler_node_id = convert_action_call(state, action)?;

            // Add exception edges TO the handler node FROM try nodes
            // The handler depends on exceptions from try nodes
            if let Some(handler_node) = state.nodes.iter_mut().find(|n| n.id == handler_node_id) {
                for try_id in &try_node_ids {
                    for exc_type in &handler.exception_types {
                        let edge = msg::WorkflowExceptionEdge {
                            source_node_id: try_id.clone(),
                            exception_type: exc_type.name.clone().unwrap_or_default(),
                            exception_module: exc_type.module.clone().unwrap_or_default(),
                        };
                        handler_node.exception_edges.push(edge);
                    }
                }
            }
        }

        handler_end_ids.push(state.last_node_id.clone());
    }

    // After try/except, code should continue from whichever path completed
    // For now, set last_node_id to try_end (success path) - handlers set variables
    // that downstream code can use
    state.last_node_id = try_end;

    Ok(())
}

fn convert_sleep(state: &mut ConversionState, sleep: &ir::Sleep) -> Result<(), ConversionError> {
    let node_id = state.next_node_id("sleep");

    let mut node = WorkflowDagNode {
        id: node_id.clone(),
        action: "sleep".to_string(),
        ..Default::default()
    };

    // Store duration expression in kwargs for the scheduler to use
    node.kwargs.insert("__duration".to_string(), sleep.duration_expr.clone());

    state.add_node(node);
    Ok(())
}

fn convert_return(state: &mut ConversionState, ret: &ir::Return) -> Result<(), ConversionError> {
    let value = ret.value.as_ref();

    match value {
        Some(ir::r#return::Value::Expr(expr)) => {
            // Expression return - check if it's a simple variable reference or a complex expression
            // Simple variable: just letters/digits/underscores, no operators or brackets
            let is_simple_var = expr.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !expr.is_empty()
                && !expr.chars().next().unwrap().is_numeric();

            if is_simple_var {
                // Simple variable reference - use it directly as return variable
                state.return_variable = Some(expr.clone());
            } else {
                // Complex expression - create a python_block to evaluate it
                // The block will produce __workflow_return
                let return_node_id = state.next_node_id("return");
                let mut return_node = WorkflowDagNode {
                    id: return_node_id.clone(),
                    action: "python_block".to_string(),
                    produces: vec!["__workflow_return".to_string()],
                    ..Default::default()
                };

                // Generate code: __workflow_return = <expr>
                let code = format!("__workflow_return = {}", expr);
                return_node.kwargs.insert("code".to_string(), code);

                // Depend on whatever ran last
                if let Some(last) = &state.last_node_id {
                    return_node.depends_on.push(last.clone());
                }

                state.nodes.push(return_node);
                state.last_node_id = Some(return_node_id);
                state.return_variable = Some("__workflow_return".to_string());
            }
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
    // Spread creates a loop structure that iterates over a collection
    // [await action(x=v) for v in collection] becomes a loop with accumulator
    let loop_id = state.next_node_id("spread");
    let loop_head_id = format!("{}_head", loop_id);

    // The spread result is stored in the target variable
    let accumulator = spread.target.clone().unwrap_or_else(|| format!("__spread_{}_result", loop_id));

    // Find the node ID that produces the iterable
    let iterator_source_node = state.variable_producers.get(&spread.iterable).cloned()
        .unwrap_or_else(|| spread.iterable.clone());

    // Create the loop head node
    let mut loop_head = WorkflowDagNode {
        id: loop_head_id.clone(),
        action: "__loop_head".to_string(),
        node_type: msg::NodeType::LoopHead as i32,
        loop_id: Some(loop_id.clone()),
        ..Default::default()
    };

    // Add loop metadata
    // Note: iterator_source should be the node ID that produces the iterable,
    // not the variable name itself. The scheduler looks up the node to get its produces[0].
    loop_head.loop_head_meta = Some(msg::LoopHeadMeta {
        iterator_source: iterator_source_node,
        loop_var: spread.loop_var.clone(),
        body_entry: vec![],  // Will be filled after creating body node
        body_tail: String::new(),
        exit_target: String::new(),
        accumulators: vec![msg::AccumulatorSpec {
            var: accumulator.clone(),
            source_node: None,
            source_expr: None,
        }],
        preamble: vec![
            // Set the loop variable to the current iterator value
            msg::PreambleOp {
                op: Some(msg::preamble_op::Op::SetIteratorValue(msg::SetIteratorValue {
                    var: spread.loop_var.clone(),
                })),
            },
        ],
        body_nodes: vec![],
    });

    if let Some(last) = &state.last_node_id {
        loop_head.depends_on.push(last.clone());
    }

    state.nodes.push(loop_head);
    state.last_node_id = Some(loop_head_id.clone());

    // Create the body action node
    if let Some(action) = &spread.action {
        let action_node_id = state.next_node_id("action");

        let mut action_node = WorkflowDagNode {
            id: action_node_id.clone(),
            action: action.action.clone(),
            module: action.module.clone().unwrap_or_default(),
            loop_id: Some(loop_id.clone()),
            ..Default::default()
        };

        // Copy kwargs from inner action
        for (k, v) in &action.kwargs {
            action_node.kwargs.insert(k.clone(), v.clone());
        }

        // Depend on the loop head
        action_node.depends_on.push(loop_head_id.clone());

        state.nodes.push(action_node.clone());

        // Update loop head meta with body info
        if let Some(head) = state.nodes.iter_mut().find(|n| n.id == loop_head_id) {
            if let Some(meta) = &mut head.loop_head_meta {
                meta.body_entry = vec![action_node_id.clone()];
                meta.body_tail = action_node_id.clone();
                meta.body_nodes = vec![action_node_id.clone()];
                // Set accumulator source to the action result
                if let Some(acc) = meta.accumulators.first_mut() {
                    acc.source_node = Some(action_node_id.clone());
                }
            }
        }

        // Add Continue edge: loop_head -> body action
        state.edges.push(msg::DagEdge {
            from_node: loop_head_id.clone(),
            to_node: action_node_id.clone(),
            edge_type: msg::EdgeType::Continue as i32,
            ..Default::default()
        });

        // Add Back edge: body action -> loop_head (for next iteration)
        state.edges.push(msg::DagEdge {
            from_node: action_node_id.clone(),
            to_node: loop_head_id.clone(),
            edge_type: msg::EdgeType::Back as i32,
            ..Default::default()
        });

        state.last_node_id = Some(action_node_id);
    }

    // Track the accumulator as produced by the loop
    state.variable_producers.insert(accumulator.clone(), loop_head_id.clone());

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

    #[test]
    fn test_loop_with_preamble() {
        let workflow = ir::Workflow {
            name: "test".to_string(),
            params: vec![],
            body: vec![
                // First action produces items
                make_statement(ir::statement::Kind::ActionCall(make_action("fetch_items", Some("items")))),
                // Loop with preamble
                make_statement(ir::statement::Kind::Loop(ir::Loop {
                    loop_var: "item".to_string(),
                    iterator_expr: "items".to_string(),
                    accumulators: vec!["outputs".to_string()],
                    preamble: vec![ir::PythonBlock {
                        code: "processed = f'{item}-processed'".to_string(),
                        inputs: vec!["item".to_string()],
                        outputs: vec!["processed".to_string()],
                        imports: vec![],
                        definitions: vec![],
                        location: None,
                    }],
                    yields: vec![],
                    body: vec![ir::ActionCall {
                        action: "process_item".to_string(),
                        module: Some("test".to_string()),
                        kwargs: {
                            let mut m = HashMap::new();
                            m.insert("data".to_string(), "processed".to_string());
                            m
                        },
                        target: Some("result".to_string()),
                        config: None,
                        location: None,
                    }],
                    location: None,
                })),
            ],
            return_type: None,
        };

        let dag = convert_workflow(&workflow, false).unwrap();

        // Should have: fetch_items, loop_head, preamble (python_block), process_item
        assert_eq!(dag.nodes.len(), 4, "Expected 4 nodes: fetch_items, loop_head, preamble, process_item");

        // Check node types
        let loop_head = dag.nodes.iter().find(|n| n.action == "__loop_head").unwrap();
        let preamble = dag.nodes.iter().find(|n| n.action == "python_block").unwrap();
        let action_node = dag.nodes.iter().find(|n| n.action == "process_item").unwrap();

        // Preamble should have the code
        assert_eq!(preamble.kwargs.get("code"), Some(&"processed = f'{item}-processed'".to_string()));
        // Preamble should produce 'processed'
        assert!(preamble.produces.contains(&"processed".to_string()));
        // Preamble should be part of the loop
        assert_eq!(preamble.loop_id, Some("loop_1".to_string()));

        // Action should depend on preamble, not loop_head
        assert!(action_node.depends_on.contains(&preamble.id), "Action should depend on preamble");
        assert!(!action_node.depends_on.contains(&loop_head.id), "Action should not directly depend on loop_head");

        // Check edges: should have Continue edge from loop_head to preamble
        let continue_edge = dag.edges.iter().find(|e| e.from_node == loop_head.id && e.edge_type == msg::EdgeType::Continue as i32);
        assert!(continue_edge.is_some(), "Should have Continue edge from loop_head");
        assert_eq!(continue_edge.unwrap().to_node, preamble.id, "Continue edge should go to preamble");

        // Check edges: should have Back edge from action to loop_head
        let back_edge = dag.edges.iter().find(|e| e.to_node == loop_head.id && e.edge_type == msg::EdgeType::Back as i32);
        assert!(back_edge.is_some(), "Should have Back edge to loop_head");
        assert_eq!(back_edge.unwrap().from_node, action_node.id, "Back edge should come from action");
    }
}
