//! Convert IR (Intermediate Representation) to DAG (Directed Acyclic Graph).
//!
//! This module transforms the hierarchical IR workflow structure into a flat
//! DAG representation suitable for execution scheduling.

use crate::dag::{BackoffPolicy, BranchMeta, Dag, EdgeKind, HandlerMeta, LoopHeadMeta, Node};
use crate::ir_parser::proto as ir;
use std::collections::HashSet;

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
    /// The last completed node ID (for sequencing)
    last_node_id: Option<String>,
}

impl ConversionState {
    fn new(concurrent: bool) -> Self {
        Self {
            dag: Dag::new(concurrent),
            node_counter: 0,
            last_node_id: None,
        }
    }

    fn next_node_id(&mut self, prefix: &str) -> String {
        let id = format!("{}_{}", prefix, self.node_counter);
        self.node_counter += 1;
        id
    }

    fn add_node(&mut self, node: Node) {
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

    // Copy structured args as kwargs (converting Expression to code string)
    for kwarg in &action.args {
        if let Some(value) = &kwarg.value {
            let value_str = expression_to_code(value);
            node.kwargs.insert(kwarg.name.clone(), value_str);
        }
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

    // Track body nodes - capture node IDs that exist BEFORE processing body
    let nodes_before: HashSet<String> = state.dag.nodes.keys().cloned().collect();
    let prev_last = state.last_node_id.clone();

    // Temporarily set last_node_id to None so body nodes don't depend on previous
    state.last_node_id = None;

    // Track first and last node IDs during body processing
    let mut body_entry: Option<String> = None;

    for stmt in &loop_.body {
        convert_statement(state, stmt)?;
        // Capture first node created for body_entry
        if body_entry.is_none() {
            // Find the first node that was added (wasn't in nodes_before)
            for key in state.dag.nodes.keys() {
                if !nodes_before.contains(key) {
                    body_entry = Some(key.clone());
                    break;
                }
            }
        }
    }

    let body_tail = state.last_node_id.clone();

    // Get all body nodes - those that weren't in nodes_before
    let body_nodes: HashSet<String> = state.dag.nodes.keys()
        .filter(|k| !nodes_before.contains(*k))
        .cloned()
        .collect();

    // Mark all body nodes as belonging to this loop
    for node_id in &body_nodes {
        if let Some(node) = state.dag.get_node_mut(node_id) {
            node.loop_id = Some(loop_id.clone());
        }
    }

    let body_entry_id = body_entry.unwrap_or_default();
    let body_tail_id = body_tail.unwrap_or_default();

    // Convert iterator expression to string
    let iterator_var = loop_.iterator.as_ref()
        .map(expression_to_code)
        .unwrap_or_default();

    // Create loop head node
    let loop_meta = LoopHeadMeta {
        iterator_var,
        loop_var: loop_.loop_var.clone(),
        body_entry: body_entry_id.clone(),
        body_tail: body_tail_id.clone(),
        body_nodes: body_nodes.clone(),
    };

    let mut loop_head = Node::loop_head(&loop_head_id, loop_meta);

    // The loop produces the accumulator variable
    loop_head.produces.push(loop_.accumulator.clone());

    // Add Continue edge from loop_head to body entry
    if !body_entry_id.is_empty() {
        loop_head.add_edge(body_entry_id.clone(), EdgeKind::Continue);
    }

    // Connect to previous node
    if let Some(prev_id) = &prev_last {
        if let Some(prev_node) = state.dag.get_node_mut(prev_id) {
            prev_node.add_edge(loop_head_id.clone(), EdgeKind::Data);
        }
    }

    state.dag.add_node(loop_head);

    // Add Back edge from body_tail to loop_head
    if !body_tail_id.is_empty() {
        if let Some(tail_node) = state.dag.get_node_mut(&body_tail_id) {
            tail_node.add_edge(loop_head_id.clone(), EdgeKind::Back);
        }
    }

    // Set last_node_id to loop_head (downstream nodes wait for loop completion)
    state.last_node_id = Some(loop_head_id);

    Ok(())
}

fn convert_conditional(state: &mut ConversionState, cond: &ir::Conditional) -> Result<(), ConversionError> {
    let prev_last = state.last_node_id.clone();

    // For if/elif/else, we create a chain of branch nodes.
    // Each branch node evaluates one guard:
    //   - If true: execute that branch's body, then go to merge
    //   - If false: proceed to next branch node (or else branch if last)
    //
    // Structure for 3 branches (if/elif/else):
    //   Branch_0 --guard_0_true--> Body_0 --> Merge
    //            --guard_0_false--> Branch_1 --guard_1_true--> Body_1 --> Merge
    //                                        --guard_1_false--> Body_2 --> Merge

    struct BranchInfo {
        guard: Option<ir::Expression>,
        entry: Option<String>,
        end_node: Option<String>,
        nodes: HashSet<String>,
    }

    let mut branch_infos: Vec<BranchInfo> = Vec::new();

    // First pass: create all body nodes for each branch
    for branch in &cond.branches {
        let nodes_before: HashSet<String> = state.dag.nodes.keys().cloned().collect();
        state.last_node_id = None;

        // Track the first node added in this branch
        let mut first_node_in_branch: Option<String> = None;

        // Process preamble
        for pre in &branch.preamble {
            convert_python_block(state, pre)?;
            if first_node_in_branch.is_none() {
                first_node_in_branch = state.last_node_id.clone();
            }
        }

        // Process actions
        for action in &branch.actions {
            convert_action_call(state, action)?;
            if first_node_in_branch.is_none() {
                first_node_in_branch = state.last_node_id.clone();
            }
        }

        // Process postamble
        for post in &branch.postamble {
            convert_python_block(state, post)?;
            if first_node_in_branch.is_none() {
                first_node_in_branch = state.last_node_id.clone();
            }
        }

        // Collect nodes for this branch
        let branch_nodes: HashSet<String> = state.dag.nodes.keys()
            .filter(|k| !nodes_before.contains(*k))
            .cloned()
            .collect();

        branch_infos.push(BranchInfo {
            guard: branch.guard.clone(),
            entry: first_node_in_branch,
            end_node: state.last_node_id.clone(),
            nodes: branch_nodes,
        });
    }

    // Create merge node if there's a target, or just to join branches
    let merge_id = state.next_node_id("cond_merge");
    let mut merge_node = Node::gather_join(&merge_id);
    if let Some(target) = &cond.target {
        merge_node.produces.push(target.clone());
    }

    // Add edges from branch ends to merge
    for info in &branch_infos {
        if let Some(end_id) = &info.end_node {
            if let Some(end_node) = state.dag.get_node_mut(end_id) {
                end_node.add_edge(merge_id.clone(), EdgeKind::Data);
            }
        }
    }

    state.dag.add_node(merge_node);

    // Create branch nodes in reverse order (last to first)
    // so that each branch's false_entry points to the next branch node
    let mut next_branch_id: Option<String> = None;
    let mut branch_node_ids: Vec<String> = Vec::new();
    // Track branch node IDs that should be skipped when earlier branches are taken
    let mut later_branch_node_ids: HashSet<String> = HashSet::new();

    for i in (0..branch_infos.len()).rev() {
        let info = &branch_infos[i];
        let branch_id = state.next_node_id("branch");
        branch_node_ids.push(branch_id.clone());

        // For the last branch (else), there's no guard - it's always taken
        let is_else_branch = info.guard.is_none() && i == branch_infos.len() - 1;

        // Collect all nodes that belong to branches AFTER this one (to mark as skipped when true)
        // This includes both body nodes AND the branch nodes themselves
        let mut later_branch_nodes: HashSet<String> = HashSet::new();
        for j in (i + 1)..branch_infos.len() {
            later_branch_nodes.extend(branch_infos[j].nodes.clone());
        }
        // Also include the later branch node IDs (they need to be skipped too)
        later_branch_nodes.extend(later_branch_node_ids.clone());

        let branch_meta = BranchMeta {
            guard: info.guard.clone(),
            true_entry: info.entry.clone(),
            false_entry: if is_else_branch { None } else { next_branch_id.clone().or(info.entry.clone()) },
            true_nodes: info.nodes.clone(),
            false_nodes: later_branch_nodes,
            merge_node: Some(merge_id.clone()),
        };

        let mut branch_node = Node::branch(&branch_id, branch_meta);

        // Add edge from this branch to its body entry (for dependency tracking)
        if let Some(entry) = &info.entry {
            branch_node.add_edge(entry.clone(), EdgeKind::Data);
        }

        // Add edge from this branch to the next branch (for chaining elif/else)
        if let Some(next_id) = &next_branch_id {
            branch_node.add_edge(next_id.clone(), EdgeKind::Data);
        }

        state.dag.add_node(branch_node);
        next_branch_id = Some(branch_id.clone());
        // Add this branch's ID to the set that will be skipped by earlier branches
        later_branch_node_ids.insert(branch_id);
    }

    // The first branch node is the entry to the conditional chain
    let first_branch_id = branch_node_ids.last().cloned();

    // Connect previous node to first branch
    if let Some(prev_id) = &prev_last {
        if let Some(first_branch) = &first_branch_id {
            if let Some(prev_node) = state.dag.get_node_mut(prev_id) {
                prev_node.add_edge(first_branch.clone(), EdgeKind::Data);
            }
        }
    }

    state.last_node_id = Some(merge_id);

    Ok(())
}

fn convert_try_except(state: &mut ConversionState, te: &ir::TryExcept) -> Result<(), ConversionError> {
    let prev_last = state.last_node_id.clone();

    // Process try block - capture nodes before
    let nodes_before_try: HashSet<String> = state.dag.nodes.keys().cloned().collect();
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
        .filter(|k| !nodes_before_try.contains(*k))
        .cloned()
        .collect();

    let try_end = state.last_node_id.clone();

    // Process handlers
    let mut _handlers_meta = Vec::new();
    let mut handler_end_ids = Vec::new();

    for handler in &te.handlers {
        // Capture nodes before this handler
        let nodes_before_handler: HashSet<String> = state.dag.nodes.keys().cloned().collect();
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
            .filter(|k| !nodes_before_handler.contains(*k))
            .cloned()
            .collect();

        let entry = state.dag.nodes.keys()
            .find(|k| !nodes_before_handler.contains(*k))
            .cloned()
            .unwrap_or_default();

        // Add exception edges FROM try nodes TO handler entry
        // This allows the scheduler to find the handler when a try node fails
        let exc_types: Vec<_> = handler.exception_types.iter()
            .map(|et| (et.module.clone(), et.name.clone()))
            .collect();

        for try_node_id in &try_nodes {
            if let Some(try_node) = state.dag.get_node_mut(try_node_id) {
                if exc_types.is_empty() {
                    // Bare except: catches all
                    try_node.add_exception_edge(entry.clone(), None, None);
                } else {
                    for (module, name) in &exc_types {
                        try_node.add_exception_edge(
                            entry.clone(),
                            name.clone(),
                            module.clone(),
                        );
                    }
                }
            }
        }

        // Mark the handler entry as an exception handler entry (won't be made ready at init)
        if !entry.is_empty() {
            if let Some(entry_node) = state.dag.get_node_mut(&entry) {
                entry_node.is_exception_handler_entry = true;
            }
        }

        _handlers_meta.push(HandlerMeta {
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
    let duration_str = sleep.duration.as_ref()
        .map(expression_to_code)
        .unwrap_or_default();
    let node = Node::sleep(&node_id, &duration_str);
    state.add_node(node);
    Ok(())
}

fn convert_return(state: &mut ConversionState, ret: &ir::Return) -> Result<(), ConversionError> {
    let value = ret.value.as_ref();

    match value {
        Some(ir::r#return::Value::Expression(expr)) => {
            // Convert structured expression to code string for evaluation
            let expr_str = expression_to_code(expr);
            let is_simple_var = expr_str.chars().all(|c| c.is_alphanumeric() || c == '_')
                && !expr_str.is_empty()
                && !expr_str.chars().next().unwrap().is_numeric();

            if is_simple_var {
                state.dag.return_variable = Some(expr_str);
            } else {
                let node_id = state.next_node_id("return");
                let mut node = Node::computed(&node_id);
                node.kwargs.insert("code".to_string(), format!("__workflow_return = {}", expr_str));
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
        // Copy structured args as kwargs
        for kwarg in &action.args {
            if let Some(value) = &kwarg.value {
                let value_str = expression_to_code(value);
                node.kwargs.insert(kwarg.name.clone(), value_str);
            }
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

    // Convert iterable expression to string
    let iterator_var = spread.iterable.as_ref()
        .map(expression_to_code)
        .unwrap_or_default();

    // Create loop head
    let loop_meta = LoopHeadMeta {
        iterator_var,
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

    state.last_node_id = Some(loop_head_id);

    Ok(())
}

/// Convert a structured Expression to Python code string
fn expression_to_code(expr: &ir::Expression) -> String {
    let kind = match &expr.kind {
        Some(k) => k,
        None => return String::new(),
    };

    match kind {
        ir::expression::Kind::Literal(lit) => literal_to_code(lit),
        ir::expression::Kind::Variable(name) => name.clone(),
        ir::expression::Kind::Subscript(sub) => {
            let base = sub.base.as_ref().map(|e| expression_to_code(e)).unwrap_or_default();
            let key = sub.key.as_ref().map(|e| expression_to_code(e)).unwrap_or_default();
            format!("{}[{}]", base, key)
        }
        ir::expression::Kind::Array(arr) => {
            let elements: Vec<String> = arr.elements.iter().map(expression_to_code).collect();
            format!("[{}]", elements.join(", "))
        }
        ir::expression::Kind::Dict(dict) => {
            let entries: Vec<String> = dict.entries.iter()
                .map(|e| {
                    let val = e.value.as_ref().map(expression_to_code).unwrap_or_default();
                    format!("\"{}\": {}", e.key, val)
                })
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        ir::expression::Kind::BinaryOp(binop) => {
            let left = binop.left.as_ref().map(|e| expression_to_code(e)).unwrap_or_default();
            let right = binop.right.as_ref().map(|e| expression_to_code(e)).unwrap_or_default();
            let op = match ir::binary_op::Op::try_from(binop.op) {
                Ok(ir::binary_op::Op::Add) => "+",
                Ok(ir::binary_op::Op::Sub) => "-",
                Ok(ir::binary_op::Op::Mul) => "*",
                Ok(ir::binary_op::Op::Div) => "/",
                Ok(ir::binary_op::Op::Mod) => "%",
                Ok(ir::binary_op::Op::Eq) => "==",
                Ok(ir::binary_op::Op::Ne) => "!=",
                Ok(ir::binary_op::Op::Lt) => "<",
                Ok(ir::binary_op::Op::Le) => "<=",
                Ok(ir::binary_op::Op::Gt) => ">",
                Ok(ir::binary_op::Op::Ge) => ">=",
                Ok(ir::binary_op::Op::And) => "and",
                Ok(ir::binary_op::Op::Or) => "or",
                Ok(ir::binary_op::Op::In) => "in",
                Ok(ir::binary_op::Op::NotIn) => "not in",
                _ => "??",
            };
            format!("({} {} {})", left, op, right)
        }
        ir::expression::Kind::UnaryOp(unop) => {
            let operand = unop.operand.as_ref().map(|e| expression_to_code(e)).unwrap_or_default();
            match ir::unary_op::Op::try_from(unop.op) {
                Ok(ir::unary_op::Op::Not) => format!("(not {})", operand),
                Ok(ir::unary_op::Op::Neg) => format!("(-{})", operand),
                _ => operand,
            }
        }
        ir::expression::Kind::Call(call) => {
            let args: Vec<String> = call.args.iter().map(expression_to_code).collect();
            format!("{}({})", call.function, args.join(", "))
        }
        ir::expression::Kind::Attribute(attr) => {
            let base = attr.base.as_ref().map(|e| expression_to_code(e)).unwrap_or_default();
            format!("{}.{}", base, attr.attribute)
        }
    }
}

fn literal_to_code(lit: &ir::Literal) -> String {
    match &lit.value {
        Some(ir::literal::Value::NullValue(_)) => "None".to_string(),
        Some(ir::literal::Value::BoolValue(b)) => if *b { "True" } else { "False" }.to_string(),
        Some(ir::literal::Value::IntValue(i)) => i.to_string(),
        Some(ir::literal::Value::FloatValue(f)) => f.to_string(),
        Some(ir::literal::Value::StringValue(s)) => format!("\"{}\"", s),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::NodeKind;

    /// Helper function to create a variable expression for tests
    fn make_var_expr(name: &str) -> ir::Expression {
        ir::Expression {
            kind: Some(ir::expression::Kind::Variable(name.to_string())),
        }
    }

    fn make_action(name: &str, target: Option<&str>) -> ir::ActionCall {
        ir::ActionCall {
            action: name.to_string(),
            module: Some("test".to_string()),
            args: vec![],
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

        let _process_node = dag.nodes.values().find(|n| n.action.as_deref() == Some("process")).unwrap();
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
                    value: Some(ir::r#return::Value::Expression(make_var_expr("data"))),
                    location: None,
                })),
            ],
            return_type: None,
        };

        let dag = convert_workflow(&workflow, false).unwrap();
        assert_eq!(dag.return_variable, Some("data".to_string()));
    }
}
