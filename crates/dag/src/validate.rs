//! DAG validation utilities.

use std::collections::HashSet;

use waymark_proto::ast as ir;

use super::models::{DAG, DAGNode, DagConversionError, EXCEPTION_SCOPE_VAR, EdgeType};

pub fn validate_dag(dag: &DAG) -> Result<(), DagConversionError> {
    validate_edges_reference_existing_nodes(dag)?;
    validate_output_nodes_have_no_outgoing_edges(dag)?;
    validate_loop_incr_edges(dag)?;
    validate_no_duplicate_state_machine_edges(dag)?;
    validate_input_nodes_have_no_incoming_edges(dag)?;
    validate_variable_references_have_data_flow(dag)?;
    Ok(())
}

/// Fail if any edge references a missing source or target node.
pub fn validate_edges_reference_existing_nodes(dag: &DAG) -> Result<(), DagConversionError> {
    for edge in &dag.edges {
        if !dag.nodes.contains_key(&edge.source) {
            return Err(DagConversionError(format!(
                "DAG edge references non-existent source node '{}' -> '{}'",
                edge.source, edge.target
            )));
        }
        if !dag.nodes.contains_key(&edge.target) {
            return Err(DagConversionError(format!(
                "DAG edge references non-existent target node '{}' (from '{}', edge_type={:?}, exception_types={:?})",
                edge.target, edge.source, edge.edge_type, edge.exception_types
            )));
        }
    }
    Ok(())
}

/// Fail if a main output node has non-exception control flow continuing past it.
pub fn validate_output_nodes_have_no_outgoing_edges(dag: &DAG) -> Result<(), DagConversionError> {
    for (node_id, node) in &dag.nodes {
        if node.node_type() == "output" && !node_id.contains(':') {
            for edge in &dag.edges {
                if edge.source == *node_id
                    && edge.edge_type == EdgeType::StateMachine
                    && edge.exception_types.is_none()
                {
                    return Err(DagConversionError(format!(
                        "Main output node '{}' has non-exception outgoing state machine edge to '{}'",
                        node_id, edge.target
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Fail if a loop increment node has unexpected control-flow edges.
///
/// Loop increment nodes should only connect to loop condition nodes via loop-back edges,
/// or have exception edges. Any other state-machine edge indicates a conversion bug.
pub fn validate_loop_incr_edges(dag: &DAG) -> Result<(), DagConversionError> {
    for node_id in dag.nodes.keys() {
        if !node_id.contains("loop_incr") {
            continue;
        }
        for edge in &dag.edges {
            if edge.source != *node_id || edge.edge_type != EdgeType::StateMachine {
                continue;
            }
            if edge.exception_types.is_some() {
                continue;
            }
            if edge.is_loop_back && edge.target.contains("loop_cond") {
                continue;
            }
            return Err(DagConversionError(format!(
                "Loop increment node '{}' has unexpected state machine edge to '{}'. Loop_incr should only have loop_back edges to loop_cond or exception edges. This suggests incorrect 'last_real_node' tracking during function expansion.",
                node_id, edge.target
            )));
        }
    }
    Ok(())
}

/// Fail if duplicate state-machine edges exist between the same nodes.
pub fn validate_no_duplicate_state_machine_edges(dag: &DAG) -> Result<(), DagConversionError> {
    let mut seen: HashSet<String> = HashSet::new();
    for edge in &dag.edges {
        if edge.edge_type != EdgeType::StateMachine {
            continue;
        }
        if edge.exception_types.is_none() {
            let key = format!(
                "{}->{}:loop_back={},is_else={},guard={}",
                edge.source,
                edge.target,
                edge.is_loop_back,
                edge.is_else,
                edge.guard_string.as_deref().unwrap_or("")
            );
            if seen.contains(&key) {
                return Err(DagConversionError(format!(
                    "Duplicate state machine edge: {} -> {} (loop_back={}, is_else={})",
                    edge.source, edge.target, edge.is_loop_back, edge.is_else
                )));
            }
            seen.insert(key);
        }
    }
    Ok(())
}

/// Fail if input nodes have incoming state-machine edges.
pub fn validate_input_nodes_have_no_incoming_edges(dag: &DAG) -> Result<(), DagConversionError> {
    for (node_id, node) in &dag.nodes {
        if node.is_input() {
            for edge in &dag.edges {
                if edge.target == *node_id && edge.edge_type == EdgeType::StateMachine {
                    return Err(DagConversionError(format!(
                        "Input node '{}' has incoming state machine edge from '{}'",
                        node_id, edge.source
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Fail if a node references a variable that has no incoming data-flow edge.
pub fn validate_variable_references_have_data_flow(dag: &DAG) -> Result<(), DagConversionError> {
    let incoming_data_flow: HashSet<(String, String)> = dag
        .edges
        .iter()
        .filter(|edge| edge.edge_type == EdgeType::DataFlow)
        .filter_map(|edge| {
            edge.variable
                .as_ref()
                .map(|var| (edge.target.clone(), var.clone()))
        })
        .collect();

    let mut missing_references: Vec<String> = Vec::new();
    for (node_id, node) in &dag.nodes {
        let mut referenced_vars = node_referenced_variables(node);
        referenced_vars.extend(node_guard_variables(dag, node_id));
        for bound_var in node_locally_bound_variables(node) {
            referenced_vars.remove(&bound_var);
        }
        for var_name in referenced_vars {
            if var_name == EXCEPTION_SCOPE_VAR {
                continue;
            }
            if !incoming_data_flow.contains(&(node_id.clone(), var_name.clone())) {
                missing_references.push(format!(
                    "node '{}' ({}) references variable '{}' without incoming data-flow",
                    node_id,
                    node.node_type(),
                    var_name
                ));
            }
        }
    }

    if missing_references.is_empty() {
        return Ok(());
    }
    missing_references.sort();
    missing_references.dedup();
    Err(DagConversionError(format!(
        "Undefined variable references detected: {}",
        missing_references.join("; ")
    )))
}

fn node_guard_variables(dag: &DAG, node_id: &str) -> HashSet<String> {
    let mut vars = HashSet::new();
    for edge in &dag.edges {
        if edge.edge_type != EdgeType::StateMachine || edge.source != node_id {
            continue;
        }
        if let Some(guard_expr) = edge.guard_expr.as_ref() {
            collect_expr_variables(guard_expr, &mut vars);
        }
    }
    vars
}

fn node_referenced_variables(node: &DAGNode) -> HashSet<String> {
    let mut vars = HashSet::new();
    match node {
        DAGNode::ActionCall(action) => {
            for value in action.kwargs.values() {
                if let Some(var_name) = value.strip_prefix('$') {
                    vars.insert(var_name.to_string());
                }
            }
            for expr in action.kwarg_exprs.values() {
                collect_expr_variables(expr, &mut vars);
            }
            if let Some(expr) = action.spread_collection_expr.as_ref() {
                collect_expr_variables(expr, &mut vars);
            }
        }
        DAGNode::FnCall(function) => {
            for value in function.kwargs.values() {
                if let Some(var_name) = value.strip_prefix('$') {
                    vars.insert(var_name.to_string());
                }
            }
            for expr in function.kwarg_exprs.values() {
                collect_expr_variables(expr, &mut vars);
            }
            if let Some(expr) = function.assign_expr.as_ref() {
                collect_expr_variables(expr, &mut vars);
            }
        }
        DAGNode::Assignment(assignment) => {
            if let Some(expr) = assignment.assign_expr.as_ref() {
                collect_expr_variables(expr, &mut vars);
            }
        }
        DAGNode::Return(ret) => {
            if let Some(expr) = ret.assign_expr.as_ref() {
                collect_expr_variables(expr, &mut vars);
            }
        }
        DAGNode::Sleep(sleep) => {
            if let Some(expr) = sleep.duration_expr.as_ref() {
                collect_expr_variables(expr, &mut vars);
            }
        }
        DAGNode::Input(_)
        | DAGNode::Output(_)
        | DAGNode::Parallel(_)
        | DAGNode::Aggregator(_)
        | DAGNode::Branch(_)
        | DAGNode::Join(_)
        | DAGNode::Break(_)
        | DAGNode::Continue(_)
        | DAGNode::Expression(_) => {}
    }
    vars
}

fn node_locally_bound_variables(node: &DAGNode) -> HashSet<String> {
    let mut vars = HashSet::new();
    if let DAGNode::ActionCall(action) = node
        && let Some(loop_var) = action.spread_loop_var.as_ref()
    {
        vars.insert(loop_var.clone());
    }
    vars
}

fn collect_expr_variables(expr: &ir::Expr, vars: &mut HashSet<String>) {
    match expr.kind.as_ref() {
        Some(ir::expr::Kind::Variable(var)) => {
            vars.insert(var.name.clone());
        }
        Some(ir::expr::Kind::BinaryOp(op)) => {
            if let Some(left) = op.left.as_ref() {
                collect_expr_variables(left, vars);
            }
            if let Some(right) = op.right.as_ref() {
                collect_expr_variables(right, vars);
            }
        }
        Some(ir::expr::Kind::UnaryOp(op)) => {
            if let Some(operand) = op.operand.as_ref() {
                collect_expr_variables(operand, vars);
            }
        }
        Some(ir::expr::Kind::List(list_expr)) => {
            for element in &list_expr.elements {
                collect_expr_variables(element, vars);
            }
        }
        Some(ir::expr::Kind::Dict(dict_expr)) => {
            for entry in &dict_expr.entries {
                if let Some(key) = entry.key.as_ref() {
                    collect_expr_variables(key, vars);
                }
                if let Some(value) = entry.value.as_ref() {
                    collect_expr_variables(value, vars);
                }
            }
        }
        Some(ir::expr::Kind::Index(index)) => {
            if let Some(object) = index.object.as_ref() {
                collect_expr_variables(object, vars);
            }
            if let Some(index_expr) = index.index.as_ref() {
                collect_expr_variables(index_expr, vars);
            }
        }
        Some(ir::expr::Kind::Dot(dot)) => {
            if let Some(object) = dot.object.as_ref() {
                collect_expr_variables(object, vars);
            }
        }
        Some(ir::expr::Kind::FunctionCall(call)) => {
            for arg in &call.args {
                collect_expr_variables(arg, vars);
            }
            for kw in &call.kwargs {
                if let Some(value) = kw.value.as_ref() {
                    collect_expr_variables(value, vars);
                }
            }
        }
        Some(ir::expr::Kind::ActionCall(call)) => {
            for kw in &call.kwargs {
                if let Some(value) = kw.value.as_ref() {
                    collect_expr_variables(value, vars);
                }
            }
        }
        Some(ir::expr::Kind::ParallelExpr(parallel)) => {
            for call in &parallel.calls {
                match call.kind.as_ref() {
                    Some(ir::call::Kind::Action(action)) => {
                        for kw in &action.kwargs {
                            if let Some(value) = kw.value.as_ref() {
                                collect_expr_variables(value, vars);
                            }
                        }
                    }
                    Some(ir::call::Kind::Function(function)) => {
                        for arg in &function.args {
                            collect_expr_variables(arg, vars);
                        }
                        for kw in &function.kwargs {
                            if let Some(value) = kw.value.as_ref() {
                                collect_expr_variables(value, vars);
                            }
                        }
                    }
                    None => {}
                }
            }
        }
        Some(ir::expr::Kind::SpreadExpr(spread)) => {
            if let Some(collection) = spread.collection.as_ref() {
                collect_expr_variables(collection, vars);
            }
            if let Some(action) = spread.action.as_ref() {
                for kw in &action.kwargs {
                    if let Some(value) = kw.value.as_ref() {
                        collect_expr_variables(value, vars);
                    }
                }
            }
        }
        Some(ir::expr::Kind::Literal(_)) | None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::validate_dag;
    use crate::convert_to_dag;
    use waymark::waymark_core::ir_parser::parse_program;

    #[test]
    fn validate_dag_rejects_unresolved_variable_reference() {
        let source = r#"
fn main(input: [input_text], output: [result]):
    result = @double(value=global_fallback)
    return result
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let err = convert_to_dag(&program).expect_err("expected unresolved variable error");
        assert!(err.0.contains("global_fallback"));
    }

    #[test]
    fn validate_dag_allows_resolved_variable_reference() {
        let source = r#"
fn main(input: [input_text], output: [result]):
    result = @double(value=input_text)
    return result
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let dag = convert_to_dag(&program).expect("convert dag");
        validate_dag(&dag).expect("validate dag");
    }

    #[test]
    fn validate_dag_allows_spread_loop_variable_reference() {
        let source = r#"
fn main(input: [values], output: [doubles]):
    doubles = spread values:item -> @double(value=item)
    return doubles
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let dag = convert_to_dag(&program).expect("convert dag");
        validate_dag(&dag).expect("validate dag");
    }
}
