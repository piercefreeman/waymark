//! DAG validation utilities.

use std::collections::HashSet;

use super::models::{DAG, DagConversionError, EdgeType};

pub fn validate_dag(dag: &DAG) -> Result<(), DagConversionError> {
    validate_edges_reference_existing_nodes(dag)?;
    validate_output_nodes_have_no_outgoing_edges(dag)?;
    validate_loop_incr_edges(dag)?;
    validate_no_duplicate_state_machine_edges(dag)?;
    validate_input_nodes_have_no_incoming_edges(dag)?;
    Ok(())
}

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
