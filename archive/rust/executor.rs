//! Shared inline execution logic for runners.

use tracing::{debug, warn};

use crate::{
    DAG, DAGHelper, DAGNode, ExecutionState, ExpressionEvaluator, WorkflowValue,
    messages::{decode_message, encode_message, proto},
    value::{workflow_value_from_proto_bytes, workflow_value_to_proto_bytes},
};

/// Execute an inline (non-worker) node and return the encoded result payload.
pub fn execute_inline_node(
    state: &mut ExecutionState,
    dag: &DAG,
    node_id: &str,
    dag_node: &DAGNode,
) -> Result<Vec<u8>, String> {
    let scope = state.build_scope_for_node(node_id);

    match dag_node.node_type.as_str() {
        "assignment" | "fn_call" => {
            if let Some(assign_expr) = &dag_node.assign_expr {
                match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                    Ok(value) => {
                        let result_bytes = workflow_value_to_proto_bytes(&value);

                        if let Some(targets) = &dag_node.targets {
                            if targets.len() > 1 {
                                match &value {
                                    WorkflowValue::Tuple(items) | WorkflowValue::List(items) => {
                                        for (target, item) in targets.iter().zip(items.iter()) {
                                            state.store_variable_for_node(
                                                node_id,
                                                &dag_node.node_type,
                                                target,
                                                item,
                                            );
                                            debug!(
                                                node_id = %node_id,
                                                target = %target,
                                                "Stored assignment result"
                                            );
                                        }
                                    }
                                    _ => {
                                        warn!(
                                            node_id = %node_id,
                                            targets = ?targets,
                                            value = ?value,
                                            "Assignment value is not iterable for tuple unpacking"
                                        );
                                        for target in targets {
                                            state.store_variable_for_node(
                                                node_id,
                                                &dag_node.node_type,
                                                target,
                                                &value,
                                            );
                                        }
                                    }
                                }
                            } else {
                                for target in targets {
                                    state.store_variable_for_node(
                                        node_id,
                                        &dag_node.node_type,
                                        target,
                                        &value,
                                    );
                                    debug!(
                                        node_id = %node_id,
                                        target = %target,
                                        "Stored assignment result"
                                    );
                                }
                            }
                        } else if let Some(target) = &dag_node.target {
                            state.store_variable_for_node(
                                node_id,
                                &dag_node.node_type,
                                target,
                                &value,
                            );
                            debug!(
                                node_id = %node_id,
                                target = %target,
                                "Stored assignment result"
                            );
                        }

                        Ok(result_bytes)
                    }
                    Err(e) => {
                        warn!(
                            node_id = %node_id,
                            error = %e,
                            "Failed to evaluate assignment expression"
                        );
                        Err(format!("Assignment evaluation error: {}", e))
                    }
                }
            } else {
                Ok(vec![])
            }
        }
        "branch" | "if" | "elif" => {
            debug!(node_id = %node_id, "Branch node completed");
            Ok(vec![])
        }
        "join" | "else" => {
            debug!(node_id = %node_id, "Join node completed");
            Ok(vec![])
        }
        "aggregator" => {
            let helper = DAGHelper::new(dag);
            let exec_node = state.graph.nodes.get(node_id);
            let source_is_spread = dag_node
                .aggregates_from
                .as_ref()
                .and_then(|id| dag.nodes.get(id))
                .map(|n| n.is_spread)
                .unwrap_or(false);

            let source_ids: Vec<String> = if source_is_spread {
                if let Some(exec_node) = exec_node
                    && !exec_node.waiting_for.is_empty()
                {
                    exec_node.waiting_for.clone()
                } else if let Some(spread_source) = dag_node.aggregates_from.as_deref() {
                    let mut indexed: Vec<(i32, String)> = state
                        .graph
                        .nodes
                        .iter()
                        .filter_map(|(id, node)| {
                            if node.template_id == spread_source {
                                node.spread_index.map(|idx| (idx, id.clone()))
                            } else {
                                None
                            }
                        })
                        .collect();
                    indexed.sort_by_key(|(idx, _)| *idx);
                    indexed.into_iter().map(|(_, id)| id).collect()
                } else {
                    Vec::new()
                }
            } else if let Some(exec_node) = exec_node
                && !exec_node.waiting_for.is_empty()
            {
                exec_node.waiting_for.clone()
            } else {
                helper
                    .get_incoming_edges(&dag_node.id)
                    .iter()
                    .filter(|edge| edge.edge_type == crate::dag::EdgeType::StateMachine)
                    .filter(|edge| edge.exception_types.is_none())
                    .map(|edge| edge.source.clone())
                    .collect()
            };

            let mut values = Vec::new();
            for source_id in source_ids {
                if let Some(source_node) = state.graph.nodes.get(&source_id) {
                    if let Some(result_bytes) = &source_node.result {
                        let value =
                            extract_result_value(result_bytes).unwrap_or(WorkflowValue::Null);
                        values.push(value);
                    } else {
                        values.push(WorkflowValue::Null);
                    }
                }
            }

            let should_store_list = dag_node
                .targets
                .as_ref()
                .map(|targets| targets.len() == 1)
                .unwrap_or(false);

            if should_store_list {
                let list_value = WorkflowValue::List(values);
                let args = proto::WorkflowArguments {
                    arguments: vec![proto::WorkflowArgument {
                        key: "result".to_string(),
                        value: Some(list_value.to_proto()),
                    }],
                };
                Ok(encode_message(&args))
            } else {
                Ok(encode_message(&proto::WorkflowArguments {
                    arguments: vec![],
                }))
            }
        }
        "input" | "output" => {
            debug!(node_id = %node_id, node_type = %dag_node.node_type, "Boundary node completed");
            Ok(vec![])
        }
        "return" => {
            if let Some(assign_expr) = &dag_node.assign_expr {
                match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                    Ok(value) => {
                        let result_bytes = workflow_value_to_proto_bytes(&value);

                        // Handle tuple unpacking for return nodes (from expanded fn_call)
                        if let Some(targets) = &dag_node.targets {
                            if targets.len() > 1 {
                                match &value {
                                    WorkflowValue::Tuple(items) | WorkflowValue::List(items) => {
                                        for (target, item) in targets.iter().zip(items.iter()) {
                                            state.store_variable_for_node(
                                                node_id,
                                                &dag_node.node_type,
                                                target,
                                                item,
                                            );
                                            debug!(
                                                node_id = %node_id,
                                                target = %target,
                                                "Stored return value (unpacked)"
                                            );
                                        }
                                    }
                                    _ => {
                                        warn!(
                                            node_id = %node_id,
                                            targets = ?targets,
                                            value = ?value,
                                            "Return value is not iterable for tuple unpacking"
                                        );
                                        for target in targets {
                                            state.store_variable_for_node(
                                                node_id,
                                                &dag_node.node_type,
                                                target,
                                                &value,
                                            );
                                        }
                                    }
                                }
                            } else {
                                for target in targets {
                                    state.store_variable_for_node(
                                        node_id,
                                        &dag_node.node_type,
                                        target,
                                        &value,
                                    );
                                    debug!(
                                        node_id = %node_id,
                                        target = %target,
                                        "Stored return value"
                                    );
                                }
                            }
                        } else if let Some(target) = &dag_node.target {
                            state.store_variable_for_node(
                                node_id,
                                &dag_node.node_type,
                                target,
                                &value,
                            );
                            debug!(
                                node_id = %node_id,
                                target = %target,
                                "Stored return value"
                            );
                        }

                        Ok(result_bytes)
                    }
                    Err(e) => {
                        warn!(
                            node_id = %node_id,
                            error = %e,
                            "Failed to evaluate return expression"
                        );
                        Err(format!("Return evaluation error: {}", e))
                    }
                }
            } else {
                debug!(node_id = %node_id, "Return node with no expression");
                Ok(vec![])
            }
        }
        _ => {
            debug!(
                node_id = %node_id,
                node_type = %dag_node.node_type,
                "Unknown inline node type completed"
            );
            Ok(vec![])
        }
    }
}

pub fn extract_result_value(result_bytes: &[u8]) -> Option<WorkflowValue> {
    if let Ok(args) = decode_message::<proto::WorkflowArguments>(result_bytes) {
        for arg in args.arguments {
            if arg.key == "result" {
                if let Some(value) = arg.value {
                    return Some(WorkflowValue::from_proto(&value));
                }
                return None;
            }
        }
    }

    workflow_value_from_proto_bytes(result_bytes)
}
