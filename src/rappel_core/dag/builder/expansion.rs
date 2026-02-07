//! Function expansion helpers.

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use super::super::models::DAGNode;
use super::super::models::{DAG, DAGEdge, DagConversionError, EdgeType};
use super::super::nodes::AssignmentNode;
use super::converter::DAGConverter;

/// Inline function calls and remap expansion edges.
impl DAGConverter {
    /// Redirect exception edges to expanded call entries and dedupe edges.
    ///
    /// After function expansion, exception edges may still point at a call
    /// prefix (the "virtual" target). We map those prefixes to the first
    /// node id in the expanded call subtree, then remove duplicate edges.
    pub fn remap_exception_targets(&self, dag: &mut DAG) {
        let call_entry_map = Self::build_call_entry_map(dag);
        for edge in dag
            .edges
            .iter_mut()
            .filter(|edge| edge.exception_types.is_some())
        {
            if dag.nodes.contains_key(&edge.target) {
                continue;
            }
            if let Some(mapped) = call_entry_map.get(&edge.target) {
                edge.target = mapped.clone();
            }
        }

        let mut seen: HashSet<String> = HashSet::new();
        let mut deduped: Vec<DAGEdge> = Vec::new();
        for edge in &dag.edges {
            let key = format!(
                "{}|{}|{:?}|{:?}|{:?}|{:?}|{}|{:?}",
                edge.source,
                edge.target,
                edge.edge_type,
                edge.condition,
                edge.exception_types,
                edge.guard_string,
                edge.is_loop_back,
                edge.variable,
            );
            if seen.contains(&key) {
                continue;
            }
            seen.insert(key);
            deduped.push(edge.clone());
        }
        dag.edges = deduped;
    }

    /// Return a mapping from call prefixes to their first expanded node id.
    ///
    /// Example:
    /// - Expanded nodes: "foo:call_1:action_3", "foo:call_1:return_4"
    /// - Mapping entry: "foo:call_1" -> "foo:call_1:action_3"
    pub fn build_call_entry_map(dag: &DAG) -> HashMap<String, String> {
        let mut mapping: HashMap<String, String> = HashMap::new();
        for node_id in dag.nodes.keys() {
            let parts: Vec<&str> = node_id.split(':').collect();
            if parts.len() <= 1 {
                continue;
            }
            for idx in 1..parts.len() {
                let prefix = parts[..idx].join(":");
                let existing = mapping.get(&prefix).cloned();
                if existing.is_none() || node_id < existing.as_ref().unwrap() {
                    mapping.insert(prefix, node_id.clone());
                }
            }
        }
        mapping
    }

    /// Inline the entry function and all reachable calls into a new DAG.
    pub fn expand_functions(
        &mut self,
        unexpanded: &DAG,
        entry_fn: &str,
    ) -> Result<DAG, DagConversionError> {
        let mut expanded = DAG::default();
        let mut visited_calls: HashSet<String> = HashSet::new();
        self.expand_function_recursive(
            unexpanded,
            entry_fn,
            &mut expanded,
            &mut visited_calls,
            None,
        )?;
        Ok(expanded)
    }

    /// Inline a function into the target DAG and return (first, last) node ids.
    ///
    /// The expansion clones nodes from the callee, prefixes their ids with the
    /// call site, wires argument binding nodes before the callee entry, and
    /// rewrites edges and aggregation pointers.
    ///
    /// Example:
    /// - Caller node "call_2" invokes function "foo".
    /// - Expanded ids become "call_2:foo_input_1", "call_2:action_3", etc.
    /// - The return node's targets are rewritten to match the call assignment.
    pub fn expand_function_recursive(
        &mut self,
        unexpanded: &DAG,
        fn_name: &str,
        target: &mut DAG,
        visited_calls: &mut HashSet<String>,
        id_prefix: Option<String>,
    ) -> Result<Option<(String, String)>, DagConversionError> {
        let fn_nodes = unexpanded.get_nodes_for_function(fn_name);
        if fn_nodes.is_empty() {
            return Ok(None);
        }

        let input_node = fn_nodes.values().find(|node| node.is_input());
        let is_entry_function = id_prefix.is_none();
        let fn_node_ids: HashSet<String> = fn_nodes.keys().cloned().collect();
        let ordered_nodes = self.get_topo_order(unexpanded, &fn_node_ids);

        let mut id_map: HashMap<String, String> = HashMap::new();
        let mut first_real_node: Option<String> = None;
        let mut last_real_node: Option<String> = None;
        let mut output_return_node: Option<String> = None;

        for old_id in ordered_nodes {
            let node = match unexpanded.nodes.get(&old_id) {
                Some(node) => node.clone(),
                None => continue,
            };

            if !is_entry_function && node.is_input() {
                continue;
            }

            if let DAGNode::FnCall(fn_node) = &node
                && self.function_defs.contains_key(&fn_node.called_function)
            {
                let called_fn = fn_node.called_function.clone();
                let call_key = format!("{fn_name}:{old_id}");
                if visited_calls.contains(&call_key) {
                    return Err(DagConversionError(format!(
                        "Recursive function call detected: {fn_name} -> {called_fn}"
                    )));
                }
                visited_calls.insert(call_key.clone());

                let exception_edges: Vec<DAGEdge> = unexpanded
                    .edges
                    .iter()
                    .filter(|edge| edge.source == old_id && edge.exception_types.is_some())
                    .cloned()
                    .collect();

                let child_prefix = id_prefix
                    .as_ref()
                    .map(|prefix| format!("{prefix}:{old_id}"))
                    .unwrap_or_else(|| old_id.clone());

                let expansion = self.expand_function_recursive(
                    unexpanded,
                    &called_fn,
                    target,
                    visited_calls,
                    Some(child_prefix.clone()),
                )?;

                visited_calls.remove(&call_key);

                if let Some((child_first, child_last)) = expansion {
                    let mut bind_ids: Vec<String> = Vec::new();
                    if let Some(fn_def) = self.function_defs.get(&called_fn)
                        && let Some(io) = &fn_def.io
                        && !io.inputs.is_empty()
                    {
                        let kwarg_exprs = fn_node.kwarg_exprs.clone();
                        for (idx, input_name) in io.inputs.iter().enumerate() {
                            if let Some(expr) = kwarg_exprs.get(input_name) {
                                let bind_id = format!("{child_prefix}:bind_{input_name}_{idx}");
                                let bind_node = AssignmentNode::new(
                                    bind_id.clone(),
                                    vec![input_name.clone()],
                                    None,
                                    Some(expr.clone()),
                                    None,
                                    Some(called_fn.clone()),
                                );
                                target.add_node(bind_node.into());
                                bind_ids.push(bind_id);
                            }
                        }
                    }

                    if !bind_ids.is_empty() {
                        for idx in 1..bind_ids.len() {
                            let prev = &bind_ids[idx - 1];
                            let next = &bind_ids[idx];
                            target.add_edge(DAGEdge::state_machine(prev, next));
                        }
                        target.add_edge(DAGEdge::state_machine(
                            bind_ids.last().unwrap(),
                            &child_first,
                        ));
                    }

                    let call_entry = bind_ids
                        .first()
                        .cloned()
                        .unwrap_or_else(|| child_first.clone());
                    id_map.insert(old_id.clone(), call_entry.clone());
                    id_map.insert(format!("{old_id}_last"), child_last.clone());

                    if !exception_edges.is_empty() {
                        let expanded_action_ids: Vec<String> = target
                            .nodes
                            .iter()
                            .filter(|(expanded_id, expanded_node)| {
                                expanded_id.starts_with(&child_prefix)
                                    && matches!(expanded_node, DAGNode::ActionCall(_))
                                    && !expanded_node.is_fn_call()
                            })
                            .map(|(expanded_id, _)| expanded_id.clone())
                            .collect();

                        for expanded_node_id in expanded_action_ids {
                            for exc_edge in &exception_edges {
                                let mut new_edge = exc_edge.clone();
                                new_edge.source = expanded_node_id.clone();
                                if fn_node_ids.contains(&exc_edge.target)
                                    && let Some(prefix) = &id_prefix
                                {
                                    new_edge.target = format!("{prefix}:{}", exc_edge.target);
                                }
                                target.add_edge(new_edge);
                            }
                        }
                    }

                    let fn_call_targets = if let Some(targets) = &fn_node.targets {
                        Some(targets.clone())
                    } else if let Some(target) = &fn_node.target {
                        Some(vec![target.clone()])
                    } else {
                        None
                    };

                    if let Some(targets) = fn_call_targets {
                        let expanded_return_ids: Vec<String> = target
                            .nodes
                            .iter()
                            .filter(|(expanded_id, expanded_node)| {
                                expanded_id.starts_with(&child_prefix)
                                    && expanded_node.node_type() == "return"
                                    && expanded_node.function_name() == Some(called_fn.as_str())
                            })
                            .map(|(expanded_id, _)| expanded_id.clone())
                            .collect();
                        for return_id in expanded_return_ids {
                            if let Some(DAGNode::Return(return_node)) =
                                target.nodes.get_mut(&return_id)
                            {
                                return_node.targets = Some(targets.clone());
                                return_node.target = targets.first().cloned();
                            }
                        }
                    }

                    if first_real_node.is_none() {
                        first_real_node = Some(call_entry);
                    }
                    last_real_node = Some(child_last);
                    continue;
                }
            }

            let new_id = if let Some(prefix) = &id_prefix {
                format!("{prefix}:{}", node.id())
            } else {
                node.id().to_string()
            };

            let mut cloned = node.clone();
            self.update_node_id(&mut cloned, &new_id);
            self.update_node_uuid(&mut cloned);
            if let Some(prefix) = &id_prefix {
                if let DAGNode::ActionCall(action_node) = &mut cloned
                    && let Some(aggregates_to) = &action_node.aggregates_to
                    && fn_node_ids.contains(aggregates_to)
                {
                    action_node.aggregates_to = Some(format!("{prefix}:{aggregates_to}"));
                }
                if let DAGNode::FnCall(fn_node) = &mut cloned
                    && let Some(aggregates_to) = &fn_node.aggregates_to
                    && fn_node_ids.contains(aggregates_to)
                {
                    fn_node.aggregates_to = Some(format!("{prefix}:{aggregates_to}"));
                }
                if let DAGNode::Aggregator(agg_node) = &mut cloned
                    && fn_node_ids.contains(&agg_node.aggregates_from)
                {
                    agg_node.aggregates_from = format!("{prefix}:{}", agg_node.aggregates_from);
                }
            }

            id_map.insert(old_id.clone(), new_id.clone());

            if first_real_node.is_none() {
                first_real_node = Some(new_id.clone());
            }
            last_real_node = Some(new_id.clone());
            if matches!(cloned.node_type(), "output" | "return") {
                output_return_node = Some(new_id.clone());
            }
            target.add_node(cloned);
        }

        let input_id = input_node.map(|node| node.id().to_string());
        for edge in &unexpanded.edges {
            if !fn_node_ids.contains(&edge.source) {
                continue;
            }
            if !is_entry_function
                && let Some(input_node_id) = &input_id
                && &edge.source == input_node_id
            {
                continue;
            }

            let mut new_source = match id_map.get(&edge.source) {
                Some(mapped) => mapped.clone(),
                None => continue,
            };
            if let Some(source_node) = unexpanded.nodes.get(&edge.source)
                && source_node.is_fn_call()
                && let Some(mapped) = id_map.get(&format!("{}_last", edge.source))
            {
                new_source = mapped.clone();
            }

            if edge.edge_type == EdgeType::DataFlow
                && let Some(target_node) = unexpanded.nodes.get(&edge.target)
                && target_node.is_fn_call()
            {
                continue;
            }

            let new_target = id_map
                .get(&edge.target)
                .cloned()
                .unwrap_or_else(|| edge.target.clone());

            let mut cloned_edge = edge.clone();
            cloned_edge.source = new_source;
            cloned_edge.target = new_target;
            target.add_edge(cloned_edge);
        }

        let canonical_last = output_return_node.or(last_real_node);
        if let (Some(first), Some(last)) = (first_real_node, canonical_last) {
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    /// Return a topological order for the given node subset.
    ///
    /// This only considers state-machine edges and ignores loop backs.
    pub fn get_topo_order(&self, dag: &DAG, node_ids: &HashSet<String>) -> Vec<String> {
        let mut in_degree: HashMap<String, i32> =
            node_ids.iter().map(|id| (id.clone(), 0)).collect();
        let mut adjacency: HashMap<String, Vec<String>> =
            node_ids.iter().map(|id| (id.clone(), Vec::new())).collect();

        for edge in &dag.edges {
            if edge.is_loop_back {
                continue;
            }
            if edge.edge_type == EdgeType::StateMachine
                && node_ids.contains(&edge.source)
                && node_ids.contains(&edge.target)
            {
                adjacency
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
                *in_degree.entry(edge.target.clone()).or_insert(0) += 1;
            }
        }

        let mut queue: Vec<String> = in_degree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(node_id, _)| node_id.clone())
            .collect();
        let mut order: Vec<String> = Vec::new();

        while let Some(node_id) = queue.pop() {
            order.push(node_id.clone());
            if let Some(neighbors) = adjacency.get(&node_id) {
                for neighbor in neighbors {
                    if let Some(degree) = in_degree.get_mut(neighbor) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(neighbor.clone());
                        }
                    }
                }
            }
        }

        order
    }

    fn update_node_id(&self, node: &mut DAGNode, new_id: &str) {
        match node {
            DAGNode::Input(node) => node.id = new_id.to_string(),
            DAGNode::Output(node) => node.id = new_id.to_string(),
            DAGNode::Assignment(node) => node.id = new_id.to_string(),
            DAGNode::ActionCall(node) => node.id = new_id.to_string(),
            DAGNode::FnCall(node) => node.id = new_id.to_string(),
            DAGNode::Parallel(node) => node.id = new_id.to_string(),
            DAGNode::Aggregator(node) => node.id = new_id.to_string(),
            DAGNode::Branch(node) => node.id = new_id.to_string(),
            DAGNode::Join(node) => node.id = new_id.to_string(),
            DAGNode::Return(node) => node.id = new_id.to_string(),
            DAGNode::Break(node) => node.id = new_id.to_string(),
            DAGNode::Continue(node) => node.id = new_id.to_string(),
            DAGNode::Sleep(node) => node.id = new_id.to_string(),
            DAGNode::Expression(node) => node.id = new_id.to_string(),
        }
    }

    fn update_node_uuid(&self, node: &mut DAGNode) {
        match node {
            DAGNode::Input(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Output(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Assignment(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::ActionCall(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::FnCall(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Parallel(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Aggregator(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Branch(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Join(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Return(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Break(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Continue(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Sleep(node) => node.node_uuid = Uuid::new_v4(),
            DAGNode::Expression(node) => node.node_uuid = Uuid::new_v4(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::super::super::models::{DAG, DAGEdge};
    use super::super::super::nodes::{ActionCallNode, ActionCallParams, InputNode};
    use super::*;

    #[test]
    fn test_remap_exception_targets_happy_path() {
        let mut dag = DAG::default();
        dag.add_node(InputNode::new("main_input", vec![], Some("main".to_string())).into());
        dag.add_node(
            ActionCallNode::new(
                "call_1:action_2",
                "work",
                ActionCallParams {
                    module_name: None,
                    kwargs: HashMap::new(),
                    kwarg_exprs: HashMap::new(),
                    policies: Vec::new(),
                    targets: None,
                    target: None,
                    parallel_index: None,
                    aggregates_to: None,
                    spread_loop_var: None,
                    spread_collection_expr: None,
                    function_name: Some("main".to_string()),
                },
            )
            .into(),
        );
        dag.add_edge(DAGEdge::state_machine_with_exception(
            "main_input",
            "call_1",
            vec!["Exception".to_string()],
        ));

        DAGConverter::new().remap_exception_targets(&mut dag);

        assert!(
            dag.edges
                .iter()
                .any(|edge| edge.target == "call_1:action_2"),
            "remap should resolve exception edge target to expanded call entry"
        );
    }
}
