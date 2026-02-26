//! Data-flow edge construction helpers.

use std::collections::{HashMap, HashSet, VecDeque};

use waymark_proto::ast as ir;

use super::super::models::DAGNode;
use super::super::models::{DAG, DAGEdge, EdgeType};
use super::super::nodes::{
    ActionCallNode, AssignmentNode, FnCallNode, InputNode, ReturnNode, SleepNode,
};
use super::converter::DAGConverter;

/// Rebuild data-flow edges from variable definition/use analysis.
impl DAGConverter {
    /// Recompute and add data-flow edges for the full expanded DAG.
    ///
    /// This rebuilds data-flow edges at the global level so variable uses in
    /// guards, loop bodies, and expanded functions all point to the correct
    /// definition nodes.
    ///
    /// Example:
    /// - x = 1
    /// - if x > 0: @action(x)
    ///   The guard and action both receive data-flow edges from the assignment.
    pub fn add_global_data_flow_edges(&self, dag: &mut DAG) {
        let existing_data_flow: Vec<DAGEdge> = dag
            .edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::DataFlow)
            .cloned()
            .collect();
        dag.edges
            .retain(|edge| edge.edge_type != EdgeType::DataFlow);

        let node_ids: HashSet<String> = dag.nodes.keys().cloned().collect();
        let mut in_degree: HashMap<String, i32> =
            node_ids.iter().map(|id| (id.clone(), 0)).collect();
        let mut adjacency: HashMap<String, Vec<String>> =
            node_ids.iter().map(|id| (id.clone(), Vec::new())).collect();

        for edge in dag.get_state_machine_edges() {
            if edge.is_loop_back {
                continue;
            }
            if node_ids.contains(&edge.source) && node_ids.contains(&edge.target) {
                adjacency
                    .entry(edge.source.clone())
                    .or_default()
                    .push(edge.target.clone());
                *in_degree.entry(edge.target.clone()).or_insert(0) += 1;
            }
        }

        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(node_id, _)| node_id.clone())
            .collect();
        let mut order: Vec<String> = Vec::new();

        while let Some(node_id) = queue.pop_front() {
            order.push(node_id.clone());
            if let Some(neighbors) = adjacency.get(&node_id) {
                for neighbor in neighbors {
                    if let Some(degree) = in_degree.get_mut(neighbor) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(neighbor.clone());
                        }
                    }
                }
            }
        }

        let loop_back_edges: Vec<DAGEdge> = dag
            .edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::StateMachine && edge.is_loop_back)
            .cloned()
            .collect();

        let mut var_modifications: HashMap<String, Vec<String>> = HashMap::new();
        for node_id in &order {
            let node = match dag.nodes.get(node_id) {
                Some(node) => node,
                None => continue,
            };
            if node.node_type() == "join" {
                continue;
            }
            if node.is_input()
                && let DAGNode::Input(InputNode { io_vars, .. }) = node
            {
                for input_name in io_vars {
                    var_modifications
                        .entry(input_name.clone())
                        .or_default()
                        .push(node_id.clone());
                }
            }
            for target in Self::targets_for_node(node) {
                if node.node_type() == "return" && var_modifications.contains_key(&target) {
                    continue;
                }
                var_modifications
                    .entry(target.clone())
                    .or_default()
                    .push(node_id.clone());
            }
        }

        let var_modifications_clone = var_modifications.clone();

        let mut node_guard_exprs: HashMap<String, Vec<ir::Expr>> = HashMap::new();
        for edge in &dag.edges {
            if let Some(expr) = &edge.guard_expr {
                node_guard_exprs
                    .entry(edge.source.clone())
                    .or_default()
                    .push(expr.clone());
            }
        }

        let uses_var =
            |node: &DAGNode, var_name: &str, node_guard_exprs: &HashMap<String, Vec<ir::Expr>>| {
                if let DAGNode::ActionCall(ActionCallNode {
                    kwargs,
                    kwarg_exprs,
                    spread_collection_expr,
                    ..
                }) = node
                {
                    if kwargs
                        .values()
                        .any(|value| value == &format!("${var_name}"))
                    {
                        return true;
                    }
                    if kwarg_exprs
                        .values()
                        .any(|expr| Self::expr_uses_var(expr, var_name))
                    {
                        return true;
                    }
                    if let Some(expr) = spread_collection_expr
                        && Self::expr_uses_var(expr, var_name)
                    {
                        return true;
                    }
                }
                if let DAGNode::FnCall(FnCallNode {
                    kwargs,
                    kwarg_exprs,
                    assign_expr,
                    ..
                }) = node
                {
                    if kwargs
                        .values()
                        .any(|value| value == &format!("${var_name}"))
                    {
                        return true;
                    }
                    if kwarg_exprs
                        .values()
                        .any(|expr| Self::expr_uses_var(expr, var_name))
                    {
                        return true;
                    }
                    if let Some(expr) = assign_expr
                        && Self::expr_uses_var(expr, var_name)
                    {
                        return true;
                    }
                }
                if let DAGNode::Assignment(AssignmentNode {
                    assign_expr: Some(expr),
                    ..
                }) = node
                    && Self::expr_uses_var(expr, var_name)
                {
                    return true;
                }
                if let DAGNode::Return(ReturnNode {
                    assign_expr: Some(expr),
                    ..
                }) = node
                    && Self::expr_uses_var(expr, var_name)
                {
                    return true;
                }
                if let DAGNode::Sleep(SleepNode {
                    duration_expr: Some(expr),
                    ..
                }) = node
                    && Self::expr_uses_var(expr, var_name)
                {
                    return true;
                }
                if let Some(guards) = node_guard_exprs.get(node.id()) {
                    for guard in guards {
                        if Self::expr_uses_var(guard, var_name) {
                            return true;
                        }
                    }
                }
                false
            };

        let mut seen_edges: HashSet<(String, String, Option<String>)> = existing_data_flow
            .iter()
            .map(|edge| {
                (
                    edge.source.clone(),
                    edge.target.clone(),
                    edge.variable.clone(),
                )
            })
            .collect();

        let exception_action_sources: HashSet<String> = dag
            .edges
            .iter()
            .filter(|edge| edge.exception_types.is_some())
            .filter_map(|edge| {
                dag.nodes
                    .get(&edge.source)
                    .map(|node| (edge.source.clone(), node))
            })
            .filter(|(_, node)| node.node_type() == "action_call" && !node.is_fn_call())
            .map(|(source, _)| source)
            .collect();

        let mut reachable_via_normal_edges: HashMap<String, HashSet<String>> = HashMap::new();
        let mut normal_adj: HashMap<String, Vec<String>> = HashMap::new();
        for edge in dag.get_state_machine_edges() {
            if edge.is_loop_back || edge.exception_types.is_some() {
                continue;
            }
            normal_adj
                .entry(edge.source.clone())
                .or_default()
                .push(edge.target.clone());
        }

        for start in dag.nodes.keys() {
            let mut reachable: HashSet<String> = HashSet::new();
            let mut queue: Vec<String> = vec![start.clone()];
            while let Some(node_id) = queue.pop() {
                if exception_action_sources.contains(&node_id) {
                    continue;
                }
                if let Some(neighbors) = normal_adj.get(&node_id) {
                    for neighbor in neighbors {
                        if reachable.insert(neighbor.clone()) {
                            queue.push(neighbor.clone());
                        }
                    }
                }
            }
            reachable_via_normal_edges.insert(start.clone(), reachable);
        }

        for (var_name, modifications) in &var_modifications {
            let mut mods: Vec<String> = modifications
                .iter()
                .cloned()
                .collect::<HashSet<String>>()
                .into_iter()
                .collect();
            mods.sort_by_key(|node_id| {
                order
                    .iter()
                    .position(|item| item == node_id)
                    .unwrap_or(order.len())
            });

            for (idx, mod_node) in mods.iter().enumerate() {
                if !order.contains(mod_node) {
                    continue;
                }
                let mod_pos = order.iter().position(|item| item == mod_node).unwrap_or(0);
                let next_mod = mods.get(idx + 1);

                let mut next_mod_reachable = false;
                if let Some(next_mod_node) = next_mod
                    && let Some(reachable) = reachable_via_normal_edges.get(mod_node)
                {
                    next_mod_reachable = reachable.contains(next_mod_node);
                }

                for (pos, node_id) in order.iter().enumerate() {
                    if pos <= mod_pos {
                        continue;
                    }
                    if let Some(next_mod_node) = next_mod
                        && node_id == next_mod_node
                    {
                        if let Some(node) = dag.nodes.get(node_id)
                            && uses_var(node, var_name, &node_guard_exprs)
                        {
                            let key = (mod_node.clone(), node_id.clone(), Some(var_name.clone()));
                            if !seen_edges.contains(&key) {
                                seen_edges.insert(key);
                                dag.edges
                                    .push(DAGEdge::data_flow(mod_node, node_id, var_name));
                            }
                        }
                        if next_mod_reachable {
                            break;
                        }
                    }

                    if let Some(node) = dag.nodes.get(node_id)
                        && uses_var(node, var_name, &node_guard_exprs)
                    {
                        let key = (mod_node.clone(), node_id.clone(), Some(var_name.clone()));
                        if !seen_edges.contains(&key) {
                            seen_edges.insert(key);
                            dag.edges
                                .push(DAGEdge::data_flow(mod_node, node_id, var_name));
                        }
                    }
                }
            }
        }

        let loop_back_sources: HashSet<String> = loop_back_edges
            .iter()
            .map(|edge| edge.source.clone())
            .collect();
        let loop_back_targets: HashSet<String> = loop_back_edges
            .iter()
            .map(|edge| edge.target.clone())
            .collect();
        let mut nodes_in_loop: HashSet<String> = loop_back_sources.clone();

        let mut queue: Vec<String> = loop_back_sources.iter().cloned().collect();
        let mut visited: HashSet<String> = loop_back_sources.clone();
        while let Some(node_id) = queue.pop() {
            if loop_back_targets.contains(&node_id) {
                continue;
            }
            for edge in dag.get_state_machine_edges() {
                if edge.target == node_id
                    && !edge.is_loop_back
                    && visited.insert(edge.source.clone())
                {
                    nodes_in_loop.insert(edge.source.clone());
                    queue.push(edge.source.clone());
                }
            }
        }

        for (var_name, modifications) in var_modifications_clone {
            for mod_node in modifications {
                if !nodes_in_loop.contains(&mod_node) {
                    continue;
                }
                for node_id in &order {
                    if node_id == &mod_node {
                        continue;
                    }
                    let reachable = reachable_via_normal_edges
                        .get(&mod_node)
                        .cloned()
                        .unwrap_or_default();
                    let target_in_loop = nodes_in_loop.contains(node_id);
                    if !target_in_loop && !reachable.contains(node_id) {
                        continue;
                    }
                    let node = match dag.nodes.get(node_id) {
                        Some(node) => node,
                        None => continue,
                    };
                    let is_loop_index = var_name.starts_with("__loop_");
                    let is_action_in_loop =
                        node.node_type() == "action_call" && nodes_in_loop.contains(node_id);
                    let should_add = uses_var(node, &var_name, &node_guard_exprs)
                        || (is_loop_index && is_action_in_loop);

                    if should_add {
                        let key = (mod_node.clone(), node_id.clone(), Some(var_name.clone()));
                        if !seen_edges.contains(&key) {
                            seen_edges.insert(key);
                            let mut edge = DAGEdge::data_flow(&mod_node, node_id, &var_name);
                            if mod_node.contains("loop_incr") && is_action_in_loop {
                                edge.is_loop_back = true;
                            }
                            dag.edges.push(edge);
                        }
                    }
                }

                let self_key = (mod_node.clone(), mod_node.clone(), Some(var_name.clone()));
                if !seen_edges.contains(&self_key) {
                    seen_edges.insert(self_key);
                    dag.edges
                        .push(DAGEdge::data_flow(&mod_node, &mod_node, &var_name));
                }
            }
        }

        for edge in &loop_back_edges {
            if let Some(source_node) = dag.nodes.get(&edge.source) {
                let defined_vars = Self::targets_for_node(source_node);
                for var in defined_vars {
                    dag.edges
                        .push(DAGEdge::data_flow(&edge.source, &edge.target, var));
                }
            }
        }

        for node in dag.nodes.values() {
            if node.node_type() != "join" {
                continue;
            }
            let label = node.label();
            if !label.starts_with("end for ") && !label.starts_with("end while ") {
                continue;
            }
            let join_targets = Self::targets_for_node(node);
            if join_targets.is_empty() {
                continue;
            }
            for var in join_targets {
                for node_id in &order {
                    if nodes_in_loop.contains(node_id) {
                        continue;
                    }
                    if let Some(target_node) = dag.nodes.get(node_id)
                        && uses_var(target_node, &var, &node_guard_exprs)
                    {
                        let key = (node.id().to_string(), node_id.clone(), Some(var.clone()));
                        if !seen_edges.contains(&key) {
                            seen_edges.insert(key);
                            dag.edges.push(DAGEdge::data_flow(node.id(), node_id, &var));
                        }
                    }
                }
            }
        }

        dag.edges.extend(existing_data_flow);
    }

    /// Add per-function data-flow edges after a function conversion.
    pub fn add_data_flow_edges_for_function(&mut self, function_name: &str) {
        let fn_node_ids: HashSet<String> = self
            .dag
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();
        let order = self.get_execution_order_for_nodes(&fn_node_ids);
        self.add_data_flow_from_definitions(function_name, &order);
    }

    /// Add data-flow edges using the current variable definition history.
    pub fn add_data_flow_from_definitions(&mut self, function_name: &str, order: &[String]) {
        let fn_node_ids: HashSet<String> = self
            .dag
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();
        let mut edges_to_add: Vec<(String, String, String)> = Vec::new();

        for (var_name, modifications) in &self.var_modifications {
            for (idx, mod_node) in modifications.iter().enumerate() {
                if !fn_node_ids.contains(mod_node) {
                    continue;
                }
                let next_mod = modifications.get(idx + 1);
                let mod_pos = order.iter().position(|node_id| node_id == mod_node);

                for (pos, node_id) in order.iter().enumerate() {
                    if let Some(pos_idx) = mod_pos
                        && pos <= pos_idx
                    {
                        continue;
                    }
                    if node_id == mod_node {
                        continue;
                    }
                    if let Some(node) = self.dag.nodes.get(node_id)
                        && self.node_uses_variable(node, var_name)
                    {
                        edges_to_add.push((var_name.clone(), mod_node.clone(), node_id.clone()));
                    }
                    if let Some(next_mod_node) = next_mod
                        && node_id == next_mod_node
                    {
                        break;
                    }
                }
            }
        }

        for (var_name, source, target) in edges_to_add {
            let is_loop_back = source.contains("loop_incr") && !target.contains("loop_exit");
            let mut edge = DAGEdge::data_flow(&source, &target, var_name);
            if is_loop_back {
                edge.is_loop_back = true;
            }
            self.dag.add_edge(edge);
        }
    }

    /// Return True if a node's arguments reference the variable.
    pub fn node_uses_variable(&self, node: &DAGNode, var_name: &str) -> bool {
        match node {
            DAGNode::ActionCall(ActionCallNode {
                kwargs,
                kwarg_exprs,
                ..
            })
            | DAGNode::FnCall(FnCallNode {
                kwargs,
                kwarg_exprs,
                ..
            }) => {
                if kwargs
                    .values()
                    .any(|value| value == &format!("${var_name}"))
                {
                    return true;
                }
                kwarg_exprs
                    .values()
                    .any(|expr| Self::expr_uses_var(expr, var_name))
            }
            _ => false,
        }
    }

    /// Return True if an expression tree references the variable name.
    pub fn expr_uses_var(expr: &ir::Expr, var_name: &str) -> bool {
        match expr.kind.as_ref() {
            Some(ir::expr::Kind::Literal(_)) => false,
            Some(ir::expr::Kind::Variable(var)) => var.name == var_name,
            Some(ir::expr::Kind::BinaryOp(op)) => {
                let left = op
                    .left
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false);
                let right = op
                    .right
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false);
                left || right
            }
            Some(ir::expr::Kind::UnaryOp(op)) => op
                .operand
                .as_ref()
                .map(|expr| Self::expr_uses_var(expr, var_name))
                .unwrap_or(false),
            Some(ir::expr::Kind::List(list_expr)) => list_expr
                .elements
                .iter()
                .any(|element| Self::expr_uses_var(element, var_name)),
            Some(ir::expr::Kind::Dict(dict_expr)) => dict_expr.entries.iter().any(|entry| {
                entry
                    .key
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false)
                    || entry
                        .value
                        .as_ref()
                        .map(|expr| Self::expr_uses_var(expr, var_name))
                        .unwrap_or(false)
            }),
            Some(ir::expr::Kind::Index(index)) => {
                let object = index
                    .object
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false);
                let index_expr = index
                    .index
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false);
                object || index_expr
            }
            Some(ir::expr::Kind::Dot(dot)) => dot
                .object
                .as_ref()
                .map(|expr| Self::expr_uses_var(expr, var_name))
                .unwrap_or(false),
            Some(ir::expr::Kind::FunctionCall(call)) => {
                call.args
                    .iter()
                    .any(|arg| Self::expr_uses_var(arg, var_name))
                    || call.kwargs.iter().any(|kw| {
                        kw.value
                            .as_ref()
                            .map(|expr| Self::expr_uses_var(expr, var_name))
                            .unwrap_or(false)
                    })
            }
            Some(ir::expr::Kind::ActionCall(call)) => call.kwargs.iter().any(|kw| {
                kw.value
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false)
            }),
            Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                for call in &parallel.calls {
                    match call.kind.as_ref() {
                        Some(ir::call::Kind::Action(action)) => {
                            if action.kwargs.iter().any(|kw| {
                                kw.value
                                    .as_ref()
                                    .map(|expr| Self::expr_uses_var(expr, var_name))
                                    .unwrap_or(false)
                            }) {
                                return true;
                            }
                        }
                        Some(ir::call::Kind::Function(function)) => {
                            if function
                                .args
                                .iter()
                                .any(|arg| Self::expr_uses_var(arg, var_name))
                                || function.kwargs.iter().any(|kw| {
                                    kw.value
                                        .as_ref()
                                        .map(|expr| Self::expr_uses_var(expr, var_name))
                                        .unwrap_or(false)
                                })
                            {
                                return true;
                            }
                        }
                        None => continue,
                    }
                }
                false
            }
            Some(ir::expr::Kind::SpreadExpr(spread)) => {
                let collection = spread
                    .collection
                    .as_ref()
                    .map(|expr| Self::expr_uses_var(expr, var_name))
                    .unwrap_or(false);
                if collection {
                    return true;
                }
                if let Some(action) = &spread.action {
                    return action.kwargs.iter().any(|kw| {
                        kw.value
                            .as_ref()
                            .map(|expr| Self::expr_uses_var(expr, var_name))
                            .unwrap_or(false)
                    });
                }
                false
            }
            None => false,
        }
    }

    /// Topologically order nodes using state-machine edges (ignoring loop backs).
    pub fn get_execution_order_for_nodes(&self, node_ids: &HashSet<String>) -> Vec<String> {
        let mut in_degree: HashMap<String, i32> =
            node_ids.iter().map(|id| (id.clone(), 0)).collect();
        let mut adjacency: HashMap<String, Vec<String>> =
            node_ids.iter().map(|id| (id.clone(), Vec::new())).collect();

        for edge in self.dag.get_state_machine_edges() {
            if edge.is_loop_back {
                continue;
            }
            if node_ids.contains(&edge.source) && node_ids.contains(&edge.target) {
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
}

#[cfg(test)]
mod tests {
    use super::super::super::models::EdgeType;
    use super::super::test_helpers::build_dag;

    #[test]
    fn test_add_global_data_flow_edges_happy_path() {
        let dag = build_dag(
            r#"
            fn main(input: [x], output: [y]):
                z = x + 1
                y = z + 2
                return y
            "#,
        );

        assert!(
            dag.edges.iter().any(|edge| {
                edge.edge_type == EdgeType::DataFlow && edge.variable.as_deref() == Some("z")
            }),
            "expanded DAG should include global data-flow edges for derived variables"
        );
    }
}
