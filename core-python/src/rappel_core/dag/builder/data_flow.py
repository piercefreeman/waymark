"""Data-flow edge construction helpers."""

from __future__ import annotations

import copy
from typing import Dict, List, Optional, Set

from proto import ast_pb2 as ir

from ..models import DAG, DAGEdge, EdgeType, assert_never
from ..nodes import (
    ActionCallNode,
    AggregatorNode,
    AssignmentNode,
    DAGNode,
    FnCallNode,
    InputNode,
    JoinNode,
    ReturnNode,
)


class DataFlowMixin:
    """Rebuild data-flow edges from variable definition/use analysis."""

    def add_global_data_flow_edges(self, dag: "DAG") -> None:
        """Recompute and add data-flow edges for the full expanded DAG.

        This rebuilds data-flow edges at the global level so variable uses in
        guards, loop bodies, and expanded functions all point to the correct
        definition nodes.

        Example:
        - x = 1
        - if x > 0: @action(x)
        The guard and action both receive data-flow edges from the assignment.
        """
        existing_data_flow = [
            copy.deepcopy(edge) for edge in dag.edges if edge.edge_type == EdgeType.DATA_FLOW
        ]
        dag.edges = [edge for edge in dag.edges if edge.edge_type != EdgeType.DATA_FLOW]

        node_ids: Set[str] = set(dag.nodes.keys())
        in_degree: Dict[str, int] = {node_id: 0 for node_id in node_ids}
        adjacency: Dict[str, List[str]] = {node_id: [] for node_id in node_ids}

        # Build a topo order for state-machine edges (without loop backs).
        for edge in dag.get_state_machine_edges():
            if edge.is_loop_back:
                continue
            if edge.source in node_ids and edge.target in node_ids:
                adjacency[edge.source].append(edge.target)
                in_degree[edge.target] = in_degree.get(edge.target, 0) + 1

        queue: List[str] = [node_id for node_id, degree in in_degree.items() if degree == 0]
        order: List[str] = []

        while queue:
            node_id = queue.pop()
            order.append(node_id)
            for neighbor in adjacency.get(node_id, []):
                degree = in_degree.get(neighbor, 0)
                if degree > 0:
                    in_degree[neighbor] = degree - 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        # Keep loop-back state-machine edges to reapply data-flow across iterations.
        loop_back_edges = [
            copy.deepcopy(edge)
            for edge in dag.edges
            if edge.edge_type == EdgeType.STATE_MACHINE and edge.is_loop_back
        ]

        # Track all nodes that define each variable in topo order.
        var_modifications: Dict[str, List[str]] = {}
        for node_id in order:
            node = dag.nodes.get(node_id)
            if node is None:
                continue
            if node.node_type == "join":
                continue
            if node.is_input and isinstance(node, InputNode):
                for input_name in node.io_vars:
                    var_modifications.setdefault(input_name, []).append(node_id)
            for target in self._targets_for_node(node):
                if node.node_type == "return" and target in var_modifications:
                    continue
                var_modifications.setdefault(target, []).append(node_id)

        var_modifications_clone = {k: list(v) for k, v in var_modifications.items()}

        # Guard expressions count as variable uses that should receive data edges.
        node_guard_exprs: Dict[str, List[ir.Expr]] = {}
        for edge in dag.edges:
            if edge.guard_expr is not None:
                node_guard_exprs.setdefault(edge.source, []).append(edge.guard_expr)

        def uses_var(node: DAGNode, var_name: str) -> bool:
            if isinstance(node, (ActionCallNode, FnCallNode)):
                for value in node.kwargs.values():
                    if value == f"${var_name}":
                        return True

            if isinstance(node, (AssignmentNode, FnCallNode, ReturnNode)) and node.assign_expr:
                if self.expr_uses_var(node.assign_expr, var_name):
                    return True

            for guard in node_guard_exprs.get(node.id, []):
                if self.expr_uses_var(guard, var_name):
                    return True

            if isinstance(node, (ActionCallNode, FnCallNode)):
                for expr in node.kwarg_exprs.values():
                    if self.expr_uses_var(expr, var_name):
                        return True

            if isinstance(node, ActionCallNode) and node.spread_collection_expr:
                if self.expr_uses_var(node.spread_collection_expr, var_name):
                    return True

            return False

        seen_edges: Set[tuple[str, str, Optional[str]]] = {
            (edge.source, edge.target, edge.variable) for edge in existing_data_flow
        }

        # Exception edges start from action calls; we skip data edges that would
        # imply dependencies through exception-only paths.
        exception_action_sources: Set[str] = {
            edge.source
            for edge in dag.edges
            if edge.exception_types is not None
            and dag.nodes.get(edge.source)
            and dag.nodes[edge.source].node_type == "action_call"
            and not dag.nodes[edge.source].is_fn_call
        }

        reachable_via_normal_edges: Dict[str, Set[str]] = {}
        normal_adj: Dict[str, List[str]] = {}
        for edge in dag.get_state_machine_edges():
            if edge.is_loop_back or edge.exception_types is not None:
                continue
            normal_adj.setdefault(edge.source, []).append(edge.target)

        for start in dag.nodes:
            reachable: Set[str] = set()
            queue = [start]
            while queue:
                node_id = queue.pop()
                if node_id in exception_action_sources:
                    continue
                for neighbor in normal_adj.get(node_id, []):
                    if neighbor not in reachable:
                        reachable.add(neighbor)
                        queue.append(neighbor)
            reachable_via_normal_edges[start] = reachable

        for var_name, modifications in var_modifications.items():
            mods = sorted(
                set(modifications),
                key=lambda node_id: order.index(node_id) if node_id in order else len(order),
            )

            for idx, mod_node in enumerate(mods):
                if mod_node not in order:
                    continue
                mod_pos = order.index(mod_node)
                next_mod = mods[idx + 1] if idx + 1 < len(mods) else None

                next_mod_reachable = False
                if next_mod:
                    reachable = reachable_via_normal_edges.get(mod_node, set())
                    next_mod_reachable = next_mod in reachable

                # Add data edges to each downstream use until the next reachable
                # modification of the same variable.
                for pos, node_id in enumerate(order):
                    if pos <= mod_pos:
                        continue

                    if next_mod and node_id == next_mod:
                        node = dag.nodes.get(node_id)
                        if node and uses_var(node, var_name):
                            key = (mod_node, node_id, var_name)
                            if key not in seen_edges:
                                seen_edges.add(key)
                                dag.edges.append(DAGEdge.data_flow(mod_node, node_id, var_name))
                        if next_mod_reachable:
                            break

                    node = dag.nodes.get(node_id)
                    if node and uses_var(node, var_name):
                        key = (mod_node, node_id, var_name)
                        if key not in seen_edges:
                            seen_edges.add(key)
                            dag.edges.append(DAGEdge.data_flow(mod_node, node_id, var_name))

        loop_back_sources: Set[str] = {edge.source for edge in loop_back_edges}
        loop_back_targets: Set[str] = {edge.target for edge in loop_back_edges}
        nodes_in_loop: Set[str] = set(loop_back_sources)

        # Walk backwards from loop-back sources to find all nodes in the loop body.
        queue = list(loop_back_sources)
        visited: Set[str] = set(loop_back_sources)
        while queue:
            node_id = queue.pop()
            if node_id in loop_back_targets:
                continue
            for edge in dag.get_state_machine_edges():
                if edge.target == node_id and not edge.is_loop_back:
                    if edge.source not in visited:
                        visited.add(edge.source)
                        nodes_in_loop.add(edge.source)
                        queue.append(edge.source)

        for var_name, modifications in var_modifications_clone.items():
            for mod_node in modifications:
                if mod_node not in nodes_in_loop:
                    continue

                # Inside loops we allow data-flow to loop-internal nodes and
                # to reachable nodes outside the loop body.
                for node_id in order:
                    if node_id == mod_node:
                        continue
                    reachable = reachable_via_normal_edges.get(mod_node, set())
                    target_in_loop = node_id in nodes_in_loop
                    if not target_in_loop and node_id not in reachable:
                        continue

                    node = dag.nodes.get(node_id)
                    if node is None:
                        continue

                    is_loop_index = var_name.startswith("__loop_")
                    is_action_in_loop = node.node_type == "action_call" and node_id in nodes_in_loop
                    should_add = uses_var(node, var_name) or (is_loop_index and is_action_in_loop)

                    if should_add:
                        key = (mod_node, node_id, var_name)
                        if key not in seen_edges:
                            seen_edges.add(key)
                            edge = DAGEdge.data_flow(mod_node, node_id, var_name)
                            if "loop_incr" in mod_node and is_action_in_loop:
                                edge.is_loop_back = True
                            dag.edges.append(edge)

                # Self edges inside loops keep loop-carried variables explicit.
                self_key = (mod_node, mod_node, var_name)
                if self_key not in seen_edges:
                    seen_edges.add(self_key)
                    dag.edges.append(DAGEdge.data_flow(mod_node, mod_node, var_name))

        # Loop-back state-machine edges also imply data-flow for defined vars.
        for edge in loop_back_edges:
            source_node = dag.nodes.get(edge.source)
            if source_node is None:
                continue
            defined_vars = self._targets_for_node(source_node)

            for var in defined_vars:
                dag.edges.append(DAGEdge.data_flow(edge.source, edge.target, var))

        # After loops, connect loop "join" nodes to downstream uses of their targets.
        for node in dag.nodes.values():
            if node.node_type != "join":
                continue
            if not (node.label.startswith("end for ") or node.label.startswith("end while ")):
                continue
            join_targets = self._targets_for_node(node)
            if not join_targets:
                continue
            for var in join_targets:
                for node_id in order:
                    if node_id in nodes_in_loop:
                        continue
                    target_node = dag.nodes.get(node_id)
                    if target_node and uses_var(target_node, var):
                        key = (node.id, node_id, var)
                        if key not in seen_edges:
                            seen_edges.add(key)
                            dag.edges.append(DAGEdge.data_flow(node.id, node_id, var))

        dag.edges.extend(existing_data_flow)

    def add_data_flow_edges_for_function(self, function_name: str) -> None:
        """Add per-function data-flow edges after a function conversion."""
        fn_node_ids = set(self.dag.get_nodes_for_function(function_name).keys())
        order = self.get_execution_order_for_nodes(fn_node_ids)
        self.add_data_flow_from_definitions(function_name, order)

    def add_data_flow_from_definitions(self, function_name: str, order: List[str]) -> None:
        """Add data-flow edges using the current variable definition history."""
        fn_node_ids = set(self.dag.get_nodes_for_function(function_name).keys())
        edges_to_add: List[tuple[str, str, str]] = []

        for var_name, modifications in self.var_modifications.items():
            for idx, mod_node in enumerate(modifications):
                if mod_node not in fn_node_ids:
                    continue
                next_mod = modifications[idx + 1] if idx + 1 < len(modifications) else None
                mod_pos = order.index(mod_node) if mod_node in order else None

                for pos, node_id in enumerate(order):
                    if mod_pos is not None and pos <= mod_pos:
                        continue
                    if node_id == mod_node:
                        continue

                    node = self.dag.nodes.get(node_id)
                    if node and self.node_uses_variable(node, var_name):
                        edges_to_add.append((var_name, mod_node, node_id))

                    if next_mod and node_id == next_mod:
                        break

        for var_name, source, target in edges_to_add:
            is_loop_back = "loop_incr" in source and "loop_exit" not in target
            edge = DAGEdge.data_flow(source, target, var_name)
            if is_loop_back:
                edge.is_loop_back = True
            self.dag.add_edge(edge)

    def node_uses_variable(self, node: DAGNode, var_name: str) -> bool:
        """Return True if a node's arguments reference the variable."""
        if isinstance(node, (ActionCallNode, FnCallNode)):
            for value in node.kwargs.values():
                if value == f"${var_name}":
                    return True

            for expr in node.kwarg_exprs.values():
                if self.expr_uses_var(expr, var_name):
                    return True

        return False

    @staticmethod
    def expr_uses_var(expr: ir.Expr, var_name: str) -> bool:
        """Return True if an expression tree references the variable name."""
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return False
            case "variable":
                return expr.variable.name == var_name
            case "binary_op":
                left = expr.binary_op.left
                right = expr.binary_op.right
                return DataFlowMixin.expr_uses_var(left, var_name) or DataFlowMixin.expr_uses_var(
                    right, var_name
                )
            case "unary_op":
                return DataFlowMixin.expr_uses_var(expr.unary_op.operand, var_name)
            case "list":
                return any(
                    DataFlowMixin.expr_uses_var(element, var_name)
                    for element in expr.list.elements
                )
            case "dict":
                return any(
                    (entry.HasField("key") and DataFlowMixin.expr_uses_var(entry.key, var_name))
                    or (
                        entry.HasField("value")
                        and DataFlowMixin.expr_uses_var(entry.value, var_name)
                    )
                    for entry in expr.dict.entries
                )
            case "index":
                return DataFlowMixin.expr_uses_var(
                    expr.index.object, var_name
                ) or DataFlowMixin.expr_uses_var(expr.index.index, var_name)
            case "dot":
                return DataFlowMixin.expr_uses_var(expr.dot.object, var_name)
            case "function_call":
                return any(
                    DataFlowMixin.expr_uses_var(arg, var_name) for arg in expr.function_call.args
                ) or any(
                    kw.HasField("value") and DataFlowMixin.expr_uses_var(kw.value, var_name)
                    for kw in expr.function_call.kwargs
                )
            case "action_call":
                return any(
                    kw.HasField("value") and DataFlowMixin.expr_uses_var(kw.value, var_name)
                    for kw in expr.action_call.kwargs
                )
            case "parallel_expr":
                for call in expr.parallel_expr.calls:
                    call_kind = call.WhichOneof("kind")
                    match call_kind:
                        case "action":
                            if any(
                                kw.HasField("value")
                                and DataFlowMixin.expr_uses_var(kw.value, var_name)
                                for kw in call.action.kwargs
                            ):
                                return True
                        case "function":
                            if any(
                                DataFlowMixin.expr_uses_var(arg, var_name)
                                for arg in call.function.args
                            ) or any(
                                kw.HasField("value")
                                and DataFlowMixin.expr_uses_var(kw.value, var_name)
                                for kw in call.function.kwargs
                            ):
                                return True
                        case None:
                            continue
                        case _:
                            assert_never(call_kind)
                return False
            case "spread_expr":
                if DataFlowMixin.expr_uses_var(expr.spread_expr.collection, var_name):
                    return True
                return any(
                    kw.HasField("value")
                    and DataFlowMixin.expr_uses_var(kw.value, var_name)
                    for kw in expr.spread_expr.action.kwargs
                )
            case None:
                return False
            case _:
                assert_never(kind)

    def get_execution_order_for_nodes(self, node_ids: Set[str]) -> List[str]:
        """Topologically order nodes using state-machine edges (ignoring loop backs)."""
        in_degree = {node_id: 0 for node_id in node_ids}
        adjacency = {node_id: [] for node_id in node_ids}

        for edge in self.dag.get_state_machine_edges():
            if edge.is_loop_back:
                continue
            if edge.source in node_ids and edge.target in node_ids:
                adjacency[edge.source].append(edge.target)
                in_degree[edge.target] = in_degree.get(edge.target, 0) + 1

        queue: List[str] = [node_id for node_id, degree in in_degree.items() if degree == 0]
        order: List[str] = []

        while queue:
            node_id = queue.pop()
            order.append(node_id)
            for neighbor in adjacency.get(node_id, []):
                in_degree[neighbor] = in_degree.get(neighbor, 0) - 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order
