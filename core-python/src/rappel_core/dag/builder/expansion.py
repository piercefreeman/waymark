"""Function expansion helpers."""

from __future__ import annotations

import copy
import uuid
from typing import Dict, List, Optional, Set

from ..models import DAG, DagConversionError, DAGEdge, EdgeType
from ..nodes import ActionCallNode, AggregatorNode, AssignmentNode, FnCallNode, InputNode


class ExpansionMixin:
    """Inline function calls and remap expansion edges."""

    def remap_exception_targets(self, dag: "DAG") -> None:
        """Redirect exception edges to expanded call entries and dedupe edges.

        After function expansion, exception edges may still point at a call
        prefix (the "virtual" target). We map those prefixes to the first
        node id in the expanded call subtree, then remove duplicate edges.
        """
        call_entry_map = self.build_call_entry_map(dag)
        for edge in [edge for edge in dag.edges if edge.exception_types is not None]:
            if edge.target in dag.nodes:
                continue
            mapped = call_entry_map.get(edge.target)
            if mapped is not None:
                edge.target = mapped

        seen: Set[str] = set()
        deduped: List[DAGEdge] = []
        for edge in dag.edges:
            key = (
                f"{edge.source}|{edge.target}|{edge.edge_type}|{edge.condition}|"
                f"{edge.exception_types}|{edge.guard_string}|{edge.is_loop_back}|{edge.variable}"
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(edge)
        dag.edges = deduped

    @staticmethod
    def build_call_entry_map(dag: "DAG") -> Dict[str, str]:
        """Return a mapping from call prefixes to their first expanded node id.

        Example:
        - Expanded nodes: "foo:call_1:action_3", "foo:call_1:return_4"
        - Mapping entry: "foo:call_1" -> "foo:call_1:action_3"
        """
        mapping: Dict[str, str] = {}
        for node_id in dag.nodes:
            parts = node_id.split(":")
            for idx in range(1, len(parts)):
                prefix = ":".join(parts[:idx])
                existing = mapping.get(prefix)
                if existing is None or node_id < existing:
                    mapping[prefix] = node_id
        return mapping

    def expand_functions(self, unexpanded: "DAG", entry_fn: str) -> "DAG":
        """Inline the entry function and all reachable calls into a new DAG."""
        expanded = DAG()
        visited_calls: Set[str] = set()
        self.expand_function_recursive(unexpanded, entry_fn, expanded, visited_calls, None)
        return expanded

    def expand_function_recursive(
        self,
        unexpanded: "DAG",
        fn_name: str,
        target: "DAG",
        visited_calls: Set[str],
        id_prefix: Optional[str],
    ) -> Optional[tuple[str, str]]:
        """Inline a function into the target DAG and return (first, last) node ids.

        The expansion clones nodes from the callee, prefixes their ids with the
        call site, wires argument binding nodes before the callee entry, and
        rewrites edges and aggregation pointers.

        Example:
        - Caller node "call_2" invokes function "foo".
        - Expanded ids become "call_2:foo_input_1", "call_2:action_3", etc.
        - The return node's targets are rewritten to match the call assignment.
        """
        fn_nodes = unexpanded.get_nodes_for_function(fn_name)
        if not fn_nodes:
            return None

        input_node = next((node for node in fn_nodes.values() if node.is_input), None)
        is_entry_function = id_prefix is None

        fn_node_ids: Set[str] = set(fn_nodes.keys())
        ordered_nodes = self.get_topo_order(unexpanded, fn_node_ids)

        id_map: Dict[str, str] = {}
        first_real_node: Optional[str] = None
        last_real_node: Optional[str] = None
        output_return_node: Optional[str] = None

        for old_id in ordered_nodes:
            node = unexpanded.nodes.get(old_id)
            if node is None:
                continue

            if not is_entry_function and node.is_input:
                continue

            if node.is_fn_call and node.called_function in self.function_defs:
                called_fn = node.called_function
                call_key = f"{fn_name}:{old_id}"
                if call_key in visited_calls:
                    raise DagConversionError(
                        f"Recursive function call detected: {fn_name} -> {called_fn}"
                    )
                visited_calls.add(call_key)

                exception_edges = [
                    copy.deepcopy(edge)
                    for edge in unexpanded.edges
                    if edge.source == old_id and edge.exception_types is not None
                ]

                child_prefix = f"{id_prefix}:{old_id}" if id_prefix else old_id
                expansion = self.expand_function_recursive(
                    unexpanded,
                    called_fn,
                    target,
                    visited_calls,
                    child_prefix,
                )
                visited_calls.remove(call_key)

                if expansion is not None:
                    child_first, child_last = expansion

                    # Bind call arguments to callee input names before entry.
                    bind_ids: List[str] = []
                    fn_def = self.function_defs.get(called_fn)
                    if fn_def is not None and fn_def.io.inputs:
                        kwarg_exprs = node.kwarg_exprs or {}
                        for idx, input_name in enumerate(fn_def.io.inputs):
                            expr = kwarg_exprs.get(input_name)
                            if expr is None:
                                continue
                            bind_id = f"{child_prefix}:bind_{input_name}_{idx}"
                            bind_node = AssignmentNode(
                                id=bind_id,
                                targets=[input_name],
                                assign_expr=expr,
                                function_name=called_fn,
                            )
                            target.add_node(bind_node)
                            bind_ids.append(bind_id)

                    if bind_ids:
                        # Chain bindings in order, then connect to callee entry.
                        for idx in range(1, len(bind_ids)):
                            prev = bind_ids[idx - 1]
                            nxt = bind_ids[idx]
                            target.add_edge(DAGEdge.state_machine(prev, nxt))
                        target.add_edge(DAGEdge.state_machine(bind_ids[-1], child_first))

                    call_entry = bind_ids[0] if bind_ids else child_first
                    id_map[old_id] = call_entry
                    id_map[f"{old_id}_last"] = child_last

                    if node.is_spread:
                        # Propagate spread metadata to expanded action nodes.
                        for expanded_id, action_node in list(target.nodes.items()):
                            if (
                                expanded_id.startswith(child_prefix)
                                and action_node.node_type == "action_call"
                            ):
                                if isinstance(action_node, ActionCallNode):
                                    action_node.spread_loop_var = node.spread_loop_var
                                    action_node.spread_collection_expr = copy.deepcopy(
                                        node.spread_collection_expr
                                    )
                                    action_node.aggregates_to = node.aggregates_to

                    if exception_edges:
                        # Exception edges from the call become exception edges from each action.
                        expanded_action_ids = [
                            expanded_id
                            for expanded_id, expanded_node in target.nodes.items()
                            if expanded_id.startswith(child_prefix)
                            and expanded_node.node_type == "action_call"
                            and not expanded_node.is_fn_call
                        ]
                        for expanded_node_id in expanded_action_ids:
                            for exc_edge in exception_edges:
                                new_edge = copy.deepcopy(exc_edge)
                                new_edge.source = expanded_node_id
                                if exc_edge.target in fn_node_ids and id_prefix is not None:
                                    new_edge.target = f"{id_prefix}:{exc_edge.target}"
                                target.add_edge(new_edge)

                    fn_call_targets = (
                        list(node.targets)
                        if node.targets
                        else [node.target]
                        if node.target
                        else None
                    )
                    if fn_call_targets:
                        # Return nodes inside the callee bind back to the call targets.
                        expanded_return_ids = [
                            expanded_id
                            for expanded_id, expanded_node in target.nodes.items()
                            if expanded_id.startswith(child_prefix)
                            and expanded_node.node_type == "return"
                            and expanded_node.function_name == called_fn
                        ]
                        for return_id in expanded_return_ids:
                            return_node = target.nodes.get(return_id)
                            if return_node is not None:
                                return_node.targets = list(fn_call_targets)
                                return_node.target = fn_call_targets[0]

                    if first_real_node is None:
                        first_real_node = call_entry
                    last_real_node = child_last
                    continue

            new_id = f"{id_prefix}:{node.id}" if id_prefix else node.id
            cloned = copy.deepcopy(node)
            cloned.id = new_id
            cloned.node_uuid = uuid.uuid4()
            if id_prefix:
                # Keep aggregator wiring aligned with the prefixed node ids.
                if isinstance(cloned, (ActionCallNode, FnCallNode)):
                    if cloned.aggregates_to and cloned.aggregates_to in fn_node_ids:
                        cloned.aggregates_to = f"{id_prefix}:{cloned.aggregates_to}"
                if isinstance(cloned, AggregatorNode):
                    if cloned.aggregates_from and cloned.aggregates_from in fn_node_ids:
                        cloned.aggregates_from = f"{id_prefix}:{cloned.aggregates_from}"

            id_map[old_id] = new_id

            if first_real_node is None:
                first_real_node = new_id
            last_real_node = new_id

            if node.node_type in {"output", "return"}:
                output_return_node = new_id

            target.add_node(cloned)

        input_id = input_node.id if input_node else None

        for edge in unexpanded.edges:
            if edge.source not in fn_node_ids:
                continue

            if not is_entry_function and input_id and edge.source == input_id:
                continue

            if edge.source in id_map:
                mapped = id_map[edge.source]
                source_node = unexpanded.nodes.get(edge.source)
                if source_node and source_node.is_fn_call:
                    mapped = id_map.get(f"{edge.source}_last", mapped)
                new_source = mapped
            else:
                continue

            if edge.edge_type == EdgeType.DATA_FLOW:
                target_node = unexpanded.nodes.get(edge.target)
                if target_node and target_node.is_fn_call:
                    continue

            # Default to the original edge target if it was not expanded.
            new_target = id_map.get(edge.target, edge.target)

            cloned_edge = copy.deepcopy(edge)
            cloned_edge.source = new_source
            cloned_edge.target = new_target
            target.add_edge(cloned_edge)

        canonical_last = output_return_node or last_real_node
        if first_real_node and canonical_last:
            return (first_real_node, canonical_last)
        return None

    def get_topo_order(self, dag: "DAG", node_ids: Set[str]) -> List[str]:
        """Return a topological order for the given node subset.

        Only state-machine edges are considered and loop-back edges are ignored
        so loops do not collapse the ordering.
        """
        in_degree: Dict[str, int] = {node_id: 0 for node_id in node_ids}
        adjacency: Dict[str, List[str]] = {node_id: [] for node_id in node_ids}

        for edge in dag.edges:
            if edge.is_loop_back:
                continue
            if edge.edge_type == EdgeType.STATE_MACHINE:
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
