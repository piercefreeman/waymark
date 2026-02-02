"""DAG representation and IR -> DAG converter."""

from __future__ import annotations

import copy
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, List, NoReturn, Optional, Sequence, Set

from proto import ast_pb2 as ir

EXCEPTION_SCOPE_VAR = "__rappel_exception__"


class DagConversionError(Exception):
    """Raised when IR -> DAG conversion fails."""


def assert_never(value: NoReturn) -> NoReturn:
    raise AssertionError(f"Unhandled value: {value!r}")


class EdgeType(str, Enum):
    STATE_MACHINE = "state_machine"
    DATA_FLOW = "data_flow"


@dataclass
class DAGEdge:
    source: str
    target: str
    edge_type: EdgeType
    condition: Optional[str] = None
    variable: Optional[str] = None
    guard_expr: Optional[ir.Expr] = None
    is_else: bool = False
    exception_types: Optional[List[str]] = None
    exception_depth: Optional[int] = None
    is_loop_back: bool = False
    guard_string: Optional[str] = None

    @staticmethod
    def state_machine(source: str, target: str) -> "DAGEdge":
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.STATE_MACHINE,
        )

    @staticmethod
    def state_machine_with_condition(source: str, target: str, condition: str) -> "DAGEdge":
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.STATE_MACHINE,
            condition=condition,
        )

    @staticmethod
    def state_machine_with_guard(source: str, target: str, guard: ir.Expr) -> "DAGEdge":
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.STATE_MACHINE,
            condition="guarded",
            guard_expr=copy.deepcopy(guard),
        )

    @staticmethod
    def state_machine_else(source: str, target: str) -> "DAGEdge":
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.STATE_MACHINE,
            condition="else",
            is_else=True,
        )

    @staticmethod
    def state_machine_with_exception(
        source: str,
        target: str,
        exception_types: List[str],
    ) -> "DAGEdge":
        normalized = (
            []
            if len(exception_types) == 1 and exception_types[0] == "Exception"
            else list(exception_types)
        )
        condition = "except:*" if not normalized else f"except:{','.join(normalized)}"
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.STATE_MACHINE,
            condition=condition,
            exception_types=normalized,
        )

    @staticmethod
    def state_machine_success(source: str, target: str) -> "DAGEdge":
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.STATE_MACHINE,
            condition="success",
        )

    @staticmethod
    def data_flow(source: str, target: str, variable: str) -> "DAGEdge":
        return DAGEdge(
            source=source,
            target=target,
            edge_type=EdgeType.DATA_FLOW,
            variable=variable,
        )

    def with_loop_back(self, is_loop_back: bool) -> "DAGEdge":
        self.is_loop_back = is_loop_back
        return self

    def with_guard(self, guard: str) -> "DAGEdge":
        self.guard_string = guard
        return self


@dataclass
class DAGNode:
    id: str
    node_type: str
    label: str
    node_uuid: uuid.UUID = field(default_factory=uuid.uuid4)
    function_name: Optional[str] = None
    is_aggregator: bool = False
    aggregates_from: Optional[str] = None
    is_fn_call: bool = False
    called_function: Optional[str] = None
    is_input: bool = False
    is_output: bool = False
    io_vars: Optional[List[str]] = None
    action_name: Optional[str] = None
    module_name: Optional[str] = None
    kwargs: Optional[Dict[str, str]] = None
    kwarg_exprs: Optional[Dict[str, ir.Expr]] = None
    target: Optional[str] = None
    targets: Optional[List[str]] = None
    is_spread: bool = False
    spread_loop_var: Optional[str] = None
    spread_collection_expr: Optional[ir.Expr] = None
    aggregates_to: Optional[str] = None
    guard_expr: Optional[ir.Expr] = None
    assign_expr: Optional[ir.Expr] = None
    join_required_count: Optional[int] = None
    policies: List[ir.PolicyBracket] = field(default_factory=list)

    @staticmethod
    def new(node_id: str, node_type: str, label: str) -> "DAGNode":
        return DAGNode(id=node_id, node_type=node_type, label=label)

    def with_function_name(self, name: str) -> "DAGNode":
        self.function_name = name
        return self

    def with_action(self, action_name: str, module_name: Optional[str]) -> "DAGNode":
        self.action_name = action_name
        self.module_name = module_name
        return self

    def with_kwargs(self, kwargs: Dict[str, str]) -> "DAGNode":
        self.kwargs = dict(kwargs)
        return self

    def with_kwarg_exprs(self, kwarg_exprs: Dict[str, ir.Expr]) -> "DAGNode":
        self.kwarg_exprs = {name: copy.deepcopy(expr) for name, expr in kwarg_exprs.items()}
        return self

    def with_target(self, target: str) -> "DAGNode":
        self.target = target
        self.targets = [target]
        return self

    def with_targets(self, targets: Sequence[str]) -> "DAGNode":
        self.targets = list(targets)
        if targets:
            self.target = targets[0]
        return self

    def with_input(self, vars_list: Sequence[str]) -> "DAGNode":
        self.is_input = True
        self.io_vars = list(vars_list)
        return self

    def with_output(self, vars_list: Sequence[str]) -> "DAGNode":
        self.is_output = True
        self.io_vars = list(vars_list)
        return self

    def with_fn_call(self, called_function: str) -> "DAGNode":
        self.is_fn_call = True
        self.called_function = called_function
        return self

    def with_aggregator(self, source_id: str) -> "DAGNode":
        self.is_aggregator = True
        self.aggregates_from = source_id
        return self

    def with_guard(self, guard_expr: ir.Expr) -> "DAGNode":
        self.guard_expr = copy.deepcopy(guard_expr)
        return self

    def with_assign_expr(self, assign_expr: ir.Expr) -> "DAGNode":
        self.assign_expr = copy.deepcopy(assign_expr)
        return self

    def with_policies(self, policies: Sequence[ir.PolicyBracket]) -> "DAGNode":
        self.policies = list(policies)
        return self

    def with_spread(self, loop_var: str, collection_expr: ir.Expr) -> "DAGNode":
        self.is_spread = True
        self.spread_loop_var = loop_var
        self.spread_collection_expr = copy.deepcopy(collection_expr)
        return self

    def with_aggregates_to(self, aggregator_id: str) -> "DAGNode":
        self.aggregates_to = aggregator_id
        return self

    def with_join_required_count(self, count: int) -> "DAGNode":
        self.join_required_count = count
        return self


@dataclass
class DAG:
    nodes: Dict[str, DAGNode] = field(default_factory=dict)
    edges: List[DAGEdge] = field(default_factory=list)
    entry_node: Optional[str] = None

    def add_node(self, node: DAGNode) -> None:
        if self.entry_node is None:
            self.entry_node = node.id
        self.nodes[node.id] = node

    def add_edge(self, edge: DAGEdge) -> None:
        self.edges.append(edge)

    def get_incoming_edges(self, node_id: str) -> List[DAGEdge]:
        return [edge for edge in self.edges if edge.target == node_id]

    def get_outgoing_edges(self, node_id: str) -> List[DAGEdge]:
        return [edge for edge in self.edges if edge.source == node_id]

    def get_state_machine_edges(self) -> List[DAGEdge]:
        return [edge for edge in self.edges if edge.edge_type == EdgeType.STATE_MACHINE]

    def get_data_flow_edges(self) -> List[DAGEdge]:
        return [edge for edge in self.edges if edge.edge_type == EdgeType.DATA_FLOW]

    def get_functions(self) -> List[str]:
        functions: Set[str] = set()
        for node in self.nodes.values():
            if node.function_name is not None:
                functions.add(node.function_name)
        return sorted(functions)

    def get_nodes_for_function(self, function_name: str) -> Dict[str, DAGNode]:
        return {
            node_id: node
            for node_id, node in self.nodes.items()
            if node.function_name == function_name
        }

    def get_edges_for_function(self, function_name: str) -> List[DAGEdge]:
        fn_nodes = set(self.get_nodes_for_function(function_name).keys())
        return [edge for edge in self.edges if edge.source in fn_nodes and edge.target in fn_nodes]


@dataclass
class ConvertedSubgraph:
    entry: Optional[str]
    exits: List[str]
    nodes: List[str]
    is_noop: bool

    @staticmethod
    def noop() -> "ConvertedSubgraph":
        return ConvertedSubgraph(entry=None, exits=[], nodes=[], is_noop=True)


class DAGConverter:
    def __init__(self) -> None:
        self.dag = DAG()
        self.node_counter = 0
        self.current_function: Optional[str] = None
        self.function_defs: Dict[str, ir.FunctionDef] = {}
        self.current_scope_vars: Dict[str, str] = {}
        self.var_modifications: Dict[str, List[str]] = {}
        self.loop_exit_stack: List[str] = []
        self.loop_incr_stack: List[str] = []
        self.try_depth = 0

    def convert(self, program: ir.Program) -> DAG:
        unexpanded = self.convert_with_pointers(program)

        entry_fn = None
        for func in program.functions:
            if func.name == "main":
                entry_fn = func.name
                break
        if entry_fn is None:
            for func in program.functions:
                if not func.name.startswith("__"):
                    entry_fn = func.name
                    break
        if entry_fn is None and program.functions:
            entry_fn = program.functions[0].name
        if entry_fn is None:
            entry_fn = "main"

        dag = self.expand_functions(unexpanded, entry_fn)
        self.remap_exception_targets(dag)
        self.add_global_data_flow_edges(dag)
        validate_dag(dag)
        return dag

    def convert_with_pointers(self, program: ir.Program) -> DAG:
        self.dag = DAG()
        self.node_counter = 0
        self.function_defs.clear()

        for func in program.functions:
            self.function_defs[func.name] = copy.deepcopy(func)

        for func in program.functions:
            self.convert_function(func)

        dag = self.dag
        self.dag = DAG()
        return dag

    def remap_exception_targets(self, dag: DAG) -> None:
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
    def build_call_entry_map(dag: DAG) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for node_id in dag.nodes:
            parts = node_id.split(":")
            for idx in range(1, len(parts)):
                prefix = ":".join(parts[:idx])
                existing = mapping.get(prefix)
                if existing is None or node_id < existing:
                    mapping[prefix] = node_id
        return mapping

    def expand_functions(self, unexpanded: DAG, entry_fn: str) -> DAG:
        expanded = DAG()
        visited_calls: Set[str] = set()
        self.expand_function_recursive(unexpanded, entry_fn, expanded, visited_calls, None)
        return expanded

    def expand_function_recursive(
        self,
        unexpanded: DAG,
        fn_name: str,
        target: DAG,
        visited_calls: Set[str],
        id_prefix: Optional[str],
    ) -> Optional[tuple[str, str]]:
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

                    bind_ids: List[str] = []
                    fn_def = self.function_defs.get(called_fn)
                    if fn_def is not None and fn_def.io.inputs:
                        kwarg_exprs = node.kwarg_exprs or {}
                        for idx, input_name in enumerate(fn_def.io.inputs):
                            expr = kwarg_exprs.get(input_name)
                            if expr is None:
                                continue
                            bind_id = f"{child_prefix}:bind_{input_name}_{idx}"
                            label = f"{input_name} = ..."
                            bind_node = (
                                DAGNode.new(bind_id, "assignment", label)
                                .with_target(input_name)
                                .with_assign_expr(expr)
                                .with_function_name(called_fn)
                            )
                            target.add_node(bind_node)
                            bind_ids.append(bind_id)

                    if bind_ids:
                        for idx in range(1, len(bind_ids)):
                            prev = bind_ids[idx - 1]
                            nxt = bind_ids[idx]
                            target.add_edge(DAGEdge.state_machine(prev, nxt))
                        target.add_edge(DAGEdge.state_machine(bind_ids[-1], child_first))

                    call_entry = bind_ids[0] if bind_ids else child_first
                    id_map[old_id] = call_entry
                    id_map[f"{old_id}_last"] = child_last

                    if node.is_spread:
                        for expanded_id, action_node in list(target.nodes.items()):
                            if (
                                expanded_id.startswith(child_prefix)
                                and action_node.node_type == "action_call"
                            ):
                                action_node.is_spread = True
                                action_node.spread_loop_var = node.spread_loop_var
                                action_node.spread_collection_expr = copy.deepcopy(
                                    node.spread_collection_expr
                                )
                                action_node.aggregates_to = node.aggregates_to

                    if exception_edges:
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
                if cloned.aggregates_to and cloned.aggregates_to in fn_node_ids:
                    cloned.aggregates_to = f"{id_prefix}:{cloned.aggregates_to}"
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

            new_target = id_map.get(edge.target, edge.target)

            cloned_edge = copy.deepcopy(edge)
            cloned_edge.source = new_source
            cloned_edge.target = new_target
            target.add_edge(cloned_edge)

        canonical_last = output_return_node or last_real_node
        if first_real_node and canonical_last:
            return (first_real_node, canonical_last)
        return None

    def get_topo_order(self, dag: DAG, node_ids: Set[str]) -> List[str]:
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

    def add_global_data_flow_edges(self, dag: DAG) -> None:
        existing_data_flow = [
            copy.deepcopy(edge) for edge in dag.edges if edge.edge_type == EdgeType.DATA_FLOW
        ]
        dag.edges = [edge for edge in dag.edges if edge.edge_type != EdgeType.DATA_FLOW]

        node_ids: Set[str] = set(dag.nodes.keys())
        in_degree: Dict[str, int] = {node_id: 0 for node_id in node_ids}
        adjacency: Dict[str, List[str]] = {node_id: [] for node_id in node_ids}

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

        loop_back_edges = [
            copy.deepcopy(edge)
            for edge in dag.edges
            if edge.edge_type == EdgeType.STATE_MACHINE and edge.is_loop_back
        ]

        var_modifications: Dict[str, List[str]] = {}
        for node_id in order:
            node = dag.nodes.get(node_id)
            if node is None:
                continue
            if node.node_type == "join":
                continue
            if node.is_input and node.io_vars:
                for input_name in node.io_vars:
                    var_modifications.setdefault(input_name, []).append(node_id)
            if node.target:
                if not (node.node_type == "return" and node.target in var_modifications):
                    var_modifications.setdefault(node.target, []).append(node_id)
            if node.targets:
                for target in node.targets:
                    if node.node_type == "return" and target in var_modifications:
                        continue
                    var_modifications.setdefault(target, []).append(node_id)

        var_modifications_clone = {k: list(v) for k, v in var_modifications.items()}

        node_guard_exprs: Dict[str, List[ir.Expr]] = {}
        for edge in dag.edges:
            if edge.guard_expr is not None:
                node_guard_exprs.setdefault(edge.source, []).append(edge.guard_expr)

        def uses_var(node: DAGNode, var_name: str) -> bool:
            if node.kwargs:
                for value in node.kwargs.values():
                    if value == f"${var_name}":
                        return True

            if node.assign_expr and self.expr_uses_var(node.assign_expr, var_name):
                return True

            for guard in node_guard_exprs.get(node.id, []):
                if self.expr_uses_var(guard, var_name):
                    return True

            if node.kwarg_exprs:
                for expr in node.kwarg_exprs.values():
                    if self.expr_uses_var(expr, var_name):
                        return True

            if node.spread_collection_expr and self.expr_uses_var(
                node.spread_collection_expr, var_name
            ):
                return True

            return False

        seen_edges: Set[tuple[str, str, Optional[str]]] = {
            (edge.source, edge.target, edge.variable) for edge in existing_data_flow
        }

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

                self_key = (mod_node, mod_node, var_name)
                if self_key not in seen_edges:
                    seen_edges.add(self_key)
                    dag.edges.append(DAGEdge.data_flow(mod_node, mod_node, var_name))

        for edge in loop_back_edges:
            source_node = dag.nodes.get(edge.source)
            if source_node is None:
                continue
            defined_vars: List[str] = []
            if source_node.target:
                defined_vars.append(source_node.target)
            if source_node.targets:
                defined_vars.extend(source_node.targets)

            for var in defined_vars:
                dag.edges.append(DAGEdge.data_flow(edge.source, edge.target, var))

        for node in dag.nodes.values():
            if node.node_type != "join":
                continue
            if not (node.label.startswith("end for ") or node.label.startswith("end while ")):
                continue
            if not node.targets:
                continue
            for var in node.targets:
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

    def convert_function(self, fn_def: ir.FunctionDef) -> None:
        self.current_function = fn_def.name
        self.current_scope_vars.clear()
        self.var_modifications.clear()

        input_id = self.next_id(f"{fn_def.name}_input")
        input_label = (
            "input: []" if not fn_def.io.inputs else f"input: [{', '.join(fn_def.io.inputs)}]"
        )

        input_node = (
            DAGNode.new(input_id, "input", input_label)
            .with_function_name(fn_def.name)
            .with_input(fn_def.io.inputs)
        )
        self.dag.add_node(input_node)

        for var in fn_def.io.inputs:
            self.track_var_definition(var, input_id)

        frontier = [input_id]
        for stmt in fn_def.body.statements:
            converted = self.convert_statement(stmt)
            if converted.is_noop:
                continue
            if converted.entry:
                for prev in frontier:
                    self.dag.add_edge(DAGEdge.state_machine(prev, converted.entry))
            frontier = converted.exits

        output_id = self.next_id(f"{fn_def.name}_output")
        output_label = f"output: [{', '.join(fn_def.io.outputs)}]"

        output_node = (
            DAGNode.new(output_id, "output", output_label)
            .with_function_name(fn_def.name)
            .with_output(fn_def.io.outputs)
            .with_join_required_count(1)
        )
        self.dag.add_node(output_node)

        for prev in frontier:
            self.dag.add_edge(DAGEdge.state_machine(prev, output_id))

        return_nodes = [
            node_id
            for node_id, node in self.dag.nodes.items()
            if node.node_type == "return" and node.function_name == fn_def.name
        ]

        for return_id in return_nodes:
            already_connected = any(
                edge.source == return_id and edge.target == output_id for edge in self.dag.edges
            )
            if not already_connected:
                self.dag.add_edge(DAGEdge.state_machine(return_id, output_id))

        self.add_data_flow_edges_for_function(fn_def.name)
        self.current_function = None

    def next_id(self, prefix: str) -> str:
        self.node_counter += 1
        return f"{prefix}_{self.node_counter}"

    @staticmethod
    def build_loop_guard(loop_i_var: str, collection: Optional[ir.Expr]) -> Optional[ir.Expr]:
        if collection is None:
            return None
        return ir.Expr(
            binary_op=ir.BinaryOp(
                left=ir.Expr(variable=ir.Variable(name=loop_i_var)),
                op=ir.BinaryOperator.BINARY_OP_LT,
                right=ir.Expr(
                    function_call=ir.FunctionCall(
                        name="len",
                        args=[],
                        kwargs=[ir.Kwarg(name="items", value=copy.deepcopy(collection))],
                        global_function=ir.GlobalFunction.GLOBAL_FUNCTION_LEN,
                    )
                ),
            )
        )

    def convert_block(self, block: ir.Block) -> ConvertedSubgraph:
        nodes: List[str] = []
        entry: Optional[str] = None
        frontier: Optional[List[str]] = None

        for stmt in block.statements:
            converted = self.convert_statement(stmt)
            nodes.extend(converted.nodes)

            if converted.is_noop:
                continue

            if entry is None:
                entry = converted.entry

            if frontier is not None and converted.entry is not None:
                for prev in frontier:
                    self.dag.add_edge(DAGEdge.state_machine(prev, converted.entry))

            frontier = converted.exits

        if entry is None:
            return ConvertedSubgraph.noop()

        return ConvertedSubgraph(
            entry=entry,
            exits=frontier or [],
            nodes=nodes,
            is_noop=False,
        )

    def convert_statement(self, stmt: ir.Statement) -> ConvertedSubgraph:
        kind = stmt.WhichOneof("kind")
        match kind:
            case "assignment":
                node_ids = self.convert_assignment(stmt.assignment)
            case "action_call":
                node_ids = self.convert_action_call_with_targets(stmt.action_call, [])
            case "spread_action":
                node_ids = self.convert_spread_action(stmt.spread_action)
            case "parallel_block":
                node_ids = self.convert_parallel_block(stmt.parallel_block)
            case "for_loop":
                return self.convert_for_loop(stmt.for_loop)
            case "while_loop":
                return self.convert_while_loop(stmt.while_loop)
            case "conditional":
                return self.convert_conditional(stmt.conditional)
            case "try_except":
                return self.convert_try_except(stmt.try_except)
            case "return_stmt":
                node_ids = self.convert_return(stmt.return_stmt)
                return ConvertedSubgraph(
                    entry=node_ids[0] if node_ids else None,
                    exits=[],
                    nodes=node_ids,
                    is_noop=False,
                )
            case "break_stmt":
                return self.convert_break()
            case "continue_stmt":
                return self.convert_continue()
            case "expr_stmt":
                node_ids = self.convert_expr_statement(stmt.expr_stmt)
            case None:
                return ConvertedSubgraph.noop()
            case _:
                assert_never(kind)

        if not node_ids:
            return ConvertedSubgraph.noop()

        return ConvertedSubgraph(
            entry=node_ids[0],
            exits=[node_ids[-1]],
            nodes=node_ids,
            is_noop=False,
        )

    def convert_assignment(self, assign: ir.Assignment) -> List[str]:
        value = assign.value
        if value is None:
            return []

        targets = list(assign.targets)
        kind = value.WhichOneof("kind")
        match kind:
            case "function_call":
                target = targets[0] if targets else "_"
                return self.convert_fn_call_assignment(target, targets, value.function_call)
            case "action_call":
                return self.convert_action_call_with_targets(value.action_call, targets)
            case "parallel_expr":
                return self.convert_parallel_expr(value.parallel_expr, targets)
            case "spread_expr":
                return self.convert_spread_expr(value.spread_expr, targets)
            case (
                "literal"
                | "variable"
                | "binary_op"
                | "unary_op"
                | "list"
                | "dict"
                | "index"
                | "dot"
            ):
                node_id = self.next_id("assign")
                target = targets[0] if targets else "_"
                label = f"{', '.join(targets)} = ..." if len(targets) > 1 else f"{target} = ..."
                node = (
                    DAGNode.new(node_id, "assignment", label)
                    .with_targets(targets)
                    .with_assign_expr(value)
                )
                if self.current_function:
                    node = node.with_function_name(self.current_function)
                self.dag.add_node(node)
                for t in targets:
                    self.track_var_definition(t, node_id)
                return [node_id]
            case None:
                return []
            case _:
                assert_never(kind)

    def convert_fn_call_assignment(
        self,
        target: str,
        targets: Sequence[str],
        call: ir.FunctionCall,
    ) -> List[str]:
        node_id = self.next_id("fn_call")
        label = (
            f"{call.name}() -> {', '.join(targets)}"
            if len(targets) > 1
            else f"{call.name}() -> {target}"
        )

        kwargs, kwarg_exprs = self.extract_fn_call_args(call)
        call_expr = ir.Expr(function_call=copy.deepcopy(call))
        node = (
            DAGNode.new(node_id, "fn_call", label)
            .with_fn_call(call.name)
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs)
            .with_targets(targets)
            .with_assign_expr(call_expr)
        )
        if self.current_function:
            node = node.with_function_name(self.current_function)
        self.dag.add_node(node)

        for t in targets:
            self.track_var_definition(t, node_id)

        return [node_id]

    def convert_action_call_with_targets(
        self,
        action: ir.ActionCall,
        targets: Sequence[str],
    ) -> List[str]:
        node_id = self.next_id("action")
        if not targets:
            label = f"@{action.action_name}()"
        elif len(targets) == 1:
            label = f"@{action.action_name}() -> {targets[0]}"
        else:
            label = f"@{action.action_name}() -> ({', '.join(targets)})"

        kwargs = self.extract_kwargs(action.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
        module_name = action.module_name if action.HasField("module_name") else None

        node = (
            DAGNode.new(node_id, "action_call", label)
            .with_action(action.action_name, module_name)
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs)
            .with_policies(action.policies)
        )

        if targets:
            node = node.with_targets(targets)

        if self.current_function:
            node = node.with_function_name(self.current_function)
        self.dag.add_node(node)

        for t in targets:
            self.track_var_definition(t, node_id)

        return [node_id]

    def extract_kwargs(self, kwargs: Iterable[ir.Kwarg]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for kwarg in kwargs:
            if kwarg.HasField("value"):
                result[kwarg.name] = self.expr_to_string(kwarg.value)
        return result

    def extract_kwarg_exprs(self, kwargs: Iterable[ir.Kwarg]) -> Dict[str, ir.Expr]:
        result: Dict[str, ir.Expr] = {}
        for kwarg in kwargs:
            if kwarg.HasField("value"):
                result[kwarg.name] = copy.deepcopy(kwarg.value)
        return result

    def extract_fn_call_args(
        self, call: ir.FunctionCall
    ) -> tuple[Dict[str, str], Dict[str, ir.Expr]]:
        kwargs = self.extract_kwargs(call.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(call.kwargs)

        input_names: Optional[Sequence[str]] = None
        fn_def = self.function_defs.get(call.name)
        if fn_def is not None:
            input_names = fn_def.io.inputs

        if input_names is not None:
            for idx, arg in enumerate(call.args):
                if idx >= len(input_names):
                    break
                param_name = input_names[idx]
                if param_name not in kwargs:
                    kwargs[param_name] = self.expr_to_string(arg)
                if param_name not in kwarg_exprs:
                    kwarg_exprs[param_name] = copy.deepcopy(arg)

        return kwargs, kwarg_exprs

    def expr_to_string(self, expr: ir.Expr) -> str:
        kind = expr.WhichOneof("kind")
        match kind:
            case "variable":
                return f"${expr.variable.name}"
            case "literal":
                return self.literal_to_string(expr.literal)
            case "list":
                items = [self.expr_to_string(item) for item in expr.list.elements]
                return f"[{', '.join(items)}]"
            case "dict":
                entries = []
                for entry in expr.dict.entries:
                    key = self.expr_to_string(entry.key) if entry.HasField("key") else ""
                    val = self.expr_to_string(entry.value) if entry.HasField("value") else ""
                    entries.append(f"{key}: {val}")
                return f"{{{', '.join(entries)}}}"
            case "function_call":
                parts = [self.expr_to_string(arg) for arg in expr.function_call.args]
                for kw in expr.function_call.kwargs:
                    if kw.HasField("value"):
                        parts.append(f"{kw.name}={self.expr_to_string(kw.value)}")
                return f"{expr.function_call.name}({', '.join(parts)})"
            case (
                "binary_op"
                | "unary_op"
                | "index"
                | "dot"
                | "action_call"
                | "parallel_expr"
                | "spread_expr"
            ):
                return "null"
            case None:
                return "null"
            case _:
                assert_never(kind)

    def literal_to_string(self, lit: ir.Literal) -> str:
        kind = lit.WhichOneof("value")
        match kind:
            case "int_value":
                return str(lit.int_value)
            case "float_value":
                return str(lit.float_value)
            case "string_value":
                return f'"{lit.string_value}"'
            case "bool_value":
                return str(lit.bool_value).lower()
            case "is_none":
                return "null" if lit.is_none else "null"
            case None:
                return "null"
            case _:
                assert_never(kind)

    def convert_spread_action(self, spread: ir.SpreadAction) -> List[str]:
        return self.convert_spread_action_with_targets(spread, [])

    def convert_spread_expr(self, spread: ir.SpreadExpr, targets: Sequence[str]) -> List[str]:
        action = spread.action
        action_id = self.next_id("spread_action")
        action_label = f"@{action.action_name}() [spread over {spread.loop_var}]"

        kwargs = self.extract_kwargs(action.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
        collection_expr = (
            copy.deepcopy(spread.collection) if spread.HasField("collection") else ir.Expr()
        )
        spread_result_var = "_spread_result"

        agg_id = self.next_id("aggregator")
        module_name = action.module_name if action.HasField("module_name") else None

        action_node = (
            DAGNode.new(action_id, "action_call", action_label)
            .with_action(action.action_name, module_name)
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs)
            .with_spread(spread.loop_var, collection_expr)
            .with_target(spread_result_var)
            .with_aggregates_to(agg_id)
        )
        if self.current_function:
            action_node = action_node.with_function_name(self.current_function)
        self.dag.add_node(action_node)

        target_label = (
            f"aggregate -> {targets[0]}"
            if len(targets) == 1
            else f"aggregate -> ({', '.join(targets)})"
            if targets
            else "aggregate"
        )

        agg_node = DAGNode.new(agg_id, "aggregator", target_label).with_aggregator(action_id)
        if targets:
            agg_node = agg_node.with_targets(targets)
        if self.current_function:
            agg_node = agg_node.with_function_name(self.current_function)
        self.dag.add_node(agg_node)

        self.dag.add_edge(DAGEdge.state_machine(action_id, agg_id))
        self.dag.add_edge(DAGEdge.data_flow(action_id, agg_id, spread_result_var))

        for t in targets:
            self.track_var_definition(t, agg_id)

        return [action_id, agg_id]

    def convert_spread_action_with_targets(
        self, spread: ir.SpreadAction, targets: Sequence[str]
    ) -> List[str]:
        action = spread.action
        action_id = self.next_id("spread_action")
        action_label = f"@{action.action_name}() [spread over {spread.loop_var}]"

        kwargs = self.extract_kwargs(action.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
        collection_expr = (
            copy.deepcopy(spread.collection) if spread.HasField("collection") else ir.Expr()
        )
        spread_result_var = "_spread_result"

        agg_id = self.next_id("aggregator")
        module_name = action.module_name if action.HasField("module_name") else None

        action_node = (
            DAGNode.new(action_id, "action_call", action_label)
            .with_action(action.action_name, module_name)
            .with_kwargs(kwargs)
            .with_kwarg_exprs(kwarg_exprs)
            .with_spread(spread.loop_var, collection_expr)
            .with_target(spread_result_var)
            .with_aggregates_to(agg_id)
        )
        if self.current_function:
            action_node = action_node.with_function_name(self.current_function)
        self.dag.add_node(action_node)

        target_label = (
            f"aggregate -> {targets[0]}"
            if len(targets) == 1
            else f"aggregate -> ({', '.join(targets)})"
            if targets
            else "aggregate"
        )
        agg_node = DAGNode.new(agg_id, "aggregator", target_label).with_aggregator(action_id)
        if targets:
            agg_node = agg_node.with_targets(targets)
        if self.current_function:
            agg_node = agg_node.with_function_name(self.current_function)
        self.dag.add_node(agg_node)

        self.dag.add_edge(DAGEdge.state_machine(action_id, agg_id))
        self.dag.add_edge(DAGEdge.data_flow(action_id, agg_id, spread_result_var))

        for t in targets:
            self.track_var_definition(t, agg_id)

        return [action_id, agg_id]

    def convert_parallel_block(self, parallel: ir.ParallelBlock) -> List[str]:
        return self.convert_parallel_block_with_targets(parallel.calls, [])

    def convert_parallel_expr(self, parallel: ir.ParallelExpr, targets: Sequence[str]) -> List[str]:
        return self.convert_parallel_block_with_targets(parallel.calls, targets)

    def convert_parallel_block_with_targets(
        self,
        calls: Sequence[ir.Call],
        targets: Sequence[str],
    ) -> List[str]:
        result_nodes: List[str] = []

        parallel_id = self.next_id("parallel")
        parallel_node = DAGNode.new(parallel_id, "parallel", "parallel")
        if self.current_function:
            parallel_node = parallel_node.with_function_name(self.current_function)
        self.dag.add_node(parallel_node)
        result_nodes.append(parallel_id)

        list_aggregate = len(targets) == 1
        agg_id = self.next_id("parallel_aggregator")

        has_fn_calls = any(call.WhichOneof("kind") == "function" for call in calls)

        call_node_ids: List[str] = []
        for idx, call in enumerate(calls):
            call_target = None if list_aggregate else (targets[idx] if idx < len(targets) else None)
            kind = call.WhichOneof("kind")
            if kind == "action":
                action = call.action
                call_id = self.next_id("parallel_action")
                label = (
                    f"@{action.action_name}() [{idx}] -> {call_target}"
                    if call_target
                    else f"@{action.action_name}() [{idx}]"
                )
                kwargs = self.extract_kwargs(action.kwargs)
                kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
                module_name = action.module_name if action.HasField("module_name") else None
                node = (
                    DAGNode.new(call_id, "action_call", label)
                    .with_action(action.action_name, module_name)
                    .with_kwargs(kwargs)
                    .with_kwarg_exprs(kwarg_exprs)
                )
                if call_target:
                    node = node.with_target(call_target)
                if list_aggregate:
                    node = node.with_aggregates_to(agg_id)
                if self.current_function:
                    node = node.with_function_name(self.current_function)
            elif kind == "function":
                func = call.function
                call_id = self.next_id("parallel_fn_call")
                label = (
                    f"{func.name}() [{idx}] -> {call_target}"
                    if call_target
                    else f"{func.name}() [{idx}]"
                )
                kwargs, kwarg_exprs = self.extract_fn_call_args(func)
                node = (
                    DAGNode.new(call_id, "fn_call", label)
                    .with_fn_call(func.name)
                    .with_kwargs(kwargs)
                    .with_kwarg_exprs(kwarg_exprs)
                )
                if call_target:
                    node = node.with_target(call_target)
                if list_aggregate:
                    node = node.with_aggregates_to(agg_id)
                if self.current_function:
                    node = node.with_function_name(self.current_function)
            elif kind is None:
                continue
            else:
                assert_never(kind)

            self.dag.add_node(node)

            if call_target:
                self.track_var_definition(call_target, call_id)

            call_node_ids.append(call_id)
            result_nodes.append(call_id)

            self.dag.add_edge(
                DAGEdge.state_machine_with_condition(parallel_id, call_id, f"parallel:{idx}")
            )

        aggregator_targets = [] if has_fn_calls else list(targets)
        target_label = (
            f"parallel_aggregate -> {aggregator_targets[0]}"
            if len(aggregator_targets) == 1
            else f"parallel_aggregate -> ({', '.join(aggregator_targets)})"
            if aggregator_targets
            else "parallel_aggregate"
        )

        agg_node = DAGNode.new(agg_id, "aggregator", target_label).with_aggregator(parallel_id)
        if aggregator_targets:
            agg_node = agg_node.with_targets(aggregator_targets)
        if self.current_function:
            agg_node = agg_node.with_function_name(self.current_function)
        self.dag.add_node(agg_node)
        result_nodes.append(agg_id)

        for call_id in call_node_ids:
            self.dag.add_edge(DAGEdge.state_machine(call_id, agg_id))

        return result_nodes

    def convert_for_loop(self, for_loop: ir.ForLoop) -> ConvertedSubgraph:
        nodes: List[str] = []

        loop_id = self.next_id("loop")
        loop_vars_str = ", ".join(for_loop.loop_vars)
        collection_expr = (
            copy.deepcopy(for_loop.iterable) if for_loop.HasField("iterable") else None
        )
        collection_str = self.expr_to_string(collection_expr) if collection_expr else ""

        loop_i_var = f"__loop_{loop_id}_i"
        continue_guard = self.build_loop_guard(loop_i_var, collection_expr)
        break_guard = (
            ir.Expr(
                unary_op=ir.UnaryOp(
                    op=ir.UnaryOperator.UNARY_OP_NOT,
                    operand=copy.deepcopy(continue_guard),
                )
            )
            if continue_guard is not None
            else None
        )

        init_id = self.next_id("loop_init")
        init_label = f"{loop_i_var} = 0"
        init_expr = ir.Expr(literal=ir.Literal(int_value=0))
        init_node = (
            DAGNode.new(init_id, "assignment", init_label)
            .with_target(loop_i_var)
            .with_assign_expr(init_expr)
        )
        if self.current_function:
            init_node = init_node.with_function_name(self.current_function)
        self.dag.add_node(init_node)
        self.track_var_definition(loop_i_var, init_id)
        nodes.append(init_id)

        cond_id = self.next_id("loop_cond")
        cond_label = f"for {loop_vars_str} in {collection_str}"
        cond_node = DAGNode.new(cond_id, "branch", cond_label).with_join_required_count(1)
        if self.current_function:
            cond_node = cond_node.with_function_name(self.current_function)
        self.dag.add_node(cond_node)
        nodes.append(cond_id)

        self.dag.add_edge(DAGEdge.state_machine(init_id, cond_id))

        extract_id = self.next_id("loop_extract")
        if collection_expr is None:
            raise DagConversionError(
                f"for-loop collection expression is None for loop '{loop_vars_str}'"
            )
        index_expr = ir.Expr(
            index=ir.IndexAccess(
                object=copy.deepcopy(collection_expr),
                index=ir.Expr(variable=ir.Variable(name=loop_i_var)),
            )
        )
        extract_label = f"{loop_vars_str} = {collection_str}[{loop_i_var}]"
        extract_node = (
            DAGNode.new(extract_id, "assignment", extract_label)
            .with_targets(for_loop.loop_vars)
            .with_assign_expr(index_expr)
            .with_join_required_count(1)
        )
        if self.current_function:
            extract_node = extract_node.with_function_name(self.current_function)
        self.dag.add_node(extract_node)
        nodes.append(extract_id)

        for loop_var in for_loop.loop_vars:
            self.track_var_definition(loop_var, extract_id)

        if continue_guard is not None:
            self.dag.add_edge(DAGEdge.state_machine_with_guard(cond_id, extract_id, continue_guard))
        else:
            self.dag.add_edge(DAGEdge.state_machine(cond_id, extract_id))

        exit_id = self.next_id("loop_exit")
        self.loop_exit_stack.append(exit_id)
        incr_id = self.next_id("loop_incr")
        self.loop_incr_stack.append(incr_id)

        body_targets: List[str] = []
        body_graph = (
            self.convert_block(for_loop.block_body)
            if for_loop.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        if for_loop.HasField("block_body"):
            self.collect_assigned_targets(for_loop.block_body.statements, body_targets)

        self.loop_incr_stack.pop()
        self.loop_exit_stack.pop()
        nodes.extend(body_graph.nodes)

        if body_graph.is_noop:
            pass
        elif body_graph.entry:
            self.dag.add_edge(DAGEdge.state_machine(extract_id, body_graph.entry))

        incr_label = f"{loop_i_var} = {loop_i_var} + 1"
        incr_expr = ir.Expr(
            binary_op=ir.BinaryOp(
                left=ir.Expr(variable=ir.Variable(name=loop_i_var)),
                op=ir.BinaryOperator.BINARY_OP_ADD,
                right=ir.Expr(literal=ir.Literal(int_value=1)),
            )
        )
        incr_node = (
            DAGNode.new(incr_id, "assignment", incr_label)
            .with_target(loop_i_var)
            .with_assign_expr(incr_expr)
            .with_join_required_count(1)
        )
        if self.current_function:
            incr_node = incr_node.with_function_name(self.current_function)
        self.dag.add_node(incr_node)
        self.track_var_definition(loop_i_var, incr_id)
        nodes.append(incr_id)

        if body_graph.is_noop:
            self.dag.add_edge(DAGEdge.state_machine(extract_id, incr_id))
        else:
            for exit_node in body_graph.exits:
                self.dag.add_edge(DAGEdge.state_machine(exit_node, incr_id))

        self.dag.add_edge(DAGEdge.state_machine(incr_id, cond_id).with_loop_back(True))

        exit_label = f"end for {loop_vars_str}"
        exit_node = DAGNode.new(exit_id, "join", exit_label).with_join_required_count(1)
        if self.current_function:
            exit_node = exit_node.with_function_name(self.current_function)
        if body_targets:
            exit_node.targets = list(body_targets)
            for target in body_targets:
                self.track_var_definition(target, exit_id)
        self.dag.add_node(exit_node)
        nodes.append(exit_id)

        if break_guard is not None:
            self.dag.add_edge(DAGEdge.state_machine_with_guard(cond_id, exit_id, break_guard))
        else:
            self.dag.add_edge(DAGEdge.state_machine(cond_id, exit_id))

        return ConvertedSubgraph(
            entry=init_id,
            exits=[exit_id],
            nodes=nodes,
            is_noop=False,
        )

    def convert_while_loop(self, while_loop: ir.WhileLoop) -> ConvertedSubgraph:
        nodes: List[str] = []

        if not while_loop.HasField("condition"):
            raise DagConversionError("while loop missing condition")
        condition = copy.deepcopy(while_loop.condition)
        condition_str = self.expr_to_string(condition)

        cond_id = self.next_id("loop_cond")
        cond_label = f"while {condition_str}"
        cond_node = DAGNode.new(cond_id, "branch", cond_label).with_join_required_count(1)
        if self.current_function:
            cond_node = cond_node.with_function_name(self.current_function)
        self.dag.add_node(cond_node)
        nodes.append(cond_id)

        continue_guard = copy.deepcopy(condition)
        break_guard = ir.Expr(
            unary_op=ir.UnaryOp(
                op=ir.UnaryOperator.UNARY_OP_NOT,
                operand=copy.deepcopy(condition),
            )
        )

        exit_id = self.next_id("loop_exit")
        self.loop_exit_stack.append(exit_id)
        continue_id = self.next_id("loop_continue")
        self.loop_incr_stack.append(continue_id)

        body_targets: List[str] = []
        body_graph = (
            self.convert_block(while_loop.block_body)
            if while_loop.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        if while_loop.HasField("block_body"):
            self.collect_assigned_targets(while_loop.block_body.statements, body_targets)

        self.loop_incr_stack.pop()
        self.loop_exit_stack.pop()
        nodes.extend(body_graph.nodes)

        continue_node = DAGNode.new(
            continue_id, "assignment", "loop_continue"
        ).with_join_required_count(1)
        if self.current_function:
            continue_node = continue_node.with_function_name(self.current_function)
        self.dag.add_node(continue_node)
        nodes.append(continue_id)

        if body_graph.is_noop:
            self.dag.add_edge(
                DAGEdge.state_machine_with_guard(cond_id, continue_id, continue_guard)
            )
        elif body_graph.entry:
            self.dag.add_edge(
                DAGEdge.state_machine_with_guard(cond_id, body_graph.entry, continue_guard)
            )

        if not body_graph.is_noop:
            for exit_node in body_graph.exits:
                self.dag.add_edge(DAGEdge.state_machine(exit_node, continue_id))

        self.dag.add_edge(DAGEdge.state_machine(continue_id, cond_id).with_loop_back(True))

        exit_label = f"end while {condition_str}"
        exit_node = DAGNode.new(exit_id, "join", exit_label).with_join_required_count(1)
        if self.current_function:
            exit_node = exit_node.with_function_name(self.current_function)
        if body_targets:
            exit_node.targets = list(body_targets)
            for target in body_targets:
                self.track_var_definition(target, exit_id)
        self.dag.add_node(exit_node)
        nodes.append(exit_id)

        self.dag.add_edge(DAGEdge.state_machine_with_guard(cond_id, exit_id, break_guard))

        return ConvertedSubgraph(
            entry=cond_id,
            exits=[exit_id],
            nodes=nodes,
            is_noop=False,
        )

    def convert_conditional(self, cond: ir.Conditional) -> ConvertedSubgraph:
        nodes: List[str] = []

        branch_id = self.next_id("branch")
        branch_node = DAGNode.new(branch_id, "branch", "branch")
        if self.current_function:
            branch_node = branch_node.with_function_name(self.current_function)
        self.dag.add_node(branch_node)
        nodes.append(branch_id)

        if not cond.HasField("if_branch"):
            raise DagConversionError("conditional missing if_branch")
        if_branch = cond.if_branch
        if not if_branch.HasField("condition"):
            raise DagConversionError("if branch missing guard expression")
        if_guard = copy.deepcopy(if_branch.condition)

        if_body_graph = (
            self.convert_block(if_branch.block_body)
            if if_branch.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        nodes.extend(if_body_graph.nodes)

        prior_guards = [if_guard]
        elif_graphs: List[tuple[ir.Expr, ConvertedSubgraph]] = []

        for elif_branch in cond.elif_branches:
            if not elif_branch.HasField("condition"):
                raise DagConversionError("elif branch missing guard expression")
            elif_cond = copy.deepcopy(elif_branch.condition)
            compound_guard = self.build_compound_guard(prior_guards, elif_cond)
            prior_guards.append(elif_cond)

            graph = (
                self.convert_block(elif_branch.block_body)
                if elif_branch.HasField("block_body")
                else ConvertedSubgraph.noop()
            )
            nodes.extend(graph.nodes)
            elif_graphs.append((compound_guard, graph))

        else_graph = (
            self.convert_block(cond.else_branch.block_body)
            if cond.HasField("else_branch") and cond.else_branch.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        nodes.extend(else_graph.nodes)

        join_needed = (
            if_body_graph.is_noop
            or bool(if_body_graph.exits)
            or any(graph.is_noop or graph.exits for _, graph in elif_graphs)
            or else_graph.is_noop
            or bool(else_graph.exits)
        )

        join_id: Optional[str] = None
        if join_needed:
            join_id = self.next_id("join")
            join_node = DAGNode.new(join_id, "join", "join").with_join_required_count(1)
            if self.current_function:
                join_node = join_node.with_function_name(self.current_function)
            self.dag.add_node(join_node)
            nodes.append(join_id)

        def connect_guarded_branch(
            guard: ir.Expr, graph: ConvertedSubgraph, join_target: Optional[str]
        ) -> None:
            if graph.is_noop:
                if join_target:
                    self.dag.add_edge(
                        DAGEdge.state_machine_with_guard(branch_id, join_target, guard)
                    )
                return

            if graph.entry:
                self.dag.add_edge(DAGEdge.state_machine_with_guard(branch_id, graph.entry, guard))

            if join_target:
                for exit_node in graph.exits:
                    self.dag.add_edge(DAGEdge.state_machine(exit_node, join_target))

        def connect_else_branch(graph: ConvertedSubgraph, join_target: Optional[str]) -> None:
            if graph.is_noop:
                if join_target:
                    self.dag.add_edge(DAGEdge.state_machine_else(branch_id, join_target))
                return

            if graph.entry:
                self.dag.add_edge(DAGEdge.state_machine_else(branch_id, graph.entry))

            if join_target:
                for exit_node in graph.exits:
                    self.dag.add_edge(DAGEdge.state_machine(exit_node, join_target))

        connect_guarded_branch(if_guard, if_body_graph, join_id)
        for guard, graph in elif_graphs:
            connect_guarded_branch(guard, graph, join_id)
        connect_else_branch(else_graph, join_id)

        return ConvertedSubgraph(
            entry=branch_id,
            exits=[join_id] if join_id else [],
            nodes=nodes,
            is_noop=False,
        )

    def build_compound_guard(
        self, prior_guards: Sequence[ir.Expr], current_condition: Optional[ir.Expr]
    ) -> ir.Expr:
        parts: List[ir.Expr] = []
        for guard in prior_guards:
            parts.append(
                ir.Expr(
                    unary_op=ir.UnaryOp(
                        op=ir.UnaryOperator.UNARY_OP_NOT,
                        operand=copy.deepcopy(guard),
                    )
                )
            )

        if current_condition is not None:
            parts.append(copy.deepcopy(current_condition))

        if not parts:
            raise DagConversionError(
                "build_compound_guard called with no prior conditions and no current condition"
            )
        if len(parts) == 1:
            return parts[0]

        result = parts.pop(0)
        for part in parts:
            result = ir.Expr(
                binary_op=ir.BinaryOp(
                    left=copy.deepcopy(result),
                    op=ir.BinaryOperator.BINARY_OP_AND,
                    right=copy.deepcopy(part),
                )
            )
        return result

    def convert_try_except(self, try_except: ir.TryExcept) -> ConvertedSubgraph:
        self.try_depth += 1
        current_depth = self.try_depth
        try:
            try_graph = (
                self.convert_block(try_except.try_block)
                if try_except.HasField("try_block")
                else ConvertedSubgraph.noop()
            )
            if try_graph.is_noop:
                return ConvertedSubgraph.noop()

            nodes = list(try_graph.nodes)
            handler_graphs: List[tuple[List[str], ConvertedSubgraph]] = []

            for handler in try_except.handlers:
                graph = (
                    self.convert_block(handler.block_body)
                    if handler.HasField("block_body")
                    else ConvertedSubgraph.noop()
                )
                if handler.exception_var:
                    graph = self.prepend_exception_binding(handler.exception_var, graph)
                nodes.extend(graph.nodes)
                handler_graphs.append((list(handler.exception_types), graph))

            join_needed = bool(try_graph.exits) or any(
                graph.is_noop or graph.exits for _, graph in handler_graphs
            )

            join_id: Optional[str] = None
            if join_needed:
                join_id = self.next_id("join")
                join_node = DAGNode.new(join_id, "join", "join").with_join_required_count(1)
                if self.current_function:
                    join_node = join_node.with_function_name(self.current_function)
                self.dag.add_node(join_node)
                nodes.append(join_id)

            if join_id:
                for try_exit in try_graph.exits:
                    self.dag.add_edge(DAGEdge.state_machine_success(try_exit, join_id))

            try_exception_sources = list(try_graph.nodes)
            for exception_types, handler_graph in handler_graphs:
                if handler_graph.is_noop:
                    if join_id:
                        for source in try_exception_sources:
                            edge = DAGEdge.state_machine_with_exception(
                                source, join_id, list(exception_types)
                            )
                            edge.exception_depth = current_depth
                            self.dag.add_edge(edge)
                    continue

                if handler_graph.entry:
                    for source in try_exception_sources:
                        edge = DAGEdge.state_machine_with_exception(
                            source, handler_graph.entry, list(exception_types)
                        )
                        edge.exception_depth = current_depth
                        self.dag.add_edge(edge)

                if join_id:
                    for handler_exit in handler_graph.exits:
                        self.dag.add_edge(DAGEdge.state_machine(handler_exit, join_id))

            return ConvertedSubgraph(
                entry=try_graph.entry,
                exits=[join_id] if join_id else [],
                nodes=nodes,
                is_noop=False,
            )
        finally:
            self.try_depth = max(0, self.try_depth - 1)

    def prepend_exception_binding(
        self, exception_var: str, graph: ConvertedSubgraph
    ) -> ConvertedSubgraph:
        binding_id = self.next_id("exc_bind")
        label = f"{exception_var} = {EXCEPTION_SCOPE_VAR}"
        assign_expr = ir.Expr(variable=ir.Variable(name=EXCEPTION_SCOPE_VAR))
        node = (
            DAGNode.new(binding_id, "assignment", label)
            .with_target(exception_var)
            .with_assign_expr(assign_expr)
        )
        if self.current_function:
            node = node.with_function_name(self.current_function)
        self.dag.add_node(node)
        self.track_var_definition(exception_var, binding_id)

        if graph.entry:
            self.dag.add_edge(DAGEdge.state_machine(binding_id, graph.entry))

        nodes = [binding_id, *graph.nodes]
        exits = [binding_id] if graph.is_noop else list(graph.exits)

        return ConvertedSubgraph(entry=binding_id, exits=exits, nodes=nodes, is_noop=False)

    def convert_return(self, ret: ir.ReturnStmt) -> List[str]:
        node_id = self.next_id("return")
        node = DAGNode.new(node_id, "return", "return").with_join_required_count(1)

        if ret.HasField("value"):
            node.assign_expr = copy.deepcopy(ret.value)
            node.target = "result"

        if self.current_function:
            node = node.with_function_name(self.current_function)
        has_target = node.target is not None
        self.dag.add_node(node)

        if has_target:
            self.track_var_definition("result", node_id)

        return [node_id]

    def convert_break(self) -> ConvertedSubgraph:
        if not self.loop_exit_stack:
            raise DagConversionError("break statement outside of loop")
        loop_exit = self.loop_exit_stack[-1]

        node_id = self.next_id("break")
        node = DAGNode.new(node_id, "break", "break")
        if self.current_function:
            node = node.with_function_name(self.current_function)
        self.dag.add_node(node)

        self.dag.add_edge(DAGEdge.state_machine(node_id, loop_exit))

        return ConvertedSubgraph(entry=node_id, exits=[], nodes=[node_id], is_noop=False)

    def convert_continue(self) -> ConvertedSubgraph:
        if not self.loop_incr_stack:
            raise DagConversionError("continue statement outside of loop")
        loop_incr = self.loop_incr_stack[-1]

        node_id = self.next_id("continue")
        node = DAGNode.new(node_id, "continue", "continue")
        if self.current_function:
            node = node.with_function_name(self.current_function)
        self.dag.add_node(node)

        self.dag.add_edge(DAGEdge.state_machine(node_id, loop_incr))

        return ConvertedSubgraph(entry=node_id, exits=[], nodes=[node_id], is_noop=False)

    def convert_expr_statement(self, expr_stmt: ir.ExprStmt) -> List[str]:
        if not expr_stmt.HasField("expr"):
            return []

        expr = expr_stmt.expr
        kind = expr.WhichOneof("kind")
        match kind:
            case "action_call":
                return self.convert_action_call_with_targets(expr.action_call, [])
            case "function_call":
                node_id = self.next_id("fn_call")
                kwargs, kwarg_exprs = self.extract_fn_call_args(expr.function_call)
                node = (
                    DAGNode.new(node_id, "fn_call", f"{expr.function_call.name}()")
                    .with_fn_call(expr.function_call.name)
                    .with_kwargs(kwargs)
                    .with_kwarg_exprs(kwarg_exprs)
                )
                if self.current_function:
                    node = node.with_function_name(self.current_function)
                self.dag.add_node(node)
                return [node_id]
            case (
                "literal"
                | "variable"
                | "binary_op"
                | "unary_op"
                | "list"
                | "dict"
                | "index"
                | "dot"
                | "parallel_expr"
                | "spread_expr"
            ):
                node_id = self.next_id("expr")
                node = DAGNode.new(node_id, "expression", "expr")
                if self.current_function:
                    node = node.with_function_name(self.current_function)
                self.dag.add_node(node)
                return [node_id]
            case None:
                return []
            case _:
                assert_never(kind)

    def track_var_definition(self, var_name: str, node_id: str) -> None:
        self.current_scope_vars[var_name] = node_id
        self.var_modifications.setdefault(var_name, []).append(node_id)

    @staticmethod
    def push_unique_target(targets: List[str], target: str) -> None:
        if target not in targets:
            targets.append(target)

    @classmethod
    def collect_assigned_targets(
        cls, statements: Iterable[ir.Statement], targets: List[str]
    ) -> None:
        for stmt in statements:
            kind = stmt.WhichOneof("kind")
            match kind:
                case "assignment":
                    for target in stmt.assignment.targets:
                        cls.push_unique_target(targets, target)
                case "conditional":
                    if stmt.conditional.HasField(
                        "if_branch"
                    ) and stmt.conditional.if_branch.HasField("block_body"):
                        cls.collect_assigned_targets(
                            stmt.conditional.if_branch.block_body.statements, targets
                        )
                    for elif_branch in stmt.conditional.elif_branches:
                        if elif_branch.HasField("block_body"):
                            cls.collect_assigned_targets(elif_branch.block_body.statements, targets)
                    if stmt.conditional.HasField(
                        "else_branch"
                    ) and stmt.conditional.else_branch.HasField("block_body"):
                        cls.collect_assigned_targets(
                            stmt.conditional.else_branch.block_body.statements, targets
                        )
                case "for_loop":
                    if stmt.for_loop.HasField("block_body"):
                        cls.collect_assigned_targets(stmt.for_loop.block_body.statements, targets)
                case "while_loop":
                    if stmt.while_loop.HasField("block_body"):
                        cls.collect_assigned_targets(stmt.while_loop.block_body.statements, targets)
                case "try_except":
                    if stmt.try_except.HasField("try_block"):
                        cls.collect_assigned_targets(stmt.try_except.try_block.statements, targets)
                    for handler in stmt.try_except.handlers:
                        if handler.HasField("block_body"):
                            cls.collect_assigned_targets(handler.block_body.statements, targets)
                case (
                    "parallel_block"
                    | "spread_action"
                    | "action_call"
                    | "return_stmt"
                    | "break_stmt"
                    | "continue_stmt"
                    | "expr_stmt"
                    | None
                ):
                    continue
                case _:
                    assert_never(kind)

    def add_data_flow_edges_for_function(self, function_name: str) -> None:
        fn_node_ids = set(self.dag.get_nodes_for_function(function_name).keys())
        order = self.get_execution_order_for_nodes(fn_node_ids)
        self.add_data_flow_from_definitions(function_name, order)

    def add_data_flow_from_definitions(self, function_name: str, order: List[str]) -> None:
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
        if node.kwargs:
            for value in node.kwargs.values():
                if value == f"${var_name}":
                    return True

        if node.kwarg_exprs:
            for expr in node.kwarg_exprs.values():
                if self.expr_uses_var(expr, var_name):
                    return True

        return False

    @staticmethod
    def expr_uses_var(expr: ir.Expr, var_name: str) -> bool:
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return False
            case "variable":
                return expr.variable.name == var_name
            case "binary_op":
                left = expr.binary_op.left
                right = expr.binary_op.right
                return DAGConverter.expr_uses_var(left, var_name) or DAGConverter.expr_uses_var(
                    right, var_name
                )
            case "unary_op":
                return DAGConverter.expr_uses_var(expr.unary_op.operand, var_name)
            case "list":
                return any(
                    DAGConverter.expr_uses_var(element, var_name) for element in expr.list.elements
                )
            case "dict":
                return any(
                    (entry.HasField("key") and DAGConverter.expr_uses_var(entry.key, var_name))
                    or (
                        entry.HasField("value")
                        and DAGConverter.expr_uses_var(entry.value, var_name)
                    )
                    for entry in expr.dict.entries
                )
            case "index":
                return DAGConverter.expr_uses_var(
                    expr.index.object, var_name
                ) or DAGConverter.expr_uses_var(expr.index.index, var_name)
            case "dot":
                return DAGConverter.expr_uses_var(expr.dot.object, var_name)
            case "function_call":
                return any(
                    DAGConverter.expr_uses_var(arg, var_name) for arg in expr.function_call.args
                ) or any(
                    kw.HasField("value") and DAGConverter.expr_uses_var(kw.value, var_name)
                    for kw in expr.function_call.kwargs
                )
            case "action_call":
                return any(
                    kw.HasField("value") and DAGConverter.expr_uses_var(kw.value, var_name)
                    for kw in expr.action_call.kwargs
                )
            case "parallel_expr":
                for call in expr.parallel_expr.calls:
                    call_kind = call.WhichOneof("kind")
                    match call_kind:
                        case "action":
                            if any(
                                kw.HasField("value")
                                and DAGConverter.expr_uses_var(kw.value, var_name)
                                for kw in call.action.kwargs
                            ):
                                return True
                        case "function":
                            if any(
                                DAGConverter.expr_uses_var(arg, var_name)
                                for arg in call.function.args
                            ) or any(
                                kw.HasField("value")
                                and DAGConverter.expr_uses_var(kw.value, var_name)
                                for kw in call.function.kwargs
                            ):
                                return True
                        case None:
                            continue
                        case _:
                            assert_never(call_kind)
                return False
            case "spread_expr":
                if DAGConverter.expr_uses_var(expr.spread_expr.collection, var_name):
                    return True
                return any(
                    kw.HasField("value") and DAGConverter.expr_uses_var(kw.value, var_name)
                    for kw in expr.spread_expr.action.kwargs
                )
            case None:
                return False
            case _:
                assert_never(kind)

    def get_execution_order_for_nodes(self, node_ids: Set[str]) -> List[str]:
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


def convert_to_dag(program: ir.Program) -> DAG:
    return DAGConverter().convert(program)


def validate_dag(dag: DAG) -> None:
    validate_edges_reference_existing_nodes(dag)
    validate_output_nodes_have_no_outgoing_edges(dag)
    validate_loop_incr_edges(dag)
    validate_no_duplicate_state_machine_edges(dag)
    validate_input_nodes_have_no_incoming_edges(dag)


def validate_edges_reference_existing_nodes(dag: DAG) -> None:
    for edge in dag.edges:
        if edge.source not in dag.nodes:
            raise DagConversionError(
                f"DAG edge references non-existent source node '{edge.source}' -> '{edge.target}'"
            )
        if edge.target not in dag.nodes:
            raise DagConversionError(
                "DAG edge references non-existent target node "
                f"'{edge.target}' (from '{edge.source}', edge_type={edge.edge_type}, "
                f"exception_types={edge.exception_types})"
            )


def validate_output_nodes_have_no_outgoing_edges(dag: DAG) -> None:
    for node_id, node in dag.nodes.items():
        if node.node_type == "output" and ":" not in node_id:
            for edge in dag.edges:
                if (
                    edge.source == node_id
                    and edge.edge_type == EdgeType.STATE_MACHINE
                    and edge.exception_types is None
                ):
                    raise DagConversionError(
                        "Main output node "
                        f"'{node_id}' has non-exception outgoing state machine edge to "
                        f"'{edge.target}'"
                    )


def validate_loop_incr_edges(dag: DAG) -> None:
    for node_id in dag.nodes:
        if "loop_incr" not in node_id:
            continue
        for edge in dag.edges:
            if edge.source != node_id or edge.edge_type != EdgeType.STATE_MACHINE:
                continue
            if edge.exception_types is not None:
                continue
            if edge.is_loop_back and "loop_cond" in edge.target:
                continue
            raise DagConversionError(
                "Loop increment node "
                f"'{node_id}' has unexpected state machine edge to '{edge.target}'. "
                "Loop_incr should only have loop_back edges to loop_cond or exception edges. "
                "This suggests incorrect 'last_real_node' tracking during function expansion."
            )


def validate_no_duplicate_state_machine_edges(dag: DAG) -> None:
    seen: Set[str] = set()
    for edge in dag.edges:
        if edge.edge_type != EdgeType.STATE_MACHINE:
            continue
        if edge.exception_types is None:
            key = (
                f"{edge.source}->{edge.target}:loop_back={edge.is_loop_back},is_else={edge.is_else},"
                f"guard={edge.guard_string}"
            )
            if key in seen:
                raise DagConversionError(
                    "Duplicate state machine edge: "
                    f"{edge.source} -> {edge.target} (loop_back={edge.is_loop_back}, "
                    f"is_else={edge.is_else})"
                )
            seen.add(key)


def validate_input_nodes_have_no_incoming_edges(dag: DAG) -> None:
    for node_id, node in dag.nodes.items():
        if node.is_input:
            for edge in dag.edges:
                if edge.target == node_id and edge.edge_type == EdgeType.STATE_MACHINE:
                    raise DagConversionError(
                        f"Input node '{node_id}' has incoming state machine edge from '{edge.source}'"
                    )
