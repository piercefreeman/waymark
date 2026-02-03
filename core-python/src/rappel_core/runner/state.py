"""Execution-time DAG state with unrolled nodes and symbolic values."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Sequence
from uuid import UUID, uuid4

from proto import ast_pb2 as ir

from ..dag import (
    DAG,
    ActionCallNode,
    AggregatorNode,
    AssignmentNode,
    DAGNode,
    EdgeType,
    FnCallNode,
    JoinNode,
    ReturnNode,
    assert_never,
)
from .value_visitor import ValueExpr, ValueExprResolver, ValueExprSourceCollector


class RunnerStateError(Exception):
    """Raised when the runner state cannot be updated safely."""


@dataclass(frozen=True)
class ActionCallSpec:
    action_name: str
    module_name: Optional[str]
    kwargs: dict[str, "ValueExpr"]


@dataclass(frozen=True)
class LiteralValue:
    value: Any


@dataclass(frozen=True)
class VariableValue:
    name: str


@dataclass(frozen=True)
class ActionResultValue:
    node_id: UUID
    action_name: str
    iteration_index: Optional[int] = None
    result_index: Optional[int] = None

    def label(self) -> str:
        label = self.action_name
        if self.iteration_index is not None:
            label = f"{label}[{self.iteration_index}]"
        if self.result_index is not None:
            label = f"{label}[{self.result_index}]"
        return label


@dataclass(frozen=True)
class BinaryOpValue:
    left: "ValueExpr"
    op: ir.BinaryOperator
    right: "ValueExpr"


@dataclass(frozen=True)
class UnaryOpValue:
    op: ir.UnaryOperator
    operand: "ValueExpr"


@dataclass(frozen=True)
class ListValue:
    elements: tuple["ValueExpr", ...]


@dataclass(frozen=True)
class DictEntryValue:
    key: "ValueExpr"
    value: "ValueExpr"


@dataclass(frozen=True)
class DictValue:
    entries: tuple[DictEntryValue, ...]


@dataclass(frozen=True)
class IndexValue:
    object: "ValueExpr"
    index: "ValueExpr"


@dataclass(frozen=True)
class DotValue:
    object: "ValueExpr"
    attribute: str


@dataclass(frozen=True)
class FunctionCallValue:
    name: str
    args: tuple["ValueExpr", ...]
    kwargs: dict[str, "ValueExpr"]
    global_function: Optional[ir.GlobalFunction] = None


@dataclass(frozen=True)
class SpreadValue:
    collection: "ValueExpr"
    loop_var: str
    action: ActionCallSpec


class NodeStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ExecutionNode:
    node_id: UUID
    node_type: str
    label: str
    status: NodeStatus
    template_id: Optional[str] = None
    targets: list[str] = field(default_factory=list)
    action: Optional[ActionCallSpec] = None
    value_expr: Optional[ValueExpr] = None
    assignments: dict[str, ValueExpr] = field(default_factory=dict)
    action_attempt: int = 0


@dataclass(frozen=True)
class ExecutionEdge:
    source: UUID
    target: UUID
    edge_type: EdgeType = EdgeType.STATE_MACHINE


class RunnerState:
    """Track queued/executed DAG nodes with an unrolled, symbolic state.

    Design overview:
    - The runner state is not a variable heap; it is the runtime graph itself,
      unrolled to the exact nodes that have been queued or executed.
    - Each execution node stores assignments as symbolic expressions so action
      results can be replayed later without having the concrete payloads.
    - Data-flow edges encode which execution node supplies a value to another,
      while state-machine edges encode execution ordering and control flow. This
      mirrors how the ground truth IR->DAG functions.

    Expected usage:
    - Callers queue nodes as the program executes (ie. the DAG template is
      walked) so loops and spreads expand into explicit iterations.
    - Callers never mutate variables directly; they record assignments on nodes
      and let replay walk the graph to reconstruct values.
    - Persisted state can be rehydrated only with nodes/edges. The constructor will
      rebuild in-memory cache (like timeline ordering and latest assignment tracking).

    In short, RunnerState is the ground-truth runtime DAG: symbolic assignments
    plus control/data edges, suitable for replay and visualization.

    Cycle walkthrough (mid-loop example):
    Suppose we are partway through:
    - results = []
    - for item in items:
        - action_result = @action(item)
        - results = results + [action_result + 1]

    On a single iteration update:
    1) The runner queues an action node for @action(item).
       - A new execution node is created with a UUID id.
       - Its assignments map action_result -> ActionResultValue(node_id).
       - Data-flow edges are added from the node that last defined `item`.
    2) The runner queues the assignment node for results update.
       - The RHS expression is materialized:
         results + [action_result + 1] becomes a BinaryOpValue whose tree
         contains the ActionResultValue from step (1), plus a LiteralValue(1).
       - Data-flow edges are added from the prior results definition node and
         from the action node created in step (1).
       - Latest assignment tracking is updated so `results` now points to this
         new execution node.

    After this iteration, the state graph has explicit nodes for the current
    action and the results update. Subsequent iterations repeat the same
    sequence, producing a chain of assignments where replay can reconstruct the
    incremental `results` value by following data-flow edges.

    """

    def __init__(
        self,
        dag: Optional[DAG] = None,
        nodes: Optional[Mapping[UUID, ExecutionNode]] = None,
        edges: Optional[Iterable[ExecutionEdge]] = None,
        *,
        link_queued_nodes: bool = True,
    ) -> None:
        self._dag = dag
        self.nodes = dict(nodes or {})
        self.edges: set[ExecutionEdge] = set(edges or [])
        self.ready_queue: list[UUID] = []
        self.timeline: list[UUID] = []
        self._link_queued_nodes = link_queued_nodes
        self._latest_assignments: dict[str, UUID] = {}

        if self.nodes or self.edges:
            self._rehydrate_state()

    def queue_template_node(
        self,
        template_id: str,
        *,
        iteration_index: Optional[int] = None,
    ) -> ExecutionNode:
        """Queue a runtime node based on the DAG template and apply its effects.

        Use this when stepping through a compiled DAG so the runtime state mirrors
        the template node (assignments, action results, and data-flow edges).

        Example IR:
        - total = a + b
        When the AssignmentNode template is queued, the execution node records
        the symbolic BinaryOpValue and updates data-flow edges from a/b.
        """
        if self._dag is None:
            raise RunnerStateError("runner state has no DAG template")
        template = self._dag.nodes.get(template_id)
        if template is None:
            raise RunnerStateError(f"template node not found: {template_id}")

        node_id = uuid4()
        node = ExecutionNode(
            node_id=node_id,
            node_type=template.node_type,
            label=template.label,
            status=NodeStatus.QUEUED,
            template_id=template_id,
            targets=self._node_targets(template),
            action_attempt=1 if template.node_type == "action_call" else 0,
        )
        if isinstance(template, ActionCallNode):
            node.action = self._action_spec_from_node(template)

        self._register_node(node)
        self._apply_template_node(node, template, iteration_index)
        return node

    def queue_node(
        self,
        node_type: str,
        label: str,
        *,
        node_id: Optional[UUID] = None,
        template_id: Optional[str] = None,
        targets: Optional[Sequence[str]] = None,
        action: Optional[ActionCallSpec] = None,
        value_expr: Optional[ValueExpr] = None,
    ) -> ExecutionNode:
        """Create a runtime node directly without a DAG template.

        Use this for ad-hoc nodes (tests, synthetic steps) and as a common
        builder for higher-level queue helpers like queue_action.

        Example:
        - queue_node(node_type="assignment", label="results = []")
        """
        node_id = node_id or uuid4()
        action_attempt = 1 if node_type == "action_call" else 0
        node = ExecutionNode(
            node_id=node_id,
            node_type=node_type,
            label=label,
            status=NodeStatus.QUEUED,
            template_id=template_id,
            targets=list(targets or []),
            action=action,
            value_expr=value_expr,
            action_attempt=action_attempt,
        )
        self._register_node(node)
        return node

    def queue_action_call(
        self,
        action: ir.ActionCall,
        *,
        targets: Optional[Sequence[str]] = None,
        iteration_index: Optional[int] = None,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> ActionResultValue:
        """Queue an action call from IR, respecting a local scope for loop vars.

        Use this during IR -> runner-state conversion (including spreads) so
        action arguments are converted to symbolic expressions.

        Example IR:
        - @double(value=item)
        With local_scope={"item": LiteralValue(2)}, the queued action uses a
        literal argument and links data-flow to the literal's source nodes.
        """
        spec = self._action_spec_from_ir(action, local_scope)
        node = self.queue_node(
            node_type="action_call",
            label=f"@{spec.action_name}()",
            targets=targets,
            action=spec,
        )
        for value in spec.kwargs.values():
            self._record_data_flow_from_value(node.node_id, value)
        result = self._assign_action_results(node, spec.action_name, targets, iteration_index)
        node.value_expr = result
        return result

    def mark_running(self, node_id: UUID) -> None:
        node = self._get_node(node_id)
        node.status = NodeStatus.RUNNING

    def mark_completed(self, node_id: UUID) -> None:
        node = self._get_node(node_id)
        node.status = NodeStatus.COMPLETED
        if node_id in self.ready_queue:
            self.ready_queue.remove(node_id)

    def mark_failed(self, node_id: UUID) -> None:
        node = self._get_node(node_id)
        node.status = NodeStatus.FAILED
        if node_id in self.ready_queue:
            self.ready_queue.remove(node_id)

    def add_edge(
        self, source: UUID, target: UUID, edge_type: EdgeType = EdgeType.STATE_MACHINE
    ) -> None:
        self._register_edge(ExecutionEdge(source=source, target=target, edge_type=edge_type))

    def _register_node(self, node: ExecutionNode) -> None:
        """Insert a node into the runtime bookkeeping and optional control flow.

        Use this for all queued nodes so the ready queue, timeline, and implicit
        state-machine edge ordering remain consistent.

        Example:
        - queue node A then node B with link_queued_nodes=True
        This creates a state-machine edge A -> B automatically.
        """
        if node.node_id in self.nodes:
            raise RunnerStateError(f"execution node already queued: {node.node_id}")
        self.nodes[node.node_id] = node
        self.ready_queue.append(node.node_id)
        if self._link_queued_nodes and self.timeline:
            self._register_edge(ExecutionEdge(source=self.timeline[-1], target=node.node_id))
        self.timeline.append(node.node_id)

    def _register_edge(self, edge: ExecutionEdge) -> None:
        self.edges.add(edge)

    def _rehydrate_state(self) -> None:
        """Rebuild derived structures from persisted nodes and edges.

        Use this when loading a snapshot so timeline ordering, latest assignment
        tracking, and ready queue reflect the current node set.

        Example:
        - Given nodes {A, B} and edge A -> B, rehydration restores timeline
          [A, B] and marks the latest assignment targets from node B.
        """
        self.timeline = self._build_timeline()
        self._latest_assignments.clear()
        for node_id in self.timeline:
            node = self.nodes.get(node_id)
            if node is None:
                continue
            for target in node.assignments:
                self._latest_assignments[target] = node_id
        if not self.ready_queue:
            self.ready_queue = [
                node_id
                for node_id in self.timeline
                if self.nodes[node_id].status == NodeStatus.QUEUED
            ]

    def _build_timeline(self) -> list[UUID]:
        if not self.edges:
            return list(self.nodes.keys())
        adjacency: dict[UUID, list[UUID]] = {node_id: [] for node_id in self.nodes}
        in_degree: dict[UUID, int] = {node_id: 0 for node_id in self.nodes}
        for edge in sorted(
            self.edges,
            key=lambda edge: (str(edge.source), str(edge.target), edge.edge_type.value),
        ):
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue
            if edge.source not in adjacency or edge.target not in adjacency:
                continue
            adjacency[edge.source].append(edge.target)
            in_degree[edge.target] += 1

        queue = sorted((node_id for node_id, degree in in_degree.items() if degree == 0), key=str)
        order: list[UUID] = []
        while queue:
            node_id = queue.pop(0)
            order.append(node_id)
            for neighbor in sorted(adjacency.get(node_id, []), key=str):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
            queue.sort(key=str)

        remaining = [node_id for node_id in self.nodes if node_id not in order]
        return order + sorted(remaining, key=str)

    def _get_node(self, node_id: UUID) -> ExecutionNode:
        node = self.nodes.get(node_id)
        if node is None:
            raise RunnerStateError(f"execution node not found: {node_id}")
        return node

    def _node_targets(self, node: DAGNode) -> list[str]:
        if isinstance(
            node,
            (
                AssignmentNode,
                ActionCallNode,
                FnCallNode,
                JoinNode,
                AggregatorNode,
                ReturnNode,
            ),
        ):
            if node.targets:
                return list(node.targets)
            if node.target:
                return [node.target]
        return []

    def _apply_template_node(
        self,
        exec_node: ExecutionNode,
        template: DAGNode,
        iteration_index: Optional[int],
    ) -> None:
        """Apply DAG template semantics to a queued execution node.

        Use this right after queue_template_node so assignments, action result
        references, and data-flow edges are populated from the template.

        Example IR:
        - total = @sum(values=items)
        The ActionCallNode template produces an ActionResultValue and defines
        total via assignments on the execution node.
        """
        if isinstance(template, AssignmentNode):
            if template.assign_expr is None:
                return
            value_expr = self._expr_to_value(template.assign_expr)
            exec_node.value_expr = value_expr
            self._record_data_flow_from_value(exec_node.node_id, value_expr)
            assignments = self._build_assignments(self._node_targets(template), value_expr)
            exec_node.assignments.update(assignments)
            self._mark_latest_assignments(exec_node.node_id, assignments)
            return

        if isinstance(template, ActionCallNode):
            for expr in template.kwarg_exprs.values():
                self._record_data_flow_from_value(exec_node.node_id, self._expr_to_value(expr))
            exec_node.value_expr = self._assign_action_results(
                exec_node,
                template.action_name,
                template.targets or ([template.target] if template.target else None),
                iteration_index,
            )
            return

        if isinstance(template, FnCallNode) and template.assign_expr is not None:
            value_expr = self._expr_to_value(template.assign_expr)
            exec_node.value_expr = value_expr
            self._record_data_flow_from_value(exec_node.node_id, value_expr)
            assignments = self._build_assignments(self._node_targets(template), value_expr)
            exec_node.assignments.update(assignments)
            self._mark_latest_assignments(exec_node.node_id, assignments)
            return

        if isinstance(template, ReturnNode) and template.assign_expr is not None:
            value_expr = self._expr_to_value(template.assign_expr)
            exec_node.value_expr = value_expr
            self._record_data_flow_from_value(exec_node.node_id, value_expr)
            target = template.target or "result"
            assignments = self._build_assignments([target], value_expr)
            exec_node.assignments.update(assignments)
            self._mark_latest_assignments(exec_node.node_id, assignments)

    def _assign_action_results(
        self,
        node: ExecutionNode,
        action_name: str,
        targets: Optional[Sequence[str]],
        iteration_index: Optional[int],
    ) -> ActionResultValue:
        """Create symbolic action results and map them to targets.

        Use this when an action produces one or more results that are assigned
        to variables (including tuple unpacking).

        Example IR:
        - a, b = @pair()
        This yields ActionResultValue(node_id, result_index=0/1) for a and b.
        """
        result_ref = ActionResultValue(
            node_id=node.node_id,
            action_name=action_name,
            iteration_index=iteration_index,
        )
        assignments = self._build_assignments(targets or [], result_ref)
        if assignments:
            node.assignments.update(assignments)
            self._mark_latest_assignments(node.node_id, assignments)
        return result_ref

    def _build_assignments(
        self,
        targets: Sequence[str],
        value: ValueExpr,
    ) -> dict[str, ValueExpr]:
        """Expand an assignment into per-target symbolic values.

        Use this for single-target assignments, tuple unpacking, and action
        multi-result binding to keep definitions explicit.

        Example IR:
        - a, b = [1, 2]
        Produces {"a": LiteralValue(1), "b": LiteralValue(2)}.
        """
        value = self._materialize_value(value)
        targets_list = list(targets)
        if not targets_list:
            return {}

        if len(targets_list) == 1:
            return {targets_list[0]: value}

        if isinstance(value, ListValue):
            if len(value.elements) != len(targets_list):
                raise RunnerStateError("tuple unpacking mismatch")
            return {target: item for target, item in zip(targets_list, value.elements, strict=True)}

        if isinstance(value, ActionResultValue):
            return {
                target: ActionResultValue(
                    node_id=value.node_id,
                    action_name=value.action_name,
                    iteration_index=value.iteration_index,
                    result_index=idx,
                )
                for idx, target in enumerate(targets_list)
            }

        if isinstance(value, FunctionCallValue):
            return {
                target: IndexValue(object=value, index=LiteralValue(idx))
                for idx, target in enumerate(targets_list)
            }

        raise RunnerStateError("tuple unpacking mismatch")

    def _materialize_value(self, value: ValueExpr) -> ValueExpr:
        """Inline variable references and apply light constant folding.

        Use this before storing assignments so values are self-contained and
        list concatenations are simplified.

        Example IR:
        - xs = [1]
        - ys = xs + [2]
        Materialization turns ys into ListValue([1, 2]) rather than keeping xs.
        """
        resolved = self._resolve_value_tree(value, set())
        if isinstance(resolved, BinaryOpValue) and resolved.op == ir.BinaryOperator.BINARY_OP_ADD:
            left = resolved.left
            right = resolved.right
            if isinstance(left, ListValue) and isinstance(right, ListValue):
                return ListValue(elements=left.elements + right.elements)
        return resolved

    def _resolve_variable_value(
        self,
        name: str,
        seen: set[str],
    ) -> ValueExpr:
        """Resolve a variable name to its latest symbolic definition.

        Use this when materializing expressions so variables become their
        defining expression while guarding against cycles.

        Example IR:
        - x = 1
        - y = x + 2
        When materializing y, the VariableValue("x") is replaced with the
        LiteralValue(1), yielding a BinaryOpValue(1 + 2) instead of a reference
        to x. This makes downstream replay use the symbolic expression rather
        than requiring a separate variable lookup.
        """
        if name in seen:
            return VariableValue(name)
        node_id = self._latest_assignments.get(name)
        if node_id is None:
            return VariableValue(name)
        node = self.nodes.get(node_id)
        if node is None:
            return VariableValue(name)
        assigned = node.assignments.get(name)
        if assigned is None:
            return VariableValue(name)
        if isinstance(assigned, VariableValue):
            seen.add(name)
            return self._resolve_variable_value(assigned.name, seen)
        return assigned

    def _resolve_value_tree(self, value: ValueExpr, seen: set[str]) -> ValueExpr:
        """Recursively resolve variable references throughout a value tree.

        Use this as the core materialization step before assignment storage.

        Example IR:
        - z = (x + y) * 2
        The tree walk replaces VariableValue("x")/("y") with their latest
        symbolic definitions before storing z.
        """
        resolver = ValueExprResolver(self._resolve_variable_value, seen)
        return resolver.visit(value)

    def _mark_latest_assignments(self, node_id: UUID, assignments: Mapping[str, ValueExpr]) -> None:
        for target in assignments:
            self._latest_assignments[target] = node_id

    def _record_data_flow_from_value(self, node_id: UUID, value: ValueExpr) -> None:
        """Add data-flow edges implied by a value expression.

        Use this when a node consumes an expression so upstream dependencies are
        encoded in the runtime graph.

        Example IR:
        - total = @sum(values)
        A data-flow edge is added from the values assignment node to the action.
        """
        source_ids = self._value_source_nodes(value)
        self._record_data_flow_edges(node_id, source_ids)

    def _record_data_flow_edges(self, node_id: UUID, source_ids: set[UUID]) -> None:
        """Register data-flow edges from sources to the given node.

        Example:
        - sources {A, B} and node C produce edges A -> C and B -> C.
        """
        for source_id in source_ids:
            if source_id == node_id:
                continue
            self._register_edge(
                ExecutionEdge(source=source_id, target=node_id, edge_type=EdgeType.DATA_FLOW)
            )

    def _value_source_nodes(self, value: ValueExpr) -> set[UUID]:
        """Find execution node ids that supply data to the given value.

        Example IR:
        - total = a + @sum(values)
        Returns the latest assignment node for a and the action node for sum().
        """
        collector = ValueExprSourceCollector(self._latest_assignments.get)
        return collector.visit(value)

    def _expr_to_value(
        self,
        expr: ir.Expr,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> ValueExpr:
        """Convert an IR expression into a symbolic ValueExpr tree.

        Use this when interpreting IR statements or DAG templates into the
        runtime state; it queues actions and spreads as needed.

        Example IR:
        - total = base + 1
        Produces BinaryOpValue(VariableValue("base"), LiteralValue(1)).
        """
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return LiteralValue(self._literal_value(expr.literal))
            case "variable":
                name = expr.variable.name
                if local_scope is not None and name in local_scope:
                    return local_scope[name]
                return VariableValue(name)
            case "binary_op":
                left = self._expr_to_value(expr.binary_op.left, local_scope)
                right = self._expr_to_value(expr.binary_op.right, local_scope)
                return self._binary_op_value(expr.binary_op.op, left, right)
            case "unary_op":
                operand = self._expr_to_value(expr.unary_op.operand, local_scope)
                return self._unary_op_value(expr.unary_op.op, operand)
            case "list":
                elements = tuple(
                    self._expr_to_value(item, local_scope) for item in expr.list.elements
                )
                return ListValue(elements=elements)
            case "dict":
                entries = tuple(
                    DictEntryValue(
                        key=self._expr_to_value(entry.key, local_scope),
                        value=self._expr_to_value(entry.value, local_scope),
                    )
                    for entry in expr.dict.entries
                )
                return DictValue(entries=entries)
            case "index":
                obj_value = self._expr_to_value(expr.index.object, local_scope)
                idx_value = self._expr_to_value(expr.index.index, local_scope)
                return self._index_value(obj_value, idx_value)
            case "dot":
                obj_value = self._expr_to_value(expr.dot.object, local_scope)
                return DotValue(object=obj_value, attribute=expr.dot.attribute)
            case "function_call":
                args = tuple(
                    self._expr_to_value(arg, local_scope) for arg in expr.function_call.args
                )
                kwargs = {
                    kw.name: self._expr_to_value(kw.value, local_scope)
                    for kw in expr.function_call.kwargs
                }
                global_fn = (
                    expr.function_call.global_function
                    if expr.function_call.global_function
                    else None
                )
                return FunctionCallValue(
                    name=expr.function_call.name,
                    args=args,
                    kwargs=kwargs,
                    global_function=global_fn,
                )
            case "action_call":
                return self.queue_action_call(expr.action_call, local_scope=local_scope)
            case "parallel_expr":
                calls = [
                    self._call_to_value(call, local_scope) for call in expr.parallel_expr.calls
                ]
                return ListValue(elements=tuple(calls))
            case "spread_expr":
                return self._spread_expr_value(expr.spread_expr, local_scope)
            case None:
                return LiteralValue(None)
            case _:
                assert_never(kind)

    def _call_to_value(
        self,
        call: ir.Call,
        local_scope: Optional[Mapping[str, ValueExpr]],
    ) -> ValueExpr:
        """Convert an IR call (action/function) into a ValueExpr.

        Use this for parallel expressions that contain mixed call types.

        Example IR:
        - parallel { @double(x), helper(x) }
        Action calls become ActionResultValue nodes; function calls become
        FunctionCallValue expressions.
        """
        kind = call.WhichOneof("kind")
        match kind:
            case "action":
                return self.queue_action_call(call.action, local_scope=local_scope)
            case "function":
                return self._expr_to_value(
                    ir.Expr(function_call=call.function),
                    local_scope,
                )
            case None:
                return LiteralValue(None)
            case _:
                assert_never(kind)

    def _spread_expr_value(
        self,
        spread: ir.SpreadExpr,
        local_scope: Optional[Mapping[str, ValueExpr]],
    ) -> ValueExpr:
        """Materialize a spread expression into concrete calls or a symbolic spread.

        Use this when converting IR spreads so known list collections unroll to
        explicit action calls, while unknown collections stay symbolic.

        Example IR:
        - spread [1, 2]:item -> @double(value=item)
        Produces a ListValue of ActionResultValue entries for each item.
        """
        collection = self._expr_to_value(spread.collection, local_scope)
        if isinstance(collection, ListValue):
            results: list[ValueExpr] = []
            for idx, item in enumerate(collection.elements):
                results.append(
                    self.queue_action_call(
                        spread.action,
                        iteration_index=idx,
                        local_scope={spread.loop_var: item},
                    )
                )
            return ListValue(elements=tuple(results))

        action_spec = self._action_spec_from_ir(spread.action)
        return SpreadValue(
            collection=collection,
            loop_var=spread.loop_var,
            action=action_spec,
        )

    def _binary_op_value(
        self,
        op: ir.BinaryOperator,
        left: ValueExpr,
        right: ValueExpr,
    ) -> ValueExpr:
        """Build a binary-op value with simple constant folding.

        Use this when converting IR so literals and list concatenations are
        simplified early.

        Example IR:
        - total = 1 + 2
        Produces LiteralValue(3) instead of a BinaryOpValue.
        """
        if op == ir.BinaryOperator.BINARY_OP_ADD:
            if isinstance(left, ListValue) and isinstance(right, ListValue):
                return ListValue(elements=left.elements + right.elements)
        if isinstance(left, LiteralValue) and isinstance(right, LiteralValue):
            folded = self._fold_literal_binary(op, left.value, right.value)
            if folded is not None:
                return LiteralValue(folded)
        return BinaryOpValue(left=left, op=op, right=right)

    def _unary_op_value(self, op: ir.UnaryOperator, operand: ValueExpr) -> ValueExpr:
        """Build a unary-op value with constant folding for literals.

        Example IR:
        - neg = -1
        Produces LiteralValue(-1) instead of UnaryOpValue.
        """
        if isinstance(operand, LiteralValue):
            folded = self._fold_literal_unary(op, operand.value)
            if folded is not None:
                return LiteralValue(folded)
        return UnaryOpValue(op=op, operand=operand)

    def _index_value(self, obj_value: ValueExpr, idx_value: ValueExpr) -> ValueExpr:
        """Build an index value, folding list literals when possible.

        Example IR:
        - first = [10, 20][0]
        Produces LiteralValue(10) when the list is fully literal.
        """
        if isinstance(obj_value, ListValue) and isinstance(idx_value, LiteralValue):
            if isinstance(idx_value.value, int) and 0 <= idx_value.value < len(obj_value.elements):
                return obj_value.elements[idx_value.value]
        return IndexValue(object=obj_value, index=idx_value)

    def _action_spec_from_node(self, node: ActionCallNode) -> ActionCallSpec:
        """Extract an action call spec from a DAG node.

        Use this when queueing nodes from the DAG template.

        Example:
        - ActionCallNode(action_name="double", kwargs={"value": "$x"})
        Produces ActionCallSpec(action_name="double", kwargs={"value": VariableValue("x")}).
        """
        kwargs = {name: self._expr_to_value(expr) for name, expr in node.kwarg_exprs.items()}
        return ActionCallSpec(
            action_name=node.action_name,
            module_name=node.module_name,
            kwargs=kwargs,
        )

    def _action_spec_from_ir(
        self,
        action: ir.ActionCall,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
    ) -> ActionCallSpec:
        """Extract an action call spec from IR, applying local scope bindings.

        Example IR:
        - @double(value=item) with local_scope["item"]=LiteralValue(2)
        Produces kwargs {"value": LiteralValue(2)}.
        """
        kwargs = {kw.name: self._expr_to_value(kw.value, local_scope) for kw in action.kwargs}
        module_name = action.module_name if action.HasField("module_name") else None
        return ActionCallSpec(
            action_name=action.action_name,
            module_name=module_name,
            kwargs=kwargs,
        )

    @staticmethod
    def _literal_value(lit: ir.Literal) -> Any:
        """Convert an IR literal into a Python value.

        Example IR:
        - Literal(int_value=3) -> 3
        """
        kind = lit.WhichOneof("value")
        match kind:
            case "int_value":
                return int(lit.int_value)
            case "float_value":
                return float(lit.float_value)
            case "string_value":
                return lit.string_value
            case "bool_value":
                return bool(lit.bool_value)
            case "is_none":
                return None
            case None:
                return None
            case _:
                assert_never(kind)

    @staticmethod
    def _fold_literal_binary(
        op: ir.BinaryOperator,
        left: Any,
        right: Any,
    ) -> Optional[Any]:
        """Try to fold a literal binary operation to a concrete value.

        Example:
        - (1, 2, BINARY_OP_ADD) -> 3
        """
        try:
            match op:
                case ir.BinaryOperator.BINARY_OP_ADD:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left + right
                    if isinstance(left, str) and isinstance(right, str):
                        return left + right
                case ir.BinaryOperator.BINARY_OP_SUB:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left - right
                case ir.BinaryOperator.BINARY_OP_MUL:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left * right
                case ir.BinaryOperator.BINARY_OP_DIV:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left / right
                case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left // right
                case ir.BinaryOperator.BINARY_OP_MOD:
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left % right
                case _:
                    return None
        except Exception:
            return None
        return None

    @staticmethod
    def _fold_literal_unary(op: ir.UnaryOperator, operand: Any) -> Optional[Any]:
        """Try to fold a literal unary operation to a concrete value.

        Example:
        - (UNARY_OP_NEG, 4) -> -4
        """
        try:
            match op:
                case ir.UnaryOperator.UNARY_OP_NEG:
                    if isinstance(operand, (int, float)):
                        return -operand
                case ir.UnaryOperator.UNARY_OP_NOT:
                    return not operand
                case _:
                    return None
        except Exception:
            return None
        return None

    # Test harness helpers (not used in main runtime).

    def queue_action(
        self,
        action_name: str,
        *,
        targets: Optional[Sequence[str]] = None,
        kwargs: Optional[Mapping[str, ValueExpr]] = None,
        module_name: Optional[str] = None,
        iteration_index: Optional[int] = None,
    ) -> ActionResultValue:
        """Queue an action call from raw parameters and return a symbolic result.

        Use this when constructing runner state programmatically without IR
        objects, while still wiring data-flow edges and assignments.

        Example:
        - queue_action("double", targets=["out"], kwargs={"value": LiteralValue(2)})
        Defines out via an ActionResultValue and records data-flow from the literal.
        """
        spec = ActionCallSpec(
            action_name=action_name,
            module_name=module_name,
            kwargs=dict(kwargs or {}),
        )
        node = self.queue_node(
            node_type="action_call",
            label=f"@{action_name}()",
            targets=targets,
            action=spec,
        )
        for value in spec.kwargs.values():
            self._record_data_flow_from_value(node.node_id, value)
        result = self._assign_action_results(node, action_name, targets, iteration_index)
        node.value_expr = result
        return result

    def record_assignment(
        self,
        *,
        targets: Sequence[str],
        expr: ir.Expr,
        node_id: Optional[UUID] = None,
        label: Optional[str] = None,
    ) -> ExecutionNode:
        """Record an IR assignment as a runtime node with symbolic values.

        Use this when interpreting IR statements into the unrolled runtime graph.

        Example IR:
        - results = []
        Produces an assignment node with targets ["results"] and a ListValue([]).
        """
        value_expr = self._expr_to_value(expr)
        return self.record_assignment_value(
            targets=targets,
            value_expr=value_expr,
            node_id=node_id,
            label=label,
        )

    def record_assignment_value(
        self,
        *,
        targets: Sequence[str],
        value_expr: ValueExpr,
        node_id: Optional[UUID] = None,
        label: Optional[str] = None,
    ) -> ExecutionNode:
        """Record a symbolic assignment node and update data-flow/definitions.

        Use this for assignments created programmatically after ValueExpr
        construction (tests or state rewrites).

        Example:
        - record_assignment_value(targets=["x"], value_expr=LiteralValue(1))
        Creates an assignment node with x bound to LiteralValue(1).
        """
        exec_node_id = node_id or uuid4()
        node = self.queue_node(
            node_type="assignment",
            label=label or "assignment",
            node_id=exec_node_id,
            targets=targets,
            value_expr=value_expr,
        )
        self._record_data_flow_from_value(exec_node_id, value_expr)
        assignments = self._build_assignments(targets, value_expr)
        node.assignments.update(assignments)
        self._mark_latest_assignments(node.node_id, assignments)
        return node


def format_value(expr: ValueExpr) -> str:
    """Render a ValueExpr to a python-like string for debugging/visualization.

    Example:
    - BinaryOpValue(VariableValue("a"), +, LiteralValue(1)) -> "a + 1"
    """
    return _format_value(expr, 0)


def _format_value(expr: ValueExpr, parent_prec: int) -> str:
    """Recursive ValueExpr formatter with operator precedence handling.

    Example:
    - (a + b) * c renders with parentheses when needed.
    """
    if isinstance(expr, LiteralValue):
        return _format_literal(expr.value)
    if isinstance(expr, VariableValue):
        return expr.name
    if isinstance(expr, ActionResultValue):
        return expr.label()
    if isinstance(expr, BinaryOpValue):
        op_str, prec = _binary_operator(expr.op)
        left = _format_value(expr.left, prec)
        right = _format_value(expr.right, prec + 1)
        rendered = f"{left} {op_str} {right}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, UnaryOpValue):
        op_str, prec = _unary_operator(expr.op)
        operand = _format_value(expr.operand, prec)
        rendered = f"{op_str}{operand}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, ListValue):
        items = ", ".join(_format_value(item, 0) for item in expr.elements)
        return f"[{items}]"
    if isinstance(expr, DictValue):
        entries = ", ".join(
            f"{_format_value(entry.key, 0)}: {_format_value(entry.value, 0)}"
            for entry in expr.entries
        )
        return f"{{{entries}}}"
    if isinstance(expr, IndexValue):
        prec = _precedence("index")
        obj = _format_value(expr.object, prec)
        idx = _format_value(expr.index, 0)
        rendered = f"{obj}[{idx}]"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, DotValue):
        prec = _precedence("dot")
        obj = _format_value(expr.object, prec)
        rendered = f"{obj}.{expr.attribute}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered
    if isinstance(expr, FunctionCallValue):
        args = [_format_value(arg, 0) for arg in expr.args]
        for name, value in expr.kwargs.items():
            args.append(f"{name}={_format_value(value, 0)}")
        return f"{expr.name}({', '.join(args)})"
    if isinstance(expr, SpreadValue):
        collection = _format_value(expr.collection, 0)
        args = [f"{name}={_format_value(value, 0)}" for name, value in expr.action.kwargs.items()]
        call = f"@{expr.action.action_name}({', '.join(args)})"
        return f"spread {collection}:{expr.loop_var} -> {call}"
    assert_never(expr)


def _binary_operator(op: ir.BinaryOperator) -> tuple[str, int]:
    """Map binary operator enums to (symbol, precedence) for formatting.

    Example:
    - BINARY_OP_ADD -> ("+", 40)
    """
    match op:
        case ir.BinaryOperator.BINARY_OP_OR:
            return "or", 10
        case ir.BinaryOperator.BINARY_OP_AND:
            return "and", 20
        case ir.BinaryOperator.BINARY_OP_EQ:
            return "==", 30
        case ir.BinaryOperator.BINARY_OP_NE:
            return "!=", 30
        case ir.BinaryOperator.BINARY_OP_LT:
            return "<", 30
        case ir.BinaryOperator.BINARY_OP_LE:
            return "<=", 30
        case ir.BinaryOperator.BINARY_OP_GT:
            return ">", 30
        case ir.BinaryOperator.BINARY_OP_GE:
            return ">=", 30
        case ir.BinaryOperator.BINARY_OP_IN:
            return "in", 30
        case ir.BinaryOperator.BINARY_OP_NOT_IN:
            return "not in", 30
        case ir.BinaryOperator.BINARY_OP_ADD:
            return "+", 40
        case ir.BinaryOperator.BINARY_OP_SUB:
            return "-", 40
        case ir.BinaryOperator.BINARY_OP_MUL:
            return "*", 50
        case ir.BinaryOperator.BINARY_OP_DIV:
            return "/", 50
        case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
            return "//", 50
        case ir.BinaryOperator.BINARY_OP_MOD:
            return "%", 50
        case ir.BinaryOperator.BINARY_OP_UNSPECIFIED:
            return "?", 0
        case _:
            assert_never(op)


def _unary_operator(op: ir.UnaryOperator) -> tuple[str, int]:
    """Map unary operator enums to (symbol, precedence) for formatting.

    Example:
    - UNARY_OP_NEG -> ("-", 60)
    """
    match op:
        case ir.UnaryOperator.UNARY_OP_NEG:
            return "-", 60
        case ir.UnaryOperator.UNARY_OP_NOT:
            return "not ", 60
        case ir.UnaryOperator.UNARY_OP_UNSPECIFIED:
            return "?", 0
        case _:
            assert_never(op)


def _precedence(kind: str) -> int:
    """Return precedence for non-operator constructs like index/dot.

    Example:
    - "index" -> 80
    """
    match kind:
        case "index" | "dot":
            return 80
        case _:
            return 0


def _format_literal(value: Any) -> str:
    """Format Python literals as source-like text.

    Example:
    - "hi" -> "\"hi\""
    """
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, str):
        return json.dumps(value)
    return str(value)
