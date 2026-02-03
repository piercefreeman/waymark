"""Incremental DAG executor for runner state graphs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence
from uuid import UUID

from proto import ast_pb2 as ir

from ..backends.base import ActionDone, BaseBackend, GraphUpdate
from ..dag import (
    DAG,
    ActionCallNode,
    AggregatorNode,
    DAGEdge,
    EdgeType,
    assert_never,
)
from .state import (
    ActionCallSpec,
    ActionResultValue,
    BinaryOpValue,
    DictEntryValue,
    DictValue,
    DotValue,
    ExecutionEdge,
    ExecutionNode,
    FunctionCallValue,
    IndexValue,
    ListValue,
    LiteralValue,
    NodeStatus,
    RunnerState,
    UnaryOpValue,
    VariableValue,
)
from .value_visitor import ValueExpr, ValueExprEvaluator

LOGGER = logging.getLogger(__name__)

LOGGER = logging.getLogger(__name__)


class RunnerExecutorError(Exception):
    """Raised when the runner executor cannot advance safely."""


@dataclass(frozen=True)
class ExecutorStep:
    """Return value for executor steps with newly queued action nodes."""

    actions: list[ExecutionNode]


class RunnerExecutor:
    """Advance a DAG template using the current runner state and action results.

    The executor treats the DAG as a control-flow template. It queues runtime
    execution nodes into RunnerState, unrolling loops/spreads into explicit
    iterations, and stops when it encounters action calls that must be executed
    by an external worker.

    Each call to increment() starts from a finished execution node, walks
    downstream through inline nodes (assignments, branches, joins, etc.), and
    returns any newly queued action nodes that are now unblocked.
    """

    def __init__(
        self,
        dag: DAG,
        *,
        state: Optional[RunnerState] = None,
        nodes: Optional[Mapping[UUID, ExecutionNode]] = None,
        edges: Optional[Iterable[ExecutionEdge]] = None,
        action_results: Optional[Mapping[UUID, Any]] = None,
        backend: Optional[BaseBackend] = None,
    ) -> None:
        self._dag = dag
        self._state = state or RunnerState(dag=dag, nodes=nodes, edges=edges, link_queued_nodes=False)
        self._state._dag = dag
        self._state._link_queued_nodes = False
        self._action_results: dict[UUID, Any]
        if action_results is None:
            self._action_results = {}
        elif isinstance(action_results, dict):
            self._action_results = action_results
        else:
            self._action_results = dict(action_results)
        self._backend = backend
        self._template_outgoing = self._build_template_outgoing()
        self._template_incoming = self._build_template_incoming()
        self._incoming_exec_edges = self._build_incoming_exec_edges()
        self._eval_cache: dict[tuple[UUID, str], Any] = {}

    @property
    def state(self) -> RunnerState:
        return self._state

    @property
    def dag(self) -> DAG:
        return self._dag

    @property
    def action_results(self) -> Mapping[UUID, Any]:
        return self._action_results

    def set_action_result(self, node_id: UUID, result: Any) -> None:
        """Store an action result value for a specific execution node."""
        self._action_results[node_id] = result

    def clear_action_result(self, node_id: UUID) -> None:
        """Remove any cached action result for a specific execution node."""
        self._action_results.pop(node_id, None)

    def resume(self) -> ExecutorStep:
        """Fail inflight actions and return any that should be retried.

        Use this after recovering from a crash: running actions are treated as
        failed, their attempt counter is incremented if retry policies allow,
        and retryable nodes are re-queued for execution.
        """
        retry_nodes: list[ExecutionNode] = []
        for node in self._state.nodes.values():
            if node.node_type != "action_call" or node.status != NodeStatus.RUNNING:
                continue
            if self._can_retry(node):
                node.action_attempt += 1
                node.status = NodeStatus.QUEUED
                if node.node_id not in self._state.ready_queue:
                    self._state.ready_queue.append(node.node_id)
                retry_nodes.append(node)
            else:
                node.status = NodeStatus.FAILED
        self._persist_updates(actions_done=[])
        return ExecutorStep(actions=retry_nodes)

    def increment(self, finished_node: UUID) -> ExecutorStep:
        """Advance execution from a finished node and return newly queued actions.

        Example:
        - Action A finishes -> assignment -> branch -> action B
        increment(A) queues the assignment and branch inline, then returns action B.
        """
        return self.increment_batch([finished_node])

    def increment_batch(self, finished_nodes: Sequence[UUID]) -> ExecutorStep:
        """Advance execution for multiple finished nodes in a single batch.

        Use this when multiple actions complete in the same tick so the graph
        update and action inserts are persisted together.
        """
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug("increment_batch finished_nodes=%s", [str(node) for node in finished_nodes])
        self._eval_cache.clear()
        actions_done: list[ActionDone] = []
        pending_starts: list[tuple[ExecutionNode, Optional[Any]]] = []
        actions: list[ExecutionNode] = []
        seen_actions: set[UUID] = set()

        for finished_node in finished_nodes:
            node = self._state.nodes.get(finished_node)
            if node is None:
                raise RunnerExecutorError(f"execution node not found: {finished_node}")
            start, exception_value, done, retry_action = self._apply_finished_node(node)
            if start is not None:
                pending_starts.append((start, exception_value))
            if done is not None:
                actions_done.append(done)
            if retry_action is not None and retry_action.node_id not in seen_actions:
                seen_actions.add(retry_action.node_id)
                actions.append(retry_action)

        while pending_starts:
            start, exception_value = pending_starts.pop(0)
            for action in self._walk_from(start, exception_value=exception_value):
                if action.node_id in seen_actions:
                    continue
                seen_actions.add(action.node_id)
                actions.append(action)

        self._persist_updates(actions_done=actions_done)
        return ExecutorStep(actions=actions)

    def _walk_from(
        self,
        node: ExecutionNode,
        *,
        exception_value: Optional[Any],
    ) -> list[ExecutionNode]:
        """Walk downstream from a node, executing inline nodes until blocked."""
        pending: list[tuple[ExecutionNode, Optional[Any]]] = [(node, exception_value)]
        actions: list[ExecutionNode] = []

        while pending:
            current, current_exception = pending.pop(0)
            if current.template_id is None:
                continue
            template_edges = self._template_outgoing.get(current.template_id, [])
            edges = self._select_edges(template_edges, current, current_exception)
            for edge in edges:
                successors = self._queue_successor(current, edge)
                for successor in successors:
                    if successor.status == NodeStatus.COMPLETED:
                        continue
                    if successor.node_type == "action_call":
                        actions.append(successor)
                        continue
                    if not self._inline_ready(successor):
                        continue
                    if LOGGER.isEnabledFor(logging.DEBUG):
                        LOGGER.debug(
                            "execute_inline node=%s template=%s type=%s",
                            successor.node_id,
                            successor.template_id,
                            successor.node_type,
                        )
                    self._execute_inline_node(successor)
                    pending.append((successor, None))
        return actions

    def _apply_finished_node(
        self, node: ExecutionNode
    ) -> tuple[Optional[ExecutionNode], Optional[Any], Optional[ActionDone], Optional[ExecutionNode]]:
        """Update state for a finished node and return replay metadata."""
        exception_value: Optional[Any] = None
        action_done: Optional[ActionDone] = None
        retry_action: Optional[ExecutionNode] = None

        if node.node_type == "action_call":
            action_value = self._action_results.get(node.node_id)
            if action_value is None:
                raise RunnerExecutorError(f"missing action result for {node.node_id}")
            if self._is_exception_value(action_value):
                exception_value = action_value
                if self._can_retry(node):
                    node.action_attempt += 1
                    node.status = NodeStatus.QUEUED
                    if node.node_id not in self._state.ready_queue:
                        self._state.ready_queue.append(node.node_id)
                    retry_action = node
                    return None, None, None, retry_action
                self._state.mark_failed(node.node_id)
            else:
                self._state.mark_completed(node.node_id)
                action_done = ActionDone(
                    node_id=node.node_id,
                    action_name=self._action_name_for_node(node),
                    attempt=node.action_attempt,
                    result=action_value,
                )
        else:
            self._state.mark_completed(node.node_id)

        return node, exception_value, action_done, retry_action

    def _select_edges(
        self,
        edges: Sequence[DAGEdge],
        node: ExecutionNode,
        exception_value: Optional[Any],
    ) -> list[DAGEdge]:
        """Select outgoing edges based on guards and exception state."""
        if exception_value is not None:
            return [
                edge
                for edge in edges
                if edge.exception_types is not None and self._exception_matches(edge, exception_value)
            ]

        guard_edges = [edge for edge in edges if edge.guard_expr is not None]
        else_edges = [edge for edge in edges if edge.is_else]

        if guard_edges or else_edges:
            passed = [edge for edge in guard_edges if self._evaluate_guard(edge.guard_expr)]
            if passed:
                return passed
            return else_edges

        return [edge for edge in edges if edge.exception_types is None]

    def _queue_successor(self, source: ExecutionNode, edge: DAGEdge) -> list[ExecutionNode]:
        """Queue successor nodes for a template edge, handling spreads/aggregators."""
        if edge.edge_type != EdgeType.STATE_MACHINE:
            return []
        template = self._dag.nodes.get(edge.target)
        if template is None:
            raise RunnerExecutorError(f"template node not found: {edge.target}")

        if isinstance(template, ActionCallNode) and template.spread_loop_var:
            return self._expand_spread_action(source, template)

        if isinstance(template, AggregatorNode):
            existing = self._find_connected_aggregator(source.node_id, template.id)
            if existing is not None:
                return [existing]
            agg_node = self._get_or_create_aggregator(template.id)
            self._add_exec_edge(source.node_id, agg_node.node_id)
            return [agg_node]

        exec_node = self._get_or_create_exec_node(template.id)
        self._add_exec_edge(source.node_id, exec_node.node_id)
        return [exec_node]

    def _expand_spread_action(
        self,
        source: ExecutionNode,
        template: ActionCallNode,
    ) -> list[ExecutionNode]:
        """Unroll a spread action into per-item action nodes and a shared aggregator.

        Example IR:
        - results = spread items:item -> @work(item=item)
        Produces one action execution node per element in items and connects
        them to a single aggregator node for results.
        """
        if template.spread_collection_expr is None:
            raise RunnerExecutorError("spread action missing collection expression")
        if template.spread_loop_var is None:
            raise RunnerExecutorError("spread action missing loop variable")

        elements = self._expand_collection(template.spread_collection_expr)
        if template.aggregates_to is None:
            raise RunnerExecutorError("spread action missing aggregator link")

        agg_node = self._state.queue_template_node(template.aggregates_to)
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "spread_expand template=%s loop_var=%s elements=%d agg_node=%s",
                template.id,
                template.spread_loop_var,
                len(elements),
                agg_node.node_id,
            )
        if not elements:
            return [agg_node]

        created_actions: list[ExecutionNode] = []
        for idx, element in enumerate(elements):
            exec_node = self._queue_action_from_template(
                template,
                local_scope={template.spread_loop_var: element},
                iteration_index=idx,
            )
            self._add_exec_edge(source.node_id, exec_node.node_id)
            self._add_exec_edge(exec_node.node_id, agg_node.node_id)
            created_actions.append(exec_node)
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug(
                    "spread_action node=%s iter=%s agg_node=%s",
                    exec_node.node_id,
                    idx,
                    agg_node.node_id,
                )
        if LOGGER.isEnabledFor(logging.DEBUG):
            incoming = self._incoming_exec_edges.get(agg_node.node_id, [])
            LOGGER.debug(
                "spread_agg_edges agg_node=%s incoming=%d",
                agg_node.node_id,
                len(incoming),
            )
        return created_actions

    def _queue_action_from_template(
        self,
        template: ActionCallNode,
        *,
        local_scope: Optional[Mapping[str, ValueExpr]] = None,
        iteration_index: Optional[int] = None,
    ) -> ExecutionNode:
        """Create an action execution node from a template with optional bindings.

        Example IR:
        - @work(value=item) with local_scope{"item": LiteralValue(3)}
        Produces an action node whose kwargs include the literal 3.
        """
        kwargs = {
            name: self._state._expr_to_value(expr, local_scope)
            for name, expr in template.kwarg_exprs.items()
        }
        spec = ActionCallSpec(
            action_name=template.action_name,
            module_name=template.module_name,
            kwargs=kwargs,
        )
        targets = template.targets or ([template.target] if template.target else [])
        node = self._state.queue_node(
            node_type="action_call",
            label=template.label,
            template_id=template.id,
            targets=targets,
            action=spec,
        )
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "queue_action node=%s template=%s action=%s iter=%s targets=%s",
                node.node_id,
                template.id,
                spec.action_name,
                iteration_index,
                targets,
            )
        for value in spec.kwargs.values():
            self._state._record_data_flow_from_value(node.node_id, value)
        result = self._state._assign_action_results(node, template.action_name, targets, iteration_index)
        node.value_expr = result
        return node

    def _execute_inline_node(self, node: ExecutionNode) -> None:
        """Execute a non-action node inline and update assignments/edges."""
        template = self._dag.nodes.get(node.template_id or "")
        if template is None:
            raise RunnerExecutorError(f"template node not found: {node.template_id}")

        if isinstance(template, AggregatorNode):
            self._apply_aggregator_assignments(node, template)

        self._state.mark_completed(node.node_id)

    def _inline_ready(self, node: ExecutionNode) -> bool:
        """Check if an inline node is ready to run based on incoming edges."""
        if node.status == NodeStatus.COMPLETED:
            return False
        incoming = self._incoming_exec_edges.get(node.node_id, [])
        if not incoming:
            return True
        template = self._dag.nodes.get(node.template_id or "")
        if template is None:
            return False

        if isinstance(template, AggregatorNode):
            required = self._template_incoming.get(template.id, set())
            connected = self._connected_template_sources(node.node_id)
            if not required.issubset(connected):
                return False
            for edge in incoming:
                source = self._state.nodes.get(edge.source)
                if source is None or source.status != NodeStatus.COMPLETED:
                    return False
            if LOGGER.isEnabledFor(logging.DEBUG):
                statuses = [
                    f"{edge.source}:{self._state.nodes[edge.source].status.value}"
                    for edge in incoming
                    if edge.source in self._state.nodes
                ]
                LOGGER.debug(
                    "aggregator_ready node=%s incoming=%d required=%s connected=%s statuses=%s",
                    node.node_id,
                    len(incoming),
                    list(required),
                    list(connected),
                    statuses,
                )
            return True

        for edge in incoming:
            source = self._state.nodes.get(edge.source)
            if source is None or source.status != NodeStatus.COMPLETED:
                return False
        return True

    def _apply_aggregator_assignments(self, node: ExecutionNode, template: AggregatorNode) -> None:
        """Populate aggregated list assignments for a ready aggregator node.

        Example:
        - results = spread items: @work(item)
        When all action nodes complete, the aggregator assigns
        results = [ActionResultValue(...), ...].
        """
        targets = template.targets or ([template.target] if template.target else [])
        if len(targets) != 1:
            return

        incoming_nodes = [
            self._state.nodes[edge.source]
            for edge in self._incoming_exec_edges.get(node.node_id, [])
            if edge.edge_type == EdgeType.STATE_MACHINE
        ]
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "aggregator_assign node=%s incoming_nodes=%d",
                node.node_id,
                len(incoming_nodes),
            )
        values: list[ValueExpr] = []
        for source in incoming_nodes:
            if source.value_expr is None:
                raise RunnerExecutorError("aggregator missing source value")
            values.append(source.value_expr)
        if LOGGER.isEnabledFor(logging.DEBUG):
            labels: list[str] = []
            for value in values:
                if isinstance(value, ActionResultValue):
                    labels.append(value.label())
                else:
                    labels.append(type(value).__name__)
            LOGGER.debug("aggregator_values node=%s values=%s", node.node_id, labels)

        ordered = self._order_aggregated_values(incoming_nodes, values)
        list_value = ListValue(elements=tuple(ordered))
        assignment = {targets[0]: list_value}
        node.assignments.update(assignment)
        self._state._mark_latest_assignments(node.node_id, assignment)
        self._state._record_data_flow_from_value(node.node_id, list_value)

    def _order_aggregated_values(
        self,
        sources: Sequence[ExecutionNode],
        values: Sequence[ValueExpr],
    ) -> list[ValueExpr]:
        """Order aggregator values by spread iteration or parallel index."""
        if len(sources) != len(values):
            raise RunnerExecutorError("aggregator sources/value mismatch")

        timeline_index = {node_id: idx for idx, node_id in enumerate(self._state.timeline)}
        pairs: list[tuple[tuple[int, int], ValueExpr]] = []
        for source, value in zip(sources, values, strict=True):
            primary = 2
            secondary = timeline_index.get(source.node_id, 0)
            if isinstance(value, ActionResultValue) and value.iteration_index is not None:
                primary = 0
                secondary = value.iteration_index
            elif source.template_id:
                template = self._dag.nodes.get(source.template_id)
                if isinstance(template, ActionCallNode) and template.parallel_index is not None:
                    primary = 1
                    secondary = template.parallel_index
            pairs.append(((primary, secondary), value))

        pairs.sort(key=lambda item: item[0])
        return [value for _, value in pairs]

    def _expand_collection(self, expr: ir.Expr) -> list[ValueExpr]:
        """Expand a collection expression into element ValueExprs.

        Example IR:
        - spread range(3):i -> @work(i)
        Produces [LiteralValue(0), LiteralValue(1), LiteralValue(2)].
        """
        value = self._expr_to_value(expr)
        value = self._state._materialize_value(value)

        if isinstance(value, ListValue):
            return list(value.elements)

        if isinstance(value, ActionResultValue):
            action_value = self._resolve_action_result(value)
            if not isinstance(action_value, (list, tuple)):
                raise RunnerExecutorError("spread collection is not iterable")
            return [
                IndexValue(object=value, index=LiteralValue(idx))
                for idx in range(len(action_value))
            ]

        evaluated = self._evaluate_value_expr(value)
        if isinstance(evaluated, (list, tuple)):
            return [LiteralValue(item) for item in evaluated]

        raise RunnerExecutorError("spread collection is not iterable")

    def _expr_to_value(self, expr: ir.Expr) -> ValueExpr:
        """Convert a pure IR expression into a ValueExpr without side effects."""
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return LiteralValue(self._state._literal_value(expr.literal))
            case "variable":
                return VariableValue(expr.variable.name)
            case "binary_op":
                left = self._expr_to_value(expr.binary_op.left)
                right = self._expr_to_value(expr.binary_op.right)
                return BinaryOpValue(left=left, op=expr.binary_op.op, right=right)
            case "unary_op":
                operand = self._expr_to_value(expr.unary_op.operand)
                return UnaryOpValue(op=expr.unary_op.op, operand=operand)
            case "list":
                return ListValue(
                    elements=tuple(self._expr_to_value(item) for item in expr.list.elements)
                )
            case "dict":
                entries = tuple(
                    DictEntryValue(
                        key=self._expr_to_value(entry.key),
                        value=self._expr_to_value(entry.value),
                    )
                    for entry in expr.dict.entries
                )
                return DictValue(entries=entries)
            case "index":
                obj_value = self._expr_to_value(expr.index.object)
                idx_value = self._expr_to_value(expr.index.index)
                return IndexValue(object=obj_value, index=idx_value)
            case "dot":
                obj_value = self._expr_to_value(expr.dot.object)
                return DotValue(object=obj_value, attribute=expr.dot.attribute)
            case "function_call":
                args = tuple(self._expr_to_value(arg) for arg in expr.function_call.args)
                kwargs = {
                    kw.name: self._expr_to_value(kw.value)
                    for kw in expr.function_call.kwargs
                    if kw.HasField("value")
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
            case "action_call" | "parallel_expr" | "spread_expr":
                raise RunnerExecutorError("action/spread calls not allowed in guard expressions")
            case None:
                return LiteralValue(None)
            case _:
                assert_never(kind)

    def _evaluate_guard(self, expr: Optional[ir.Expr]) -> bool:
        """Evaluate a guard expression using current symbolic assignments."""
        if expr is None:
            return False
        value_expr = self._state._materialize_value(self._expr_to_value(expr))
        result = self._evaluate_value_expr(value_expr)
        return bool(result)

    def resolve_action_kwargs(self, action: ActionCallSpec) -> dict[str, Any]:
        """Resolve an action's symbolic kwargs to concrete Python values.

        Example:
        - spec.kwargs={"value": VariableValue("x")}
        - with x assigned to LiteralValue(10), returns {"value": 10}.
        """
        resolved: dict[str, Any] = {}
        for name, expr in action.kwargs.items():
            materialized = self._state._materialize_value(expr)
            resolved[name] = self._evaluate_value_expr(materialized)
        return resolved

    def _evaluate_value_expr(self, expr: ValueExpr) -> Any:
        """Evaluate a ValueExpr into a concrete Python value."""
        evaluator = ValueExprEvaluator(
            resolve_variable=lambda name: self._evaluate_variable(name, set()),
            resolve_action_result=self._resolve_action_result,
            resolve_function_call=self._evaluate_function_call,
            apply_binary=self._apply_binary,
            apply_unary=self._apply_unary,
            error_factory=RunnerExecutorError,
        )
        return evaluator.visit(expr)

    def _evaluate_variable(self, name: str, stack: set[tuple[UUID, str]]) -> Any:
        node_id = self._state._latest_assignments.get(name)
        if node_id is None:
            raise RunnerExecutorError(f"variable not found: {name}")
        return self._evaluate_assignment(node_id, name, stack)

    def _evaluate_assignment(
        self,
        node_id: UUID,
        target: str,
        stack: set[tuple[UUID, str]],
    ) -> Any:
        key = (node_id, target)
        if key in self._eval_cache:
            return self._eval_cache[key]
        if key in stack:
            raise RunnerExecutorError(f"recursive assignment detected for {target}")

        node = self._state.nodes.get(node_id)
        if node is None or target not in node.assignments:
            raise RunnerExecutorError(f"missing assignment for {target}")

        stack.add(key)
        evaluator = ValueExprEvaluator(
            resolve_variable=lambda name: self._evaluate_variable(name, stack),
            resolve_action_result=self._resolve_action_result,
            resolve_function_call=self._evaluate_function_call,
            apply_binary=self._apply_binary,
            apply_unary=self._apply_unary,
            error_factory=RunnerExecutorError,
        )
        value = evaluator.visit(node.assignments[target])
        stack.remove(key)
        self._eval_cache[key] = value
        return value

    def _resolve_action_result(self, expr: ActionResultValue) -> Any:
        if expr.node_id not in self._action_results:
            raise RunnerExecutorError(f"missing action result for {expr.node_id}")
        value = self._action_results[expr.node_id]
        if expr.result_index is None:
            return value
        try:
            return value[expr.result_index]
        except Exception as exc:  # noqa: BLE001
            raise RunnerExecutorError(
                f"action result for {expr.node_id} has no index {expr.result_index}"
            ) from exc

    def _evaluate_function_call(
        self,
        expr: FunctionCallValue,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> Any:
        if (
            expr.global_function
            and expr.global_function != ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED
        ):
            return self._evaluate_global_function(expr.global_function, args, kwargs)
        raise RunnerExecutorError(f"cannot evaluate non-global function call: {expr.name}")

    @staticmethod
    def _evaluate_global_function(
        global_function: ir.GlobalFunction,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> Any:
        match global_function:
            case ir.GlobalFunction.GLOBAL_FUNCTION_RANGE:
                return list(range(*args))
            case ir.GlobalFunction.GLOBAL_FUNCTION_LEN:
                if args:
                    return len(args[0])
                if "items" in kwargs:
                    return len(kwargs["items"])
                raise RunnerExecutorError("len() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE:
                if args:
                    return list(enumerate(args[0]))
                if "items" in kwargs:
                    return list(enumerate(kwargs["items"]))
                raise RunnerExecutorError("enumerate() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION:
                if args:
                    return RunnerExecutor._is_exception_value(args[0])
                if "value" in kwargs:
                    return RunnerExecutor._is_exception_value(kwargs["value"])
                raise RunnerExecutorError("isexception() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED:
                raise RunnerExecutorError("global function unspecified")
            case _:
                assert_never(global_function)

    @staticmethod
    def _apply_binary(op: ir.BinaryOperator, left: Any, right: Any) -> Any:
        match op:
            case ir.BinaryOperator.BINARY_OP_OR:
                return left or right
            case ir.BinaryOperator.BINARY_OP_AND:
                return left and right
            case ir.BinaryOperator.BINARY_OP_EQ:
                return left == right
            case ir.BinaryOperator.BINARY_OP_NE:
                return left != right
            case ir.BinaryOperator.BINARY_OP_LT:
                return left < right
            case ir.BinaryOperator.BINARY_OP_LE:
                return left <= right
            case ir.BinaryOperator.BINARY_OP_GT:
                return left > right
            case ir.BinaryOperator.BINARY_OP_GE:
                return left >= right
            case ir.BinaryOperator.BINARY_OP_IN:
                return left in right
            case ir.BinaryOperator.BINARY_OP_NOT_IN:
                return left not in right
            case ir.BinaryOperator.BINARY_OP_ADD:
                return left + right
            case ir.BinaryOperator.BINARY_OP_SUB:
                return left - right
            case ir.BinaryOperator.BINARY_OP_MUL:
                return left * right
            case ir.BinaryOperator.BINARY_OP_DIV:
                return left / right
            case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
                return left // right
            case ir.BinaryOperator.BINARY_OP_MOD:
                return left % right
            case ir.BinaryOperator.BINARY_OP_UNSPECIFIED:
                raise RunnerExecutorError("binary operator unspecified")
            case _:
                assert_never(op)

    @staticmethod
    def _apply_unary(op: ir.UnaryOperator, operand: Any) -> Any:
        match op:
            case ir.UnaryOperator.UNARY_OP_NEG:
                return -operand
            case ir.UnaryOperator.UNARY_OP_NOT:
                return not operand
            case ir.UnaryOperator.UNARY_OP_UNSPECIFIED:
                raise RunnerExecutorError("unary operator unspecified")
            case _:
                assert_never(op)

    @staticmethod
    def _is_exception_value(value: Any) -> bool:
        if isinstance(value, BaseException):
            return True
        if isinstance(value, dict) and "type" in value and "message" in value:
            return True
        return False

    def _exception_matches(self, edge: DAGEdge, exception_value: Any) -> bool:
        if edge.exception_types is None:
            return False
        if not edge.exception_types:
            return True
        exc_name = None
        if isinstance(exception_value, BaseException):
            exc_name = type(exception_value).__name__
        if isinstance(exception_value, dict) and "type" in exception_value:
            exc_name = str(exception_value["type"])
        if exc_name is None:
            return False
        return exc_name in edge.exception_types

    def _can_retry(self, node: ExecutionNode) -> bool:
        if node.template_id is None:
            return False
        template = self._dag.nodes.get(node.template_id)
        if not isinstance(template, ActionCallNode):
            return False
        max_retries = 0
        for policy in template.policies:
            kind = policy.WhichOneof("kind")
            match kind:
                case "retry":
                    max_retries = max(max_retries, int(policy.retry.max_retries))
                case "timeout" | None:
                    continue
                case _:
                    assert_never(kind)
        return node.action_attempt - 1 < max_retries

    def _build_template_outgoing(self) -> dict[str, list[DAGEdge]]:
        outgoing: dict[str, list[DAGEdge]] = {}
        for edge in self._dag.edges:
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue
            outgoing.setdefault(edge.source, []).append(edge)
        return outgoing

    def _build_template_incoming(self) -> dict[str, set[str]]:
        incoming: dict[str, set[str]] = {}
        for edge in self._dag.edges:
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue
            incoming.setdefault(edge.target, set()).add(edge.source)
        return incoming

    def _build_incoming_exec_edges(self) -> dict[UUID, list[ExecutionEdge]]:
        incoming: dict[UUID, list[ExecutionEdge]] = {}
        for edge in self._state.edges:
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue
            incoming.setdefault(edge.target, []).append(edge)
        return incoming

    def _add_exec_edge(self, source: UUID, target: UUID) -> None:
        edge = ExecutionEdge(source=source, target=target, edge_type=EdgeType.STATE_MACHINE)
        if edge in self._state.edges:
            return
        self._state.edges.add(edge)
        self._incoming_exec_edges.setdefault(target, []).append(edge)
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "exec_edge_add source=%s target=%s type=%s",
                source,
                target,
                edge.edge_type.value,
            )

    def _connected_template_sources(self, exec_node_id: UUID) -> set[str]:
        connected: set[str] = set()
        for edge in self._incoming_exec_edges.get(exec_node_id, []):
            source = self._state.nodes.get(edge.source)
            if source is None or source.template_id is None:
                continue
            connected.add(source.template_id)
        return connected

    def _find_connected_aggregator(
        self, source_id: UUID, template_id: str
    ) -> Optional[ExecutionNode]:
        for edge in self._state.edges:
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue
            if edge.source != source_id:
                continue
            target = self._state.nodes.get(edge.target)
            if target is None:
                continue
            if target.template_id == template_id:
                if LOGGER.isEnabledFor(logging.DEBUG):
                    LOGGER.debug(
                        "aggregator_reuse source=%s template=%s node=%s status=%s",
                        source_id,
                        template_id,
                        target.node_id,
                        target.status.value,
                    )
                return target
        return None

    def _get_or_create_aggregator(self, template_id: str) -> ExecutionNode:
        candidates = [
            node
            for node in self._state.nodes.values()
            if node.template_id == template_id and node.status != NodeStatus.COMPLETED
        ]
        if candidates:
            timeline_index = {node_id: idx for idx, node_id in enumerate(self._state.timeline)}
            candidates.sort(key=lambda node: timeline_index.get(node.node_id, 0), reverse=True)
            return candidates[0]
        return self._state.queue_template_node(template_id)

    def _get_or_create_exec_node(self, template_id: str) -> ExecutionNode:
        candidates = [
            node
            for node in self._state.nodes.values()
            if node.template_id == template_id and node.status != NodeStatus.COMPLETED
        ]
        if candidates:
            timeline_index = {node_id: idx for idx, node_id in enumerate(self._state.timeline)}
            candidates.sort(key=lambda node: timeline_index.get(node.node_id, 0), reverse=True)
            return candidates[0]
        return self._state.queue_template_node(template_id)

    def _persist_updates(self, *, actions_done: Sequence[ActionDone]) -> None:
        if self._backend is None:
            return
        if actions_done:
            self._backend.save_actions_done(list(actions_done))
        self._backend.save_graphs(
            [
                GraphUpdate(
                    nodes=dict(self._state.nodes),
                    edges=set(self._state.edges),
                )
            ]
        )

    def _action_name_for_node(self, node: ExecutionNode) -> str:
        if node.action is not None:
            return node.action.action_name
        if node.template_id is not None:
            template = self._dag.nodes.get(node.template_id)
            if isinstance(template, ActionCallNode):
                return template.action_name
        return node.label
