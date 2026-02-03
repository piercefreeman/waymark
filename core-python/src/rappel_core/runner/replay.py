"""Replay variable values from a runner state snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence
from uuid import UUID

from proto import ast_pb2 as ir

from ..dag import EdgeType, assert_never
from .state import ActionResultValue, FunctionCallValue, RunnerState
from .value_visitor import ValueExprEvaluator


class ReplayError(Exception):
    """Raised when replay cannot reconstruct variable values."""


@dataclass(frozen=True)
class ReplayResult:
    variables: dict[str, Any]


class ReplayEngine:
    """Replay variable values from a runner state snapshot."""

    def __init__(self, state: RunnerState, action_results: Mapping[UUID, Any]) -> None:
        self._state = state
        self._action_results = action_results
        self._cache: dict[tuple[UUID, str], Any] = {}
        self._timeline: Sequence[UUID] = state.timeline or list(state.nodes.keys())
        self._index = {node_id: idx for idx, node_id in enumerate(self._timeline)}
        self._incoming_data = self._build_incoming_data_map()

    def replay_variables(self) -> ReplayResult:
        variables: dict[str, Any] = {}
        for node_id in self._timeline:
            node = self._state.nodes.get(node_id)
            if node is None or not node.assignments:
                continue
            for target in node.assignments:
                variables[target] = self._evaluate_assignment(node_id, target, set())
        return ReplayResult(variables=variables)

    def _evaluate_assignment(
        self,
        node_id: UUID,
        target: str,
        stack: set[tuple[UUID, str]],
    ) -> Any:
        key = (node_id, target)
        if key in self._cache:
            return self._cache[key]
        if key in stack:
            raise ReplayError(f"recursive assignment detected for {target} in {node_id}")

        node = self._state.nodes.get(node_id)
        if node is None or target not in node.assignments:
            raise ReplayError(f"missing assignment for {target} in {node_id}")

        stack.add(key)
        evaluator = ValueExprEvaluator(
            resolve_variable=lambda name: self._resolve_variable(node_id, name, stack),
            resolve_action_result=self._resolve_action_result,
            resolve_function_call=self._evaluate_function_call,
            apply_binary=self._apply_binary,
            apply_unary=self._apply_unary,
            error_factory=ReplayError,
        )
        value = evaluator.visit(node.assignments[target])
        stack.remove(key)
        self._cache[key] = value
        return value

    def _resolve_variable(
        self,
        current_node_id: UUID,
        name: str,
        stack: set[tuple[UUID, str]],
    ) -> Any:
        source_node_id = self._find_variable_source_node(current_node_id, name)
        if source_node_id is None:
            raise ReplayError(f"variable not found via data-flow edges: {name}")
        return self._evaluate_assignment(source_node_id, name, stack)

    def _find_variable_source_node(
        self,
        current_node_id: UUID,
        name: str,
    ) -> Optional[UUID]:
        sources = self._incoming_data.get(current_node_id, ())
        current_idx = self._index.get(current_node_id, len(self._index))
        for source_id in sources:
            if self._index.get(source_id, -1) > current_idx:
                continue
            node = self._state.nodes.get(source_id)
            if node is not None and name in node.assignments:
                return source_id
        return None

    def _resolve_action_result(self, expr: ActionResultValue) -> Any:
        if expr.node_id not in self._action_results:
            raise ReplayError(f"missing action result for {expr.node_id}")
        value = self._action_results[expr.node_id]
        if expr.result_index is None:
            return value
        try:
            return value[expr.result_index]
        except Exception as exc:  # noqa: BLE001
            raise ReplayError(
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

        raise ReplayError(f"cannot replay non-global function call: {expr.name}")

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
                raise ReplayError("len() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE:
                if args:
                    return list(enumerate(args[0]))
                if "items" in kwargs:
                    return list(enumerate(kwargs["items"]))
                raise ReplayError("enumerate() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION:
                if args:
                    return ReplayEngine._is_exception_value(args[0])
                if "value" in kwargs:
                    return ReplayEngine._is_exception_value(kwargs["value"])
                raise ReplayError("isexception() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED:
                raise ReplayError("global function unspecified")
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
                raise ReplayError("binary operator unspecified")
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
                raise ReplayError("unary operator unspecified")
            case _:
                assert_never(op)

    @staticmethod
    def _is_exception_value(value: Any) -> bool:
        if isinstance(value, BaseException):
            return True
        if isinstance(value, dict) and "type" in value and "message" in value:
            return True
        return False

    def _build_incoming_data_map(self) -> dict[UUID, list[UUID]]:
        incoming: dict[UUID, list[UUID]] = {}
        for edge in self._state.edges:
            if edge.edge_type != EdgeType.DATA_FLOW:
                continue
            incoming.setdefault(edge.target, []).append(edge.source)
        for _target, sources in incoming.items():
            sources.sort(
                key=lambda node_id: (self._index.get(node_id, -1), str(node_id)),
                reverse=True,
            )
        return incoming


def replay_variables(
    state: RunnerState,
    action_results: Mapping[UUID, Any],
) -> ReplayResult:
    return ReplayEngine(state, action_results).replay_variables()
