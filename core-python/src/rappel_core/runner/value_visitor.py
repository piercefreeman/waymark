"""Shared ValueExpr visitors for traversal, resolution, and evaluation."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Mapping,
    Optional,
    Sequence,
    TypeAlias,
    TypeVar,
)
from uuid import UUID

from ..dag import assert_never

if TYPE_CHECKING:  # pragma: no cover - type checkers only
    from .state import (  # noqa: F401
        ActionResultValue,
        BinaryOpValue,
        DictValue,
        DotValue,
        FunctionCallValue,
        IndexValue,
        ListValue,
        LiteralValue,
        SpreadValue,
        UnaryOpValue,
        VariableValue,
    )

ValueExpr: TypeAlias = (
    "LiteralValue | VariableValue | ActionResultValue | BinaryOpValue | UnaryOpValue | ListValue | "
    "DictValue | IndexValue | DotValue | FunctionCallValue | SpreadValue"
)

TExpr = TypeVar("TExpr")


class ValueExprVisitor(Generic[TExpr]):
    """Visit ValueExpr nodes with explicit handlers.

    Example:
    - A visitor can count nodes in `a + 1` by visiting a BinaryOpValue and its children.
    """

    def visit(self, expr: "ValueExpr") -> TExpr:
        from .state import (  # local import to avoid circular dependency
            ActionResultValue,
            BinaryOpValue,
            DictValue,
            DotValue,
            FunctionCallValue,
            IndexValue,
            ListValue,
            LiteralValue,
            SpreadValue,
            UnaryOpValue,
            VariableValue,
        )

        if isinstance(expr, LiteralValue):
            return self.visit_literal(expr)
        if isinstance(expr, VariableValue):
            return self.visit_variable(expr)
        if isinstance(expr, ActionResultValue):
            return self.visit_action_result(expr)
        if isinstance(expr, BinaryOpValue):
            return self.visit_binary(expr)
        if isinstance(expr, UnaryOpValue):
            return self.visit_unary(expr)
        if isinstance(expr, ListValue):
            return self.visit_list(expr)
        if isinstance(expr, DictValue):
            return self.visit_dict(expr)
        if isinstance(expr, IndexValue):
            return self.visit_index(expr)
        if isinstance(expr, DotValue):
            return self.visit_dot(expr)
        if isinstance(expr, FunctionCallValue):
            return self.visit_function_call(expr)
        if isinstance(expr, SpreadValue):
            return self.visit_spread(expr)
        assert_never(expr)

    def visit_literal(self, expr: "LiteralValue") -> TExpr:
        raise NotImplementedError

    def visit_variable(self, expr: "VariableValue") -> TExpr:
        raise NotImplementedError

    def visit_action_result(self, expr: "ActionResultValue") -> TExpr:
        raise NotImplementedError

    def visit_binary(self, expr: "BinaryOpValue") -> TExpr:
        raise NotImplementedError

    def visit_unary(self, expr: "UnaryOpValue") -> TExpr:
        raise NotImplementedError

    def visit_list(self, expr: "ListValue") -> TExpr:
        raise NotImplementedError

    def visit_dict(self, expr: "DictValue") -> TExpr:
        raise NotImplementedError

    def visit_index(self, expr: "IndexValue") -> TExpr:
        raise NotImplementedError

    def visit_dot(self, expr: "DotValue") -> TExpr:
        raise NotImplementedError

    def visit_function_call(self, expr: "FunctionCallValue") -> TExpr:
        raise NotImplementedError

    def visit_spread(self, expr: "SpreadValue") -> TExpr:
        raise NotImplementedError


class ValueExprResolver(ValueExprVisitor["ValueExpr"]):
    """Resolve variables inside a ValueExpr tree without executing actions.

    Example IR:
    - y = x + 1 (where x -> LiteralValue(2))
    Produces BinaryOpValue(LiteralValue(2), +, LiteralValue(1)).
    """

    def __init__(
        self,
        resolve_variable: Callable[[str, set[str]], "ValueExpr"],
        seen: set[str],
    ) -> None:
        self._resolve_variable = resolve_variable
        self._seen = seen

    def visit_literal(self, expr: "LiteralValue") -> "ValueExpr":
        return expr

    def visit_variable(self, expr: "VariableValue") -> "ValueExpr":
        return self._resolve_variable(expr.name, self._seen)

    def visit_action_result(self, expr: "ActionResultValue") -> "ValueExpr":
        return expr

    def visit_binary(self, expr: "BinaryOpValue") -> "ValueExpr":
        from .state import BinaryOpValue

        return BinaryOpValue(
            left=self.visit(expr.left),
            op=expr.op,
            right=self.visit(expr.right),
        )

    def visit_unary(self, expr: "UnaryOpValue") -> "ValueExpr":
        from .state import UnaryOpValue

        return UnaryOpValue(
            op=expr.op,
            operand=self.visit(expr.operand),
        )

    def visit_list(self, expr: "ListValue") -> "ValueExpr":
        from .state import ListValue

        return ListValue(elements=tuple(self.visit(item) for item in expr.elements))

    def visit_dict(self, expr: "DictValue") -> "ValueExpr":
        from .state import DictEntryValue, DictValue

        return DictValue(
            entries=tuple(
                DictEntryValue(
                    key=self.visit(entry.key),
                    value=self.visit(entry.value),
                )
                for entry in expr.entries
            )
        )

    def visit_index(self, expr: "IndexValue") -> "ValueExpr":
        from .state import IndexValue

        return IndexValue(
            object=self.visit(expr.object),
            index=self.visit(expr.index),
        )

    def visit_dot(self, expr: "DotValue") -> "ValueExpr":
        from .state import DotValue

        return DotValue(
            object=self.visit(expr.object),
            attribute=expr.attribute,
        )

    def visit_function_call(self, expr: "FunctionCallValue") -> "ValueExpr":
        from .state import FunctionCallValue

        return FunctionCallValue(
            name=expr.name,
            args=tuple(self.visit(arg) for arg in expr.args),
            kwargs={name: self.visit(value) for name, value in expr.kwargs.items()},
            global_function=expr.global_function,
        )

    def visit_spread(self, expr: "SpreadValue") -> "ValueExpr":
        from .state import ActionCallSpec, SpreadValue

        collection = self.visit(expr.collection)
        kwargs = {name: self.visit(value) for name, value in expr.action.kwargs.items()}
        action = ActionCallSpec(
            action_name=expr.action.action_name,
            module_name=expr.action.module_name,
            kwargs=kwargs,
        )
        return SpreadValue(collection=collection, loop_var=expr.loop_var, action=action)


class ValueExprSourceCollector(ValueExprVisitor[set[UUID]]):
    """Collect execution node ids that supply data to a ValueExpr tree.

    Example IR:
    - total = a + @sum(values)
    Returns the node ids that last defined `a` and the action node for sum().
    """

    def __init__(self, resolve_variable: Callable[[str], Optional[UUID]]) -> None:
        self._resolve_variable = resolve_variable

    def visit_literal(self, expr: "LiteralValue") -> set[UUID]:
        return set()

    def visit_variable(self, expr: "VariableValue") -> set[UUID]:
        source = self._resolve_variable(expr.name)
        return {source} if source is not None else set()

    def visit_action_result(self, expr: "ActionResultValue") -> set[UUID]:
        return {expr.node_id}

    def visit_binary(self, expr: "BinaryOpValue") -> set[UUID]:
        return self.visit(expr.left) | self.visit(expr.right)

    def visit_unary(self, expr: "UnaryOpValue") -> set[UUID]:
        return self.visit(expr.operand)

    def visit_list(self, expr: "ListValue") -> set[UUID]:
        sources: set[UUID] = set()
        for item in expr.elements:
            sources.update(self.visit(item))
        return sources

    def visit_dict(self, expr: "DictValue") -> set[UUID]:
        sources: set[UUID] = set()
        for entry in expr.entries:
            sources.update(self.visit(entry.key))
            sources.update(self.visit(entry.value))
        return sources

    def visit_index(self, expr: "IndexValue") -> set[UUID]:
        return self.visit(expr.object) | self.visit(expr.index)

    def visit_dot(self, expr: "DotValue") -> set[UUID]:
        return self.visit(expr.object)

    def visit_function_call(self, expr: "FunctionCallValue") -> set[UUID]:
        sources: set[UUID] = set()
        for arg in expr.args:
            sources.update(self.visit(arg))
        for arg in expr.kwargs.values():
            sources.update(self.visit(arg))
        return sources

    def visit_spread(self, expr: "SpreadValue") -> set[UUID]:
        sources = self.visit(expr.collection)
        for arg in expr.action.kwargs.values():
            sources.update(self.visit(arg))
        return sources


class ValueExprEvaluator(ValueExprVisitor[Any]):
    """Evaluate ValueExpr nodes into concrete Python values.

    Example:
    - BinaryOpValue(VariableValue("a"), +, LiteralValue(1)) becomes the
      current value of a plus 1.
    """

    def __init__(
        self,
        resolve_variable: Callable[[str], Any],
        resolve_action_result: Callable[["ActionResultValue"], Any],
        resolve_function_call: Callable[["FunctionCallValue", Sequence[Any], Mapping[str, Any]], Any],
        apply_binary: Callable[[Any, Any, Any], Any],
        apply_unary: Callable[[Any, Any], Any],
        error_factory: Callable[[str], Exception],
    ) -> None:
        self._resolve_variable = resolve_variable
        self._resolve_action_result = resolve_action_result
        self._resolve_function_call = resolve_function_call
        self._apply_binary = apply_binary
        self._apply_unary = apply_unary
        self._error_factory = error_factory

    def visit_literal(self, expr: "LiteralValue") -> Any:
        return expr.value

    def visit_variable(self, expr: "VariableValue") -> Any:
        return self._resolve_variable(expr.name)

    def visit_action_result(self, expr: "ActionResultValue") -> Any:
        return self._resolve_action_result(expr)

    def visit_binary(self, expr: "BinaryOpValue") -> Any:
        return self._apply_binary(expr.op, self.visit(expr.left), self.visit(expr.right))

    def visit_unary(self, expr: "UnaryOpValue") -> Any:
        return self._apply_unary(expr.op, self.visit(expr.operand))

    def visit_list(self, expr: "ListValue") -> Any:
        return [self.visit(item) for item in expr.elements]

    def visit_dict(self, expr: "DictValue") -> Any:
        return {self.visit(entry.key): self.visit(entry.value) for entry in expr.entries}

    def visit_index(self, expr: "IndexValue") -> Any:
        obj = self.visit(expr.object)
        idx = self.visit(expr.index)
        return obj[idx]

    def visit_dot(self, expr: "DotValue") -> Any:
        obj = self.visit(expr.object)
        if isinstance(obj, dict):
            if expr.attribute in obj:
                return obj[expr.attribute]
            raise self._error_factory(f"dict has no key '{expr.attribute}'")
        try:
            return object.__getattribute__(obj, expr.attribute)
        except AttributeError as exc:
            raise self._error_factory(f"attribute '{expr.attribute}' not found") from exc

    def visit_function_call(self, expr: "FunctionCallValue") -> Any:
        args = [self.visit(arg) for arg in expr.args]
        kwargs = {name: self.visit(value) for name, value in expr.kwargs.items()}
        return self._resolve_function_call(expr, args, kwargs)

    def visit_spread(self, expr: "SpreadValue") -> Any:
        raise self._error_factory("cannot replay unresolved spread expression")
