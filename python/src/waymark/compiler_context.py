"""Shared compiler operations available to language-call handlers."""

import ast
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from waymark.language_calls import LanguageCallHandler

from waymark.compiler_errors import UnsupportedPatternError
from waymark.proto import ast_pb2 as ir

GLOBAL_FUNCTIONS = {
    "enumerate": ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE,
    "isexception": ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION,
    "len": ir.GlobalFunction.GLOBAL_FUNCTION_LEN,
    "range": ir.GlobalFunction.GLOBAL_FUNCTION_RANGE,
}
ALLOWED_SYNC_FUNCTIONS = set(GLOBAL_FUNCTIONS)


@dataclass
class CompilerContext:
    """Compile nested syntax using the current workflow's symbols and temporary names."""

    expression: Callable[[ast.expr], ir.Expr | None]
    statement: Callable[[ast.stmt], list[ir.Statement]]
    action: Callable[[ast.expr], ir.ActionCall | None]
    function: Callable[[ast.Call], ir.FunctionCall | None]
    next_name: Callable[[str], str]
    function_names: Collection[str]
    resolve: Callable[[ast.expr], "LanguageCallHandler[Any] | None"]
    namespace: Mapping[str, Any]
    instance_attrs: Mapping[str, ast.expr] = field(default_factory=dict)
    ambiguous_instance_attrs: set[str] = field(default_factory=set)

    def call(self, node: ast.Call) -> ir.Call | None:
        """Lower a registered action or workflow helper, including run_action policies."""
        handler = self.resolve(node.func)
        if handler is not None:
            if not handler.requires_await:
                raise handler.error(node, "expected an awaitable")
            lowered = handler.compile(node, self)
            if not lowered.statements:
                if lowered.value.HasField("action_call"):
                    return ir.Call(action=lowered.value.action_call)
                if lowered.value.HasField("function_call"):
                    return ir.Call(function=lowered.value.function_call)
            return None

        if is_run_action_call(node) and (not node.args or not isinstance(node.args[0], ast.Call)):
            raise UnsupportedPatternError(
                "run_action() requires an action call",
                "Use self.run_action(action(...), retry=..., timeout=...).",
                line=node.lineno,
                col=node.col_offset,
            )

        awaited = ast.copy_location(ast.Await(value=node), node)
        action = self.action(awaited)
        if action is not None:
            return ir.Call(action=action)

        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
            and node.func.attr in self.function_names
        ):
            function = self.function(node)
            if function is not None:
                return ir.Call(function=function)
        return None


def is_run_action_call(node: ast.Call) -> bool:
    """Recognize the action-policy wrapper shared by awaited calls and handlers."""
    return isinstance(node.func, ast.Attribute) and node.func.attr == "run_action"
