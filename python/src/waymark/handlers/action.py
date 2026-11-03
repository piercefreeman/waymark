"""Registered actions bind source arguments and emit durable dispatches."""

import ast
import inspect
from dataclasses import dataclass
from types import FunctionType
from typing import Any

from waymark.call_binding import bind_call
from waymark.compiler.context import CompilerContext, LoweredCall
from waymark.dependencies import strip_depends_from_signature
from waymark.language_calls import LanguageCallHandler
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class ActionArguments:
    values: dict[str, ast.expr]


class ActionHandler(LanguageCallHandler[ActionArguments]):
    arguments_type = ActionArguments
    recommendation = "Await the registered action directly and pass its declared arguments."

    def __init__(self, function: FunctionType):
        self.function = function
        self.name = function.__name__
        self.signature = strip_depends_from_signature(function)
        original = inspect.unwrap(function)
        assert inspect.isfunction(original)
        self.namespace = original.__globals__

    def parse_arguments(self, node: ast.Call) -> ActionArguments:
        """Bind the decorated action's declared parameters in source evaluation order."""
        bound = bind_call(node, self.signature, self.namespace)
        return ActionArguments(dict(bound.arguments))

    def lower(
        self, arguments: ActionArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Emit a dispatch using the resolved action's registered identity."""
        metadata = vars(self.function)
        call = ir.ActionCall(
            action_name=metadata["__waymark_action_name__"],
            module_name=metadata["__waymark_action_module__"],
        )
        for name, argument in arguments.values.items():
            value = context.expression(argument)
            call.kwargs.append(ir.Kwarg(name=name, value=value))
        return LoweredCall(ir.Expr(action_call=call))


def action_handler(value: Any) -> ActionHandler | None:
    """Recognize the actual decorated function, including aliases and module exports."""
    if inspect.isfunction(value) and "__waymark_action_name__" in vars(value):
        return ActionHandler(value)
    return None
