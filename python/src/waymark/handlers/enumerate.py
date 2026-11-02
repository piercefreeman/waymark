"""Enumerate currently supports the VM's zero-based loop form."""

import ast
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from waymark.compiler_context import CompilerContext
from waymark.compiler_types import ValueKind
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class EnumerateArguments:
    iterable: ast.expr
    start: Literal[0] = 0


class EnumerateHandler(LanguageCallHandler[EnumerateArguments]):
    target = enumerate
    name = "enumerate"
    arguments_type = EnumerateArguments
    positional_only = ("iterable",)
    requires_await = False
    recommendation = "Use enumerate(items) directly in a for loop; only start=0 is supported."

    def validate_usage(
        self, node: ast.Call, parent: ast.AST | None, kind: Callable[[ast.expr | None], ValueKind]
    ) -> None:
        arguments = self.parse_arguments(node)
        if not isinstance(parent, (ast.For, ast.comprehension)) or parent.iter is not node:
            raise self.error(node, "supported only directly in a for loop")
        if kind(arguments.iterable) in {"dict", "str", "exception"}:
            raise self.error(node, "requires a sequence supported by the VM")

    def lower(
        self, arguments: EnumerateArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        value = context.expression(arguments.iterable)
        assert value is not None
        return LoweredCall(
            ir.Expr(
                function_call=ir.FunctionCall(
                    name="enumerate",
                    global_function=ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE,
                    args=[value],
                )
            )
        )
