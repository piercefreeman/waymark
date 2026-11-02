"""Validate the range forms the VM can lower."""

import ast
from collections.abc import Callable
from dataclasses import dataclass

from waymark.compiler.context import CompilerContext
from waymark.compiler.types import ValueKind
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class RangeArguments:
    bounds: tuple[ast.expr, ...]


class RangeHandler(LanguageCallHandler[RangeArguments]):
    target = range
    name = "range"
    arguments_type = RangeArguments
    variadic_argument = "bounds"
    requires_await = False
    recommendation = (
        "Use range(stop), range(start, stop), or range(start, stop, step) with integer bounds."
    )

    def validate_usage(
        self, node: ast.Call, parent: ast.AST | None, kind: Callable[[ast.expr | None], ValueKind]
    ) -> None:
        """Restrict ranges to integer bounds in supported loop, assignment, and return positions."""
        arguments = self.parse_arguments(node)
        if any(kind(argument) not in {"unknown", "int"} for argument in arguments.bounds):
            raise self.error(node, "bounds must be integers")
        if not isinstance(
            parent, (ast.For, ast.comprehension, ast.Assign, ast.AnnAssign, ast.Return)
        ):
            raise self.error(node, "supported only as a loop iterable, assignment, or return")

    def lower(
        self, arguments: RangeArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Bind one to three bounds, reject a literal zero step, and emit the range intrinsic."""
        if not 1 <= len(arguments.bounds) <= 3:
            raise self.error(node, "expected one to three positional arguments")
        for argument in arguments.bounds:
            if isinstance(argument, ast.Starred):
                raise self.error(argument, "positional unpacking is unsupported")
            if isinstance(argument, ast.Constant) and type(argument.value) is not int:
                raise self.error(argument, "bounds must be integers")
        if (
            len(arguments.bounds) == 3
            and isinstance(arguments.bounds[2], ast.Constant)
            and arguments.bounds[2].value == 0
        ):
            raise self.error(arguments.bounds[2], "step cannot be zero")
        values = [context.expression(argument) for argument in arguments.bounds]
        assert all(value is not None for value in values)
        return LoweredCall(
            ir.Expr(
                function_call=ir.FunctionCall(
                    name="range",
                    global_function=ir.GlobalFunction.GLOBAL_FUNCTION_RANGE,
                    args=values,
                )
            )
        )
