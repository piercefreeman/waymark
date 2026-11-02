"""The VM length intrinsic's argument contract."""

import ast
from collections.abc import Callable
from dataclasses import dataclass

from waymark.compiler_context import CompilerContext
from waymark.compiler_types import ValueKind
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class LengthArguments:
    obj: ast.expr


class LengthHandler(LanguageCallHandler[LengthArguments]):
    target = len
    name = "len"
    arguments_type = LengthArguments
    positional_only = ("obj",)
    requires_await = False
    recommendation = "Use len(collection); compute string lengths inside an action."

    def validate_usage(
        self, node: ast.Call, parent: ast.AST | None, kind: Callable[[ast.expr | None], ValueKind]
    ) -> None:
        arguments = self.parse_arguments(node)
        if kind(arguments.obj) in {"str", "exception"} and not (
            isinstance(arguments.obj, ast.Constant) and isinstance(arguments.obj.value, str)
        ):
            raise self.error(node, "strings or caught exceptions are unsupported by the VM")

    def lower(
        self, arguments: LengthArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        if isinstance(arguments.obj, ast.Constant) and isinstance(arguments.obj.value, str):
            return LoweredCall(ir.Expr(literal=ir.Literal(int_value=len(arguments.obj.value))))
        value = context.expression(arguments.obj)
        assert value is not None
        return LoweredCall(
            ir.Expr(
                function_call=ir.FunctionCall(
                    name="len", global_function=ir.GlobalFunction.GLOBAL_FUNCTION_LEN, args=[value]
                )
            )
        )
