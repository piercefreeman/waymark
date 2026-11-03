"""Append is supported only for a proven fresh, unaliased local list."""

import ast
from dataclasses import dataclass

from waymark.compiler.context import CompilerContext
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class AppendArguments:
    object: ast.expr


class AppendHandler(LanguageCallHandler[AppendArguments]):
    target = list.append
    name = "list.append"
    arguments_type = AppendArguments
    positional_only = ("object",)
    requires_await = False
    recommendation = (
        "Append only to a fresh local list; use an action for mutation through aliases."
    )

    def lower(
        self, arguments: AppendArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Replace a proven exclusive local list with its contents plus the appended value."""
        if not isinstance(node.func, ast.Attribute) or not isinstance(node.func.value, ast.Name):
            raise self.error(node, "requires a local list")
        value = context.expression(arguments.object)
        name = node.func.value.id
        if name not in context.analysis.fresh_lists - context.analysis.aliased:
            raise self.error(node, "requires a fresh, unaliased local list")
        assignment = ir.Statement(
            assignment=ir.Assignment(
                targets=[name],
                value=ir.Expr(
                    binary_op=ir.BinaryOp(
                        op=ir.BinaryOperator.BINARY_OP_ADD,
                        left=ir.Expr(variable=ir.Variable(name=name)),
                        right=ir.Expr(list=ir.ListExpr(elements=[value])),
                    )
                ),
            )
        )
        return LoweredCall(ir.Expr(literal=ir.Literal(is_none=True)), [assignment])
