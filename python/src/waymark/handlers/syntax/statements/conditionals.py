"""Conditional lowering preserves branch-local setup and evaluation order."""

import ast

from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.language_syntax import StatementHandler
from waymark.proto import ast_pb2 as ir


class ConditionalHandler(StatementHandler[ast.If]):
    node_types = (ast.If,)

    def validate(self, node: ast.If, context: CompilerContext) -> None:
        """Conditions and bodies are validated by their expression and statement handlers."""

    def lower(self, node: ast.If, context: CompilerContext) -> list[ir.Statement]:
        """Keep Python's nested elif form so each guard's setup stays in its own branch."""
        lowered = context.lower_value(node.test)
        statements = list(lowered.statements)
        condition = lowered.value
        if isinstance(node.test, ast.Await):
            temporary = context.next_name("if_cond")
            statements.append(
                ir.Statement(
                    assignment=ir.Assignment(targets=[temporary], value=condition),
                    span=make_span(node.test),
                )
            )
            condition = ir.Expr(variable=ir.Variable(name=temporary), span=make_span(node.test))

        conditional = ir.Conditional(
            if_branch=ir.IfBranch(
                condition=condition,
                block_body=context.block(node.body, node),
                span=make_span(node),
            )
        )
        if node.orelse:
            conditional.else_branch.CopyFrom(
                ir.ElseBranch(block_body=context.block(node.orelse, node), span=make_span(node))
            )
        return [*statements, ir.Statement(conditional=conditional, span=make_span(node))]
