"""Return statements own result normalization and return emission."""

import ast

from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.language_syntax import StatementHandler
from waymark.proto import ast_pb2 as ir


class ReturnHandler(StatementHandler[ast.Return]):
    node_types = (ast.Return,)

    def validate(self, node: ast.Return, context: CompilerContext) -> None:
        """Returned values are checked by their expression and call handlers."""

    def lower(self, node: ast.Return, context: CompilerContext) -> list[ir.Statement]:
        """Run setup before the result and materialize calls before returning from this function."""
        if node.value is None:
            return [ir.Statement(return_stmt=ir.ReturnStmt(), span=make_span(node))]
        lowered = context.lower_value(node.value)
        statements = list(lowered.statements)
        value = lowered.value
        if isinstance(node.value, ast.Await) or value.HasField("function_call"):
            temporary = context.next_name("return_tmp")
            statements.append(
                ir.Statement(
                    assignment=ir.Assignment(targets=[temporary], value=value), span=make_span(node)
                )
            )
            value = ir.Expr(variable=ir.Variable(name=temporary), span=make_span(node))
        statements.append(
            ir.Statement(return_stmt=ir.ReturnStmt(value=value), span=make_span(node))
        )
        return statements
