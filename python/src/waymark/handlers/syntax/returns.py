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
        """Normalize returned calls and preserve explicit and implicit None results."""
        if node.value:
            # Check if returning an action call - normalize to assignment + return
            action_call = context.action(node.value)
            if action_call:
                # Create a temporary variable for the action result
                tmp_var = context.next_name("return_tmp")

                # Create assignment: _return_tmp = await action()
                assign_stmt = ir.Statement(span=make_span(node))
                value = ir.Expr(action_call=action_call, span=make_span(node))
                assign = ir.Assignment(targets=[tmp_var], value=value)
                assign_stmt.assignment.CopyFrom(assign)

                # Create return: return _return_tmp
                return_stmt = ir.Statement(span=make_span(node))
                var_expr = ir.Expr(variable=ir.Variable(name=tmp_var), span=make_span(node))
                ret = ir.ReturnStmt(value=var_expr)
                return_stmt.return_stmt.CopyFrom(ret)

                return [assign_stmt, return_stmt]

            # Normalize return of function calls into assignment + return
            expr = context.expression(node.value)
            if expr.HasField("function_call"):
                tmp_var = context.next_name("return_tmp")

                assign_stmt = ir.Statement(span=make_span(node))
                assign_stmt.assignment.CopyFrom(ir.Assignment(targets=[tmp_var], value=expr))

                return_stmt = ir.Statement(span=make_span(node))
                var_expr = ir.Expr(variable=ir.Variable(name=tmp_var), span=make_span(node))
                return_stmt.return_stmt.CopyFrom(ir.ReturnStmt(value=var_expr))
                return [assign_stmt, return_stmt]

            # Regular return with expression (variable, literal, etc.)
            return [ir.Statement(return_stmt=ir.ReturnStmt(value=expr), span=make_span(node))]

        # Return with no value
        stmt = ir.Statement(span=make_span(node))
        stmt.return_stmt.CopyFrom(ir.ReturnStmt())
        return [stmt]
