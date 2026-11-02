"""Conditional lowering preserves branch-local setup and evaluation order."""

import ast

from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.compiler.errors import UnsupportedPatternError
from waymark.language_syntax import StatementHandler
from waymark.proto import ast_pb2 as ir


class ConditionalHandler(StatementHandler[ast.If]):
    node_types = (ast.If,)

    def validate(self, node: ast.If, context: CompilerContext) -> None:
        """Conditions and bodies are validated by their expression and statement handlers."""

    def lower(self, node: ast.If, context: CompilerContext) -> list[ir.Statement]:
        """Convert if statement to IR.

        Normalizes patterns like:
            if await some_action(...):
                ...
        into:
            __if_cond_n__ = await some_action(...)
            if __if_cond_n__:
                ...
        """

        def normalize_condition(test: ast.expr) -> tuple[list[ir.Statement], ir.Expr | None]:
            action_call = context.action(test)
            if action_call is None:
                return ([], context.expression(test))

            if not isinstance(test, ast.Await):
                line = test.lineno
                col = test.col_offset
                raise UnsupportedPatternError(
                    "Action calls inside boolean expressions are not supported in if conditions",
                    "Assign the awaited action result to a variable, then use the variable in the if condition.",
                    line=line,
                    col=col,
                )

            cond_var = context.next_name("if_cond")
            assign_stmt = ir.Statement(span=make_span(test))
            assign_stmt.assignment.CopyFrom(
                ir.Assignment(
                    targets=[cond_var],
                    value=ir.Expr(action_call=action_call, span=make_span(test)),
                )
            )
            cond_expr = ir.Expr(variable=ir.Variable(name=cond_var), span=make_span(test))
            return ([assign_stmt], cond_expr)

        def visit_body(nodes: list[ast.stmt]) -> list[ir.Statement]:
            stmts: list[ir.Statement] = []
            for body_node in nodes:
                stmts.extend(context.statement(body_node))
            return stmts

        # Collect if/elif branches as (test_expr, body_nodes)
        branches: list[tuple[ast.expr, list[ast.stmt], ast.AST]] = [(node.test, node.body, node)]
        current = node
        while current.orelse and len(current.orelse) == 1 and isinstance(current.orelse[0], ast.If):
            elif_node = current.orelse[0]
            branches.append((elif_node.test, elif_node.body, elif_node))
            current = elif_node

        else_nodes = current.orelse

        normalized: list[
            tuple[list[ir.Statement], ir.Expr | None, list[ir.Statement], ast.AST]
        ] = []
        for test_expr, body_nodes, span_node in branches:
            prefix, cond = normalize_condition(test_expr)
            normalized.append((prefix, cond, visit_body(body_nodes), span_node))

        else_body = visit_body(else_nodes) if else_nodes else []

        # If any non-first branch needs normalization, preserve Python semantics by nesting.
        requires_nested = any(prefix for prefix, _, _, _ in normalized[1:])

        def build_conditional_stmt(
            condition: ir.Expr,
            then_body: list[ir.Statement],
            else_body_statements: list[ir.Statement],
            span_node: ast.AST,
        ) -> ir.Statement:
            conditional_stmt = ir.Statement(span=make_span(span_node))
            if_branch = ir.IfBranch(
                condition=condition,
                block_body=ir.Block(statements=then_body, span=make_span(span_node)),
                span=make_span(span_node),
            )
            conditional = ir.Conditional(if_branch=if_branch)
            if else_body_statements:
                else_branch = ir.ElseBranch(
                    block_body=ir.Block(
                        statements=else_body_statements,
                        span=make_span(span_node),
                    ),
                    span=make_span(span_node),
                )
                conditional.else_branch.CopyFrom(else_branch)
            conditional_stmt.conditional.CopyFrom(conditional)
            return conditional_stmt

        if requires_nested:
            nested_else: list[ir.Statement] = else_body
            for prefix, cond, then_body, span_node in reversed(normalized):
                if cond is None:
                    continue
                nested_if_stmt = build_conditional_stmt(
                    condition=cond,
                    then_body=then_body,
                    else_body_statements=nested_else,
                    span_node=span_node,
                )
                nested_else = [*prefix, nested_if_stmt]
            return nested_else

        # Flat conditional with elif/else (original behavior), plus optional prefix for the if guard.
        if_prefix, if_condition, if_body, if_span_node = normalized[0]
        if if_condition is None:
            return []

        conditional_stmt = ir.Statement(span=make_span(if_span_node))
        if_branch = ir.IfBranch(
            condition=if_condition,
            block_body=ir.Block(statements=if_body, span=make_span(if_span_node)),
            span=make_span(if_span_node),
        )
        conditional = ir.Conditional(if_branch=if_branch)

        for _, elif_condition, elif_body, elif_span_node in normalized[1:]:
            if elif_condition is None:
                continue
            elif_branch = ir.ElifBranch(
                condition=elif_condition,
                block_body=ir.Block(statements=elif_body, span=make_span(elif_span_node)),
                span=make_span(elif_span_node),
            )
            conditional.elif_branches.append(elif_branch)

        if else_body:
            else_branch = ir.ElseBranch(
                block_body=ir.Block(statements=else_body, span=make_span(if_span_node)),
                span=make_span(if_span_node),
            )
            conditional.else_branch.CopyFrom(else_branch)

        conditional_stmt.conditional.CopyFrom(conditional)
        return [*if_prefix, conditional_stmt]
