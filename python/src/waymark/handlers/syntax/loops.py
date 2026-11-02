"""Loop clauses and lowering share one syntax handler."""

import ast

from waymark.compiler_ast import make_span
from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.compiler_targets import assignment_targets
from waymark.language_syntax import StatementHandler
from waymark.proto import ast_pb2 as ir


class LoopHandler(StatementHandler[ast.For | ast.While | ast.comprehension]):
    node_types = (ast.For, ast.While, ast.comprehension)

    def validate(
        self, node: ast.For | ast.While | ast.comprehension, context: CompilerContext
    ) -> None:
        """Reject ignored loop clauses and iteration kinds the VM cannot preserve."""
        if isinstance(node, ast.While):
            if node.orelse:
                reject(node, "while/else is unsupported")
            return
        if isinstance(node, ast.For):
            if node.orelse:
                reject(node, "for/else is unsupported")
            assignment_targets([node.target])
        if context.analysis.kind(node.iter) in {"dict", "str", "exception"}:
            reject(node.iter, "Iteration over dictionaries, strings, or exceptions is unsupported")

    def lower(
        self, node: ast.For | ast.While | ast.comprehension, context: CompilerContext
    ) -> list[ir.Statement]:
        """Emit full loop bodies; comprehension clauses are lowered by their owning expression."""
        assert isinstance(node, (ast.For, ast.While))
        if isinstance(node, ast.For):
            iterable = context.expression(node.iter)
            assert iterable is not None
            return [
                ir.Statement(
                    for_loop=ir.ForLoop(
                        loop_vars=assignment_targets([node.target]),
                        iterable=iterable,
                        block_body=context.block(node.body, node),
                    ),
                    span=make_span(node),
                )
            ]
        condition = context.expression(node.test)
        assert condition is not None
        return [
            ir.Statement(
                while_loop=ir.WhileLoop(
                    condition=condition,
                    block_body=context.block(node.body, node),
                ),
                span=make_span(node),
            )
        ]
