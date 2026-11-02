"""List and tuple literals lower through the same sequence representation."""

import ast

from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


class SequenceHandler(ExpressionHandler[ast.List | ast.Tuple]):
    node_types = (ast.List, ast.Tuple)

    def validate(self, node: ast.List | ast.Tuple, context: CompilerContext) -> None:
        """Each element is validated by its own expression handler."""

    def lower(self, node: ast.List | ast.Tuple, context: CompilerContext) -> ir.Expr:
        """Emit elements in order, concatenating starred list segments where supported."""
        if isinstance(node, ast.List) and any(
            isinstance(element, ast.Starred) for element in node.elts
        ):
            segments: list[ir.Expr] = []
            buffered: list[ir.Expr] = []

            def flush() -> None:
                if buffered:
                    segments.append(ir.Expr(list=ir.ListExpr(elements=list(buffered))))
                    buffered.clear()

            for element in node.elts:
                if isinstance(element, ast.Starred):
                    flush()
                    value = context.expression(element.value)
                    assert value is not None
                    segments.append(value)
                else:
                    value = context.expression(element)
                    assert value is not None
                    buffered.append(value)
            flush()
            combined = segments[0]
            for segment in segments[1:]:
                combined = ir.Expr(
                    binary_op=ir.BinaryOp(
                        left=combined, op=ir.BinaryOperator.BINARY_OP_ADD, right=segment
                    )
                )
            return combined
        elements = [context.expression(element) for element in node.elts]
        assert all(element is not None for element in elements)
        return ir.Expr(
            list=ir.ListExpr(elements=[element for element in elements if element is not None]),
            span=make_span(node),
        )
