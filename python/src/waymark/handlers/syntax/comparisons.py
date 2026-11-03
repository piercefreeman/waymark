"""Comparison semantics and lowering live with their syntax handler."""

import ast

from waymark.compiler_ast import make_span
from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


def comparison_operator(op: ast.cmpop) -> ir.BinaryOperator | None:
    """Convert Python comparison operator to IR BinaryOperator."""
    mapping = {
        ast.Eq: ir.BinaryOperator.BINARY_OP_EQ,
        ast.NotEq: ir.BinaryOperator.BINARY_OP_NE,
        ast.Lt: ir.BinaryOperator.BINARY_OP_LT,
        ast.LtE: ir.BinaryOperator.BINARY_OP_LE,
        ast.Gt: ir.BinaryOperator.BINARY_OP_GT,
        ast.GtE: ir.BinaryOperator.BINARY_OP_GE,
        ast.In: ir.BinaryOperator.BINARY_OP_IN,
        ast.NotIn: ir.BinaryOperator.BINARY_OP_NOT_IN,
        # Identity comparisons are treated as equality in waymark since we deal
        # with serialized values where object identity doesn't apply
        ast.Is: ir.BinaryOperator.BINARY_OP_EQ,
        ast.IsNot: ir.BinaryOperator.BINARY_OP_NE,
    }
    return mapping.get(type(op))


class ComparisonHandler(ExpressionHandler[ast.Compare]):
    node_types = (ast.Compare,)

    def validate(self, node: ast.Compare, context: CompilerContext) -> None:
        """Check this construct against the shared source facts."""
        if len(node.ops) != 1:
            reject(node, "Chained comparisons are unsupported")
        if context.analysis.kind(node.left) in {
            "list",
            "tuple",
            "dict",
        } or context.analysis.kind(node.comparators[0]) in {
            "list",
            "tuple",
            "dict",
        }:
            reject(node, "Collection comparisons are unsupported")
        left, right = (
            context.analysis.kind(node.left),
            context.analysis.kind(node.comparators[0]),
        )
        if left in {"int", "float", "bool"} and right in {"int", "float", "bool"} and left != right:
            reject(node, "Mixed numeric comparisons require VM support")

    def lower(self, node: ast.Compare, context: CompilerContext) -> ir.Expr:
        """Emit the supported single comparison with recursively compiled operands."""
        left = context.expression(node.left)
        right = context.expression(node.comparators[0])
        operator = comparison_operator(node.ops[0])
        if operator is None:
            reject(node, "Unsupported comparison operator")
        assert left is not None and right is not None
        return ir.Expr(
            binary_op=ir.BinaryOp(left=left, op=operator, right=right), span=make_span(node)
        )
