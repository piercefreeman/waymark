"""Arithmetic support rules and expression lowering."""

import ast

from waymark.compiler_ast import make_span
from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.compiler_literals import constant_to_literal
from waymark.compiler_operators import binary_operator, unary_operator
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


class ArithmeticHandler(ExpressionHandler[ast.BinOp | ast.UnaryOp | ast.BoolOp]):
    node_types = (ast.BinOp, ast.UnaryOp, ast.BoolOp)

    def validate(
        self, node: ast.BinOp | ast.UnaryOp | ast.BoolOp, context: CompilerContext
    ) -> None:
        """Check this construct against the shared source facts."""
        if isinstance(node, ast.BoolOp):
            reject(
                node, "Short-circuit and/or expressions are unsupported; use nested if statements"
            )
        if isinstance(node, ast.UnaryOp):
            return
        assert isinstance(node, ast.BinOp)
        if not isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod)):
            reject(node, "Unsupported binary or augmented operator")
        left, right = context.analysis.kind(node.left), context.analysis.kind(node.right)
        numeric = {"int", "float", "bool"}
        if left in numeric and right in numeric and (left != right or "bool" in (left, right)):
            reject(node, "Mixed numeric types and boolean arithmetic require VM support")
        if isinstance(node.op, ast.Div) and (left == "int" or right == "int"):
            reject(node, "Integer true division requires VM support")
        if (
            isinstance(node.op, (ast.Div, ast.FloorDiv, ast.Mod))
            and right == "float"
            and isinstance(node.right, ast.Constant)
            and node.right.value == 0
        ):
            reject(node, "Float division by zero requires VM exception support")
        if isinstance(node.op, ast.Mult) and ({left, right} & {"str", "list", "tuple"}):
            reject(node, "Sequence repetition requires VM support")

    def lower(
        self, node: ast.BinOp | ast.UnaryOp | ast.BoolOp, context: CompilerContext
    ) -> ir.Expr:
        """Preserve operand evaluation and signed literal folding in the emitted IR."""
        if isinstance(node, ast.BoolOp):
            reject(
                node, "Short-circuit and/or expressions are unsupported; use nested if statements"
            )
        if isinstance(node, ast.BinOp):
            left = context.expression(node.left)
            right = context.expression(node.right)
            operator = binary_operator(node.op)
            if operator is None:
                reject(node, "Unsupported binary or augmented operator")
            assert left is not None and right is not None
            return ir.Expr(
                binary_op=ir.BinaryOp(left=left, op=operator, right=right), span=make_span(node)
            )
        assert isinstance(node, ast.UnaryOp)
        if (
            isinstance(node.op, ast.USub)
            and isinstance(node.operand, ast.Constant)
            and type(node.operand.value) in (int, float)
        ):
            return ir.Expr(literal=constant_to_literal(-node.operand.value), span=make_span(node))
        operand = context.expression(node.operand)
        operator = unary_operator(node.op)
        if operator is None:
            reject(node, "Unsupported unary operator")
        assert operand is not None
        return ir.Expr(unary_op=ir.UnaryOp(op=operator, operand=operand), span=make_span(node))
