"""Literal source validation and wire-value lowering."""

import ast

from waymark.compiler_ast import make_span
from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.compiler_errors import UnsupportedPatternError
from waymark.compiler_literals import constant_to_literal, validate_literal
from waymark.compiler_recommendations import RECOMMENDATIONS
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


class LiteralHandler(ExpressionHandler[ast.Constant]):
    node_types = (ast.Constant,)

    def validate(self, node: ast.Constant, context: CompilerContext) -> None:
        """Check this construct against the shared source facts."""
        parent = context.analysis.parents.get(node)
        value = node.value
        if (
            isinstance(parent, ast.UnaryOp)
            and isinstance(parent.op, ast.USub)
            and type(value) in (int, float)
        ):
            value = -value
        try:
            validate_literal(value)
        except UnsupportedPatternError as error:
            reject(node, error.message)

    def lower(self, node: ast.Constant, context: CompilerContext) -> ir.Expr:
        """Encode primitive values only after their representable range has been checked."""
        literal = constant_to_literal(node.value)
        if literal is None:
            raise UnsupportedPatternError(
                f"Unsupported literal type '{type(node.value).__name__}'",
                RECOMMENDATIONS["unsupported_literal"],
                line=node.lineno,
                col=node.col_offset,
            )
        return ir.Expr(literal=literal, span=make_span(node))
