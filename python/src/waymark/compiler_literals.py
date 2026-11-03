"""Convert supported constant defaults without executing constructors or factories."""

import ast
import math
from typing import Any

from waymark.compiler_errors import UnsupportedPatternError


def literal_expression(value: Any, location: ast.expr) -> ast.expr:
    """Build AST for primitive/container defaults; never invoke user factories or hooks."""
    if value is None or type(value) in (str, int, float, bool):
        expression: ast.expr = ast.Constant(value=value)
    elif type(value) in (list, tuple):
        expression = ast.List(
            elts=[literal_expression(item, location) for item in value], ctx=ast.Load()
        )
    elif type(value) is dict and all(type(key) is str for key in value):
        expression = ast.Dict(
            keys=[ast.Constant(value=key) for key in value],
            values=[literal_expression(item, location) for item in value.values()],
        )
    else:
        raise UnsupportedPatternError(
            f"Unsupported default value of type {type(value).__name__}",
            "Pass the value explicitly or construct it inside an action.",
            line=location.lineno,
            col=location.col_offset,
        )
    return ast.fix_missing_locations(ast.copy_location(expression, location))


def validate_literal(value: Any) -> None:
    """Wire literals must fit the VM's finite, signed 64-bit value representation."""
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise UnsupportedPatternError(
            "Integer literal is outside the signed 64-bit range",
            "Use a string for larger integers, and compute inside an action.",
        )
    if type(value) is float and not math.isfinite(value):
        raise UnsupportedPatternError(
            "Non-finite float literals are unsupported", "Use finite numbers."
        )
