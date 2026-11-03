"""Source-located diagnostics shared by compiler handlers."""

import ast
from typing import NoReturn

from waymark.compiler.errors import UnsupportedPatternError


def reject(node: ast.expr | ast.stmt | ast.ExceptHandler, message: str) -> NoReturn:
    """Report an unsupported source construct at its original location."""
    raise UnsupportedPatternError(
        message,
        "Move this operation into an action, or rewrite it using supported explicit statements.",
        line=node.lineno,
        col=node.col_offset,
    )
