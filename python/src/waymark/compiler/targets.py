"""Shared binding targets for assignments, loops, and comprehensions."""

import ast

from waymark.compiler.errors import UnsupportedPatternError


def assignment_targets(targets: list[ast.expr]) -> list[str]:
    """Get all target variable names from assignment targets (including tuple unpacking)."""
    result: list[str] = []
    for target in targets:
        if isinstance(target, ast.Name):
            result.append(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)) and all(
            isinstance(element, ast.Name) for element in target.elts
        ):
            result.extend(element.id for element in target.elts if isinstance(element, ast.Name))
        else:
            raise UnsupportedPatternError(
                "Unsupported assignment or loop target",
                "Use a name or flat list/tuple of names.",
                line=target.lineno,
                col=target.col_offset,
            )
    return result
