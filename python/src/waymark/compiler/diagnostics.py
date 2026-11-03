"""Source-located diagnostics shared by compiler handlers."""

import ast
from typing import NoReturn

from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.recommendations import RECOMMENDATIONS, RecommendationKey

_UNSUPPORTED_SYNTAX: dict[type[ast.AST], tuple[str, RecommendationKey]] = {
    ast.With: ("Context managers (with statements) are not supported", "with_statement"),
    ast.AsyncWith: ("Context managers (with statements) are not supported", "with_statement"),
    ast.Raise: ("The 'raise' statement is not supported", "raise_statement"),
    ast.Assert: ("Assert statements are not supported", "assert_statement"),
    ast.Delete: ("The 'del' statement is not supported", "delete"),
    ast.Global: ("Global statements are not supported", "global_statement"),
    ast.Nonlocal: ("Nonlocal statements are not supported", "nonlocal_statement"),
    ast.Import: ("Import statements inside workflow run() are not supported", "import_statement"),
    ast.ImportFrom: (
        "Import statements inside workflow run() are not supported",
        "import_statement",
    ),
    ast.ClassDef: ("Class definitions inside workflow run() are not supported", "class_def"),
    ast.FunctionDef: ("Nested function definitions are not supported", "function_def"),
    ast.AsyncFunctionDef: ("Nested function definitions are not supported", "function_def"),
    ast.Match: ("Match statements are not supported", "match"),
    ast.JoinedStr: ("F-strings are not supported", "fstring"),
    ast.Lambda: ("Lambda expressions are not supported", "lambda"),
    ast.ListComp: ("List comprehensions are not supported in this context", "list_comprehension"),
    ast.SetComp: ("Set comprehensions are not supported", "set_comprehension"),
    ast.GeneratorExp: ("Generator expressions are not supported", "generator"),
    ast.NamedExpr: ("The walrus operator (:=) is not supported", "walrus"),
    ast.Yield: ("Yield expressions are not supported", "yield_statement"),
    ast.YieldFrom: ("Yield expressions are not supported", "yield_statement"),
}


def reject(
    node: ast.expr | ast.stmt | ast.ExceptHandler,
    message: str,
    recommendation: str = "Move this operation into an action, or rewrite it using supported explicit statements.",
) -> NoReturn:
    """Report an unsupported source construct at its original location."""
    raise UnsupportedPatternError(message, recommendation, line=node.lineno, col=node.col_offset)


def reject_unsupported(node: ast.expr | ast.stmt) -> NoReturn:
    """Use the syntax's diagnostic, retaining a rejection fallback for every unknown node."""
    fallback: tuple[str, RecommendationKey]
    if isinstance(node, ast.stmt):
        fallback = (f"Unhandled statement type: {type(node).__name__}", "unsupported_statement")
    else:
        fallback = (
            f"Unsupported expression type '{type(node).__name__}'",
            "unsupported_expression",
        )
    message, recommendation = _UNSUPPORTED_SYNTAX.get(type(node), fallback)
    reject(node, message, RECOMMENDATIONS[recommendation])
