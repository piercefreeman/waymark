"""Unsupported syntax retains useful guidance and precise source locations."""

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.recommendations import RECOMMENDATIONS, RecommendationKey


@pytest.mark.parametrize(
    ("source", "message", "recommendation", "column"),
    [
        ("raise ValueError", "The 'raise' statement is not supported", "raise_statement", 4),
        (
            "async with value:\n    pass",
            "Context managers (with statements) are not supported",
            "with_statement",
            4,
        ),
        ("return f'{value}'", "F-strings are not supported", "fstring", 11),
        ("yield from values", "Yield expressions are not supported", "yield_statement", 4),
        (
            "async for value in values:\n    pass",
            "Unhandled statement type: AsyncFor",
            "unsupported_statement",
            4,
        ),
        ("return {1, 2}", "Unsupported expression type 'Set'", "unsupported_expression", 11),
    ],
)
def test_unsupported_syntax_preserves_diagnostics(
    source: str, message: str, recommendation: RecommendationKey, column: int
) -> None:
    with pytest.raises(UnsupportedPatternError) as raised:
        compile_source(source)
    assert isinstance(raised.value, UnsupportedPatternError)
    assert raised.value.message == message
    assert raised.value.recommendation == RECOMMENDATIONS[recommendation]
    assert (raised.value.line, raised.value.col) == (2, column)
