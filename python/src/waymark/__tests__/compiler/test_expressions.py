"""Unsupported expressions must not silently discard their surrounding statements."""

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@pytest.mark.parametrize(
    "source",
    [
        'return f"{value}"',
        'result = f"{value}"',
        'if f"{value}":\n    return 1',
        'return [f"{value}"]',
        'await task(f"{value}")',
    ],
)
def test_failed_expression_compilation_always_reports_its_source(source: str) -> None:
    """Regression: missing expressions must never become empty branches or bare returns."""
    with pytest.raises(UnsupportedPatternError) as error:
        compile_source(source, "value")
    assert isinstance(error.value, UnsupportedPatternError)
    assert error.value.line == 2
    assert error.value.col is not None
