"""Literal handlers retain representable values and source-located bounds errors."""

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@pytest.mark.parametrize("value", [-(2**63), 0, 2**63 - 1])
def test_signed_integer_literals_preserve_their_values(value: int) -> None:
    """Signed minimum folding must not reject the positive operand before negation."""
    function = compile_source(f"return {value}")
    assert function.body.statements[0].return_stmt.value.literal.int_value == value


@pytest.mark.parametrize("expression", ["9223372036854775808", "-9223372036854775809", "1e999"])
def test_invalid_wire_literals_report_their_source(expression: str) -> None:
    """Values the wire cannot encode are rejected at their original location."""
    with pytest.raises(UnsupportedPatternError) as error:
        compile_source(f"return {expression}")
    assert isinstance(error.value, UnsupportedPatternError)
    assert error.value.line == 2
