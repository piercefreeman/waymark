"""Comparison extraction keeps supported outcomes and rejects unsupported shapes."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@workflow
class ComparisonWorkflow(Workflow):
    async def run(self, value: int) -> bool:
        """Compare a runtime argument against a literal bound."""
        return value < 3


@pytest.mark.parametrize(("value", "expected"), [(2, True), (3, False)])
def test_comparisons_execute_both_outcomes(value: int, expected: bool) -> None:
    """The VM receives the selected operator and both operands."""
    assert asyncio.run(ComparisonWorkflow().run(value)) is expected


@pytest.mark.parametrize("expression", ["1 < 2 < 3", "[1] == [1]", "1 == 1.0"])
def test_comparisons_reject_unsupported_semantics(expression: str) -> None:
    """Chaining and known operand mismatches remain source errors."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(f"return {expression}")
