"""Arithmetic handlers preserve supported results and reject known VM mismatches."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@workflow
class ArithmeticWorkflow(Workflow):
    async def run(self, value: int) -> int:
        """Exercise nested binary and unary operations through the VM."""
        return -(value + 2) * 3


def test_arithmetic_preserves_operand_results() -> None:
    """Expression dispatch remains recursive for both operator kinds."""
    assert asyncio.run(ArithmeticWorkflow().run(4)) == -18


@pytest.mark.parametrize("expression", ["1 + 2.0", "True + 1", "4 / 2", "'x' * 2"])
def test_arithmetic_rejects_known_vm_mismatches(expression: str) -> None:
    """Known type mismatches still fail before lowering."""
    with pytest.raises(UnsupportedPatternError, match="VM"):
        compile_source(f"return {expression}")


@pytest.mark.parametrize("expression", ["False and 1", "1 or 2"])
def test_short_circuit_operators_are_rejected(expression: str) -> None:
    """Boolean operators cannot fall back to eager binary evaluation."""
    with pytest.raises(UnsupportedPatternError, match="Short-circuit"):
        compile_source(f"return {expression}")
