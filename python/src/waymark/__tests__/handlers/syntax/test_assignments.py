"""Assignment handlers preserve values, scope, and supported unpacking."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


@workflow
class AssignmentWorkflow(Workflow):
    async def run(self, value: int) -> list[int]:
        """Exercise assignment variants and comprehension-local names together."""
        first, second = [value, 2]
        total: int = first + second
        total += 1
        item = 99
        results = [item + total for item in [1, 2]]
        return [item, *results]


def test_assignments_preserve_results_and_comprehension_scope() -> None:
    """Executing the resulting workflow verifies writes and names, not only node shapes."""
    assert asyncio.run(AssignmentWorkflow().run(3)) == [99, 7, 8]


@pytest.mark.parametrize(
    "source",
    [
        "a = b = 1",
        "a, b = [1]",
        "self.value = 1",
        "values = [1]\nvalues[0] = 2",
        "values = [1]\nvalues[0] += 2",
    ],
)
def test_assignments_reject_unsupported_targets(source: str) -> None:
    """Unsupported assignments cannot disappear or lose targets during lowering."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(source)
