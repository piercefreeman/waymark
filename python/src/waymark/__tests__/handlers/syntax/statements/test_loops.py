"""Loop handlers retain iteration, unpacking, and early exits."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@workflow
class LoopWorkflow(Workflow):
    async def run(self) -> int:
        """Combine for iteration with a while loop and break/continue."""
        total = 0
        for first, second in [[1, 2], [3, 4]]:
            total += first + second
        while total < 20:
            total += 1
            if total < 15:
                continue
            break
        return total


def test_loops_execute_all_bindings_and_early_exits() -> None:
    """The result exercises both loop lowering paths through the VM."""
    assert asyncio.run(LoopWorkflow().run()) == 15


@pytest.mark.parametrize("loop", ["for item in [1]", "while False"])
def test_loop_else_is_rejected(loop: str) -> None:
    """Else bodies cannot be silently dropped."""
    with pytest.raises(UnsupportedPatternError, match="/else"):
        compile_source(f"{loop}:\n    pass\nelse:\n    pass")


def test_comprehensions_share_the_iteration_restrictions() -> None:
    """A comprehension clause uses the same unsupported iterable check as for."""
    with pytest.raises(UnsupportedPatternError, match="Iteration over dictionaries"):
        compile_source("mapping = {'key': 1}\nvalues = [key for key in mapping]")
