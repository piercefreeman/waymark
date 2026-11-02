"""Await syntax preserves helper execution and rejects unsupported placements."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


@workflow
class AwaitWorkflow(Workflow):
    async def double(self, value: int) -> int:
        """Provide an awaited workflow helper distinct from an action."""
        return value + value

    async def run(self, value: int) -> int:
        """Bind the helper result before using it in another expression."""
        result = await self.double(value)
        return result + 1


def test_awaited_helpers_preserve_their_return_values() -> None:
    """A registered syntax handler must not break the separate helper-call path."""
    assert asyncio.run(AwaitWorkflow().run(3)) == 7


@pytest.mark.parametrize(
    "source", ["return (await task()) + 1", "return [await task()]", "return await len([1])"]
)
def test_await_rejects_unsupported_placement_and_sync_calls(source: str) -> None:
    """Await restrictions live with the syntax instead of a central validation switch."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(source)
