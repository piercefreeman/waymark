"""Return lowering executes calls and returns their actual values."""

import asyncio

from waymark import Workflow, action, workflow


@action
async def returned_value(value: int) -> int:
    return value + 1


@workflow
class HelperReturnWorkflow(Workflow):
    async def run(self, value: int) -> int:
        return await self.helper(value)

    async def helper(self, value: int) -> int:
        return await returned_value(value)


@workflow
class GatherReturnWorkflow(Workflow):
    async def run(self, values: list[int]) -> list[int]:
        return await asyncio.gather(*[returned_value(item) for item in values if item > 1])


@workflow
class BareReturnWorkflow(Workflow):
    async def run(self) -> None:
        return


def test_return_preserves_nested_helper_and_action_results() -> None:
    assert asyncio.run(HelperReturnWorkflow().run(5)) == 6


def test_return_preserves_filtered_gather_setup() -> None:
    """Regression: setup statements must execute before returning the gathered values."""
    assert asyncio.run(GatherReturnWorkflow().run([1, 2, 3])) == [3, 4]


def test_bare_return_produces_none() -> None:
    assert asyncio.run(BareReturnWorkflow().run()) is None
