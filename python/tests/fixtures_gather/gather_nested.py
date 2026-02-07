"""Test fixture: Nested gather - gather result used in another action."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_a() -> int:
    return 10


@action
async def fetch_b() -> int:
    return 20


@action
async def combine(values: tuple[int | BaseException, int | BaseException]) -> int:
    numeric_values = [value for value in values if isinstance(value, int)]
    return sum(numeric_values)


@workflow
class GatherNestedWorkflow(Workflow):
    """Gather results fed into another action (fan-in)."""

    async def run(self) -> int:
        # Fan-out
        results = await asyncio.gather(
            fetch_a(),
            fetch_b(),
            return_exceptions=True,
        )
        # Fan-in
        total = await combine(values=results)
        return total
