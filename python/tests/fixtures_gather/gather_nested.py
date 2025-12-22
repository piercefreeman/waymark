"""Test fixture: Nested gather - gather result used in another action."""

import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def fetch_a() -> int:
    return 10


@action
async def fetch_b() -> int:
    return 20


@action
async def combine(values: tuple[int, int]) -> int:
    return sum(values)


@workflow
class GatherNestedWorkflow(Workflow):
    """Gather results fed into another action (fan-in)."""

    async def run(self) -> int:
        # Fan-out
        results = await asyncio.gather(
            fetch_a(),
            fetch_b(),
        )
        # Fan-in
        total = await combine(values=results)
        return total
