"""Test fixture: Simple asyncio.gather with two actions."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def action_a() -> int:
    return 1


@action
async def action_b() -> int:
    return 2


@workflow
class GatherSimpleWorkflow(Workflow):
    """Simple gather of two actions."""

    async def run(self) -> tuple[int | BaseException, int | BaseException]:
        a, b = await asyncio.gather(
            action_a(),
            action_b(),
            return_exceptions=True,
        )
        return a, b
