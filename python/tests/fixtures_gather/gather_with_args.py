"""Test fixture: asyncio.gather with action arguments."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def compute_square(n: int) -> int:
    return n * n


@action
async def compute_cube(n: int) -> int:
    return n * n * n


@workflow
class GatherWithArgsWorkflow(Workflow):
    """Gather actions that take arguments."""

    async def run(self, value: int) -> tuple[int | BaseException, int | BaseException]:
        square, cube = await asyncio.gather(
            compute_square(n=value),
            compute_cube(n=value),
            return_exceptions=True,
        )
        return square, cube
