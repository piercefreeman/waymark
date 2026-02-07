"""Parallel workflow fixture - tests asyncio.gather fan-out/fan-in pattern."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def compute_double(n: int) -> int:
    """Double the input."""
    return n * 2


@action
async def compute_square(n: int) -> int:
    """Square the input."""
    return n * n


@action
async def combine_results(doubled: int, squared: int) -> str:
    """Combine the parallel results."""
    return "doubled:" + str(doubled) + ",squared:" + str(squared)


@workflow
class ParallelWorkflow(Workflow):
    """Workflow with parallel fan-out and fan-in using asyncio.gather."""

    async def run(self, value: int = 5) -> str:
        # Fan out: compute double and square in parallel
        doubled, squared = await asyncio.gather(
            compute_double(value),
            compute_square(value),
            return_exceptions=True,
        )
        # Fan in: combine results
        result = await combine_results(doubled=doubled, squared=squared)
        return result
