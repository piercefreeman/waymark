"""Integration test: asyncio.gather list comprehension spread."""

import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def echo(item: int) -> int:
    return item


@workflow
class GatherListCompWorkflow(Workflow):
    async def run(self, items: list[int]) -> list[int | BaseException]:
        results = await asyncio.gather(
            *[echo(item=item) for item in items],
            return_exceptions=True,
        )
        return results
