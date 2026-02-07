"""Test fixture: asyncio.gather with starred list comprehension."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_item(item: str, multiplier: int) -> str:
    return f"{item}_{multiplier}"


@workflow
class GatherListCompWorkflow(Workflow):
    """Gather with starred list comprehension for parallel fan-out.

    Pattern: await asyncio.gather(*[action(x) for x in items], return_exceptions=True)
    This is a common idiom for parallel processing of collections.
    """

    async def run(self, items: list, multiplier: int) -> list[str | BaseException]:
        # Starred list comprehension - fan out over items
        results = await asyncio.gather(
            *[process_item(item=item, multiplier=multiplier) for item in items],
            return_exceptions=True,
        )
        return results
