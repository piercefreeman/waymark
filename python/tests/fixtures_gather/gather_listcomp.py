"""Test fixture: asyncio.gather with starred list comprehension."""

import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def process_item(item: str, multiplier: int) -> str:
    return f"{item}_{multiplier}"


@workflow
class GatherListCompWorkflow(Workflow):
    """Gather with starred list comprehension for parallel fan-out.

    Pattern: await asyncio.gather(*[action(x) for x in items])
    This is a common idiom for parallel processing of collections.
    """

    async def run(self, items: list, multiplier: int) -> list[str]:
        # Starred list comprehension - fan out over items
        results = await asyncio.gather(
            *[process_item(item=item, multiplier=multiplier) for item in items]
        )
        return results
