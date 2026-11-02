"""Test fixture: asyncio.gather with a generator-expression argument."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_item(item: str, multiplier: int) -> str:
    return f"{item}_{multiplier}"


@workflow
class GatherGeneratorWorkflow(Workflow):
    """Gather using the starred generator-expression spelling."""

    async def run(self, items: list, multiplier: int) -> list[str]:
        results = await asyncio.gather(
            *(process_item(item=item, multiplier=multiplier) for item in items),
            return_exceptions=False,
        )
        return results
