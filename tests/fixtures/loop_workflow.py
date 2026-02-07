"""Loop workflow fixture - tests for loop iteration pattern.

Uses spread pattern with asyncio.gather for parallel processing,
which is the supported pattern for processing collections.
"""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_item(item: str) -> str:
    """Process a single item - uppercase it."""
    return item.upper()


@action
async def join_results(items: list) -> str:
    """Join processed items with comma."""
    return ",".join(items)


@workflow
class LoopWorkflow(Workflow):
    """Workflow that processes items using spread/gather pattern."""

    async def run(self, items: list) -> str:
        # Process all items in parallel using gather
        processed = await asyncio.gather(
            *[process_item(item=item) for item in items],
            return_exceptions=True,
        )

        # Join all results
        final = await join_results(processed)
        return final
