"""Test fixture: asyncio.gather with self.run_action in spread pattern."""

import asyncio
from datetime import timedelta

from rappel import action, workflow
from rappel.workflow import RetryPolicy, Workflow


@action
async def process_item(item: str) -> str:
    """Process a single item."""
    return f"processed:{item}"


@action
async def combine_results(results: list) -> str:
    """Combine all processed results."""
    return ",".join(results)


@workflow
class GatherRunActionSpreadWorkflow(Workflow):
    """Workflow using self.run_action with retry/timeout in gather spread pattern.

    Pattern: await asyncio.gather(*[
        self.run_action(action(x), retry=..., timeout=...)
        for x in items
    ])
    """

    async def run(self, items: list) -> str:
        # Spread pattern with run_action wrapper for retry and timeout policies
        results = await asyncio.gather(
            *[
                self.run_action(
                    process_item(item=item),
                    retry=RetryPolicy(attempts=3),
                    timeout=timedelta(seconds=30),
                )
                for item in items
            ]
        )
        return await combine_results(results=results)
