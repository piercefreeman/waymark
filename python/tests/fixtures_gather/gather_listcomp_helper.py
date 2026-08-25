"""Test fixture: asyncio.gather with starred comprehension over a workflow helper."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def increment(value: int) -> int:
    return value + 1


@workflow
class GatherListCompHelperWorkflow(Workflow):
    """Gather fan-out over an async helper method defined on the workflow.

    Pattern: await asyncio.gather(*[self.helper(x) for x in items], return_exceptions=True)
    Each helper call awaits an action internally, so the fan-out still
    produces durable action dispatches per item.
    """

    async def helper(self, value: int) -> int:
        return await increment(value=value)

    async def run(self, items: list) -> list[int | BaseException]:
        results = await asyncio.gather(
            *[self.helper(item) for item in items],
            return_exceptions=True,
        )
        return results
