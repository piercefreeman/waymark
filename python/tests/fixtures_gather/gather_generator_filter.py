"""Test fixture: asyncio.gather with a filtered generator-expression argument."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_user(user: object) -> str:
    return "processed"


@workflow
class GatherGeneratorFilterWorkflow(Workflow):
    """Gather using generator-expression sugar with an `if` filter."""

    async def run(self, users: list) -> tuple[str | BaseException, ...]:
        results = await asyncio.gather(
            (process_user(user=user) for user in users if user.active),
            return_exceptions=True,
        )
        return results
