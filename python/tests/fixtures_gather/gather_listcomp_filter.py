"""Test fixture: asyncio.gather with a filtered starred list comprehension."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_user(user: object) -> str:
    return "processed"


@workflow
class GatherListCompFilterWorkflow(Workflow):
    """Gather spread with an `if` filter in the list comprehension."""

    async def run(self, users: list) -> list[str | BaseException]:
        results = await asyncio.gather(
            *[process_user(user=user) for user in users if user.active],
            return_exceptions=True,
        )
        return results
