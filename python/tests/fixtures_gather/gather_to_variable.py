"""Test fixture: asyncio.gather assigned to single variable."""

import asyncio

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_value_1() -> str:
    return "one"


@action
async def fetch_value_2() -> str:
    return "two"


@action
async def fetch_value_3() -> str:
    return "three"


@workflow
class GatherToVariableWorkflow(Workflow):
    """Gather results assigned to a single variable (list/tuple)."""

    async def run(
        self,
    ) -> tuple[str | BaseException, str | BaseException, str | BaseException]:
        results = await asyncio.gather(
            fetch_value_1(),
            fetch_value_2(),
            fetch_value_3(),
            return_exceptions=True,
        )
        return results
