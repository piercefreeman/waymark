"""Test fixture: Workflow with async helper method called via self.method()."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def increment(value: int) -> int:
    return value + 1


@workflow
class WorkflowWithAsyncHelper(Workflow):
    """Workflow that awaits an async helper method on self."""

    async def run_internal(self, value: int) -> int:
        return await increment(value=value)

    async def run(self, value: int) -> int:
        result = await self.run_internal(value=value)
        return result
