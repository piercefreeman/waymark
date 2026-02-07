"""Integration test: while loop with action-based increment."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def increment(value: int) -> int:
    return value + 1


@action
async def format_result(value: int) -> str:
    return f"done:{value}"


@workflow
class WhileLoopWorkflow(Workflow):
    async def run(self, limit: int) -> str:
        current = 0
        while current < limit:
            current = await increment(current)
        return await format_result(current)
