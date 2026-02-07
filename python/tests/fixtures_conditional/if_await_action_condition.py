"""Test fixture: if condition uses await action() (should be normalized)."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="is_even")
async def is_even(value: int) -> bool:
    return value % 2 == 0


@workflow
class IfAwaitActionConditionWorkflow(Workflow):
    async def run(self, value: int) -> int:
        if await is_even(value=value):
            return 1
        return 0
