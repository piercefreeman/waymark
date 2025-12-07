"""Fixture: simple action call is valid."""

from rappel import Workflow, action, workflow


@action
async def process(value: int) -> int:
    return value * 2


@workflow
class SimpleActionWorkflow(Workflow):
    """Workflow with simple action call - valid pattern."""

    async def run(self, value: int) -> int:
        # This is valid: calling an @action decorated function
        result = await process(value)
        return result
