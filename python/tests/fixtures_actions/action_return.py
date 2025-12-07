"""Fixture: returning an action result directly is valid."""

from rappel import Workflow, action, workflow


@action
async def compute_result(x: int) -> int:
    return x * 2


@workflow
class ActionReturnWorkflow(Workflow):
    """Workflow that returns an action result directly - valid pattern."""

    async def run(self, value: int) -> int:
        # This is valid: returning awaited action result
        return await compute_result(value)
