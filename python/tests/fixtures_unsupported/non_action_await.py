"""Fixture: awaiting a non-action function is not supported."""

from rappel import Workflow, workflow


# This function is NOT decorated with @action
async def helper_function(x: int) -> int:
    return x * 2


@workflow
class NonActionAwaitWorkflow(Workflow):
    """Workflow that awaits a non-action function - should fail validation."""

    async def run(self, value: int) -> int:
        # This should fail: awaiting a function not decorated with @action
        result = await helper_function(value)
        return result
