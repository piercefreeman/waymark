"""Fixture: calling built-in functions directly is not supported."""

from waymark import Workflow, action, workflow


@action
async def get_items() -> list:
    return [1, 2, 3, 4, 5]


@workflow
class BuiltinCallWorkflow(Workflow):
    """Workflow that calls len() directly - should fail validation."""

    async def run(self) -> int:
        items = await get_items()
        # This should fail: calling built-in function directly
        count = len(items)
        return count
