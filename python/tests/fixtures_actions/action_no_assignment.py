"""Test fixture: Action call without assignment (side effect only)."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def log_event(message: str) -> None:
    # Side effect only - no meaningful return
    pass


@action
async def get_final_value() -> str:
    return "done"


@workflow
class ActionNoAssignmentWorkflow(Workflow):
    """Action called without capturing return value."""

    async def run(self) -> str:
        # Action without assignment - side effect only
        await log_event(message="starting")
        result = await get_final_value()
        await log_event(message="finished")
        return result
