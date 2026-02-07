"""Test fixture: try/except with action call."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="try_action")
async def try_action() -> int:
    """An action that might fail."""
    return 1


@workflow
class TryWithActionWorkflow(Workflow):
    """Workflow with try/except containing action call."""

    async def run(self) -> int:
        try:
            result = await try_action()
        except ValueError:
            result = 0
        return result
