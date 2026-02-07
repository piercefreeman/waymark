"""Test fixture: Simple try/except with single action in try."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def risky_action_simple() -> str:
    return "success"


@action
async def handle_error_simple() -> str:
    return "error_handled"


@workflow
class TrySimpleWorkflow(Workflow):
    """Simple try/except with one action in try block."""

    async def run(self) -> str:
        try:
            result = await risky_action_simple()
        except Exception:
            result = await handle_error_simple()
        return result
