"""Test fixture: Try/except that catches Exception into a variable."""

from typing import Any, cast

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def risky_action_catchall() -> str:
    """An action that may raise."""
    return "ok"


@action
async def handle_catchall_error(error: object) -> str:
    """Handler that inspects the caught exception."""
    if isinstance(error, dict):
        error_map = cast(dict[str, Any], error)
        return str(error_map.get("message", "no message"))
    return str(error)


@workflow
class TryExceptCatchAllWorkflow(Workflow):
    """Try/except that catches Exception into a variable."""

    async def run(self) -> str:
        try:
            result = await risky_action_catchall()
        except Exception as err:
            result = await handle_catchall_error(error=err)
        return result
