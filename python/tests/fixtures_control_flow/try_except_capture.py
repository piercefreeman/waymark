"""Test fixture: Try/except with captured exception variable."""

from typing import Any, cast

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def risky_action_capture() -> str:
    return "ok"


@action
async def handle_error_capture(error: object) -> str:
    if isinstance(error, dict):
        error_map = cast(dict[str, Any], error)
        return str(error_map.get("message", "no message"))
    return str(error)


@workflow
class TryExceptCaptureWorkflow(Workflow):
    """Try/except that captures an exception variable."""

    async def run(self) -> str:
        try:
            result = await risky_action_capture()
        except ValueError as err:
            result = await handle_error_capture(error=err)
        return result
