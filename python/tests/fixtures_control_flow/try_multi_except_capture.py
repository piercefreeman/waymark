"""Test fixture: Try with multiple except handlers, each capturing the exception."""

from typing import Any, cast

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def risky_action_multi_capture() -> str:
    """An action that may raise."""
    return "ok"


@action
async def handle_value_err(error: object) -> str:
    """Handler for ValueError."""
    if isinstance(error, dict):
        return str(cast(dict[str, Any], error).get("message", "ValueError"))
    return str(error)


@action
async def handle_type_err(error: object) -> str:
    """Handler for TypeError."""
    if isinstance(error, dict):
        return str(cast(dict[str, Any], error).get("message", "TypeError"))
    return str(error)


@action
async def handle_generic_err(error: object) -> str:
    """Handler for any other Exception."""
    if isinstance(error, dict):
        return str(cast(dict[str, Any], error).get("message", "generic"))
    return str(error)


@workflow
class TryMultiExceptCaptureWorkflow(Workflow):
    """Try with multiple typed exception handlers, each capturing the exception."""

    async def run(self) -> str:
        try:
            result = await risky_action_multi_capture()
        except ValueError as verr:
            result = await handle_value_err(error=verr)
        except TypeError as terr:
            result = await handle_type_err(error=terr)
        except Exception as err:
            result = await handle_generic_err(error=err)
        return result
