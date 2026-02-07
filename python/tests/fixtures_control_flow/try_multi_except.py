"""Test fixture: Try with multiple except handlers."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def risky_action_multi_except() -> str:
    return "ok"


@action
async def handle_value_error() -> str:
    return "value_error_handled"


@action
async def handle_type_error() -> str:
    return "type_error_handled"


@action
async def handle_generic_error() -> str:
    return "generic_error_handled"


@workflow
class TryMultiExceptWorkflow(Workflow):
    """Try with multiple typed exception handlers."""

    async def run(self) -> str:
        try:
            result = await risky_action_multi_except()
        except ValueError:
            result = await handle_value_error()
        except TypeError:
            result = await handle_type_error()
        except Exception:
            result = await handle_generic_error()
        return result
