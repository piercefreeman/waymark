"""Test fixture: Try/except that lists a base class of the raised exception."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def lookup_action() -> str:
    """An action that raises `KeyError`."""
    raise KeyError("absent")


@action
async def handle_lookup_error() -> str:
    """Handler for any lookup error."""
    return "lookup_error_handled"


@workflow
class TryExceptHierarchyWorkflow(Workflow):
    """Try/except whose handler lists `LookupError`, a base class of `KeyError`."""

    async def run(self) -> str:
        try:
            result = await lookup_action()
        except LookupError:
            result = await handle_lookup_error()
        return result
