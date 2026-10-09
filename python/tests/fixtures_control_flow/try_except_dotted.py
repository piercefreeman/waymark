"""Test fixture: Except clauses that spell the class through a module."""

import urllib.error

import waymark
from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def flaky_action() -> str:
    return "ok"


@action
async def handle_timeout() -> str:
    return "timeout_handled"


@action
async def handle_lookup() -> str:
    return "lookup_handled"


@workflow
class TryExceptDottedWorkflow(Workflow):
    """Dotted except types resolve to the class name the VM matches."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except waymark.ActionTimeout:
            result = await handle_timeout()
        except (KeyError, waymark.ActionExecutionLost):
            result = await handle_lookup()
        except urllib.error.HTTPError:
            result = await handle_lookup()
        return result
