"""Fixture: `except os.sep:` names a value through a module, so it is rejected."""

import os

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptModuleValueWorkflow(Workflow):
    """Workflow listing `except os.sep:` - would compile to a class called sep."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except os.sep:  # type: ignore[invalid-exception-caught] - the builder must reject it
            result = "handled"
        return result
