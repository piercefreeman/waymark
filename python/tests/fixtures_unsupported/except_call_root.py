"""Fixture: an except clause dotted through a call result is rejected."""

from typing import Any

from waymark import Workflow, action, workflow


def errors() -> Any:
    return ValueError


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptCallRootWorkflow(Workflow):
    """Workflow listing `except errors().Primary:` - a call-rooted chain fails validation."""

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except errors().Primary:
            result = "handled"
        return result
