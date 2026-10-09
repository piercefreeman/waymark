"""Fixture: an except clause dotted through a value, not a module or class, is rejected."""

from typing import Any

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptValueRootWorkflow(Workflow):
    """Workflow listing `except cfg.error_cls:` with `cfg` a run parameter - fails validation."""

    async def run(self, cfg: Any) -> str:
        try:
            result = await flaky_action()
        except cfg.error_cls:
            result = "handled"
        return result
