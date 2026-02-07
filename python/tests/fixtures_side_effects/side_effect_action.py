"""Test fixture: Side-effect action call without assignment."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def side_effect() -> None:
    """A side-effect action that returns nothing."""
    pass


@workflow
class SideEffectWorkflow(Workflow):
    """Workflow with side-effect action call (no assignment)."""

    async def run(self) -> None:
        await side_effect()
        return
