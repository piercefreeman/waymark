"""Fixture: returning a literal is valid."""

from rappel import Workflow, action, workflow


@action
async def do_something() -> None:
    pass


@workflow
class LiteralReturnWorkflow(Workflow):
    """Workflow that returns a literal - valid pattern."""

    async def run(self) -> int:
        await do_something()
        # This is valid: returning a literal
        return 42
