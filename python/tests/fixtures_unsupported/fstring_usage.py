"""Fixture: using f-strings is not supported."""

from waymark import Workflow, action, workflow


@action
async def get_name() -> str:
    return "World"


@workflow
class FstringWorkflow(Workflow):
    """Workflow that uses f-strings - should fail validation."""

    async def run(self) -> str:
        name = await get_name()
        # This should fail: using f-string
        message = f"Hello, {name}!"
        return message
