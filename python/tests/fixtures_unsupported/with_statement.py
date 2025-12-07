"""Fixture: with statements (context managers) are not supported."""

from rappel import Workflow, action, workflow


@action
async def get_path() -> str:
    return "/tmp/test.txt"


@workflow
class WithStatementWorkflow(Workflow):
    """Workflow that uses with statement - should fail validation."""

    async def run(self) -> str:
        path = await get_path()
        # This should fail: with statements not supported
        with open(path) as f:
            content = f.read()
        return content
