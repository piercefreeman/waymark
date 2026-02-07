"""Test fixture: Full if/elif/else chain."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def handle_case_a() -> str:
    return "case_a"


@action
async def handle_case_b() -> str:
    return "case_b"


@action
async def handle_case_c() -> str:
    return "case_c"


@action
async def handle_default() -> str:
    return "default"


@workflow
class IfElifElseWorkflow(Workflow):
    """Full if/elif/elif/else chain."""

    async def run(self, value: int) -> str:
        if value == 1:
            result = await handle_case_a()
        elif value == 2:
            result = await handle_case_b()
        elif value == 3:
            result = await handle_case_c()
        else:
            result = await handle_default()
        return result
