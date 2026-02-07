"""Test fixture: Simple if/else with single action per branch."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def handle_true_case() -> str:
    return "was_true"


@action
async def handle_false_case() -> str:
    return "was_false"


@workflow
class IfSimpleWorkflow(Workflow):
    """Simple if/else with one action per branch."""

    async def run(self, flag: bool) -> str:
        if flag:
            result = await handle_true_case()
        else:
            result = await handle_false_case()
        return result
