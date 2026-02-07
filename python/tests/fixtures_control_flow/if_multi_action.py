"""Test fixture: If branches with multiple actions (needs wrapping)."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def action_if_1() -> int:
    return 1


@action
async def action_if_2(x: int) -> int:
    return x + 10


@action
async def action_else_1() -> int:
    return 100


@action
async def action_else_2(x: int) -> int:
    return x + 1000


@workflow
class IfMultiActionWorkflow(Workflow):
    """If/else where both branches have multiple actions - should wrap both."""

    async def run(self, flag: bool) -> int:
        if flag:
            # Multiple actions in if branch
            a = await action_if_1()
            b = await action_if_2(x=a)
            result = b
        else:
            # Multiple actions in else branch
            c = await action_else_1()
            d = await action_else_2(x=c)
            result = d
        return result
