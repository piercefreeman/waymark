"""Test fixture: For loop with multiple actions per iteration."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def step_one_for(x: int) -> int:
    return x + 1


@action
async def step_two_for(x: int) -> int:
    return x * 2


@workflow
class ForMultiActionWorkflow(Workflow):
    """For loop with multiple actions that should be wrapped into implicit function."""

    async def run(self) -> list[int]:
        items = [1, 2, 3]
        results = []
        for item in items:
            # Multiple actions per iteration - should create implicit function
            first = await step_one_for(x=item)
            second = await step_two_for(x=first)
            results.append(second)
        return results
