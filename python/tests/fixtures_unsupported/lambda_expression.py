"""Fixture: lambda expressions are not supported."""

from rappel import Workflow, action, workflow


@action
async def get_items_for_lambda() -> list:
    return [3, 1, 4, 1, 5]


@workflow
class LambdaExpressionWorkflow(Workflow):
    """Workflow that uses lambda - should fail validation."""

    async def run(self) -> list:
        items = await get_items_for_lambda()
        # This should fail: lambda expressions not supported
        key_func = lambda x: -x  # noqa: E731, F841
        return items
