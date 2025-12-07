"""Fixture: list comprehensions outside gather are not supported."""

from rappel import Workflow, action, workflow


@action
async def get_items() -> list:
    return [1, 2, 3]


@workflow
class ListComprehensionWorkflow(Workflow):
    """Workflow that uses list comprehension outside gather - should fail validation."""

    async def run(self) -> list:
        items = await get_items()
        # This should fail: list comprehension outside asyncio.gather()
        doubled = [x * 2 for x in items]
        return doubled
