"""Integration fixture for a conditional guard that skips an action."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_items() -> list[int]:
    return []


@action
async def count_items(items: list[int]) -> int:
    return len(items)


@action
async def finalize(count: int) -> str:
    return f"final:{count}"


@workflow
class DeadEndConditionalWorkflow(Workflow):
    """Workflow that skips an action when a falsy guard is encountered."""

    async def run(self) -> str:
        items = await fetch_items()
        count = 0
        if items:
            count = await count_items(items)
        result = await finalize(count)
        return result
