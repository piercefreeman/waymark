"""Test fixture: Simple for loop with single action."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_item_simple(item: str) -> str:
    return f"processed:{item}"


@workflow
class ForSimpleWorkflow(Workflow):
    """Simple for loop iterating over a list with one action per iteration."""

    async def run(self) -> list[str]:
        items = ["a", "b", "c"]
        results = []
        for item in items:
            processed = await process_item_simple(item=item)
            results.append(processed)
        return results
