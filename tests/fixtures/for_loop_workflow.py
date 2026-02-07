"""For-loop workflow fixture - tests for loop iteration with append pattern.

This tests the classic for-loop pattern where items are processed
one at a time in a loop with results appended to a list.
"""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_item(item: str) -> str:
    """Process a single item - uppercase it."""
    return item.upper()


@action
async def finalize_results(processed: list) -> str:
    """Join processed items with comma."""
    return ",".join(processed)


@workflow
class ForLoopWorkflow(Workflow):
    """Workflow that processes items using for-loop with append pattern."""

    async def run(self, items: list) -> str:
        # Use the for-loop with append pattern
        processed = []
        for item in items:
            result = await process_item(item)
            processed.append(result)

        # Finalize all results
        final = await finalize_results(processed)
        return final
