"""Fixture testing list.append() transformation in for loops."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def process_value(value: str) -> str:
    """Process a single value."""
    return value.upper()


@workflow
class ForWithAppendWorkflow(Workflow):
    """Workflow with for loop that uses list.append()."""

    async def run(self, items: list) -> list:
        results = []
        for item in items:
            processed = await process_value(item)
            results.append(processed)
        return results
