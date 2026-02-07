"""Fixture: for loop with single list accumulator via append."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="single_accum_process_value")
async def process_value(value: str) -> str:
    """Process a single value."""
    return value.upper()


@workflow
class ForSingleAccumulatorWorkflow(Workflow):
    """For loop that accumulates results into a single list via append."""

    async def run(self, items: list) -> list:
        results = []
        for item in items:
            processed = await process_value(item)
            results.append(processed)
        return results
