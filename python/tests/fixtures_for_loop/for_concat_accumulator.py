"""Fixture: for loop with list concatenation accumulation."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="concat_accum_process_value")
async def process_value(value: str) -> str:
    """Process a single value."""
    return value.upper()


@workflow
class ForConcatAccumulatorWorkflow(Workflow):
    """For loop that accumulates via list concatenation (results = results + [x])."""

    async def run(self, items: list) -> list:
        results = []
        for item in items:
            processed = await process_value(item)
            results = results + [processed]
        return results
