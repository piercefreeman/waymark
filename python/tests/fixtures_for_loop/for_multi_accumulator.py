"""Fixture: for loop with multiple accumulators."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="multi_accum_classify_item")
async def classify_item(item: str) -> tuple:
    """Classify item as valid/invalid based on length."""
    if len(item) > 2:
        return (True, item.upper())
    else:
        return (False, item.lower())


@workflow
class ForMultiAccumulatorWorkflow(Workflow):
    """For loop that accumulates into multiple lists based on condition."""

    async def run(self, items: list) -> dict:
        valid_items = []
        invalid_items = []
        for item in items:
            is_valid, processed = await classify_item(item)
            if is_valid:
                valid_items.append(processed)
            else:
                invalid_items.append(processed)
        return {"valid": valid_items, "invalid": invalid_items}
