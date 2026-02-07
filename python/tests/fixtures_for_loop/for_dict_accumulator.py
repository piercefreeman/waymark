"""Fixture: for loop with dict accumulation."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action(name="dict_accum_get_key_value")
async def get_key_value(item: str) -> tuple:
    """Get key-value pair from item."""
    return (item, item.upper())


@workflow
class ForDictAccumulatorWorkflow(Workflow):
    """For loop that builds a dict from items."""

    async def run(self, items: list) -> dict:
        result = {}
        for item in items:
            key, value = await get_key_value(item)
            result[key] = value
        return result
