"""Test fixture: If statement without else, with continuation after the if block.

This tests the scenario where:
1. An action returns a value
2. An if statement conditionally calls another action based on that value
3. Code MUST continue after the if block regardless of whether the if body executed
"""

from pydantic import BaseModel

from rappel import action, workflow
from rappel.workflow import Workflow


class FirstResponse(BaseModel):
    items: list[str]


@action
async def get_items() -> FirstResponse:
    """Returns a response with potentially empty items list."""
    return FirstResponse(items=[])


@action
async def process_items(items: list[str]) -> int:
    """Only called if items is non-empty."""
    return len(items)


@action
async def finalize(count: int) -> str:
    """Called regardless of whether process_items was called."""
    return f"done with count {count}"


@workflow
class IfNoElseContinueWorkflow(Workflow):
    """If without else - execution should continue after the if block."""

    async def run(self) -> str:
        response = await get_items()

        count = 0
        if response.items:
            # This action only runs if items is non-empty
            count = await process_items(response.items)

        # This MUST run regardless of whether the if body executed
        result = await finalize(count)
        return result
