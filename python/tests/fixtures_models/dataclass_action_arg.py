"""Test fixture: Dataclass passed directly as action argument.

This tests the case where a dataclass constructor is passed directly
as an argument to an action call, rather than being assigned to a variable first.

Example workflow pattern:
    await self.run_action(my_action(MyDataclass(field=value)))

This should be converted to a dict expression in the IR, just like:
    model = MyDataclass(field=value)  # This already works
    await self.run_action(my_action(model))
"""

from dataclasses import dataclass

from waymark import action, workflow
from waymark.workflow import Workflow


@dataclass
class RequestData:
    """Request dataclass with a list field."""

    items: list[str]


@dataclass
class ResponseData:
    """Response dataclass."""

    count: int


@action
async def process_data(request: RequestData) -> ResponseData:
    """Action that takes a dataclass as argument."""
    return ResponseData(count=len(request.items))


@action
async def get_data_items() -> list[str]:
    """Action that returns items to process."""
    return ["item1", "item2", "item3"]


@workflow
class DataclassActionArgWorkflow(Workflow):
    """Workflow that passes dataclass directly to action."""

    async def run(self) -> int:
        items = await get_data_items()
        # Dataclass constructor passed directly as action argument
        result = await process_data(RequestData(items=items))
        return result.count
