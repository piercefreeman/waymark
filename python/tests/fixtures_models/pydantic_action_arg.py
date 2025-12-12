"""Test fixture: Pydantic model passed directly as action argument.

This tests the case where a Pydantic model constructor is passed directly
as an argument to an action call, rather than being assigned to a variable first.

Example workflow pattern:
    await self.run_action(my_action(MyModel(field=value)))

This should be converted to a dict expression in the IR, just like:
    model = MyModel(field=value)  # This already works
    await self.run_action(my_action(model))
"""

from pydantic import BaseModel

from rappel import action, workflow
from rappel.workflow import Workflow


class RequestModel(BaseModel):
    """Request model with a list field - similar to ArchiveS3ObjectsRequest."""

    items: list[str]


class ResponseModel(BaseModel):
    """Response model."""

    count: int


@action
async def process_request(request: RequestModel) -> ResponseModel:
    """Action that takes a Pydantic model as argument."""
    return ResponseModel(count=len(request.items))


@action
async def get_items() -> list[str]:
    """Action that returns items to process."""
    return ["item1", "item2", "item3"]


@workflow
class PydanticActionArgWorkflow(Workflow):
    """Workflow that passes Pydantic model directly to action."""

    async def run(self) -> int:
        items = await get_items()
        # This is the pattern that should work but currently fails:
        # Pydantic model constructor passed directly as action argument
        result = await process_request(RequestModel(items=items))
        return result.count
