"""Test fixture: Simple Pydantic model assignment."""

from pydantic import BaseModel

from waymark import action, workflow
from waymark.workflow import Workflow


class SimpleResult(BaseModel):
    """Simple Pydantic model with two fields."""

    value: int
    message: str


@action
async def get_value() -> int:
    return 42


@workflow
class PydanticSimpleWorkflow(Workflow):
    """Workflow that creates a Pydantic model instance."""

    async def run(self) -> SimpleResult:
        value = await get_value()
        result = SimpleResult(value=value, message="success")
        return result
