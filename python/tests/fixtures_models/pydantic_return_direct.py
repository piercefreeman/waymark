"""Test fixture: Direct return of a Pydantic model constructor."""

from pydantic import BaseModel

from waymark import workflow
from waymark.workflow import Workflow


class SimpleResult(BaseModel):
    """Simple Pydantic model with two fields."""

    value: int
    message: str


@workflow
class PydanticReturnDirectWorkflow(Workflow):
    """Workflow that returns a Pydantic model constructor directly."""

    async def run(self) -> SimpleResult:
        return SimpleResult(value=1, message="ok")
