"""Fixture: returning a constructor call is not supported."""

from pydantic import BaseModel

from rappel import Workflow, action, workflow


class MyResult(BaseModel):
    value: int
    message: str


@action
async def get_value() -> int:
    return 42


@workflow
class ConstructorReturnWorkflow(Workflow):
    """Workflow that returns a constructor - should fail validation."""

    async def run(self) -> MyResult:
        value = await get_value()
        # This should fail: returning a constructor call
        return MyResult(value=value, message="done")
