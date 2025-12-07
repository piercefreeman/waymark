"""Fixture: assigning a constructor call is not supported."""

from pydantic import BaseModel

from rappel import Workflow, action, workflow


class Config(BaseModel):
    timeout: int
    retries: int


@action
async def get_timeout() -> int:
    return 30


@workflow
class ConstructorAssignmentWorkflow(Workflow):
    """Workflow that assigns a constructor - should fail validation."""

    async def run(self) -> int:
        timeout = await get_timeout()
        # This should fail: assigning a constructor call
        config = Config(timeout=timeout, retries=3)
        return config.timeout
