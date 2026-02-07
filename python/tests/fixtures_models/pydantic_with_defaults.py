"""Test fixture: Pydantic model with default values."""

from pydantic import BaseModel

from waymark import action, workflow
from waymark.workflow import Workflow


class ResultWithDefaults(BaseModel):
    """Pydantic model with default values."""

    value: int
    status: str = "ok"
    count: int = 0


@action
async def compute_value() -> int:
    return 100


@workflow
class PydanticDefaultsWorkflow(Workflow):
    """Workflow that uses Pydantic model with defaults."""

    async def run(self) -> ResultWithDefaults:
        value = await compute_value()
        # Only provide 'value', defaults should be applied
        result = ResultWithDefaults(value=value)
        return result
