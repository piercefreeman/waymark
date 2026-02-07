"""Test fixture: Dataclass with default values."""

from dataclasses import dataclass

from waymark import action, workflow
from waymark.workflow import Workflow


@dataclass
class StatusResult:
    """Dataclass with default values."""

    value: int
    status: str = "pending"
    retry_count: int = 0


@action
async def get_result() -> int:
    return 50


@workflow
class DataclassDefaultsWorkflow(Workflow):
    """Workflow that uses dataclass with defaults."""

    async def run(self) -> StatusResult:
        value = await get_result()
        # Only provide 'value', defaults should be applied
        result = StatusResult(value=value)
        return result
