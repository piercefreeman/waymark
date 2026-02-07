"""Test fixture: Base workflow with async helper method."""

from waymark import action
from waymark.workflow import Workflow


@action
async def base_increment(value: int) -> int:
    return value + 1


class BaseWorkflowWithHelper(Workflow):
    """Base workflow providing an async helper."""

    async def run_internal(self, value: int) -> int:
        return await base_increment(value=value)
