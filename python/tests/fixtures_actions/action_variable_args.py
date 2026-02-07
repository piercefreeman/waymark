"""Test fixture: Action with variable references as arguments."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def get_base_value() -> int:
    return 10


@action
async def multiply_by(value: int, factor: int) -> int:
    return value * factor


@workflow
class ActionVariableArgsWorkflow(Workflow):
    """Action arguments that are variable references."""

    async def run(self, factor: int) -> int:
        base = await get_base_value()
        # Both args are variable references
        result = await multiply_by(value=base, factor=factor)
        return result
