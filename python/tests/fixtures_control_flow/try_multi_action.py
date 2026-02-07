"""Test fixture: Try/except with multiple actions in try (needs wrapping)."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def step_1_try() -> int:
    return 1


@action
async def step_2_try(x: int) -> int:
    return x * 2


@action
async def step_3_try(x: int) -> str:
    return f"result:{x}"


@action
async def handle_exception_multi() -> str:
    return "fallback"


@workflow
class TryMultiActionWorkflow(Workflow):
    """Try block with multiple actions - should wrap into implicit function."""

    async def run(self) -> str:
        try:
            a = await step_1_try()
            b = await step_2_try(x=a)
            result = await step_3_try(x=b)
        except Exception:
            result = await handle_exception_multi()
        return result
