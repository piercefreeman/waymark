"""Dynamic sleep durations pass through compilation and VM execution."""

import asyncio

import pytest

from waymark import Workflow, action, workflow


@workflow
class DynamicSleepWorkflow(Workflow):
    async def run(self, delay: float) -> str:
        """Exercise a duration supplied as workflow input."""
        return await asyncio.sleep(delay, result="ready")


@action
async def sleep_duration(delay: float) -> float:
    """Supply a duration that the compiler cannot know before execution."""
    return delay


@workflow
class ComputedSleepWorkflow(Workflow):
    async def run(self, delay: float) -> float:
        """Compute a sleep duration from an action's runtime result."""
        duration = await sleep_duration(delay)
        return await asyncio.sleep(duration + duration, result=duration)


@pytest.mark.parametrize("delay", [1, 0.25])
def test_sleep_uses_a_workflow_argument(delay: float) -> None:
    """The VM accepts a dynamic delay and preserves sleep's explicit result."""
    assert asyncio.run(DynamicSleepWorkflow().run(delay)) == "ready"


@pytest.mark.parametrize("delay", [1, 0.25])
def test_sleep_uses_an_expression_with_an_action_result(delay: float) -> None:
    """Action-produced values can participate in duration expressions at runtime."""
    assert asyncio.run(ComputedSleepWorkflow().run(delay)) == delay
