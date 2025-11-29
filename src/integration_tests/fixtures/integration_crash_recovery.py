from datetime import timedelta

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def step_one(value: str) -> str:
    """First step - transforms input."""
    return f"step1({value})"


@action
async def step_two(value: str) -> str:
    """Second step - transforms the output of step one."""
    return f"step2({value})"


@action
async def step_three(value: str) -> str:
    """Third step - transforms the output of step two."""
    return f"step3({value})"


@action
async def step_four(value: str) -> str:
    """Fourth step - final transformation."""
    return f"step4({value})"


@workflow
class CrashRecoveryWorkflow(Workflow):
    """
    A simple sequential workflow with 4 actions, each with a short timeout.
    Used to test crash recovery: we complete some actions, simulate a crash,
    then verify the workflow can resume from where it left off.

    Each action has a 2-second timeout so the test can verify timeout-based
    recovery without waiting too long.

    Expected result: "step4(step3(step2(step1(start))))"
    """
    async def run(self):
        # Use run_action with explicit short timeouts for testing crash recovery
        a = await self.run_action(step_one(value="start"), timeout=timedelta(seconds=2))
        b = await self.run_action(step_two(value=a), timeout=timedelta(seconds=2))
        c = await self.run_action(step_three(value=b), timeout=timedelta(seconds=2))
        d = await self.run_action(step_four(value=c), timeout=timedelta(seconds=2))
        return d
