"""
Integration test: retry exhaustion inside a loop with break, then loop again.

This reproduces a bug where the handler action completes, but the next loop
never starts because guard evaluation lacks variables that only exist in the
next loop's inbox.
"""

from rappel import RetryPolicy, action, workflow
from rappel.workflow import Workflow


@action
async def always_fail() -> None:
    raise ValueError("boom")


@action
async def log(label: str) -> str:
    return label


@action
async def wrap(item: str) -> str:
    return f"wrap:{item}"


@action
async def join_labels(labels: list[str]) -> str:
    return ",".join(labels)


@workflow
class RetryExhaustedBreakWorkflow(Workflow):
    async def run(self) -> str:
        items: list[str] = ["seed"]

        for _ in range(3):
            try:
                await self.run_action(always_fail(), retry=RetryPolicy(attempts=3))
                await self.run_action(log("ok"))
            except Exception:
                await self.run_action(log("handled"))
                break

        outputs: list[str] = []
        for item in items:
            result = await self.run_action(wrap(item=item))
            outputs.append(result)

        return await self.run_action(join_labels(labels=outputs))
