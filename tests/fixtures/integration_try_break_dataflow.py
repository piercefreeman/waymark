"""Integration test: try/except with break, then a loop over a try-updated list."""

from datetime import timedelta

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


@action
async def always_fail() -> str:
    raise ValueError("boom")


@action
async def echo(label: str) -> str:
    return label


@action
async def join_labels(labels: list[str]) -> str:
    return ",".join(labels)


@workflow
class TryBreakDataflowWorkflow(Workflow):
    async def run(self) -> str:
        outputs: list[str] = []

        for _ in range(1):
            try:
                value = await self.run_action(
                    always_fail(),
                    retry=RetryPolicy(attempts=1),
                    timeout=timedelta(seconds=1),
                )
                outputs.append(value)
            except Exception:
                await self.run_action(echo(label="handled"))
                break

        labels: list[str] = []
        for item in outputs:
            label = await self.run_action(echo(label=item))
            labels.append(label)

        return await self.run_action(join_labels(labels=labels))
