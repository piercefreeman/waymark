import asyncio
import time
from datetime import timedelta

import pytest

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def always_fails() -> None:
    raise ValueError("boom")


@workflow
class UnhandledFailureWorkflow(Workflow):
    async def run(self) -> None:
        await self.run_action(
            always_fails(),
            retry=RetryPolicy(attempts=3),
            timeout=timedelta(seconds=5),
        )


@workflow
class SleepWorkflow(Workflow):
    async def run(self) -> str:
        await asyncio.sleep(20)
        return "done"


@workflow
class ListMergeSyntaxWorkflow(Workflow):
    async def run(self) -> list[int]:
        left = [1, 2]
        right = [3, 4]
        left += right
        left = left + [5]
        left = [0, *left, *right]
        return left


def test_pytest_runtime_raises_for_unhandled_action_failure() -> None:
    with pytest.raises(RuntimeError, match="workflow failed") as exc_info:
        asyncio.run(UnhandledFailureWorkflow().run())
    assert "ValueError" in str(exc_info.value)
    assert "boom" in str(exc_info.value)


def test_pytest_runtime_skips_sleep_nodes() -> None:
    started = time.monotonic()
    result = asyncio.run(SleepWorkflow().run())
    elapsed = time.monotonic() - started

    assert result == "done"
    assert elapsed < 5.0


def test_pytest_runtime_executes_list_merge_syntax_variants() -> None:
    result = asyncio.run(ListMergeSyntaxWorkflow().run())

    assert result == [0, 1, 2, 3, 4, 5, 3, 4]
