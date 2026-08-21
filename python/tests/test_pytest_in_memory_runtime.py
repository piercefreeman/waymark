import asyncio
import time
from datetime import timedelta

import pytest

from waymark import RetryPolicy, Workflow, action, workflow


@action
async def always_fails() -> None:
    raise ValueError("boom")


@action
async def instant(label: str) -> str:
    return label


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
class TimeoutPolicyWorkflow(Workflow):
    async def run(self) -> str:
        return await self.run_action(
            instant("prompt"),
            timeout=timedelta(seconds=30),
        )


@workflow
class ListMergeSyntaxWorkflow(Workflow):
    async def run(self) -> list[int]:
        left = [1, 2]
        right = [3, 4]
        left += right
        left = left + [5]
        left = [0, *left, *right]
        return left


@workflow
class ResultKeyMappingWorkflow(Workflow):
    async def run(self) -> dict[str, str]:
        return {
            "result": "inner",
            "other": "must not be lost",
        }


@workflow
class SingleKeyMappingWorkflow(Workflow):
    async def run(self) -> dict[str, str]:
        return {"only": "value"}


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


def test_pytest_runtime_does_not_wait_out_action_timeouts() -> None:
    started = time.monotonic()
    result = asyncio.run(TimeoutPolicyWorkflow().run())
    elapsed = time.monotonic() - started

    assert result == "prompt"
    assert elapsed < 5.0


def test_pytest_runtime_executes_list_merge_syntax_variants() -> None:
    result = asyncio.run(ListMergeSyntaxWorkflow().run())

    assert result == [0, 1, 2, 3, 4, 5, 3, 4]


def test_pytest_runtime_preserves_mapping_with_result_key() -> None:
    """A returned mapping must not treat one user field as an envelope."""
    result = asyncio.run(ResultKeyMappingWorkflow().run())

    assert result == {
        "result": "inner",
        "other": "must not be lost",
    }


def test_pytest_runtime_preserves_single_key_mapping() -> None:
    """A single-entry mapping must round-trip as a mapping, not its value."""
    result = asyncio.run(SingleKeyMappingWorkflow().run())

    assert result == {"only": "value"}
