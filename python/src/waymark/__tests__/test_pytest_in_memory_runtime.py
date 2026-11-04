import asyncio
import time
from datetime import timedelta
from typing import Generic, TypeVar

import pytest

from waymark import RetryPolicy, Workflow, WorkflowFailedError, action, workflow


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


@action
async def describe_value(value: int | None = 5) -> str:
    return repr(value)


@workflow
class ExplicitNoneArgumentWorkflow(Workflow):
    async def run(self) -> str:
        return await describe_value(value=None)


@workflow
class GatherWorkflow(Workflow):
    async def run(self) -> list[str]:
        first, second = await asyncio.gather(instant("first"), instant("second"))
        return [first, second]


@workflow
class GatherFailureWorkflow(Workflow):
    async def run(self) -> None:
        await asyncio.gather(instant("ready"), always_fails(), return_exceptions=False)


@workflow
class GatherFilteredListTargetWorkflow(Workflow):
    async def run(self, items: list[tuple[bool, str]]) -> list[str]:
        return await asyncio.gather(*[instant(label) for [keep, label] in items if keep])


@workflow
class GatherFilteredNestedTargetWorkflow(Workflow):
    async def run(self, items: list[tuple[bool, tuple[str, int]]]) -> list[str]:
        return await asyncio.gather(
            *[instant(label) for keep, (label, count) in items if keep if count > 0]
        )


def test_pytest_runtime_raises_for_unhandled_action_failure() -> None:
    with pytest.raises(WorkflowFailedError) as exc_info:
        asyncio.run(UnhandledFailureWorkflow().run())

    error = exc_info.value
    assert isinstance(error, WorkflowFailedError)
    assert error.exception_value.type_id == "ValueError"
    assert error.exception_value.details["message"] == "boom"


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


def test_pytest_runtime_passes_an_explicit_none_to_the_action() -> None:
    """An explicit `None` reaches the action instead of its default."""
    result = asyncio.run(ExplicitNoneArgumentWorkflow().run())

    assert result == "None"


def test_pytest_runtime_gather_preserves_result_order() -> None:
    assert asyncio.run(GatherWorkflow().run()) == ["first", "second"]


def test_pytest_runtime_gather_propagates_action_failures() -> None:
    with pytest.raises(WorkflowFailedError) as raised:
        asyncio.run(GatherFailureWorkflow().run())
    assert isinstance(raised.value, WorkflowFailedError)
    assert raised.value.exception_value.type_id == "ValueError"


@pytest.mark.parametrize(
    ("items", "expected"),
    [([], []), ([(True, "first"), (False, "skip"), (True, "last")], ["first", "last"])],
)
def test_filtered_gather_with_list_target(
    items: list[tuple[bool, str]], expected: list[str]
) -> None:
    """Regression: list unpacking binds filter and action inputs before fan-out."""
    assert asyncio.run(GatherFilteredListTargetWorkflow().run(items)) == expected


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ([], []),
        (
            [(True, ("first", 1)), (False, ("skip", 1)), (True, ("skip", 0)), (True, ("last", 2))],
            ["first", "last"],
        ),
    ],
)
def test_filtered_gather_with_nested_target(
    items: list[tuple[bool, tuple[str, int]]], expected: list[str]
) -> None:
    """Regression: all filters and the action see names bound by nested unpacking."""
    assert asyncio.run(GatherFilteredNestedTargetWorkflow().run(items)) == expected


@workflow
class HandlerContractsWorkflow(Workflow):
    async def run(self) -> list[int]:
        values = []
        values.append(7)
        result = await asyncio.sleep(0, result=11)
        length = len("é🙂")
        return [values[0], result, length]


def test_registered_handlers_preserve_results_in_the_vm() -> None:
    assert asyncio.run(HandlerContractsWorkflow().run()) == [7, 11, 2]


@workflow
class ComprehensionScopeWorkflow(Workflow):
    async def run(self) -> list[int]:
        item = 9
        values = [1, 2]
        values = [item + 1 for item in values]
        return [item, values[0], values[1]]


def test_list_comprehensions_capture_the_input_and_preserve_outer_bindings() -> None:
    """Regression: comprehension iteration has its own bindings and captures its input once."""
    assert asyncio.run(ComprehensionScopeWorkflow().run()) == [9, 2, 3]


@workflow
class ComprehensionDestinationWorkflow(Workflow):
    async def run(self) -> list[int]:
        values = [7]
        values = [values[0] for item in [1, 2] if values]
        return values


def test_comprehensions_publish_the_destination_after_elements_and_filters_finish() -> None:
    """Regression: elements and filters must read the destination's original value."""
    assert asyncio.run(ComprehensionDestinationWorkflow().run()) == [7, 7]


@action
async def identity_list(values: list[int]) -> list[int]:
    return values


@workflow
class AccumulateThenDispatchWorkflow(Workflow):
    async def run(self) -> list[int]:
        values = []
        for item in [1, 2]:
            values.append(item)
        result = await identity_list(values)
        return result


def test_local_list_can_escape_after_its_last_mutation() -> None:
    """Regression: returning an accumulated list through an action needs no alias mutation."""
    assert asyncio.run(AccumulateThenDispatchWorkflow().run()) == [1, 2]


Label = TypeVar("Label", bound=str)


class LabelingWorkflow(Workflow, Generic[Label]):
    async def label_all(self, labels: list[Label]) -> list[str]:
        results = []
        for label in labels:
            result = await instant(label)
            results.append(result)
        return results


@workflow
class SpecializedLabelingWorkflow(LabelingWorkflow[str]):
    async def run(self, labels: list[str]) -> list[str]:
        return await self.label_all(labels)


def test_specialized_generic_helpers_run() -> None:
    """Regression: helpers typed by a generic base's parameter were rejected as open generics."""
    assert asyncio.run(SpecializedLabelingWorkflow().run(labels=["a", "b"])) == ["a", "b"]


BATCH_SIZE = 5 * 6
LABEL_PREFIX = "batch-"
STAGES = ("fetch", "score")
STAGE_ATTEMPTS = 1 + 2
STAGE_TIMEOUT = timedelta(minutes=1) * 2


@workflow
class ModuleConstantsWorkflow(Workflow):
    async def run(self, count: int) -> list[str]:
        labels = []
        for stage in STAGES:
            label = await self.run_action(
                instant(LABEL_PREFIX + stage),
                retry=RetryPolicy(attempts=STAGE_ATTEMPTS),
                timeout=STAGE_TIMEOUT,
            )
            labels.append(label)
        if count < BATCH_SIZE:
            labels.append(LABEL_PREFIX + "size")
        return labels


def test_module_constants_run_as_their_values() -> None:
    """Regression: module constants were rejected in policies and unbound at runtime.

    Each global must reach the runtime as the value it held at compile time,
    in expressions, loop iterables, conditions and policy fields alike.
    """
    assert asyncio.run(ModuleConstantsWorkflow().run(count=2)) == [
        "batch-fetch",
        "batch-score",
        "batch-size",
    ]
    assert asyncio.run(ModuleConstantsWorkflow().run(count=BATCH_SIZE)) == [
        "batch-fetch",
        "batch-score",
    ]
