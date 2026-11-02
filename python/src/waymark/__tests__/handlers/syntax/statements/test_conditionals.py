"""Conditions execute in Python order and skip unreachable actions."""

import asyncio

import pytest

from waymark import Workflow, action, workflow

calls: list[str] = []


@action
async def condition(value: bool, label: str) -> bool:
    """Record only the guards the VM actually executes."""
    calls.append(label)
    return value


@workflow
class BranchWorkflow(Workflow):
    async def run(self, first: bool, second: bool) -> int:
        """An elif guard runs only after the preceding guard is false."""
        if await condition(first, "first"):
            return 1
        elif await condition(second, "second"):
            return 2
        else:
            return 3


@pytest.mark.parametrize(
    ("first", "second", "expected", "visited"),
    [
        (True, True, 1, ["first"]),
        (False, True, 2, ["first", "second"]),
        (False, False, 3, ["first", "second"]),
    ],
)
def test_conditional_setup_preserves_evaluation_order(
    first: bool, second: bool, expected: int, visited: list[str]
) -> None:
    """Regression: extracting conditional setup must not eagerly run later guards."""
    calls.clear()
    assert asyncio.run(BranchWorkflow().run(first, second)) == expected
    assert calls == visited


@workflow
class SetupGuardWorkflow(Workflow):
    async def run(self, first: bool, values: list[int]) -> int:
        """Make eager evaluation of the later guard fail when values is empty."""
        if first:
            return 1
        elif await asyncio.sleep(1, result=values[0] > 0):
            return 2
        return 3


@pytest.mark.parametrize(
    ("first", "values", "expected"), [(True, [], 1), (False, [1], 2), (False, [0], 3)]
)
def test_call_setup_stays_inside_its_conditional_branch(
    first: bool, values: list[int], expected: int
) -> None:
    """Regression: evaluating the skipped sleep result would index an empty list."""
    assert asyncio.run(SetupGuardWorkflow().run(first, values)) == expected
