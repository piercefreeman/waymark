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


@pytest.mark.parametrize(
    ("value", "expected"), [(1, "case_a"), (2, "case_b"), (3, "case_c"), (4, "default")]
)
def test_elif_branches_join_with_the_selected_result(value: int, expected: str) -> None:
    """Each nested branch must carry its assigned value into the shared return."""
    from tests.fixtures_control_flow.if_elif_else import IfElifElseWorkflow

    assert asyncio.run(IfElifElseWorkflow().run(value)) == expected


@workflow
class LoopBranchesWorkflow(Workflow):
    async def run(self, values: list[int]) -> int:
        total = 0
        for value in values:
            if value == 0:
                continue
            elif value < 0:
                break
            elif value == 1:
                total += 10
            else:
                total += value
        return total


def test_elif_loop_exits_skip_remaining_branches_and_preserve_updates() -> None:
    assert asyncio.run(LoopBranchesWorkflow().run([0, 1, 2, -1, 99])) == 12
