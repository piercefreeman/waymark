"""Sleep's complete declared signature and its representable duration range."""

import asyncio

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@pytest.mark.parametrize("callee", ["asyncio.sleep", "nap"])
def test_sleep_aliases_preserve_delay_and_result(callee: str) -> None:
    function = compile_source(
        f"result = await {callee}(delay=0.25, result='done')\nreturn result",
        namespace={"nap": asyncio.sleep},
    )
    statements = function.body.statements
    assert statements[0].sleep_stmt.duration.literal.float_value == 0.25
    assert statements[1].assignment.value.literal.string_value == "done"


@pytest.mark.parametrize("delay", [0, -1, -0.5])
def test_nonpositive_sleep_does_not_emit_an_invalid_wait(delay: float) -> None:
    function = compile_source(f"return await asyncio.sleep({delay}, result=42)")
    assert not any(statement.HasField("sleep_stmt") for statement in function.body.statements)
    assert function.body.statements[0].assignment.value.literal.int_value == 42


@pytest.mark.parametrize(
    "arguments",
    [
        "",
        "seconds=1",
        "duration=1",
        "1, 2, 3",
        "1, delay=2",
        "True",
        "None",
        "'soon'",
        "[]",
        "1e999",
        "0.00000000001",
        "99999999999",
        "**options",
    ],
)
def test_sleep_rejects_unrepresentable_calls(arguments: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(f"await asyncio.sleep({arguments})", "delay: float, options: dict")


def test_sleep_evaluates_its_result_before_waiting() -> None:
    """Regression: evaluating sleep's arguments must happen before its wait is scheduled."""
    function = compile_source(
        "return await asyncio.sleep(1, result=values[0])", "values: list[int]"
    )
    statements = function.body.statements
    assert statements[0].assignment.value.HasField("index")
    assert statements[1].HasField("sleep_stmt")
    assert statements[2].assignment.value.variable.name == statements[0].assignment.targets[0]


@pytest.mark.parametrize("delay", ["delay", "delay + 1.0"])
def test_sleep_lowers_dynamic_delays(delay: str) -> None:
    """Keep runtime expressions in the IR instead of requiring a literal duration."""
    function = compile_source(f"return await asyncio.sleep({delay})", "delay: float")
    statements = function.body.statements
    assert statements[0].assignment.value.HasField("variable" if delay == "delay" else "binary_op")
    assert statements[1].sleep_stmt.duration.variable.name == statements[0].assignment.targets[0]
    assert statements[2].assignment.value.literal.is_none


def test_sleep_accepts_a_locally_assigned_delay() -> None:
    """A local binding must remain usable as a sleep duration."""
    function = compile_source("delay = 5\nawait asyncio.sleep(delay)")
    statements = function.body.statements
    assert statements[1].assignment.value.variable.name == "delay"
    assert statements[2].sleep_stmt.duration.variable.name == statements[1].assignment.targets[0]


@pytest.mark.parametrize(
    ("arguments", "first", "second"),
    [
        ("delays[0], results[0]", "delays", "results"),
        ("delay=delays[0], result=results[0]", "delays", "results"),
        ("result=results[0], delay=delays[0]", "results", "delays"),
    ],
)
def test_sleep_evaluates_dynamic_arguments_in_source_order(
    arguments: str, first: str, second: str
) -> None:
    """Keyword reordering must not change when argument values or errors are evaluated."""
    function = compile_source(
        f"return await asyncio.sleep({arguments})", "delays: list[float], results: list[str]"
    )
    statements = function.body.statements
    assert statements[0].assignment.value.index.object.variable.name == first
    assert statements[1].assignment.value.index.object.variable.name == second
    delay_index = 0 if first == "delays" else 1
    result_index = 1 - delay_index
    assert (
        statements[2].sleep_stmt.duration.variable.name
        == statements[delay_index].assignment.targets[0]
    )
    assert (
        statements[3].assignment.value.variable.name
        == statements[result_index].assignment.targets[0]
    )
