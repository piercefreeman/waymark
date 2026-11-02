"""Sleep's complete declared signature and its representable duration range."""

import asyncio

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


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
        "delay",
        "True",
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
