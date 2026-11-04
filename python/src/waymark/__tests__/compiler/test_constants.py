"""Module-level values compile as the literals they hold, wherever a literal is accepted.

Regression: `RetryPolicy(attempts=AGENT_ATTEMPTS)` with `AGENT_ATTEMPTS = 3` at
module level was rejected as "attempts must be an integer literal", and a
global read in an expression compiled to a lookup of a variable the runtime
never defines. A global is read by the value it holds when the workflow
compiles, so computed values (`5 * 6`) need no handling of their own.
"""

from datetime import timedelta
from enum import Enum

import pytest

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError
from waymark.proto import ast_pb2 as ir
from waymark.workflow import RetryPolicy


class Tier(Enum):
    PAID = "paid"


def returned(function: ir.FunctionDef) -> ir.Expr:
    return function.body.statements[-1].return_stmt.value


def assigned(function: ir.FunctionDef) -> ir.Expr:
    """The value of the first assignment, where an awaited `return` lowers its call."""
    return next(
        statement.assignment.value
        for statement in function.body.statements
        if statement.HasField("assignment")
    )


def action_policies(function: ir.FunctionDef) -> list[ir.PolicyBracket]:
    return list(assigned(function).action_call.policies)


def test_computed_global_compiles_as_its_value() -> None:
    expression = returned(compile_source("return LIMIT + value", "value", {"LIMIT": 5 * 6}))
    assert expression.binary_op.left.literal.int_value == 30
    assert expression.binary_op.right.variable.name == "value"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Tier.PAID, ir.Expr(literal=ir.Literal(string_value="paid"))),
        (None, ir.Expr(literal=ir.Literal(is_none=True))),
        (True, ir.Expr(literal=ir.Literal(bool_value=True))),
        (2.5, ir.Expr(literal=ir.Literal(float_value=2.5))),
    ],
)
def test_scalar_globals_compile_to_literals(value: object, expected: ir.Expr) -> None:
    """An enum member compiles to its value, as `Tier.PAID` does in workflow code."""
    expression = returned(compile_source("return VALUE", namespace={"VALUE": value}))
    expression.ClearField("span")
    assert expression == expected


def test_container_globals_compile_to_container_literals() -> None:
    function = compile_source(
        "return [STAGES, LIMITS]",
        namespace={"STAGES": ("fetch", "score"), "LIMITS": {"paid": 30}},
    )
    stages, limits = returned(function).list.elements
    assert [element.literal.string_value for element in stages.list.elements] == [
        "fetch",
        "score",
    ]
    (entry,) = limits.dict.entries
    assert entry.key.literal.string_value == "paid"
    assert entry.value.literal.int_value == 30


@pytest.mark.parametrize(
    ("source", "parameters"),
    [
        ("return LIMIT", "LIMIT"),
        ("LIMIT = 1\nreturn LIMIT", ""),
        ("for LIMIT in [1]:\n    pass\nreturn LIMIT", ""),
        ("items = [LIMIT for LIMIT in [1]]\nreturn LIMIT", ""),
    ],
)
def test_names_the_function_binds_shadow_globals(source: str, parameters: str) -> None:
    """Python scoping: a name bound anywhere in the function is local throughout it."""
    compiled = str(compile_source(source, parameters, {"LIMIT": 30}))
    assert 'name: "LIMIT"' in compiled
    assert "int_value: 30" not in compiled


def test_globals_without_a_literal_form_are_not_inlined() -> None:
    """Objects keep their prior handling; only literal-shaped values are substituted."""
    function = compile_source("return VALUE", namespace={"VALUE": object()})
    assert returned(function).variable.name == "VALUE"


def test_policy_fields_read_globals() -> None:
    function = compile_source(
        """
        return await self.run_action(
            task(),
            retry=RetryPolicy(attempts=ATTEMPTS, exception_types=RETRY_ON, backoff_seconds=WAIT),
            timeout=TIMEOUT,
        )
        """,
        namespace={
            "ATTEMPTS": 1 + 2,
            "RETRY_ON": ["ConnectionError"],
            "WAIT": 2.5,
            "TIMEOUT": 60 * 2,
        },
    )
    retry, timeout = action_policies(function)
    assert retry.retry.max_retries == 2
    assert list(retry.retry.exception_types) == ["ConnectionError"]
    assert retry.retry.backoff.seconds == 2.5
    assert timeout.timeout.timeout.seconds == 120


def test_whole_policies_read_globals() -> None:
    function = compile_source(
        "return await self.run_action(task(), retry=POLICY, timeout=TIMEOUT)",
        namespace={
            "POLICY": RetryPolicy(attempts=4, exception_types=["ValueError"]),
            "TIMEOUT": timedelta(minutes=2) * 3,
        },
    )
    retry, timeout = action_policies(function)
    assert retry.retry.max_retries == 3
    assert list(retry.retry.exception_types) == ["ValueError"]
    assert timeout.timeout.timeout.seconds == 360


@pytest.mark.parametrize(
    ("keyword", "value", "message"),
    [
        ("retry", RetryPolicy(attempts=0), "attempts must be at least 1"),
        ("timeout", timedelta(0), "timeout must be more than zero seconds"),
        ("timeout", 0, "timeout must be more than zero seconds"),
    ],
)
def test_global_policies_are_validated_like_literals(
    keyword: str, value: object, message: str
) -> None:
    """A global's value meets the same bounds as the literal it stands for, located at its use."""
    with pytest.raises(UnsupportedPatternError, match=message) as error:
        compile_source(
            f"return await self.run_action(task(), {keyword}=VALUE)",
            namespace={"VALUE": value},
        )
    assert isinstance(error.value, UnsupportedPatternError)
    assert error.value.line == 2


def test_language_call_literal_arguments_read_globals() -> None:
    """Handlers that require a literal argument accept a global holding it."""
    function = compile_source(
        "return await asyncio.gather(task(), return_exceptions=RAISE)",
        namespace={"RAISE": False},
    )
    assert assigned(function).HasField("parallel_expr")
