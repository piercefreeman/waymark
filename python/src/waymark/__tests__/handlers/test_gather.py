"""Supported gather forms, rejection rules, and lowering regressions."""

import ast
import asyncio
import inspect
from typing import Any

import pytest

from waymark import Workflow, action, workflow
from waymark.compiler_errors import UnsupportedPatternError
from waymark.ir_builder import IRBuilder, TransformContext
from waymark.proto import ast_pb2 as ir


@action
async def task(value: int = 1) -> int:
    """Supply a signature for the synthetic action definition without registration."""
    return value


def compile_gather_statement(statement: str) -> ir.FunctionDef:
    """Compile one statement with a known action and three bindings of the gather target."""
    builder = IRBuilder(
        TransformContext(),
        module_globals={"asyncio": asyncio, "aio": asyncio, "g": asyncio.gather, "task": task},
        receiver_type=Workflow,
    )
    builder.visit(ast.parse(f"async def run(items, options):\n    {statement}\n"))
    assert builder.function_def is not None
    return builder.function_def


@pytest.mark.parametrize("option", ["", ", return_exceptions=False"])
@pytest.mark.parametrize("callee", ["asyncio.gather", "g", "aio.gather"])
def test_gather_accepts_default_and_explicit_false(option: str, callee: str) -> None:
    """Supported options and aliases preserve every action argument in source order."""
    function = compile_gather_statement(f"results = await {callee}(task(1), task(2){option})")
    assignment = function.body.statements[0].assignment
    assert list(assignment.targets) == ["results"]
    assert [
        call.action.kwargs[0].value.literal.int_value
        for call in assignment.value.parallel_expr.calls
    ] == [1, 2]


@pytest.mark.parametrize(
    "statement",
    [
        "results = await asyncio.gather(task(), return_exceptions=True)",
        "results = await aio.gather(task(), return_exceptions=True)",
        "await asyncio.gather(task(), return_exceptions=True)",
        "results: list = await asyncio.gather(task(), return_exceptions=True)",
        "return await asyncio.gather(task(), return_exceptions=True)",
        "results = await asyncio.gather(*[task(x) for x in items], return_exceptions=True)",
        "results = await asyncio.gather(*[task(x) for x in items if x], return_exceptions=True)",
        "results = await asyncio.gather(*[task(x) for x, y in items], return_exceptions=True)",
        "results = (await asyncio.gather(task(), return_exceptions=True))[0]",
    ],
)
def test_all_gather_paths_reject_exception_collection(statement: str) -> None:
    """Every statement and comprehension path must reject the unimplemented option."""
    with pytest.raises(UnsupportedPatternError, match="return_exceptions must be a literal False"):
        compile_gather_statement(statement)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ("task(), unknown=True", "unexpected keyword argument 'unknown'"),
        ("task(), return_exceptions=False, unknown=True", "unexpected keyword argument 'unknown'"),
        ("task(), return_exceptions=False, **options", "keyword unpacking"),
        ("task(), **options, return_exceptions=False", "keyword unpacking"),
        ("task(), return_exceptions=False, return_exceptions=False", "duplicate argument"),
        ("task(), return_exceptions=0", "return_exceptions must be a literal False"),
        ("task(), return_exceptions=options", "return_exceptions must be a literal False"),
        ("task(), items", "unsupported awaitables"),
        ("task(), asyncio.sleep(1)", "unsupported awaitable"),
        ("*[asyncio.sleep(x) for x in items]", "unsupported awaitable"),
        ("*[asyncio.sleep(x) for x in items if x]", "unsupported awaitable"),
        ("task(), unknown()", "unsupported awaitable 'unknown'"),
        ("self.run_action()", "missing a required argument"),
        ("self.run_action(items)", "unsupported awaitable"),
        ("self.run_action(task(), unknown=True)", "unexpected keyword argument"),
        ("*items", "unsupported awaitable '*items'"),
        ("task(), *[task(x) for x in items]", "must be the only positional argument"),
        ("*[task(x) async for x in items]", "single synchronous comprehension loop"),
        ("*[task(x) for x in items for y in items]", "single synchronous comprehension loop"),
        ("*[x for x in items]", "comprehension element must be a call"),
        ("(task(x) for x in items)", "unsupported awaitables"),
        ("", "empty gathers are not supported"),
    ],
)
def test_gather_never_drops_unsupported_arguments(arguments: str, message: str) -> None:
    """Unknown options and unsupported operands fail rather than disappearing from IR."""
    with pytest.raises(UnsupportedPatternError) as raised:
        compile_gather_statement(f"results = await asyncio.gather({arguments})")
    assert isinstance(raised.value, UnsupportedPatternError)
    assert message in raised.value.message


def test_filtered_gather_validates_keywords_before_lowering() -> None:
    """Emitting a filter's setup statements must not bypass the call contract."""
    with pytest.raises(UnsupportedPatternError, match="unexpected keyword argument 'unknown'"):
        compile_gather_statement(
            "results = await asyncio.gather(*[task(x) for x in items if x], unknown=True)"
        )


def test_module_alias_cannot_bypass_keyword_unpacking_validation() -> None:
    """Regression: an asyncio module alias must use the same argument contract."""
    with pytest.raises(UnsupportedPatternError, match="keyword unpacking"):
        compile_gather_statement("await aio.gather(task(), return_exceptions=False, **options)")


def test_return_gather_uses_parallel_lowering() -> None:
    """A direct return waits for parallel results and returns their temporary variable."""
    function = compile_gather_statement("return await asyncio.gather(task(1), task(2))")
    assignment, returned = function.body.statements
    assert len(assignment.assignment.value.parallel_expr.calls) == 2
    assert returned.return_stmt.value.variable.name == assignment.assignment.targets[0]


@pytest.mark.parametrize("statement", ["results =", "results: list =", "return", ""])
def test_handler_lowers_filtered_destructuring_in_every_statement_form(statement: str) -> None:
    """Regression: handler-owned setup must precede the spread for every dispatch path."""
    function = compile_gather_statement(
        f"{statement} await asyncio.gather(*[task(x + y) for x, y in items if x > 0])"
    )
    statements = list(function.body.statements)
    if statement == "return":
        returned = statements.pop()
        assert returned.return_stmt.value.variable.name == statements[-1].assignment.targets[0]
    assert len(statements) == 3
    initialized, filtered, spread_statement = statements
    assert initialized.assignment.value.HasField("list")
    assert filtered.HasField("for_loop")
    spread = spread_statement.assignment.value.spread_expr
    assert spread.collection.variable.name == initialized.assignment.targets[0]
    argument = spread.call.action.kwargs[0].value.binary_op
    assert argument.left.index.object.variable.name == spread.loop_var
    assert argument.right.index.object.variable.name == spread.loop_var


def test_nested_gather_does_not_fall_back_to_unknown_function() -> None:
    """An unsupported expression context reports the required standalone call form."""
    with pytest.raises(UnsupportedPatternError, match="Nested await"):
        compile_gather_statement("results = (await asyncio.gather(task()))[0]")


@action
async def registered_task() -> int:
    """Provide a globally discoverable action for complete workflow compilation tests."""
    return 1


def test_gather_resolves_a_callable_captured_in_a_closure() -> None:
    """Regression: a default generator's locals must not shadow a captured call target."""
    gather_alias = asyncio.gather

    @workflow
    class CapturedGatherWorkflow(Workflow):
        async def run(
            self, items: object = (gather_alias for gather_alias in (1, 2))
        ) -> tuple[int | BaseException]:
            return await gather_alias(registered_task(), return_exceptions=True)

    with pytest.raises(UnsupportedPatternError, match="return_exceptions must be a literal False"):
        CapturedGatherWorkflow.workflow_ir()


def test_default_lambda_does_not_hide_parameter_shadowing() -> None:
    """Regression: function locals come from its code, not a default expression's scope."""

    @workflow
    class ShadowedGatherWorkflow(Workflow):
        async def run(self, asyncio: Any, items: object = lambda: 1) -> None:
            await asyncio.gather()

    with pytest.raises(UnsupportedPatternError, match="Awaiting non-action"):
        ShadowedGatherWorkflow.workflow_ir()


@workflow
class UnsupportedGatherWorkflow(Workflow):
    async def run(self) -> tuple[int | BaseException]:
        return await asyncio.gather(registered_task(), return_exceptions=True)


def test_gather_error_preserves_original_source_location() -> None:
    """Regression: diagnostics must point into the original indented method source."""
    with pytest.raises(UnsupportedPatternError) as raised:
        UnsupportedGatherWorkflow.workflow_ir()
    assert isinstance(raised.value, UnsupportedPatternError)
    assert raised.value.filename == __file__
    assert raised.value.line is not None
    source = inspect.getsource(UnsupportedGatherWorkflow)
    _, start = inspect.getsourcelines(UnsupportedGatherWorkflow)
    failing_line = source.splitlines()[raised.value.line - start]
    assert "return_exceptions=True" in failing_line
    assert raised.value.col is not None
    assert failing_line[raised.value.col - 1 :].startswith("True")


@pytest.mark.parametrize("target", ["first, second, third", "(first,)"])
def test_gather_rejects_known_unpacking_arity_mismatches(target: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="target count"):
        compile_gather_statement(f"{target} = await asyncio.gather(task(), task())")
