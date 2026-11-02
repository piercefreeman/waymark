import ast
import asyncio
import inspect
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Literal

import pytest

from waymark import Workflow, action, workflow
from waymark.compiler_context import CompilerContext
from waymark.compiler_errors import UnsupportedPatternError
from waymark.ir_builder import ActionDefinition, IRBuilder, TransformContext
from waymark.language_calls import LanguageCallHandler, LanguageCallRegistry, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class ExampleArguments:
    value: ast.expr
    mode: Literal["safe"] = field(default="safe", kw_only=True)


def example(value: object, *, mode: str = "safe") -> object:
    """Provide a real callable identity unrelated to asyncio for registry tests."""
    return value


class ExampleHandler(LanguageCallHandler[ExampleArguments]):
    target = example
    name = "example"
    arguments_type = ExampleArguments
    recommendation = "Use example(value, mode='safe')."

    def lower(
        self, arguments: ExampleArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Use the same shared expression compiler that production handlers receive."""
        value = context.expression(arguments.value)
        assert value is not None
        return LoweredCall(value=value)


def call_expression(source: str) -> ast.Call:
    """Parse one expression while retaining positions for validation diagnostics."""
    expression = ast.parse(source, mode="eval").body
    assert isinstance(expression, ast.Call)
    return expression


@pytest.fixture
def compiler_context() -> CompilerContext:
    """Supply the workflow compiler's shared operations without registering any actions."""
    return IRBuilder({}, TransformContext())._compiler_context


def test_dataclass_contract_binds_positionals_keywords_and_defaults() -> None:
    """Binding preserves runtime expressions and supplies declared compile-time defaults."""
    handler = ExampleHandler()
    positional = handler.parse_arguments(call_expression("example(value + 1)"))
    keyword = handler.parse_arguments(call_expression("example(value=value + 1, mode='safe')"))
    assert isinstance(positional.value, ast.BinOp)
    assert ast.dump(positional.value) == ast.dump(keyword.value)
    assert positional.mode == keyword.mode == "safe"


@pytest.mark.parametrize(
    "callee", ["example", "alias", "package.exported", "package.nested.exported"]
)
def test_registry_resolves_registered_identity_through_aliases(
    callee: str, compiler_context: CompilerContext
) -> None:
    """The registration alone recognizes a different target, without gather-specific logic."""
    handler = ExampleHandler()
    registry = LanguageCallRegistry([handler])
    package = ModuleType("other_package")
    vars(package).update(exported=example, nested=package)
    call = call_expression(f"{callee}(1, mode='safe')")
    resolved = registry.resolve(
        call.func, {"example": example, "alias": example, "package": package}
    )
    assert resolved is handler
    lowered = resolved.compile(call, compiler_context)
    assert lowered.value.literal.int_value == 1


def test_registry_does_not_match_a_name_without_the_registered_callable() -> None:
    """A familiar name cannot turn an unrelated value into a compiler-handled call."""
    registry = LanguageCallRegistry([ExampleHandler()])
    call = call_expression("example(1)")
    assert registry.resolve(call.func, {}) is None
    assert registry.resolve(call.func, {"example": lambda value: value}) is None
    assert registry.resolve(call.func, {"example": []}) is None


def test_registry_does_not_evaluate_attributes_or_dynamic_callees() -> None:
    """Resolving syntax must not execute properties or factory calls."""
    registry = LanguageCallRegistry([ExampleHandler()])

    class DynamicOwner:
        @property
        def example(self) -> object:
            """Fail if resolution performs ordinary object attribute access."""
            raise AssertionError("compiler must not execute property access")

    class DynamicModule(ModuleType):
        @property
        def __dict__(self) -> dict[str, Any]:
            """Fail if a module subclass can intercept dictionary lookup."""
            raise AssertionError("compiler must not execute module properties")

    namespace = {"owner": DynamicOwner()}
    assert registry.resolve(call_expression("owner.example(1)").func, namespace) is None
    assert registry.resolve(call_expression("factory().example(1)").func, namespace) is None
    assert (
        registry.resolve(
            call_expression("package.example(1)").func, {"package": DynamicModule("package")}
        )
        is None
    )


@pytest.mark.parametrize("statement", ["result =", "result: int =", "return", ""])
def test_builder_dispatches_a_new_handler_from_its_registration(statement: str) -> None:
    """A different target lowers in every statement form without adding builder branches."""
    builder = IRBuilder({}, TransformContext(), module_globals={"alias": example})
    builder._language_calls = LanguageCallRegistry([ExampleHandler()])
    builder.visit(ast.parse(f"async def run():\n    {statement} await alias(7)"))
    assert builder.function_def is not None
    statements = builder.function_def.body.statements
    assert statements[0].assignment.value.literal.int_value == 7
    if statement == "return":
        assert statements[1].return_stmt.value.variable.name == statements[0].assignment.targets[0]


@pytest.mark.parametrize(
    ("source", "local_name"),
    [
        ("async def run(asyncio):\n    await asyncio.gather()", "asyncio"),
        ("async def run(g):\n    await g()", "g"),
        ("async def run():\n    g = 1\n    await g()", "g"),
        ("async def run():\n    await g()\n    g = 1", "g"),
    ],
)
def test_local_names_cannot_resolve_to_registered_globals(source: str, local_name: str) -> None:
    """Regression: parameters and assignments shadow imports throughout the function."""
    builder = IRBuilder(
        {},
        TransformContext(),
        module_globals={"asyncio": asyncio, "g": asyncio.gather},
        function_locals=(local_name,),
    )
    builder.visit(ast.parse(source))
    assert builder.function_def is not None
    expressions = [
        statement.expr_stmt.expr
        for statement in builder.function_def.body.statements
        if statement.HasField("expr_stmt")
    ]
    assert len(expressions) == 1
    assert expressions[0].HasField("function_call")


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("example()", "missing a required argument: 'value'"),
        ("example(1, 2)", "too many positional arguments"),
        ("example(1, extra=True)", "unexpected keyword argument 'extra'"),
        ("example(1, value=2)", "multiple values for argument 'value'"),
        ("example(value=1, value=2)", "duplicate argument 'value'"),
        ("example(1, **options)", "keyword unpacking"),
        ("example(*values)", "positional unpacking"),
        ("example(1, mode=mode)", "mode must be a literal 'safe'"),
        ("example(1, mode='unsafe')", "mode must be a literal 'safe'"),
    ],
)
def test_dataclass_contract_rejects_unsupported_arguments(
    source: str, message: str, compiler_context: CompilerContext
) -> None:
    """Invalid calls fail during binding, before the example's lowering can run."""
    with pytest.raises(UnsupportedPatternError) as raised:
        ExampleHandler().compile(call_expression(source), compiler_context)
    assert isinstance(raised.value, UnsupportedPatternError)
    assert message in raised.value.message
    assert raised.value.line == 1
    assert raised.value.col is not None


async def task(value: int = 1) -> int:
    """Supply a signature for the synthetic action definition without registration."""
    return value


def compile_gather_statement(statement: str) -> ir.FunctionDef:
    """Compile one statement with a known action and three bindings of the gather target."""
    builder = IRBuilder(
        {"task": ActionDefinition("task", __name__, inspect.signature(task))},
        TransformContext(),
        module_globals={"asyncio": asyncio, "aio": asyncio, "g": asyncio.gather},
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
        ("task(), asyncio.sleep(1)", "unsupported awaitable 'asyncio.sleep'"),
        ("*[asyncio.sleep(x) for x in items]", "unsupported awaitable 'asyncio.sleep'"),
        ("*[asyncio.sleep(x) for x in items if x]", "unsupported awaitable 'asyncio.sleep'"),
        ("task(), unknown()", "unsupported awaitable 'unknown'"),
        ("self.run_action()", "run_action() requires an action call"),
        ("self.run_action(items)", "run_action() requires an action call"),
        ("self.run_action(task(), unknown=True)", "run_action() does not take"),
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
    with pytest.raises(UnsupportedPatternError, match="own assignment, return, or expression"):
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

    program = ShadowedGatherWorkflow.workflow_ir()
    main = next(function for function in program.functions if function.name == "main")
    assert main.body.statements[0].expr_stmt.expr.HasField("function_call")


def test_captured_nonlocal_keeps_its_compiler_diagnostic() -> None:
    """Scope resolution must leave unsupported statements to the compiler's diagnostics."""
    captured = 0

    @workflow
    class NonlocalWorkflow(Workflow):
        async def run(self) -> int:
            nonlocal captured
            captured = 1
            return captured

    with pytest.raises(UnsupportedPatternError, match="Nonlocal statements are not supported"):
        NonlocalWorkflow.workflow_ir()


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
