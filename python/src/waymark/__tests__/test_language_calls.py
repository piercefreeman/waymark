"""Contracts shared by every registered language-call handler."""

import ast
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any, Literal

import pytest

from waymark import Workflow, workflow
from waymark.compiler_context import CompilerContext
from waymark.compiler_errors import UnsupportedPatternError
from waymark.ir_builder import IRBuilder, TransformContext
from waymark.language_calls import LanguageCallHandler, LanguageCallRegistry, LoweredCall


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
    return IRBuilder(TransformContext())._compiler_context


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
    """The registration alone recognizes a different target, without target-specific logic."""
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
    builder = IRBuilder(TransformContext(), module_globals={"alias": example})
    builder._language_calls = LanguageCallRegistry([ExampleHandler()])
    builder.visit(ast.parse(f"async def run():\n    {statement} await alias(7)"))
    assert builder.function_def is not None
    statements = builder.function_def.body.statements
    value = statements[0].expr_stmt.expr if statement == "" else statements[0].assignment.value
    assert value.literal.int_value == 7
    if statement == "return":
        assert statements[1].return_stmt.value.variable.name == statements[0].assignment.targets[0]


@pytest.mark.parametrize(
    ("source", "local_name"),
    [
        ("async def run(package):\n    await package.exported()", "package"),
        ("async def run(alias):\n    await alias()", "alias"),
        ("async def run():\n    alias = 1\n    await alias()", "alias"),
        ("async def run():\n    await alias()\n    alias = 1", "alias"),
    ],
)
def test_local_names_cannot_resolve_to_registered_globals(source: str, local_name: str) -> None:
    """Regression: parameters and assignments shadow imports throughout the function."""
    package = ModuleType("example_package")
    vars(package)["exported"] = example
    builder = IRBuilder(
        TransformContext(),
        module_globals={"package": package, "alias": example},
        function_locals=(local_name,),
    )
    builder._language_calls = LanguageCallRegistry([ExampleHandler()])
    with pytest.raises(UnsupportedPatternError, match="Awaiting non-action"):
        builder.visit(ast.parse(source))


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
