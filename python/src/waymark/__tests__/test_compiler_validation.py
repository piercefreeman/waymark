"""Compiler-rejectable cases from #772 must never become misleading IR."""

from typing import Generic, TypeVar

import pytest
from pydantic import BaseModel

from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


@pytest.mark.parametrize(
    ("source", "fragment"),
    [
        ("try:\n    await task()\nfinally:\n    await task()", "try/finally"),
        (
            "try:\n    await task()\nexcept ValueError:\n    pass\nelse:\n    await task()",
            "try/else",
        ),
        ("for x in [1]:\n    pass\nelse:\n    await task()", "for/else"),
        ("while False:\n    pass\nelse:\n    await task()", "while/else"),
        ("return 1 < 2 < 3", "Chained comparisons"),
        ("return False and (1 / 0)", "Short-circuit"),
        ("return 3 or 4", "Short-circuit"),
        ("values = [1]\nvalues[0] = 2", "subscript writes"),
        ("values = [1]\nvalues[0] += 2", "subscript and attribute writes"),
        ("self.value = 1", "attribute and subscript writes"),
        ("a = b = [1]", "Chained assignments"),
        ("a, b = [1]", "Unpacking target count"),
        ("return {x: x for x in [1]}", "Dict comprehensions"),
        ("return {**{'x': 1}}", "Dictionary unpacking"),
        ("return {1: 'one'}", "keys must be known strings"),
        ("values = {'x': 1}\nfor key in values:\n    pass", "Iteration over dictionaries"),
        ("return (await task()) + 1", "Nested await"),
        ("return [await task()]", "Nested await"),
        ("return await task(**{'value': 1})", "keyword unpacking"),
        ("return await task(1, 2)", "too many positional"),
        ("return await task(1, value=2)", "multiple values"),
        ("return await task(unknown=2)", "unexpected keyword"),
        ("return 9223372036854775808", "signed 64-bit"),
        ("return -9223372036854775809", "signed 64-bit"),
        ("return 1e999", "Non-finite float"),
        (
            "try:\n    await task()\nexcept ValueError as error:\n    alias = error\n    return alias.args",
            "exception inspection",
        ),
        (
            "try:\n    await task()\nexcept ValueError as error:\n    return error[0]",
            "exception inspection",
        ),
        ("return self.missing", "Instance attribute"),
        ("return [1] < [2]", "Collection comparisons"),
    ],
)
def test_unsupported_source_is_rejected_with_a_location(source: str, fragment: str) -> None:
    with pytest.raises(UnsupportedPatternError, match=fragment) as error:
        compile_source(source)
    assert isinstance(error.value, UnsupportedPatternError)
    assert error.value.line is not None
    assert error.value.col is not None


@pytest.mark.parametrize("value", [-(2**63), 2**63 - 1])
def test_signed_integer_boundaries_are_supported(value: int) -> None:
    function = compile_source(f"return {value}")
    assert function.body.statements[0].return_stmt.value.literal.int_value == value


def test_flat_list_unpacking_preserves_all_targets() -> None:
    function = compile_source("[a, b] = [1, 2]\nreturn a + b")
    assert list(function.body.statements[0].assignment.targets) == ["a", "b"]


@pytest.mark.parametrize(
    "expression", ["1 + 2.0", "1 == 1.0", "True + 1", "4 / 2", "1.0 / 0.0", "'x' * 2", "[1] * 2"]
)
def test_known_vm_operator_mismatches_are_rejected(expression: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="VM"):
        compile_source(f"return {expression}")


def test_helper_calls_bind_the_full_signature_before_lowering() -> None:
    import ast
    import inspect

    from waymark.ir_builder import IRBuilder, TransformContext

    async def helper(required: int, *, optional: int = 2) -> int:
        return required + optional

    for expression in (
        "self.helper()",
        "self.helper(1, 2)",
        "self.helper(1, required=2)",
        "self.helper(**options)",
        "self.helper(1, extra=3)",
    ):
        context = TransformContext(function_signatures={"helper": inspect.signature(helper)})
        builder = IRBuilder(context)
        with pytest.raises(UnsupportedPatternError):
            builder.visit(ast.parse(f"async def run(self):\n    return await {expression}"))


def test_plain_instance_literal_is_resolved_inside_an_expression() -> None:
    from waymark import Workflow, workflow

    @workflow
    class LiteralAttributeWorkflow(Workflow):
        def __init__(self):
            self.value = 3

        async def run(self) -> int:
            return self.value + 2

    program = LiteralAttributeWorkflow.workflow_ir()
    main = next(function for function in program.functions if function.name == "main")
    assert main.body.statements[0].return_stmt.value.binary_op.left.literal.int_value == 3


def test_action_annotations_reject_open_type_parameters() -> None:
    import ast
    from typing import TypeVar

    from waymark.ir_builder import IRBuilder, TransformContext

    Payload = TypeVar("Payload")

    from waymark import action

    @action
    async def identity(value: Payload) -> Payload:
        return value

    builder = IRBuilder(TransformContext(), module_globals={"identity": identity})
    with pytest.raises(UnsupportedPatternError, match="open generic"):
        builder.visit(ast.parse("async def run(self):\n    return await identity(1)"))


@pytest.mark.parametrize("parameters", ["*args", "**kwargs"])
def test_variadic_workflow_parameters_are_rejected(parameters: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="Variadic"):
        compile_source("return 1", parameters)


@pytest.mark.parametrize(
    "body", ["value = 4\nvalue /= 2", "value = [1]\nvalue *= 2", "value = True\nvalue += 1"]
)
def test_augmented_operators_use_the_same_vm_type_guards(body: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="VM"):
        compile_source(body)


@pytest.mark.parametrize("operator", ["**=", "|=", "&=", "^=", "<<=", ">>="])
def test_unsupported_augmented_operators_are_never_dropped(operator: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="Unsupported binary"):
        compile_source(f"value = 3\nvalue {operator} 2\nreturn value")


def test_awaiting_a_synchronous_handler_in_a_condition_is_rejected() -> None:
    with pytest.raises(UnsupportedPatternError, match="cannot be awaited"):
        compile_source("if await len([1]):\n    return 1")


def test_flat_list_loop_targets_keep_all_bindings() -> None:
    function = compile_source("for [first, second] in [[1, 2]]:\n    await task(first + second)")
    assert list(function.body.statements[0].for_loop.loop_vars) == ["first", "second"]


@pytest.mark.parametrize("target", ["[first, [second]]", "holder.value"])
def test_unsupported_comprehension_targets_are_rejected(target: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="target"):
        compile_source(f"values = [first for {target} in items]", "items: list, holder: object")


Value = TypeVar("Value")


class GenericModel(BaseModel, Generic[Value]):
    value: Value


@pytest.mark.parametrize(
    "annotation", ["Value", "GenericModel", "list[GenericModel]", "list['Value']"]
)
def test_workflow_inputs_reject_open_type_parameters(annotation: str) -> None:
    with pytest.raises(UnsupportedPatternError, match="generic"):
        compile_source(
            "return value", f"value: {annotation}", {"Value": Value, "GenericModel": GenericModel}
        )
