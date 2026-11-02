"""Access handlers preserve runtime indexing and statically known instance attributes."""

import ast
import asyncio
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.context import CompilerContext, LoweredCall
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.source import InstanceAttrs
from waymark.handlers.model import ModelArguments, ModelHandler
from waymark.ir_builder import IRBuilder, TransformContext


@workflow
class AccessWorkflow(Workflow):
    def __init__(self):
        """Provide a compiler-readable instance constant."""
        self.offset = 2

    async def run(self, values: list[int]) -> int:
        """Combine runtime indexing with a resolved instance attribute."""
        return values[0] + self.offset


def test_access_preserves_runtime_values_and_static_attributes() -> None:
    """Nested expression dispatch reaches the same access handler."""
    assert asyncio.run(AccessWorkflow().run([5])) == 7


@pytest.mark.parametrize("expression", ["error.args", "error[0]"])
def test_exception_inspection_is_rejected(expression: str) -> None:
    """Both access forms use the same exception-type facts."""
    with pytest.raises(UnsupportedPatternError, match="exception inspection"):
        compile_source(
            f"try:\n    await task()\nexcept ValueError as error:\n    return {expression}"
        )


class Status(IntEnum):
    OK = 4


status_type = Status


@workflow
class InitializerEnumWorkflow(Workflow):
    def __init__(self):
        """Capture enum names in the initializer's defining scope."""
        self.state = status_type.OK
        self.limit = status_type.OK + 1

    async def run(self, status_type: Any) -> list[int]:
        """A run argument with the same spelling must not rebind stored initializer syntax."""
        return [self.state, self.limit]


@workflow
class LocalEnumShadowWorkflow(Workflow):
    async def run(self, status_type: Any) -> int:
        """Names written in run still refer to its arguments."""
        return status_type.OK


def test_initializer_enums_keep_their_defining_scope() -> None:
    """Regression: run arguments must not rebind enum names captured in initializer expressions."""
    assert asyncio.run(InitializerEnumWorkflow().run({"OK": 99})) == [4, 5]


def test_run_arguments_still_shadow_global_enum_aliases() -> None:
    """Regression: initializer scope resolution must not replace run-local access with global enums."""
    assert asyncio.run(LocalEnumShadowWorkflow().run({"OK": 99})) == 99


@dataclass
class NestedValue:
    value: int


@dataclass
class NestedContainer:
    inner: NestedValue


@workflow
class InitializerConstructorWorkflow(Workflow):
    def __init__(self):
        """Provide nested constructor syntax for compile-time attribute access."""
        self.config = NestedContainer(inner=NestedValue(value=3))

    async def run(self) -> int:
        """Read a nested field without rebuilding either constructor."""
        return self.config.inner.value


def test_initializer_constructors_are_lowered_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: selecting a nested initializer field must not lower its constructor twice."""
    lowerings: list[type] = []
    original = ModelHandler.lower

    def count_lowerings(
        self: ModelHandler, arguments: ModelArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Count emitted model values while retaining the production lowering behavior."""
        lowerings.append(self.model)
        return original(self, arguments, node, context)

    monkeypatch.setattr(ModelHandler, "lower", count_lowerings)
    assert asyncio.run(InitializerConstructorWorkflow().run()) == 3
    assert lowerings == [NestedContainer, NestedValue]


def test_initializer_dictionary_access_keeps_the_last_duplicate_key() -> None:
    """Regression: selecting from lowered entries must agree with Python dictionary construction."""
    initializer = ast.parse('{"value": 1, "value": 2}', mode="eval").body
    builder = IRBuilder(
        TransformContext(), instance_attrs=InstanceAttrs({"options": initializer}, set())
    )
    builder.visit(ast.parse("async def run(self):\n    return self.options.value"))
    assert builder.function_def is not None
    assert builder.function_def.body.statements[0].return_stmt.value.literal.int_value == 2
