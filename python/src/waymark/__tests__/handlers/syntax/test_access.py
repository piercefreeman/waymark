"""Access handlers preserve runtime indexing and statically known instance attributes."""

import asyncio
from enum import IntEnum
from typing import Any

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


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
