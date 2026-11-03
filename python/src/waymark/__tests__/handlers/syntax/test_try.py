"""Try handlers preserve ordinary success and caught failures."""

import asyncio
from abc import ABCMeta

import pytest

from waymark import Workflow, action, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler_errors import UnsupportedPatternError


@action
async def try_value(fail: bool) -> int:
    """Provide both the successful and exceptional action paths."""
    if fail:
        raise ValueError("expected")
    return 7


@workflow
class TryWorkflow(Workflow):
    async def run(self, fail: bool) -> int:
        """Return the successful value or the matching catch result."""
        try:
            return await try_value(fail)
        except ValueError:
            return 9


@pytest.mark.parametrize(("fail", "expected"), [(False, 7), (True, 9)])
def test_try_preserves_both_execution_paths(fail: bool, expected: int) -> None:
    """Handler extraction must preserve actual VM exception dispatch."""
    assert asyncio.run(TryWorkflow().run(fail)) == expected


@pytest.mark.parametrize("clause", ["", " ValueError", " ValueError as error"])
def test_exception_binding_is_present_only_when_named(clause: str) -> None:
    """An unnamed catch must not allocate an exception variable in the Rust compiler."""
    function = compile_source(f"try:\n    await task()\nexcept{clause}:\n    pass")
    handler = function.body.statements[0].try_except.handlers[0]
    named = clause.endswith(" as error")
    assert handler.HasField("exception_var") is named
    if named:
        assert handler.exception_var == "error"


@pytest.mark.parametrize("clause", ["finally", "except ValueError:\n    pass\nelse"])
def test_try_rejects_unsupported_clauses(clause: str) -> None:
    """Unsupported bodies are rejected with their source location instead of discarded."""
    with pytest.raises(UnsupportedPatternError, match="try/") as error:
        compile_source(f"try:\n    await task()\n{clause}:\n    await task()")
    assert isinstance(error.value, UnsupportedPatternError)
    assert error.value.line == 2


class BaseErrors:
    Failure = ValueError


class Errors(BaseErrors, metaclass=ABCMeta):
    pass


@workflow
class NamespacedExceptionWorkflow(Workflow):
    async def run(self) -> int:
        """Catch an exception resolved through a class namespace and its base classes."""
        try:
            return await try_value(True)
        except Errors.Failure:
            return 11


def test_exception_lookup_preserves_metaclass_and_inherited_names() -> None:
    """Regression: extracting TryHandler must retain exception lookup through custom metaclasses."""
    assert asyncio.run(NamespacedExceptionWorkflow().run()) == 11
