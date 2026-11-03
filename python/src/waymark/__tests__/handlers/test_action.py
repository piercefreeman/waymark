"""Durable action calls resolve identities and preserve source argument order."""

from types import ModuleType
from typing import Any, TypeVar

import pytest

from waymark import Workflow, action, workflow
from waymark.__tests__.compiler_test_helpers import compile_source, task
from waymark.compiler_errors import UnsupportedPatternError


@pytest.mark.parametrize("callee", ["alias", "package.exported"])
def test_actions_resolve_import_aliases(callee: str) -> None:
    package = ModuleType("package")
    vars(package)["exported"] = task
    function = compile_source(
        f"return await {callee}(3)", namespace={"alias": task, "package": package}
    )
    call = function.body.statements[0].assignment.value.action_call
    assert call.action_name == "task"
    assert call.kwargs[0].value.literal.int_value == 3


def test_parameter_shadowing_cannot_dispatch_a_global_action() -> None:
    @workflow
    class ShadowedActionWorkflow(Workflow):
        async def run(self, task: Any) -> int:
            return await task(1)

    with pytest.raises(UnsupportedPatternError, match="non-action"):
        ShadowedActionWorkflow.workflow_ir()


def test_local_shadowing_cannot_dispatch_a_global_action() -> None:
    @workflow
    class LocallyShadowedActionWorkflow(Workflow):
        async def run(self, replacement: Any) -> int:
            task = replacement
            return await task(1)

    with pytest.raises(UnsupportedPatternError, match="non-action"):
        LocallyShadowedActionWorkflow.workflow_ir()


def test_unrelated_attribute_with_an_action_name_is_not_an_action() -> None:
    with pytest.raises(UnsupportedPatternError, match="non-action"):
        compile_source("return await unrelated.task(1)", "unrelated: object")


def test_action_keyword_evaluation_order_matches_the_source() -> None:
    @action
    async def pair(a: int, b: int) -> list[int]:
        return [a, b]

    function = compile_source(
        "return await pair(b=values[0], a=values[1])", "values: list[int]", {"pair": pair}
    )
    call = function.body.statements[0].assignment.value.action_call
    assert [keyword.name for keyword in call.kwargs] == ["b", "a"]
    assert [keyword.value.index.index.literal.int_value for keyword in call.kwargs] == [0, 1]


Payload = TypeVar("Payload")


def test_forward_generic_action_annotations_use_the_action_module_scope() -> None:
    @action
    async def generic(value: "Payload") -> "Payload":
        return value

    with pytest.raises(UnsupportedPatternError, match="open generic"):
        compile_source("return await generic(1)", namespace={"generic": generic})
