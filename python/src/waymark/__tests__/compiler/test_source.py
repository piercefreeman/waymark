"""Source discovery retains lexical bindings and source coordinates."""

import inspect
from typing import Generic, TypeVar

import pytest
from pydantic import BaseModel

from waymark import Workflow
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.source import load_workflow_source, parse_function


def test_source_discovery_keeps_helper_defaults_and_closure_bindings() -> None:
    """Inherited helpers are parsed once, with their defining closure and signature."""
    offset = 4

    class Base(Workflow):
        async def helper(self, value: int = 3) -> int:
            return value + offset

    class Example(Base):
        async def run(self) -> int:
            return await self.helper()

    functions = load_workflow_source(Example)
    assert [function.name for function in functions] == ["main", "helper"]
    assert functions[1].namespace["offset"] == 4
    assert functions[1].signature.parameters["value"].default == 3
    assert "self" not in functions[1].signature.parameters


def test_parser_preserves_original_diagnostic_coordinates() -> None:
    """Dedenting source must not move diagnostics away from the offending expression."""

    def example(self):
        return object()

    parsed = parse_function(example)
    location = parsed.node.body[0]
    error = parsed.locate_error(
        UnsupportedPatternError(
            "unsupported", "rewrite", line=location.lineno, col=location.col_offset
        )
    )
    assert error.filename == inspect.getsourcefile(example)
    assert error.line == inspect.getsourcelines(example)[1] + 1
    assert error.col == 9


Request = TypeVar("Request")
Item = TypeVar("Item")


class Prompt(BaseModel):
    text: str


class GenericBase(Workflow, Generic[Request]):
    """A library base whose helpers are typed by the request its subclasses choose."""

    async def handle(self, request: Request) -> Request | None:
        return request

    async def handle_many(self, requests: list[Request]) -> list[Request]:
        return requests


class GenericMiddle(GenericBase[list[Item]], Generic[Item]):
    """A base that forwards its own parameter into the library's."""


def test_specialized_generic_helpers_compile_without_open_parameters() -> None:
    """Regression: helpers on `GenericBase[Request]` were rejected as open generics.

    The compiler read `Request` from the helper's own module, where it is still
    the open TypeVar, instead of applying the subclass's `GenericBase[Prompt]`.
    Libraries had to copy every helper per subclass to compile at all.
    """

    class Concrete(GenericBase[Prompt]):
        async def run(self, prompt: Prompt) -> Prompt | None:
            await self.handle_many([prompt])
            return await self.handle(prompt)

    functions = load_workflow_source(Concrete)
    assert [function.name for function in functions] == ["main", "handle_many", "handle"]
    helper = next(function for function in functions if function.name == "handle")
    assert helper.signature.parameters["request"].annotation is Prompt
    assert helper.signature.return_annotation == Prompt | None
    assert helper.namespace["Request"] is Prompt
    assert {function.name for function in Concrete.workflow_ir().functions} == {
        "main",
        "handle_many",
        "handle",
    }


def test_generic_parameters_resolve_through_intermediate_bases() -> None:
    class Concrete(GenericMiddle[Prompt]):
        async def run(self, prompt: Prompt) -> list[Prompt] | None:
            return await self.handle([prompt])

    helper = next(
        function for function in load_workflow_source(Concrete) if function.name == "handle"
    )
    assert helper.signature.parameters["request"].annotation == list[Prompt]
    Concrete.workflow_ir()


def test_unspecialized_generic_helpers_stay_rejected() -> None:
    """A subclass that never specializes the base still compiles an open helper."""

    class Unspecialized(GenericBase):  # type: ignore[type-arg]
        async def run(self, prompt: Prompt) -> Prompt | None:
            return await self.handle(prompt)

    with pytest.raises(UnsupportedPatternError, match="open generic"):
        Unspecialized.workflow_ir()


def test_method_type_parameters_are_not_class_bindings() -> None:
    """Only the class's own parameters are specialized; a generic method stays open."""

    class Concrete(GenericBase[Prompt]):
        async def echo(self, value: Item) -> Item:
            return value

        async def run(self, prompt: Prompt) -> Prompt:
            return await self.echo(prompt)

    with pytest.raises(UnsupportedPatternError, match="open generic"):
        Concrete.workflow_ir()
