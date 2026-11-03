"""Data-constructor contracts must never discard Python initialization behavior."""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Generic, TypeVar

import pytest
from pydantic import AfterValidator, BaseModel, Field, computed_field

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


def forbidden_factory() -> list[int]:
    raise AssertionError("the compiler must never execute a factory")


@dataclass
class Data:
    count: int
    enabled: bool = True


class Model(BaseModel):
    count: int
    values: list[int] = [1, 2]


@dataclass
class FactoryData:
    values: list[int] = field(default_factory=forbidden_factory)


class FactoryModel(BaseModel):
    values: list[int] = Field(default_factory=forbidden_factory)


class HookModel(BaseModel):
    count: int = 0

    def model_post_init(self, context: object) -> None:
        raise AssertionError("must not run")


class InheritedHookModel(HookModel):
    pass


class ComputedModel(BaseModel):
    @computed_field
    @property
    def count(self) -> int:
        raise AssertionError("must not run")


class AnnotatedModel(BaseModel):
    count: Annotated[int, AfterValidator(lambda value: value + 1)] = 0


@dataclass
class HookData:
    count: int = 0

    def __post_init__(self) -> None:
        raise AssertionError("must not run")


@dataclass
class InheritedHookData(HookData):
    pass


@dataclass(init=False)
class CustomData:
    count: int = 0

    def __init__(self) -> None:
        raise AssertionError("must not run")


Value = TypeVar("Value")


class GenericModel(BaseModel, Generic[Value]):
    value: Value


@pytest.mark.parametrize(
    "model",
    [
        FactoryData,
        FactoryModel,
        HookModel,
        InheritedHookModel,
        ComputedModel,
        AnnotatedModel,
        HookData,
        InheritedHookData,
        CustomData,
        GenericModel,
    ],
)
def test_unsupported_initialization_is_rejected_without_running_it(model: type) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source("return Constructor()", namespace={"Constructor": model})


@pytest.mark.parametrize("arguments", ["", "1, count=2", "1, 2, 3", "unknown=1", "**options"])
def test_dataclass_signature_is_fully_bound(arguments: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(f"return Constructor({arguments})", namespace={"Constructor": Data})


def test_pydantic_positional_arguments_are_rejected() -> None:
    with pytest.raises(UnsupportedPatternError, match="positional"):
        compile_source("return Constructor(1)", namespace={"Constructor": Model})


def test_model_literal_collection_defaults_are_preserved() -> None:
    function = compile_source("return Constructor(count=3)", namespace={"Constructor": Model})
    value = function.body.statements[0].return_stmt.value.dict
    fields = {entry.key.literal.string_value: entry.value for entry in value.entries}
    assert fields["count"].literal.int_value == 3
    assert [element.literal.int_value for element in fields["values"].list.elements] == [1, 2]


def test_dataclass_positional_and_default_fields_are_preserved() -> None:
    function = compile_source("return Constructor(3)", namespace={"Constructor": Data})
    fields = {
        entry.key.literal.string_value: entry.value
        for entry in function.body.statements[0].return_stmt.value.dict.entries
    }
    assert fields["count"].literal.int_value == 3
    assert fields["enabled"].literal.bool_value


class TransformingSetter:
    def __setattr__(self, name: str, value: int) -> None:
        object.__setattr__(self, name, value + 1)


@dataclass
class InheritedSetterData(TransformingSetter):
    count: int


class AllocatingBase:
    def __new__(cls):
        raise AssertionError("compiler must not allocate an instance")


@dataclass
class InheritedAllocationData(AllocatingBase):
    count: int


@pytest.mark.parametrize("model", [InheritedSetterData, InheritedAllocationData])
def test_inherited_construction_hooks_are_rejected(model: type) -> None:
    with pytest.raises(UnsupportedPatternError, match="hooks"):
        compile_source("return Constructor(1)", namespace={"Constructor": model})


def test_model_keyword_evaluation_order_matches_the_source() -> None:
    function = compile_source(
        "return Constructor(enabled=values[0], count=values[1])",
        "values: list",
        {"Constructor": Data},
    )
    entries = function.body.statements[0].return_stmt.value.dict.entries
    assert [entry.key.literal.string_value for entry in entries] == ["enabled", "count"]
    assert [entry.value.index.index.literal.int_value for entry in entries] == [0, 1]


class CollectionDefaultsModel(BaseModel):
    values: list[int] = []
    mapping: dict[str, int] = {"key": 2}
    pair: tuple[int, int] = (3, 4)


@workflow
class CollectionDefaultsWorkflow(Workflow):
    async def run(self) -> list[int]:
        model = CollectionDefaultsModel()
        return [len(model.values), model.mapping["key"], model.pair[0], model.pair[1]]


def test_collection_defaults_are_available_at_runtime() -> None:
    """Regression #829: omitted collection defaults must populate readable fields."""
    assert asyncio.run(CollectionDefaultsWorkflow().run()) == [0, 2, 3, 4]


class DefaultStatus(Enum):
    READY = "ready"


class EnumDefaultModel(BaseModel):
    state: DefaultStatus = DefaultStatus.READY


def test_unsupported_default_must_be_supplied_explicitly() -> None:
    """Regression #829: an enum default cannot silently disappear from the lowered dictionary."""
    with pytest.raises(UnsupportedPatternError, match="Unsupported default"):
        compile_source("return Constructor()", namespace={"Constructor": EnumDefaultModel})
    function = compile_source(
        "return Constructor(state=Status.READY)",
        namespace={"Constructor": EnumDefaultModel, "Status": DefaultStatus},
    )
    entry = function.body.statements[0].return_stmt.value.dict.entries[0]
    assert entry.key.literal.string_value == "state"
    assert entry.value.literal.string_value == "ready"


@pytest.mark.parametrize("model", [FactoryData, FactoryModel])
def test_explicit_fields_do_not_enable_factory_models(model: type) -> None:
    """Regression #829: factories remain unsupported even when a call supplies their fields."""
    with pytest.raises(UnsupportedPatternError, match="factor"):
        compile_source("return Constructor(values=[])", namespace={"Constructor": model})
