"""Tests for the simplified workflow runtime execution."""

import asyncio
from dataclasses import dataclass as python_dataclass
from typing import Annotated

from pydantic import BaseModel

from proto import messages_pb2 as pb2
from rappel import registry as action_registry
from rappel.actions import action
from rappel.dependencies import Depend
from rappel.workflow_runtime import ActionExecutionResult, execute_action


@action
async def multiply(value: int) -> int:
    return value * 2


@action
async def failing_action() -> None:
    raise ValueError("intentional failure")


async def provide_suffix() -> str:
    return "suffix"


@action
async def with_dependency(value: int, suffix: Annotated[str, Depend(provide_suffix)]) -> str:
    return f"{value}-{suffix}"


def _build_action_dispatch(
    action_name: str,
    module_name: str,
    kwargs: dict,
) -> pb2.ActionDispatch:
    """Build an ActionDispatch proto message."""
    dispatch = pb2.ActionDispatch(
        action_id="test-action-id",
        instance_id="test-instance-id",
        sequence=1,
        action_name=action_name,
        module_name=module_name,
    )

    # Build kwargs
    for key, value in kwargs.items():
        arg = dispatch.kwargs.arguments.add()
        arg.key = key
        if isinstance(value, int):
            arg.value.primitive.int_value = value
        elif isinstance(value, str):
            arg.value.primitive.string_value = value
        elif isinstance(value, float):
            arg.value.primitive.double_value = value
        elif isinstance(value, bool):
            arg.value.primitive.bool_value = value

    return dispatch


def test_execute_action_with_kwargs() -> None:
    """Test executing an action with resolved kwargs."""
    if action_registry.get(__name__, "multiply") is None:
        action_registry.register(__name__, "multiply", multiply)

    dispatch = _build_action_dispatch(
        action_name="multiply",
        module_name=__name__,
        kwargs={"value": 10},
    )

    result = asyncio.run(execute_action(dispatch))

    assert isinstance(result, ActionExecutionResult)
    assert result.result == 20
    assert result.exception is None


def test_execute_action_resolves_dependencies() -> None:
    """Test executing an action with injected dependencies."""
    if action_registry.get(__name__, "with_dependency") is None:
        action_registry.register(__name__, "with_dependency", with_dependency)

    dispatch = _build_action_dispatch(
        action_name="with_dependency",
        module_name=__name__,
        kwargs={"value": 3},
    )

    result = asyncio.run(execute_action(dispatch))

    assert isinstance(result, ActionExecutionResult)
    assert result.result == "3-suffix"
    assert result.exception is None


def test_execute_action_handles_error() -> None:
    """Test that action errors are captured in the result."""
    if action_registry.get(__name__, "failing_action") is None:
        action_registry.register(__name__, "failing_action", failing_action)

    dispatch = _build_action_dispatch(
        action_name="failing_action",
        module_name=__name__,
        kwargs={},
    )

    result = asyncio.run(execute_action(dispatch))

    assert isinstance(result, ActionExecutionResult)
    assert result.result is None
    assert result.exception is not None
    assert "ValueError" in str(type(result.exception).__name__)
    assert "intentional failure" in str(result.exception)


def test_execute_action_unknown_action() -> None:
    """Test error handling for unknown action names."""
    dispatch = _build_action_dispatch(
        action_name="nonexistent_action",
        module_name=__name__,
        kwargs={},
    )

    result = asyncio.run(execute_action(dispatch))

    assert isinstance(result, ActionExecutionResult)
    assert result.result is None
    assert result.exception is not None
    assert "not registered" in str(result.exception)


# Pydantic model for testing coercion
class PersonModel(BaseModel):
    name: str
    age: int


# Dataclass for testing coercion
@python_dataclass
class PointData:
    x: int
    y: int


@action
async def greet_person(person: PersonModel) -> str:
    """Action that expects a Pydantic model argument."""
    return f"Hello {person.name}, you are {person.age} years old"


@action
async def compute_distance(point: PointData) -> int:
    """Action that expects a dataclass argument."""
    return point.x + point.y


def _build_action_dispatch_with_dict(
    action_name: str,
    module_name: str,
    kwargs: dict,
) -> pb2.ActionDispatch:
    """Build an ActionDispatch proto message with dict values.

    This version handles nested dict values for testing model coercion.
    """
    dispatch = pb2.ActionDispatch(
        action_id="test-action-id",
        instance_id="test-instance-id",
        sequence=1,
        action_name=action_name,
        module_name=module_name,
    )

    def add_value_to_proto(proto_value: pb2.WorkflowArgumentValue, value: object) -> None:
        """Recursively add a value to a proto message."""
        if isinstance(value, int):
            proto_value.primitive.int_value = value
        elif isinstance(value, str):
            proto_value.primitive.string_value = value
        elif isinstance(value, float):
            proto_value.primitive.double_value = value
        elif isinstance(value, bool):
            proto_value.primitive.bool_value = value
        elif isinstance(value, dict):
            proto_value.dict_value.SetInParent()
            for k, v in value.items():
                entry = proto_value.dict_value.entries.add()
                entry.key = k
                add_value_to_proto(entry.value, v)

    # Build kwargs
    for key, value in kwargs.items():
        arg = dispatch.kwargs.arguments.add()
        arg.key = key
        add_value_to_proto(arg.value, value)

    return dispatch


def test_execute_action_coerces_dict_to_pydantic_model() -> None:
    """Test that dict arguments are coerced to Pydantic models based on type hints."""
    if action_registry.get(__name__, "greet_person") is None:
        action_registry.register(__name__, "greet_person", greet_person)

    # Pass a dict that should be coerced to PersonModel
    dispatch = _build_action_dispatch_with_dict(
        action_name="greet_person",
        module_name=__name__,
        kwargs={"person": {"name": "Alice", "age": 30}},
    )

    result = asyncio.run(execute_action(dispatch))

    assert isinstance(result, ActionExecutionResult)
    assert result.exception is None, f"Unexpected exception: {result.exception}"
    assert result.result == "Hello Alice, you are 30 years old"


def test_execute_action_coerces_dict_to_dataclass() -> None:
    """Test that dict arguments are coerced to dataclasses based on type hints."""
    if action_registry.get(__name__, "compute_distance") is None:
        action_registry.register(__name__, "compute_distance", compute_distance)

    # Pass a dict that should be coerced to PointData
    dispatch = _build_action_dispatch_with_dict(
        action_name="compute_distance",
        module_name=__name__,
        kwargs={"point": {"x": 3, "y": 4}},
    )

    result = asyncio.run(execute_action(dispatch))

    assert isinstance(result, ActionExecutionResult)
    assert result.exception is None, f"Unexpected exception: {result.exception}"
    assert result.result == 7  # 3 + 4
