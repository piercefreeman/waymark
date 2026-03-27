"""Tests for the simplified workflow runtime execution."""

import asyncio
from dataclasses import dataclass as python_dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel

from proto import messages_pb2 as pb2
from waymark import registry as action_registry
from waymark.actions import action
from waymark.dependencies import Depend
from waymark.workflow_runtime import ActionExecutionResult, _coerce_value, execute_action


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


# ---- Tests for primitive type coercion ----


def test_coerce_uuid_from_string() -> None:
    """Test that UUID strings are coerced to UUID objects."""
    uuid_str = "12345678-1234-5678-1234-567812345678"
    result = _coerce_value(uuid_str, UUID)
    assert isinstance(result, UUID)
    assert str(result) == uuid_str


def test_coerce_datetime_from_string() -> None:
    """Test that ISO datetime strings are coerced to datetime objects."""
    dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
    result = _coerce_value(dt.isoformat(), datetime)
    assert isinstance(result, datetime)
    assert result == dt


def test_coerce_date_from_string() -> None:
    """Test that ISO date strings are coerced to date objects."""
    d = date(2024, 1, 15)
    result = _coerce_value(d.isoformat(), date)
    assert isinstance(result, date)
    assert result == d


def test_coerce_time_from_string() -> None:
    """Test that ISO time strings are coerced to time objects."""
    t = time(10, 30, 45)
    result = _coerce_value(t.isoformat(), time)
    assert isinstance(result, time)
    assert result == t


def test_coerce_timedelta_from_seconds() -> None:
    """Test that numeric values are coerced to timedelta objects."""
    td = timedelta(hours=2, minutes=30)
    result = _coerce_value(td.total_seconds(), timedelta)
    assert isinstance(result, timedelta)
    assert result == td


def test_coerce_decimal_from_string() -> None:
    """Test that string values are coerced to Decimal objects."""
    d = Decimal("123.456789012345678901234567890")
    result = _coerce_value(str(d), Decimal)
    assert isinstance(result, Decimal)
    assert result == d


def test_coerce_bytes_from_base64() -> None:
    """Test that base64 strings are coerced to bytes."""
    from base64 import b64encode

    data = b"hello world"
    result = _coerce_value(b64encode(data).decode("ascii"), bytes)
    assert isinstance(result, bytes)
    assert result == data


def test_coerce_path_from_string() -> None:
    """Test that strings are coerced to Path objects."""
    p = Path("/usr/local/bin")
    result = _coerce_value(str(p), Path)
    assert isinstance(result, Path)
    assert result == p


def test_coerce_list_of_uuids() -> None:
    """Test that list[UUID] coerces string items to UUIDs."""
    uuid_strs = [
        "12345678-1234-5678-1234-567812345678",
        "87654321-4321-8765-4321-876543218765",
    ]
    result = _coerce_value(uuid_strs, list[UUID])
    assert isinstance(result, list)
    assert all(isinstance(u, UUID) for u in result)
    assert [str(u) for u in result] == uuid_strs


def test_coerce_set_of_datetimes() -> None:
    """Test that set[datetime] coerces list of ISO strings to set of datetimes."""
    dt1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    dt2 = datetime(2024, 1, 16, 11, 0, 0, tzinfo=timezone.utc)
    result = _coerce_value([dt1.isoformat(), dt2.isoformat()], set[datetime])
    assert isinstance(result, set)
    assert all(isinstance(d, datetime) for d in result)
    assert result == {dt1, dt2}


def test_coerce_dict_with_uuid_values() -> None:
    """Test that dict[str, UUID] coerces string values to UUIDs."""
    uuid_str = "12345678-1234-5678-1234-567812345678"
    result = _coerce_value({"user_id": uuid_str}, dict[str, UUID])
    assert isinstance(result, dict)
    assert isinstance(result["user_id"], UUID)
    assert str(result["user_id"]) == uuid_str


def test_coerce_preserves_already_correct_type() -> None:
    """Test that values already of the correct type are preserved."""
    uuid_obj = UUID("12345678-1234-5678-1234-567812345678")
    result = _coerce_value(uuid_obj, UUID)
    assert result is uuid_obj


def test_coerce_none_returns_none() -> None:
    """Test that None values are preserved."""
    result = _coerce_value(None, UUID)
    assert result is None
