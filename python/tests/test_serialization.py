from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel

from waymark.actions import (
    deserialize_result_payload,
    serialize_error_payload,
    serialize_result_payload,
)


class SampleModel(BaseModel):
    payload: str


@dataclass
class SampleDataclass:
    payload: str
    count: int


def test_result_round_trip_with_basemodel() -> None:
    payload = serialize_result_payload(SampleModel(payload="hello"))
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert isinstance(decoded.result, SampleModel)
    assert decoded.result.payload == "hello"


def test_error_payload_serialization() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        payload = serialize_error_payload("demo.echo", exc)
    decoded = deserialize_result_payload(payload)
    assert decoded.result is None
    assert decoded.error is not None
    assert decoded.error["type"] == "RuntimeError"
    assert decoded.error["module"] == "builtins"
    assert "boom" in decoded.error["message"]
    assert "Traceback" in decoded.error["traceback"]
    assert decoded.error["values"]["args"][0] == "boom"


def test_error_payload_captures_exception_values() -> None:
    class CustomError(Exception):
        def __init__(self, message: str, code: int) -> None:
            super().__init__(message)
            self.code = code

    try:
        raise CustomError("boom", 404)
    except CustomError as exc:
        payload = serialize_error_payload("demo.echo", exc)

    decoded = deserialize_result_payload(payload)
    assert decoded.error is not None
    assert decoded.error["values"]["code"] == 404


def test_collections_round_trip() -> None:
    payload = serialize_result_payload({"items": [1, 2, 3], "pair": (4, 5)})
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result == {"items": [1, 2, 3], "pair": (4, 5)}


def test_primitives_preserve_types() -> None:
    payload = serialize_result_payload({"count": 5, "ratio": 2.5, "flag": True, "missing": None})
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    result = decoded.result
    assert isinstance(result["count"], int)
    assert result["count"] == 5
    assert isinstance(result["ratio"], float)
    assert result["ratio"] == 2.5
    assert result["flag"] is True
    assert result["missing"] is None


def test_result_round_trip_with_dataclass() -> None:
    """Test that dataclasses can be serialized and deserialized like Pydantic models."""
    payload = serialize_result_payload(SampleDataclass(payload="world", count=42))
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert isinstance(decoded.result, SampleDataclass)
    assert decoded.result.payload == "world"
    assert decoded.result.count == 42


class ModelWithUUID(BaseModel):
    id: UUID
    name: str


class ModelWithUUIDList(BaseModel):
    ids: list[UUID]


def test_uuid_serialization() -> None:
    """Test that UUIDs are serialized as strings."""
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    payload = serialize_result_payload({"user_id": test_uuid})
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    # UUID is serialized as string
    assert decoded.result == {"user_id": str(test_uuid)}


def test_uuid_in_pydantic_model() -> None:
    """Test that UUIDs in Pydantic models round-trip correctly."""
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    payload = serialize_result_payload(ModelWithUUID(id=test_uuid, name="test"))
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert isinstance(decoded.result, ModelWithUUID)
    assert decoded.result.id == test_uuid
    assert decoded.result.name == "test"


def test_uuid_list_in_pydantic_model() -> None:
    """Test that lists of UUIDs in Pydantic models round-trip correctly."""
    test_uuids = [
        UUID("12345678-1234-5678-1234-567812345678"),
        UUID("87654321-4321-8765-4321-876543218765"),
    ]
    payload = serialize_result_payload(ModelWithUUIDList(ids=test_uuids))
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert isinstance(decoded.result, ModelWithUUIDList)
    assert decoded.result.ids == test_uuids


def test_datetime_serialization() -> None:
    """Test that datetime types are serialized as ISO strings."""
    dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
    d = date(2024, 1, 15)
    t = time(10, 30, 45)
    td = timedelta(hours=2, minutes=30)

    payload = serialize_result_payload(
        {
            "datetime": dt,
            "date": d,
            "time": t,
            "timedelta": td,
        }
    )
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    result = decoded.result
    assert result["datetime"] == dt.isoformat()
    assert result["date"] == d.isoformat()
    assert result["time"] == t.isoformat()
    assert result["timedelta"] == td.total_seconds()


def test_decimal_serialization() -> None:
    """Test that Decimal is serialized as string to preserve precision."""
    value = Decimal("123.456789012345678901234567890")
    payload = serialize_result_payload({"amount": value})
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    assert decoded.result["amount"] == str(value)


class Status(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"


class Priority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


def test_enum_serialization() -> None:
    """Test that Enums are serialized as their values."""
    payload = serialize_result_payload(
        {
            "status": Status.ACTIVE,
            "priority": Priority.HIGH,
        }
    )
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    assert decoded.result["status"] == "active"
    assert decoded.result["priority"] == 3


def test_bytes_serialization() -> None:
    """Test that bytes are serialized as base64 strings."""
    from base64 import b64encode

    data = b"hello world"
    payload = serialize_result_payload({"data": data})
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    assert decoded.result["data"] == b64encode(data).decode("ascii")


def test_path_serialization() -> None:
    """Test that Path is serialized as string."""
    p = Path("/usr/local/bin")
    payload = serialize_result_payload({"path": p})
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    assert decoded.result["path"] == str(p)


def test_set_serialization() -> None:
    """Test that sets are serialized as lists."""
    s = {1, 2, 3}
    fs = frozenset(["a", "b", "c"])
    payload = serialize_result_payload(
        {
            "set": s,
            "frozenset": fs,
        }
    )
    decoded = deserialize_result_payload(payload)
    assert decoded.error is None
    assert decoded.result is not None
    # Sets become lists, order may vary
    assert set(decoded.result["set"]) == s
    assert set(decoded.result["frozenset"]) == fs
