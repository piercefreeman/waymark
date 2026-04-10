from base64 import b64encode
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, Union
from uuid import UUID

import pytest
from pydantic import BaseModel

from waymark.type_coercion import _coerce_dict_to_dataclass, coerce_value

UUID_STR = "12345678-1234-5678-1234-567812345678"
UUID_OBJ = UUID(UUID_STR)
SECOND_UUID_STR = "87654321-4321-8765-4321-876543218765"
SECOND_UUID_OBJ = UUID(SECOND_UUID_STR)
RECORDED_AT = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
NEXT_RECORDED_AT = datetime(2024, 1, 16, 11, 15, 0, tzinfo=timezone.utc)
RECORDED_DATE = date(2024, 1, 15)
RECORDED_TIME = time(10, 30, 45)
DURATION = timedelta(hours=2, minutes=30)
DECIMAL_VALUE = Decimal("123.456789012345678901234567890")
BINARY_PAYLOAD = b"hello world"
BINARY_PAYLOAD_B64 = b64encode(BINARY_PAYLOAD).decode("ascii")
BIN_PATH = Path("/usr/local/bin")


@dataclass
class DataclassWithDefaults:
    name: str
    enabled: bool = True
    tags: list[str] = field(default_factory=list)


@dataclass
class StrictDataclass:
    name: str
    retries: int
    active: bool


@dataclass
class TypedDataclass:
    reading_id: UUID


class TypedModel(BaseModel):
    name: str


def _assert_coerced_value(annotation: Any, payload: Any, expected: Any) -> None:
    result = coerce_value(payload, annotation)

    if expected is None:
        assert result is None
        return

    assert result == expected
    assert isinstance(result, type(expected))


def _pep604_optional(inner_type: Any) -> Any:
    return inner_type | None


def _typing_optional(inner_type: Any) -> Any:
    return Optional[inner_type]


def _typing_union_optional(inner_type: Any) -> Any:
    return Union[inner_type, None]


def _pep604_union(primary_type: Any, secondary_type: Any) -> Any:
    return primary_type | secondary_type


def _typing_union(primary_type: Any, secondary_type: Any) -> Any:
    return Union[primary_type, secondary_type]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"name": "alpha"}, DataclassWithDefaults(name="alpha")),
        (
            {"name": "beta", "enabled": False},
            DataclassWithDefaults(name="beta", enabled=False),
        ),
        (
            {"name": "gamma", "tags": ["ops"]},
            DataclassWithDefaults(name="gamma", tags=["ops"]),
        ),
    ],
)
def test_coerce_dict_to_dataclass_uses_defaults_for_missing_fields(
    payload: dict[str, object],
    expected: DataclassWithDefaults,
) -> None:
    result = _coerce_dict_to_dataclass(payload, DataclassWithDefaults)

    assert result == expected


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {"name": "alpha", "retries": 1, "active": True},
            StrictDataclass(name="alpha", retries=1, active=True),
        ),
        (
            {"name": "beta", "retries": 3, "active": False},
            StrictDataclass(name="beta", retries=3, active=False),
        ),
    ],
)
def test_coerce_dict_to_dataclass_accepts_exact_payload(
    payload: dict[str, object],
    expected: StrictDataclass,
) -> None:
    result = _coerce_dict_to_dataclass(payload, StrictDataclass)

    assert result == expected


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"name": "alpha", "retries": 1, "active": True, "extra": "value"},
            "StrictDataclass got unexpected field(s): extra",
        ),
        (
            {
                "name": "beta",
                "retries": 2,
                "active": False,
                "extra_one": "value",
                "extra_two": "value",
            },
            "StrictDataclass got unexpected field(s): extra_one, extra_two",
        ),
    ],
)
def test_coerce_dict_to_dataclass_rejects_extra_fields(
    payload: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(TypeError) as exc_info:
        _coerce_dict_to_dataclass(payload, StrictDataclass)

    assert str(exc_info.value) == message


@pytest.mark.parametrize(
    ("annotation", "payload", "expected"),
    [
        pytest.param(UUID, UUID_STR, UUID_OBJ, id="uuid"),
        pytest.param(datetime, RECORDED_AT.isoformat(), RECORDED_AT, id="datetime"),
        pytest.param(date, RECORDED_DATE.isoformat(), RECORDED_DATE, id="date"),
        pytest.param(time, RECORDED_TIME.isoformat(), RECORDED_TIME, id="time"),
        pytest.param(timedelta, DURATION.total_seconds(), DURATION, id="timedelta"),
        pytest.param(Decimal, str(DECIMAL_VALUE), DECIMAL_VALUE, id="decimal"),
        pytest.param(bytes, BINARY_PAYLOAD_B64, BINARY_PAYLOAD, id="bytes"),
        pytest.param(Path, str(BIN_PATH), BIN_PATH, id="path"),
    ],
)
def test_coerce_value_primitives(annotation: Any, payload: Any, expected: Any) -> None:
    _assert_coerced_value(annotation, payload, expected)


@pytest.mark.parametrize(
    ("annotation", "payload", "expected"),
    [
        pytest.param(
            list[UUID],
            [UUID_STR, SECOND_UUID_STR],
            [UUID_OBJ, SECOND_UUID_OBJ],
            id="builtins-list",
        ),
        pytest.param(
            List[UUID],
            [UUID_STR, SECOND_UUID_STR],
            [UUID_OBJ, SECOND_UUID_OBJ],
            id="typing-list",
        ),
        pytest.param(
            set[datetime],
            [RECORDED_AT.isoformat(), NEXT_RECORDED_AT.isoformat()],
            {RECORDED_AT, NEXT_RECORDED_AT},
            id="builtins-set",
        ),
        pytest.param(
            Set[datetime],
            [RECORDED_AT.isoformat(), NEXT_RECORDED_AT.isoformat()],
            {RECORDED_AT, NEXT_RECORDED_AT},
            id="typing-set",
        ),
        pytest.param(
            frozenset[UUID],
            [UUID_STR, SECOND_UUID_STR],
            frozenset({UUID_OBJ, SECOND_UUID_OBJ}),
            id="builtins-frozenset",
        ),
        pytest.param(
            FrozenSet[UUID],
            [UUID_STR, SECOND_UUID_STR],
            frozenset({UUID_OBJ, SECOND_UUID_OBJ}),
            id="typing-frozenset",
        ),
        pytest.param(
            tuple[UUID, datetime],
            [UUID_STR, RECORDED_AT.isoformat()],
            (UUID_OBJ, RECORDED_AT),
            id="builtins-tuple-fixed",
        ),
        pytest.param(
            Tuple[UUID, datetime],
            [UUID_STR, RECORDED_AT.isoformat()],
            (UUID_OBJ, RECORDED_AT),
            id="typing-tuple-fixed",
        ),
        pytest.param(
            tuple[UUID, ...],
            [UUID_STR, SECOND_UUID_STR],
            (UUID_OBJ, SECOND_UUID_OBJ),
            id="builtins-tuple-variadic",
        ),
        pytest.param(
            Tuple[UUID, ...],
            [UUID_STR, SECOND_UUID_STR],
            (UUID_OBJ, SECOND_UUID_OBJ),
            id="typing-tuple-variadic",
        ),
        pytest.param(
            dict[str, UUID],
            {"user_id": UUID_STR},
            {"user_id": UUID_OBJ},
            id="builtins-dict",
        ),
        pytest.param(
            Dict[str, UUID],
            {"user_id": UUID_STR},
            {"user_id": UUID_OBJ},
            id="typing-dict",
        ),
    ],
)
def test_coerce_value_container_annotations(annotation: Any, payload: Any, expected: Any) -> None:
    _assert_coerced_value(annotation, payload, expected)


@pytest.mark.parametrize(
    "annotation_factory",
    [
        pytest.param(_pep604_optional, id="pep604-optional"),
        pytest.param(_typing_optional, id="typing-optional"),
        pytest.param(_typing_union_optional, id="typing-union-optional"),
    ],
)
@pytest.mark.parametrize(
    ("inner_type", "payload", "expected"),
    [
        pytest.param(UUID, UUID_STR, UUID_OBJ, id="uuid"),
        pytest.param(
            TypedDataclass,
            {"reading_id": UUID_STR},
            TypedDataclass(reading_id=UUID_OBJ),
            id="dataclass",
        ),
        pytest.param(TypedModel, {"name": "alpha"}, TypedModel(name="alpha"), id="pydantic"),
        pytest.param(UUID, None, None, id="none"),
    ],
)
def test_coerce_value_optional_annotation_variants(
    annotation_factory: Any,
    inner_type: Any,
    payload: Any,
    expected: Any,
) -> None:
    annotation = annotation_factory(inner_type)

    _assert_coerced_value(annotation, payload, expected)


@pytest.mark.parametrize(
    "annotation_factory",
    [
        pytest.param(_pep604_union, id="pep604-union"),
        pytest.param(_typing_union, id="typing-union"),
    ],
)
@pytest.mark.parametrize(
    ("primary_type", "secondary_type", "payload", "expected"),
    [
        pytest.param(UUID, int, UUID_STR, UUID_OBJ, id="uuid-int"),
        pytest.param(
            TypedDataclass,
            str,
            {"reading_id": UUID_STR},
            TypedDataclass(reading_id=UUID_OBJ),
            id="dataclass-str",
        ),
        pytest.param(
            TypedModel,
            str,
            {"name": "beta"},
            TypedModel(name="beta"),
            id="pydantic-str",
        ),
    ],
)
def test_coerce_value_union_annotation_variants(
    annotation_factory: Any,
    primary_type: Any,
    secondary_type: Any,
    payload: Any,
    expected: Any,
) -> None:
    annotation = annotation_factory(primary_type, secondary_type)

    _assert_coerced_value(annotation, payload, expected)
