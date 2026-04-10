from dataclasses import dataclass, field

import pytest

from waymark.type_coercion import _coerce_dict_to_dataclass


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
