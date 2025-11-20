from __future__ import annotations

from pydantic import BaseModel

from rappel.actions import (
    deserialize_result_payload,
    serialize_error_payload,
    serialize_result_payload,
)


class SampleModel(BaseModel):
    payload: str


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
