from __future__ import annotations

from pydantic import BaseModel

from carabiner_worker.serialization import (
    deserialize_action_call,
    deserialize_result_payload,
    serialize_action_call,
    serialize_error_payload,
    serialize_result_payload,
)


class SampleModel(BaseModel):
    payload: str


def test_action_round_trip_with_basemodel() -> None:
    payload = serialize_action_call("demo.echo", request=SampleModel(payload="hello"))
    invocation = deserialize_action_call(payload)
    assert invocation.action == "demo.echo"
    assert isinstance(invocation.kwargs["request"], SampleModel)
    assert invocation.kwargs["request"].payload == "hello"


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
    assert decoded.error["action"] == "demo.echo"
    assert decoded.error["type"] == "RuntimeError"
    assert "boom" in decoded.error["message"]
