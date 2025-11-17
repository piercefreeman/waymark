from __future__ import annotations

import io

import worker
from proto import messages_pb2 as pb2


def test_write_read_round_trip() -> None:
    ack = pb2.Ack(acked_delivery_id=42)
    env = pb2.Envelope(
        delivery_id=1,
        partition_id=0,
        kind=pb2.MessageKind.MESSAGE_KIND_ACK,
        payload=ack.SerializeToString(),
    )
    buffer = io.BytesIO()
    worker._write_frame(buffer, env)
    buffer.seek(0)
    parsed = worker._read_frame(buffer)
    assert parsed is not None
    assert parsed.delivery_id == env.delivery_id
    assert parsed.partition_id == env.partition_id


def test_send_ack_helper() -> None:
    buffer = io.BytesIO()
    worker._send_ack(buffer, delivery_id=7, partition_id=3)
    buffer.seek(0)
    envelope = worker._read_frame(buffer)
    assert envelope is not None
    assert envelope.kind == pb2.MessageKind.MESSAGE_KIND_ACK
    ack = pb2.Ack()
    ack.ParseFromString(envelope.payload)
    assert ack.acked_delivery_id == 7
