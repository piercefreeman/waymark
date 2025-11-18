from __future__ import annotations

import asyncio

from carabiner_worker import worker
from proto import messages_pb2 as pb2


def test_outgoing_stream_includes_handshake() -> None:
    async def scenario() -> None:
        queue: "asyncio.Queue[pb2.Envelope]" = asyncio.Queue()
        stream = worker._outgoing_stream(queue, worker_id=99)
        hello = await anext(stream)
        assert hello.kind == pb2.MessageKind.MESSAGE_KIND_WORKER_HELLO
        hello_msg = pb2.WorkerHello()
        hello_msg.ParseFromString(hello.payload)
        assert hello_msg.worker_id == 99

        payload = pb2.Envelope(
            delivery_id=10, partition_id=2, kind=pb2.MessageKind.MESSAGE_KIND_ACK
        )
        await queue.put(payload)
        forwarded = await anext(stream)
        assert forwarded.delivery_id == payload.delivery_id

    asyncio.run(scenario())


def test_send_ack_helper() -> None:
    async def scenario() -> None:
        queue: "asyncio.Queue[pb2.Envelope]" = asyncio.Queue()
        envelope = pb2.Envelope(delivery_id=7, partition_id=3)
        await worker._send_ack(queue, envelope)
        sent = queue.get_nowait()
        assert sent.kind == pb2.MessageKind.MESSAGE_KIND_ACK
        ack = pb2.Ack()
        ack.ParseFromString(sent.payload)
        assert ack.acked_delivery_id == 7

    asyncio.run(scenario())
