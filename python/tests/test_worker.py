import argparse
import asyncio

from waymark import worker
from waymark.grpc_config import GRPC_CHANNEL_OPTIONS
from waymark.proto import messages_pb2 as pb2


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


def test_run_worker_configures_grpc_message_limit(monkeypatch) -> None:
    created_channels: list[tuple[str, object]] = []

    class FakeChannel:
        async def __aenter__(self) -> "FakeChannel":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    def fake_insecure_channel(target: str, *, options: object) -> FakeChannel:
        created_channels.append((target, options))
        return FakeChannel()

    class FakeStub:
        def __init__(self, channel: FakeChannel) -> None:
            self.channel = channel

    async def fake_handle_incoming_stream(
        _stub: FakeStub,
        _worker_id: int,
        _outgoing: "asyncio.Queue[pb2.Envelope]",
    ) -> None:
        return None

    monkeypatch.setattr(worker.aio, "insecure_channel", fake_insecure_channel)
    monkeypatch.setattr(worker.pb2_grpc, "WorkerBridgeStub", FakeStub)
    monkeypatch.setattr(worker, "_handle_incoming_stream", fake_handle_incoming_stream)

    args = argparse.Namespace(bridge="127.0.0.1:24118", worker_id=7, user_module=[])
    asyncio.run(worker._run_worker(args))

    assert created_channels == [("127.0.0.1:24118", GRPC_CHANNEL_OPTIONS)]
