import argparse
import asyncio
import os
import sys

import pytest

from waymark import worker, workflow_runtime
from waymark.actions import deserialize_action_result
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


def test_handle_dispatch_echoes_metadata(monkeypatch) -> None:
    metadata = b"\x2a\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00"

    async def fake_execute_action(_dispatch: pb2.ActionDispatch) -> object:
        return workflow_runtime.ActionExecutionResult(result="ok")

    monkeypatch.setattr(workflow_runtime, "execute_action", fake_execute_action)

    async def scenario() -> None:
        outgoing: "asyncio.Queue[pb2.Envelope]" = asyncio.Queue()
        dispatch = pb2.ActionDispatch(
            action_name="noop",
            metadata=metadata,
        )
        envelope = pb2.Envelope(
            delivery_id=5,
            partition_id=1,
            kind=pb2.MessageKind.MESSAGE_KIND_ACTION_DISPATCH,
            payload=dispatch.SerializeToString(),
        )
        await worker._handle_dispatch(envelope, outgoing)

        # First message is the ack, second is the action result.
        ack = outgoing.get_nowait()
        assert ack.kind == pb2.MessageKind.MESSAGE_KIND_ACK
        result_envelope = outgoing.get_nowait()
        assert result_envelope.kind == pb2.MessageKind.MESSAGE_KIND_ACTION_RESULT
        result = pb2.ActionResult()
        result.ParseFromString(result_envelope.payload)
        assert deserialize_action_result(result).result == "ok"
        assert result.metadata == metadata

    asyncio.run(scenario())


def test_handle_dispatch_without_metadata_leaves_it_empty(monkeypatch) -> None:
    async def fake_execute_action(_dispatch: pb2.ActionDispatch) -> object:
        return workflow_runtime.ActionExecutionResult(result="ok")

    monkeypatch.setattr(workflow_runtime, "execute_action", fake_execute_action)

    async def scenario() -> None:
        outgoing: "asyncio.Queue[pb2.Envelope]" = asyncio.Queue()
        dispatch = pb2.ActionDispatch(action_name="noop")
        envelope = pb2.Envelope(
            delivery_id=6,
            partition_id=1,
            kind=pb2.MessageKind.MESSAGE_KIND_ACTION_DISPATCH,
            payload=dispatch.SerializeToString(),
        )
        await worker._handle_dispatch(envelope, outgoing)

        outgoing.get_nowait()  # ack
        result_envelope = outgoing.get_nowait()
        result = pb2.ActionResult()
        result.ParseFromString(result_envelope.payload)
        assert result.metadata == b""

    asyncio.run(scenario())


def _run_main_with_a_stubbed_worker(monkeypatch) -> list[argparse.Namespace]:
    ran: list[argparse.Namespace] = []

    async def fake_run_worker(args: argparse.Namespace) -> None:
        ran.append(args)

    monkeypatch.setattr(worker, "_run_worker", fake_run_worker)
    worker.main(["--bridge", "127.0.0.1:24118", "--worker-id", "1"])
    return ran


def test_main_puts_the_working_directory_first(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(worker.sys, "path", ["/somewhere/bin", "/somewhere/site-packages"])

    ran = _run_main_with_a_stubbed_worker(monkeypatch)

    assert ran
    assert worker.sys.path == [os.getcwd(), "/somewhere/bin", "/somewhere/site-packages"]


def test_main_keeps_the_working_directory_first_when_already_there(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(worker.sys, "path", [os.getcwd(), "/somewhere/site-packages"])

    _run_main_with_a_stubbed_worker(monkeypatch)

    assert worker.sys.path == [os.getcwd(), "/somewhere/site-packages"]


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows cannot remove a process's working directory"
)
def test_main_leaves_sys_path_alone_when_the_working_directory_is_gone(
    monkeypatch, tmp_path
) -> None:
    gone = tmp_path / "gone"
    gone.mkdir()
    monkeypatch.chdir(gone)
    gone.rmdir()
    monkeypatch.setattr(worker.sys, "path", ["/somewhere/bin", "/somewhere/site-packages"])

    ran = _run_main_with_a_stubbed_worker(monkeypatch)

    assert ran
    assert worker.sys.path == ["/somewhere/bin", "/somewhere/site-packages"]


def test_main_keeps_the_empty_entry_python_dash_m_leaves_first(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(worker.sys, "path", ["", "/somewhere/site-packages"])

    _run_main_with_a_stubbed_worker(monkeypatch)

    assert worker.sys.path == ["", "/somewhere/site-packages"]


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
