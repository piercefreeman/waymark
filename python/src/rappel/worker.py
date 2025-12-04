"""gRPC worker client that executes rappel actions."""

import argparse
import asyncio
import importlib
import logging
import sys
import time
from typing import Any, AsyncIterator, cast

import grpc

from proto import messages_pb2 as pb2
from proto import messages_pb2_grpc as pb2_grpc
from rappel.actions import serialize_error_payload, serialize_result_payload

from . import workflow_runtime
from .logger import configure as configure_logger

LOGGER = configure_logger("rappel.worker")
aio = cast(Any, grpc).aio


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rappel workflow worker")
    parser.add_argument("--bridge", required=True, help="gRPC address of the Rust bridge")
    parser.add_argument("--worker-id", required=True, type=int, help="Logical worker identifier")
    parser.add_argument(
        "--user-module",
        action="append",
        default=[],
        help="Optional user module(s) to import eagerly",
    )
    return parser.parse_args(argv)


async def _outgoing_stream(
    queue: "asyncio.Queue[pb2.Envelope]", worker_id: int
) -> AsyncIterator[pb2.Envelope]:
    hello = pb2.WorkerHello(worker_id=worker_id)
    envelope = pb2.Envelope(
        delivery_id=0,
        partition_id=0,
        kind=pb2.MessageKind.MESSAGE_KIND_WORKER_HELLO,
        payload=hello.SerializeToString(),
    )
    yield envelope
    try:
        while True:
            message = await queue.get()
            yield message
    except asyncio.CancelledError:  # pragma: no cover - best effort shutdown
        return


async def _send_ack(outgoing: "asyncio.Queue[pb2.Envelope]", envelope: pb2.Envelope) -> None:
    ack = pb2.Ack(acked_delivery_id=envelope.delivery_id)
    ack_envelope = pb2.Envelope(
        delivery_id=envelope.delivery_id,
        partition_id=envelope.partition_id,
        kind=pb2.MessageKind.MESSAGE_KIND_ACK,
        payload=ack.SerializeToString(),
    )
    await outgoing.put(ack_envelope)


async def _handle_dispatch(
    envelope: pb2.Envelope,
    outgoing: "asyncio.Queue[pb2.Envelope]",
) -> None:
    await _send_ack(outgoing, envelope)
    dispatch = pb2.ActionDispatch()
    dispatch.ParseFromString(envelope.payload)
    timeout_seconds = dispatch.timeout_seconds if dispatch.HasField("timeout_seconds") else 0

    worker_start = time.perf_counter_ns()
    success = True
    action_name = dispatch.action_name
    execution: workflow_runtime.ActionExecutionResult | None = None
    try:
        if timeout_seconds > 0:
            execution = await asyncio.wait_for(
                workflow_runtime.execute_action(dispatch), timeout=timeout_seconds
            )
        else:
            execution = await workflow_runtime.execute_action(dispatch)

        if execution.exception:
            success = False
            response_payload = serialize_error_payload(action_name, execution.exception)
        else:
            response_payload = serialize_result_payload(execution.result)
    except asyncio.TimeoutError:
        success = False
        error = TimeoutError(f"action {action_name} timed out after {timeout_seconds} seconds")
        response_payload = serialize_error_payload(action_name, error)
        LOGGER.warning(
            "Action %s timed out after %ss for action_id=%s sequence=%s",
            action_name,
            timeout_seconds,
            dispatch.action_id,
            dispatch.sequence,
        )
    except Exception as exc:  # noqa: BLE001 - propagate structured errors
        success = False
        response_payload = serialize_error_payload(action_name, exc)
        LOGGER.exception(
            "Action %s failed for action_id=%s sequence=%s",
            action_name,
            dispatch.action_id,
            dispatch.sequence,
        )
    worker_end = time.perf_counter_ns()
    response = pb2.ActionResult(
        action_id=dispatch.action_id,
        success=success,
        worker_start_ns=worker_start,
        worker_end_ns=worker_end,
    )
    response.payload.CopyFrom(response_payload)
    if dispatch.dispatch_token:
        response.dispatch_token = dispatch.dispatch_token
    response_envelope = pb2.Envelope(
        delivery_id=envelope.delivery_id,
        partition_id=envelope.partition_id,
        kind=pb2.MessageKind.MESSAGE_KIND_ACTION_RESULT,
        payload=response.SerializeToString(),
    )
    await outgoing.put(response_envelope)
    LOGGER.debug("Handled action=%s seq=%s success=%s", action_name, dispatch.sequence, success)


async def _handle_incoming_stream(
    stub: pb2_grpc.WorkerBridgeStub,
    worker_id: int,
    outgoing: "asyncio.Queue[pb2.Envelope]",
) -> None:
    """Process incoming messages, running action dispatches concurrently."""
    pending_tasks: set[asyncio.Task[None]] = set()

    async for envelope in stub.Attach(_outgoing_stream(outgoing, worker_id)):
        kind = envelope.kind
        if kind == pb2.MessageKind.MESSAGE_KIND_ACTION_DISPATCH:
            # Spawn task to handle dispatch concurrently
            task = asyncio.create_task(_handle_dispatch(envelope, outgoing))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)
        elif kind == pb2.MessageKind.MESSAGE_KIND_HEARTBEAT:
            LOGGER.debug("Received heartbeat delivery=%s", envelope.delivery_id)
            await _send_ack(outgoing, envelope)
        else:
            LOGGER.warning("Unhandled message kind: %s", kind)
            await _send_ack(outgoing, envelope)

    # Wait for any remaining tasks on stream close
    if pending_tasks:
        await asyncio.gather(*pending_tasks, return_exceptions=True)


async def _run_worker(args: argparse.Namespace) -> None:
    outgoing: "asyncio.Queue[pb2.Envelope]" = asyncio.Queue()
    for module_name in args.user_module:
        if not module_name:
            continue
        LOGGER.info("Preloading user module %s", module_name)
        importlib.import_module(module_name)

    async with aio.insecure_channel(args.bridge) as channel:
        stub = pb2_grpc.WorkerBridgeStub(channel)
        LOGGER.info("Worker %s connected to %s", args.worker_id, args.bridge)
        try:
            await _handle_incoming_stream(stub, args.worker_id, outgoing)
        except aio.AioRpcError as exc:  # pragma: no cover
            status = exc.code()
            LOGGER.error("Worker stream closed: %s", status)
            raise


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="[worker] %(message)s", stream=sys.stderr)
    try:
        asyncio.run(_run_worker(args))
    except KeyboardInterrupt:  # pragma: no cover - exit quietly on Ctrl+C
        return
    except grpc.RpcError:
        sys.exit(1)


if __name__ == "__main__":
    main()
