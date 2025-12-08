"""
Action Worker - Executes individual actions.

This worker connects to the ActionWorkerBridge gRPC service and:
1. Receives ActionDispatch messages
2. Executes the action function
3. Returns ActionResult messages

Action workers are stateless - they just execute functions.
"""

import argparse
import asyncio
import importlib
import sys
import time
from typing import Any

import grpc

from rappel.actions import get_action, get_action_registry
from rappel.serialization import deserialize_kwargs, serialize_value

# Import generated proto
from proto import messages_pb2 as pb
from proto import messages_pb2_grpc as pb_grpc


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Rappel Action Worker")
    parser.add_argument(
        "--bridge",
        required=True,
        help="gRPC address of the ActionWorkerBridge (e.g., localhost:24118)",
    )
    parser.add_argument(
        "--worker-id",
        type=int,
        required=True,
        help="Unique worker ID assigned by the server",
    )
    parser.add_argument(
        "--user-module",
        action="append",
        default=[],
        help="Python modules to import (contain @action definitions)",
    )
    return parser.parse_args(argv)


async def execute_action(func_name: str, kwargs: dict[str, Any]) -> tuple[bool, Any, str | None]:
    """
    Execute an action function and return the result.

    Args:
        func_name: Name of the action to execute
        kwargs: Keyword arguments for the action

    Returns:
        Tuple of (success, result, error_message)
    """
    action_func = get_action(func_name)
    if action_func is None:
        return False, None, f"Unknown action: {func_name}"

    try:
        # Get the original function (not the wrapper)
        original = getattr(action_func, "_action_func", action_func)
        result = await original(**kwargs)
        return True, result, None
    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        return False, None, f"{type(e).__name__}: {e}\n{tb}"


def kwargs_from_proto(proto_kwargs: pb.WorkflowArguments) -> dict[str, Any]:
    """Convert proto WorkflowArguments to Python dict."""
    result = {}
    for arg in proto_kwargs.arguments:
        result[arg.key] = value_from_proto(arg.value)
    return result


def value_from_proto(proto_value: pb.WorkflowArgumentValue) -> Any:
    """Convert proto WorkflowArgumentValue to Python value."""
    kind = proto_value.WhichOneof("kind")
    if kind == "primitive":
        prim = proto_value.primitive
        prim_kind = prim.WhichOneof("kind")
        if prim_kind == "string_value":
            return prim.string_value
        elif prim_kind == "double_value":
            return prim.double_value
        elif prim_kind == "int_value":
            return prim.int_value
        elif prim_kind == "bool_value":
            return prim.bool_value
        elif prim_kind == "null_value":
            return None
    elif kind == "list_value":
        return [value_from_proto(item) for item in proto_value.list_value.items]
    elif kind == "tuple_value":
        return tuple(value_from_proto(item) for item in proto_value.tuple_value.items)
    elif kind == "dict_value":
        return {
            entry.key: value_from_proto(entry.value)
            for entry in proto_value.dict_value.entries
        }
    elif kind == "basemodel":
        # TODO: Reconstruct Pydantic model
        bm = proto_value.basemodel
        data = {
            entry.key: value_from_proto(entry.value)
            for entry in bm.data.entries
        }
        return data
    return None


def value_to_proto(value: Any) -> pb.WorkflowArgumentValue:
    """Convert Python value to proto WorkflowArgumentValue."""
    proto = pb.WorkflowArgumentValue()

    if value is None:
        proto.primitive.null_value = 0
    elif isinstance(value, bool):
        proto.primitive.bool_value = value
    elif isinstance(value, int):
        proto.primitive.int_value = value
    elif isinstance(value, float):
        proto.primitive.double_value = value
    elif isinstance(value, str):
        proto.primitive.string_value = value
    elif isinstance(value, list):
        for item in value:
            proto.list_value.items.append(value_to_proto(item))
    elif isinstance(value, tuple):
        for item in value:
            proto.tuple_value.items.append(value_to_proto(item))
    elif isinstance(value, dict):
        for k, v in value.items():
            entry = proto.dict_value.entries.add()
            entry.key = str(k)
            entry.value.CopyFrom(value_to_proto(v))
    else:
        # Fallback: convert to string
        proto.primitive.string_value = str(value)

    return proto


def result_to_proto(success: bool, result: Any, error: str | None) -> pb.WorkflowArguments:
    """Convert action result to proto WorkflowArguments."""
    proto = pb.WorkflowArguments()

    if success:
        arg = proto.arguments.add()
        arg.key = "result"
        arg.value.CopyFrom(value_to_proto(result))
    else:
        arg = proto.arguments.add()
        arg.key = "error"
        arg.value.primitive.string_value = error or "Unknown error"

    return proto


async def run_worker(bridge_addr: str, worker_id: int) -> None:
    """
    Run the action worker main loop.

    Connects to the gRPC bridge and processes action dispatches.
    """
    print(f"[ActionWorker {worker_id}] Connecting to {bridge_addr}")

    # Create outgoing queue for responses
    outgoing: asyncio.Queue[pb.Envelope] = asyncio.Queue()

    async def outgoing_stream():
        """Generate outgoing messages."""
        # Send WorkerHello first
        hello = pb.WorkerHello(
            worker_id=worker_id,
            worker_type=pb.WORKER_TYPE_ACTION
        )
        yield pb.Envelope(
            delivery_id=0,
            kind=pb.MESSAGE_KIND_WORKER_HELLO,
            payload=hello.SerializeToString(),
        )

        # Then yield responses from the queue
        while True:
            envelope = await outgoing.get()
            yield envelope

    try:
        async with grpc.aio.insecure_channel(bridge_addr) as channel:
            stub = pb_grpc.ActionWorkerBridgeStub(channel)

            async for envelope in stub.Attach(outgoing_stream()):
                if envelope.kind == pb.MESSAGE_KIND_ACTION_DISPATCH:
                    dispatch = pb.ActionDispatch()
                    dispatch.ParseFromString(envelope.payload)

                    # Send ACK immediately
                    ack = pb.Ack(acked_delivery_id=envelope.delivery_id)
                    await outgoing.put(pb.Envelope(
                        delivery_id=envelope.delivery_id,
                        kind=pb.MESSAGE_KIND_ACK,
                        payload=ack.SerializeToString(),
                    ))

                    # Execute action asynchronously
                    asyncio.create_task(_handle_dispatch(
                        dispatch, envelope.delivery_id, outgoing
                    ))

                elif envelope.kind == pb.MESSAGE_KIND_HEARTBEAT:
                    # Send ACK for heartbeat
                    ack = pb.Ack(acked_delivery_id=envelope.delivery_id)
                    await outgoing.put(pb.Envelope(
                        delivery_id=envelope.delivery_id,
                        kind=pb.MESSAGE_KIND_ACK,
                        payload=ack.SerializeToString(),
                    ))

    except Exception as e:
        print(f"[ActionWorker {worker_id}] Error: {e}")
        raise


async def _handle_dispatch(
    dispatch: pb.ActionDispatch,
    delivery_id: int,
    outgoing: asyncio.Queue[pb.Envelope],
) -> None:
    """Handle a single action dispatch."""
    start_ns = time.perf_counter_ns()

    # Parse kwargs
    kwargs = kwargs_from_proto(dispatch.kwargs)

    # Execute action
    success, result, error = await execute_action(dispatch.action_name, kwargs)

    end_ns = time.perf_counter_ns()

    # Build result
    action_result = pb.ActionResult(
        action_id=dispatch.action_id,
        success=success,
        payload=result_to_proto(success, result, error),
        worker_start_ns=start_ns,
        worker_end_ns=end_ns,
        dispatch_token=dispatch.dispatch_token if dispatch.HasField("dispatch_token") else None,
        error_message=error if not success else None,
    )

    # Send result
    await outgoing.put(pb.Envelope(
        delivery_id=delivery_id,
        kind=pb.MESSAGE_KIND_ACTION_RESULT,
        payload=action_result.SerializeToString(),
    ))


def main() -> None:
    """Entry point for rappel-action-worker command."""
    args = _parse_args(sys.argv[1:])

    # Add current directory to sys.path so user modules can be imported
    import os
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    # Import user modules to register actions
    for module_name in args.user_module:
        print(f"[ActionWorker] Importing module: {module_name}")
        importlib.import_module(module_name)

    print(f"[ActionWorker] Registered actions: {list(get_action_registry().keys())}")

    # Run the worker
    asyncio.run(run_worker(args.bridge, args.worker_id))


if __name__ == "__main__":
    main()
