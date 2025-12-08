"""
Instance Worker - Runs workflow instances with durable execution.

This worker connects to the InstanceWorkerBridge gRPC service and:
1. Receives InstanceDispatch messages (containing completed actions to replay)
2. Runs the workflow until it blocks on new actions
3. Returns InstanceActions (pending actions) or InstanceComplete

Instance workers are the orchestrators - they run workflows but delegate
action execution to action workers via the server.
"""

import argparse
import asyncio
import importlib
import sys
import uuid
from typing import Any

import grpc

from rappel.durable import ActionResult, ActionStatus, WorkflowInstance, run_until_actions
from rappel.workflow import get_workflow, get_workflow_registry

# Import generated proto
from proto import messages_pb2 as pb
from proto import messages_pb2_grpc as pb_grpc


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Rappel Instance Worker")
    parser.add_argument(
        "--bridge",
        required=True,
        help="gRPC address of the InstanceWorkerBridge (e.g., localhost:24119)",
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
        help="Python modules to import (contain @workflow and @action definitions)",
    )
    return parser.parse_args(argv)


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
        bm = proto_value.basemodel
        data = {
            entry.key: value_from_proto(entry.value)
            for entry in bm.data.entries
        }
        return data
    return None


def kwargs_from_proto(proto_kwargs: pb.WorkflowArguments | None) -> dict[str, Any]:
    """Convert proto WorkflowArguments to Python dict."""
    if proto_kwargs is None:
        return {}
    result = {}
    for arg in proto_kwargs.arguments:
        result[arg.key] = value_from_proto(arg.value)
    return result


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


def kwargs_to_proto(kwargs: dict[str, Any]) -> pb.WorkflowArguments:
    """Convert Python dict to proto WorkflowArguments."""
    proto = pb.WorkflowArguments()
    for k, v in kwargs.items():
        arg = proto.arguments.add()
        arg.key = str(k)
        arg.value.CopyFrom(value_to_proto(v))
    return proto


async def run_instance(
    instance_id: str,
    workflow_name: str,
    module_name: str,
    initial_args: dict[str, Any],
    completed_actions: list[ActionResult],
) -> tuple[list[Any], Any | None, int]:
    """
    Run a workflow instance until it blocks on actions or completes.

    Args:
        instance_id: Unique identifier for this instance
        workflow_name: Name of the workflow class to run
        module_name: Module containing the workflow
        initial_args: Initial arguments for the workflow
        completed_actions: Previously completed actions to replay

    Returns:
        Tuple of (pending_actions, result, replayed_count)
    """
    workflow_cls = get_workflow(workflow_name)
    if workflow_cls is None:
        raise ValueError(f"Unknown workflow: {workflow_name}")

    # Create workflow instance state
    instance = WorkflowInstance(
        id=instance_id,
        action_queue=completed_actions,
    )

    # Instantiate workflow and create the coroutine
    workflow_obj = workflow_cls()

    async def workflow_coro() -> Any:
        return await workflow_obj.run(**initial_args)

    # Run until blocked on actions
    pending = await run_until_actions(instance, workflow_coro())
    replayed_count = instance.replay_index

    if not pending:
        # Workflow completed - re-run to get the result
        instance.reset_replay()
        try:
            result = await asyncio.wait_for(workflow_coro(), timeout=1.0)
            return [], result, replayed_count
        except asyncio.TimeoutError:
            # This shouldn't happen if workflow truly completed
            return [], None, replayed_count
    else:
        return pending, None, replayed_count


async def run_worker(bridge_addr: str, worker_id: int) -> None:
    """
    Run the instance worker main loop.

    Connects to the gRPC bridge and processes instance dispatches.
    """
    print(f"[InstanceWorker {worker_id}] Connecting to {bridge_addr}")

    # Create outgoing queue for responses
    outgoing: asyncio.Queue[pb.Envelope] = asyncio.Queue()

    async def outgoing_stream():
        """Generate outgoing messages."""
        # Send WorkerHello first
        hello = pb.WorkerHello(
            worker_id=worker_id,
            worker_type=pb.WORKER_TYPE_INSTANCE
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
            stub = pb_grpc.InstanceWorkerBridgeStub(channel)

            async for envelope in stub.Attach(outgoing_stream()):
                if envelope.kind == pb.MESSAGE_KIND_INSTANCE_DISPATCH:
                    dispatch = pb.InstanceDispatch()
                    dispatch.ParseFromString(envelope.payload)

                    # Send ACK immediately
                    ack = pb.Ack(acked_delivery_id=envelope.delivery_id)
                    await outgoing.put(pb.Envelope(
                        delivery_id=envelope.delivery_id,
                        kind=pb.MESSAGE_KIND_ACK,
                        payload=ack.SerializeToString(),
                    ))

                    # Handle dispatch asynchronously
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
        print(f"[InstanceWorker {worker_id}] Error: {e}")
        raise


async def _handle_dispatch(
    dispatch: pb.InstanceDispatch,
    delivery_id: int,
    outgoing: asyncio.Queue[pb.Envelope],
) -> None:
    """Handle a single instance dispatch."""
    try:
        # Convert completed actions from proto
        completed = [
            ActionResult(
                action_id=ar.action_id,
                status=ActionStatus.COMPLETED if ar.success else ActionStatus.FAILED,
                result=kwargs_from_proto(ar.payload).get("result") if ar.success else None,
                error=ar.error_message if not ar.success else None,
            )
            for ar in dispatch.completed_actions
        ]

        # Parse initial args
        initial_args = kwargs_from_proto(dispatch.initial_args)

        # Run instance
        pending, result, replayed_count = await run_instance(
            dispatch.instance_id,
            dispatch.workflow_name,
            dispatch.module_name,
            initial_args,
            completed,
        )

        dispatch_token = dispatch.dispatch_token if dispatch.HasField("dispatch_token") else None

        if pending:
            # Report pending actions
            pending_protos = []
            for i, action_call in enumerate(pending):
                pending_proto = pb.PendingAction(
                    action_id=action_call.id,
                    sequence=replayed_count + i,
                    action_name=action_call.func_name,
                    module_name=action_call.module_name or "",
                    kwargs=kwargs_to_proto(action_call.kwargs),
                )
                if action_call.timeout_seconds:
                    pending_proto.timeout_seconds = action_call.timeout_seconds
                pending_protos.append(pending_proto)

            msg = pb.InstanceActions(
                instance_id=dispatch.instance_id,
                replayed_count=replayed_count,
                pending_actions=pending_protos,
                dispatch_token=dispatch_token,
            )
            await outgoing.put(pb.Envelope(
                delivery_id=delivery_id,
                kind=pb.MESSAGE_KIND_INSTANCE_ACTIONS,
                payload=msg.SerializeToString(),
            ))
        else:
            # Workflow completed
            result_proto = pb.WorkflowArguments()
            if result is not None:
                arg = result_proto.arguments.add()
                arg.key = "result"
                arg.value.CopyFrom(value_to_proto(result))

            msg = pb.InstanceComplete(
                instance_id=dispatch.instance_id,
                result=result_proto,
                total_actions=replayed_count,
                dispatch_token=dispatch_token,
            )
            await outgoing.put(pb.Envelope(
                delivery_id=delivery_id,
                kind=pb.MESSAGE_KIND_INSTANCE_COMPLETE,
                payload=msg.SerializeToString(),
            ))

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[InstanceWorker] Error handling dispatch: {e}\n{tb}")

        # Report failure
        dispatch_token = dispatch.dispatch_token if dispatch.HasField("dispatch_token") else None
        msg = pb.InstanceFailed(
            instance_id=dispatch.instance_id,
            error_type=type(e).__name__,
            error_message=str(e),
            traceback=tb,
            actions_completed=0,
            dispatch_token=dispatch_token,
        )
        await outgoing.put(pb.Envelope(
            delivery_id=delivery_id,
            kind=pb.MESSAGE_KIND_INSTANCE_FAILED,
            payload=msg.SerializeToString(),
        ))


def main() -> None:
    """Entry point for rappel-instance-worker command."""
    args = _parse_args(sys.argv[1:])

    # Add current directory to sys.path so user modules can be imported
    import os
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    # Import user modules to register workflows and actions
    for module_name in args.user_module:
        print(f"[InstanceWorker] Importing module: {module_name}")
        importlib.import_module(module_name)

    print(f"[InstanceWorker] Registered workflows: {list(get_workflow_registry().keys())}")

    # Run the worker
    asyncio.run(run_worker(args.bridge, args.worker_id))


if __name__ == "__main__":
    main()
