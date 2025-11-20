from __future__ import annotations

import asyncio
from typing import List

from proto import messages_pb2 as pb2
from rappel import registry as action_registry
from rappel.actions import action, serialize_error_payload, serialize_result_payload
from rappel.workflow_runtime import WorkflowNodeResult, execute_node

_guard_calls: List[str] = []


@action
async def guarded_noop() -> str:
    _guard_calls.append("ran")
    return "ok"


@action
async def exception_handler() -> str:
    return "handled"


def _build_dispatch(flag: bool) -> pb2.WorkflowNodeDispatch:
    if action_registry.get("guarded_noop") is None:
        action_registry.register("guarded_noop", guarded_noop)
    node = pb2.WorkflowDagNode(
        id="node_guard",
        action="guarded_noop",
        module=__name__,
        guard="flag",
    )
    node.produces.append("result")
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    workflow_input = pb2.WorkflowArguments()
    entry = workflow_input.arguments.add()
    entry.key = "flag"
    entry.value.primitive.bool_value = flag
    dispatch.workflow_input.CopyFrom(workflow_input)
    payload = serialize_result_payload(flag)
    entry = dispatch.context.add()
    entry.variable = "flag"
    entry.payload.CopyFrom(payload)
    return dispatch


def _build_exception_dispatch(include_error: bool) -> pb2.WorkflowNodeDispatch:
    if action_registry.get("exception_handler") is None:
        action_registry.register("exception_handler", exception_handler)
    node = pb2.WorkflowDagNode(
        id="node_handler",
        action="exception_handler",
        module=__name__,
    )
    node.produces.append("value")
    node.guard = "__workflow_exceptions.get('node_source') is not None"
    edge = node.exception_edges.add()
    edge.source_node_id = "node_source"
    edge.exception_type = "RuntimeError"
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    if include_error:
        payload = serialize_error_payload("source", RuntimeError("boom"))
    else:
        payload = serialize_result_payload("noop")
    entry = dispatch.context.add()
    entry.variable = ""
    entry.workflow_node_id = "node_source"
    entry.payload.CopyFrom(payload)
    return dispatch


def test_execute_node_skips_guarded_action() -> None:
    _guard_calls.clear()
    payload = _build_dispatch(flag=False)
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, WorkflowNodeResult)
    assert result.variables == {}
    assert _guard_calls == []


def test_execute_node_runs_guarded_action_when_true() -> None:
    _guard_calls.clear()
    payload = _build_dispatch(flag=True)
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, WorkflowNodeResult)
    assert result.variables == {"result": "ok"}
    assert _guard_calls == ["ran"]


def test_execute_node_handles_exception_when_edge_matches() -> None:
    payload = _build_exception_dispatch(include_error=True)
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, WorkflowNodeResult)
    assert result.variables == {"value": "handled"}


def test_execute_node_skips_exception_handler_without_error() -> None:
    payload = _build_exception_dispatch(include_error=False)
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, WorkflowNodeResult)
    assert result.variables == {}
