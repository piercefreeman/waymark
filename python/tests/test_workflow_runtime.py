from __future__ import annotations

import asyncio
import base64
from typing import List

from carabiner_worker import registry as action_registry
from carabiner_worker.actions import action, serialize_result_payload
from carabiner_worker.workflow_runtime import WorkflowNodeResult, execute_node
from proto import messages_pb2 as pb2

_guard_calls: List[str] = []


@action
async def guarded_noop() -> str:
    _guard_calls.append("ran")
    return "ok"


def _build_dispatch(flag: bool) -> str:
    if action_registry.get("guarded_noop") is None:
        action_registry.register("guarded_noop", guarded_noop)
    node = pb2.WorkflowDagNode(
        id="node_guard",
        action="guarded_noop",
        module=__name__,
        guard="flag",
    )
    node.produces.append("result")
    dispatch = pb2.WorkflowNodeDispatch(node=node, workflow_input=b"")
    payload = serialize_result_payload(flag)
    dispatch.context.append(pb2.WorkflowNodeContext(variable="flag", payload=payload))
    return base64.b64encode(dispatch.SerializeToString()).decode("utf-8")


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
