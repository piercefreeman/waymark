"""Runtime helpers for executing workflow DAG nodes inside the worker."""

from __future__ import annotations

import asyncio
import base64
import importlib
import json
from typing import Any, Dict

from pydantic import BaseModel

from proto import messages_pb2 as pb2

from .actions import action, deserialize_result_payload
from .registry import registry


class WorkflowNodeResult(BaseModel):
    variables: Dict[str, Any]


def _decode_workflow_input(payload: bytes) -> dict[str, Any]:
    if not payload:
        return {}
    return json.loads(payload.decode("utf-8"))


def _build_context(dispatch: pb2.WorkflowNodeDispatch) -> dict[str, Any]:
    context: dict[str, Any] = {}
    inputs = _decode_workflow_input(dispatch.workflow_input)
    context.update(inputs)
    for entry in dispatch.context:
        if not entry.variable:
            continue
        decoded = deserialize_result_payload(entry.payload)
        if decoded.error is not None:
            raise RuntimeError(f"dependency {entry.variable} failed")
        result = decoded.result
        if isinstance(result, WorkflowNodeResult):
            if entry.variable in result.variables:
                context[entry.variable] = result.variables[entry.variable]
        else:
            context[entry.variable] = result
    return context


def _ensure_action_module(node: pb2.WorkflowDagNode) -> None:
    module_name = getattr(node, "module", "")
    if module_name:
        importlib.import_module(module_name)


def _evaluate_kwargs(node: pb2.WorkflowDagNode, context: dict[str, Any]) -> Dict[str, Any]:
    namespace = {**context}
    evaluated: Dict[str, Any] = {}
    for key, expr in node.kwargs.items():
        evaluated[key] = eval(expr, {}, namespace)  # noqa: S307 - controlled input
    return evaluated


def _guard_allows_execution(node: pb2.WorkflowDagNode, context: dict[str, Any]) -> bool:
    guard_expr = getattr(node, "guard", "")
    if not guard_expr:
        return True
    namespace = {**context}
    return bool(eval(guard_expr, {}, namespace))  # noqa: S307 - controlled input


def _import_support_blocks(node: pb2.WorkflowDagNode, namespace: dict[str, Any]) -> None:
    imports = node.kwargs.get("imports")
    definitions = node.kwargs.get("definitions")
    if imports:
        if isinstance(imports, str):
            blocks = [imports]
        else:
            blocks = list(imports)
        for block in blocks:
            exec(block, namespace)  # noqa: S102 - controlled by workflow author
    if definitions:
        if isinstance(definitions, str):
            defs = [definitions]
        else:
            defs = list(definitions)
        for definition in defs:
            exec(definition, namespace)  # noqa: S102 - controlled by workflow author


def _execute_python_block(node: pb2.WorkflowDagNode, context: dict[str, Any]) -> dict[str, Any]:
    namespace = dict(context)
    _import_support_blocks(node, namespace)
    code = node.kwargs.get("code")
    if not code:
        return {}
    exec(code, namespace)  # noqa: S102 - controlled by workflow author
    result: dict[str, Any] = {}
    for name in node.produces:
        if name in namespace:
            result[name] = namespace[name]
    return result


@action(name="workflow.execute_node")
async def execute_node(dispatch_b64: str) -> Any:
    payload = base64.b64decode(dispatch_b64)
    dispatch = pb2.WorkflowNodeDispatch()
    dispatch.ParseFromString(payload)
    context = _build_context(dispatch)
    node = dispatch.node
    if not _guard_allows_execution(node, context):
        return WorkflowNodeResult(variables={})
    if node.action == "python_block":
        result_map = _execute_python_block(node, context)
    else:
        _ensure_action_module(node)
        handler = registry.get(node.action)
        if handler is None:
            raise RuntimeError(f"action '{node.action}' not registered")
        kwargs = _evaluate_kwargs(node, context)
        value = handler(**kwargs)
        if asyncio.iscoroutine(value):
            value = await value
        if node.produces:
            result_map = {node.produces[0]: value}
        else:
            result_map = {}
    return WorkflowNodeResult(variables=result_map)


def build_dispatch_payload(
    node: pb2.WorkflowDagNode, context: dict[str, bytes], workflow_input: bytes
) -> bytes:
    dispatch = pb2.WorkflowNodeDispatch(node=node, workflow_input=workflow_input)
    for variable, payload in context.items():
        entry = pb2.WorkflowNodeContext(variable=variable, payload=payload)
        dispatch.context.append(entry)
    return dispatch.SerializeToString()
