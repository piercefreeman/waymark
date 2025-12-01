"""Runtime helpers for executing workflow DAG nodes inside the worker."""

import asyncio
import importlib
from dataclasses import dataclass
from typing import Any, Dict, Tuple

from pydantic import BaseModel

from proto import messages_pb2 as pb2

from .actions import deserialize_result_payload
from .dependencies import provide_dependencies
from .registry import registry
from .serialization import arguments_to_kwargs


class WorkflowNodeResult(BaseModel):
    variables: Dict[str, Any]


@dataclass
class NodeExecutionResult:
    result: Any
    control: pb2.WorkflowNodeControl | None = None


def _decode_workflow_input(payload: pb2.WorkflowArguments | None) -> dict[str, Any]:
    return arguments_to_kwargs(payload)


def _build_context(
    dispatch: pb2.WorkflowNodeDispatch,
) -> Tuple[dict[str, Any], dict[str, Dict[str, Any]]]:
    context: dict[str, Any] = {}
    exceptions: dict[str, Dict[str, Any]] = {}
    inputs = _decode_workflow_input(dispatch.workflow_input)
    context.update(inputs)
    for entry in dispatch.context:
        variable = entry.variable
        decoded = deserialize_result_payload(entry.payload)
        if decoded.error is not None:
            source_id = getattr(entry, "workflow_node_id", "")
            if source_id:
                error_data = dict(decoded.error)
                exceptions[source_id] = error_data
            continue
        result = decoded.result
        if not variable:
            continue
        # Extract the value to assign
        value_to_assign = None
        has_value = False
        if isinstance(result, WorkflowNodeResult):
            if variable in result.variables:
                value_to_assign = result.variables[variable]
                has_value = True
        elif (
            isinstance(result, dict)
            and "data" in result
            and isinstance(result.get("data"), dict)
            and "variables" in result["data"]
        ):
            # Handle dict format of WorkflowNodeResult from Rust scheduler
            # Format: {"data": {"variables": {"var_name": value}}}
            variables = result["data"]["variables"]
            if variable in variables:
                value_to_assign = variables[variable]
                has_value = True
        elif isinstance(result, dict) and variable.startswith("__") and variable in result:
            # Unwrap dict results that contain the variable name as a key
            # This only applies to internal temp variables (like __branch_*) where
            # the scheduler wraps results as {temp_var: value}. For user-defined
            # variables, we want to assign the entire dict even if it happens to
            # contain a key matching the variable name.
            value_to_assign = result[variable]
            has_value = True
        else:
            value_to_assign = result
            has_value = True

        # Don't overwrite existing non-None values with None - this handles convergent
        # branches (try/except, if/else) where skipped nodes produce None values that
        # shouldn't replace real values from the branch that actually executed.
        if value_to_assign is None and variable in context and context[variable] is not None:
            continue

        if has_value:
            context[variable] = value_to_assign
    context["__workflow_exceptions"] = exceptions
    return context, exceptions


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


def _matching_exception_sources(
    node: pb2.WorkflowDagNode, exceptions: dict[str, Dict[str, Any]]
) -> list[str]:
    matches: list[str] = []
    for edge in getattr(node, "exception_edges", []):
        source_id = edge.source_node_id
        if not source_id:
            continue
        error = exceptions.get(source_id)
        if error is None:
            continue
        type_name = edge.exception_type or ""
        module_name = edge.exception_module or ""
        if type_name and error.get("type") != type_name:
            continue
        if module_name and error.get("module") != module_name:
            continue
        matches.append(source_id)
    return matches


def _validate_exception_context(
    node: pb2.WorkflowDagNode,
    exceptions: dict[str, Dict[str, Any]],
    matched_sources: list[str],
) -> None:
    if not exceptions:
        return
    allowed = set(matched_sources)
    unmatched = [source for source in exceptions if source not in allowed]
    if unmatched:
        source = unmatched[0]
        raise RuntimeError(f"dependency {source} failed")


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


async def execute_node(dispatch: pb2.WorkflowNodeDispatch) -> NodeExecutionResult:
    context, exceptions = _build_context(dispatch)
    node = dispatch.node
    if node is None:
        raise RuntimeError("workflow dispatch missing node definition")
    matched_sources = _matching_exception_sources(node, exceptions)
    _validate_exception_context(node, exceptions, matched_sources)
    resolved_kwargs = {}
    if dispatch.HasField("resolved_kwargs"):
        resolved_kwargs = arguments_to_kwargs(dispatch.resolved_kwargs)
    if node.action == "python_block":
        result_map = _execute_python_block(node, context)
        return NodeExecutionResult(result=WorkflowNodeResult(variables=result_map))
    if node.action == "loop":
        raise RuntimeError("loop nodes must be handled in the scheduler")
    _ensure_action_module(node)
    handler = registry.get(node.action)
    if handler is None:
        raise RuntimeError(f"action '{node.action}' not registered")
    kwargs = resolved_kwargs or _evaluate_kwargs(node, context)
    async with provide_dependencies(handler, kwargs) as call_kwargs:
        value = handler(**call_kwargs)
        if asyncio.iscoroutine(value):
            value = await value
    return NodeExecutionResult(result=value)
