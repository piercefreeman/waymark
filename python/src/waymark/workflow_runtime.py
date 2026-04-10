"""Runtime helpers for executing actions inside the worker.

This module provides the execution layer for Python workers that receive
action dispatch commands from the Rust scheduler.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, get_type_hints

from pydantic import BaseModel

from waymark.proto import messages_pb2 as pb2

from .dependencies import provide_dependencies
from .registry import registry
from .serialization import arguments_to_kwargs
from .type_coercion import coerce_value as _coerce_value


class WorkflowNodeResult(BaseModel):
    """Result from a workflow node execution containing variable bindings."""

    variables: Dict[str, Any]


@dataclass
class ActionExecutionResult:
    """Result of an action execution."""

    result: Any
    exception: BaseException | None = None


def _coerce_kwargs_to_type_hints(handler: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce kwargs to expected types based on handler's type hints.

    Handles:
    - Pydantic models and dataclasses (from dicts)
    - Primitive types like UUID, datetime, Decimal, etc.
    - Generic collections like list[UUID], dict[str, datetime]
    """
    try:
        type_hints = get_type_hints(handler)
    except Exception:
        # If we can't get type hints (e.g., forward references), return as-is
        return kwargs

    coerced = {}
    for key, value in kwargs.items():
        if key in type_hints:
            target_type = type_hints[key]
            coerced[key] = _coerce_value(value, target_type)
        else:
            coerced[key] = value

    return coerced


async def execute_action(dispatch: pb2.ActionDispatch) -> ActionExecutionResult:
    """Execute an action based on the dispatch command.

    Args:
        dispatch: The action dispatch command from the Rust scheduler.

    Returns:
        The result of executing the action.
    """
    action_name = dispatch.action_name
    module_name = dispatch.module_name

    module = None
    if module_name:
        import importlib

        module = importlib.import_module(module_name)

    # Get the action handler using both module and name
    handler = registry.get(module_name, action_name)
    if handler is None and module is not None:
        import importlib

        module = importlib.reload(module)
        handler = registry.get(module_name, action_name)
    if handler is None:
        return ActionExecutionResult(
            result=None,
            exception=KeyError(f"action '{module_name}:{action_name}' not registered"),
        )

    # Deserialize kwargs
    kwargs = arguments_to_kwargs(dispatch.kwargs)

    # Coerce dict arguments to Pydantic models or dataclasses based on type hints
    # This is needed because the IR converts model constructor calls to dicts
    kwargs = _coerce_kwargs_to_type_hints(handler, kwargs)

    try:
        async with provide_dependencies(handler, kwargs) as call_kwargs:
            value = handler(**call_kwargs)
            if asyncio.iscoroutine(value):
                value = await value
        return ActionExecutionResult(result=value)
    except Exception as e:
        return ActionExecutionResult(
            result=None,
            exception=e,
        )
