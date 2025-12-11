"""Runtime helpers for executing actions inside the worker.

This module provides the execution layer for Python workers that receive
action dispatch commands from the Rust scheduler.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Dict

from pydantic import BaseModel

from proto import messages_pb2 as pb2

from .dependencies import provide_dependencies
from .registry import registry
from .serialization import arguments_to_kwargs


class WorkflowNodeResult(BaseModel):
    """Result from a workflow node execution containing variable bindings."""

    variables: Dict[str, Any]


@dataclass
class ActionExecutionResult:
    """Result of an action execution."""

    result: Any
    exception: BaseException | None = None


async def execute_action(dispatch: pb2.ActionDispatch) -> ActionExecutionResult:
    """Execute an action based on the dispatch command.

    Args:
        dispatch: The action dispatch command from the Rust scheduler.

    Returns:
        The result of executing the action.
    """
    action_name = dispatch.action_name
    module_name = dispatch.module_name

    # Import the module if specified (this registers actions via @action decorator)
    if module_name:
        import importlib

        importlib.import_module(module_name)

    # Get the action handler using both module and name
    handler = registry.get(module_name, action_name)
    if handler is None:
        return ActionExecutionResult(
            result=None,
            exception=KeyError(f"action '{module_name}:{action_name}' not registered"),
        )

    # Deserialize kwargs
    kwargs = arguments_to_kwargs(dispatch.kwargs)

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
