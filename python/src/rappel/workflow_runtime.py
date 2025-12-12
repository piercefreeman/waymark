"""Runtime helpers for executing actions inside the worker.

This module provides the execution layer for Python workers that receive
action dispatch commands from the Rust scheduler.
"""

import asyncio
import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, get_type_hints

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


def _is_pydantic_model(cls: type) -> bool:
    """Check if a class is a Pydantic BaseModel subclass."""
    try:
        return isinstance(cls, type) and issubclass(cls, BaseModel)
    except TypeError:
        return False


def _is_dataclass_type(cls: type) -> bool:
    """Check if a class is a dataclass."""
    return dataclasses.is_dataclass(cls) and isinstance(cls, type)


def _coerce_dict_to_model(value: Any, target_type: type) -> Any:
    """Convert a dict to a Pydantic model or dataclass if needed.

    If value is a dict and target_type is a Pydantic model or dataclass,
    instantiate the model with the dict values. Otherwise, return value unchanged.
    """
    if not isinstance(value, dict):
        return value

    if _is_pydantic_model(target_type):
        # Use model_validate for Pydantic v2, fall back to direct instantiation
        model_validate = getattr(target_type, "model_validate", None)
        if model_validate is not None:
            return model_validate(value)
        return target_type(**value)

    if _is_dataclass_type(target_type):
        return target_type(**value)

    return value


def _coerce_kwargs_to_type_hints(handler: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce dict kwargs to Pydantic models or dataclasses based on type hints.

    When the IR converts a Pydantic model or dataclass constructor call to a dict,
    the action runner needs to convert that dict back to the expected type based
    on the handler's type annotations.
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
            coerced[key] = _coerce_dict_to_model(value, target_type)
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
