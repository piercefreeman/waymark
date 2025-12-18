"""Runtime helpers for executing actions inside the worker.

This module provides the execution layer for Python workers that receive
action dispatch commands from the Rust scheduler.
"""

import asyncio
import dataclasses
from base64 import b64decode
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path, PurePath
from typing import Any, Dict, get_args, get_origin, get_type_hints
from uuid import UUID

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


def _coerce_primitive(value: Any, target_type: type) -> Any:
    """Coerce a value to a primitive type based on target_type.

    Handles conversion of serialized values (strings, floats) back to their
    native Python types (UUID, datetime, etc.).
    """
    # Handle None
    if value is None:
        return None

    # UUID from string
    if target_type is UUID:
        if isinstance(value, UUID):
            return value
        if isinstance(value, str):
            return UUID(value)
        return value

    # datetime from ISO string
    if target_type is datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    # date from ISO string
    if target_type is date:
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        return value

    # time from ISO string
    if target_type is time:
        if isinstance(value, time):
            return value
        if isinstance(value, str):
            return time.fromisoformat(value)
        return value

    # timedelta from total seconds
    if target_type is timedelta:
        if isinstance(value, timedelta):
            return value
        if isinstance(value, (int, float)):
            return timedelta(seconds=value)
        return value

    # Decimal from string
    if target_type is Decimal:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (str, int, float)):
            return Decimal(str(value))
        return value

    # bytes from base64 string
    if target_type is bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return b64decode(value)
        return value

    # Path from string
    if target_type is Path or target_type is PurePath:
        if isinstance(value, PurePath):
            return value
        if isinstance(value, str):
            return Path(value)
        return value

    return value


# Types that can be coerced from serialized form
COERCIBLE_TYPES = (UUID, datetime, date, time, timedelta, Decimal, bytes, Path, PurePath)


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


def _coerce_value(value: Any, target_type: type) -> Any:
    """Coerce a value to the target type.

    Handles:
    - Primitive types (UUID, datetime, etc.)
    - Pydantic models and dataclasses (from dicts)
    - Generic collections like list[UUID], set[datetime]
    """
    # Handle None
    if value is None:
        return None

    # Check for coercible primitive types
    if isinstance(target_type, type) and issubclass(target_type, COERCIBLE_TYPES):
        return _coerce_primitive(value, target_type)

    # Check for Pydantic models or dataclasses
    if isinstance(value, dict):
        coerced = _coerce_dict_to_model(value, target_type)
        if coerced is not value:
            return coerced

    # Handle generic types like list[UUID], set[datetime]
    origin = get_origin(target_type)
    if origin is not None:
        args = get_args(target_type)

        # Handle list[T]
        if origin is list and isinstance(value, list) and args:
            item_type = args[0]
            return [_coerce_value(item, item_type) for item in value]

        # Handle set[T] (serialized as list)
        if origin is set and isinstance(value, list) and args:
            item_type = args[0]
            return {_coerce_value(item, item_type) for item in value}

        # Handle frozenset[T] (serialized as list)
        if origin is frozenset and isinstance(value, list) and args:
            item_type = args[0]
            return frozenset(_coerce_value(item, item_type) for item in value)

        # Handle tuple[T, ...] (serialized as list)
        if origin is tuple and isinstance(value, (list, tuple)) and args:
            # Variable length tuple like tuple[int, ...]
            if len(args) == 2 and args[1] is ...:
                item_type = args[0]
                return tuple(_coerce_value(item, item_type) for item in value)
            # Fixed length tuple like tuple[int, str, UUID]
            return tuple(
                _coerce_value(item, item_type) for item, item_type in zip(value, args, strict=False)
            )

        # Handle dict[K, V]
        if origin is dict and isinstance(value, dict) and len(args) == 2:
            key_type, val_type = args
            return {
                _coerce_value(k, key_type): _coerce_value(v, val_type) for k, v in value.items()
            }

    return value


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
