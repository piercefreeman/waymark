from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar, overload

from .registry import AsyncAction, registry
from .serialization import _decode_value, _dumps, _encode_value, _loads

TAsync = TypeVar("TAsync", bound=AsyncAction)


@dataclass
class ActionCall:
    module: str
    action: str
    kwargs: dict[str, Any]


@dataclass
class ActionResultPayload:
    result: Any | None
    error: dict[str, str] | None


def serialize_action_call(module: str, action: str, /, **kwargs: Any) -> bytes:
    """Serialize an action name and keyword arguments into bytes."""
    if not isinstance(module, str) or not module:
        raise ValueError("action module must be a non-empty string")
    if not isinstance(action, str) or not action:
        raise ValueError("action name must be a non-empty string")
    encoded_kwargs = {key: _encode_value(value) for key, value in kwargs.items()}
    payload = {"module": module, "action": action, "kwargs": encoded_kwargs}
    return _dumps(payload)


def deserialize_action_call(payload: bytes) -> ActionCall:
    """Deserialize a payload into an action invocation."""
    data = _loads(payload)
    module = data.get("module")
    if not isinstance(module, str) or not module:
        raise ValueError("payload missing module name")
    action = data.get("action")
    if not isinstance(action, str) or not action:
        raise ValueError("payload missing action name")
    kwargs_data = data.get("kwargs", {})
    if not isinstance(kwargs_data, dict):
        raise ValueError("payload kwargs must be an object")
    kwargs = {key: _decode_value(value) for key, value in kwargs_data.items()}
    return ActionCall(module=module, action=action, kwargs=kwargs)


def serialize_result_payload(value: Any) -> bytes:
    """Serialize a successful action result."""
    return _dumps({"result": _encode_value(value)})


def serialize_error_payload(action: str, exc: BaseException) -> bytes:
    """Serialize an error raised during action execution."""
    error_payload = {
        "error": {
            "action": action,
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
    }
    return _dumps(error_payload)


def deserialize_result_payload(payload: bytes) -> ActionResultPayload:
    """Deserialize bytes produced by serialize_result_payload/error."""
    data = _loads(payload)
    if "error" in data:
        error = data["error"]
        if not isinstance(error, dict):
            raise ValueError("error payload must be an object")
        return ActionResultPayload(result=None, error=error)
    if "result" not in data:
        raise ValueError("result payload missing 'result' field")
    return ActionResultPayload(result=_decode_value(data["result"]), error=None)


@overload
def action(func: TAsync, /) -> TAsync: ...


@overload
def action(*, name: Optional[str] = None) -> Callable[[TAsync], TAsync]: ...


def action(
    func: Optional[TAsync] = None, *, name: Optional[str] = None
) -> Callable[[TAsync], TAsync] | TAsync:
    """Decorator for registering async actions."""

    def decorator(target: TAsync) -> TAsync:
        if not inspect.iscoroutinefunction(target):
            raise TypeError(f"action '{target.__name__}' must be defined with 'async def'")
        action_name = name or target.__name__
        registry.register(action_name, target)
        target.__carabiner_action_name__ = action_name
        target.__carabiner_action_module__ = target.__module__
        return target

    if func is not None:
        return decorator(func)
    return decorator


class ActionRunner:
    """Executes registered actions for incoming invocations."""

    def __init__(self) -> None:
        self._loaded_modules: set[str] = set()

    def _ensure_module_loaded(self, module_name: str) -> None:
        if module_name in self._loaded_modules:
            return
        logging.info("Importing user module %s", module_name)
        importlib.import_module(module_name)
        self._loaded_modules.add(module_name)
        names = registry.names()
        summary = ", ".join(names) if names else "<none>"
        logging.info("Registered %s actions: %s", len(names), summary)

    def run_serialized(self, payload: bytes) -> tuple[ActionCall, Any]:
        """Deserialize a payload and execute the referenced action."""
        invocation = deserialize_action_call(payload)
        self._ensure_module_loaded(invocation.module)
        handler = registry.get(invocation.action)
        if handler is None:
            raise RuntimeError(f"action '{invocation.action}' is not registered")
        result = handler(**invocation.kwargs)
        if asyncio.iscoroutine(result):
            return invocation, asyncio.run(result)
        raise RuntimeError(
            f"action '{invocation.action}' did not return a coroutine; "
            "ensure it is defined with 'async def'"
        )
