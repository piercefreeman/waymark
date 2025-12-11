import inspect
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar, overload

from proto import messages_pb2 as pb2

from .registry import AsyncAction, registry
from .serialization import dumps, loads

TAsync = TypeVar("TAsync", bound=AsyncAction)


@dataclass
class ActionResultPayload:
    result: Any | None
    error: dict[str, str] | None


def serialize_result_payload(value: Any) -> pb2.WorkflowArguments:
    """Serialize a successful action result."""
    arguments = pb2.WorkflowArguments()
    entry = arguments.arguments.add()
    entry.key = "result"
    entry.value.CopyFrom(dumps(value))
    return arguments


def serialize_error_payload(_action: str, exc: BaseException) -> pb2.WorkflowArguments:
    """Serialize an error raised during action execution."""
    arguments = pb2.WorkflowArguments()
    entry = arguments.arguments.add()
    entry.key = "error"
    entry.value.CopyFrom(dumps(exc))
    return arguments


def deserialize_result_payload(payload: pb2.WorkflowArguments | None) -> ActionResultPayload:
    """Deserialize WorkflowArguments produced by serialize_result_payload/error."""
    if payload is None:
        return ActionResultPayload(result=None, error=None)
    values = {entry.key: entry.value for entry in payload.arguments}
    if "error" in values:
        error_value = values["error"]
        data = loads(error_value)
        if not isinstance(data, dict):
            raise ValueError("error payload must deserialize to a mapping")
        return ActionResultPayload(result=None, error=data)
    result_value = values.get("result")
    if result_value is None:
        raise ValueError("result payload missing 'result' field")
    return ActionResultPayload(result=loads(result_value), error=None)


@overload
def action(func: TAsync, /) -> TAsync: ...


@overload
def action(*, name: Optional[str] = None) -> Callable[[TAsync], TAsync]: ...


def action(
    func: Optional[TAsync] = None,
    *,
    name: Optional[str] = None,
) -> Callable[[TAsync], TAsync] | TAsync:
    """Decorator for registering async actions."""

    def decorator(target: TAsync) -> TAsync:
        if not inspect.iscoroutinefunction(target):
            raise TypeError(f"action '{target.__name__}' must be defined with 'async def'")
        action_name = name or target.__name__
        action_module = target.__module__
        registry.register(action_module, action_name, target)
        target.__rappel_action_name__ = action_name
        target.__rappel_action_module__ = action_module
        return target

    if func is not None:
        return decorator(func)
    return decorator
