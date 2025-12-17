import inspect
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, overload

from proto import messages_pb2 as pb2

from .dependencies import provide_dependencies
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
    """Decorator for registering async actions.

    Actions decorated with @action will automatically resolve Depend() markers
    when called directly (e.g., during pytest runs where workflows bypass the
    gRPC bridge).
    """

    def decorator(target: TAsync) -> TAsync:
        if not inspect.iscoroutinefunction(target):
            raise TypeError(f"action '{target.__name__}' must be defined with 'async def'")
        action_name = name or target.__name__
        action_module = target.__module__

        @wraps(target)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Convert positional args to kwargs based on the signature
            sig = inspect.signature(target)
            params = list(sig.parameters.keys())
            for i, arg in enumerate(args):
                if i < len(params):
                    kwargs[params[i]] = arg

            # Resolve dependencies using the same mechanism as execute_action
            async with provide_dependencies(target, kwargs) as call_kwargs:
                return await target(**call_kwargs)

        # Copy over the original function's attributes for introspection
        wrapper.__wrapped__ = target  # type: ignore[attr-defined]
        wrapper.__rappel_action_name__ = action_name  # type: ignore[attr-defined]
        wrapper.__rappel_action_module__ = action_module  # type: ignore[attr-defined]

        # Register the original function (not the wrapper) so execute_action
        # doesn't double-resolve dependencies
        registry.register(action_module, action_name, target)

        return wrapper  # type: ignore[return-value]

    if func is not None:
        return decorator(func)
    return decorator
