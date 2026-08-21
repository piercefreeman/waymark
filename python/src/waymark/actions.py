import inspect
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, overload

from typing_extensions import assert_never

from waymark.proto import messages_pb2 as pb2
from waymark.proto import python_value_pb2 as pb2v

from .dependencies import provide_dependencies
from .registry import AsyncAction, registry
from .serialization import dumps, dumps_exception, loads

TAsync = TypeVar("TAsync", bound=AsyncAction)


@dataclass
class ActionResultPayload:
    result: Any | None
    error: dict[str, Any] | None


def serialize_returned_value(value: Any) -> bytes:
    """Build the encoded result of an action that returned `value`."""
    result = pb2v.ActionOutcome()
    result.value.CopyFrom(dumps(value))
    return result.SerializeToString()


def serialize_raised_exception(exc: BaseException) -> bytes:
    """Build the encoded result of an action that raised `exc`.

    Raising is not the same as returning the exception: only this arm
    settles the awaiting promise in the raised state.
    """
    result = pb2v.ActionOutcome()
    result.exception.CopyFrom(dumps_exception(exc))
    return result.SerializeToString()


def deserialize_action_result(result: pb2.ActionResult) -> ActionResultPayload:
    """Deserialize the result an [`ActionResult`] carries."""
    result_value = pb2v.ActionOutcome()
    result_value.ParseFromString(result.payload)

    outcome = result_value.WhichOneof("outcome")
    match outcome:
        case "value":
            return ActionResultPayload(result=loads(result_value.value), error=None)
        case "exception":
            return ActionResultPayload(
                result=None,
                error={
                    "type_id": result_value.exception.type_id,
                    "details": loads(result_value.exception.details),
                },
            )
        case None:
            return ActionResultPayload(result=None, error=None)
        case _:
            assert_never(outcome)


@dataclass
class WorkflowOutcomePayload:
    result: Any | None
    error: dict[str, Any] | None


def deserialize_workflow_outcome(payload: bytes) -> WorkflowOutcomePayload:
    """Deserialize an encoded [`WorkflowOutcome`] completion payload.

    Workflow completions discriminate structurally, like the action
    results above: the value the workflow returned, or the exception
    that ended it.
    """
    outcome_value = pb2v.WorkflowOutcome()
    outcome_value.ParseFromString(payload)

    outcome = outcome_value.WhichOneof("outcome")
    match outcome:
        case "value":
            return WorkflowOutcomePayload(result=loads(outcome_value.value), error=None)
        case "exception":
            return WorkflowOutcomePayload(
                result=None,
                error={
                    "type_id": outcome_value.exception.type_id,
                    "details": loads(outcome_value.exception.details),
                },
            )
        case None:
            raise ValueError("workflow outcome names neither arm")
        case _:
            assert_never(outcome)


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

    Actions decorated with @action will automatically resolve dependency markers
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
        wrapper.__waymark_action_name__ = action_name  # type: ignore[attr-defined]
        wrapper.__waymark_action_module__ = action_module  # type: ignore[attr-defined]

        # Register the original function (not the wrapper) so execute_action
        # doesn't double-resolve dependencies
        registry.register(action_module, action_name, target)

        return wrapper  # type: ignore[return-value]

    if func is not None:
        return decorator(func)
    return decorator
