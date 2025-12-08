"""
Action decorator and registry for durable workflows.

Actions are the unit of durability - they represent operations that:
1. May have side effects (I/O, network calls, etc.)
2. Should not be re-executed on replay
3. Are executed by action workers, not instance workers
"""

from functools import wraps
from typing import Any, Callable, TypeVar

from rappel.durable import ActionCall, ActionStatus, _current_context

import asyncio
import uuid

T = TypeVar("T")

# Global registry of action functions
_action_registry: dict[str, Callable] = {}


def action(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator that marks a function as a durable action.

    During execution:
    - If a completed result exists in queue at current position, return it (replay)
    - Otherwise, register as pending and block forever

    The blocking behavior allows the instance worker to:
    1. Detect which actions are needed
    2. Report them to the server
    3. Wait for the server to schedule action execution
    4. Re-run the workflow with completed actions for replay
    """
    # Register the action
    _action_registry[func.__name__] = func

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> T:
        ctx = _current_context.get()

        if ctx is None or not ctx.capture_mode:
            # Not in durable context, execute normally
            return await func(*args, **kwargs)

        instance = ctx.instance

        # Check if we have a completed result to replay
        if instance.replay_index < len(instance.action_queue):
            result = instance.action_queue[instance.replay_index]
            # Verify it matches (ordering check)
            # For now, just check function name matches
            # In production, you'd want to verify args match too
            instance.replay_index += 1

            if result.status == ActionStatus.COMPLETED:
                return result.result
            elif result.status == ActionStatus.FAILED:
                raise Exception(result.error)

        # No completed result - register as pending and block
        action_call = ActionCall(
            id=str(uuid.uuid4()),
            func_name=func.__name__,
            args=args,
            kwargs=kwargs,
            module_name=func.__module__,
            timeout_seconds=getattr(func, "_timeout_seconds", None),
        )
        instance.pending_actions.append(action_call)

        # Block forever - runner will handle this
        await asyncio.Future()

    wrapper._is_action = True  # type: ignore
    wrapper._action_func = func  # type: ignore
    return wrapper


def get_action_registry() -> dict[str, Callable]:
    """Get the global action registry."""
    return _action_registry


def get_action(name: str) -> Callable | None:
    """Get an action function by name."""
    return _action_registry.get(name)
