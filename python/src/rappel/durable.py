"""
Durable execution primitives for workflow replay.

This module provides the core data structures and execution logic for
durable workflows. The key insight is:

1. Workflows are executed multiple times
2. Completed actions are replayed from a queue (no re-execution)
3. New actions are captured as pending
4. External action workers execute pending actions
5. Results are added to the queue and workflow re-runs
"""

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ActionStatus(Enum):
    """Status of an action execution."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ActionCall:
    """
    Represents a call to an action with its arguments.

    This captures all information needed to execute the action later.
    """

    id: str
    func_name: str
    args: tuple
    kwargs: dict
    module_name: str | None = None
    timeout_seconds: int | None = None

    def __repr__(self) -> str:
        args_str = ", ".join(
            [repr(a) for a in self.args] + [f"{k}={v!r}" for k, v in self.kwargs.items()]
        )
        return f"ActionCall({self.func_name}({args_str}))"


@dataclass
class ActionResult:
    """
    Result of an action execution.

    Stored in the action queue for replay on subsequent runs.
    """

    action_id: str
    status: ActionStatus
    result: Any = None
    error: str | None = None


@dataclass
class WorkflowInstance:
    """
    State for a single workflow instance.

    - action_queue: completed actions in order they were called
    - pending_actions: actions waiting to be executed
    - replay_index: current position when replaying from action_queue
    """

    id: str
    action_queue: list[ActionResult] = field(default_factory=list)
    pending_actions: list[ActionCall] = field(default_factory=list)
    replay_index: int = 0

    def reset_replay(self) -> None:
        """Reset replay index for a new run."""
        self.replay_index = 0
        self.pending_actions = []


@dataclass
class ExecutionContext:
    """Context for a single workflow execution."""

    instance: WorkflowInstance
    capture_mode: bool = True


# ContextVar to hold the current execution context
_current_context: ContextVar[ExecutionContext | None] = ContextVar(
    "execution_context", default=None
)


async def run_until_actions(
    instance: WorkflowInstance, coro: Any
) -> list[ActionCall]:
    """
    Run a workflow until all branches are blocked on actions.

    Returns list of pending actions that need to be executed.
    Replays completed actions from the instance's action_queue.

    Args:
        instance: The workflow instance state
        coro: The workflow coroutine to execute

    Returns:
        List of pending actions discovered during execution.
        Empty list means workflow completed successfully.
    """
    instance.reset_replay()

    ctx = ExecutionContext(instance=instance, capture_mode=True)
    token = _current_context.set(ctx)

    try:

        async def monitor() -> None:
            """Detect when action count stabilizes."""
            last_count = 0
            stable_iterations = 0
            required_stable = 5

            while True:
                await asyncio.sleep(0)
                current_count = len(instance.pending_actions)

                if current_count > 0 and current_count == last_count:
                    stable_iterations += 1
                    if stable_iterations >= required_stable:
                        return
                else:
                    stable_iterations = 0
                    last_count = current_count

        main_task = asyncio.create_task(coro)
        monitor_task = asyncio.create_task(monitor())

        done, pending = await asyncio.wait(
            [main_task, monitor_task], return_when=asyncio.FIRST_COMPLETED
        )

        # Check if main completed (workflow finished without pending actions)
        workflow_completed = main_task in done and not main_task.cancelled()

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if workflow_completed:
            return []  # No pending actions, workflow is done

        return instance.pending_actions

    finally:
        _current_context.reset(token)
