"""Inline worker pool that executes actions in-process."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Mapping

from .base import ActionCompletion, ActionRequest, BaseWorkerPool

ActionCallable = Callable[..., Awaitable[Any]]


class InlineWorkerPoolError(Exception):
    """Raised when inline execution cannot be scheduled."""


class InlineWorkerPool(BaseWorkerPool):
    """Execute action requests by calling async functions in the same loop."""

    def __init__(self, actions: Mapping[str, ActionCallable]) -> None:
        self._actions = dict(actions)
        self._completions: "asyncio.Queue[ActionCompletion]" = asyncio.Queue()
        self._tasks: set[asyncio.Task[None]] = set()

    def queue(self, request: ActionRequest) -> None:
        if request.action_name not in self._actions:
            raise InlineWorkerPoolError(f"unknown action: {request.action_name}")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise InlineWorkerPoolError("inline worker pool requires an active event loop") from exc
        task = loop.create_task(self._run_action(request))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def get_complete(self) -> list[ActionCompletion]:
        first = await self._completions.get()
        completions = [first]
        while True:
            try:
                completions.append(self._completions.get_nowait())
            except asyncio.QueueEmpty:
                break
        return completions

    async def _run_action(self, request: ActionRequest) -> None:
        handler = self._actions[request.action_name]
        try:
            result = await handler(**request.kwargs)
        except Exception as exc:  # noqa: BLE001 - propagate exception values
            result = exc
        await self._completions.put(
            ActionCompletion(
                executor_id=request.executor_id,
                node_id=request.node_id,
                result=result,
            )
        )
