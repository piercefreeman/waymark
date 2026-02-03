"""Runloop for coordinating executors and worker pools."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any, Iterable
from uuid import UUID, uuid4

from .runner.executor import RunnerExecutor
from .runner.state import ExecutionNode
from .workers.base import ActionCompletion, ActionRequest, BaseWorkerPool


class RunLoopError(Exception):
    """Raised when the run loop cannot coordinate execution."""


@dataclass
class RunLoopResult:
    """Aggregated action completions from the run loop."""

    completed_actions: dict[UUID, list[ExecutionNode]] = field(default_factory=dict)


class RunLoop:
    """Coordinate RunnerExecutors with a shared worker pool.

    RunLoop manages multiple executors concurrently. Each executor advances its
    DAG template until it hits action calls, which are queued on a worker pool.
    The run loop then polls for completions, delegates results back to the
    owning executor, and continues until no actions remain in flight.
    """

    def __init__(self, worker_pool: BaseWorkerPool) -> None:
        self._worker_pool = worker_pool
        self._executors: dict[UUID, RunnerExecutor] = {}
        self._entry_nodes: dict[UUID, UUID] = {}
        self._inflight: set[tuple[UUID, UUID]] = set()
        self._completion_queue: "asyncio.Queue[list[ActionCompletion]]" = asyncio.Queue()
        self._stop = asyncio.Event()

    def register_executor(self, executor: RunnerExecutor, entry_node: UUID) -> UUID:
        """Register an executor and its entry node, returning an executor id."""
        executor_id = uuid4()
        self._executors[executor_id] = executor
        self._entry_nodes[executor_id] = entry_node
        return executor_id

    async def run(self) -> RunLoopResult:
        """Run all registered executors until no actions remain in flight."""
        if not self._executors:
            return RunLoopResult()

        result = RunLoopResult(
            completed_actions={executor_id: [] for executor_id in self._executors}
        )

        for executor_id, executor in self._executors.items():
            entry_node = self._entry_nodes.get(executor_id)
            if entry_node is None:
                raise RunLoopError(f"missing entry node for executor {executor_id}")
            step = executor.increment(entry_node)
            self._queue_actions(executor_id, executor, step.actions)

        poll_task = asyncio.create_task(self._poll_completions())
        process_task = asyncio.create_task(self._process_completions(result))

        if not self._inflight:
            self._stop.set()

        await self._stop.wait()
        poll_task.cancel()
        process_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poll_task
        with contextlib.suppress(asyncio.CancelledError):
            await process_task
        return result

    async def _poll_completions(self) -> None:
        while not self._stop.is_set():
            completions = await self._worker_pool.get_complete()
            if completions:
                await self._completion_queue.put(completions)

    async def _process_completions(self, result: RunLoopResult) -> None:
        while not self._stop.is_set():
            completions = await self._completion_queue.get()
            if not completions:
                continue
            grouped: dict[UUID, list[ActionCompletion]] = {}
            for completion in completions:
                grouped.setdefault(completion.executor_id, []).append(completion)

            for executor_id, batch in grouped.items():
                executor = self._executors.get(executor_id)
                if executor is None:
                    raise RunLoopError(f"unknown executor: {executor_id}")

                finished_nodes: list[UUID] = []
                for completion in batch:
                    node = executor.state.nodes.get(completion.node_id)
                    if node is None:
                        raise RunLoopError(f"unknown execution node: {completion.node_id}")

                    executor.set_action_result(completion.node_id, completion.result)
                    result.completed_actions[executor_id].append(node)
                    self._inflight.discard((executor_id, completion.node_id))
                    finished_nodes.append(completion.node_id)

                if finished_nodes:
                    step = executor.increment_batch(finished_nodes)
                    self._queue_actions(executor_id, executor, step.actions)

            if not self._inflight:
                self._stop.set()

    def _queue_actions(
        self,
        executor_id: UUID,
        executor: RunnerExecutor,
        actions: Iterable[ExecutionNode],
    ) -> None:
        for action in actions:
            if action.action is None:
                raise RunLoopError("action node missing action spec")
            key = (executor_id, action.node_id)
            if key in self._inflight:
                continue
            executor.clear_action_result(action.node_id)
            executor.state.mark_running(action.node_id)
            kwargs = executor.resolve_action_kwargs(action.action)
            request = ActionRequest(
                executor_id=executor_id,
                node_id=action.node_id,
                action_name=action.action.action_name,
                kwargs=kwargs,
            )
            self._worker_pool.queue(request)
            self._inflight.add(key)
