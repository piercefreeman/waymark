"""Runloop for coordinating executors and worker pools."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence
from uuid import UUID, uuid4

from .backends.base import ActionDone, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance
from .dag import DAG, OutputNode, ReturnNode
from .runner import replay_variables
from .runner.executor import DurableUpdates, ExecutorStep, RunnerExecutor
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

    def __init__(
        self,
        worker_pool: BaseWorkerPool,
        backend: BaseBackend,
        *,
        instance_batch_size: int = 25,
        instance_done_batch_size: int | None = None,
        poll_interval: float = 0.05,
        persistence_interval: float = 0.1,
    ) -> None:
        self._worker_pool = worker_pool
        self._backend = backend
        self._instance_batch_size = max(1, instance_batch_size)
        self._instance_done_batch_size = max(
            1,
            instance_done_batch_size if instance_done_batch_size is not None else instance_batch_size,
        )
        self._poll_interval = max(0.0, poll_interval)
        self._persistence_interval = max(0.0, persistence_interval)
        self._executors: dict[UUID, RunnerExecutor] = {}
        self._entry_nodes: dict[UUID, UUID] = {}
        self._inflight: set[tuple[UUID, UUID]] = set()
        self._completion_queue: "asyncio.Queue[list[ActionCompletion]]" = asyncio.Queue()
        self._instance_queue: "asyncio.Queue[list[QueuedInstance]]" = asyncio.Queue()
        self._persistence_queue: "asyncio.Queue[tuple[DurableUpdates, asyncio.Future[None]]]" = (
            asyncio.Queue()
        )
        self._persistence_inflight = 0
        self._action_queue_pending = 0
        self._stop = asyncio.Event()
        self._instances_idle = asyncio.Event()
        self._completed_executors: set[UUID] = set()
        self._instances_done_pending: list[InstanceDone] = []
        self._task_errors: list[BaseException] = []

    def register_executor(self, executor: RunnerExecutor, entry_node: UUID) -> UUID:
        """Register an executor and its entry node, returning an executor id."""
        executor_id = uuid4()
        executor.set_instance_id(executor_id)
        self._executors[executor_id] = executor
        self._entry_nodes[executor_id] = entry_node
        return executor_id

    async def run(self) -> RunLoopResult:
        """Run all registered executors until no actions remain in flight."""
        result = RunLoopResult(
            completed_actions={executor_id: [] for executor_id in self._executors}
        )

        initial_steps: list[tuple[UUID, RunnerExecutor, ExecutorStep]] = []
        for executor_id, executor in self._executors.items():
            entry_node = self._entry_nodes.get(executor_id)
            if entry_node is None:
                raise RunLoopError(f"missing entry node for executor {executor_id}")
            step = executor.increment(entry_node)
            initial_steps.append((executor_id, executor, step))
        self._action_queue_pending += 1
        try:
            await self._persist_steps(initial_steps)
            for executor_id, executor, step in initial_steps:
                self._queue_actions(executor_id, executor, step.actions)
                self._finalize_executor_if_done(executor_id, executor)
        finally:
            self._action_queue_pending -= 1

        poll_instances_task = asyncio.create_task(self._poll_instances())
        process_instances_task = asyncio.create_task(self._process_instances(result))
        poll_task = asyncio.create_task(self._poll_completions())
        process_task = asyncio.create_task(self._process_completions(result))
        persistence_task = asyncio.create_task(self._flush_persistence())
        tasks = [
            poll_instances_task,
            process_instances_task,
            poll_task,
            process_task,
            persistence_task,
        ]
        for task in tasks:
            task.add_done_callback(self._capture_task_error)

        if not self._inflight and self._executors:
            self._stop.set()

        await self._stop.wait()
        await self._flush_persistence_pending()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._flush_persistence_pending()
        self._flush_instances_done()
        if self._task_errors:
            raise self._task_errors[0]
        return result

    def _capture_task_error(self, task: "asyncio.Task[None]") -> None:
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is None:
            return
        self._task_errors.append(exc)
        self._stop.set()

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

            steps: list[tuple[UUID, RunnerExecutor, ExecutorStep]] = []
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
                    steps.append((executor_id, executor, step))
            self._action_queue_pending += 1
            try:
                await self._persist_steps(steps)
                for executor_id, executor, step in steps:
                    if step.actions:
                        self._queue_actions(executor_id, executor, step.actions)
                    self._finalize_executor_if_done(executor_id, executor)
            finally:
                self._action_queue_pending -= 1

            self._maybe_stop()

    async def _poll_instances(self) -> None:
        while not self._stop.is_set():
            instances = await self._backend.get_queued_instances(self._instance_batch_size)
            if instances:
                self._instances_idle.clear()
                await self._instance_queue.put(instances)
            else:
                self._instances_idle.set()
                self._maybe_stop()
            if self._poll_interval > 0:
                await asyncio.sleep(self._poll_interval)
            else:
                await asyncio.sleep(0)

    async def _process_instances(self, result: RunLoopResult) -> None:
        while not self._stop.is_set():
            instances = await self._instance_queue.get()
            steps: list[tuple[UUID, RunnerExecutor, ExecutorStep]] = []
            for instance in instances:
                executor_id = self._register_instance(instance, result)
                executor = self._executors[executor_id]
                step = executor.increment(instance.entry_node)
                steps.append((executor_id, executor, step))
            self._action_queue_pending += 1
            try:
                await self._persist_steps(steps)
                for executor_id, executor, step in steps:
                    if step.actions:
                        self._queue_actions(executor_id, executor, step.actions)
                    self._finalize_executor_if_done(executor_id, executor)
            finally:
                self._action_queue_pending -= 1
            self._maybe_stop()

    def _register_instance(self, instance: QueuedInstance, result: RunLoopResult) -> UUID:
        if instance.state is not None:
            executor = RunnerExecutor(
                instance.dag,
                state=instance.state,
                action_results=instance.action_results,
                backend=self._backend,
            )
        else:
            executor = RunnerExecutor(
                instance.dag,
                nodes=instance.nodes,
                edges=instance.edges,
                action_results=instance.action_results,
                backend=self._backend,
            )
        executor_id = instance.instance_id
        executor.set_instance_id(executor_id)
        self._executors[executor_id] = executor
        self._entry_nodes[executor_id] = instance.entry_node
        result.completed_actions.setdefault(executor_id, [])
        return executor_id

    def _finalize_executor_if_done(self, executor_id: UUID, executor: RunnerExecutor) -> None:
        if executor_id in self._completed_executors:
            return
        if self._has_inflight(executor_id):
            return
        result_payload, error_payload = self._compute_instance_payload(executor)
        self._queue_instance_done(
            InstanceDone(
                executor_id=executor_id,
                entry_node=self._entry_nodes[executor_id],
                result=result_payload,
                error=error_payload,
            )
        )
        self._completed_executors.add(executor_id)

    def _queue_instance_done(self, instance_done: InstanceDone) -> None:
        self._instances_done_pending.append(instance_done)
        if len(self._instances_done_pending) >= self._instance_done_batch_size:
            self._flush_instances_done()

    def _flush_instances_done(self) -> None:
        if not self._instances_done_pending:
            return
        pending = self._instances_done_pending
        self._instances_done_pending = []
        with self._backend.batching() as backend:
            backend.save_instances_done(pending)

    def _has_inflight(self, executor_id: UUID) -> bool:
        return any(exec_id == executor_id for exec_id, _ in self._inflight)

    def _compute_instance_payload(self, executor: RunnerExecutor) -> tuple[Any | None, Any | None]:
        try:
            outputs = self._output_vars(executor.dag)
            replayed = replay_variables(executor.state, executor.action_results)
            if outputs:
                result = {name: replayed.variables.get(name) for name in outputs}
            else:
                result = replayed.variables
            return result, None
        except Exception as exc:  # noqa: BLE001 - surface payload error
            return None, exc

    @staticmethod
    def _output_vars(dag: "DAG") -> list[str]:
        names: list[str] = []
        seen: set[str] = set()
        for node in dag.nodes.values():
            if isinstance(node, OutputNode):
                for name in node.io_vars:
                    if name in seen:
                        continue
                    seen.add(name)
                    names.append(name)
            elif isinstance(node, ReturnNode):
                if node.targets:
                    for name in node.targets:
                        if name in seen:
                            continue
                        seen.add(name)
                        names.append(name)
                elif node.target and node.target not in seen:
                    seen.add(node.target)
                    names.append(node.target)
        return names

    def _maybe_stop(self) -> None:
        if (
            not self._inflight
            and self._instances_idle.is_set()
            and self._instance_queue.empty()
            and not self._has_pending_persistence()
            and self._action_queue_pending == 0
        ):
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

    def _has_pending_persistence(self) -> bool:
        return self._persistence_inflight > 0 or not self._persistence_queue.empty()

    async def _persist_steps(
        self, steps: Sequence[tuple[UUID, RunnerExecutor, ExecutorStep]]
    ) -> None:
        futures: list[asyncio.Future[None]] = []
        for _, _, step in steps:
            if step.updates is None:
                continue
            futures.append(await self._enqueue_persistence(step.updates))
        if futures:
            await asyncio.gather(*futures)

    async def _enqueue_persistence(
        self, updates: DurableUpdates
    ) -> "asyncio.Future[None]":
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[None]" = loop.create_future()
        self._persistence_inflight += 1
        future.add_done_callback(self._on_persistence_done)
        try:
            await self._persistence_queue.put((updates, future))
        except Exception:
            if not future.done():
                future.cancel()
            raise
        return future

    def _on_persistence_done(self, _: "asyncio.Future[None]") -> None:
        self._persistence_inflight = max(0, self._persistence_inflight - 1)

    async def _flush_persistence(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._persistence_interval)
            await self._flush_persistence_pending()
        await self._flush_persistence_pending()

    async def _flush_persistence_pending(self) -> None:
        items: list[tuple[DurableUpdates, asyncio.Future[None]]] = []
        while True:
            try:
                items.append(self._persistence_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if not items:
            return
        actions_done: list[ActionDone] = []
        graph_updates: list[GraphUpdate] = []
        futures: list[asyncio.Future[None]] = []
        for updates, future in items:
            if updates.actions_done:
                actions_done.extend(updates.actions_done)
            if updates.graph_updates:
                graph_updates.extend(updates.graph_updates)
            futures.append(future)
        if not actions_done and not graph_updates:
            for future in futures:
                if not future.done():
                    future.set_result(None)
            return
        try:
            with self._backend.batching() as backend:
                if actions_done:
                    backend.save_actions_done(actions_done)
                if graph_updates:
                    backend.save_graphs(graph_updates)
        except Exception as exc:
            for future in futures:
                if not future.done():
                    future.set_exception(exc)
            raise
        for future in futures:
            if not future.done():
                future.set_result(None)
