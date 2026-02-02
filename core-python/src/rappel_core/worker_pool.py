"""Asyncio worker pool and gRPC bridge for Python action execution."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Optional

import grpc

from proto import messages_pb2 as pb2
from proto import messages_pb2_grpc as pb2_grpc

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActionDispatchPayload:
    """Payload for dispatching an action to a worker."""

    action_id: str
    instance_id: str
    sequence: int
    action_name: str
    module_name: str
    kwargs: pb2.WorkflowArguments
    timeout_seconds: int
    max_retries: int
    attempt_number: int
    dispatch_token: uuid.UUID


@dataclass(frozen=True)
class RoundTripMetrics:
    """Metrics for a single action dispatch round trip."""

    action_id: str
    instance_id: str
    delivery_id: int
    sequence: int
    ack_latency: timedelta
    round_trip: timedelta
    worker_duration: timedelta
    response_payload: bytes
    success: bool
    dispatch_token: Optional[uuid.UUID]
    error_type: Optional[str]
    error_message: Optional[str]


@dataclass(frozen=True)
class WorkerThroughputSnapshot:
    """Throughput snapshot for a worker."""

    worker_id: int
    throughput_per_min: float
    total_completed: int
    last_action_at: Optional[datetime]


@dataclass
class WorkerBridgeChannels:
    """Channels for communicating with a connected worker."""

    to_worker: "asyncio.Queue[pb2.Envelope]"
    from_worker: "asyncio.Queue[pb2.Envelope]"


@dataclass
class PythonWorkerConfig:
    """Configuration for spawning Python workers."""

    script_path: Path
    script_args: list[str]
    user_modules: list[str]
    extra_python_paths: list[Path]

    @staticmethod
    def _default_runner() -> tuple[Path, list[str]]:
        found = shutil.which("rappel-worker")
        if found:
            return Path(found), []
        return (
            Path("uv"),
            ["run", "python", "-m", "rappel.worker"],
        )

    @classmethod
    def new(cls) -> "PythonWorkerConfig":
        script_path, script_args = cls._default_runner()
        return cls(
            script_path=script_path,
            script_args=script_args,
            user_modules=[],
            extra_python_paths=[],
        )

    def with_user_module(self, module: str) -> "PythonWorkerConfig":
        return PythonWorkerConfig(
            script_path=self.script_path,
            script_args=list(self.script_args),
            user_modules=[module],
            extra_python_paths=list(self.extra_python_paths),
        )

    def with_user_modules(self, modules: list[str]) -> "PythonWorkerConfig":
        return PythonWorkerConfig(
            script_path=self.script_path,
            script_args=list(self.script_args),
            user_modules=list(modules),
            extra_python_paths=list(self.extra_python_paths),
        )

    def with_python_paths(self, paths: list[Path]) -> "PythonWorkerConfig":
        return PythonWorkerConfig(
            script_path=self.script_path,
            script_args=list(self.script_args),
            user_modules=list(self.user_modules),
            extra_python_paths=list(paths),
        )


class WorkerPoolError(Exception):
    """Base error for worker pool failures."""


class WorkerBridgeError(WorkerPoolError):
    """Error raised when the worker bridge fails."""


class _WorkerBridgeState:
    def __init__(self) -> None:
        self._pending: dict[int, asyncio.Future[WorkerBridgeChannels]] = {}
        self._lock = asyncio.Lock()

    async def reserve_worker(self, worker_id: int) -> asyncio.Future[WorkerBridgeChannels]:
        future: asyncio.Future[WorkerBridgeChannels] = asyncio.get_running_loop().create_future()
        async with self._lock:
            self._pending[worker_id] = future
        return future

    async def cancel_worker(self, worker_id: int) -> None:
        async with self._lock:
            future = self._pending.pop(worker_id, None)
        if future and not future.done():
            future.cancel()

    async def register_worker(self, worker_id: int, channels: WorkerBridgeChannels) -> None:
        async with self._lock:
            future = self._pending.pop(worker_id, None)
        if future is None:
            raise WorkerBridgeError(f"unknown worker id {worker_id}")
        if not future.done():
            future.set_result(channels)


class _WorkerBridgeServicer(pb2_grpc.WorkerBridgeServicer):
    def __init__(self, state: _WorkerBridgeState) -> None:
        self._state = state

    async def Attach(self, request_iterator: Any, context: grpc.aio.ServicerContext) -> Any:  # noqa: N802 - gRPC method name
        try:
            handshake = await anext(request_iterator)
        except StopAsyncIteration:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "missing worker handshake")
            return

        if handshake.kind != pb2.MessageKind.MESSAGE_KIND_WORKER_HELLO:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "expected WorkerHello as first message",
            )
            return

        hello = pb2.WorkerHello()
        hello.ParseFromString(handshake.payload)
        worker_id = hello.worker_id

        to_worker: asyncio.Queue[pb2.Envelope] = asyncio.Queue(maxsize=64)
        from_worker: asyncio.Queue[pb2.Envelope] = asyncio.Queue(maxsize=64)

        await self._state.register_worker(worker_id, WorkerBridgeChannels(to_worker, from_worker))

        async def reader() -> None:
            try:
                async for envelope in request_iterator:
                    await from_worker.put(envelope)
            except Exception:  # pragma: no cover - best effort cleanup
                LOGGER.exception("worker stream receive error", extra={"worker_id": worker_id})
            finally:
                await self._state.cancel_worker(worker_id)

        reader_task = asyncio.create_task(reader())

        try:
            while True:
                envelope = await to_worker.get()
                yield envelope
        except asyncio.CancelledError:
            return
        finally:
            reader_task.cancel()


class WorkerBridgeServer:
    """Async gRPC server for worker connections."""

    def __init__(
        self,
        server: grpc.aio.Server,
        state: _WorkerBridgeState,
        addr: str,
        worker_id_counter: int,
    ) -> None:
        self._server = server
        self._state = state
        self._addr = addr
        self._next_worker_id = worker_id_counter
        self._lock = asyncio.Lock()

    @property
    def addr(self) -> str:
        return self._addr

    @classmethod
    async def start(cls, addr: Optional[str]) -> "WorkerBridgeServer":
        state = _WorkerBridgeState()
        server = grpc.aio.server()
        pb2_grpc.add_WorkerBridgeServicer_to_server(_WorkerBridgeServicer(state), server)
        bind_addr = addr or "127.0.0.1:0"
        port = server.add_insecure_port(bind_addr)
        if port == 0:
            raise WorkerBridgeError(f"failed to bind worker bridge on {bind_addr}")
        await server.start()
        host = bind_addr.split(":")[0]
        addr_value = f"{host}:{port}"
        LOGGER.info("Worker bridge listening on %s", addr_value)
        return cls(server=server, state=state, addr=addr_value, worker_id_counter=1)

    async def reserve_worker(self) -> tuple[int, asyncio.Future[WorkerBridgeChannels]]:
        async with self._lock:
            worker_id = self._next_worker_id
            self._next_worker_id += 1
        future = await self._state.reserve_worker(worker_id)
        return worker_id, future

    async def cancel_worker(self, worker_id: int) -> None:
        await self._state.cancel_worker(worker_id)

    async def shutdown(self) -> None:
        await self._server.stop(grace=1)
        await self._server.wait_for_termination()


@dataclass
class _SharedState:
    pending_acks: dict[int, asyncio.Future[float]] = field(default_factory=dict)
    pending_responses: dict[int, asyncio.Future[tuple[pb2.ActionResult, float]]] = field(
        default_factory=dict
    )
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class PythonWorker:
    """A single Python worker process."""

    def __init__(
        self,
        child: asyncio.subprocess.Process,
        sender: "asyncio.Queue[pb2.Envelope]",
        shared: _SharedState,
        worker_id: int,
        reader_task: asyncio.Task[None],
    ) -> None:
        self._child = child
        self._sender = sender
        self._shared = shared
        self._worker_id = worker_id
        self._reader_task = reader_task
        self._next_delivery = 1
        self._delivery_lock = asyncio.Lock()

    @classmethod
    async def spawn(cls, config: PythonWorkerConfig, bridge: WorkerBridgeServer) -> "PythonWorker":
        worker_id, connection_future = await bridge.reserve_worker()

        python_path = _build_python_path(config.extra_python_paths)
        command = [str(config.script_path), *config.script_args]
        command.extend(["--bridge", bridge.addr, "--worker-id", str(worker_id)])
        for module in config.user_modules:
            command.extend(["--user-module", module])

        env = dict(os.environ)
        if python_path:
            existing = env.get("PYTHONPATH", "")
            if existing:
                env["PYTHONPATH"] = f"{existing}:{python_path}"
            else:
                env["PYTHONPATH"] = python_path

        working_dir = _resolve_worker_cwd()

        try:
            child = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(working_dir) if working_dir else None,
                env=env,
            )
        except Exception as exc:
            await bridge.cancel_worker(worker_id)
            raise WorkerPoolError("failed to launch python worker") from exc

        try:
            channels = await asyncio.wait_for(connection_future, timeout=15)
        except Exception as exc:
            await bridge.cancel_worker(worker_id)
            child.kill()
            await child.wait()
            raise WorkerPoolError("worker failed to connect to bridge") from exc

        shared = _SharedState()
        reader_task = asyncio.create_task(cls._reader_loop(worker_id, channels.from_worker, shared))

        LOGGER.info("Worker %s connected", worker_id)
        return cls(
            child=child,
            sender=channels.to_worker,
            shared=shared,
            worker_id=worker_id,
            reader_task=reader_task,
        )

    async def send_action(self, dispatch: ActionDispatchPayload) -> RoundTripMetrics:
        async with self._delivery_lock:
            delivery_id = self._next_delivery
            self._next_delivery += 1

        send_time = time.perf_counter()
        loop = asyncio.get_running_loop()
        ack_future: asyncio.Future[float] = loop.create_future()
        response_future: asyncio.Future[tuple[pb2.ActionResult, float]] = loop.create_future()

        async with self._shared.lock:
            self._shared.pending_acks[delivery_id] = ack_future
            self._shared.pending_responses[delivery_id] = response_future

        dispatch_message = pb2.ActionDispatch(
            action_id=dispatch.action_id,
            instance_id=dispatch.instance_id,
            sequence=dispatch.sequence,
            action_name=dispatch.action_name,
            module_name=dispatch.module_name,
        )
        dispatch_message.kwargs.CopyFrom(dispatch.kwargs)
        dispatch_message.timeout_seconds = dispatch.timeout_seconds
        dispatch_message.max_retries = dispatch.max_retries
        dispatch_message.attempt_number = dispatch.attempt_number
        dispatch_message.dispatch_token = str(dispatch.dispatch_token)

        envelope = pb2.Envelope(
            delivery_id=delivery_id,
            partition_id=0,
            kind=pb2.MessageKind.MESSAGE_KIND_ACTION_DISPATCH,
            payload=dispatch_message.SerializeToString(),
        )
        await self._sender.put(envelope)

        ack_time = await ack_future
        response, response_time = await response_future

        ack_latency = timedelta(seconds=ack_time - send_time)
        round_trip = timedelta(seconds=response_time - send_time)
        worker_ns = max(0, response.worker_end_ns - response.worker_start_ns)
        worker_duration = timedelta(seconds=worker_ns / 1_000_000_000)

        response_payload = b""
        if response.HasField("payload"):
            response_payload = response.payload.SerializeToString()

        dispatch_token = None
        if response.dispatch_token:
            try:
                dispatch_token = uuid.UUID(response.dispatch_token)
            except ValueError:
                dispatch_token = None

        return RoundTripMetrics(
            action_id=dispatch.action_id,
            instance_id=dispatch.instance_id,
            delivery_id=delivery_id,
            sequence=dispatch.sequence,
            ack_latency=ack_latency,
            round_trip=round_trip,
            worker_duration=worker_duration,
            response_payload=response_payload,
            success=response.success,
            dispatch_token=dispatch_token,
            error_type=response.error_type if response.error_type else None,
            error_message=response.error_message if response.error_message else None,
        )

    @staticmethod
    async def _reader_loop(
        worker_id: int,
        incoming: "asyncio.Queue[pb2.Envelope]",
        shared: _SharedState,
    ) -> None:
        while True:
            envelope = await incoming.get()
            kind = envelope.kind
            if kind == pb2.MessageKind.MESSAGE_KIND_ACK:
                ack = pb2.Ack()
                ack.ParseFromString(envelope.payload)
                async with shared.lock:
                    future = shared.pending_acks.pop(ack.acked_delivery_id, None)
                if future and not future.done():
                    future.set_result(time.perf_counter())
                else:
                    LOGGER.warning("Unexpected ACK for delivery %s", ack.acked_delivery_id)
            elif kind == pb2.MessageKind.MESSAGE_KIND_ACTION_RESULT:
                response = pb2.ActionResult()
                response.ParseFromString(envelope.payload)
                async with shared.lock:
                    future = shared.pending_responses.pop(envelope.delivery_id, None)
                if future and not future.done():
                    future.set_result((response, time.perf_counter()))
                else:
                    LOGGER.warning("Orphan action result for delivery %s", envelope.delivery_id)
            elif kind == pb2.MessageKind.MESSAGE_KIND_HEARTBEAT:
                LOGGER.debug("Worker %s heartbeat", worker_id)
            else:
                LOGGER.warning("Unhandled message kind %s", kind)

    async def shutdown(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
        self._child.kill()
        await self._child.wait()

    def worker_id(self) -> int:
        return self._worker_id


@dataclass
class _ThroughputState:
    worker_id: int
    recent_completions: Deque[float] = field(default_factory=deque)
    total_completed: int = 0
    last_action_at: Optional[datetime] = None

    def prune_before(self, cutoff: float) -> None:
        while self.recent_completions and self.recent_completions[0] < cutoff:
            self.recent_completions.popleft()


class WorkerThroughputTracker:
    def __init__(self, worker_ids: list[int], window: timedelta) -> None:
        self._window = window
        self._workers = [_ThroughputState(worker_id=worker_id) for worker_id in worker_ids]

    def record_completion(self, worker_idx: int) -> None:
        now = time.perf_counter()
        wall_time = datetime.now(tz=timezone.utc)
        if 0 <= worker_idx < len(self._workers):
            worker = self._workers[worker_idx]
            cutoff = max(0.0, now - self._window.total_seconds())
            worker.prune_before(cutoff)
            worker.recent_completions.append(now)
            worker.total_completed += 1
            worker.last_action_at = wall_time

    def snapshot(self) -> list[WorkerThroughputSnapshot]:
        now = time.perf_counter()
        window_secs = self._window.total_seconds()
        cutoff = max(0.0, now - window_secs)
        snapshots: list[WorkerThroughputSnapshot] = []
        for worker in self._workers:
            worker.prune_before(cutoff)
            throughput = 0.0
            if window_secs > 0.0:
                throughput = (len(worker.recent_completions) / window_secs) * 60.0
            snapshots.append(
                WorkerThroughputSnapshot(
                    worker_id=worker.worker_id,
                    throughput_per_min=throughput,
                    total_completed=worker.total_completed,
                    last_action_at=worker.last_action_at,
                )
            )
        return snapshots

    def reset_worker(self, worker_idx: int, worker_id: int) -> None:
        if 0 <= worker_idx < len(self._workers):
            self._workers[worker_idx] = _ThroughputState(worker_id=worker_id)


WorkerFactory = Callable[[PythonWorkerConfig, WorkerBridgeServer], Awaitable[PythonWorker]]


class PythonWorkerPool:
    """Pool of Python workers for action execution."""

    def __init__(
        self,
        workers: list[PythonWorker],
        bridge: WorkerBridgeServer,
        config: PythonWorkerConfig,
        max_action_lifecycle: Optional[int],
        max_concurrent_per_worker: int,
        worker_factory: WorkerFactory,
    ) -> None:
        self._workers = workers
        self._workers_lock = asyncio.Lock()
        self._cursor = 0
        self._cursor_lock = threading.Lock()
        self._throughput = WorkerThroughputTracker(
            [worker.worker_id() for worker in workers], timedelta(seconds=60)
        )
        self._action_counts = [0 for _ in workers]
        self._in_flight_counts = [0 for _ in workers]
        self._counts_lock = threading.Lock()
        self._max_concurrent_per_worker = max(1, max_concurrent_per_worker)
        self._max_action_lifecycle = max_action_lifecycle
        self._bridge = bridge
        self._config = config
        self._worker_factory = worker_factory

    @classmethod
    async def new(
        cls,
        config: PythonWorkerConfig,
        count: int,
        bridge: WorkerBridgeServer,
        max_action_lifecycle: Optional[int],
    ) -> "PythonWorkerPool":
        return await cls.new_with_concurrency(
            config=config,
            count=count,
            bridge=bridge,
            max_action_lifecycle=max_action_lifecycle,
            max_concurrent_per_worker=10,
        )

    @classmethod
    async def new_with_concurrency(
        cls,
        config: PythonWorkerConfig,
        count: int,
        bridge: WorkerBridgeServer,
        max_action_lifecycle: Optional[int],
        max_concurrent_per_worker: int,
        worker_factory: Optional[WorkerFactory] = None,
    ) -> "PythonWorkerPool":
        worker_count = max(1, count)
        factory = worker_factory or PythonWorker.spawn

        tasks = [asyncio.create_task(factory(config, bridge)) for _ in range(worker_count)]
        workers: list[PythonWorker] = []
        for idx, task in enumerate(tasks):
            try:
                worker = await task
            except Exception as exc:
                LOGGER.warning("Failed to spawn worker %s", idx)
                for existing in workers:
                    await existing.shutdown()
                raise WorkerPoolError("failed to spawn worker") from exc
            else:
                workers.append(worker)

        return cls(
            workers=workers,
            bridge=bridge,
            config=config,
            max_action_lifecycle=max_action_lifecycle,
            max_concurrent_per_worker=max_concurrent_per_worker,
            worker_factory=factory,
        )

    async def get_worker(self, idx: int) -> PythonWorker:
        async with self._workers_lock:
            return self._workers[idx % len(self._workers)]

    async def workers_snapshot(self) -> list[PythonWorker]:
        async with self._workers_lock:
            return list(self._workers)

    def next_worker_idx(self) -> int:
        with self._cursor_lock:
            idx = self._cursor
            self._cursor += 1
            return idx

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._workers)

    def is_empty(self) -> bool:
        return len(self._workers) == 0

    def max_concurrent_per_worker(self) -> int:
        return self._max_concurrent_per_worker

    def total_capacity(self) -> int:
        return len(self._workers) * self._max_concurrent_per_worker

    def total_in_flight(self) -> int:
        with self._counts_lock:
            return sum(self._in_flight_counts)

    def available_capacity(self) -> int:
        total = self.total_capacity()
        in_flight = self.total_in_flight()
        return max(0, total - in_flight)

    def try_acquire_slot(self) -> Optional[int]:
        worker_count = len(self._workers)
        if worker_count == 0:
            return None

        start = self.next_worker_idx()
        for offset in range(worker_count):
            idx = (start + offset) % worker_count
            if self.try_acquire_slot_for_worker(idx):
                return idx
        return None

    def try_acquire_slot_for_worker(self, worker_idx: int) -> bool:
        with self._counts_lock:
            idx = worker_idx % len(self._workers)
            current = self._in_flight_counts[idx]
            if current >= self._max_concurrent_per_worker:
                return False
            self._in_flight_counts[idx] = current + 1
            return True

    def release_slot(self, worker_idx: int) -> None:
        with self._counts_lock:
            idx = worker_idx % len(self._workers)
            current = self._in_flight_counts[idx]
            if current <= 0:
                LOGGER.warning("release_slot called with zero in-flight count")
                self._in_flight_counts[idx] = 0
            else:
                self._in_flight_counts[idx] = current - 1

    def in_flight_for_worker(self, worker_idx: int) -> int:
        with self._counts_lock:
            return self._in_flight_counts[worker_idx % len(self._workers)]

    def throughput_snapshots(self) -> list[WorkerThroughputSnapshot]:
        return self._throughput.snapshot()

    def record_completion(self, worker_idx: int) -> None:
        async def recycle_task() -> None:
            try:
                await self._recycle_worker(worker_idx)
            except Exception:
                LOGGER.exception("failed to recycle worker %s", worker_idx)

        self.release_slot(worker_idx)
        self._throughput.record_completion(worker_idx)
        if 0 <= worker_idx < len(self._action_counts):
            with self._counts_lock:
                self._action_counts[worker_idx] += 1
                should_recycle = False
                if self._max_action_lifecycle is not None:
                    should_recycle = self._action_counts[worker_idx] >= self._max_action_lifecycle
            if should_recycle:
                LOGGER.info("recycling worker %s", worker_idx)
                asyncio.create_task(recycle_task())

    async def _recycle_worker(self, worker_idx: int) -> None:
        new_worker = await self._worker_factory(self._config, self._bridge)
        async with self._workers_lock:
            idx = worker_idx % len(self._workers)
            self._workers[idx] = new_worker
        with self._counts_lock:
            self._action_counts[worker_idx % len(self._action_counts)] = 0
        self._throughput.reset_worker(worker_idx, new_worker.worker_id())

    async def shutdown(self) -> None:
        async with self._workers_lock:
            workers = list(self._workers)
        for worker in workers:
            await worker.shutdown()


def _build_python_path(extra_paths: list[Path]) -> str:
    root = _resolve_repo_root()
    module_paths: list[Path] = []
    if root:
        python_root = root / "python"
        if python_root.exists():
            module_paths.append(python_root)
            src_dir = python_root / "src"
            if src_dir.exists():
                module_paths.append(src_dir)
            proto_dir = python_root / "proto"
            if proto_dir.exists():
                module_paths.append(proto_dir)
    module_paths.extend(extra_paths)
    return ":".join(str(path) for path in module_paths)


def _resolve_repo_root() -> Optional[Path]:
    current = Path(__file__).resolve()
    for parent in [current, *current.parents]:
        if (parent / "python" / "pyproject.toml").exists():
            return parent
    return None


def _resolve_worker_cwd() -> Optional[Path]:
    root = _resolve_repo_root()
    if root is None:
        return None
    python_root = root / "python"
    if python_root.exists():
        return python_root
    return None
