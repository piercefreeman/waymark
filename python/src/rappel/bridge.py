import asyncio
import os
import shlex
import shutil
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock, RLock
from typing import AsyncIterator, NoReturn, Optional

import grpc
from grpc import aio  # type: ignore[attr-defined]

from proto import messages_pb2 as pb2
from proto import messages_pb2_grpc as pb2_grpc
from rappel.logger import configure as configure_logger

from .actions import serialize_error_payload, serialize_result_payload
from .workflow_runtime import execute_action

DEFAULT_HOST = "127.0.0.1"
LOGGER = configure_logger("rappel.bridge")

_PORT_LOCK = RLock()
_CACHED_GRPC_PORT: Optional[int] = None
_GRPC_TARGET: Optional[str] = None
_GRPC_CHANNEL: Optional[aio.Channel] = None
_GRPC_STUB: Optional[pb2_grpc.WorkflowServiceStub] = None
_GRPC_LOOP: Optional[asyncio.AbstractEventLoop] = None
_BOOT_MUTEX = Lock()
_ASYNC_BOOT_LOCK: asyncio.Lock = asyncio.Lock()


@dataclass
class RunInstanceResult:
    workflow_version_id: str
    workflow_instance_id: str


@dataclass
class RunBatchResult:
    workflow_version_id: str
    workflow_instance_ids: list[str]
    queued: int


def _boot_command() -> list[str]:
    override = os.environ.get("RAPPEL_BOOT_COMMAND")
    if override:
        LOGGER.debug("Using RAPPEL_BOOT_COMMAND=%s", override)
        return shlex.split(override)
    binary = os.environ.get("RAPPEL_BOOT_BINARY", "boot-rappel-singleton")
    LOGGER.debug("Using RAPPEL_BOOT_BINARY=%s", binary)
    return [binary]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_boot_binary(binary: str) -> str:
    if Path(binary).is_absolute():
        return binary
    resolved = shutil.which(binary)
    if resolved:
        return resolved
    repo_root = _repo_root()
    for profile in ("debug", "release"):
        candidate = repo_root / "target" / profile / binary
        if candidate.exists():
            return str(candidate)
    return binary


def _ensure_boot_binary(binary: str) -> str:
    resolved = _resolve_boot_binary(binary)
    if Path(resolved).exists():
        return resolved
    repo_root = _repo_root()
    cargo_toml = repo_root / "Cargo.toml"
    if cargo_toml.exists():
        LOGGER.info("boot binary %s not found; building via cargo", binary)
        subprocess.run(
            [
                "cargo",
                "build",
                "--bin",
                "boot-rappel-singleton",
                "--bin",
                "rappel-bridge",
            ],
            cwd=repo_root,
            check=True,
        )
        resolved = _resolve_boot_binary(binary)
    return resolved


def _remember_grpc_port(port: int) -> int:
    global _CACHED_GRPC_PORT
    with _PORT_LOCK:
        _CACHED_GRPC_PORT = port
    return port


def _cached_grpc_port() -> Optional[int]:
    with _PORT_LOCK:
        return _CACHED_GRPC_PORT


def _env_grpc_port_override() -> Optional[int]:
    """Check for explicit gRPC port override via environment."""
    override = os.environ.get("RAPPEL_BRIDGE_GRPC_PORT")
    if not override:
        return None
    try:
        return int(override)
    except ValueError as exc:  # pragma: no cover
        raise RuntimeError(f"invalid RAPPEL_BRIDGE_GRPC_PORT value: {override}") from exc


def _boot_singleton_blocking() -> int:
    """Boot the singleton and return the gRPC port."""
    command = _boot_command()
    if os.environ.get("RAPPEL_BOOT_COMMAND") is None:
        command[0] = _ensure_boot_binary(command[0])
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".txt") as f:
        output_file = Path(f.name)

        command.extend(["--output-file", str(output_file)])
        LOGGER.info("Booting rappel singleton via: %s", " ".join(command))

        try:
            subprocess.run(
                command,
                check=True,
                timeout=10,
            )
        except subprocess.TimeoutExpired as exc:  # pragma: no cover
            LOGGER.error("boot command timed out after %s seconds", exc.timeout)
            raise RuntimeError("unable to boot rappel server") from exc
        except subprocess.CalledProcessError as exc:  # pragma: no cover
            LOGGER.error("boot command failed: %s", exc)
            raise RuntimeError("unable to boot rappel server") from exc
        except OSError as exc:  # pragma: no cover
            LOGGER.error("unable to spawn boot command: %s", exc)
            raise RuntimeError("unable to boot rappel server") from exc

        try:
            # We use a file as a message passer because passing a PIPE to the singleton launcher
            # will block our code indefinitely
            # The singleton launches the webserver subprocess to inherit the stdin/stdout that the
            # singleton launcher receives; which means that in the case of a PIPE it would pass that
            # pipe to the subprocess and therefore never correctly close the file descriptor and signal
            # exit process status to Python.
            port_str = output_file.read_text().strip()
            grpc_port = int(port_str)
            LOGGER.info("boot command reported singleton gRPC port %s", grpc_port)
            return grpc_port
        except (ValueError, FileNotFoundError) as exc:  # pragma: no cover
            raise RuntimeError(f"unable to read port from output file: {exc}") from exc


def _resolve_grpc_port() -> int:
    """Resolve the gRPC port, booting singleton if necessary."""
    cached = _cached_grpc_port()
    if cached is not None:
        return cached
    env_port = _env_grpc_port_override()
    if env_port is not None:
        return _remember_grpc_port(env_port)
    with _BOOT_MUTEX:
        cached = _cached_grpc_port()
        if cached is not None:
            return cached
        port = _boot_singleton_blocking()
        return _remember_grpc_port(port)


async def _ensure_grpc_port_async() -> int:
    """Ensure we have a gRPC port, booting singleton if necessary."""
    cached = _cached_grpc_port()
    if cached is not None:
        return cached
    env_port = _env_grpc_port_override()
    if env_port is not None:
        return _remember_grpc_port(env_port)
    async with _ASYNC_BOOT_LOCK:
        cached = _cached_grpc_port()
        if cached is not None:
            return cached
        loop = asyncio.get_running_loop()
        LOGGER.info("No cached singleton found, booting new instance")
        port = await loop.run_in_executor(None, _boot_singleton_blocking)
        LOGGER.info("Singleton ready on gRPC port %s", port)
        return _remember_grpc_port(port)


@asynccontextmanager
async def ensure_singleton() -> AsyncIterator[int]:
    """Yield the gRPC port for the singleton server, booting it exactly once."""
    port = await _ensure_grpc_port_async()
    yield port


def _grpc_target() -> str:
    """Get the gRPC target address for the bridge server."""
    # Check for explicit full address override
    explicit = os.environ.get("RAPPEL_BRIDGE_GRPC_ADDR")
    if explicit:
        return explicit

    # Otherwise, use host + port
    host = os.environ.get("RAPPEL_BRIDGE_GRPC_HOST", DEFAULT_HOST)
    port = _resolve_grpc_port()
    return f"{host}:{port}"


def assert_never(value: object) -> NoReturn:
    raise AssertionError(f"Unhandled value: {value!r}")


async def _workflow_stub() -> pb2_grpc.WorkflowServiceStub:
    global _GRPC_TARGET, _GRPC_CHANNEL, _GRPC_STUB, _GRPC_LOOP
    target = _grpc_target()
    loop = asyncio.get_running_loop()
    channel_to_wait: Optional[aio.Channel] = None
    with _PORT_LOCK:
        if (
            _GRPC_STUB is not None
            and _GRPC_TARGET == target
            and _GRPC_LOOP is loop
            and not loop.is_closed()
        ):
            return _GRPC_STUB
        channel = aio.insecure_channel(target)
        stub = pb2_grpc.WorkflowServiceStub(channel)
        _GRPC_CHANNEL = channel
        _GRPC_STUB = stub
        _GRPC_TARGET = target
        _GRPC_LOOP = loop
        channel_to_wait = channel
    if channel_to_wait is not None:
        await channel_to_wait.channel_ready()
    return _GRPC_STUB  # type: ignore[return-value]


async def run_instance(payload: bytes) -> RunInstanceResult:
    """Register a workflow definition and start an instance over the gRPC bridge."""
    async with ensure_singleton():
        stub = await _workflow_stub()
    registration = pb2.WorkflowRegistration()
    registration.ParseFromString(payload)
    request = pb2.RegisterWorkflowRequest(
        registration=registration,
    )
    try:
        response = await stub.RegisterWorkflow(request, timeout=30.0)
    except aio.AioRpcError as exc:  # pragma: no cover
        raise RuntimeError(f"register_workflow failed: {exc}") from exc
    return RunInstanceResult(
        workflow_version_id=response.workflow_version_id,
        workflow_instance_id=response.workflow_instance_id,
    )


async def run_instances_batch(
    payload: bytes,
    *,
    count: int = 1,
    inputs: Optional[pb2.WorkflowArguments] = None,
    inputs_list: Optional[list[pb2.WorkflowArguments]] = None,
    batch_size: int = 500,
    include_instance_ids: bool = False,
) -> RunBatchResult:
    """Register a workflow definition and start multiple instances over the gRPC bridge."""
    if count < 1 and not inputs_list:
        raise ValueError("count must be >= 1 when inputs_list is empty")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    async with ensure_singleton():
        stub = await _workflow_stub()
    registration = pb2.WorkflowRegistration()
    registration.ParseFromString(payload)
    request = pb2.RegisterWorkflowBatchRequest(
        registration=registration,
        count=count,
        batch_size=batch_size,
        include_instance_ids=include_instance_ids,
    )
    if inputs is not None:
        request.inputs.CopyFrom(inputs)
    if inputs_list:
        request.inputs_list.extend(inputs_list)
    try:
        response = await stub.RegisterWorkflowBatch(request, timeout=30.0)
    except aio.AioRpcError as exc:  # pragma: no cover
        raise RuntimeError(f"register_workflow_batch failed: {exc}") from exc
    return RunBatchResult(
        workflow_version_id=response.workflow_version_id,
        workflow_instance_ids=list(response.workflow_instance_ids),
        queued=response.queued,
    )


async def execute_workflow(payload: bytes) -> bytes:
    """Execute a workflow via the in-memory workflow streaming API."""
    os.environ.setdefault("RAPPEL_BRIDGE_IN_MEMORY", "1")
    async with ensure_singleton():
        stub = await _workflow_stub()

    registration = pb2.WorkflowRegistration()
    registration.ParseFromString(payload)
    LOGGER.debug(
        "pytest stream start: workflow=%s ir_hash=%s",
        registration.workflow_name,
        registration.ir_hash,
    )

    queue: asyncio.Queue[Optional[pb2.WorkflowStreamRequest]] = asyncio.Queue()
    await queue.put(pb2.WorkflowStreamRequest(registration=registration))

    async def request_stream() -> AsyncIterator[pb2.WorkflowStreamRequest]:
        while True:
            item = await queue.get()
            if item is None:
                return
            yield item

    call = stub.ExecuteWorkflow(request_stream(), timeout=300.0)
    result_payload: Optional[bytes] = None

    async for response in call:
        kind = response.WhichOneof("kind")
        match kind:
            case "action_dispatch":
                dispatch = response.action_dispatch
                LOGGER.debug(
                    "pytest stream dispatch: action_id=%s module=%s action=%s",
                    dispatch.action_id,
                    dispatch.module_name,
                    dispatch.action_name,
                )
                start_ns = time.monotonic_ns()
                execution = await execute_action(dispatch)
                end_ns = time.monotonic_ns()
                action_result = pb2.ActionResult(
                    action_id=dispatch.action_id,
                    success=execution.exception is None,
                    payload=(
                        serialize_result_payload(execution.result)
                        if execution.exception is None
                        else serialize_error_payload(dispatch.action_name, execution.exception)
                    ),
                    worker_start_ns=start_ns,
                    worker_end_ns=end_ns,
                    error_type=(
                        type(execution.exception).__name__
                        if execution.exception is not None
                        else ""
                    ),
                    error_message=str(execution.exception)
                    if execution.exception is not None
                    else "",
                )
                LOGGER.debug(
                    "pytest stream result: action_id=%s success=%s",
                    dispatch.action_id,
                    execution.exception is None,
                )
                await queue.put(pb2.WorkflowStreamRequest(action_result=action_result))
            case "workflow_result":
                result_payload = response.workflow_result.payload
                LOGGER.debug(
                    "pytest stream complete: workflow=%s payload_bytes=%s",
                    registration.workflow_name,
                    len(result_payload),
                )
                await queue.put(None)
                break
            case None:
                continue
            case _:
                assert_never(kind)

    if result_payload is None:
        raise RuntimeError("workflow stream ended without a result")
    return result_payload


async def wait_for_instance(
    instance_id: str,
    poll_interval_secs: float = 1.0,
) -> Optional[bytes]:
    """Block until the workflow daemon produces the requested instance payload."""
    async with ensure_singleton():
        stub = await _workflow_stub()
    request = pb2.WaitForInstanceRequest(
        instance_id=instance_id,
        poll_interval_secs=poll_interval_secs,
    )
    try:
        response = await stub.WaitForInstance(request, timeout=None)
    except aio.AioRpcError as exc:  # pragma: no cover
        status_fn = exc.code
        if callable(status_fn) and status_fn() == grpc.StatusCode.NOT_FOUND:
            return None
        raise RuntimeError(f"wait_for_instance failed: {exc}") from exc
    return bytes(response.payload)
