import asyncio
import os
import shlex
import subprocess
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock, RLock
from typing import AsyncIterator, Optional

import grpc
from grpc import aio  # type: ignore[attr-defined]

from proto import messages_pb2 as pb2
from proto import messages_pb2_grpc as pb2_grpc
from rappel.logger import configure as configure_logger

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


def _boot_command() -> list[str]:
    override = os.environ.get("RAPPEL_BOOT_COMMAND")
    if override:
        LOGGER.debug("Using RAPPEL_BOOT_COMMAND=%s", override)
        return shlex.split(override)
    binary = os.environ.get("RAPPEL_BOOT_BINARY", "boot-rappel-singleton")
    LOGGER.debug("Using RAPPEL_BOOT_BINARY=%s", binary)
    return [binary]


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
