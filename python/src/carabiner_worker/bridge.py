from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
from contextlib import asynccontextmanager
from threading import Lock, RLock
from typing import AsyncIterator, Optional
from urllib.parse import urlparse

import grpc
from grpc import aio  # type: ignore[attr-defined]

from proto import messages_pb2 as pb2
from proto import messages_pb2_grpc as pb2_grpc

DEFAULT_HOST = "127.0.0.1"

_PORT_LOCK = RLock()
_CACHED_PORT: Optional[int] = None
_GRPC_TARGET: Optional[str] = None
_GRPC_CHANNEL: Optional[aio.Channel] = None
_GRPC_STUB: Optional[pb2_grpc.WorkflowServiceStub] = None
_BOOT_MUTEX = Lock()
_ASYNC_BOOT_LOCK: asyncio.Lock = asyncio.Lock()


def _boot_command() -> list[str]:
    override = os.environ.get("CARABINER_BOOT_COMMAND")
    if override:
        return shlex.split(override)
    binary = os.environ.get("CARABINER_BOOT_BINARY", "boot-carabiner-singleton")
    return [binary]


def _remember_port(port: int) -> int:
    global _CACHED_PORT
    with _PORT_LOCK:
        _CACHED_PORT = port
    return port


def _cached_port() -> Optional[int]:
    with _PORT_LOCK:
        return _CACHED_PORT


def _env_port_override() -> Optional[int]:
    override = os.environ.get("CARABINER_SERVER_PORT")
    if not override:
        return None
    try:
        return int(override)
    except ValueError as exc:  # pragma: no cover
        raise RuntimeError(f"invalid CARABINER_SERVER_PORT value: {override}") from exc


def _boot_singleton_blocking() -> int:
    command = _boot_command()
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:  # pragma: no cover
        raise RuntimeError("unable to boot carabiner server") from exc
    output = result.stdout.strip()
    try:
        return int(output)
    except ValueError as exc:  # pragma: no cover
        raise RuntimeError(f"boot command returned invalid port: {output}") from exc


def _resolve_port() -> int:
    cached = _cached_port()
    if cached is not None:
        return cached
    env_port = _env_port_override()
    if env_port is not None:
        return _remember_port(env_port)
    with _BOOT_MUTEX:
        cached = _cached_port()
        if cached is not None:
            return cached
        port = _boot_singleton_blocking()
        return _remember_port(port)


async def _ensure_port_async() -> int:
    cached = _cached_port()
    if cached is not None:
        return cached
    env_port = _env_port_override()
    if env_port is not None:
        return _remember_port(env_port)
    async with _ASYNC_BOOT_LOCK:
        cached = _cached_port()
        if cached is not None:
            return cached
        loop = asyncio.get_running_loop()
        port = await loop.run_in_executor(None, _boot_singleton_blocking)
        return _remember_port(port)


@asynccontextmanager
async def ensure_singleton() -> AsyncIterator[int]:
    """Yield the HTTP port for the singleton server, booting it exactly once."""
    port = await _ensure_port_async()
    yield port


def _grpc_target() -> str:
    explicit = os.environ.get("CARABINER_GRPC_ADDR")
    if explicit:
        return explicit
    http_url = os.environ.get("CARABINER_SERVER_URL")
    host_from_url = None
    port_from_url = None
    if http_url:
        parsed = urlparse(http_url)
        host_from_url = parsed.hostname
        port_from_url = parsed.port
    host = host_from_url or os.environ.get("CARABINER_SERVER_HOST", DEFAULT_HOST)
    port_override = os.environ.get("CARABINER_GRPC_PORT")
    if port_override:
        try:
            port = int(port_override)
        except ValueError as exc:  # pragma: no cover
            raise RuntimeError(f"invalid CARABINER_GRPC_PORT value: {port_override}") from exc
    else:
        http_port = port_from_url if port_from_url is not None else _resolve_port()
        port = http_port + 1
    return f"{host}:{port}"


async def _workflow_stub() -> pb2_grpc.WorkflowServiceStub:
    global _GRPC_TARGET, _GRPC_CHANNEL, _GRPC_STUB
    target = _grpc_target()
    channel_to_wait: Optional[aio.Channel] = None
    with _PORT_LOCK:
        if _GRPC_STUB is not None and _GRPC_TARGET == target:
            return _GRPC_STUB
        channel = aio.insecure_channel(target)
        stub = pb2_grpc.WorkflowServiceStub(channel)
        _GRPC_CHANNEL = channel
        _GRPC_STUB = stub
        _GRPC_TARGET = target
        channel_to_wait = channel
    if channel_to_wait is not None:
        await channel_to_wait.channel_ready()
    return _GRPC_STUB  # type: ignore[return-value]


async def run_instance(database_url: str, payload: bytes) -> int:
    """Register a workflow definition over the gRPC bridge."""
    async with ensure_singleton():
        stub = await _workflow_stub()
    registration = pb2.WorkflowRegistration()
    registration.ParseFromString(payload)
    request = pb2.RegisterWorkflowRequest(
        database_url=database_url,
        registration=registration,
    )
    try:
        response = await stub.RegisterWorkflow(request, timeout=30.0)
    except aio.AioRpcError as exc:  # pragma: no cover
        raise RuntimeError(f"register_workflow failed: {exc}") from exc
    return int(response.workflow_version_id)


async def wait_for_instance(database_url: str, poll_interval_secs: float = 1.0) -> Optional[bytes]:
    """Block until the workflow daemon produces another instance payload."""
    async with ensure_singleton():
        stub = await _workflow_stub()
    request = pb2.WaitForInstanceRequest(
        database_url=database_url,
        poll_interval_secs=poll_interval_secs,
    )
    try:
        response = await stub.WaitForInstance(request, timeout=None)
    except aio.AioRpcError as exc:  # pragma: no cover
        status_fn = getattr(exc, "code", None)
        if callable(status_fn) and status_fn() == grpc.StatusCode.NOT_FOUND:
            return None
        raise RuntimeError(f"wait_for_instance failed: {exc}") from exc
    return bytes(response.payload)
