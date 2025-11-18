from __future__ import annotations

import base64
import os
import shlex
import subprocess
from threading import RLock
from typing import Optional

import httpx

DEFAULT_HOST = "127.0.0.1"

_PORT_LOCK = RLock()
_CACHED_PORT: Optional[int] = None


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


def _resolve_port() -> int:
    cached = _cached_port()
    if cached is not None:
        return cached
    override = os.environ.get("CARABINER_SERVER_PORT")
    if override:
        try:
            return _remember_port(int(override))
        except ValueError as exc:  # pragma: no cover
            raise RuntimeError(f"invalid CARABINER_SERVER_PORT value: {override}") from exc
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
        port = int(output)
    except ValueError as exc:  # pragma: no cover
        raise RuntimeError(f"boot command returned invalid port: {output}") from exc
    return _remember_port(port)


def _base_url() -> str:
    explicit = os.environ.get("CARABINER_SERVER_URL")
    if explicit:
        return explicit.rstrip("/")
    host = os.environ.get("CARABINER_SERVER_HOST", DEFAULT_HOST)
    port = _resolve_port()
    return f"http://{host}:{port}"


def _post_json(path: str, payload: dict, *, timeout: Optional[float]) -> dict:
    url = f"{_base_url()}{path}"
    try:
        response = httpx.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
    except httpx.HTTPError as exc:  # pragma: no cover
        raise RuntimeError(f"carabiner server request failed: {exc}") from exc
    return response.json()


def run_instance(database_url: str, payload: bytes) -> int:
    """Register a workflow definition over the local HTTP bridge."""
    body = {
        "database_url": database_url,
        "registration_b64": base64.b64encode(payload).decode("ascii"),
    }
    data = _post_json("/v1/workflows/register", body, timeout=30.0)
    return int(data["workflow_version_id"])


def wait_for_instance(database_url: str, poll_interval_secs: float = 1.0) -> Optional[bytes]:
    """Block until the workflow daemon produces another instance payload."""
    body = {
        "database_url": database_url,
        "poll_interval_secs": poll_interval_secs,
    }
    data = _post_json("/v1/workflows/wait", body, timeout=None)
    payload_b64 = data.get("payload_b64")
    if not payload_b64:
        return None
    return base64.b64decode(payload_b64)
