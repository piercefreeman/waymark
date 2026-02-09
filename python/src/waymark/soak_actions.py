"""Synthetic actions for soak-harness load testing."""

import asyncio
import hashlib

from waymark import action

_MAX_PAYLOAD_BYTES = 256 * 1024


@action
async def simulated_action(
    delay_ms: int,
    should_fail: bool,
    payload_bytes: int,
) -> dict[str, int | str | bool]:
    """Sleep for `delay_ms`, optionally fail, and return a small checksum payload."""
    bounded_delay_ms = max(0, delay_ms)
    bounded_payload_bytes = max(0, min(payload_bytes, _MAX_PAYLOAD_BYTES))

    if bounded_delay_ms > 0:
        await asyncio.sleep(bounded_delay_ms / 1000.0)

    if should_fail:
        raise RuntimeError(
            "simulated soak failure "
            f"(delay_ms={bounded_delay_ms}, payload_bytes={bounded_payload_bytes})"
        )

    payload = "x" * bounded_payload_bytes
    checksum = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

    return {
        "delay_ms": bounded_delay_ms,
        "payload_bytes": bounded_payload_bytes,
        "checksum": checksum,
        "failed": False,
    }
