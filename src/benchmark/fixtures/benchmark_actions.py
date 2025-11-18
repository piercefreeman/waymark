from __future__ import annotations

from carabiner_worker import action

from .benchmark_common import PayloadRequest, PayloadResponse, summarize_payload


@action(name="benchmark.echo_payload")
async def echo_payload(request: PayloadRequest) -> PayloadResponse:
    """Echo the provided payload and return checksum metadata."""
    return summarize_payload(request.payload)

