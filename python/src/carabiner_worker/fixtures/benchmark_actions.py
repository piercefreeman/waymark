from __future__ import annotations

from pydantic import BaseModel

from carabiner_worker import action


class PayloadRequest(BaseModel):
    payload: str


class PayloadResponse(BaseModel):
    payload: str
    length: int
    checksum: int


@action(name="benchmark.echo_payload")
async def echo_payload(request: PayloadRequest) -> PayloadResponse:
    """Echo the provided payload and return checksum metadata."""
    data = request.payload
    checksum = sum(data.encode("utf-8"))
    return PayloadResponse(payload=data, length=len(data), checksum=checksum)
