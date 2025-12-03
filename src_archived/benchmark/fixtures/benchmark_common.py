"""Shared helpers for benchmark fixtures."""



from pydantic import BaseModel


class PayloadRequest(BaseModel):
    payload: str


class PayloadResponse(BaseModel):
    payload: str
    length: int
    checksum: int


def build_payload_string(payload_size: int) -> str:
    return "x" * payload_size


def build_requests(count: int, payload_size: int) -> list[PayloadRequest]:
    payload = build_payload_string(payload_size)
    return [PayloadRequest(payload=payload) for _ in range(count)]


def summarize_payload(payload: str) -> PayloadResponse:
    encoded = payload.encode("utf-8")
    checksum = sum(encoded)
    return PayloadResponse(payload=payload, length=len(encoded), checksum=checksum)


class InstanceRunStats(BaseModel):
    responses: int
    checksum: int
    total_bytes: int

