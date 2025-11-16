from typing import Protocol

class _ProtoMessage(Protocol):
    def SerializeToString(self) -> bytes: ...
    def ParseFromString(self, data: bytes) -> None: ...

class Ack(_ProtoMessage):
    def __init__(self, acked_delivery_id: int = ...) -> None: ...
    acked_delivery_id: int

class BenchmarkCommand(_ProtoMessage):
    def __init__(
        self,
        monotonic_ns: int = ...,
        sequence: int = ...,
        payload_size: int = ...,
        payload: bytes = ...,
    ) -> None: ...
    monotonic_ns: int
    sequence: int
    payload_size: int
    payload: bytes

class BenchmarkResponse(_ProtoMessage):
    def __init__(
        self,
        correlated_delivery_id: int = ...,
        sequence: int = ...,
        worker_start_ns: int = ...,
        worker_end_ns: int = ...,
    ) -> None: ...
    correlated_delivery_id: int
    sequence: int
    worker_start_ns: int
    worker_end_ns: int

class Envelope(_ProtoMessage):
    def __init__(
        self,
        delivery_id: int = ...,
        partition_id: int = ...,
        kind: int = ...,
        payload: bytes = ...,
    ) -> None: ...
    delivery_id: int
    partition_id: int
    kind: int
    payload: bytes

class MessageKind:
    MESSAGE_KIND_UNSPECIFIED: int
    MESSAGE_KIND_BENCHMARK_COMMAND: int
    MESSAGE_KIND_BENCHMARK_RESPONSE: int
    MESSAGE_KIND_ACK: int
    MESSAGE_KIND_HEARTBEAT: int
