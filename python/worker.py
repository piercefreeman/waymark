"""Minimal worker loop that speaks the benchmarking protocol."""

from __future__ import annotations

import logging
import struct
import sys
import time
from typing import BinaryIO

from proto.messages_pb2 import (
    Ack,
    BenchmarkCommand,
    BenchmarkResponse,
    Envelope,
    MessageKind,
)

logging.basicConfig(
    level=logging.INFO, format="[worker] %(message)s", stream=sys.stderr
)

_HEADER = struct.Struct("<I")


def _read_frame(stream: BinaryIO) -> Envelope | None:
    header = stream.read(_HEADER.size)
    if not header:
        return None
    (length,) = _HEADER.unpack(header)
    payload = stream.read(length)
    message = Envelope()
    message.ParseFromString(payload)
    return message


def _write_frame(stream: BinaryIO, envelope: Envelope) -> None:
    payload = envelope.SerializeToString()
    stream.write(_HEADER.pack(len(payload)))
    stream.write(payload)
    stream.flush()


def _send_ack(stream: BinaryIO, delivery_id: int, partition_id: int) -> None:
    ack = Ack(acked_delivery_id=delivery_id)
    envelope = Envelope(
        delivery_id=delivery_id,
        partition_id=partition_id,
        kind=MessageKind.MESSAGE_KIND_ACK,
        payload=ack.SerializeToString(),
    )
    _write_frame(stream, envelope)


def main() -> None:
    logging.info("Python worker booted")
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer

    while True:
        envelope = _read_frame(stdin)
        if envelope is None:
            logging.info("stdin closed - exiting")
            break

        kind = envelope.kind
        partition = envelope.partition_id

        if kind == MessageKind.MESSAGE_KIND_BENCHMARK_COMMAND:
            _send_ack(stdout, envelope.delivery_id, partition)

            command = BenchmarkCommand()
            command.ParseFromString(envelope.payload)

            worker_start = time.perf_counter_ns()
            payload = command.payload
            # Touch the bytes so python work roughly scales with payload size.
            checksum = sum(payload)
            worker_end = time.perf_counter_ns()

            response = BenchmarkResponse(
                correlated_delivery_id=envelope.delivery_id,
                sequence=command.sequence,
                worker_start_ns=worker_start,
                worker_end_ns=worker_end,
            )
            response_envelope = Envelope(
                delivery_id=envelope.delivery_id,
                partition_id=partition,
                kind=MessageKind.MESSAGE_KIND_BENCHMARK_RESPONSE,
                payload=response.SerializeToString(),
            )
            _write_frame(stdout, response_envelope)
            logging.debug("Handled seq=%s checksum=%s", command.sequence, checksum)
        elif kind == MessageKind.MESSAGE_KIND_HEARTBEAT:
            logging.debug("Received heartbeat delivery=%s", envelope.delivery_id)
            _send_ack(stdout, envelope.delivery_id, partition)
        else:
            logging.warning("Unhandled message kind: %s", kind)
            _send_ack(stdout, envelope.delivery_id, partition)


if __name__ == "__main__":
    main()
