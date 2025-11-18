#!/usr/bin/env python3
"""Post-process generated protobuf files to enforce package-relative imports."""

from __future__ import annotations

from pathlib import Path


def main() -> None:
    proto_dir = Path("python/proto")
    target = proto_dir / "messages_pb2_grpc.py"
    if not target.exists():
        return
    text = target.read_text()
    needle = "import messages_pb2 as messages__pb2"
    replacement = "from proto import messages_pb2 as messages__pb2"
    if needle in text and replacement not in text:
        target.write_text(text.replace(needle, replacement))


if __name__ == "__main__":
    main()
