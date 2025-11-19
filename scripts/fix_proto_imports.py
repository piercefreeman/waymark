#!/usr/bin/env python3
"""Post-process generated protobuf files to enforce package-relative imports."""

from __future__ import annotations

from pathlib import Path


PROTO_DIR = Path("python/proto")


def _rewrite_messages_pb2_grpc() -> None:
    target = PROTO_DIR / "messages_pb2_grpc.py"
    if target.exists():
        text = target.read_text()
        needle = "import messages_pb2 as messages__pb2"
        replacement = "from proto import messages_pb2 as messages__pb2"
        if needle in text and replacement not in text:
            target.write_text(text.replace(needle, replacement))

    stub_target = PROTO_DIR / "messages_pb2_grpc.pyi"
    if stub_target.exists():
        stub_text = stub_target.read_text()
        stub_needle = "import messages_pb2"
        stub_replacement = "from proto import messages_pb2"
        if stub_needle in stub_text and stub_replacement not in stub_text:
            stub_target.write_text(stub_text.replace(stub_needle, stub_replacement))


STRUCT_IMPORT_LINE = (
    "from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2"
)
REGISTER_LINE = (
    "_sym_db.RegisterFileDescriptor(google_dot_protobuf_dot_struct__pb2.DESCRIPTOR)"
)


def _ensure_struct_import(pb2_text: str) -> str:
    without_existing = pb2_text.replace(f"\n{STRUCT_IMPORT_LINE}\n", "\n")
    marker = "# @@protoc_insertion_point(imports)\n\n"
    replacement = f"{marker}{STRUCT_IMPORT_LINE}\n\n"
    if marker not in without_existing:
        return without_existing
    return without_existing.replace(marker, replacement, 1)


def _ensure_struct_registration(pb2_text: str) -> str:
    needle = f"\n{REGISTER_LINE}\n"
    without_existing = pb2_text.replace(needle, "\n")
    sym_decl = "_sym_db = _symbol_database.Default()\n"
    if sym_decl not in without_existing:
        return without_existing
    replacement = f"{sym_decl}{REGISTER_LINE}\n"
    return without_existing.replace(sym_decl, replacement, 1)


def _rewrite_messages_pb2() -> None:
    target = PROTO_DIR / "messages_pb2.py"
    if not target.exists():
        return
    text = target.read_text()
    text = _ensure_struct_import(text)
    text = _ensure_struct_registration(text)
    target.write_text(text)


def main() -> None:
    _rewrite_messages_pb2_grpc()
    _rewrite_messages_pb2()


if __name__ == "__main__":
    main()
