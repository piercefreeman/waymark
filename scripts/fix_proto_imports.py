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

    pb2_target = proto_dir / "messages_pb2.py"
    if not pb2_target.exists():
        return
    pb2_text = pb2_target.read_text()
    struct_import = (
        "from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2\n"
    )
    import_marker = "# @@protoc_insertion_point(imports)\n"
    if struct_import not in pb2_text and import_marker in pb2_text:
        pb2_text = pb2_text.replace(
            import_marker,
            import_marker + "\n" + struct_import + "\n",
            1,
        )
    register_line = (
        "_sym_db.RegisterFileDescriptor(google_dot_protobuf_dot_struct__pb2.DESCRIPTOR)\n"
    )
    sym_db_marker = "_sym_db = _symbol_database.Default()\n"
    if register_line not in pb2_text and sym_db_marker in pb2_text:
        pb2_text = pb2_text.replace(sym_db_marker, sym_db_marker + register_line + "\n", 1)
    pb2_target.write_text(pb2_text)


if __name__ == "__main__":
    main()
