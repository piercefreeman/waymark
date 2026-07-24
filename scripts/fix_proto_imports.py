#!/usr/bin/env python3
"""Post-process generated protobuf files to enforce package-local imports."""

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
PROTO_DIRS = [
    ROOT_DIR / "python" / "src" / "waymark" / "proto",
]


def _rewrite_messages_pb2_grpc(proto_dir: Path) -> None:
    target = proto_dir / "messages_pb2_grpc.py"
    if target.exists():
        text = target.read_text()
        needle = "import messages_pb2 as messages__pb2"
        replacement = "from . import messages_pb2 as messages__pb2"
        if needle in text and replacement not in text:
            target.write_text(text.replace(needle, replacement))

    stub_target = proto_dir / "messages_pb2_grpc.pyi"
    if stub_target.exists():
        stub_text = stub_target.read_text()
        stub_needle = "import messages_pb2"
        stub_replacement = "from . import messages_pb2"
        if stub_needle in stub_text and stub_replacement not in stub_text:
            stub_target.write_text(stub_text.replace(stub_needle, stub_replacement))


def _rewrite_messages_pb2_imports(proto_dir: Path) -> None:
    """Fix ast_pb2 import in messages_pb2.py to use package-relative imports."""
    target = proto_dir / "messages_pb2.py"
    if not target.exists():
        return
    text = target.read_text()
    needle = "import ast_pb2 as ast__pb2"
    replacement = "from . import ast_pb2 as ast__pb2"
    if needle in text and replacement not in text:
        text = text.replace(needle, replacement)
        target.write_text(text)

    stub_target = proto_dir / "messages_pb2.pyi"
    if stub_target.exists():
        stub_text = stub_target.read_text()
        stub_needle = "import ast_pb2"
        stub_replacement = "from . import ast_pb2"
        if stub_needle in stub_text and stub_replacement not in stub_text:
            stub_target.write_text(stub_text.replace(stub_needle, stub_replacement))


STRUCT_IMPORT_LINE = "from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2"
REGISTER_LINE = "_sym_db.RegisterFileDescriptor(google_dot_protobuf_dot_struct__pb2.DESCRIPTOR)"


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


def _rewrite_messages_pb2(proto_dir: Path) -> None:
    target = proto_dir / "messages_pb2.py"
    if not target.exists():
        return
    text = target.read_text()
    text = _ensure_struct_import(text)
    text = _ensure_struct_registration(text)
    target.write_text(text)


def _rewrite_ast_pb2(proto_dir: Path) -> None:
    """Restore the shared action proto import after formatting generated code."""
    _rewrite_action_import(proto_dir / "ast_pb2.py")
    _rewrite_action_stub_import(proto_dir / "ast_pb2.pyi")


def _rewrite_action_import(target: Path) -> None:
    if not target.exists():
        return
    text = target.read_text()
    import_line = "from . import action_pb2 as action__pb2  # noqa: F401"
    if import_line in text:
        return
    marker = "# @@protoc_insertion_point(imports)\n"
    target.write_text(text.replace(marker, f"{marker}{import_line}\n", 1))


def _rewrite_action_stub_import(target: Path) -> None:
    if not target.exists():
        return
    text = target.read_text()
    target.write_text(text.replace("import action_pb2", "from . import action_pb2"))


def main() -> None:
    for proto_dir in PROTO_DIRS:
        if not proto_dir.exists():
            continue
        _rewrite_messages_pb2_grpc(proto_dir)
        _rewrite_messages_pb2(proto_dir)
        _rewrite_messages_pb2_imports(proto_dir)
        _rewrite_action_import(proto_dir / "messages_pb2.py")
        _rewrite_action_stub_import(proto_dir / "messages_pb2.pyi")
        _rewrite_ast_pb2(proto_dir)


if __name__ == "__main__":
    main()
