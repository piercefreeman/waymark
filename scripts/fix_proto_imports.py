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


# The well-known google.protobuf files messages.proto imports. The
# generated module's descriptor depends on them but protoc emits no
# imports for them here, so they are injected: imported, and registered
# with the default pool before the serialized file is added.
WELL_KNOWN_DEPENDENCIES = ["empty", "struct", "timestamp"]


def _import_line(name: str) -> str:
    return f"from google.protobuf import {name}_pb2 as google_dot_protobuf_dot_{name}__pb2"


def _register_line(name: str) -> str:
    return f"_sym_db.RegisterFileDescriptor(google_dot_protobuf_dot_{name}__pb2.DESCRIPTOR)"


def _ensure_well_known_imports(pb2_text: str) -> str:
    for name in WELL_KNOWN_DEPENDENCIES:
        pb2_text = pb2_text.replace(f"\n{_import_line(name)}\n", "\n")
    marker = "# @@protoc_insertion_point(imports)\n\n"
    if marker not in pb2_text:
        return pb2_text
    block = "".join(f"{_import_line(name)}\n" for name in WELL_KNOWN_DEPENDENCIES)
    return pb2_text.replace(marker, f"{marker}{block}\n", 1)


def _ensure_well_known_registrations(pb2_text: str) -> str:
    for name in WELL_KNOWN_DEPENDENCIES:
        pb2_text = pb2_text.replace(f"\n{_register_line(name)}\n", "\n")
    sym_decl = "_sym_db = _symbol_database.Default()\n"
    if sym_decl not in pb2_text:
        return pb2_text
    block = "".join(f"{_register_line(name)}\n" for name in WELL_KNOWN_DEPENDENCIES)
    return pb2_text.replace(sym_decl, f"{sym_decl}{block}", 1)


def _rewrite_messages_pb2(proto_dir: Path) -> None:
    """The framing proto imports empty.proto and timestamp.proto (scheduler RPCs)."""
    target = proto_dir / "messages_pb2.py"
    if not target.exists():
        return
    text = target.read_text()
    text = _ensure_well_known_imports(text)
    text = _ensure_well_known_registrations(text)
    target.write_text(text)


def _rewrite_python_value_pb2(proto_dir: Path) -> None:
    """The value-document proto imports struct.proto (NullValue)."""
    target = proto_dir / "python_value_pb2.py"
    if not target.exists():
        return
    text = target.read_text()
    text = _ensure_well_known_imports(text)
    text = _ensure_well_known_registrations(text)
    target.write_text(text)


def _rewrite_ast_pb2(proto_dir: Path) -> None:
    """Handle ast_pb2.py - no grpc needed since it's pure data structures."""
    target = proto_dir / "ast_pb2.py"
    if not target.exists():
        return
    # ast.proto doesn't need special import handling since it has no external deps


def main() -> None:
    for proto_dir in PROTO_DIRS:
        if not proto_dir.exists():
            continue
        _rewrite_messages_pb2_grpc(proto_dir)
        _rewrite_messages_pb2(proto_dir)
        _rewrite_python_value_pb2(proto_dir)
        _rewrite_messages_pb2_imports(proto_dir)
        _rewrite_ast_pb2(proto_dir)


if __name__ == "__main__":
    main()
