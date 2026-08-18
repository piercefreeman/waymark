#!/usr/bin/env python3
"""Produce fixture ground truth and IR metadata for Rust integration execution."""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import importlib
import json
import sys
from pathlib import Path
from typing import Any


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def ensure_import_paths(root: Path) -> None:
    paths = [
        root,
        root / "tests",
        root / "tests" / "integration_tests",
        root / "python",
        root / "python" / "src",
        root / "python" / "proto",
    ]
    for path in reversed(paths):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)


async def run_inline(workflow_cls: type, kwargs: dict[str, Any]) -> dict[str, str]:
    """Run the workflow as plain Python and encode what it produced.

    The outcome is encoded with the very serializer the worker uses on
    the wire, so the expected value is a value rather than a rendering
    of one.
    """
    from waymark.serialization import dumps, dumps_exception

    workflow = workflow_cls()
    run_impl = getattr(workflow, "__workflow_run_impl__", None)
    if run_impl is None:
        raise RuntimeError(
            f"workflow class '{workflow_cls.__name__}' missing __workflow_run_impl__"
        )

    try:
        result = await run_impl(**copy.deepcopy(kwargs))
        return {
            "status": "ok",
            "value_hex": dumps(result).SerializeToString().hex(),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "error",
            "value_hex": dumps_exception(exc).SerializeToString().hex(),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit fixture ground truth and IR bytes")
    parser.add_argument("--module", required=True)
    parser.add_argument("--workflow-class", required=True)
    parser.add_argument("--kwargs-hex-encoded", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    ensure_import_paths(repo_root())

    # Imported here rather than at module scope: the waymark packages are
    # only importable once the paths above are in place.
    from waymark.proto import messages_pb2
    from waymark.serialization import arguments_to_kwargs

    arguments = messages_pb2.WorkflowArguments()
    arguments.ParseFromString(bytes.fromhex(args.kwargs_hex_encoded))
    kwargs = arguments_to_kwargs(arguments)

    module = importlib.import_module(args.module)
    workflow_cls = module.__dict__.get(args.workflow_class)
    if workflow_cls is None:
        raise RuntimeError(
            f"workflow class '{args.workflow_class}' not found in module '{args.module}'"
        )

    expected = asyncio.run(run_inline(workflow_cls, kwargs))

    program = workflow_cls.workflow_ir()
    ir_bytes = program.SerializeToString()
    ir_hash = hashlib.sha256(ir_bytes).hexdigest()

    workflow_version = workflow_cls.version or ir_hash

    payload = {
        "expected": expected,
        "registration": {
            "workflow_name": workflow_cls.short_name(),
            "workflow_version": workflow_version,
            "ir_hash": ir_hash,
            "ir_bytes_hex": ir_bytes.hex(),
        },
    }
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
