#!/usr/bin/env python3
"""Produce fixture ground truth and IR metadata for Rust integration execution."""

from __future__ import annotations

import argparse
import asyncio
import copy
import dataclasses
import hashlib
import importlib
import json
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel


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


def canonicalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, BaseException):
        return {
            "type": type(value).__name__,
            "message": str(value),
        }

    if isinstance(value, BaseModel):
        return canonicalize(value.model_dump(mode="python"))

    if dataclasses.is_dataclass(value):
        return canonicalize(dataclasses.asdict(value))

    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key in sorted(value.keys(), key=lambda item: str(item)):
            normalized[str(key)] = canonicalize(value[key])
        return normalized

    if isinstance(value, (list, tuple)):
        return [canonicalize(item) for item in value]

    if isinstance(value, set):
        values = [canonicalize(item) for item in value]
        return sorted(values, key=lambda item: json.dumps(item, sort_keys=True))

    return repr(value)


async def run_inline(workflow_cls: type, kwargs: dict[str, Any]) -> dict[str, Any]:
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
            "value": canonicalize(result),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "error",
            "value": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Emit fixture ground truth and IR bytes")
    parser.add_argument("--module", required=True)
    parser.add_argument("--workflow-class", required=True)
    parser.add_argument("--kwargs-json", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    ensure_import_paths(repo_root())

    kwargs = json.loads(args.kwargs_json)
    if not isinstance(kwargs, dict):
        raise RuntimeError("--kwargs-json must deserialize to an object")

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
            "ir_bytes": list(ir_bytes),
        },
    }
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
