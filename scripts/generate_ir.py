#!/usr/bin/env python3
"""
Generate IR from a Python workflow file.

This script takes a Python workflow file path and outputs the IR in various formats.
"""

import argparse
import base64
import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING

# Add the python src and proto to path for imports
project_root = Path(__file__).parent.parent
python_src = project_root / "python" / "src"
proto_path = project_root / "python"  # proto module is at python/proto/
sys.path.insert(0, str(python_src))
sys.path.insert(0, str(proto_path))

if TYPE_CHECKING:
    sys.path.insert(0, str(python_src))
    sys.path.insert(0, str(proto_path))
    from waymark.workflow import Workflow


def load_workflow_from_file(file_path: str) -> type["Workflow"]:
    """Load a workflow class from a Python file."""
    from waymark.workflow import Workflow

    path = Path(file_path).resolve()

    # Load the module
    spec = importlib.util.spec_from_file_location("workflow_module", path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["workflow_module"] = module
    spec.loader.exec_module(module)

    # Find the workflow class
    workflow_classes = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Workflow)
            and obj is not Workflow
            and hasattr(obj, "__workflow_run_impl__")
        ):
            workflow_classes.append(obj)

    if not workflow_classes:
        raise ValueError(f"No workflow class found in {path}")

    if len(workflow_classes) > 1:
        print(
            f"Warning: Multiple workflow classes found, using {workflow_classes[0].__name__}",
            file=sys.stderr,
        )

    return workflow_classes[0]


def main():
    from waymark.ir_builder import build_workflow_ir

    parser = argparse.ArgumentParser(description="Generate IR from a Python workflow file")
    parser.add_argument("file", help="Path to Python workflow file")
    parser.add_argument(
        "--format",
        choices=["base64", "binary", "text"],
        default="base64",
        help="Output format (default: base64)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file (default: stdout for base64/text, required for binary)",
    )

    args = parser.parse_args()

    # Load and build IR
    workflow_cls = load_workflow_from_file(args.file)
    program = build_workflow_ir(workflow_cls)

    # Serialize
    binary_data = program.SerializeToString()

    if args.format == "base64":
        output = base64.b64encode(binary_data).decode("ascii")
        if args.output:
            Path(args.output).write_text(output)
        else:
            print(output)
    elif args.format == "binary":
        if not args.output:
            print("Error: --output required for binary format", file=sys.stderr)
            sys.exit(1)
        Path(args.output).write_bytes(binary_data)
    elif args.format == "text":
        # Pretty print the protobuf
        from google.protobuf import text_format

        output = text_format.MessageToString(program)
        if args.output:
            Path(args.output).write_text(output)
        else:
            print(output)


if __name__ == "__main__":
    main()
