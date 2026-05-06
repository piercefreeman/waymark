import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

from waymark.workflow import workflow_registry


def import_from_path(module_name: str, file_path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module {module_name} from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract IR from Python workflow.")
    parser.add_argument("module_path", help="Path to workflow module file.")
    parser.add_argument("-w", "--workflow", help="Name of the workflow class.")
    parser.add_argument(
        "-o", "--out", required=True, help="Path to write IR bytes; use - to write to stdout."
    )
    args = parser.parse_args()

    out_path = Path(args.out) if args.out != "-" else None

    module_path = Path(args.module_path).resolve()
    import_from_path(module_path.stem, module_path)

    workflow_name = args.workflow
    if workflow_name is None:
        names = workflow_registry.names()
        if len(names) == 1:
            workflow_name = names[0]
        else:
            parser.error("Unable to determine the workflow name")

    workflow_cls = workflow_registry.get(workflow_name)
    if workflow_cls is None:
        workflow_cls = workflow_registry.get(workflow_name.lower())
    if workflow_cls is None:
        parser.error(f"Unable to locate the workflow {workflow_name}")
    assert workflow_cls is not None

    program = workflow_cls.workflow_ir()
    program_data = program.SerializeToString()

    if out_path:
        out_path.write_bytes(program_data)
    else:
        sys.stdout.buffer.write(program_data)


if __name__ == "__main__":
    main()
