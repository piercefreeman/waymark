import argparse
import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast

from waymark.workflow import Workflow


class FuzzModule(Protocol):
    FuzzWorkflow: type[Workflow]
    TRACE_LOG: list[dict[str, object]]


def _load_module(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("fuzz_case", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def _run_workflow(module: FuzzModule, inputs: dict[str, object]) -> object:
    workflow_cls = cast(Any, module.FuzzWorkflow)
    workflow = workflow_cls()
    run_impl = workflow_cls.__workflow_run_impl__
    return await run_impl(workflow, **inputs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Python workflow oracle for fuzzing.")
    parser.add_argument("--module-path", required=True, help="Path to generated workflow module.")
    parser.add_argument("--inputs-path", required=True, help="Path to JSON inputs.")
    parser.add_argument("--trace-out", required=True, help="Path to write trace JSON.")
    parser.add_argument(
        "--registration-out", required=True, help="Path to write registration bytes."
    )
    args = parser.parse_args()

    module_path = Path(args.module_path)
    inputs_path = Path(args.inputs_path)
    trace_out = Path(args.trace_out)
    registration_out = Path(args.registration_out)

    inputs = json.loads(inputs_path.read_text())
    module = cast(FuzzModule, _load_module(module_path))

    result = asyncio.run(_run_workflow(module, inputs))

    trace_log = module.TRACE_LOG
    trace_data = {"actions": trace_log, "workflow_output": result}
    trace_out.write_text(json.dumps(trace_data, sort_keys=True))

    workflow_cls = cast(Any, module.FuzzWorkflow)
    initial_context = workflow_cls._build_initial_context((), inputs)
    payload = workflow_cls._build_registration_payload(initial_context)
    registration_out.write_bytes(payload.SerializeToString())


if __name__ == "__main__":
    main()
