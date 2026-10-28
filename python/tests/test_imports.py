"""Workflow definitions must be safe to import before a hot-reloader forks."""

import subprocess
import sys
from pathlib import Path
from textwrap import dedent


def test_workflow_import_and_compilation_do_not_load_grpc() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            dedent("""
                import sys
                import waymark
                from tests.fixtures_workflow.workflow_default_args import WorkflowWithDefaultArgs

                assert all(name in vars(waymark) for name in waymark.__all__)
                WorkflowWithDefaultArgs.workflow_ir()
                assert not any(name == "grpc" or name.startswith("grpc.") for name in sys.modules)
            """),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
