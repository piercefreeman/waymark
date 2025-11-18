"""CLI for running the workflow benchmark locally."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from fixtures.benchmark_instances import run_benchmark_instances


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark workflow instance execution")
    parser.add_argument(
        "--iterations", type=int, default=50, help="Number of workflow runs to execute"
    )
    parser.add_argument("--batch-size", type=int, default=4, help="Payloads per workflow run")
    parser.add_argument(
        "--payload-size", type=int, default=1024, help="Payload size per request in bytes"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_benchmark_instances(
        iterations=args.iterations,
        batch_size=args.batch_size,
        payload_size=args.payload_size,
    )
    print(
        f"Ran {result.iterations} workflows in {result.elapsed:.2f}s "
        f"({result.workflows_per_sec:.1f} wf/s, {result.actions_per_sec:.1f} actions/s)"
    )


if __name__ == "__main__":
    main()
