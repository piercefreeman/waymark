#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click", "pydantic"]
# ///
"""Run benchmarks and output results as JSON."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import click
from pydantic import BaseModel


class BenchmarkResult(BaseModel):
    """Successful benchmark result matching the Rust BenchmarkOutput struct."""

    total: int
    elapsed_s: float
    throughput: float
    avg_round_trip_ms: float
    p95_round_trip_ms: float


class BenchmarkError(BaseModel):
    """Error result when benchmark fails."""

    error: str
    exit_code: int | None = None
    stderr: str | None = None
    stdout: str | None = None


def reset_database():
    """Reset the database tables for clean benchmark runs."""
    db_url = os.environ.get(
        "DATABASE_URL", "postgresql://mountaineer:mountaineer@localhost:5432/mountaineer_daemons"
    )

    # Parse connection string
    match = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", db_url)
    if not match:
        print(f"Warning: Could not parse DATABASE_URL: {db_url}", file=sys.stderr)
        return

    user, password, host, port, dbname = match.groups()

    env = os.environ.copy()
    env["PGPASSWORD"] = password

    tables = [
        "action_queue",
        "instance_context",
        "loop_state",
        "workflow_instances",
        "workflow_versions",
    ]

    cmd = [
        "psql",
        "-h",
        host,
        "-p",
        port,
        "-U",
        user,
        "-d",
        dbname,
        "-c",
        f"TRUNCATE {', '.join(tables)} CASCADE;",
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Warning: Database reset failed: {result.stderr}", file=sys.stderr)


def check_benchmark_available() -> bool:
    """Check if the benchmark binary exists."""
    binary_path = Path("./target/release/benchmark")
    if not binary_path.exists():
        return False

    # Check if it responds to --help
    try:
        result = subprocess.run(
            ["./target/release/benchmark", "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def run_benchmark(args: list[str], timeout: int = 300) -> BenchmarkResult | BenchmarkError:
    """Run the benchmark binary with --json flag and parse the JSON output."""
    cmd = ["./target/release/benchmark", "--json"] + args

    print(f"Running: {' '.join(cmd)}", file=sys.stderr)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        print("Benchmark binary not found", file=sys.stderr)
        return BenchmarkError(error="binary_not_found")
    except subprocess.TimeoutExpired:
        print(f"Benchmark timed out after {timeout}s", file=sys.stderr)
        return BenchmarkError(error="timeout")

    # Check for non-zero exit code
    if result.returncode != 0:
        print(f"Benchmark failed with exit code {result.returncode}", file=sys.stderr)
        print(f"stderr: {result.stderr[-2000:]}", file=sys.stderr)
        return BenchmarkError(
            error="benchmark_failed",
            exit_code=result.returncode,
            stderr=result.stderr[-2000:] if result.stderr else None,
        )

    # Parse JSON from stdout - the JSON is on the last line, preceded by tracing logs
    try:
        # Find the JSON line (starts with '{' and ends with '}')
        json_line = None
        for line in reversed(result.stdout.strip().split("\n")):
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                json_line = line
                break

        if not json_line:
            raise ValueError("No JSON line found in output")

        return BenchmarkResult.model_validate_json(json_line)
    except Exception as e:
        print(f"Failed to parse JSON output: {e}", file=sys.stderr)
        print(f"stdout: {result.stdout[-1500:]}", file=sys.stderr)
        print(f"stderr: {result.stderr[-1500:]}", file=sys.stderr)
        return BenchmarkError(
            error="json_parse_failed",
            stdout=result.stdout[-2000:] if result.stdout else None,
            stderr=result.stderr[-2000:] if result.stderr else None,
        )


@click.command()
@click.option("--output", "-o", type=click.Path(), required=True, help="Output JSON file path")
@click.option("--width", default=16, help="Fan-out width (parallel actions)")
@click.option("--hash-iterations", default=1000, help="Hash iterations per action (CPU intensity)")
@click.option("--workers", default=4, help="Number of Python workers")
def main(output: str, width: int, hash_iterations: int, workers: int):
    """Run fan-out/fan-in benchmark and output results as JSON."""
    results: dict[str, dict] = {}

    # Check if benchmark binary is available
    if not check_benchmark_available():
        print("Benchmark binary not available", file=sys.stderr)
        results["_meta"] = {
            "benchmark_available": False,
            "reason": "Benchmark binary not found. Run 'cargo build --release' first.",
        }
        Path(output).write_text(json.dumps(results, indent=2))
        print(json.dumps(results, indent=2))
        return

    results["_meta"] = {"benchmark_available": True}

    # Run the fan-out/fan-in benchmark
    print("=== Running Fan-Out/Fan-In Benchmark ===", file=sys.stderr)
    reset_database()
    results["fanout"] = run_benchmark(
        [
            "--width",
            str(width),
            "--hash-iterations",
            str(hash_iterations),
            "--workers",
            str(workers),
            "--log-interval",
            "0",
        ],
    ).model_dump()

    # Write results
    Path(output).write_text(json.dumps(results, indent=2))
    print(f"Results written to {output}", file=sys.stderr)

    # Also print to stdout for easy viewing
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
