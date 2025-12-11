#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click", "pydantic"]
# ///
"""Run benchmarks and output results in various formats."""

import json
import os
import re
import subprocess
import sys
from abc import ABC, abstractmethod
from pathlib import Path

import click
from pydantic import BaseModel

# =============================================================================
# Data Models
# =============================================================================


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


class GridCell(BaseModel):
    """Result for a single grid cell."""

    benchmark: str
    hosts: int
    instances: int
    workers_per_host: int
    total_workers: int
    result: BenchmarkResult | BenchmarkError


class GridData(BaseModel):
    """Complete grid benchmark data."""

    config: dict
    cells: list[GridCell]


class SingleData(BaseModel):
    """Single benchmark run data."""

    benchmark: str
    result: BenchmarkResult | BenchmarkError


# =============================================================================
# Output Formatters
# =============================================================================


class OutputFormatter(ABC):
    """Base class for output formatters."""

    @abstractmethod
    def format_single(self, data: SingleData) -> str:
        """Format a single benchmark result."""
        pass

    @abstractmethod
    def format_grid(self, data: GridData) -> str:
        """Format grid benchmark results."""
        pass


class JsonFormatter(OutputFormatter):
    """Output as JSON."""

    def format_single(self, data: SingleData) -> str:
        output = {
            "_meta": {"benchmark_available": True, "mode": "single"},
            data.benchmark: data.result.model_dump(),
        }
        return json.dumps(output, indent=2)

    def format_grid(self, data: GridData) -> str:
        output = {
            "_meta": {
                "benchmark_available": True,
                "mode": "grid",
                "config": data.config,
            },
            "grid": [cell.model_dump() for cell in data.cells],
        }
        return json.dumps(output, indent=2)


class TextFormatter(OutputFormatter):
    """Output as human-readable text."""

    # Minimum runtime in seconds for reliable throughput measurements
    MIN_RELIABLE_RUNTIME = 1.0

    def format_single(self, data: SingleData) -> str:
        lines = [
            f"Benchmark: {data.benchmark}",
            "=" * 50,
        ]

        if isinstance(data.result, BenchmarkResult):
            r = data.result
            warning = " (unreliable - too short)" if r.elapsed_s < self.MIN_RELIABLE_RUNTIME else ""
            lines.extend(
                [
                    f"  Total actions:    {r.total:,}",
                    f"  Elapsed time:     {r.elapsed_s:.2f}s{warning}",
                    f"  Throughput:       {r.throughput:.1f} actions/s",
                    f"  Avg latency:      {r.avg_round_trip_ms:.1f}ms",
                    f"  P95 latency:      {r.p95_round_trip_ms:.1f}ms",
                ]
            )
        else:
            lines.append(f"  ERROR: {data.result.error}")

        return "\n".join(lines)

    def format_grid(self, data: GridData) -> str:
        lines = [
            "Benchmark Grid Results",
            "=" * 86,
            "",
            "Configuration:",
            f"  Benchmarks:       {', '.join(data.config['benchmarks'])}",
            f"  Hosts:            {data.config['hosts']}",
            f"  Instances:        {data.config['instances']}",
            f"  Workers/host:     {data.config['workers_per_host']}",
        ]

        # Show per-benchmark config if available, otherwise show global
        if "benchmark_configs" in data.config:
            for bench, cfg in data.config["benchmark_configs"].items():
                lines.append(
                    f"  {bench}: loop_size={cfg['loop_size']}, complexity={cfg['complexity']}"
                )
        else:
            lines.append(f"  Loop size:        {data.config['loop_size']}")
            lines.append(f"  Complexity:       {data.config['complexity']}")

        lines.append("")

        # Group by benchmark type
        by_benchmark: dict[str, list[GridCell]] = {}
        for cell in data.cells:
            by_benchmark.setdefault(cell.benchmark, []).append(cell)

        for bench_name, cells in by_benchmark.items():
            lines.extend(
                [
                    f"[{bench_name}]",
                    "-" * 86,
                    f"{'Hosts':>6} {'Inst':>6} {'Workers':>8} {'Actions/s':>12} {'Elapsed':>10} {'P95 (ms)':>10} {'Avg (ms)':>10}",
                    "-" * 86,
                ]
            )

            for cell in cells:
                if isinstance(cell.result, BenchmarkResult):
                    r = cell.result
                    # Add warning marker for short runs
                    elapsed_str = f"{r.elapsed_s:.2f}s"
                    if r.elapsed_s < self.MIN_RELIABLE_RUNTIME:
                        elapsed_str += "*"
                    lines.append(
                        f"{cell.hosts:>6} {cell.instances:>6} {cell.total_workers:>8} "
                        f"{r.throughput:>12.1f} {elapsed_str:>10} {r.p95_round_trip_ms:>10.1f} {r.avg_round_trip_ms:>10.1f}"
                    )
                else:
                    lines.append(
                        f"{cell.hosts:>6} {cell.instances:>6} {cell.total_workers:>8} "
                        f"{'ERROR':>12} {'-':>10} {'-':>10} {'-':>10}"
                    )

            lines.append("")

        # Add note about short runs if any exist
        has_short_runs = any(
            isinstance(cell.result, BenchmarkResult)
            and cell.result.elapsed_s < self.MIN_RELIABLE_RUNTIME
            for cell in data.cells
        )
        if has_short_runs:
            lines.append(
                f"* = Run completed in <{self.MIN_RELIABLE_RUNTIME}s - throughput may be unreliable"
            )
            lines.append("")

        # Add scaling analysis
        lines.extend(self._scaling_analysis(data))

        return "\n".join(lines)

    def _scaling_analysis(self, data: GridData) -> list[str]:
        """Analyze how throughput scales with workers/instances."""
        lines = [
            "Scaling Analysis",
            "-" * 70,
        ]

        by_benchmark: dict[str, list[GridCell]] = {}
        for cell in data.cells:
            if isinstance(cell.result, BenchmarkResult):
                by_benchmark.setdefault(cell.benchmark, []).append(cell)

        for bench_name, cells in by_benchmark.items():
            if len(cells) < 2:
                continue

            # Find baseline (1 host, 1 instance)
            baseline = next((c for c in cells if c.hosts == 1 and c.instances == 1), cells[0])
            if not isinstance(baseline.result, BenchmarkResult):
                continue

            base_tp = baseline.result.throughput
            base_workers = baseline.total_workers

            lines.append(f"\n{bench_name}:")
            lines.append(f"  Baseline: {base_tp:.1f} actions/s @ {base_workers} workers")

            # Find best result
            best = max(
                cells,
                key=lambda c: c.result.throughput if isinstance(c.result, BenchmarkResult) else 0,
            )
            if isinstance(best.result, BenchmarkResult):
                best_tp = best.result.throughput
                speedup = best_tp / base_tp if base_tp > 0 else 0
                efficiency = (
                    (best_tp / best.total_workers) / (base_tp / base_workers)
                    if base_workers > 0 and best.total_workers > 0
                    else 0
                )
                lines.append(
                    f"  Best:     {best_tp:.1f} actions/s @ {best.total_workers} workers "
                    f"({best.hosts}h x {best.instances}i)"
                )
                lines.append(f"  Speedup:  {speedup:.2f}x")
                lines.append(f"  Efficiency: {efficiency:.1%} (throughput/worker vs baseline)")

        return lines


class MarkdownFormatter(OutputFormatter):
    """Output as Markdown table."""

    # Minimum runtime in seconds for reliable throughput measurements
    MIN_RELIABLE_RUNTIME = 1.0

    def format_single(self, data: SingleData) -> str:
        lines = [
            f"## Benchmark: {data.benchmark}",
            "",
        ]

        if isinstance(data.result, BenchmarkResult):
            r = data.result
            warning = " ⚠️" if r.elapsed_s < self.MIN_RELIABLE_RUNTIME else ""
            lines.extend(
                [
                    "| Metric | Value |",
                    "|--------|------:|",
                    f"| Total actions | {r.total:,} |",
                    f"| Elapsed time | {r.elapsed_s:.2f}s{warning} |",
                    f"| Throughput | {r.throughput:.1f} actions/s |",
                    f"| Avg latency | {r.avg_round_trip_ms:.1f}ms |",
                    f"| P95 latency | {r.p95_round_trip_ms:.1f}ms |",
                ]
            )
            if r.elapsed_s < self.MIN_RELIABLE_RUNTIME:
                lines.extend(
                    [
                        "",
                        f"⚠️ *Run completed in <{self.MIN_RELIABLE_RUNTIME}s - throughput may be unreliable*",
                    ]
                )
        else:
            lines.append(f"**ERROR:** {data.result.error}")

        return "\n".join(lines)

    def format_grid(self, data: GridData) -> str:
        lines = [
            "## Benchmark Grid Results",
            "",
            "### Configuration",
            "",
            f"- **Benchmarks:** {', '.join(data.config['benchmarks'])}",
            f"- **Hosts:** {data.config['hosts']}",
            f"- **Instances:** {data.config['instances']}",
            f"- **Workers/host:** {data.config['workers_per_host']}",
        ]

        # Show per-benchmark config if available
        if "benchmark_configs" in data.config:
            for bench, cfg in data.config["benchmark_configs"].items():
                lines.append(
                    f"- **{bench}:** loop_size={cfg['loop_size']}, complexity={cfg['complexity']}"
                )
        else:
            lines.append(f"- **Loop size:** {data.config['loop_size']}")
            lines.append(f"- **Complexity:** {data.config['complexity']}")

        lines.append("")

        # Group by benchmark type
        by_benchmark: dict[str, list[GridCell]] = {}
        for cell in data.cells:
            by_benchmark.setdefault(cell.benchmark, []).append(cell)

        for bench_name, cells in by_benchmark.items():
            lines.extend(
                [
                    f"### {bench_name}",
                    "",
                    "| Hosts | Instances | Workers | Actions/s | Elapsed | P95 (ms) | Avg (ms) |",
                    "|------:|----------:|--------:|----------:|--------:|---------:|---------:|",
                ]
            )

            for cell in cells:
                if isinstance(cell.result, BenchmarkResult):
                    r = cell.result
                    warning = " ⚠️" if r.elapsed_s < self.MIN_RELIABLE_RUNTIME else ""
                    lines.append(
                        f"| {cell.hosts} | {cell.instances} | {cell.total_workers} | "
                        f"{r.throughput:.1f} | {r.elapsed_s:.2f}s{warning} | {r.p95_round_trip_ms:.1f} | {r.avg_round_trip_ms:.1f} |"
                    )
                else:
                    lines.append(
                        f"| {cell.hosts} | {cell.instances} | {cell.total_workers} | "
                        f"ERROR | - | - | - |"
                    )

            lines.append("")

        # Add note about short runs if any exist
        has_short_runs = any(
            isinstance(cell.result, BenchmarkResult)
            and cell.result.elapsed_s < self.MIN_RELIABLE_RUNTIME
            for cell in data.cells
        )
        if has_short_runs:
            lines.append(
                f"⚠️ *Runs completed in <{self.MIN_RELIABLE_RUNTIME}s may have unreliable throughput measurements*"
            )
            lines.append("")

        return "\n".join(lines)


class CsvFormatter(OutputFormatter):
    """Output as CSV for spreadsheet import."""

    def format_single(self, data: SingleData) -> str:
        lines = ["benchmark,total,elapsed_s,throughput,avg_ms,p95_ms,error"]

        if isinstance(data.result, BenchmarkResult):
            r = data.result
            lines.append(
                f"{data.benchmark},{r.total},{r.elapsed_s:.3f},{r.throughput:.2f},"
                f"{r.avg_round_trip_ms:.2f},{r.p95_round_trip_ms:.2f},"
            )
        else:
            lines.append(f"{data.benchmark},,,,,,{data.result.error}")

        return "\n".join(lines)

    def format_grid(self, data: GridData) -> str:
        lines = [
            "benchmark,hosts,instances,workers_per_host,total_workers,"
            "total,elapsed_s,throughput,avg_ms,p95_ms,error"
        ]

        for cell in data.cells:
            base = (
                f"{cell.benchmark},{cell.hosts},{cell.instances},"
                f"{cell.workers_per_host},{cell.total_workers}"
            )
            if isinstance(cell.result, BenchmarkResult):
                r = cell.result
                lines.append(
                    f"{base},{r.total},{r.elapsed_s:.3f},{r.throughput:.2f},"
                    f"{r.avg_round_trip_ms:.2f},{r.p95_round_trip_ms:.2f},"
                )
            else:
                lines.append(f"{base},,,,,,{cell.result.error}")

        return "\n".join(lines)


FORMATTERS: dict[str, OutputFormatter] = {
    "json": JsonFormatter(),
    "text": TextFormatter(),
    "markdown": MarkdownFormatter(),
    "csv": CsvFormatter(),
}


# =============================================================================
# Benchmark Runner
# =============================================================================


def reset_database():
    """Reset the database tables for clean benchmark runs."""
    db_url = os.environ.get(
        "DATABASE_URL", "postgresql://mountaineer:mountaineer@localhost:5432/mountaineer_daemons"
    )

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
        return BenchmarkError(error="binary_not_found")
    except subprocess.TimeoutExpired:
        print(f"Benchmark timed out after {timeout}s", file=sys.stderr)
        return BenchmarkError(error="timeout")

    if result.returncode != 0:
        print(f"Benchmark failed with exit code {result.returncode}", file=sys.stderr)
        print(f"stderr: {result.stderr[-2000:]}", file=sys.stderr)
        return BenchmarkError(
            error="benchmark_failed",
            exit_code=result.returncode,
            stderr=result.stderr[-2000:] if result.stderr else None,
        )

    try:
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
        return BenchmarkError(
            error="json_parse_failed",
            stdout=result.stdout[-2000:] if result.stdout else None,
            stderr=result.stderr[-2000:] if result.stderr else None,
        )


# =============================================================================
# CLI Commands
# =============================================================================


@click.group()
def cli():
    """Run Rappel benchmarks."""
    pass


@cli.command()
@click.option(
    "--output", "-o", type=click.Path(), help="Output file path (stdout if not specified)"
)
@click.option(
    "--format",
    "-f",
    "fmt",
    type=click.Choice(list(FORMATTERS.keys())),
    default="text",
    help="Output format",
)
@click.option(
    "--benchmark",
    "-b",
    default="for-loop",
    type=click.Choice(["for-loop", "fan-out"]),
    help="Benchmark type",
)
@click.option(
    "--loop-size",
    default=16,
    help="Number of actions to spawn (fan-out width / for-loop iterations)",
)
@click.option("--complexity", default=1000, help="CPU complexity per action (hash iterations)")
@click.option("--workers-per-host", default=4, help="Number of Python workers per host")
@click.option("--hosts", default=1, help="Number of hosts")
@click.option("--instances", default=1, help="Number of workflow instances")
def single(
    output: str | None,
    fmt: str,
    benchmark: str,
    loop_size: int,
    complexity: int,
    workers_per_host: int,
    hosts: int,
    instances: int,
):
    """Run a single benchmark."""
    if not check_benchmark_available():
        print("Benchmark binary not found. Run 'cargo build --release' first.", file=sys.stderr)
        sys.exit(1)

    print(f"=== Running {benchmark} Benchmark ===", file=sys.stderr)
    reset_database()

    result = run_benchmark(
        [
            "--benchmark",
            benchmark,
            "--loop-size",
            str(loop_size),
            "--complexity",
            str(complexity),
            "--workers-per-host",
            str(workers_per_host),
            "--hosts",
            str(hosts),
            "--instances",
            str(instances),
            "--log-interval",
            "0",
        ]
    )

    data = SingleData(benchmark=benchmark, result=result)
    formatter = FORMATTERS[fmt]
    formatted = formatter.format_single(data)

    if output:
        Path(output).write_text(formatted)
        print(f"Results written to {output}", file=sys.stderr)
    else:
        print(formatted)


def parse_benchmark_config(config_str: str) -> dict[str, dict[str, int]]:
    """Parse benchmark-specific config string.

    Format: "benchmark:loop_size:complexity,benchmark:loop_size:complexity,..."
    Example: "for-loop:64:500,fan-out:32:200"

    Returns dict like {"for-loop": {"loop_size": 64, "complexity": 500}, ...}
    """
    result = {}
    for part in config_str.split(","):
        parts = part.strip().split(":")
        if len(parts) == 3:
            bench, loop_size, complexity = parts
            result[bench.strip()] = {
                "loop_size": int(loop_size),
                "complexity": int(complexity),
            }
    return result


@cli.command()
@click.option(
    "--output", "-o", type=click.Path(), help="Output file path (stdout if not specified)"
)
@click.option(
    "--format",
    "-f",
    "fmt",
    type=click.Choice(list(FORMATTERS.keys())),
    default="text",
    help="Output format",
)
@click.option("--hosts", default="1,2,4", help="Comma-separated list of host counts")
@click.option("--instances", default="1,2,4,8", help="Comma-separated list of instance counts")
@click.option("--workers-per-host", default=4, help="Number of Python workers per host")
@click.option(
    "--benchmarks", default="for-loop,fan-out", help="Comma-separated list of benchmark types"
)
@click.option("--loop-size", default=16, help="Default number of actions to spawn per workflow")
@click.option(
    "--complexity", default=100, help="Default CPU complexity per action (hash iterations)"
)
@click.option(
    "--benchmark-config",
    "benchmark_config_str",
    default=None,
    help="Per-benchmark config: 'bench:loop_size:complexity,...' e.g. 'for-loop:64:500,fan-out:32:200'",
)
@click.option("--timeout", default=300, help="Timeout per benchmark run in seconds")
def grid(
    output: str | None,
    fmt: str,
    hosts: str,
    instances: str,
    workers_per_host: int,
    benchmarks: str,
    loop_size: int,
    complexity: int,
    benchmark_config_str: str | None,
    timeout: int,
):
    """Run a grid of benchmarks across hosts and instances.

    This runs ALL benchmark types with varying numbers of hosts and instances,
    allowing you to see how throughput scales with parallelism.

    You can specify different loop_size/complexity per benchmark using --benchmark-config:

        --benchmark-config "for-loop:64:500,fan-out:32:200"

    This sets for-loop to loop_size=64, complexity=500 and fan-out to loop_size=32, complexity=200.
    Benchmarks not in the config use the default --loop-size and --complexity values.
    """
    host_counts = [int(h.strip()) for h in hosts.split(",")]
    instance_counts = [int(i.strip()) for i in instances.split(",")]
    benchmark_types = [b.strip() for b in benchmarks.split(",")]

    # Parse per-benchmark config
    benchmark_configs: dict[str, dict[str, int]] = {}
    if benchmark_config_str:
        benchmark_configs = parse_benchmark_config(benchmark_config_str)

    # Fill in defaults for benchmarks not in the config
    for bench in benchmark_types:
        if bench not in benchmark_configs:
            benchmark_configs[bench] = {"loop_size": loop_size, "complexity": complexity}

    if not check_benchmark_available():
        print("Benchmark binary not found. Run 'cargo build --release' first.", file=sys.stderr)
        sys.exit(1)

    total_runs = len(benchmark_types) * len(host_counts) * len(instance_counts)
    current_run = 0

    cells: list[GridCell] = []

    print("=== Running Benchmark Grid ===", file=sys.stderr)
    print(f"Benchmarks: {benchmark_types}", file=sys.stderr)
    print(f"Hosts: {host_counts}", file=sys.stderr)
    print(f"Instances: {instance_counts}", file=sys.stderr)
    print(f"Workers per host: {workers_per_host}", file=sys.stderr)
    for bench, cfg in benchmark_configs.items():
        print(
            f"  {bench}: loop_size={cfg['loop_size']}, complexity={cfg['complexity']}",
            file=sys.stderr,
        )
    print(f"Total runs: {total_runs}", file=sys.stderr)
    print("", file=sys.stderr)

    for benchmark_type in benchmark_types:
        bench_cfg = benchmark_configs[benchmark_type]
        bench_loop_size = bench_cfg["loop_size"]
        bench_complexity = bench_cfg["complexity"]

        for host_count in host_counts:
            for instance_count in instance_counts:
                current_run += 1
                total_workers = host_count * workers_per_host

                print(
                    f"[{current_run}/{total_runs}] {benchmark_type} | "
                    f"hosts={host_count} instances={instance_count} "
                    f"workers={total_workers}",
                    file=sys.stderr,
                )

                reset_database()

                result = run_benchmark(
                    [
                        "--benchmark",
                        benchmark_type,
                        "--loop-size",
                        str(bench_loop_size),
                        "--complexity",
                        str(bench_complexity),
                        "--hosts",
                        str(host_count),
                        "--workers-per-host",
                        str(workers_per_host),
                        "--instances",
                        str(instance_count),
                        "--log-interval",
                        "0",
                        "--timeout",
                        str(timeout),
                    ],
                    timeout=timeout + 30,
                )

                cell = GridCell(
                    benchmark=benchmark_type,
                    hosts=host_count,
                    instances=instance_count,
                    workers_per_host=workers_per_host,
                    total_workers=total_workers,
                    result=result,
                )
                cells.append(cell)

                if isinstance(result, BenchmarkResult):
                    print(
                        f"    -> {result.throughput:.1f} actions/s, "
                        f"elapsed={result.elapsed_s:.2f}s, "
                        f"p95={result.p95_round_trip_ms:.1f}ms",
                        file=sys.stderr,
                    )
                else:
                    print(f"    -> ERROR: {result.error}", file=sys.stderr)

    config = {
        "hosts": host_counts,
        "instances": instance_counts,
        "workers_per_host": workers_per_host,
        "benchmarks": benchmark_types,
        "benchmark_configs": benchmark_configs,
    }

    data = GridData(config=config, cells=cells)
    formatter = FORMATTERS[fmt]
    formatted = formatter.format_grid(data)

    if output:
        Path(output).write_text(formatted)
        print(f"\nResults written to {output}", file=sys.stderr)
    else:
        print(formatted)


def main():
    """Entry point with backwards compatibility."""
    if len(sys.argv) > 1 and sys.argv[1] in ["single", "grid"]:
        cli()
    else:
        # Legacy: default to 'single' with old argument style
        sys.argv.insert(1, "single")
        cli()


if __name__ == "__main__":
    main()
