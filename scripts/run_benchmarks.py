#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click", "pydantic"]
# ///
"""Run benchmarks and output results in various formats."""

import json
import os
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Thread
from urllib.parse import unquote, urlparse

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


class SuiteData(BaseModel):
    """Multiple benchmark runs with shared configuration."""

    config: dict
    results: dict[str, BenchmarkResult | BenchmarkError]


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

    @abstractmethod
    def format_suite(self, data: SuiteData) -> str:
        """Format multiple benchmark results."""
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

    def format_suite(self, data: SuiteData) -> str:
        output = {
            "_meta": {
                "benchmark_available": True,
                "mode": "suite",
                "config": data.config,
            }
        }
        for bench, result in data.results.items():
            output[bench] = result.model_dump()
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
            f"  Max slots/worker: {data.config['max_slots_per_worker']}",
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

    def format_suite(self, data: SuiteData) -> str:
        lines = [
            "Benchmark Suite Results",
            "=" * 50,
        ]
        for key, value in data.config.items():
            lines.append(f"{key}: {value}")
        lines.append("")

        for bench_name, result in data.results.items():
            lines.append(self.format_single(SingleData(benchmark=bench_name, result=result)))
            lines.append("")

        return "\n".join(lines).strip()

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

    def format_suite(self, data: SuiteData) -> str:
        lines = [
            "## Benchmark Suite Results",
            "",
            "### Configuration",
            "",
        ]
        for key, value in data.config.items():
            lines.append(f"- **{key}:** {value}")
        lines.append("")

        for bench_name, result in data.results.items():
            lines.append(self.format_single(SingleData(benchmark=bench_name, result=result)))
            lines.append("")

        return "\n".join(lines).strip()


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

    def format_suite(self, data: SuiteData) -> str:
        lines = ["benchmark,total,elapsed_s,throughput,avg_ms,p95_ms,error"]
        for bench_name, result in data.results.items():
            if isinstance(result, BenchmarkResult):
                lines.append(
                    f"{bench_name},{result.total},{result.elapsed_s:.3f},{result.throughput:.2f},"
                    f"{result.avg_round_trip_ms:.2f},{result.p95_round_trip_ms:.2f},"
                )
            else:
                lines.append(f"{bench_name},,,,,,{result.error}")
        return "\n".join(lines)


FORMATTERS: dict[str, OutputFormatter] = {
    "json": JsonFormatter(),
    "text": TextFormatter(),
    "markdown": MarkdownFormatter(),
    "csv": CsvFormatter(),
}

# =============================================================================
# Database Configuration
# =============================================================================


@dataclass(frozen=True)
class DatabaseConfig:
    """Postgres connection parameters for benchmarks."""

    user: str
    password: str
    host: str
    port: int
    dbname: str


# Defaults align with docker-compose.yml for local runs.
DEFAULT_DB_CONFIG = DatabaseConfig(
    user="mountaineer",
    password="mountaineer",
    host="localhost",
    port=5433,
    dbname="mountaineer_daemons",
)

DEFAULT_DB_URL = (
    f"postgresql://{DEFAULT_DB_CONFIG.user}:{DEFAULT_DB_CONFIG.password}@"
    f"{DEFAULT_DB_CONFIG.host}:{DEFAULT_DB_CONFIG.port}/{DEFAULT_DB_CONFIG.dbname}"
)

STATUS_LOG_INTERVAL_S = 5.0
STATUS_QUERY_TIMEOUT_S = 3
COMPLETED_ACTIONS_QUERY = (
    "SELECT (SELECT COUNT(*) FROM action_logs)"
    " + (SELECT COUNT(*) FROM action_log_queue)"
    " + (SELECT COUNT(*) FROM action_queue WHERE status = 'completed')"
)


# =============================================================================
# Benchmark Runner
# =============================================================================


def get_database_url() -> str:
    """Return the database URL from the environment or defaults."""
    db_url = os.environ.get("RAPPEL_DATABASE_URL")
    return db_url if db_url else DEFAULT_DB_URL


def parse_database_url(db_url: str) -> DatabaseConfig:
    """Parse a postgres URL into a DatabaseConfig."""
    parsed = urlparse(db_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError("unsupported scheme (expected postgresql)")
    if not parsed.username or not parsed.password or not parsed.hostname or not parsed.port:
        raise ValueError("missing username, password, host, or port")
    dbname = parsed.path.lstrip("/")
    if not dbname:
        raise ValueError("missing database name")
    return DatabaseConfig(
        user=unquote(parsed.username),
        password=unquote(parsed.password),
        host=parsed.hostname,
        port=parsed.port,
        dbname=dbname,
    )


def check_database_connection(config: DatabaseConfig, timeout: int = 5) -> tuple[bool, str | None]:
    """Verify we can connect to Postgres before running benchmarks."""
    env = os.environ.copy()
    env["PGPASSWORD"] = config.password

    cmd = [
        "psql",
        "-h",
        config.host,
        "-p",
        str(config.port),
        "-U",
        config.user,
        "-d",
        config.dbname,
        "-c",
        "SELECT 1;",
    ]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return False, "psql not found (install postgresql-client)"
    except subprocess.TimeoutExpired:
        return False, f"connection timed out after {timeout}s"

    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        return False, detail

    return True, None


def ensure_database_available() -> DatabaseConfig:
    """Exit early if Postgres is not reachable."""
    db_url = get_database_url()
    try:
        config = parse_database_url(db_url)
    except ValueError as exc:
        print(
            f"Invalid RAPPEL_DATABASE_URL: {db_url}",
            file=sys.stderr,
        )
        print(
            f"Details: {exc}. Expected format: postgresql://USER:PASSWORD@HOST:PORT/DBNAME",
            file=sys.stderr,
        )
        sys.exit(1)

    ok, error = check_database_connection(config)
    if not ok:
        print(
            f"Database is not reachable at {config.host}:{config.port}/{config.dbname}.",
            file=sys.stderr,
        )
        if error:
            print(f"Details: {error}", file=sys.stderr)
        print(
            "Start Postgres with: docker compose up -d postgres",
            file=sys.stderr,
        )
        sys.exit(1)

    return config


def reset_database(config: DatabaseConfig) -> None:
    """Reset the database tables for clean benchmark runs."""
    env = os.environ.copy()
    env["PGPASSWORD"] = config.password

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
        config.host,
        "-p",
        str(config.port),
        "-U",
        config.user,
        "-d",
        config.dbname,
        "-c",
        f"TRUNCATE {', '.join(tables)} CASCADE;",
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Warning: Database reset failed: {result.stderr}", file=sys.stderr)


def fetch_completed_actions(
    config: DatabaseConfig, timeout: int = STATUS_QUERY_TIMEOUT_S
) -> int | None:
    """Query the database for completed action count using a dedicated connection."""
    env = os.environ.copy()
    env["PGPASSWORD"] = config.password

    cmd = [
        "psql",
        "-X",
        "-A",
        "-t",
        "-h",
        config.host,
        "-p",
        str(config.port),
        "-U",
        config.user,
        "-d",
        config.dbname,
        "-c",
        COMPLETED_ACTIONS_QUERY,
    ]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        return None

    if result.returncode != 0:
        return None

    try:
        return int(result.stdout.strip())
    except ValueError:
        return None


class BenchmarkStatusLogger:
    """Log periodic progress updates during benchmark runs."""

    def __init__(self, config: DatabaseConfig, interval_s: float) -> None:
        self._config = config
        self._interval_s = interval_s
        self._stop_event = Event()
        self._thread = Thread(target=self._run, name="benchmark-status", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=self._interval_s + STATUS_QUERY_TIMEOUT_S)

    def _run(self) -> None:
        start_time = time.monotonic()
        last_time = start_time
        last_completed: int | None = None

        while not self._stop_event.wait(self._interval_s):
            completed = fetch_completed_actions(self._config)
            if completed is None:
                continue
            now = time.monotonic()
            interval = max(0.001, now - last_time)
            if last_completed is None:
                throughput = 0.0
            else:
                throughput = max(0, completed - last_completed) / interval
            last_completed = completed
            last_time = now

            elapsed = now - start_time
            print(
                "benchmark status completed_actions=%s elapsed=%.1fs throughput=%.1f actions/s"
                % (completed, elapsed, throughput),
                file=sys.stderr,
            )


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


def run_benchmark(
    args: list[str],
    timeout: int = 300,
    db_config: DatabaseConfig | None = None,
    status_interval_s: float = STATUS_LOG_INTERVAL_S,
) -> BenchmarkResult | BenchmarkError:
    """Run the benchmark binary with --json flag and parse the JSON output."""
    cmd = ["./target/release/benchmark", "--json"] + args
    env = os.environ.copy()
    env["RAPPEL_DATABASE_URL"] = get_database_url()

    print(f"Running: {' '.join(cmd)}", file=sys.stderr)

    try:
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError:
        return BenchmarkError(error="binary_not_found")

    status_logger = None
    if db_config is not None and status_interval_s > 0:
        status_logger = BenchmarkStatusLogger(db_config, status_interval_s)
        status_logger.start()

    try:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            print(f"Benchmark timed out after {timeout}s", file=sys.stderr)
            return BenchmarkError(error="timeout")
    finally:
        if status_logger is not None:
            status_logger.stop()

    if process.returncode != 0:
        print(f"Benchmark failed with exit code {process.returncode}", file=sys.stderr)
        print(f"stderr: {stderr[-2000:]}", file=sys.stderr)
        return BenchmarkError(
            error="benchmark_failed",
            exit_code=process.returncode,
            stderr=stderr[-2000:] if stderr else None,
        )

    try:
        json_line = None
        for line in reversed(stdout.strip().split("\n")):
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
            stdout=stdout[-2000:] if stdout else None,
            stderr=stderr[-2000:] if stderr else None,
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
    type=click.Choice(["for-loop", "fan-out", "queue-noop"]),
    help="Benchmark type",
)
@click.option(
    "--loop-size",
    default=16,
    help="Number of actions to spawn (fan-out width / for-loop iterations)",
)
@click.option("--complexity", default=1000, help="CPU complexity per action (hash iterations)")
@click.option("--workers-per-host", default=4, help="Number of Python workers per host")
@click.option(
    "--max-slots-per-worker",
    default=10,
    help="Maximum concurrent actions per worker",
)
@click.option("--hosts", default=1, help="Number of hosts")
@click.option("--instances", default=1, help="Number of workflow instances")
def single(
    output: str | None,
    fmt: str,
    benchmark: str,
    loop_size: int,
    complexity: int,
    workers_per_host: int,
    max_slots_per_worker: int,
    hosts: int,
    instances: int,
):
    """Run a single benchmark."""
    if not check_benchmark_available():
        print("Benchmark binary not found. Run 'cargo build --release' first.", file=sys.stderr)
        sys.exit(1)

    db_config = ensure_database_available()

    print(f"=== Running {benchmark} Benchmark ===", file=sys.stderr)
    reset_database(db_config)

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
            "--max-slots-per-worker",
            str(max_slots_per_worker),
            "--hosts",
            str(hosts),
            "--instances",
            str(instances),
            "--log-interval",
            "0",
        ],
        db_config=db_config,
    )

    data = SingleData(benchmark=benchmark, result=result)
    formatter = FORMATTERS[fmt]
    formatted = formatter.format_single(data)

    if output:
        Path(output).write_text(formatted)
        print(f"Results written to {output}", file=sys.stderr)
    else:
        print(formatted)


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
    "--benchmarks",
    default="for-loop,fan-out,queue-noop",
    help="Comma-separated list of benchmark types",
)
@click.option(
    "--loop-size",
    default=16,
    help="Number of actions to spawn (fan-out width / for-loop iterations)",
)
@click.option("--complexity", default=1000, help="CPU complexity per action (hash iterations)")
@click.option("--workers-per-host", default=4, help="Number of Python workers per host")
@click.option(
    "--max-slots-per-worker",
    default=10,
    help="Maximum concurrent actions per worker",
)
@click.option("--hosts", default=1, help="Number of hosts")
@click.option("--instances", default=1, help="Number of workflow instances")
@click.option("--timeout", default=300, help="Timeout per benchmark run in seconds")
@click.option(
    "--benchmark-config",
    "benchmark_config_str",
    default=None,
    help="Per-benchmark config: 'bench:loop_size:complexity,...' e.g. 'for-loop:64:500,fan-out:32:200'",
)
def suite(
    output: str | None,
    fmt: str,
    benchmarks: str,
    loop_size: int,
    complexity: int,
    workers_per_host: int,
    max_slots_per_worker: int,
    hosts: int,
    instances: int,
    timeout: int,
    benchmark_config_str: str | None,
):
    """Run multiple benchmarks with the same configuration."""
    if not check_benchmark_available():
        print("Benchmark binary not found. Run 'cargo build --release' first.", file=sys.stderr)
        sys.exit(1)

    benchmark_types = [b.strip() for b in benchmarks.split(",") if b.strip()]
    allowed = {"for-loop", "fan-out", "queue-noop"}
    invalid = [b for b in benchmark_types if b not in allowed]
    if invalid:
        print(f"Unknown benchmarks: {', '.join(invalid)}", file=sys.stderr)
        sys.exit(1)

    benchmark_configs: dict[str, dict[str, int]] = {}
    if benchmark_config_str:
        benchmark_configs = parse_benchmark_config(benchmark_config_str)

    unknown_configs = [b for b in benchmark_configs if b not in allowed]
    if unknown_configs:
        print(f"Unknown benchmarks in config: {', '.join(unknown_configs)}", file=sys.stderr)
        sys.exit(1)

    for bench in benchmark_types:
        if bench not in benchmark_configs:
            benchmark_configs[bench] = {"loop_size": loop_size, "complexity": complexity}

    db_config = ensure_database_available()

    results: dict[str, BenchmarkResult | BenchmarkError] = {}
    for benchmark in benchmark_types:
        bench_cfg = benchmark_configs[benchmark]
        print(f"=== Running {benchmark} Benchmark ===", file=sys.stderr)
        reset_database(db_config)
        result = run_benchmark(
            [
                "--benchmark",
                benchmark,
                "--loop-size",
                str(bench_cfg["loop_size"]),
                "--complexity",
                str(bench_cfg["complexity"]),
                "--workers-per-host",
                str(workers_per_host),
                "--max-slots-per-worker",
                str(max_slots_per_worker),
                "--hosts",
                str(hosts),
                "--instances",
                str(instances),
                "--log-interval",
                "0",
            ],
            timeout=timeout,
            db_config=db_config,
        )
        results[benchmark] = result

    data = SuiteData(
        config={
            "benchmarks": benchmark_types,
            "loop_size": loop_size,
            "complexity": complexity,
            "workers_per_host": workers_per_host,
            "max_slots_per_worker": max_slots_per_worker,
            "hosts": hosts,
            "instances": instances,
            "timeout": timeout,
            "benchmark_configs": benchmark_configs,
        },
        results=results,
    )
    formatter = FORMATTERS[fmt]
    formatted = formatter.format_suite(data)

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
    "--max-slots-per-worker",
    default=10,
    help="Maximum concurrent actions per worker",
)
@click.option(
    "--benchmarks",
    default="for-loop,fan-out",
    help="Comma-separated list of benchmark types",
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
    max_slots_per_worker: int,
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

    db_config = ensure_database_available()

    total_runs = len(benchmark_types) * len(host_counts) * len(instance_counts)
    current_run = 0

    cells: list[GridCell] = []

    print("=== Running Benchmark Grid ===", file=sys.stderr)
    print(f"Benchmarks: {benchmark_types}", file=sys.stderr)
    print(f"Hosts: {host_counts}", file=sys.stderr)
    print(f"Instances: {instance_counts}", file=sys.stderr)
    print(f"Workers per host: {workers_per_host}", file=sys.stderr)
    print(f"Max slots per worker: {max_slots_per_worker}", file=sys.stderr)
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

                reset_database(db_config)

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
                        "--max-slots-per-worker",
                        str(max_slots_per_worker),
                        "--instances",
                        str(instance_count),
                        "--log-interval",
                        "0",
                        "--timeout",
                        str(timeout),
                    ],
                    timeout=timeout + 30,
                    db_config=db_config,
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
        "max_slots_per_worker": max_slots_per_worker,
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
    if len(sys.argv) > 1 and sys.argv[1] in ["single", "suite", "grid"]:
        cli()
    else:
        # Legacy: default to 'single' with old argument style
        sys.argv.insert(1, "single")
        cli()


if __name__ == "__main__":
    main()
