# Benchmarks

Rappel includes a benchmark suite for measuring throughput and latency under various configurations. The benchmarks test the full execution path: DAG registration, action dispatch, Python worker execution, and completion handling.

## Benchmark Types

### for-loop

A sequential fan-out pattern using a blocking for loop. Each iteration dispatches an action that performs CPU-bound hash computations. Tests sequential processing with conditional branching and loop state management.

```
workflow_input -> for i in range(count) -> hash_action(i) -> aggregate -> output
```

### fan-out

Pure parallel fan-out where all actions run concurrently. Tests maximum action dispatch parallelism and aggregator synchronization.

```
workflow_input -> spread(count) -> hash_action[0..N] -> barrier -> output
```

## Running Benchmarks

Build the benchmark binary first:

```bash
cargo build --release
```

### Single Run

Run a single benchmark configuration:

```bash
uv run scripts/run_benchmarks.py single -f text
uv run scripts/run_benchmarks.py single -b fan-out -f json -o results.json
```

Options:
- `-b, --benchmark`: Benchmark type (`for-loop` or `fan-out`)
- `-f, --format`: Output format (`text`, `json`, `markdown`, `csv`)
- `-o, --output`: Output file (stdout if not specified)
- `--count`: Number of parallel hash computations per workflow
- `--iterations`: Hash iterations per action (CPU intensity)
- `--hosts`: Number of simulated hosts
- `--instances`: Number of concurrent workflow instances
- `--workers-per-host`: Python workers per host

### Grid Run

Run a matrix of configurations to analyze scaling behavior:

```bash
uv run scripts/run_benchmarks.py grid -f text
uv run scripts/run_benchmarks.py grid --hosts "1,2,4" --instances "1,2,4,8" -f csv -o grid.csv
```

The grid command runs all combinations of hosts × instances × benchmark types and produces a summary with scaling analysis.

## Output Formats

### text

Human-readable tables with a scaling analysis section:

```
Benchmark Grid Results
======================================================================

[for-loop]
----------------------------------------------------------------------
 Hosts   Inst  Workers    Actions/s   P95 (ms)   Avg (ms)
----------------------------------------------------------------------
     1      1        4        123.4       45.6       32.1
     1      2        4        234.5       48.2       35.4
...

Scaling Analysis
----------------------------------------------------------------------
for-loop:
  Baseline: 123.4 actions/s @ 4 workers
  Best:     456.7 actions/s @ 16 workers (4h x 4i)
  Speedup:  3.70x
  Efficiency: 92.5% (throughput/worker vs baseline)
```

### json

Machine-readable format for programmatic consumption:

```json
{
  "_meta": {
    "mode": "grid",
    "config": { ... }
  },
  "grid": [
    {
      "benchmark": "for-loop",
      "hosts": 1,
      "instances": 1,
      "total_workers": 4,
      "result": {
        "throughput": 123.4,
        "p95_round_trip_ms": 45.6,
        ...
      }
    }
  ]
}
```

### markdown

GitHub-flavored markdown tables for documentation or PR comments.

### csv

Spreadsheet-importable format for external analysis tools.

## Metrics

Each benchmark run reports:

- **total**: Total actions completed
- **elapsed_s**: Wall-clock time for the full run
- **throughput**: Actions completed per second
- **avg_round_trip_ms**: Mean time from dispatch to completion
- **p95_round_trip_ms**: 95th percentile round-trip latency

## Scaling Dimensions

The grid benchmark varies two orthogonal dimensions:

**Hosts**: Number of independent DAG runners, each with its own worker pool. Simulates horizontal scaling across machines. Each host maintains separate database connections and worker gRPC channels.

**Instances**: Number of concurrent workflow instances per configuration. Tests the runner's ability to interleave work from multiple independent workflows.

**Workers per host**: Fixed multiplier determining total worker count (`hosts × workers_per_host`). More workers increase action parallelism but add gRPC overhead.

## Interpreting Results

**Throughput scaling**: Ideal scaling shows throughput increasing linearly with workers. Sub-linear scaling indicates contention (database locks, gRPC serialization, or Python GIL effects in workers).

**Latency stability**: P95 latency should remain stable as load increases. Rising P95 under load suggests queue buildup or resource exhaustion.

**Efficiency**: The ratio of per-worker throughput at scale vs baseline. 100% means perfect scaling; lower values indicate overhead from coordination.

**for-loop vs fan-out**: The for-loop benchmark is inherently sequential within each instance, so scaling comes from running multiple instances. Fan-out can parallelize within a single instance, making it more sensitive to worker count.
