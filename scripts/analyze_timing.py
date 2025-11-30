#!/usr/bin/env python3
"""
Analyzer for DB timing logs from the Rappel benchmark.

Parses JSON timing entries from benchmark output and generates statistics
to identify optimization opportunities.

Usage:
    python analyze_timing.py /tmp/benchmark_timing.log
"""

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TimingStats:
    """Statistics for a single timing metric."""
    values: list[float] = field(default_factory=list)

    def add(self, value: float) -> None:
        self.values.append(value)

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def total(self) -> float:
        return sum(self.values)

    @property
    def mean(self) -> float:
        return self.total / self.count if self.count > 0 else 0

    @property
    def min(self) -> float:
        return min(self.values) if self.values else 0

    @property
    def max(self) -> float:
        return max(self.values) if self.values else 0

    def percentile(self, p: float) -> float:
        if not self.values:
            return 0
        sorted_vals = sorted(self.values)
        idx = int(len(sorted_vals) * p / 100)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]


@dataclass
class FunctionStats:
    """Aggregated statistics for a function."""
    name: str
    call_count: int = 0
    total_time_ms: float = 0
    breakdown: dict[str, TimingStats] = field(default_factory=dict)

    def add_call(self, data: dict[str, Any]) -> None:
        self.call_count += 1
        for key, value in data.items():
            if key == "fn":
                continue
            if isinstance(value, (int, float)):
                if key not in self.breakdown:
                    self.breakdown[key] = TimingStats()
                self.breakdown[key].add(value)
                if key == "total_ms":
                    self.total_time_ms += value


def parse_timing_entries(log_file: str) -> list[dict[str, Any]]:
    """Extract JSON timing entries from log file."""
    entries = []
    # Pattern to match JSON in log lines with db_timing target
    json_pattern = re.compile(r'\{"fn":[^}]+\}')

    with open(log_file, 'r') as f:
        for line in f:
            if 'db_timing' not in line:
                continue
            match = json_pattern.search(line)
            if match:
                try:
                    entry = json.loads(match.group())
                    entries.append(entry)
                except json.JSONDecodeError:
                    continue
    return entries


def analyze_timing(entries: list[dict[str, Any]]) -> dict[str, FunctionStats]:
    """Aggregate timing entries by function."""
    stats: dict[str, FunctionStats] = {}

    for entry in entries:
        fn_name = entry.get("fn", "unknown")
        if fn_name not in stats:
            stats[fn_name] = FunctionStats(name=fn_name)
        stats[fn_name].add_call(entry)

    return stats


def print_summary(stats: dict[str, FunctionStats]) -> None:
    """Print a summary of timing statistics."""
    print("\n" + "=" * 80)
    print("DB TIMING ANALYSIS SUMMARY")
    print("=" * 80)

    # Sort by total time spent
    sorted_fns = sorted(stats.values(), key=lambda x: x.total_time_ms, reverse=True)

    for fn_stats in sorted_fns:
        print(f"\n{'='*60}")
        print(f"FUNCTION: {fn_stats.name}")
        print(f"{'='*60}")
        print(f"  Calls: {fn_stats.call_count:,}")
        print(f"  Total time: {fn_stats.total_time_ms:.2f}ms")
        print(f"  Avg per call: {fn_stats.total_time_ms / fn_stats.call_count:.3f}ms" if fn_stats.call_count > 0 else "")

        print("\n  Breakdown (all times in ms):")
        print(f"  {'Metric':<25} {'Mean':>10} {'P50':>10} {'P95':>10} {'Max':>10} {'Total':>12} {'% of fn':>10}")
        print(f"  {'-'*25} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12} {'-'*10}")

        # Sort breakdown by total time
        total_key = fn_stats.breakdown.get("total_ms")
        fn_total = total_key.total if total_key else fn_stats.total_time_ms

        breakdown_items = [
            (k, v) for k, v in fn_stats.breakdown.items()
            if k.endswith("_ms") and k != "total_ms"
        ]
        breakdown_items.sort(key=lambda x: x[1].total, reverse=True)

        for key, timing in breakdown_items:
            pct = (timing.total / fn_total * 100) if fn_total > 0 else 0
            print(f"  {key:<25} {timing.mean:>10.3f} {timing.percentile(50):>10.3f} {timing.percentile(95):>10.3f} {timing.max:>10.3f} {timing.total:>12.2f} {pct:>9.1f}%")

        # Show count-based metrics
        count_metrics = [
            (k, v) for k, v in fn_stats.breakdown.items()
            if not k.endswith("_ms") and k != "fn"
        ]
        if count_metrics:
            print("\n  Other metrics:")
            print(f"  {'Metric':<25} {'Mean':>10} {'P50':>10} {'P95':>10} {'Max':>10} {'Total':>12}")
            print(f"  {'-'*25} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")
            for key, timing in sorted(count_metrics, key=lambda x: x[1].total, reverse=True):
                print(f"  {key:<25} {timing.mean:>10.1f} {timing.percentile(50):>10.1f} {timing.percentile(95):>10.1f} {timing.max:>10.1f} {timing.total:>12.0f}")


def print_optimization_suggestions(stats: dict[str, FunctionStats]) -> None:
    """Print optimization suggestions based on the timing data."""
    print("\n" + "=" * 80)
    print("OPTIMIZATION OPPORTUNITIES")
    print("=" * 80)

    suggestions = []

    # Check process_loop_completion_tx
    if "process_loop_completion_tx" in stats:
        loop = stats["process_loop_completion_tx"]
        query_time = loop.breakdown.get("query_ms")
        multi_time = loop.breakdown.get("multi_action_ms")
        total_time = loop.breakdown.get("total_ms")

        if query_time and total_time and total_time.count > 0:
            query_pct = query_time.total / total_time.total * 100
            if query_pct > 20:
                suggestions.append({
                    "priority": "HIGH",
                    "area": "process_loop_completion_tx -> query_ms",
                    "observation": f"Initial query takes {query_pct:.1f}% of loop processing time ({query_time.mean:.2f}ms avg)",
                    "suggestion": "Batch the initial SELECT+JOIN query across multiple records using UNNEST"
                })

        if multi_time and total_time and total_time.count > 0:
            multi_pct = multi_time.total / total_time.total * 100
            if multi_pct > 50:
                suggestions.append({
                    "priority": "HIGH",
                    "area": "process_loop_completion_tx -> multi_action_ms",
                    "observation": f"Multi-action processing takes {multi_pct:.1f}% ({multi_time.mean:.2f}ms avg)",
                    "suggestion": "Investigate process_multi_action_loop_completion_tx for batching opportunities"
                })

    # Check mark_actions_batch
    if "mark_actions_batch" in stats:
        batch = stats["mark_actions_batch"]
        schedule_time = batch.breakdown.get("schedule_ms")
        total_time = batch.breakdown.get("total_ms")

        if schedule_time and total_time:
            schedule_pct = schedule_time.total / total_time.total * 100
            if schedule_pct > 50:
                suggestions.append({
                    "priority": "HIGH",
                    "area": "mark_actions_batch -> schedule_ms",
                    "observation": f"Scheduling takes {schedule_pct:.1f}% of batch processing time",
                    "suggestion": "Consider batching schedule_workflow_instance_tx calls or caching more aggressively"
                })

        tx_acquire = batch.breakdown.get("tx_acquire_ms")
        if tx_acquire and tx_acquire.percentile(95) > 5:
            suggestions.append({
                "priority": "MEDIUM",
                "area": "mark_actions_batch -> tx_acquire_ms",
                "observation": f"P95 transaction acquisition is {tx_acquire.percentile(95):.2f}ms",
                "suggestion": "Consider increasing connection pool size or connection reuse"
            })

    # Check schedule_workflow_instance_tx
    if "schedule_workflow_instance_tx" in stats:
        sched = stats["schedule_workflow_instance_tx"]

        instance_lookup = sched.breakdown.get("instance_lookup_ms")
        if instance_lookup and instance_lookup.mean > 0.5:
            suggestions.append({
                "priority": "MEDIUM",
                "area": "schedule_workflow_instance_tx -> instance_lookup_ms",
                "observation": f"Average instance lookup is {instance_lookup.mean:.2f}ms",
                "suggestion": "Ensure workflow_instances has proper index on (id) and consider FOR UPDATE SKIP LOCKED"
            })

        state_load = sched.breakdown.get("state_load_ms")
        if state_load and state_load.mean > 0.5:
            suggestions.append({
                "priority": "MEDIUM",
                "area": "schedule_workflow_instance_tx -> state_load_ms",
                "observation": f"Average state load is {state_load.mean:.2f}ms",
                "suggestion": "Ensure daemon_action_ledger has index on (instance_id) for state loading"
            })

        dag = sched.breakdown.get("dag_ms")
        if dag and dag.mean > 1.0:
            suggestions.append({
                "priority": "HIGH",
                "area": "schedule_workflow_instance_tx -> dag_ms",
                "observation": f"DAG processing averages {dag.mean:.2f}ms (total: {dag.total:.1f}ms)",
                "suggestion": "This includes DB writes for new actions. Consider bulk INSERT for multiple actions"
            })

    # Check dispatch_actions
    if "dispatch_actions" in stats:
        dispatch = stats["dispatch_actions"]
        total = dispatch.breakdown.get("total_ms")
        returned = dispatch.breakdown.get("returned")

        if total and returned:
            empty_calls = sum(1 for v in returned.values if v == 0)
            empty_pct = empty_calls / returned.count * 100
            if empty_pct > 50:
                suggestions.append({
                    "priority": "LOW",
                    "area": "dispatch_actions",
                    "observation": f"{empty_pct:.1f}% of dispatch calls return 0 actions",
                    "suggestion": "Consider using LISTEN/NOTIFY instead of polling when queue is empty"
                })

    # Print suggestions
    for i, s in enumerate(sorted(suggestions, key=lambda x: {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[x["priority"]]), 1):
        print(f"\n{i}. [{s['priority']}] {s['area']}")
        print(f"   Observation: {s['observation']}")
        print(f"   Suggestion: {s['suggestion']}")

    if not suggestions:
        print("\nNo obvious optimization opportunities identified from timing data.")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_timing.py <log_file>")
        sys.exit(1)

    log_file = sys.argv[1]
    print(f"Analyzing timing data from: {log_file}")

    entries = parse_timing_entries(log_file)
    print(f"Found {len(entries)} timing entries")

    if not entries:
        print("No timing entries found. Make sure to run with RUST_LOG=db_timing=debug")
        sys.exit(1)

    stats = analyze_timing(entries)
    print_summary(stats)
    print_optimization_suggestions(stats)


if __name__ == "__main__":
    main()
