#!/usr/bin/env python3
"""Parse tracing-chrome JSON and print a simple text breakdown."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass
class Frame:
    name: str
    start_us: float
    child_us: float = 0.0


def load_trace(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict) and isinstance(data.get("traceEvents"), list):
        return [e for e in data["traceEvents"] if isinstance(e, dict)]
    if isinstance(data, list):
        return [e for e in data if isinstance(e, dict)]
    raise ValueError("unexpected trace format")


def collect_events(
    events: List[Dict[str, Any]],
) -> Dict[Tuple[int, int], List[Tuple[float, int, str]]]:
    grouped: Dict[Tuple[int, int], List[Tuple[float, int, str]]] = {}
    for event in events:
        ph = event.get("ph")
        name = event.get("name")
        ts = event.get("ts")
        if not isinstance(name, str) or not isinstance(ts, (int, float)):
            continue
        if ph not in {"B", "E", "X", "b", "e"}:
            continue
        pid = event.get("pid")
        tid = event.get("tid")
        if not isinstance(pid, int) or not isinstance(tid, int):
            continue
        key = (pid, tid)
        bucket = grouped.setdefault(key, [])
        ts_us = float(ts)
        if ph == "X":
            dur = event.get("dur")
            if not isinstance(dur, (int, float)):
                continue
            dur_us = float(dur)
            bucket.append((ts_us, 0, name))
            bucket.append((ts_us + dur_us, 1, name))
        elif ph in {"B", "b"}:
            bucket.append((ts_us, 0, name))
        elif ph in {"E", "e"}:
            bucket.append((ts_us, 1, name))
    return grouped


def compute_totals(
    grouped: Dict[Tuple[int, int], List[Tuple[float, int, str]]],
) -> Tuple[Dict[str, float], Dict[str, float], float]:
    inclusive: Dict[str, float] = {}
    self_time: Dict[str, float] = {}
    total_span = 0.0

    for _, events in grouped.items():
        events.sort(key=lambda item: (item[0], item[1]))
        stack: List[Frame] = []
        for ts_us, kind, name in events:
            if kind == 0:  # begin
                stack.append(Frame(name=name, start_us=ts_us))
                continue
            if not stack:
                continue
            frame = stack.pop()
            duration = max(0.0, ts_us - frame.start_us)
            total_span += duration
            inclusive[frame.name] = inclusive.get(frame.name, 0.0) + duration
            self_duration = max(0.0, duration - frame.child_us)
            self_time[frame.name] = self_time.get(frame.name, 0.0) + self_duration
            if stack:
                stack[-1].child_us += duration
    return inclusive, self_time, total_span


def format_duration(us: float) -> str:
    ms = us / 1000.0
    if ms < 1.0:
        return f"{ms * 1000.0:.2f} µs"
    if ms < 1000.0:
        return f"{ms:.2f} ms"
    return f"{ms / 1000.0:.2f} s"


def print_top(title: str, data: Dict[str, float], total: float, top: int) -> None:
    print(title)
    if not data:
        print("  (no spans)")
        return
    items = sorted(data.items(), key=lambda item: item[1], reverse=True)
    for idx, (name, duration) in enumerate(items[:top], start=1):
        percent = (duration / total * 100.0) if total > 0 else 0.0
        print(f"  {idx:>2}. {name}  {percent:5.1f}%  {format_duration(duration)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse a tracing-chrome JSON into a text summary.")
    parser.add_argument("trace", help="Path to tracing-chrome JSON")
    parser.add_argument("--top", type=int, default=30, help="Number of entries to show")
    args = parser.parse_args()

    try:
        events = load_trace(args.trace)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Failed to load trace: {exc}", file=sys.stderr)
        return 1

    grouped = collect_events(events)
    inclusive, self_time, total_span = compute_totals(grouped)

    print("Tracing chrome summary")
    print(f"Threads: {len(grouped)}")
    print(f"Total span time: {format_duration(total_span)}")
    print()
    print_top("Top inclusive:", inclusive, total_span, args.top)
    print()
    print_top("Top self:", self_time, total_span, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
