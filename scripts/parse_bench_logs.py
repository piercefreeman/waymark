#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click>=8", "rich>=13"]
# ///
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import IO, Iterable, List, TextIO

import click
from rich.console import Console

ANSI_RE = re.compile(r"\x1B\[[0-9;]*[A-Za-z]")
SPAN_PREFIX = re.compile(r"^(?P<ts>\S+)\s+\S+\s+(?P<msg>.*)$")
PROGRESS_RE = re.compile(
    r"benchmark progress processed=(?P<processed>\d+) total=(?P<total>\d+) elapsed=(?P<elapsed>[^ ]+) throughput=(?P<throughput>[^ ]+) msg/s in_flight=(?P<inflight>\d+) worker_count=(?P<workers>\d+) db_queue=(?P<queue>\d+)"
)
ACTION_RE = re.compile(
    r"action completed action_id=(?P<action>\d+) round_trip_ms=(?P<rt>[^ ]+) ack_ms=(?P<ack>[^ ]+) worker_ms=(?P<worker>[^ ]+)"
)
console = Console()


@dataclass
class Progress:
    ts: str
    processed: int
    total: int
    elapsed: str
    throughput: float
    in_flight: int
    workers: int
    queue: int


@dataclass
class ActionMetric:
    ts: str
    action_id: int
    round_trip_ms: float
    ack_ms: float
    worker_ms: float


def parse_lines(lines: Iterable[str]) -> tuple[List[Progress], List[ActionMetric]]:
    progress: List[Progress] = []
    actions: List[ActionMetric] = []
    for line in lines:
        line = ANSI_RE.sub("", line).strip()
        if not line:
            continue
        sp = SPAN_PREFIX.match(line)
        if not sp:
            continue
        ts, msg = sp.group("ts"), sp.group("msg")
        if m := PROGRESS_RE.search(msg):
            progress.append(
                Progress(
                    ts,
                    int(m.group("processed")),
                    int(m.group("total")),
                    m.group("elapsed"),
                    float(m.group("throughput")),
                    int(m.group("inflight")),
                    int(m.group("workers")),
                    int(m.group("queue")),
                )
            )
            continue
        if m := ACTION_RE.search(msg):
            actions.append(
                ActionMetric(
                    ts,
                    int(m.group("action")),
                    float(m.group("rt")),
                    float(m.group("ack")),
                    float(m.group("worker")),
                )
            )
    return progress, actions


def summarize(progress: List[Progress], actions: List[ActionMetric], out: IO[str]) -> None:
    print(f"progress points: {len(progress)}", file=out)
    for p in progress:
        print(
            f"[{p.ts}] processed={p.processed}/{p.total} throughput={p.throughput:.0f} msg/s inflight={p.in_flight} workers={p.workers} queue={p.queue}",
            file=out,
        )
    print(f"actions logged: {len(actions)}", file=out)
    if actions:
        avg_rt = sum(a.round_trip_ms for a in actions) / len(actions)
        avg_ack = sum(a.ack_ms for a in actions) / len(actions)
        avg_worker = sum(a.worker_ms for a in actions) / len(actions)
        print(
            f"avg round_trip={avg_rt:.3f} ms ack={avg_ack:.3f} ms worker={avg_worker:.3f} ms",
            file=out,
        )


@click.command()
@click.argument("logfile", type=click.File("r"), default="-")
@click.option("--json-output", is_flag=True, help="Emit JSON instead of human text.")
def main(logfile: TextIO, json_output: bool) -> None:
    progress, actions = parse_lines(logfile)

    if json_output:
        console.print_json(
            data={
                "progress": [p.__dict__ for p in progress],
                "actions": [a.__dict__ for a in actions],
            }
        )
    else:
        summarize(progress, actions, console.file)


if __name__ == "__main__":
    main()
