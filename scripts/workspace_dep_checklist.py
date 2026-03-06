#!/usr/bin/env -S uv run --script
"""Generate a topologically sorted markdown checklist for workspace crates."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from heapq import heappop, heappush


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a topologically sorted markdown checklist of workspace crates."
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output markdown file path. Defaults to stdout.",
    )
    parser.add_argument(
        "--with-deps",
        action="store_true",
        help="Include direct workspace dependencies for each crate.",
    )
    return parser.parse_args()


def load_cargo_metadata() -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def build_workspace_graph(
    metadata: dict,
) -> tuple[dict[str, str], dict[str, set[str]], list[str]]:
    pkg_by_id = {package["id"]: package for package in metadata["packages"]}
    workspace_members = set(metadata["workspace_members"])
    resolve_nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}

    package_names = {
        package_id: str(pkg_by_id[package_id]["name"]) for package_id in workspace_members
    }

    package_to_deps: dict[str, set[str]] = defaultdict(set)
    for package_id in workspace_members:
        deps = resolve_nodes.get(package_id, {}).get("deps", [])
        for dep in deps:
            dep_id = dep["pkg"]
            if dep_id in workspace_members:
                package_to_deps[package_id].add(dep_id)

    dep_to_packages: dict[str, set[str]] = defaultdict(set)
    indegree: dict[str, int] = {package_id: 0 for package_id in workspace_members}
    for package_id, deps in package_to_deps.items():
        for dep_id in deps:
            dep_to_packages[dep_id].add(package_id)
            indegree[package_id] += 1

    def package_name(package_id: str) -> str:
        return package_names[package_id]

    heap: list[tuple[str, str]] = []
    for package_id, degree in indegree.items():
        if degree == 0:
            heappush(heap, (package_name(package_id), package_id))

    order: list[str] = []
    while heap:
        _, package_id = heappop(heap)
        order.append(package_id)

        for dependent in sorted(dep_to_packages[package_id], key=package_name):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                heappush(heap, (package_name(dependent), dependent))

    if len(order) != len(workspace_members):
        raise ValueError("Cycle detected in workspace dependency graph")

    return package_names, package_to_deps, order


def render_markdown(
    package_names: dict[str, str],
    package_to_deps: dict[str, set[str]],
    order: list[str],
    *,
    with_deps: bool,
) -> str:
    lines = ["# Workspace crates (topological)", ""]

    for package_id in order:
        package_name = package_names[package_id]
        if not with_deps:
            lines.append(f"- [ ] {package_name}")
            continue

        dep_names = sorted(
            package_names[dep_id] for dep_id in package_to_deps.get(package_id, set())
        )
        if dep_names:
            dep_text = ", ".join(dep_names)
            lines.append(f"- [ ] {package_name} _(depends on: {dep_text})_")
        else:
            lines.append(f"- [ ] {package_name} _(depends on: none)_")

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()

    try:
        metadata = load_cargo_metadata()
        package_names, package_to_deps, order = build_workspace_graph(metadata)
    except subprocess.CalledProcessError as error:
        print(error.stderr or str(error), file=sys.stderr)
        return 1
    except (KeyError, ValueError, json.JSONDecodeError) as error:
        print(f"Failed to generate checklist: {error}", file=sys.stderr)
        return 1

    markdown = render_markdown(
        package_names,
        package_to_deps,
        order,
        with_deps=args.with_deps,
    )
    if args.output:
        with open(args.output, "w", encoding="utf-8") as output_file:
            output_file.write(markdown)
    else:
        print(markdown, end="")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
