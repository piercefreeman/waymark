#!/usr/bin/env python3

"""Bulk rename project identifiers across tracked files and paths."""

from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path

GENERATED_PROTO_SUFFIXES = ("_pb2.py", "_pb2.pyi", "_pb2_grpc.py")


@dataclass(frozen=True)
class Replacement:
    source: str
    target: str


def run_git_ls_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    )
    raw_paths = [entry for entry in result.stdout.decode("utf-8").split("\0") if entry]
    return [repo_root / entry for entry in raw_paths]


def apply_replacements(value: str, replacements: list[Replacement]) -> str:
    output = value
    for replacement in replacements:
        output = output.replace(replacement.source, replacement.target)
    return output


def should_skip_content_edit(path: Path) -> bool:
    if path.name.endswith(GENERATED_PROTO_SUFFIXES):
        return True
    return False


def replace_contents(path: Path, replacements: list[Replacement]) -> bool:
    if should_skip_content_edit(path):
        return False

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return False

    if b"\0" in raw:
        return False

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return False

    updated = apply_replacements(text, replacements)
    if updated == text:
        return False

    path.write_text(updated, encoding="utf-8")
    return True


def move_files(repo_root: Path, tracked_files: list[Path], replacements: list[Replacement]) -> int:
    renames: list[tuple[Path, Path]] = []
    for path in tracked_files:
        relative = path.relative_to(repo_root)
        updated_relative = Path(apply_replacements(relative.as_posix(), replacements))
        if updated_relative == relative:
            continue
        renames.append((path, repo_root / updated_relative))

    # Rename deepest paths first so parent-directory changes do not conflict.
    renames.sort(key=lambda entry: len(entry[0].parts), reverse=True)

    moved_count = 0
    for src, dst in renames:
        if src == dst or not src.exists():
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)
        moved_count += 1

    return moved_count


def remove_empty_legacy_dirs(repo_root: Path, old_name: str) -> int:
    removed = 0
    directories = sorted(
        [path for path in repo_root.rglob("*") if path.is_dir() and ".git" not in path.parts],
        key=lambda value: len(value.parts),
        reverse=True,
    )
    for directory in directories:
        if old_name not in directory.name:
            continue
        try:
            directory.rmdir()
            removed += 1
        except OSError:
            continue
    return removed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rename project identifiers across tracked files.")
    parser.add_argument("--old", required=True, help="Old project name")
    parser.add_argument("--new", required=True, help="New project name")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (default: parent of this script)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.resolve()

    replacements = [
        Replacement(args.old, args.new),
        Replacement(args.old.capitalize(), args.new.capitalize()),
        Replacement(args.old.upper(), args.new.upper()),
    ]

    tracked_files = run_git_ls_files(repo_root)

    updated_files = 0
    for path in tracked_files:
        if replace_contents(path, replacements):
            updated_files += 1

    moved_files = move_files(repo_root, tracked_files, replacements)
    removed_dirs = remove_empty_legacy_dirs(repo_root, args.old)

    print(f"Updated file contents: {updated_files}")
    print(f"Moved tracked files: {moved_files}")
    print(f"Removed empty legacy dirs: {removed_dirs}")


if __name__ == "__main__":
    main()
