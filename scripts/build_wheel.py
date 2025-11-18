#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click>=8", "rich>=13"]
# ///
"""Build a distributable wheel that bundles Rust binaries and Python package."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

import click
from rich.console import Console

BIN_TARGETS = [
    "carabiner-server",
    "boot-carabiner-singleton",
]

console = Console()


def run(cmd: list[str], cwd: Path) -> None:
    console.log(f"[bold cyan]$ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


def copy_binaries(repo_root: Path, stage_dir: Path) -> list[Path]:
    target_dir = repo_root / "target" / "release"
    stage_dir.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    suffix = ".exe" if sys.platform == "win32" else ""
    for name in BIN_TARGETS:
        src = target_dir / f"{name}{suffix}"
        if not src.exists():
            raise FileNotFoundError(f"Missing compiled binary: {src}")
        dest = stage_dir / f"{name}{suffix}"
        shutil.copy2(src, dest)
        os.chmod(dest, 0o755)
        copied.append(dest)
    return copied


def cleanup_paths(paths: Iterable[Path], stage_dir: Path) -> None:
    for path in paths:
        if path.exists():
            path.unlink()
    if stage_dir.exists() and not any(stage_dir.iterdir()):
        stage_dir.rmdir()


@click.command()
@click.option(
    "--out-dir",
    default="target/wheels",
    show_default=True,
    help="Directory to write the built wheel into.",
)
def main(out_dir: str) -> None:
    """Build carabiner Python wheel with bundled binaries."""
    repo_root = Path(__file__).resolve().parents[1]
    out_path = (repo_root / out_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    console.log("[green]Building Rust binaries via cargo ...")
    run(["cargo", "build", "--release", "--bins"], cwd=repo_root)

    stage_dir = repo_root / "python" / "src" / "carabiner_worker" / "bin"
    console.log(f"[green]Staging binaries in {stage_dir} ...")
    staged = copy_binaries(repo_root, stage_dir)

    try:
        console.log("[green]Building Python wheel via uv ...")
        run(
            [
                "uv",
                "build",
                "--project",
                "python",
                "--wheel",
                "--out-dir",
                str(out_path),
            ],
            cwd=repo_root,
        )
        console.log(f"[bold green]Wheel written to {out_path}")
    finally:
        console.log("[green]Cleaning staged binaries ...")
        cleanup_paths(staged, stage_dir)


if __name__ == "__main__":
    main()
