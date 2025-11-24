#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click>=8", "rich>=13"]
# ///
"""Collect per-platform wheel artifacts into a structured directory."""

import shutil
from pathlib import Path
from typing import Iterable

import click
from rich.console import Console

console = Console()


class WheelSource(click.ParamType):
    name = "label=path"

    def convert(self, value: str, param, ctx):  # type: ignore[override]
        if "=" not in value:
            self.fail("expected LABEL=PATH syntax", param, ctx)
        label, path = value.split("=", 1)
        label = label.strip().lower()
        if not label:
            self.fail("label cannot be empty", param, ctx)
        allowed = set("abcdefghijklmnopqrstuvwxyz0123456789-_")
        if any(ch not in allowed for ch in label):
            self.fail("label must be alphanumeric with '-' or '_'", param, ctx)
        return label, Path(path).expanduser()


def _discover_wheels(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.exists():
        raise FileNotFoundError(f"wheel source not found: {path}")
    return sorted(p for p in path.rglob("*.whl") if p.is_file())


@click.command()
@click.option("--output", "output_dir", default="target/release-wheels", show_default=True)
@click.argument("sources", nargs=-1, type=WheelSource())
def main(output_dir: str, sources: Iterable[tuple[str, Path]]) -> None:
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if not sources:
        raise click.UsageError("provide at least one LABEL=PATH source")

    for label, source_path in sources:
        wheels = _discover_wheels(source_path)
        if not wheels:
            raise click.ClickException(f"no wheels found under {source_path}")
        dest_dir = output / label
        dest_dir.mkdir(parents=True, exist_ok=True)
        console.log(f"[green]Collecting {len(wheels)} wheel(s) into {dest_dir}")
        for wheel in wheels:
            destination = dest_dir / wheel.name
            shutil.copy2(wheel, destination)


if __name__ == "__main__":
    main()
