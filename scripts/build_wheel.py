#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click>=8", "rich>=13"]
# ///
"""Build a distributable wheel that bundles Rust binaries and Python package."""

import base64
import hashlib
import os
import shutil
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import click
from rich.console import Console


@dataclass(frozen=True)
class EntryPoint:
    built_name: str
    packaged_name: str


ENTRYPOINTS: Sequence[EntryPoint] = (
    EntryPoint("carabiner-server", "rappel-server"),
    EntryPoint("boot-carabiner-singleton", "boot-rappel-singleton"),
    EntryPoint("start_workers", "start_workers"),
)

SCRIPT_ALIASES: dict[str, tuple[str, ...]] = {
    "start_workers": ("start-workers",),
}

console = Console()


def run(cmd: list[str], cwd: Path) -> None:
    console.log(f"[bold cyan]$ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


def copy_binaries(repo_root: Path, stage_dir: Path) -> list[Path]:
    target_dir = repo_root / "target" / "release"
    stage_dir.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    suffix = ".exe" if sys.platform == "win32" else ""
    for entry in ENTRYPOINTS:
        src = target_dir / f"{entry.built_name}{suffix}"
        if not src.exists():
            raise FileNotFoundError(f"Missing compiled binary: {src}")
        dest = stage_dir / f"{entry.packaged_name}{suffix}"
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


def _wheel_files(out_dir: Path) -> list[Path]:
    wheels = sorted(out_dir.glob("*.whl"))
    if not wheels:
        raise FileNotFoundError(f"No wheels found in {out_dir}")
    return wheels


def _dist_info_prefix(archive: zipfile.ZipFile) -> str:
    for name in archive.namelist():
        if name.endswith(".dist-info/WHEEL"):
            return name.split(".dist-info/")[0]
    raise RuntimeError("unable to locate .dist-info directory in wheel")


def install_scripts_in_wheel(out_dir: Path, stage_dir: Path) -> None:
    wheels = _wheel_files(out_dir)
    suffix = ".exe" if sys.platform == "win32" else ""
    for wheel in wheels:
        with zipfile.ZipFile(wheel, mode="a") as archive:
            prefix = _dist_info_prefix(archive)
            scripts_dir = f"{prefix}.data/scripts"
            record_path = f"{prefix}.dist-info/RECORD"
            existing = set(archive.namelist())
            new_entries: list[tuple[str, bytes]] = []
            for binary in stage_dir.iterdir():
                if not binary.is_file():
                    continue
                binary_name = binary.name
                base_name = (
                    binary_name[: -len(suffix)]
                    if suffix and binary_name.endswith(suffix)
                    else binary_name
                )
                alias_names = SCRIPT_ALIASES.get(base_name, ())
                script_names = [binary_name, *[f"{alias}{suffix}" for alias in alias_names]]
                data = binary.read_bytes()
                for script_name in script_names:
                    target_name = f"{scripts_dir}/{script_name}"
                    if target_name in existing:
                        continue
                    info = zipfile.ZipInfo(target_name)
                    info.external_attr = 0o755 << 16
                    archive.writestr(info, data)
                    existing.add(target_name)
                    new_entries.append((target_name, data))
            if new_entries:
                record_data = archive.read(record_path).decode("utf-8")
                record_lines = [line for line in record_data.splitlines() if line]
                for filename, payload in new_entries:
                    digest = (
                        base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
                        .decode("ascii")
                        .rstrip("=")
                    )
                    record_lines.append(f"{filename},sha256={digest},{len(payload)}")
                archive.writestr(record_path, "\n".join(record_lines) + "\n")


def assert_entrypoints_in_wheel(out_dir: Path) -> None:
    wheels = _wheel_files(out_dir)
    suffix = ".exe" if sys.platform == "win32" else ""
    package_expected = {f"rappel/bin/{entry.packaged_name}{suffix}" for entry in ENTRYPOINTS}
    for wheel in wheels:
        with zipfile.ZipFile(wheel) as archive:
            contents = set(archive.namelist())
            prefix = _dist_info_prefix(archive)
            scripts_dir = f"{prefix}.data/scripts"
            script_expected: set[str] = set()
            for entry in ENTRYPOINTS:
                base = entry.packaged_name
                names = [entry.packaged_name, *SCRIPT_ALIASES.get(base, ())]
                for name in names:
                    script_expected.add(f"{scripts_dir}/{name}{suffix}")
        missing = sorted((package_expected | script_expected) - contents)
        if missing:
            raise RuntimeError(
                f"{wheel.name} is missing required entrypoints: {', '.join(missing)}"
            )


@click.command()
@click.option(
    "--out-dir",
    default="target/wheels",
    show_default=True,
    help="Directory to write the built wheel into.",
)
def main(out_dir: str) -> None:
    """Build rappel Python wheel with bundled binaries."""
    repo_root = Path(__file__).resolve().parents[1]
    out_path = (repo_root / out_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    console.log("[green]Building Rust binaries via cargo ...")
    run(["cargo", "build", "--release", "--bins"], cwd=repo_root)

    stage_dir = repo_root / "python" / "src" / "rappel" / "bin"
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
        console.log("[green]Injecting binary scripts into wheel ...")
        install_scripts_in_wheel(out_path, stage_dir)
        assert_entrypoints_in_wheel(out_path)
        console.log(f"[bold green]Wheel written to {out_path}")
    finally:
        console.log("[green]Cleaning staged binaries ...")
        cleanup_paths(staged, stage_dir)


if __name__ == "__main__":
    main()
