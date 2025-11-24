#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["packaging>=23", "toml>=0.10"]
# ///
"""Synchronize Cargo and Python versions for tagged releases."""

import sys
from pathlib import Path

import toml
from packaging.version import parse


def find_repo_root() -> Path:
    """Walk upward from this file until we find a Cargo.toml."""
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "Cargo.toml").exists():
            return candidate
    raise FileNotFoundError("Cargo.toml not found")


REPO_ROOT = find_repo_root()


def update_cargo_version(new_version: str) -> None:
    cargo_path = REPO_ROOT / "Cargo.toml"
    if not cargo_path.exists():
        raise FileNotFoundError("Cargo.toml not found")

    print(f"Updating Cargo.toml at {cargo_path}")

    filedata = toml.loads(cargo_path.read_text())
    if "package" not in filedata:
        raise ValueError("Cargo.toml missing [package] section")

    filedata["package"]["version"] = format_cargo_version(new_version)
    cargo_path.write_text(toml.dumps(filedata))


def format_cargo_version(new_version: str) -> str:
    parsed = parse(new_version)
    cargo_version = f"{parsed.major}.{parsed.minor}.{parsed.micro}"

    if parsed.is_prerelease and parsed.pre is not None:
        pre_release = "".join(str(part) for part in parsed.pre if part is not None)
        cargo_version += f"-{pre_release}"
    if parsed.is_postrelease and parsed.post is not None:
        cargo_version += f"-post{parsed.post}"
    if parsed.is_devrelease and parsed.dev is not None:
        cargo_version += f"-dev{parsed.dev}"

    return cargo_version


def update_python_version(new_version: str) -> None:
    pyproject_path = REPO_ROOT / "python" / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError("python/pyproject.toml not found")

    filedata = toml.loads(pyproject_path.read_text())
    project = filedata.get("project")
    if project is None:
        raise ValueError("pyproject.toml missing [project] section")

    project["version"] = format_python_version(new_version)
    pyproject_path.write_text(toml.dumps(filedata))


def format_python_version(new_version: str) -> str:
    parsed = parse(new_version)
    python_version = f"{parsed.major}.{parsed.minor}.{parsed.micro}"

    if parsed.is_prerelease and parsed.pre is not None:
        tag = parsed.pre[0]
        number = parsed.pre[1]
        if tag and tag.startswith("dev"):
            python_version += f".dev{number}"
        elif tag == "rc":
            python_version += f"rc{number}"
        elif tag == "a":
            python_version += f"a{number}"
        elif tag == "b":
            python_version += f"b{number}"
        else:
            python_version += f"{tag}{number}"
    if parsed.is_postrelease and parsed.post is not None:
        python_version += f".post{parsed.post}"
    if parsed.is_devrelease and parsed.dev is not None:
        python_version += f".dev{parsed.dev}"

    return python_version


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: update_version.py <new_version>")

    raw_version = sys.argv[1].lstrip("v")
    update_cargo_version(raw_version)
    update_python_version(raw_version)
    print(f"Updated project version to {raw_version}")


if __name__ == "__main__":
    main()
