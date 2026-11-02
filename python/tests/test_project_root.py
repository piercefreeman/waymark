"""Manifest-based discovery for shared Python test configuration."""

from pathlib import Path

from conftest import find_project_root


def test_project_root_uses_the_nearest_manifest(tmp_path: Path) -> None:
    """Regression: nested test directories must not require a fixed parent depth."""
    root = tmp_path / "workspace"
    nested = root / "python" / "tests"
    nested.mkdir(parents=True)
    (root / "pyproject.toml").touch()
    assert find_project_root(nested) == root
    (nested.parent / "pyproject.toml").touch()
    assert find_project_root(nested) == nested.parent
