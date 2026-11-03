import sys
from pathlib import Path

import pytest


def find_project_root(start: Path) -> Path:
    """Find the nearest Python project manifest above a test or configuration directory."""
    for directory in (start, *start.parents):
        if (directory / "pyproject.toml").is_file():
            return directory
    raise FileNotFoundError(f"No pyproject.toml found above {start}")


PROJECT_PY_DIR = find_project_root(Path(__file__).resolve().parent)
SRC_DIR = PROJECT_PY_DIR / "src"

for path in (PROJECT_PY_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


@pytest.fixture(autouse=True)
def reset_registries():
    """Reset action and workflow registries before each test.

    This prevents conflicts when multiple test fixtures use the same
    action/workflow names (e.g., 'process', 'get_value').
    """
    from waymark.registry import registry as action_registry
    from waymark.workflow import workflow_registry

    # Clear registries before test
    action_registry.reset()
    workflow_registry.reset()

    # Clear cached fixture modules so they re-register on import
    modules_to_remove = [key for key in sys.modules if key.startswith("tests.fixtures")]
    for mod in modules_to_remove:
        del sys.modules[mod]

    yield

    # Clean up after test as well
    action_registry.reset()
    workflow_registry.reset()
