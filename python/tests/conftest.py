import sys
from pathlib import Path

import pytest

PROJECT_PY_DIR = Path(__file__).resolve().parents[1]
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
