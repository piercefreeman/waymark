"""
Tests for the public API surface of the waymark package.

These tests verify that all exported symbols are importable and that
the package's __all__ matches what's actually available.
"""

import waymark


def test_all_exports_importable() -> None:
    """Verify every name in __all__ is actually importable from waymark."""
    missing = []
    for name in waymark.__all__:
        if not hasattr(waymark, name):
            missing.append(name)

    assert not missing, f"These names are in __all__ but not importable: {missing}"


def test_public_api_imports() -> None:
    """Verify key public API symbols can be imported directly."""
    # These imports should not raise any errors
    from waymark import (
        action,
        build_workflow_ir,
        schedule_workflow,
        workflow,
    )

    # Basic sanity checks that the imports are the right types
    assert callable(action)
    assert callable(workflow)
    assert callable(build_workflow_ir)
    assert callable(schedule_workflow)
