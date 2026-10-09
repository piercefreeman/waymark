"""The VM exception proxies mirror the Rust table exactly.

The expected names and bases below are copied from ``exception::classes`` in
``crates/lib/vm-value-python/src/exception.rs``. A change there is a change
here, and the other way round.
"""

import pytest

from waymark.vm_exceptions import (
    ActionExecutionLost,
    ActionExecutionNotStarted,
    ActionTimeout,
)

MIRRORED = [
    ("ACTION_TIMEOUT", ActionTimeout, "ActionTimeout", ["BaseException"]),
    (
        "ACTION_EXECUTION_NOT_STARTED",
        ActionExecutionNotStarted,
        "ActionExecutionNotStarted",
        ["Exception", "BaseException"],
    ),
    ("ACTION_EXECUTION_LOST", ActionExecutionLost, "ActionExecutionLost", ["BaseException"]),
]


@pytest.mark.parametrize(("rust_constant", "proxy", "type_id", "bases"), MIRRORED)
def test_proxy_name_matches_the_vm_type_id(
    rust_constant: str, proxy: type, type_id: str, bases: list[str]
) -> None:
    assert proxy.__name__ == type_id, (
        f"{proxy.__name__} must be spelled as the VM's type id for classes::{rust_constant}"
    )


@pytest.mark.parametrize(("rust_constant", "proxy", "type_id", "bases"), MIRRORED)
def test_proxy_bases_match_the_vm_table(
    rust_constant: str, proxy: type, type_id: str, bases: list[str]
) -> None:
    mro = [cls.__name__ for cls in proxy.__mro__[1:] if cls is not object]
    assert mro == bases, (
        f"{type_id} bases must mirror classes::{rust_constant}.mro_type_ids in Rust"
    )
