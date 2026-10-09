"""Proxies for the exceptions the VM raises inside a workflow.

NOTHING IN THIS FILE MATTERS AT RUNTIME. The real exceptions are defined in
Rust and exist only in the VM: it raises them into a workflow when an action
attempt times out, is lost, or never starts, and it matches them against
``except`` clauses by class name and bases as recorded on the Rust side. The
classes here are proxies so a workflow body can spell those names in an
``except`` clause and a ``RetryPolicy``, and so type hints and readers have
something to point at.

Waymark never raises these in Python. They do not originate in actions, and
the worker never constructs them. Do not raise them from an action either: a
hand-raised one is indistinguishable from the VM's own, since matching reads
only the class name and its bases, and the two deriving from ``BaseException``
are not reported back by the worker at all, which catches only ``Exception``.

KEEP IN SYNC WITH THE RUST SIDE. The VM's table of these exceptions, their
names and their bases, is ``exception::classes`` in the
``waymark-vm-value-python`` crate (``crates/lib/vm-value-python/src/exception.rs``).
Every class here mirrors one entry there: the same name, the same bases in the
same order. A change on either side is a change on both. Each side is pinned to
the same literals by its own test, ``tests/test_vm_exceptions.py`` here and the
crate's integration test there, so a change to either table fails its own test.
"""


class ActionTimeout(BaseException):
    """Proxy for the VM's ``ActionTimeout``; the real exception is defined in Rust.

    The VM raises it when an action attempt's timeout expires. The timed-out
    attempt may still be running, so it derives from ``BaseException``
    directly: neither ``except Exception:`` nor a ``RetryPolicy`` retrying on
    ``Exception`` takes it, only one listing it or ``BaseException``.

    Mirrors ``classes::ACTION_TIMEOUT``.
    """


class ActionExecutionLost(BaseException):
    """Proxy for the VM's ``ActionExecutionLost``; the real exception is defined in Rust.

    The VM raises it when a worker had an action attempt and its execution
    was lost. How far it got is unknown, it may have run to completion, so it
    derives from ``BaseException`` directly like :class:`ActionTimeout` and
    for the same reason: retrying it is an explicit opt-in.

    Mirrors ``classes::ACTION_EXECUTION_LOST``.
    """


class ActionExecutionNotStarted(Exception):  # noqa: N818 - the VM's name is the wire
    """Proxy for the VM's ``ActionExecutionNotStarted``; the real exception is defined in Rust.

    The VM raises it when the dispatch of an action attempt was lost before a
    worker received it. Nothing ran, so it is an ordinary ``Exception``: a
    ``RetryPolicy`` retrying on ``Exception`` retries it.

    Mirrors ``classes::ACTION_EXECUTION_NOT_STARTED``.
    """
