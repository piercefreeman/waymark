"""
Scheduled workflow execution.

This module provides functions for registering workflows to run on a cron
schedule or at fixed intervals, and for managing the registered schedules.

Schedule names form one flat namespace on the server. ``WorkflowScoped``
and ``Absolute`` are client-side naming conventions over that namespace:
a ``WorkflowScoped`` name normalizes to ``"{workflow}/{name}"``, an
``Absolute`` name is used exactly as written.
"""

import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Type, Union, overload

from grpc import StatusCode, aio  # type: ignore[attr-defined]
from typing_extensions import deprecated

from waymark.proto import messages_pb2 as pb2

from .bridge import _workflow_stub, assert_never, ensure_singleton
from .workflow import Workflow

ScheduleType = Literal["cron", "interval"]
ScheduleStatus = Literal["active", "paused"]


@dataclass(frozen=True)
class WorkflowScoped:
    """A schedule name scoped under a workflow.

    Normalizes to ``"{workflow}/{name}"``. The workflow may be given as
    its class or as its short name, so management scripts can address
    schedules without importing workflow code.
    """

    workflow: Union[Type[Workflow], str]
    name: str


@dataclass(frozen=True)
class Absolute:
    """A schedule name used exactly as written."""

    name: str


ScheduleName = Union[WorkflowScoped, Absolute]


def _normalize_schedule_name(schedule_name: ScheduleName) -> str:
    match schedule_name:
        case WorkflowScoped(workflow=workflow, name=name):
            workflow_name = workflow if isinstance(workflow, str) else workflow.short_name()
            return f"{workflow_name}/{name}"
        case Absolute(name=name):
            return name
        case _:
            assert_never(schedule_name)


@dataclass
class ScheduleInfo:
    """Information about a registered schedule."""

    workflow_name: str
    schedule_name: str
    schedule_type: ScheduleType
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    jitter_seconds: int
    allow_duplicate: bool
    status: ScheduleStatus
    next_run_at: datetime
    last_instance_id: Optional[str]


async def schedule_workflow(
    workflow_cls: Type[Workflow],
    *,
    schedule_name: Union[str, ScheduleName],
    schedule: Union[str, timedelta],
    jitter: Optional[timedelta] = None,
    arguments: Optional[Dict[str, Any]] = None,
    allow_duplicate: bool = False,
) -> str:
    """
    Register a schedule for a workflow.

    This function registers both the workflow and the schedule in a single
    call. When the schedule fires, the registered workflow version will be
    executed with the registered inputs. Registering an existing schedule
    name again replaces the schedule entirely.

    Args:
        workflow_cls: The Workflow class to schedule.
        schedule_name: Name for this schedule. A plain string is scoped under
                       the workflow ("{workflow}/{name}"); pass Absolute (or a
                       WorkflowScoped for another workflow's scope) to control
                       the name exactly. Schedule names form one flat
                       namespace on the server.
        schedule: Either a cron expression string (e.g., "0 * * * *" for hourly)
                  or a timedelta for interval-based scheduling.
        jitter: Optional jitter window to add to each scheduled run.
        arguments: Optional keyword arguments to pass to each scheduled run.
        allow_duplicate: If False (default), the scheduler skips creating a new
                         instance when one is already running for this schedule.
                         If True, always creates a new instance.

    Returns:
        The normalized schedule name — the key every management call takes.

    Examples:
        # Run every hour at minute 0
        await schedule_workflow(
            MyWorkflow,
            schedule_name="hourly-run",
            schedule="0 * * * *"
        )

        # Run every 5 minutes
        await schedule_workflow(
            MyWorkflow,
            schedule_name="frequent-check",
            schedule=timedelta(minutes=5)
        )

        # Multiple schedules with different arguments
        await schedule_workflow(
            MyWorkflow,
            schedule_name="small-batch",
            schedule="0 0 * * *",
            arguments={"batch_size": 100}
        )
        await schedule_workflow(
            MyWorkflow,
            schedule_name="large-batch",
            schedule="0 12 * * *",
            arguments={"batch_size": 1000}
        )

        # Exact schedule name, outside the workflow's scope
        await schedule_workflow(
            MyWorkflow,
            schedule_name=Absolute("global-nightly"),
            schedule="0 0 * * *"
        )

    Raises:
        ValueError: If the interval is non-positive or jitter is negative.
        RuntimeError: If the gRPC call fails. An invalid cron expression is
                      rejected by the server as INVALID_ARGUMENT.
    """
    if isinstance(schedule_name, str):
        schedule_name = WorkflowScoped(workflow=workflow_cls, name=schedule_name)
    flat_schedule_name = _normalize_schedule_name(schedule_name)

    schedule_definition = pb2.ScheduleDefinition()
    if isinstance(schedule, str):
        schedule_definition.cron_expression = schedule
    elif isinstance(schedule, timedelta):
        interval_seconds = int(schedule.total_seconds())
        if interval_seconds <= 0:
            raise ValueError("Interval must be positive")
        schedule_definition.interval_seconds = interval_seconds
    else:
        raise TypeError(f"schedule must be str or timedelta, got {type(schedule)}")

    if jitter is not None:
        jitter_seconds = int(jitter.total_seconds())
        if jitter_seconds < 0:
            raise ValueError("jitter must be non-negative")
        schedule_definition.jitter_seconds = jitter_seconds

    schedule_definition.allow_duplicate = allow_duplicate

    # The registration names the workflow, pins the compiled version, and
    # carries the run arguments as its opaque arguments payload.
    workflow_arguments = workflow_cls._build_workflow_arguments((), arguments or {})
    registration = workflow_cls._build_registration_payload(workflow_arguments)

    request = pb2.RegisterScheduleRequest(
        schedule=schedule_definition,
        registration=registration,
        schedule_name=flat_schedule_name,
    )

    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        await stub.RegisterSchedule(request, timeout=30.0)
    except aio.AioRpcError as exc:
        raise RuntimeError(f"Failed to register schedule: {exc}") from exc

    return flat_schedule_name


def _resolve_management_target(
    arg: Union[ScheduleName, Type[Workflow]],
    schedule_name: Optional[str],
    function_name: str,
) -> str:
    """Resolve a management call's target from either call convention."""
    if schedule_name is not None:
        # Legacy convention: (workflow_cls, *, schedule_name="...").
        warnings.warn(
            f"{function_name}(workflow_cls, schedule_name=...) is deprecated; "
            "pass a ScheduleName (WorkflowScoped(...) / Absolute(...)) instead",
            DeprecationWarning,
            stacklevel=3,
        )
        if isinstance(arg, (WorkflowScoped, Absolute)):
            raise TypeError(
                "pass either a ScheduleName or the deprecated "
                "(workflow_cls, schedule_name=...) convention, not both"
            )
        return _normalize_schedule_name(WorkflowScoped(workflow=arg, name=schedule_name))

    if isinstance(arg, (WorkflowScoped, Absolute)):
        return _normalize_schedule_name(arg)
    raise TypeError(
        f"{function_name} takes a ScheduleName (WorkflowScoped(...) / Absolute(...)); "
        "bare strings are only accepted by schedule_workflow, where the workflow "
        "scopes them"
    )


async def _update_schedule_status(
    flat_schedule_name: str, status: "pb2.ScheduleStatus", operation: str
) -> bool:
    request = pb2.UpdateScheduleStatusRequest(
        schedule_name=flat_schedule_name,
        status=status,
    )

    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        await stub.UpdateScheduleStatus(request, timeout=30.0)
    except aio.AioRpcError as exc:
        if exc.code() == StatusCode.NOT_FOUND:
            return False
        raise RuntimeError(f"Failed to {operation} schedule: {exc}") from exc
    return True


@overload
async def pause_schedule(schedule_name: ScheduleName, /) -> bool: ...


@overload
@deprecated("Pass a ScheduleName (WorkflowScoped(...) / Absolute(...)) instead")
async def pause_schedule(workflow_cls: Type[Workflow], /, *, schedule_name: str) -> bool: ...


async def pause_schedule(
    arg: Union[ScheduleName, Type[Workflow]],
    /,
    *,
    schedule_name: Optional[str] = None,
) -> bool:
    """
    Pause a workflow schedule.

    The schedule will not fire until resumed. Existing running instances
    are not affected.

    Args:
        schedule_name: The ScheduleName of the schedule to pause. The
                       deprecated (workflow_cls, schedule_name=...) convention
                       is still accepted and maps to
                       WorkflowScoped(workflow_cls, schedule_name).

    Returns:
        True if a schedule was found and paused, False otherwise.

    Examples:
        await pause_schedule(WorkflowScoped(MyWorkflow, "hourly-run"))
        await pause_schedule(WorkflowScoped("data_sync", "hourly-run"))
        await pause_schedule(Absolute("global-nightly"))

    Raises:
        RuntimeError: If the gRPC call fails.
    """
    flat_schedule_name = _resolve_management_target(arg, schedule_name, "pause_schedule")
    return await _update_schedule_status(flat_schedule_name, pb2.SCHEDULE_STATUS_PAUSED, "pause")


@overload
async def resume_schedule(schedule_name: ScheduleName, /) -> bool: ...


@overload
@deprecated("Pass a ScheduleName (WorkflowScoped(...) / Absolute(...)) instead")
async def resume_schedule(workflow_cls: Type[Workflow], /, *, schedule_name: str) -> bool: ...


async def resume_schedule(
    arg: Union[ScheduleName, Type[Workflow]],
    /,
    *,
    schedule_name: Optional[str] = None,
) -> bool:
    """
    Resume a paused workflow schedule.

    A schedule whose next run was left in the past fires one catch-up run.

    Args:
        schedule_name: The ScheduleName of the schedule to resume. The
                       deprecated (workflow_cls, schedule_name=...) convention
                       is still accepted and maps to
                       WorkflowScoped(workflow_cls, schedule_name).

    Returns:
        True if a schedule was found and resumed, False otherwise.

    Examples:
        await resume_schedule(WorkflowScoped(MyWorkflow, "hourly-run"))
        await resume_schedule(Absolute("global-nightly"))

    Raises:
        RuntimeError: If the gRPC call fails.
    """
    flat_schedule_name = _resolve_management_target(arg, schedule_name, "resume_schedule")
    return await _update_schedule_status(flat_schedule_name, pb2.SCHEDULE_STATUS_ACTIVE, "resume")


@overload
async def delete_schedule(schedule_name: ScheduleName, /) -> bool: ...


@overload
@deprecated("Pass a ScheduleName (WorkflowScoped(...) / Absolute(...)) instead")
async def delete_schedule(workflow_cls: Type[Workflow], /, *, schedule_name: str) -> bool: ...


async def delete_schedule(
    arg: Union[ScheduleName, Type[Workflow]],
    /,
    *,
    schedule_name: Optional[str] = None,
) -> bool:
    """
    Delete a workflow's schedule.

    The schedule is removed; already-spawned instances are not affected. It
    can be recreated by calling schedule_workflow again.

    Args:
        schedule_name: The ScheduleName of the schedule to delete. The
                       deprecated (workflow_cls, schedule_name=...) convention
                       is still accepted and maps to
                       WorkflowScoped(workflow_cls, schedule_name).

    Returns:
        True if a schedule was found and deleted, False otherwise.

    Examples:
        await delete_schedule(WorkflowScoped(MyWorkflow, "hourly-run"))
        await delete_schedule(Absolute("global-nightly"))

    Raises:
        RuntimeError: If the gRPC call fails.
    """
    flat_schedule_name = _resolve_management_target(arg, schedule_name, "delete_schedule")
    request = pb2.DeleteScheduleRequest(schedule_name=flat_schedule_name)

    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        await stub.DeleteSchedule(request, timeout=30.0)
    except aio.AioRpcError as exc:
        if exc.code() == StatusCode.NOT_FOUND:
            return False
        raise RuntimeError(f"Failed to delete schedule: {exc}") from exc
    return True


def _schedule_info_from_proto(info: "pb2.ScheduleInfo") -> ScheduleInfo:
    definition = info.definition

    schedule_type: ScheduleType
    kind = definition.WhichOneof("schedule")
    match kind:
        case "cron_expression":
            schedule_type = "cron"
            cron_expression: Optional[str] = definition.cron_expression
            interval_seconds: Optional[int] = None
        case "interval_seconds":
            schedule_type = "interval"
            cron_expression = None
            interval_seconds = definition.interval_seconds
        case None:
            raise RuntimeError("schedule definition carries no schedule")
        case _:
            assert_never(kind)

    status: ScheduleStatus
    match info.status:
        case pb2.SCHEDULE_STATUS_ACTIVE:
            status = "active"
        case pb2.SCHEDULE_STATUS_PAUSED:
            status = "paused"
        case _:
            raise RuntimeError(f"unexpected schedule status: {info.status}")

    return ScheduleInfo(
        workflow_name=info.workflow_name,
        schedule_name=info.schedule_name,
        schedule_type=schedule_type,
        cron_expression=cron_expression,
        interval_seconds=interval_seconds,
        jitter_seconds=definition.jitter_seconds,
        allow_duplicate=definition.allow_duplicate,
        status=status,
        # Timestamps are instants; ToDatetime is naive by default, so the
        # UTC awareness is restored explicitly.
        next_run_at=info.next_run_at.ToDatetime(tzinfo=timezone.utc),
        last_instance_id=info.last_instance_id or None,
    )


async def list_schedules(
    status_filter: Optional[ScheduleStatus] = None,
) -> List[ScheduleInfo]:
    """
    List all registered workflow schedules.

    Args:
        status_filter: Optional filter by status ("active" or "paused").
                       If None, returns all schedules.

    Returns:
        A list of ScheduleInfo objects containing schedule details.

    Examples:
        # List all schedules
        schedules = await list_schedules()
        for s in schedules:
            print(f"{s.schedule_name}: {s.status}")

        # List only active schedules
        active = await list_schedules(status_filter="active")

        # List only paused schedules
        paused = await list_schedules(status_filter="paused")

    Raises:
        RuntimeError: If the gRPC call fails.
    """
    request = pb2.ListSchedulesRequest()
    match status_filter:
        case None:
            pass  # The unspecified status means no filter.
        case "active":
            request.status_filter = pb2.SCHEDULE_STATUS_ACTIVE
        case "paused":
            request.status_filter = pb2.SCHEDULE_STATUS_PAUSED
        case _:
            assert_never(status_filter)

    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        response = await stub.ListSchedules(request, timeout=30.0)
    except aio.AioRpcError as exc:
        raise RuntimeError(f"Failed to list schedules: {exc}") from exc

    return [_schedule_info_from_proto(info) for info in response.schedules]
