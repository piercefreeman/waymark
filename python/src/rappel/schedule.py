"""
Scheduled workflow execution.

This module provides functions for registering workflows to run on a cron
schedule or at fixed intervals.
"""

from datetime import timedelta
from typing import Any, Dict, Optional, Type, Union

from grpc import aio  # type: ignore[attr-defined]

from proto import messages_pb2 as pb2

from .bridge import _workflow_stub, ensure_singleton
from .serialization import build_arguments_from_kwargs
from .workflow import Workflow


async def schedule_workflow(
    workflow_cls: Type[Workflow],
    *,
    schedule: Union[str, timedelta],
    inputs: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Register a schedule for a workflow.

    This function registers both the workflow DAG and the schedule in a single
    call. When the schedule fires, the registered workflow version will be
    executed.

    Args:
        workflow_cls: The Workflow class to schedule.
        schedule: Either a cron expression string (e.g., "0 * * * *" for hourly)
                  or a timedelta for interval-based scheduling.
        inputs: Optional keyword arguments to pass to each scheduled run.

    Returns:
        The schedule ID.

    Examples:
        # Run every hour at minute 0
        await schedule_workflow(MyWorkflow, schedule="0 * * * *")

        # Run every 5 minutes
        await schedule_workflow(MyWorkflow, schedule=timedelta(minutes=5))

        # Run daily at midnight with inputs
        await schedule_workflow(
            MyWorkflow,
            schedule="0 0 * * *",
            inputs={"batch_size": 100}
        )

    Raises:
        ValueError: If the cron expression is invalid or interval is non-positive.
        RuntimeError: If the gRPC call fails.
    """
    workflow_name = workflow_cls.short_name()

    # Build schedule definition
    schedule_def = pb2.ScheduleDefinition()
    if isinstance(schedule, str):
        schedule_def.type = pb2.SCHEDULE_TYPE_CRON
        schedule_def.cron_expression = schedule
    elif isinstance(schedule, timedelta):
        interval_seconds = int(schedule.total_seconds())
        if interval_seconds <= 0:
            raise ValueError("Interval must be positive")
        schedule_def.type = pb2.SCHEDULE_TYPE_INTERVAL
        schedule_def.interval_seconds = interval_seconds
    else:
        raise TypeError(f"schedule must be str or timedelta, got {type(schedule)}")

    # Build the workflow registration payload to ensure the DAG is registered
    # This is required for the schedule to execute - the scheduler needs a
    # registered workflow version to create instances from.
    registration = workflow_cls._build_registration_payload()

    # Build request with both registration and schedule
    request = pb2.RegisterScheduleRequest(
        workflow_name=workflow_name,
        schedule=schedule_def,
        registration=registration,
    )

    # Add inputs if provided
    if inputs:
        request.inputs.CopyFrom(build_arguments_from_kwargs(inputs))

    # Send to server
    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        response = await stub.RegisterSchedule(request, timeout=30.0)
    except aio.AioRpcError as exc:
        raise RuntimeError(f"Failed to register schedule: {exc}") from exc

    return response.schedule_id


async def pause_schedule(workflow_cls: Type[Workflow]) -> bool:
    """
    Pause a workflow's schedule.

    The schedule will not fire until resumed. Existing running instances
    are not affected.

    Args:
        workflow_cls: The Workflow class whose schedule to pause.

    Returns:
        True if a schedule was found and paused, False otherwise.
    """
    request = pb2.UpdateScheduleStatusRequest(
        workflow_name=workflow_cls.short_name(),
        status=pb2.SCHEDULE_STATUS_PAUSED,
    )
    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        response = await stub.UpdateScheduleStatus(request, timeout=30.0)
    except aio.AioRpcError as exc:
        raise RuntimeError(f"Failed to pause schedule: {exc}") from exc

    return response.success


async def resume_schedule(workflow_cls: Type[Workflow]) -> bool:
    """
    Resume a paused workflow schedule.

    Args:
        workflow_cls: The Workflow class whose schedule to resume.

    Returns:
        True if a schedule was found and resumed, False otherwise.
    """
    request = pb2.UpdateScheduleStatusRequest(
        workflow_name=workflow_cls.short_name(),
        status=pb2.SCHEDULE_STATUS_ACTIVE,
    )
    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        response = await stub.UpdateScheduleStatus(request, timeout=30.0)
    except aio.AioRpcError as exc:
        raise RuntimeError(f"Failed to resume schedule: {exc}") from exc

    return response.success


async def delete_schedule(workflow_cls: Type[Workflow]) -> bool:
    """
    Delete a workflow's schedule.

    The schedule is soft-deleted and can be recreated by calling
    schedule_workflow again.

    Args:
        workflow_cls: The Workflow class whose schedule to delete.

    Returns:
        True if a schedule was found and deleted, False otherwise.
    """
    request = pb2.DeleteScheduleRequest(
        workflow_name=workflow_cls.short_name(),
    )
    async with ensure_singleton():
        stub = await _workflow_stub()

    try:
        response = await stub.DeleteSchedule(request, timeout=30.0)
    except aio.AioRpcError as exc:
        raise RuntimeError(f"Failed to delete schedule: {exc}") from exc

    return response.success
