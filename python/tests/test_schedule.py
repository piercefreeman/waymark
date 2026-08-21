"""Tests for schedule.py client API."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import grpc
import pytest
from google.protobuf import empty_pb2, timestamp_pb2
from grpc import aio  # type: ignore[attr-defined]

from waymark import schedule as schedule_module
from waymark.proto import messages_pb2 as pb2
from waymark.schedule import (
    Absolute,
    ScheduleInfo,
    WorkflowScoped,
    _normalize_schedule_name,
    delete_schedule,
    list_schedules,
    pause_schedule,
    resume_schedule,
    schedule_workflow,
)
from waymark.serialization import workflow_arguments_to_kwargs
from waymark.workflow import Workflow, workflow


@workflow
class DemoScheduleWorkflow(Workflow):
    """A simple workflow for testing schedule operations."""

    async def run(self) -> str:
        return "test"


@workflow
class DemoScheduleWorkflowWithInputs(Workflow):
    """A workflow with inputs for scheduling tests."""

    async def run(self, batch_size: int = 50) -> str:
        return "test"


@pytest.fixture
def mock_stub(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Create a mock gRPC stub."""
    stub = AsyncMock()

    async def fake_workflow_stub() -> AsyncMock:
        return stub

    @asynccontextmanager
    async def fake_ensure_singleton():
        yield 8080

    monkeypatch.setattr(schedule_module, "_workflow_stub", fake_workflow_stub)
    monkeypatch.setattr(schedule_module, "ensure_singleton", fake_ensure_singleton)
    return stub


def not_found_error() -> aio.AioRpcError:
    return aio.AioRpcError(
        code=grpc.StatusCode.NOT_FOUND,
        initial_metadata=aio.Metadata(),
        trailing_metadata=aio.Metadata(),
        details="no such schedule",
    )


class TestNormalizeScheduleName:
    """Tests for the flat-namespace naming conventions."""

    def test_workflow_scoped_by_class(self) -> None:
        name = _normalize_schedule_name(WorkflowScoped(DemoScheduleWorkflow, "hourly"))
        assert name == "demoscheduleworkflow/hourly"

    def test_workflow_scoped_by_name(self) -> None:
        name = _normalize_schedule_name(WorkflowScoped("data_sync", "hourly"))
        assert name == "data_sync/hourly"

    def test_absolute_passes_through(self) -> None:
        assert _normalize_schedule_name(Absolute("global-nightly")) == "global-nightly"


class TestScheduleWorkflow:
    """Tests for schedule_workflow."""

    def test_cron_schedule(self, mock_stub: AsyncMock) -> None:
        mock_stub.RegisterSchedule.return_value = pb2.RegisterScheduleResponse()

        result = asyncio.run(
            schedule_workflow(DemoScheduleWorkflow, schedule_name="test-cron", schedule="0 * * * *")
        )

        assert result == "demoscheduleworkflow/test-cron"
        request = mock_stub.RegisterSchedule.call_args[0][0]
        assert request.schedule_name == "demoscheduleworkflow/test-cron"
        assert request.schedule.WhichOneof("schedule") == "cron_expression"
        assert request.schedule.cron_expression == "0 * * * *"
        assert request.registration.workflow_name == "demoscheduleworkflow"

    def test_interval_schedule(self, mock_stub: AsyncMock) -> None:
        mock_stub.RegisterSchedule.return_value = pb2.RegisterScheduleResponse()

        asyncio.run(
            schedule_workflow(
                DemoScheduleWorkflow,
                schedule_name="test-interval",
                schedule=timedelta(minutes=5),
            )
        )

        request = mock_stub.RegisterSchedule.call_args[0][0]
        assert request.schedule.WhichOneof("schedule") == "interval_seconds"
        assert request.schedule.interval_seconds == 300

    def test_absolute_schedule_name(self, mock_stub: AsyncMock) -> None:
        mock_stub.RegisterSchedule.return_value = pb2.RegisterScheduleResponse()

        result = asyncio.run(
            schedule_workflow(
                DemoScheduleWorkflow,
                schedule_name=Absolute("global-nightly"),
                schedule="0 0 * * *",
            )
        )

        assert result == "global-nightly"
        request = mock_stub.RegisterSchedule.call_args[0][0]
        assert request.schedule_name == "global-nightly"

    def test_inputs_travel_in_the_registration(self, mock_stub: AsyncMock) -> None:
        mock_stub.RegisterSchedule.return_value = pb2.RegisterScheduleResponse()

        asyncio.run(
            schedule_workflow(
                DemoScheduleWorkflowWithInputs,
                schedule_name="with-inputs",
                schedule="0 * * * *",
                arguments={"batch_size": 1000},
            )
        )

        request = mock_stub.RegisterSchedule.call_args[0][0]
        kwargs = workflow_arguments_to_kwargs(request.registration.arguments)
        assert kwargs == {"batch_size": 1000}

    def test_jitter_and_allow_duplicate(self, mock_stub: AsyncMock) -> None:
        mock_stub.RegisterSchedule.return_value = pb2.RegisterScheduleResponse()

        asyncio.run(
            schedule_workflow(
                DemoScheduleWorkflow,
                schedule_name="jittered",
                schedule=timedelta(minutes=1),
                jitter=timedelta(seconds=30),
                allow_duplicate=True,
            )
        )

        request = mock_stub.RegisterSchedule.call_args[0][0]
        assert request.schedule.jitter_seconds == 30
        assert request.schedule.allow_duplicate is True

    def test_defaults(self, mock_stub: AsyncMock) -> None:
        mock_stub.RegisterSchedule.return_value = pb2.RegisterScheduleResponse()

        asyncio.run(
            schedule_workflow(DemoScheduleWorkflow, schedule_name="plain", schedule="0 * * * *")
        )

        request = mock_stub.RegisterSchedule.call_args[0][0]
        assert request.schedule.jitter_seconds == 0
        assert request.schedule.allow_duplicate is False

    def test_non_positive_interval(self) -> None:
        with pytest.raises(ValueError, match="Interval must be positive"):
            asyncio.run(
                schedule_workflow(
                    DemoScheduleWorkflow,
                    schedule_name="bad",
                    schedule=timedelta(seconds=0),
                )
            )

    def test_negative_jitter(self) -> None:
        with pytest.raises(ValueError, match="jitter must be non-negative"):
            asyncio.run(
                schedule_workflow(
                    DemoScheduleWorkflow,
                    schedule_name="bad",
                    schedule="0 * * * *",
                    jitter=timedelta(seconds=-1),
                )
            )

    def test_invalid_schedule_type(self) -> None:
        with pytest.raises(TypeError, match="schedule must be str or timedelta"):
            asyncio.run(
                schedule_workflow(
                    DemoScheduleWorkflow,
                    schedule_name="bad",
                    schedule=123,  # type: ignore[arg-type]
                )
            )


class TestManagementCalls:
    """Tests for pause/resume/delete across both call conventions."""

    def test_pause_by_schedule_name(self, mock_stub: AsyncMock) -> None:
        mock_stub.UpdateScheduleStatus.return_value = empty_pb2.Empty()

        result = asyncio.run(pause_schedule(WorkflowScoped("data_sync", "hourly")))

        assert result is True
        request = mock_stub.UpdateScheduleStatus.call_args[0][0]
        assert request.schedule_name == "data_sync/hourly"
        assert request.status == pb2.SCHEDULE_STATUS_PAUSED

    def test_resume_by_schedule_name(self, mock_stub: AsyncMock) -> None:
        mock_stub.UpdateScheduleStatus.return_value = empty_pb2.Empty()

        result = asyncio.run(resume_schedule(Absolute("global-nightly")))

        assert result is True
        request = mock_stub.UpdateScheduleStatus.call_args[0][0]
        assert request.schedule_name == "global-nightly"
        assert request.status == pb2.SCHEDULE_STATUS_ACTIVE

    def test_delete_by_schedule_name(self, mock_stub: AsyncMock) -> None:
        mock_stub.DeleteSchedule.return_value = empty_pb2.Empty()

        result = asyncio.run(delete_schedule(Absolute("global-nightly")))

        assert result is True
        request = mock_stub.DeleteSchedule.call_args[0][0]
        assert request.schedule_name == "global-nightly"

    def test_missing_schedule_returns_false(self, mock_stub: AsyncMock) -> None:
        mock_stub.UpdateScheduleStatus.side_effect = not_found_error()
        mock_stub.DeleteSchedule.side_effect = not_found_error()

        assert asyncio.run(pause_schedule(Absolute("missing"))) is False
        assert asyncio.run(resume_schedule(Absolute("missing"))) is False
        assert asyncio.run(delete_schedule(Absolute("missing"))) is False

    def test_legacy_convention_is_deprecated_but_works(self, mock_stub: AsyncMock) -> None:
        mock_stub.UpdateScheduleStatus.return_value = empty_pb2.Empty()
        mock_stub.DeleteSchedule.return_value = empty_pb2.Empty()

        with pytest.deprecated_call():
            result = asyncio.run(pause_schedule(DemoScheduleWorkflow, schedule_name="hourly"))
        assert result is True
        request = mock_stub.UpdateScheduleStatus.call_args[0][0]
        assert request.schedule_name == "demoscheduleworkflow/hourly"

        with pytest.deprecated_call():
            asyncio.run(delete_schedule(DemoScheduleWorkflow, schedule_name="hourly"))
        request = mock_stub.DeleteSchedule.call_args[0][0]
        assert request.schedule_name == "demoscheduleworkflow/hourly"

    def test_bare_string_is_rejected(self) -> None:
        with pytest.raises(TypeError, match="takes a ScheduleName"):
            asyncio.run(pause_schedule("hourly"))  # type: ignore[arg-type]

    def test_mixed_conventions_are_rejected(self) -> None:
        with pytest.raises(TypeError, match="not both"), pytest.deprecated_call():
            asyncio.run(
                pause_schedule(Absolute("hourly"), schedule_name="hourly")  # type: ignore[call-overload]
            )


def wire_schedule_info(
    *,
    schedule_name: str = "data_sync/hourly",
    workflow_name: str = "data_sync",
    cron_expression: str | None = "0 * * * *",
    interval_seconds: int | None = None,
    status: int = pb2.SCHEDULE_STATUS_ACTIVE,
    last_instance_id: str = "",
) -> pb2.ScheduleInfo:
    definition = pb2.ScheduleDefinition(jitter_seconds=5, allow_duplicate=True)
    if cron_expression is not None:
        definition.cron_expression = cron_expression
    if interval_seconds is not None:
        definition.interval_seconds = interval_seconds

    next_run_at = timestamp_pb2.Timestamp()
    next_run_at.FromDatetime(datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc))

    return pb2.ScheduleInfo(
        workflow_name=workflow_name,
        schedule_name=schedule_name,
        status=status,
        next_run_at=next_run_at,
        last_instance_id=last_instance_id,
        definition=definition,
    )


class TestListSchedules:
    """Tests for list_schedules."""

    def test_empty(self, mock_stub: AsyncMock) -> None:
        mock_stub.ListSchedules.return_value = pb2.ListSchedulesResponse(schedules=[])

        assert asyncio.run(list_schedules()) == []

    def test_parses_a_cron_schedule(self, mock_stub: AsyncMock) -> None:
        mock_stub.ListSchedules.return_value = pb2.ListSchedulesResponse(
            schedules=[wire_schedule_info()]
        )

        result = asyncio.run(list_schedules())

        assert result == [
            ScheduleInfo(
                workflow_name="data_sync",
                schedule_name="data_sync/hourly",
                schedule_type="cron",
                cron_expression="0 * * * *",
                interval_seconds=None,
                jitter_seconds=5,
                allow_duplicate=True,
                status="active",
                next_run_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                last_instance_id=None,
            )
        ]

    def test_parses_an_interval_schedule_with_instance(self, mock_stub: AsyncMock) -> None:
        mock_stub.ListSchedules.return_value = pb2.ListSchedulesResponse(
            schedules=[
                wire_schedule_info(
                    cron_expression=None,
                    interval_seconds=300,
                    status=pb2.SCHEDULE_STATUS_PAUSED,
                    last_instance_id="instance-1",
                )
            ]
        )

        result = asyncio.run(list_schedules())

        assert result[0].schedule_type == "interval"
        assert result[0].cron_expression is None
        assert result[0].interval_seconds == 300
        assert result[0].status == "paused"
        assert result[0].last_instance_id == "instance-1"
        assert result[0].next_run_at.tzinfo == timezone.utc

    def test_status_filter_maps_to_the_enum(self, mock_stub: AsyncMock) -> None:
        mock_stub.ListSchedules.return_value = pb2.ListSchedulesResponse(schedules=[])

        asyncio.run(list_schedules(status_filter="paused"))
        request = mock_stub.ListSchedules.call_args[0][0]
        assert request.status_filter == pb2.SCHEDULE_STATUS_PAUSED

        asyncio.run(list_schedules())
        request = mock_stub.ListSchedules.call_args[0][0]
        assert request.status_filter == pb2.SCHEDULE_STATUS_UNSPECIFIED
