"""Tests for schedule.py client API."""

import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from proto import messages_pb2 as pb2
from rappel import schedule as schedule_module
from rappel.schedule import (
    ScheduleInfo,
    _parse_iso_datetime,
    _proto_schedule_status_to_str,
    _proto_schedule_type_to_str,
    delete_schedule,
    list_schedules,
    pause_schedule,
    resume_schedule,
    schedule_workflow,
)
from rappel.workflow import Workflow, workflow


@workflow
class DemoScheduleWorkflow(Workflow):
    """A simple workflow for testing schedule operations."""

    async def run(self) -> str:
        return "test"


class TestParseIsoDatetime:
    """Tests for _parse_iso_datetime helper."""

    def test_empty_string_returns_none(self) -> None:
        assert _parse_iso_datetime("") is None

    def test_parses_utc_z_suffix(self) -> None:
        result = _parse_iso_datetime("2024-01-15T10:30:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_parses_timezone_offset(self) -> None:
        result = _parse_iso_datetime("2024-06-20T14:00:00+00:00")
        assert result is not None
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 20


class TestProtoScheduleTypeToStr:
    """Tests for _proto_schedule_type_to_str helper."""

    def test_cron_type(self) -> None:
        assert _proto_schedule_type_to_str(pb2.SCHEDULE_TYPE_CRON) == "cron"

    def test_interval_type(self) -> None:
        assert _proto_schedule_type_to_str(pb2.SCHEDULE_TYPE_INTERVAL) == "interval"

    def test_unspecified_defaults_to_cron(self) -> None:
        assert _proto_schedule_type_to_str(pb2.SCHEDULE_TYPE_UNSPECIFIED) == "cron"


class TestProtoScheduleStatusToStr:
    """Tests for _proto_schedule_status_to_str helper."""

    def test_active_status(self) -> None:
        assert _proto_schedule_status_to_str(pb2.SCHEDULE_STATUS_ACTIVE) == "active"

    def test_paused_status(self) -> None:
        assert _proto_schedule_status_to_str(pb2.SCHEDULE_STATUS_PAUSED) == "paused"

    def test_unspecified_defaults_to_active(self) -> None:
        assert _proto_schedule_status_to_str(pb2.SCHEDULE_STATUS_UNSPECIFIED) == "active"


class TestListSchedules:
    """Tests for list_schedules function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
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

    def test_list_schedules_empty(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test listing schedules when none exist."""
        response = pb2.ListSchedulesResponse(schedules=[])
        mock_stub.ListSchedules.return_value = response

        result = asyncio.run(list_schedules())

        assert result == []
        mock_stub.ListSchedules.assert_called_once()

    def test_list_schedules_returns_schedule_info(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test listing schedules returns properly parsed ScheduleInfo objects."""
        schedule_proto = pb2.ScheduleInfo(
            id="123e4567-e89b-12d3-a456-426614174000",
            workflow_name="testscheduleworkflow",
            schedule_type=pb2.SCHEDULE_TYPE_CRON,
            cron_expression="0 * * * *",
            interval_seconds=0,
            status=pb2.SCHEDULE_STATUS_ACTIVE,
            next_run_at="2024-01-15T11:00:00Z",
            last_run_at="2024-01-15T10:00:00Z",
            last_instance_id="abc-123",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-15T10:00:00Z",
        )
        response = pb2.ListSchedulesResponse(schedules=[schedule_proto])
        mock_stub.ListSchedules.return_value = response

        result = asyncio.run(list_schedules())

        assert len(result) == 1
        schedule = result[0]
        assert isinstance(schedule, ScheduleInfo)
        assert schedule.id == "123e4567-e89b-12d3-a456-426614174000"
        assert schedule.workflow_name == "testscheduleworkflow"
        assert schedule.schedule_type == "cron"
        assert schedule.cron_expression == "0 * * * *"
        assert schedule.status == "active"
        assert schedule.last_instance_id == "abc-123"

    def test_list_schedules_with_status_filter(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test listing schedules with status filter."""
        response = pb2.ListSchedulesResponse(schedules=[])
        mock_stub.ListSchedules.return_value = response

        asyncio.run(list_schedules(status_filter="active"))

        call_args = mock_stub.ListSchedules.call_args
        request = call_args[0][0]
        assert request.status_filter == "active"

    def test_list_schedules_with_interval_type(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test listing schedules with interval type."""
        schedule_proto = pb2.ScheduleInfo(
            id="interval-uuid",
            workflow_name="intervalworkflow",
            schedule_type=pb2.SCHEDULE_TYPE_INTERVAL,
            cron_expression="",
            interval_seconds=300,
            status=pb2.SCHEDULE_STATUS_PAUSED,
            next_run_at="",
            last_run_at="",
            last_instance_id="",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:00:00Z",
        )
        response = pb2.ListSchedulesResponse(schedules=[schedule_proto])
        mock_stub.ListSchedules.return_value = response

        result = asyncio.run(list_schedules())

        assert len(result) == 1
        schedule = result[0]
        assert schedule.schedule_type == "interval"
        assert schedule.interval_seconds == 300
        assert schedule.cron_expression is None
        assert schedule.status == "paused"
        assert schedule.next_run_at is None
        assert schedule.last_run_at is None
        assert schedule.last_instance_id is None

    def test_list_schedules_multiple(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test listing multiple schedules."""
        schedules_proto = [
            pb2.ScheduleInfo(
                id=f"uuid-{i}",
                workflow_name=f"workflow{i}",
                schedule_type=pb2.SCHEDULE_TYPE_CRON,
                cron_expression="0 0 * * *",
                status=pb2.SCHEDULE_STATUS_ACTIVE,
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )
            for i in range(3)
        ]
        response = pb2.ListSchedulesResponse(schedules=schedules_proto)
        mock_stub.ListSchedules.return_value = response

        result = asyncio.run(list_schedules())

        assert len(result) == 3
        assert [s.workflow_name for s in result] == [
            "workflow0",
            "workflow1",
            "workflow2",
        ]


class TestScheduleWorkflow:
    """Tests for schedule_workflow function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
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

    def test_schedule_workflow_with_cron(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test scheduling a workflow with cron expression."""
        response = pb2.RegisterScheduleResponse(schedule_id="schedule-123")
        mock_stub.RegisterSchedule.return_value = response

        result = asyncio.run(
            schedule_workflow(DemoScheduleWorkflow, schedule_name="test-cron", schedule="0 * * * *")
        )

        assert result == "schedule-123"
        call_args = mock_stub.RegisterSchedule.call_args
        request = call_args[0][0]
        assert request.workflow_name == "demoscheduleworkflow"
        assert request.schedule_name == "test-cron"
        assert request.schedule.type == pb2.SCHEDULE_TYPE_CRON
        assert request.schedule.cron_expression == "0 * * * *"

    def test_schedule_workflow_with_interval(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test scheduling a workflow with timedelta interval."""
        response = pb2.RegisterScheduleResponse(schedule_id="schedule-456")
        mock_stub.RegisterSchedule.return_value = response

        result = asyncio.run(
            schedule_workflow(
                DemoScheduleWorkflow,
                schedule_name="test-interval",
                schedule=timedelta(minutes=5),
            )
        )

        assert result == "schedule-456"
        call_args = mock_stub.RegisterSchedule.call_args
        request = call_args[0][0]
        assert request.schedule_name == "test-interval"
        assert request.schedule.type == pb2.SCHEDULE_TYPE_INTERVAL
        assert request.schedule.interval_seconds == 300

    def test_schedule_workflow_with_inputs(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test scheduling a workflow with inputs."""
        response = pb2.RegisterScheduleResponse(schedule_id="schedule-789")
        mock_stub.RegisterSchedule.return_value = response

        result = asyncio.run(
            schedule_workflow(
                DemoScheduleWorkflow,
                schedule_name="test-inputs",
                schedule="0 0 * * *",
                inputs={"batch_size": 100},
            )
        )

        assert result == "schedule-789"
        call_args = mock_stub.RegisterSchedule.call_args
        request = call_args[0][0]
        assert request.schedule_name == "test-inputs"
        assert request.HasField("inputs")

    def test_schedule_workflow_invalid_interval(self) -> None:
        """Test that non-positive intervals raise ValueError."""
        with pytest.raises(ValueError, match="Interval must be positive"):
            asyncio.run(
                schedule_workflow(
                    DemoScheduleWorkflow,
                    schedule_name="test",
                    schedule=timedelta(seconds=0),
                )
            )

    def test_schedule_workflow_empty_schedule_name(self) -> None:
        """Test that empty schedule_name raises ValueError."""
        with pytest.raises(ValueError, match="schedule_name is required"):
            asyncio.run(
                schedule_workflow(DemoScheduleWorkflow, schedule_name="", schedule="0 * * * *")
            )

    def test_schedule_workflow_invalid_type(self) -> None:
        """Test that invalid schedule types raise TypeError."""
        with pytest.raises(TypeError, match="schedule must be str or timedelta"):
            asyncio.run(
                schedule_workflow(DemoScheduleWorkflow, schedule_name="test", schedule=123)  # type: ignore
            )


class TestPauseSchedule:
    """Tests for pause_schedule function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
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

    def test_pause_schedule_success(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test pausing a schedule successfully."""
        response = pb2.UpdateScheduleStatusResponse(success=True)
        mock_stub.UpdateScheduleStatus.return_value = response

        result = asyncio.run(pause_schedule(DemoScheduleWorkflow, schedule_name="test-schedule"))

        assert result is True
        call_args = mock_stub.UpdateScheduleStatus.call_args
        request = call_args[0][0]
        assert request.workflow_name == "demoscheduleworkflow"
        assert request.schedule_name == "test-schedule"
        assert request.status == pb2.SCHEDULE_STATUS_PAUSED

    def test_pause_schedule_not_found(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test pausing a schedule that doesn't exist."""
        response = pb2.UpdateScheduleStatusResponse(success=False)
        mock_stub.UpdateScheduleStatus.return_value = response

        result = asyncio.run(pause_schedule(DemoScheduleWorkflow, schedule_name="nonexistent"))

        assert result is False

    def test_pause_schedule_empty_name(self) -> None:
        """Test that empty schedule_name raises ValueError."""
        with pytest.raises(ValueError, match="schedule_name is required"):
            asyncio.run(pause_schedule(DemoScheduleWorkflow, schedule_name=""))


class TestResumeSchedule:
    """Tests for resume_schedule function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
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

    def test_resume_schedule_success(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test resuming a schedule successfully."""
        response = pb2.UpdateScheduleStatusResponse(success=True)
        mock_stub.UpdateScheduleStatus.return_value = response

        result = asyncio.run(resume_schedule(DemoScheduleWorkflow, schedule_name="test-schedule"))

        assert result is True
        call_args = mock_stub.UpdateScheduleStatus.call_args
        request = call_args[0][0]
        assert request.workflow_name == "demoscheduleworkflow"
        assert request.schedule_name == "test-schedule"
        assert request.status == pb2.SCHEDULE_STATUS_ACTIVE

    def test_resume_schedule_not_found(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test resuming a schedule that doesn't exist."""
        response = pb2.UpdateScheduleStatusResponse(success=False)
        mock_stub.UpdateScheduleStatus.return_value = response

        result = asyncio.run(resume_schedule(DemoScheduleWorkflow, schedule_name="nonexistent"))

        assert result is False

    def test_resume_schedule_empty_name(self) -> None:
        """Test that empty schedule_name raises ValueError."""
        with pytest.raises(ValueError, match="schedule_name is required"):
            asyncio.run(resume_schedule(DemoScheduleWorkflow, schedule_name=""))


class TestDeleteSchedule:
    """Tests for delete_schedule function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
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

    def test_delete_schedule_success(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test deleting a schedule successfully."""
        response = pb2.DeleteScheduleResponse(success=True)
        mock_stub.DeleteSchedule.return_value = response

        result = asyncio.run(delete_schedule(DemoScheduleWorkflow, schedule_name="test-schedule"))

        assert result is True
        call_args = mock_stub.DeleteSchedule.call_args
        request = call_args[0][0]
        assert request.workflow_name == "demoscheduleworkflow"
        assert request.schedule_name == "test-schedule"

    def test_delete_schedule_not_found(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test deleting a schedule that doesn't exist."""
        response = pb2.DeleteScheduleResponse(success=False)
        mock_stub.DeleteSchedule.return_value = response

        result = asyncio.run(delete_schedule(DemoScheduleWorkflow, schedule_name="nonexistent"))

        assert result is False

    def test_delete_schedule_empty_name(self) -> None:
        """Test that empty schedule_name raises ValueError."""
        with pytest.raises(ValueError, match="schedule_name is required"):
            asyncio.run(delete_schedule(DemoScheduleWorkflow, schedule_name=""))
