"""Tests for bridge.py gRPC client functions."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import grpc
import pytest
from grpc import aio  # type: ignore[attr-defined]

from proto import messages_pb2 as pb2
from rappel import bridge
from rappel.bridge import RunInstanceResult, run_instance, wait_for_instance


class TestWorkflowStub:
    def test_workflow_stub_is_scoped_to_event_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RAPPEL_BRIDGE_GRPC_ADDR", "test:123")

        bridge._GRPC_TARGET = None
        bridge._GRPC_CHANNEL = None
        bridge._GRPC_STUB = None
        bridge._GRPC_LOOP = None

        created_channels: list[object] = []

        class FakeChannel:
            async def channel_ready(self) -> None:
                return None

        def fake_insecure_channel(_target: str) -> FakeChannel:
            channel = FakeChannel()
            created_channels.append(channel)
            return channel

        class FakeStub:
            def __init__(self, channel: FakeChannel) -> None:
                self.channel = channel

        monkeypatch.setattr(bridge.aio, "insecure_channel", fake_insecure_channel)
        monkeypatch.setattr(bridge.pb2_grpc, "WorkflowServiceStub", FakeStub)

        async def first_loop() -> object:
            stub_a = await bridge._workflow_stub()
            stub_b = await bridge._workflow_stub()
            assert stub_a is stub_b
            return stub_a

        async def second_loop() -> object:
            return await bridge._workflow_stub()

        first_stub = asyncio.run(first_loop())
        second_stub = asyncio.run(second_loop())

        assert first_stub is not second_stub
        assert len(created_channels) == 2


class TestRunInstance:
    """Tests for run_instance function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
        """Create a mock gRPC stub."""
        stub = AsyncMock()

        async def fake_workflow_stub() -> AsyncMock:
            return stub

        @asynccontextmanager
        async def fake_ensure_singleton():
            yield 8080

        monkeypatch.setattr(bridge, "_workflow_stub", fake_workflow_stub)
        monkeypatch.setattr(bridge, "ensure_singleton", fake_ensure_singleton)
        return stub

    def test_run_instance_success(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test successful workflow registration."""
        # Create a mock response
        response = pb2.RegisterWorkflowResponse(
            workflow_version_id="version-123",
            workflow_instance_id="instance-456",
        )
        mock_stub.RegisterWorkflow.return_value = response

        # Create a registration payload
        registration = pb2.WorkflowRegistration(
            workflow_name="testworkflow",
            ir=b"test-ir-bytes",
            ir_hash="abc123",
            concurrent=False,
        )
        payload = registration.SerializeToString()

        result = asyncio.run(run_instance(payload))

        assert isinstance(result, RunInstanceResult)
        assert result.workflow_version_id == "version-123"
        assert result.workflow_instance_id == "instance-456"

        # Verify the request was built correctly
        call_args = mock_stub.RegisterWorkflow.call_args
        request = call_args[0][0]
        assert request.registration.workflow_name == "testworkflow"
        assert request.registration.ir == b"test-ir-bytes"
        assert request.registration.ir_hash == "abc123"

    def test_run_instance_with_initial_context(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test workflow registration with initial context."""
        response = pb2.RegisterWorkflowResponse(
            workflow_version_id="version-789",
            workflow_instance_id="instance-012",
        )
        mock_stub.RegisterWorkflow.return_value = response

        # Create a registration with initial context
        initial_context = pb2.WorkflowArguments()
        arg = initial_context.arguments.add()
        arg.key = "name"
        arg.value.primitive.string_value = "hello"

        registration = pb2.WorkflowRegistration(
            workflow_name="contextworkflow",
            ir=b"ir-data",
            ir_hash="hash123",
            initial_context=initial_context,
        )
        payload = registration.SerializeToString()

        result = asyncio.run(run_instance(payload))

        assert result.workflow_version_id == "version-789"
        assert result.workflow_instance_id == "instance-012"

        # Verify initial context was included
        call_args = mock_stub.RegisterWorkflow.call_args
        request = call_args[0][0]
        assert request.registration.HasField("initial_context")

    def test_run_instance_grpc_error(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test that gRPC errors are wrapped in RuntimeError."""

        # Create an actual exception that inherits from AioRpcError behavior
        class MockAioRpcError(aio.AioRpcError, Exception):
            def __init__(self) -> None:
                pass

            def __str__(self) -> str:
                return "Connection refused"

        mock_stub.RegisterWorkflow.side_effect = MockAioRpcError()

        registration = pb2.WorkflowRegistration(
            workflow_name="errorworkflow",
            ir=b"ir",
            ir_hash="hash",
        )
        payload = registration.SerializeToString()

        with pytest.raises(RuntimeError, match="register_workflow failed"):
            asyncio.run(run_instance(payload))


class TestWaitForInstance:
    """Tests for wait_for_instance function."""

    @pytest.fixture
    def mock_stub(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
        """Create a mock gRPC stub."""
        stub = AsyncMock()

        async def fake_workflow_stub() -> AsyncMock:
            return stub

        @asynccontextmanager
        async def fake_ensure_singleton():
            yield 8080

        monkeypatch.setattr(bridge, "_workflow_stub", fake_workflow_stub)
        monkeypatch.setattr(bridge, "ensure_singleton", fake_ensure_singleton)
        return stub

    def test_wait_for_instance_success(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test successful wait for instance completion."""
        expected_payload = b"result-payload-bytes"
        response = pb2.WaitForInstanceResponse(payload=expected_payload)
        mock_stub.WaitForInstance.return_value = response

        result = asyncio.run(wait_for_instance("instance-123"))

        assert result == expected_payload

        # Verify the request was built correctly
        call_args = mock_stub.WaitForInstance.call_args
        request = call_args[0][0]
        assert request.instance_id == "instance-123"
        assert request.poll_interval_secs == 1.0  # default

    def test_wait_for_instance_custom_poll_interval(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test wait with custom poll interval."""
        response = pb2.WaitForInstanceResponse(payload=b"data")
        mock_stub.WaitForInstance.return_value = response

        asyncio.run(wait_for_instance("instance-456", poll_interval_secs=5.0))

        call_args = mock_stub.WaitForInstance.call_args
        request = call_args[0][0]
        assert request.poll_interval_secs == 5.0

    def test_wait_for_instance_not_found(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test that NOT_FOUND status returns None."""

        class MockNotFoundError(aio.AioRpcError, Exception):
            def __init__(self) -> None:
                pass

            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.NOT_FOUND

        mock_stub.WaitForInstance.side_effect = MockNotFoundError()

        result = asyncio.run(wait_for_instance("nonexistent-instance"))

        assert result is None

    def test_wait_for_instance_other_error(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test that other gRPC errors raise RuntimeError."""

        class MockInternalError(aio.AioRpcError, Exception):
            def __init__(self) -> None:
                pass

            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.INTERNAL

            def __str__(self) -> str:
                return "Internal error"

        mock_stub.WaitForInstance.side_effect = MockInternalError()

        with pytest.raises(RuntimeError, match="wait_for_instance failed"):
            asyncio.run(wait_for_instance("instance-error"))

    def test_wait_for_instance_empty_payload(
        self, monkeypatch: pytest.MonkeyPatch, mock_stub: AsyncMock
    ) -> None:
        """Test handling of empty payload response."""
        response = pb2.WaitForInstanceResponse(payload=b"")
        mock_stub.WaitForInstance.return_value = response

        result = asyncio.run(wait_for_instance("instance-empty"))

        assert result == b""
