from __future__ import annotations

from typing import Any, Protocol

from proto.messages_pb2 import (
    RegisterWorkflowRequest,
    WaitForInstanceRequest,
    WaitForInstanceResponse,
)

GRPC_GENERATED_VERSION: str
GRPC_VERSION: str

class _Channel(Protocol):
    def unary_unary(self, *args: Any, **kwargs: Any) -> Any: ...

class WorkflowServiceStub:
    RegisterWorkflow: Any
    WaitForInstance: Any

    def __init__(self, channel: _Channel) -> None: ...

class WorkflowServiceServicer:
    def RegisterWorkflow(
        self, request: RegisterWorkflowRequest, context: Any
    ) -> WaitForInstanceResponse: ...
    def WaitForInstance(
        self, request: WaitForInstanceRequest, context: Any
    ) -> WaitForInstanceResponse: ...

def add_WorkflowServiceServicer_to_server(
    servicer: WorkflowServiceServicer, server: Any
) -> None: ...

class WorkflowService:
    @staticmethod
    def RegisterWorkflow(*args: Any, **kwargs: Any) -> Any: ...
    @staticmethod
    def WaitForInstance(*args: Any, **kwargs: Any) -> Any: ...
