from typing import Any, Iterator, Protocol

Struct = Any
Value = Any

class _ProtoMessage(Protocol):
    def SerializeToString(self) -> bytes: ...
    def ParseFromString(self, data: bytes) -> None: ...

class Ack(_ProtoMessage):
    def __init__(self, acked_delivery_id: int = ...) -> None: ...
    acked_delivery_id: int

class ActionDispatch(_ProtoMessage):
    def __init__(
        self,
        action_id: str = ...,
        instance_id: str = ...,
        sequence: int = ...,
        payload: bytes = ...,
    ) -> None: ...
    action_id: str
    instance_id: str
    sequence: int
    payload: bytes

class ActionResult(_ProtoMessage):
    def __init__(
        self,
        action_id: str = ...,
        success: bool = ...,
        payload: bytes = ...,
        worker_start_ns: int = ...,
        worker_end_ns: int = ...,
    ) -> None: ...
    action_id: str
    success: bool
    payload: bytes
    worker_start_ns: int
    worker_end_ns: int

class Envelope(_ProtoMessage):
    def __init__(
        self,
        delivery_id: int = ...,
        partition_id: int = ...,
        kind: int = ...,
        payload: bytes = ...,
    ) -> None: ...
    delivery_id: int
    partition_id: int
    kind: int
    payload: bytes

class MessageKind:
    MESSAGE_KIND_UNSPECIFIED: int
    MESSAGE_KIND_ACTION_DISPATCH: int
    MESSAGE_KIND_ACTION_RESULT: int
    MESSAGE_KIND_ACK: int
    MESSAGE_KIND_HEARTBEAT: int
    MESSAGE_KIND_WORKER_HELLO: int

class WorkerHello(_ProtoMessage):
    def __init__(self, worker_id: int = ...) -> None: ...
    worker_id: int

class WorkflowExceptionEdge(_ProtoMessage):
    def __init__(
        self,
        source_node_id: str = ...,
        exception_type: str = ...,
        exception_module: str = ...,
    ) -> None: ...
    source_node_id: str
    exception_type: str
    exception_module: str

class _ExceptionEdgeContainer(Protocol):
    def add(self) -> WorkflowExceptionEdge: ...
    def __iter__(self) -> Iterator[WorkflowExceptionEdge]: ...

class WorkflowDagNode(_ProtoMessage):
    def __init__(
        self,
        id: str = ...,
        action: str = ...,
        kwargs: dict[str, str] | None = ...,
        depends_on: list[str] | None = ...,
        wait_for_sync: list[str] | None = ...,
        produces: list[str] | None = ...,
        module: str = ...,
        guard: str = ...,
        exception_edges: list[WorkflowExceptionEdge] | None = ...,
    ) -> None: ...
    id: str
    action: str
    kwargs: dict[str, str]
    depends_on: list[str]
    wait_for_sync: list[str]
    produces: list[str]
    module: str
    guard: str
    exception_edges: _ExceptionEdgeContainer

class WorkflowDagDefinition(_ProtoMessage):
    def __init__(
        self,
        concurrent: bool = ...,
        nodes: list[WorkflowDagNode] | None = ...,
        return_variable: str = ...,
    ) -> None: ...
    concurrent: bool
    nodes: list[WorkflowDagNode]
    return_variable: str

class WorkflowRegistration(_ProtoMessage):
    def __init__(
        self,
        workflow_name: str = ...,
        dag: WorkflowDagDefinition | None = ...,
        dag_hash: str = ...,
    ) -> None: ...
    workflow_name: str
    dag: WorkflowDagDefinition
    dag_hash: str

class WorkflowNodeContext(_ProtoMessage):
    def __init__(
        self,
        variable: str = ...,
        payload: bytes = ...,
        workflow_node_id: str = ...,
    ) -> None: ...
    variable: str
    payload: bytes
    workflow_node_id: str

class WorkflowArgumentValue(_ProtoMessage):
    def __init__(
        self,
        primitive: PrimitiveWorkflowArgument | None = ...,
        basemodel: BaseModelWorkflowArgument | None = ...,
        exception: WorkflowErrorValue | None = ...,
    ) -> None: ...
    primitive: PrimitiveWorkflowArgument
    basemodel: BaseModelWorkflowArgument
    exception: WorkflowErrorValue

class PrimitiveWorkflowArgument(_ProtoMessage):
    def __init__(self, value: Value | None = ...) -> None: ...
    value: Value

class BaseModelWorkflowArgument(_ProtoMessage):
    def __init__(self, module: str = ..., name: str = ..., data: Struct | None = ...) -> None: ...
    module: str
    name: str
    data: Struct

class WorkflowErrorValue(_ProtoMessage):
    def __init__(
        self,
        type: str = ...,
        module: str = ...,
        message: str = ...,
        traceback: str = ...,
    ) -> None: ...
    type: str
    module: str
    message: str
    traceback: str

class WorkflowNodeDispatch(_ProtoMessage):
    def __init__(
        self,
        node: WorkflowDagNode | None = ...,
        workflow_input: bytes = ...,
        context: list[WorkflowNodeContext] | None = ...,
    ) -> None: ...
    node: WorkflowDagNode
    workflow_input: bytes
    context: list[WorkflowNodeContext]

class RegisterWorkflowRequest(_ProtoMessage):
    def __init__(
        self,
        registration: WorkflowRegistration | None = ...,
    ) -> None: ...
    registration: WorkflowRegistration

class RegisterWorkflowResponse(_ProtoMessage):
    def __init__(self, workflow_version_id: str = ...) -> None: ...
    workflow_version_id: str

class WaitForInstanceRequest(_ProtoMessage):
    def __init__(
        self,
        poll_interval_secs: float = ...,
    ) -> None: ...
    poll_interval_secs: float

class WaitForInstanceResponse(_ProtoMessage):
    def __init__(self, payload: bytes = ...) -> None: ...
    payload: bytes
