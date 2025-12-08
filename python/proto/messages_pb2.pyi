from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WorkerType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WORKER_TYPE_UNSPECIFIED: _ClassVar[WorkerType]
    WORKER_TYPE_ACTION: _ClassVar[WorkerType]
    WORKER_TYPE_INSTANCE: _ClassVar[WorkerType]

class MessageKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MESSAGE_KIND_UNSPECIFIED: _ClassVar[MessageKind]
    MESSAGE_KIND_ACTION_DISPATCH: _ClassVar[MessageKind]
    MESSAGE_KIND_ACTION_RESULT: _ClassVar[MessageKind]
    MESSAGE_KIND_INSTANCE_DISPATCH: _ClassVar[MessageKind]
    MESSAGE_KIND_INSTANCE_ACTIONS: _ClassVar[MessageKind]
    MESSAGE_KIND_INSTANCE_COMPLETE: _ClassVar[MessageKind]
    MESSAGE_KIND_INSTANCE_FAILED: _ClassVar[MessageKind]
    MESSAGE_KIND_ACK: _ClassVar[MessageKind]
    MESSAGE_KIND_HEARTBEAT: _ClassVar[MessageKind]
    MESSAGE_KIND_WORKER_HELLO: _ClassVar[MessageKind]
WORKER_TYPE_UNSPECIFIED: WorkerType
WORKER_TYPE_ACTION: WorkerType
WORKER_TYPE_INSTANCE: WorkerType
MESSAGE_KIND_UNSPECIFIED: MessageKind
MESSAGE_KIND_ACTION_DISPATCH: MessageKind
MESSAGE_KIND_ACTION_RESULT: MessageKind
MESSAGE_KIND_INSTANCE_DISPATCH: MessageKind
MESSAGE_KIND_INSTANCE_ACTIONS: MessageKind
MESSAGE_KIND_INSTANCE_COMPLETE: MessageKind
MESSAGE_KIND_INSTANCE_FAILED: MessageKind
MESSAGE_KIND_ACK: MessageKind
MESSAGE_KIND_HEARTBEAT: MessageKind
MESSAGE_KIND_WORKER_HELLO: MessageKind

class Envelope(_message.Message):
    __slots__ = ("delivery_id", "partition_id", "kind", "payload")
    DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    delivery_id: int
    partition_id: int
    kind: MessageKind
    payload: bytes
    def __init__(self, delivery_id: _Optional[int] = ..., partition_id: _Optional[int] = ..., kind: _Optional[_Union[MessageKind, str]] = ..., payload: _Optional[bytes] = ...) -> None: ...

class ActionDispatch(_message.Message):
    __slots__ = ("action_id", "instance_id", "sequence", "action_name", "module_name", "kwargs", "timeout_seconds", "max_retries", "attempt_number", "dispatch_token")
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_NAME_FIELD_NUMBER: _ClassVar[int]
    MODULE_NAME_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_NUMBER_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    instance_id: str
    sequence: int
    action_name: str
    module_name: str
    kwargs: WorkflowArguments
    timeout_seconds: int
    max_retries: int
    attempt_number: int
    dispatch_token: str
    def __init__(self, action_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., sequence: _Optional[int] = ..., action_name: _Optional[str] = ..., module_name: _Optional[str] = ..., kwargs: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., timeout_seconds: _Optional[int] = ..., max_retries: _Optional[int] = ..., attempt_number: _Optional[int] = ..., dispatch_token: _Optional[str] = ...) -> None: ...

class ActionResult(_message.Message):
    __slots__ = ("action_id", "success", "payload", "worker_start_ns", "worker_end_ns", "dispatch_token", "error_type", "error_message")
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WORKER_START_NS_FIELD_NUMBER: _ClassVar[int]
    WORKER_END_NS_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    success: bool
    payload: WorkflowArguments
    worker_start_ns: int
    worker_end_ns: int
    dispatch_token: str
    error_type: str
    error_message: str
    def __init__(self, action_id: _Optional[str] = ..., success: bool = ..., payload: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., worker_start_ns: _Optional[int] = ..., worker_end_ns: _Optional[int] = ..., dispatch_token: _Optional[str] = ..., error_type: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class InstanceDispatch(_message.Message):
    __slots__ = ("instance_id", "workflow_name", "module_name", "actions_until_index", "initial_args", "completed_actions", "scheduled_at_ms", "dispatch_token")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_NAME_FIELD_NUMBER: _ClassVar[int]
    MODULE_NAME_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_UNTIL_INDEX_FIELD_NUMBER: _ClassVar[int]
    INITIAL_ARGS_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_ACTIONS_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    workflow_name: str
    module_name: str
    actions_until_index: int
    initial_args: WorkflowArguments
    completed_actions: _containers.RepeatedCompositeFieldContainer[ActionResult]
    scheduled_at_ms: int
    dispatch_token: str
    def __init__(self, instance_id: _Optional[str] = ..., workflow_name: _Optional[str] = ..., module_name: _Optional[str] = ..., actions_until_index: _Optional[int] = ..., initial_args: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., completed_actions: _Optional[_Iterable[_Union[ActionResult, _Mapping]]] = ..., scheduled_at_ms: _Optional[int] = ..., dispatch_token: _Optional[str] = ...) -> None: ...

class InstanceActions(_message.Message):
    __slots__ = ("instance_id", "replayed_count", "pending_actions", "dispatch_token")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_COUNT_FIELD_NUMBER: _ClassVar[int]
    PENDING_ACTIONS_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    replayed_count: int
    pending_actions: _containers.RepeatedCompositeFieldContainer[PendingAction]
    dispatch_token: str
    def __init__(self, instance_id: _Optional[str] = ..., replayed_count: _Optional[int] = ..., pending_actions: _Optional[_Iterable[_Union[PendingAction, _Mapping]]] = ..., dispatch_token: _Optional[str] = ...) -> None: ...

class PendingAction(_message.Message):
    __slots__ = ("action_id", "sequence", "action_name", "module_name", "kwargs", "timeout_seconds", "max_retries")
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_NAME_FIELD_NUMBER: _ClassVar[int]
    MODULE_NAME_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    sequence: int
    action_name: str
    module_name: str
    kwargs: WorkflowArguments
    timeout_seconds: int
    max_retries: int
    def __init__(self, action_id: _Optional[str] = ..., sequence: _Optional[int] = ..., action_name: _Optional[str] = ..., module_name: _Optional[str] = ..., kwargs: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., timeout_seconds: _Optional[int] = ..., max_retries: _Optional[int] = ...) -> None: ...

class InstanceComplete(_message.Message):
    __slots__ = ("instance_id", "result", "total_actions", "dispatch_token")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_ACTIONS_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    result: WorkflowArguments
    total_actions: int
    dispatch_token: str
    def __init__(self, instance_id: _Optional[str] = ..., result: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., total_actions: _Optional[int] = ..., dispatch_token: _Optional[str] = ...) -> None: ...

class InstanceFailed(_message.Message):
    __slots__ = ("instance_id", "error_type", "error_message", "traceback", "actions_completed", "dispatch_token")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TRACEBACK_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    error_type: str
    error_message: str
    traceback: str
    actions_completed: int
    dispatch_token: str
    def __init__(self, instance_id: _Optional[str] = ..., error_type: _Optional[str] = ..., error_message: _Optional[str] = ..., traceback: _Optional[str] = ..., actions_completed: _Optional[int] = ..., dispatch_token: _Optional[str] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("acked_delivery_id",)
    ACKED_DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    acked_delivery_id: int
    def __init__(self, acked_delivery_id: _Optional[int] = ...) -> None: ...

class WorkerHello(_message.Message):
    __slots__ = ("worker_id", "worker_type")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_TYPE_FIELD_NUMBER: _ClassVar[int]
    worker_id: int
    worker_type: WorkerType
    def __init__(self, worker_id: _Optional[int] = ..., worker_type: _Optional[_Union[WorkerType, str]] = ...) -> None: ...

class WorkflowArgumentValue(_message.Message):
    __slots__ = ("primitive", "basemodel", "exception", "list_value", "tuple_value", "dict_value")
    PRIMITIVE_FIELD_NUMBER: _ClassVar[int]
    BASEMODEL_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    LIST_VALUE_FIELD_NUMBER: _ClassVar[int]
    TUPLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    DICT_VALUE_FIELD_NUMBER: _ClassVar[int]
    primitive: PrimitiveWorkflowArgument
    basemodel: BaseModelWorkflowArgument
    exception: WorkflowErrorValue
    list_value: WorkflowListArgument
    tuple_value: WorkflowTupleArgument
    dict_value: WorkflowDictArgument
    def __init__(self, primitive: _Optional[_Union[PrimitiveWorkflowArgument, _Mapping]] = ..., basemodel: _Optional[_Union[BaseModelWorkflowArgument, _Mapping]] = ..., exception: _Optional[_Union[WorkflowErrorValue, _Mapping]] = ..., list_value: _Optional[_Union[WorkflowListArgument, _Mapping]] = ..., tuple_value: _Optional[_Union[WorkflowTupleArgument, _Mapping]] = ..., dict_value: _Optional[_Union[WorkflowDictArgument, _Mapping]] = ...) -> None: ...

class WorkflowArgument(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: WorkflowArgumentValue
    def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[WorkflowArgumentValue, _Mapping]] = ...) -> None: ...

class WorkflowArguments(_message.Message):
    __slots__ = ("arguments",)
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    arguments: _containers.RepeatedCompositeFieldContainer[WorkflowArgument]
    def __init__(self, arguments: _Optional[_Iterable[_Union[WorkflowArgument, _Mapping]]] = ...) -> None: ...

class PrimitiveWorkflowArgument(_message.Message):
    __slots__ = ("string_value", "double_value", "int_value", "bool_value", "null_value")
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    string_value: str
    double_value: float
    int_value: int
    bool_value: bool
    null_value: _struct_pb2.NullValue
    def __init__(self, string_value: _Optional[str] = ..., double_value: _Optional[float] = ..., int_value: _Optional[int] = ..., bool_value: bool = ..., null_value: _Optional[_Union[_struct_pb2.NullValue, str]] = ...) -> None: ...

class BaseModelWorkflowArgument(_message.Message):
    __slots__ = ("module", "name", "data")
    MODULE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    module: str
    name: str
    data: WorkflowDictArgument
    def __init__(self, module: _Optional[str] = ..., name: _Optional[str] = ..., data: _Optional[_Union[WorkflowDictArgument, _Mapping]] = ...) -> None: ...

class WorkflowErrorValue(_message.Message):
    __slots__ = ("type", "module", "message", "traceback")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TRACEBACK_FIELD_NUMBER: _ClassVar[int]
    type: str
    module: str
    message: str
    traceback: str
    def __init__(self, type: _Optional[str] = ..., module: _Optional[str] = ..., message: _Optional[str] = ..., traceback: _Optional[str] = ...) -> None: ...

class WorkflowListArgument(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[WorkflowArgumentValue]
    def __init__(self, items: _Optional[_Iterable[_Union[WorkflowArgumentValue, _Mapping]]] = ...) -> None: ...

class WorkflowTupleArgument(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[WorkflowArgumentValue]
    def __init__(self, items: _Optional[_Iterable[_Union[WorkflowArgumentValue, _Mapping]]] = ...) -> None: ...

class WorkflowDictArgument(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[WorkflowArgument]
    def __init__(self, entries: _Optional[_Iterable[_Union[WorkflowArgument, _Mapping]]] = ...) -> None: ...

class WorkflowRegistration(_message.Message):
    __slots__ = ("workflow_name", "module_name", "initial_args", "concurrent")
    WORKFLOW_NAME_FIELD_NUMBER: _ClassVar[int]
    MODULE_NAME_FIELD_NUMBER: _ClassVar[int]
    INITIAL_ARGS_FIELD_NUMBER: _ClassVar[int]
    CONCURRENT_FIELD_NUMBER: _ClassVar[int]
    workflow_name: str
    module_name: str
    initial_args: WorkflowArguments
    concurrent: bool
    def __init__(self, workflow_name: _Optional[str] = ..., module_name: _Optional[str] = ..., initial_args: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., concurrent: bool = ...) -> None: ...

class RegisterWorkflowRequest(_message.Message):
    __slots__ = ("registration",)
    REGISTRATION_FIELD_NUMBER: _ClassVar[int]
    registration: WorkflowRegistration
    def __init__(self, registration: _Optional[_Union[WorkflowRegistration, _Mapping]] = ...) -> None: ...

class RegisterWorkflowResponse(_message.Message):
    __slots__ = ("workflow_id", "instance_id")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    instance_id: str
    def __init__(self, workflow_id: _Optional[str] = ..., instance_id: _Optional[str] = ...) -> None: ...

class WaitForInstanceRequest(_message.Message):
    __slots__ = ("instance_id", "poll_interval_secs")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    POLL_INTERVAL_SECS_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    poll_interval_secs: float
    def __init__(self, instance_id: _Optional[str] = ..., poll_interval_secs: _Optional[float] = ...) -> None: ...

class WaitForInstanceResponse(_message.Message):
    __slots__ = ("payload",)
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    def __init__(self, payload: _Optional[bytes] = ...) -> None: ...
