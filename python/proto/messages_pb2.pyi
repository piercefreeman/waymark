from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessageKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MESSAGE_KIND_UNSPECIFIED: _ClassVar[MessageKind]
    MESSAGE_KIND_ACTION_DISPATCH: _ClassVar[MessageKind]
    MESSAGE_KIND_ACTION_RESULT: _ClassVar[MessageKind]
    MESSAGE_KIND_ACK: _ClassVar[MessageKind]
    MESSAGE_KIND_HEARTBEAT: _ClassVar[MessageKind]
    MESSAGE_KIND_WORKER_HELLO: _ClassVar[MessageKind]

class BinOpKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BIN_OP_KIND_UNSPECIFIED: _ClassVar[BinOpKind]
    BIN_OP_KIND_ADD: _ClassVar[BinOpKind]
    BIN_OP_KIND_SUB: _ClassVar[BinOpKind]
    BIN_OP_KIND_MULT: _ClassVar[BinOpKind]
    BIN_OP_KIND_DIV: _ClassVar[BinOpKind]
    BIN_OP_KIND_MOD: _ClassVar[BinOpKind]
    BIN_OP_KIND_FLOORDIV: _ClassVar[BinOpKind]
    BIN_OP_KIND_POW: _ClassVar[BinOpKind]

class BoolOpKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BOOL_OP_KIND_UNSPECIFIED: _ClassVar[BoolOpKind]
    BOOL_OP_KIND_AND: _ClassVar[BoolOpKind]
    BOOL_OP_KIND_OR: _ClassVar[BoolOpKind]

class CmpOpKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CMP_OP_KIND_UNSPECIFIED: _ClassVar[CmpOpKind]
    CMP_OP_KIND_EQ: _ClassVar[CmpOpKind]
    CMP_OP_KIND_NOT_EQ: _ClassVar[CmpOpKind]
    CMP_OP_KIND_LT: _ClassVar[CmpOpKind]
    CMP_OP_KIND_LT_E: _ClassVar[CmpOpKind]
    CMP_OP_KIND_GT: _ClassVar[CmpOpKind]
    CMP_OP_KIND_GT_E: _ClassVar[CmpOpKind]
    CMP_OP_KIND_IN: _ClassVar[CmpOpKind]
    CMP_OP_KIND_NOT_IN: _ClassVar[CmpOpKind]
    CMP_OP_KIND_IS: _ClassVar[CmpOpKind]
    CMP_OP_KIND_IS_NOT: _ClassVar[CmpOpKind]

class UnaryOpKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNARY_OP_KIND_UNSPECIFIED: _ClassVar[UnaryOpKind]
    UNARY_OP_KIND_USUB: _ClassVar[UnaryOpKind]
    UNARY_OP_KIND_UADD: _ClassVar[UnaryOpKind]
    UNARY_OP_KIND_NOT: _ClassVar[UnaryOpKind]
MESSAGE_KIND_UNSPECIFIED: MessageKind
MESSAGE_KIND_ACTION_DISPATCH: MessageKind
MESSAGE_KIND_ACTION_RESULT: MessageKind
MESSAGE_KIND_ACK: MessageKind
MESSAGE_KIND_HEARTBEAT: MessageKind
MESSAGE_KIND_WORKER_HELLO: MessageKind
BIN_OP_KIND_UNSPECIFIED: BinOpKind
BIN_OP_KIND_ADD: BinOpKind
BIN_OP_KIND_SUB: BinOpKind
BIN_OP_KIND_MULT: BinOpKind
BIN_OP_KIND_DIV: BinOpKind
BIN_OP_KIND_MOD: BinOpKind
BIN_OP_KIND_FLOORDIV: BinOpKind
BIN_OP_KIND_POW: BinOpKind
BOOL_OP_KIND_UNSPECIFIED: BoolOpKind
BOOL_OP_KIND_AND: BoolOpKind
BOOL_OP_KIND_OR: BoolOpKind
CMP_OP_KIND_UNSPECIFIED: CmpOpKind
CMP_OP_KIND_EQ: CmpOpKind
CMP_OP_KIND_NOT_EQ: CmpOpKind
CMP_OP_KIND_LT: CmpOpKind
CMP_OP_KIND_LT_E: CmpOpKind
CMP_OP_KIND_GT: CmpOpKind
CMP_OP_KIND_GT_E: CmpOpKind
CMP_OP_KIND_IN: CmpOpKind
CMP_OP_KIND_NOT_IN: CmpOpKind
CMP_OP_KIND_IS: CmpOpKind
CMP_OP_KIND_IS_NOT: CmpOpKind
UNARY_OP_KIND_UNSPECIFIED: UnaryOpKind
UNARY_OP_KIND_USUB: UnaryOpKind
UNARY_OP_KIND_UADD: UnaryOpKind
UNARY_OP_KIND_NOT: UnaryOpKind

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
    __slots__ = ("action_id", "instance_id", "sequence", "dispatch", "timeout_seconds", "max_retries", "attempt_number", "dispatch_token")
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_NUMBER_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    instance_id: str
    sequence: int
    dispatch: WorkflowNodeDispatch
    timeout_seconds: int
    max_retries: int
    attempt_number: int
    dispatch_token: str
    def __init__(self, action_id: _Optional[str] = ..., instance_id: _Optional[str] = ..., sequence: _Optional[int] = ..., dispatch: _Optional[_Union[WorkflowNodeDispatch, _Mapping]] = ..., timeout_seconds: _Optional[int] = ..., max_retries: _Optional[int] = ..., attempt_number: _Optional[int] = ..., dispatch_token: _Optional[str] = ...) -> None: ...

class ActionResult(_message.Message):
    __slots__ = ("action_id", "success", "payload", "worker_start_ns", "worker_end_ns", "dispatch_token", "control")
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WORKER_START_NS_FIELD_NUMBER: _ClassVar[int]
    WORKER_END_NS_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CONTROL_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    success: bool
    payload: WorkflowArguments
    worker_start_ns: int
    worker_end_ns: int
    dispatch_token: str
    control: WorkflowNodeControl
    def __init__(self, action_id: _Optional[str] = ..., success: bool = ..., payload: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., worker_start_ns: _Optional[int] = ..., worker_end_ns: _Optional[int] = ..., dispatch_token: _Optional[str] = ..., control: _Optional[_Union[WorkflowNodeControl, _Mapping]] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("acked_delivery_id",)
    ACKED_DELIVERY_ID_FIELD_NUMBER: _ClassVar[int]
    acked_delivery_id: int
    def __init__(self, acked_delivery_id: _Optional[int] = ...) -> None: ...

class WorkerHello(_message.Message):
    __slots__ = ("worker_id",)
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    worker_id: int
    def __init__(self, worker_id: _Optional[int] = ...) -> None: ...

class BackoffPolicy(_message.Message):
    __slots__ = ("linear", "exponential")
    LINEAR_FIELD_NUMBER: _ClassVar[int]
    EXPONENTIAL_FIELD_NUMBER: _ClassVar[int]
    linear: LinearBackoff
    exponential: ExponentialBackoff
    def __init__(self, linear: _Optional[_Union[LinearBackoff, _Mapping]] = ..., exponential: _Optional[_Union[ExponentialBackoff, _Mapping]] = ...) -> None: ...

class LinearBackoff(_message.Message):
    __slots__ = ("base_delay_ms",)
    BASE_DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    base_delay_ms: int
    def __init__(self, base_delay_ms: _Optional[int] = ...) -> None: ...

class ExponentialBackoff(_message.Message):
    __slots__ = ("base_delay_ms", "multiplier")
    BASE_DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    base_delay_ms: int
    multiplier: float
    def __init__(self, base_delay_ms: _Optional[int] = ..., multiplier: _Optional[float] = ...) -> None: ...

class WorkflowDagNode(_message.Message):
    __slots__ = ("id", "action", "kwargs", "depends_on", "wait_for_sync", "produces", "module", "guard", "exception_edges", "timeout_seconds", "max_retries", "timeout_retry_limit", "loop", "ast", "backoff")
    class KwargsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    WAIT_FOR_SYNC_FIELD_NUMBER: _ClassVar[int]
    PRODUCES_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    GUARD_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_EDGES_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_RETRY_LIMIT_FIELD_NUMBER: _ClassVar[int]
    LOOP_FIELD_NUMBER: _ClassVar[int]
    AST_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_FIELD_NUMBER: _ClassVar[int]
    id: str
    action: str
    kwargs: _containers.ScalarMap[str, str]
    depends_on: _containers.RepeatedScalarFieldContainer[str]
    wait_for_sync: _containers.RepeatedScalarFieldContainer[str]
    produces: _containers.RepeatedScalarFieldContainer[str]
    module: str
    guard: str
    exception_edges: _containers.RepeatedCompositeFieldContainer[WorkflowExceptionEdge]
    timeout_seconds: int
    max_retries: int
    timeout_retry_limit: int
    loop: WorkflowLoopSpec
    ast: WorkflowNodeAst
    backoff: BackoffPolicy
    def __init__(self, id: _Optional[str] = ..., action: _Optional[str] = ..., kwargs: _Optional[_Mapping[str, str]] = ..., depends_on: _Optional[_Iterable[str]] = ..., wait_for_sync: _Optional[_Iterable[str]] = ..., produces: _Optional[_Iterable[str]] = ..., module: _Optional[str] = ..., guard: _Optional[str] = ..., exception_edges: _Optional[_Iterable[_Union[WorkflowExceptionEdge, _Mapping]]] = ..., timeout_seconds: _Optional[int] = ..., max_retries: _Optional[int] = ..., timeout_retry_limit: _Optional[int] = ..., loop: _Optional[_Union[WorkflowLoopSpec, _Mapping]] = ..., ast: _Optional[_Union[WorkflowNodeAst, _Mapping]] = ..., backoff: _Optional[_Union[BackoffPolicy, _Mapping]] = ...) -> None: ...

class WorkflowDagDefinition(_message.Message):
    __slots__ = ("concurrent", "nodes", "return_variable")
    CONCURRENT_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    RETURN_VARIABLE_FIELD_NUMBER: _ClassVar[int]
    concurrent: bool
    nodes: _containers.RepeatedCompositeFieldContainer[WorkflowDagNode]
    return_variable: str
    def __init__(self, concurrent: bool = ..., nodes: _Optional[_Iterable[_Union[WorkflowDagNode, _Mapping]]] = ..., return_variable: _Optional[str] = ...) -> None: ...

class WorkflowExceptionEdge(_message.Message):
    __slots__ = ("source_node_id", "exception_type", "exception_module")
    SOURCE_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXCEPTION_MODULE_FIELD_NUMBER: _ClassVar[int]
    source_node_id: str
    exception_type: str
    exception_module: str
    def __init__(self, source_node_id: _Optional[str] = ..., exception_type: _Optional[str] = ..., exception_module: _Optional[str] = ...) -> None: ...

class WorkflowLoopSpec(_message.Message):
    __slots__ = ("iterable_expr", "loop_var", "accumulator", "preamble", "body_action", "body_module", "body_kwargs")
    class BodyKwargsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ITERABLE_EXPR_FIELD_NUMBER: _ClassVar[int]
    LOOP_VAR_FIELD_NUMBER: _ClassVar[int]
    ACCUMULATOR_FIELD_NUMBER: _ClassVar[int]
    PREAMBLE_FIELD_NUMBER: _ClassVar[int]
    BODY_ACTION_FIELD_NUMBER: _ClassVar[int]
    BODY_MODULE_FIELD_NUMBER: _ClassVar[int]
    BODY_KWARGS_FIELD_NUMBER: _ClassVar[int]
    iterable_expr: str
    loop_var: str
    accumulator: str
    preamble: str
    body_action: str
    body_module: str
    body_kwargs: _containers.ScalarMap[str, str]
    def __init__(self, iterable_expr: _Optional[str] = ..., loop_var: _Optional[str] = ..., accumulator: _Optional[str] = ..., preamble: _Optional[str] = ..., body_action: _Optional[str] = ..., body_module: _Optional[str] = ..., body_kwargs: _Optional[_Mapping[str, str]] = ...) -> None: ...

class WorkflowLoopControl(_message.Message):
    __slots__ = ("node_id", "next_index", "has_next", "accumulator", "accumulator_value", "completed_phases", "phase_results", "current_phase")
    class PhaseResultsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: WorkflowArgumentValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[WorkflowArgumentValue, _Mapping]] = ...) -> None: ...
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    NEXT_INDEX_FIELD_NUMBER: _ClassVar[int]
    HAS_NEXT_FIELD_NUMBER: _ClassVar[int]
    ACCUMULATOR_FIELD_NUMBER: _ClassVar[int]
    ACCUMULATOR_VALUE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_PHASES_FIELD_NUMBER: _ClassVar[int]
    PHASE_RESULTS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PHASE_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    next_index: int
    has_next: bool
    accumulator: str
    accumulator_value: WorkflowArgumentValue
    completed_phases: _containers.RepeatedScalarFieldContainer[str]
    phase_results: _containers.MessageMap[str, WorkflowArgumentValue]
    current_phase: str
    def __init__(self, node_id: _Optional[str] = ..., next_index: _Optional[int] = ..., has_next: bool = ..., accumulator: _Optional[str] = ..., accumulator_value: _Optional[_Union[WorkflowArgumentValue, _Mapping]] = ..., completed_phases: _Optional[_Iterable[str]] = ..., phase_results: _Optional[_Mapping[str, WorkflowArgumentValue]] = ..., current_phase: _Optional[str] = ...) -> None: ...

class WorkflowNodeControl(_message.Message):
    __slots__ = ("loop",)
    LOOP_FIELD_NUMBER: _ClassVar[int]
    loop: WorkflowLoopControl
    def __init__(self, loop: _Optional[_Union[WorkflowLoopControl, _Mapping]] = ...) -> None: ...

class WorkflowNodeAst(_message.Message):
    __slots__ = ("kwargs", "guard", "loop", "sleep_duration")
    class KwargsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Expr
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    GUARD_FIELD_NUMBER: _ClassVar[int]
    LOOP_FIELD_NUMBER: _ClassVar[int]
    SLEEP_DURATION_FIELD_NUMBER: _ClassVar[int]
    kwargs: _containers.MessageMap[str, Expr]
    guard: Expr
    loop: LoopAst
    sleep_duration: Expr
    def __init__(self, kwargs: _Optional[_Mapping[str, Expr]] = ..., guard: _Optional[_Union[Expr, _Mapping]] = ..., loop: _Optional[_Union[LoopAst, _Mapping]] = ..., sleep_duration: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class LoopAst(_message.Message):
    __slots__ = ("iterable", "loop_var", "accumulator", "preamble", "body_action", "body_graph")
    ITERABLE_FIELD_NUMBER: _ClassVar[int]
    LOOP_VAR_FIELD_NUMBER: _ClassVar[int]
    ACCUMULATOR_FIELD_NUMBER: _ClassVar[int]
    PREAMBLE_FIELD_NUMBER: _ClassVar[int]
    BODY_ACTION_FIELD_NUMBER: _ClassVar[int]
    BODY_GRAPH_FIELD_NUMBER: _ClassVar[int]
    iterable: Expr
    loop_var: str
    accumulator: str
    preamble: _containers.RepeatedCompositeFieldContainer[Stmt]
    body_action: ActionAst
    body_graph: LoopBodyGraph
    def __init__(self, iterable: _Optional[_Union[Expr, _Mapping]] = ..., loop_var: _Optional[str] = ..., accumulator: _Optional[str] = ..., preamble: _Optional[_Iterable[_Union[Stmt, _Mapping]]] = ..., body_action: _Optional[_Union[ActionAst, _Mapping]] = ..., body_graph: _Optional[_Union[LoopBodyGraph, _Mapping]] = ...) -> None: ...

class LoopBodyGraph(_message.Message):
    __slots__ = ("nodes", "result_variable")
    NODES_FIELD_NUMBER: _ClassVar[int]
    RESULT_VARIABLE_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[LoopBodyNode]
    result_variable: str
    def __init__(self, nodes: _Optional[_Iterable[_Union[LoopBodyNode, _Mapping]]] = ..., result_variable: _Optional[str] = ...) -> None: ...

class LoopBodyNode(_message.Message):
    __slots__ = ("id", "action", "module", "kwargs", "depends_on", "output_var", "timeout_seconds", "max_retries")
    ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_VAR_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    id: str
    action: str
    module: str
    kwargs: _containers.RepeatedCompositeFieldContainer[Keyword]
    depends_on: _containers.RepeatedScalarFieldContainer[str]
    output_var: str
    timeout_seconds: int
    max_retries: int
    def __init__(self, id: _Optional[str] = ..., action: _Optional[str] = ..., module: _Optional[str] = ..., kwargs: _Optional[_Iterable[_Union[Keyword, _Mapping]]] = ..., depends_on: _Optional[_Iterable[str]] = ..., output_var: _Optional[str] = ..., timeout_seconds: _Optional[int] = ..., max_retries: _Optional[int] = ...) -> None: ...

class ActionAst(_message.Message):
    __slots__ = ("action_name", "module_name", "keywords")
    ACTION_NAME_FIELD_NUMBER: _ClassVar[int]
    MODULE_NAME_FIELD_NUMBER: _ClassVar[int]
    KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    action_name: str
    module_name: str
    keywords: _containers.RepeatedCompositeFieldContainer[Keyword]
    def __init__(self, action_name: _Optional[str] = ..., module_name: _Optional[str] = ..., keywords: _Optional[_Iterable[_Union[Keyword, _Mapping]]] = ...) -> None: ...

class Keyword(_message.Message):
    __slots__ = ("arg", "value")
    ARG_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    arg: str
    value: Expr
    def __init__(self, arg: _Optional[str] = ..., value: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class Stmt(_message.Message):
    __slots__ = ("assign", "expr")
    ASSIGN_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    assign: Assign
    expr: Expr
    def __init__(self, assign: _Optional[_Union[Assign, _Mapping]] = ..., expr: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class Assign(_message.Message):
    __slots__ = ("targets", "value")
    TARGETS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    targets: _containers.RepeatedCompositeFieldContainer[Expr]
    value: Expr
    def __init__(self, targets: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ..., value: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class Expr(_message.Message):
    __slots__ = ("name", "constant", "attribute", "subscript", "bin_op", "bool_op", "compare", "call", "list", "tuple", "dict", "unary_op")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONSTANT_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPT_FIELD_NUMBER: _ClassVar[int]
    BIN_OP_FIELD_NUMBER: _ClassVar[int]
    BOOL_OP_FIELD_NUMBER: _ClassVar[int]
    COMPARE_FIELD_NUMBER: _ClassVar[int]
    CALL_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    TUPLE_FIELD_NUMBER: _ClassVar[int]
    DICT_FIELD_NUMBER: _ClassVar[int]
    UNARY_OP_FIELD_NUMBER: _ClassVar[int]
    name: Name
    constant: Constant
    attribute: Attribute
    subscript: Subscript
    bin_op: BinOp
    bool_op: BoolOp
    compare: Compare
    call: Call
    list: List
    tuple: Tuple
    dict: Dict
    unary_op: UnaryOp
    def __init__(self, name: _Optional[_Union[Name, _Mapping]] = ..., constant: _Optional[_Union[Constant, _Mapping]] = ..., attribute: _Optional[_Union[Attribute, _Mapping]] = ..., subscript: _Optional[_Union[Subscript, _Mapping]] = ..., bin_op: _Optional[_Union[BinOp, _Mapping]] = ..., bool_op: _Optional[_Union[BoolOp, _Mapping]] = ..., compare: _Optional[_Union[Compare, _Mapping]] = ..., call: _Optional[_Union[Call, _Mapping]] = ..., list: _Optional[_Union[List, _Mapping]] = ..., tuple: _Optional[_Union[Tuple, _Mapping]] = ..., dict: _Optional[_Union[Dict, _Mapping]] = ..., unary_op: _Optional[_Union[UnaryOp, _Mapping]] = ...) -> None: ...

class Name(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class Constant(_message.Message):
    __slots__ = ("string_value", "float_value", "int_value", "bool_value", "is_none")
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    IS_NONE_FIELD_NUMBER: _ClassVar[int]
    string_value: str
    float_value: float
    int_value: int
    bool_value: bool
    is_none: bool
    def __init__(self, string_value: _Optional[str] = ..., float_value: _Optional[float] = ..., int_value: _Optional[int] = ..., bool_value: bool = ..., is_none: bool = ...) -> None: ...

class Attribute(_message.Message):
    __slots__ = ("value", "attr")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ATTR_FIELD_NUMBER: _ClassVar[int]
    value: Expr
    attr: str
    def __init__(self, value: _Optional[_Union[Expr, _Mapping]] = ..., attr: _Optional[str] = ...) -> None: ...

class Subscript(_message.Message):
    __slots__ = ("value", "slice")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    SLICE_FIELD_NUMBER: _ClassVar[int]
    value: Expr
    slice: Expr
    def __init__(self, value: _Optional[_Union[Expr, _Mapping]] = ..., slice: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class BinOp(_message.Message):
    __slots__ = ("left", "op", "right")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    OP_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    left: Expr
    op: BinOpKind
    right: Expr
    def __init__(self, left: _Optional[_Union[Expr, _Mapping]] = ..., op: _Optional[_Union[BinOpKind, str]] = ..., right: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class BoolOp(_message.Message):
    __slots__ = ("op", "values")
    OP_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    op: BoolOpKind
    values: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(self, op: _Optional[_Union[BoolOpKind, str]] = ..., values: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...) -> None: ...

class Compare(_message.Message):
    __slots__ = ("left", "ops", "comparators")
    LEFT_FIELD_NUMBER: _ClassVar[int]
    OPS_FIELD_NUMBER: _ClassVar[int]
    COMPARATORS_FIELD_NUMBER: _ClassVar[int]
    left: Expr
    ops: _containers.RepeatedScalarFieldContainer[CmpOpKind]
    comparators: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(self, left: _Optional[_Union[Expr, _Mapping]] = ..., ops: _Optional[_Iterable[_Union[CmpOpKind, str]]] = ..., comparators: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...) -> None: ...

class Call(_message.Message):
    __slots__ = ("func", "args", "keywords")
    FUNC_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    func: Expr
    args: _containers.RepeatedCompositeFieldContainer[Expr]
    keywords: _containers.RepeatedCompositeFieldContainer[Keyword]
    def __init__(self, func: _Optional[_Union[Expr, _Mapping]] = ..., args: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ..., keywords: _Optional[_Iterable[_Union[Keyword, _Mapping]]] = ...) -> None: ...

class UnaryOp(_message.Message):
    __slots__ = ("op", "operand")
    OP_FIELD_NUMBER: _ClassVar[int]
    OPERAND_FIELD_NUMBER: _ClassVar[int]
    op: UnaryOpKind
    operand: Expr
    def __init__(self, op: _Optional[_Union[UnaryOpKind, str]] = ..., operand: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class List(_message.Message):
    __slots__ = ("elts",)
    ELTS_FIELD_NUMBER: _ClassVar[int]
    elts: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(self, elts: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...) -> None: ...

class Tuple(_message.Message):
    __slots__ = ("elts",)
    ELTS_FIELD_NUMBER: _ClassVar[int]
    elts: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(self, elts: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...) -> None: ...

class Dict(_message.Message):
    __slots__ = ("keys", "values")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[Expr]
    values: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(self, keys: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ..., values: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...) -> None: ...

class WorkflowRegistration(_message.Message):
    __slots__ = ("workflow_name", "dag", "dag_hash", "initial_context")
    WORKFLOW_NAME_FIELD_NUMBER: _ClassVar[int]
    DAG_FIELD_NUMBER: _ClassVar[int]
    DAG_HASH_FIELD_NUMBER: _ClassVar[int]
    INITIAL_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    workflow_name: str
    dag: WorkflowDagDefinition
    dag_hash: str
    initial_context: WorkflowArguments
    def __init__(self, workflow_name: _Optional[str] = ..., dag: _Optional[_Union[WorkflowDagDefinition, _Mapping]] = ..., dag_hash: _Optional[str] = ..., initial_context: _Optional[_Union[WorkflowArguments, _Mapping]] = ...) -> None: ...

class WorkflowNodeContext(_message.Message):
    __slots__ = ("variable", "payload", "workflow_node_id")
    VARIABLE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    variable: str
    payload: WorkflowArguments
    workflow_node_id: str
    def __init__(self, variable: _Optional[str] = ..., payload: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., workflow_node_id: _Optional[str] = ...) -> None: ...

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

class WorkflowNodeDispatch(_message.Message):
    __slots__ = ("node", "workflow_input", "context", "resolved_kwargs")
    NODE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_INPUT_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_KWARGS_FIELD_NUMBER: _ClassVar[int]
    node: WorkflowDagNode
    workflow_input: WorkflowArguments
    context: _containers.RepeatedCompositeFieldContainer[WorkflowNodeContext]
    resolved_kwargs: WorkflowArguments
    def __init__(self, node: _Optional[_Union[WorkflowDagNode, _Mapping]] = ..., workflow_input: _Optional[_Union[WorkflowArguments, _Mapping]] = ..., context: _Optional[_Iterable[_Union[WorkflowNodeContext, _Mapping]]] = ..., resolved_kwargs: _Optional[_Union[WorkflowArguments, _Mapping]] = ...) -> None: ...

class RegisterWorkflowRequest(_message.Message):
    __slots__ = ("registration",)
    REGISTRATION_FIELD_NUMBER: _ClassVar[int]
    registration: WorkflowRegistration
    def __init__(self, registration: _Optional[_Union[WorkflowRegistration, _Mapping]] = ...) -> None: ...

class RegisterWorkflowResponse(_message.Message):
    __slots__ = ("workflow_version_id", "workflow_instance_id")
    WORKFLOW_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    workflow_version_id: str
    workflow_instance_id: str
    def __init__(self, workflow_version_id: _Optional[str] = ..., workflow_instance_id: _Optional[str] = ...) -> None: ...

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
