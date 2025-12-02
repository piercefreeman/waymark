from typing import (
    ClassVar as _ClassVar,
)
from typing import (
    Iterable as _Iterable,
)
from typing import (
    Mapping as _Mapping,
)
from typing import (
    Optional as _Optional,
)
from typing import (
    Union as _Union,
)

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

DESCRIPTOR: _descriptor.FileDescriptor

class SourceLocation(_message.Message):
    __slots__ = ("lineno", "col_offset", "end_lineno", "end_col_offset")
    LINENO_FIELD_NUMBER: _ClassVar[int]
    COL_OFFSET_FIELD_NUMBER: _ClassVar[int]
    END_LINENO_FIELD_NUMBER: _ClassVar[int]
    END_COL_OFFSET_FIELD_NUMBER: _ClassVar[int]
    lineno: int
    col_offset: int
    end_lineno: int
    end_col_offset: int
    def __init__(
        self,
        lineno: _Optional[int] = ...,
        col_offset: _Optional[int] = ...,
        end_lineno: _Optional[int] = ...,
        end_col_offset: _Optional[int] = ...,
    ) -> None: ...

class BackoffConfig(_message.Message):
    __slots__ = ("kind", "base_delay_ms", "multiplier")
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        KIND_UNSPECIFIED: _ClassVar[BackoffConfig.Kind]
        KIND_LINEAR: _ClassVar[BackoffConfig.Kind]
        KIND_EXPONENTIAL: _ClassVar[BackoffConfig.Kind]

    KIND_UNSPECIFIED: BackoffConfig.Kind
    KIND_LINEAR: BackoffConfig.Kind
    KIND_EXPONENTIAL: BackoffConfig.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    BASE_DELAY_MS_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    kind: BackoffConfig.Kind
    base_delay_ms: int
    multiplier: float
    def __init__(
        self,
        kind: _Optional[_Union[BackoffConfig.Kind, str]] = ...,
        base_delay_ms: _Optional[int] = ...,
        multiplier: _Optional[float] = ...,
    ) -> None: ...

class RunActionConfig(_message.Message):
    __slots__ = ("timeout_seconds", "max_retries", "backoff")
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_FIELD_NUMBER: _ClassVar[int]
    timeout_seconds: int
    max_retries: int
    backoff: BackoffConfig
    def __init__(
        self,
        timeout_seconds: _Optional[int] = ...,
        max_retries: _Optional[int] = ...,
        backoff: _Optional[_Union[BackoffConfig, _Mapping]] = ...,
    ) -> None: ...

class Literal(_message.Message):
    __slots__ = ("null_value", "bool_value", "int_value", "float_value", "string_value")
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    null_value: bool
    bool_value: bool
    int_value: int
    float_value: float
    string_value: str
    def __init__(
        self,
        null_value: bool = ...,
        bool_value: bool = ...,
        int_value: _Optional[int] = ...,
        float_value: _Optional[float] = ...,
        string_value: _Optional[str] = ...,
    ) -> None: ...

class Expression(_message.Message):
    __slots__ = (
        "literal",
        "variable",
        "subscript",
        "array",
        "dict",
        "binary_op",
        "unary_op",
        "call",
        "attribute",
    )
    LITERAL_FIELD_NUMBER: _ClassVar[int]
    VARIABLE_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPT_FIELD_NUMBER: _ClassVar[int]
    ARRAY_FIELD_NUMBER: _ClassVar[int]
    DICT_FIELD_NUMBER: _ClassVar[int]
    BINARY_OP_FIELD_NUMBER: _ClassVar[int]
    UNARY_OP_FIELD_NUMBER: _ClassVar[int]
    CALL_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    literal: Literal
    variable: str
    subscript: Subscript
    array: ArrayExpr
    dict: DictExpr
    binary_op: BinaryOp
    unary_op: UnaryOp
    call: CallExpr
    attribute: AttributeAccess
    def __init__(
        self,
        literal: _Optional[_Union[Literal, _Mapping]] = ...,
        variable: _Optional[str] = ...,
        subscript: _Optional[_Union[Subscript, _Mapping]] = ...,
        array: _Optional[_Union[ArrayExpr, _Mapping]] = ...,
        dict: _Optional[_Union[DictExpr, _Mapping]] = ...,
        binary_op: _Optional[_Union[BinaryOp, _Mapping]] = ...,
        unary_op: _Optional[_Union[UnaryOp, _Mapping]] = ...,
        call: _Optional[_Union[CallExpr, _Mapping]] = ...,
        attribute: _Optional[_Union[AttributeAccess, _Mapping]] = ...,
    ) -> None: ...

class Subscript(_message.Message):
    __slots__ = ("base", "key")
    BASE_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    base: Expression
    key: Expression
    def __init__(
        self,
        base: _Optional[_Union[Expression, _Mapping]] = ...,
        key: _Optional[_Union[Expression, _Mapping]] = ...,
    ) -> None: ...

class AttributeAccess(_message.Message):
    __slots__ = ("base", "attribute")
    BASE_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    base: Expression
    attribute: str
    def __init__(
        self, base: _Optional[_Union[Expression, _Mapping]] = ..., attribute: _Optional[str] = ...
    ) -> None: ...

class ArrayExpr(_message.Message):
    __slots__ = ("elements",)
    ELEMENTS_FIELD_NUMBER: _ClassVar[int]
    elements: _containers.RepeatedCompositeFieldContainer[Expression]
    def __init__(
        self, elements: _Optional[_Iterable[_Union[Expression, _Mapping]]] = ...
    ) -> None: ...

class DictExpr(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[DictEntry]
    def __init__(
        self, entries: _Optional[_Iterable[_Union[DictEntry, _Mapping]]] = ...
    ) -> None: ...

class DictEntry(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: Expression
    def __init__(
        self, key: _Optional[str] = ..., value: _Optional[_Union[Expression, _Mapping]] = ...
    ) -> None: ...

class BinaryOp(_message.Message):
    __slots__ = ("op", "left", "right")
    class Op(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OP_UNSPECIFIED: _ClassVar[BinaryOp.Op]
        OP_ADD: _ClassVar[BinaryOp.Op]
        OP_SUB: _ClassVar[BinaryOp.Op]
        OP_MUL: _ClassVar[BinaryOp.Op]
        OP_DIV: _ClassVar[BinaryOp.Op]
        OP_MOD: _ClassVar[BinaryOp.Op]
        OP_EQ: _ClassVar[BinaryOp.Op]
        OP_NE: _ClassVar[BinaryOp.Op]
        OP_LT: _ClassVar[BinaryOp.Op]
        OP_LE: _ClassVar[BinaryOp.Op]
        OP_GT: _ClassVar[BinaryOp.Op]
        OP_GE: _ClassVar[BinaryOp.Op]
        OP_AND: _ClassVar[BinaryOp.Op]
        OP_OR: _ClassVar[BinaryOp.Op]
        OP_IN: _ClassVar[BinaryOp.Op]
        OP_NOT_IN: _ClassVar[BinaryOp.Op]

    OP_UNSPECIFIED: BinaryOp.Op
    OP_ADD: BinaryOp.Op
    OP_SUB: BinaryOp.Op
    OP_MUL: BinaryOp.Op
    OP_DIV: BinaryOp.Op
    OP_MOD: BinaryOp.Op
    OP_EQ: BinaryOp.Op
    OP_NE: BinaryOp.Op
    OP_LT: BinaryOp.Op
    OP_LE: BinaryOp.Op
    OP_GT: BinaryOp.Op
    OP_GE: BinaryOp.Op
    OP_AND: BinaryOp.Op
    OP_OR: BinaryOp.Op
    OP_IN: BinaryOp.Op
    OP_NOT_IN: BinaryOp.Op
    OP_FIELD_NUMBER: _ClassVar[int]
    LEFT_FIELD_NUMBER: _ClassVar[int]
    RIGHT_FIELD_NUMBER: _ClassVar[int]
    op: BinaryOp.Op
    left: Expression
    right: Expression
    def __init__(
        self,
        op: _Optional[_Union[BinaryOp.Op, str]] = ...,
        left: _Optional[_Union[Expression, _Mapping]] = ...,
        right: _Optional[_Union[Expression, _Mapping]] = ...,
    ) -> None: ...

class UnaryOp(_message.Message):
    __slots__ = ("op", "operand")
    class Op(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OP_UNSPECIFIED: _ClassVar[UnaryOp.Op]
        OP_NOT: _ClassVar[UnaryOp.Op]
        OP_NEG: _ClassVar[UnaryOp.Op]

    OP_UNSPECIFIED: UnaryOp.Op
    OP_NOT: UnaryOp.Op
    OP_NEG: UnaryOp.Op
    OP_FIELD_NUMBER: _ClassVar[int]
    OPERAND_FIELD_NUMBER: _ClassVar[int]
    op: UnaryOp.Op
    operand: Expression
    def __init__(
        self,
        op: _Optional[_Union[UnaryOp.Op, str]] = ...,
        operand: _Optional[_Union[Expression, _Mapping]] = ...,
    ) -> None: ...

class CallExpr(_message.Message):
    __slots__ = ("function", "args")
    FUNCTION_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    function: str
    args: _containers.RepeatedCompositeFieldContainer[Expression]
    def __init__(
        self,
        function: _Optional[str] = ...,
        args: _Optional[_Iterable[_Union[Expression, _Mapping]]] = ...,
    ) -> None: ...

class KwArg(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: Expression
    def __init__(
        self, name: _Optional[str] = ..., value: _Optional[_Union[Expression, _Mapping]] = ...
    ) -> None: ...

class ActionCall(_message.Message):
    __slots__ = ("action", "module", "args", "target", "config", "location")
    ACTION_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    action: str
    module: str
    args: _containers.RepeatedCompositeFieldContainer[KwArg]
    target: str
    config: RunActionConfig
    location: SourceLocation
    def __init__(
        self,
        action: _Optional[str] = ...,
        module: _Optional[str] = ...,
        args: _Optional[_Iterable[_Union[KwArg, _Mapping]]] = ...,
        target: _Optional[str] = ...,
        config: _Optional[_Union[RunActionConfig, _Mapping]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class SubgraphCall(_message.Message):
    __slots__ = ("method_name", "args", "target", "location")
    METHOD_NAME_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    method_name: str
    args: _containers.RepeatedCompositeFieldContainer[KwArg]
    target: str
    location: SourceLocation
    def __init__(
        self,
        method_name: _Optional[str] = ...,
        args: _Optional[_Iterable[_Union[KwArg, _Mapping]]] = ...,
        target: _Optional[str] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class GatherCall(_message.Message):
    __slots__ = ("action", "subgraph")
    ACTION_FIELD_NUMBER: _ClassVar[int]
    SUBGRAPH_FIELD_NUMBER: _ClassVar[int]
    action: ActionCall
    subgraph: SubgraphCall
    def __init__(
        self,
        action: _Optional[_Union[ActionCall, _Mapping]] = ...,
        subgraph: _Optional[_Union[SubgraphCall, _Mapping]] = ...,
    ) -> None: ...

class Gather(_message.Message):
    __slots__ = ("calls", "target", "location")
    CALLS_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    calls: _containers.RepeatedCompositeFieldContainer[GatherCall]
    target: str
    location: SourceLocation
    def __init__(
        self,
        calls: _Optional[_Iterable[_Union[GatherCall, _Mapping]]] = ...,
        target: _Optional[str] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class PythonBlock(_message.Message):
    __slots__ = ("code", "imports", "definitions", "inputs", "outputs", "location")
    CODE_FIELD_NUMBER: _ClassVar[int]
    IMPORTS_FIELD_NUMBER: _ClassVar[int]
    DEFINITIONS_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    code: str
    imports: _containers.RepeatedScalarFieldContainer[str]
    definitions: _containers.RepeatedScalarFieldContainer[str]
    inputs: _containers.RepeatedScalarFieldContainer[str]
    outputs: _containers.RepeatedScalarFieldContainer[str]
    location: SourceLocation
    def __init__(
        self,
        code: _Optional[str] = ...,
        imports: _Optional[_Iterable[str]] = ...,
        definitions: _Optional[_Iterable[str]] = ...,
        inputs: _Optional[_Iterable[str]] = ...,
        outputs: _Optional[_Iterable[str]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Loop(_message.Message):
    __slots__ = ("iterator", "loop_var", "accumulator", "body", "location")
    ITERATOR_FIELD_NUMBER: _ClassVar[int]
    LOOP_VAR_FIELD_NUMBER: _ClassVar[int]
    ACCUMULATOR_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    iterator: Expression
    loop_var: str
    accumulator: str
    body: _containers.RepeatedCompositeFieldContainer[Statement]
    location: SourceLocation
    def __init__(
        self,
        iterator: _Optional[_Union[Expression, _Mapping]] = ...,
        loop_var: _Optional[str] = ...,
        accumulator: _Optional[str] = ...,
        body: _Optional[_Iterable[_Union[Statement, _Mapping]]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Branch(_message.Message):
    __slots__ = ("guard", "preamble", "actions", "postamble", "location")
    GUARD_FIELD_NUMBER: _ClassVar[int]
    PREAMBLE_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    POSTAMBLE_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    guard: Expression
    preamble: _containers.RepeatedCompositeFieldContainer[PythonBlock]
    actions: _containers.RepeatedCompositeFieldContainer[ActionCall]
    postamble: _containers.RepeatedCompositeFieldContainer[PythonBlock]
    location: SourceLocation
    def __init__(
        self,
        guard: _Optional[_Union[Expression, _Mapping]] = ...,
        preamble: _Optional[_Iterable[_Union[PythonBlock, _Mapping]]] = ...,
        actions: _Optional[_Iterable[_Union[ActionCall, _Mapping]]] = ...,
        postamble: _Optional[_Iterable[_Union[PythonBlock, _Mapping]]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Conditional(_message.Message):
    __slots__ = ("branches", "target", "location")
    BRANCHES_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    branches: _containers.RepeatedCompositeFieldContainer[Branch]
    target: str
    location: SourceLocation
    def __init__(
        self,
        branches: _Optional[_Iterable[_Union[Branch, _Mapping]]] = ...,
        target: _Optional[str] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class ExceptHandler(_message.Message):
    __slots__ = ("exception_types", "preamble", "body", "postamble", "location")
    EXCEPTION_TYPES_FIELD_NUMBER: _ClassVar[int]
    PREAMBLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    POSTAMBLE_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    exception_types: _containers.RepeatedCompositeFieldContainer[ExceptionType]
    preamble: _containers.RepeatedCompositeFieldContainer[PythonBlock]
    body: _containers.RepeatedCompositeFieldContainer[ActionCall]
    postamble: _containers.RepeatedCompositeFieldContainer[PythonBlock]
    location: SourceLocation
    def __init__(
        self,
        exception_types: _Optional[_Iterable[_Union[ExceptionType, _Mapping]]] = ...,
        preamble: _Optional[_Iterable[_Union[PythonBlock, _Mapping]]] = ...,
        body: _Optional[_Iterable[_Union[ActionCall, _Mapping]]] = ...,
        postamble: _Optional[_Iterable[_Union[PythonBlock, _Mapping]]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class ExceptionType(_message.Message):
    __slots__ = ("module", "name")
    MODULE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    module: str
    name: str
    def __init__(self, module: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class TryExcept(_message.Message):
    __slots__ = ("try_preamble", "try_body", "try_postamble", "handlers", "location")
    TRY_PREAMBLE_FIELD_NUMBER: _ClassVar[int]
    TRY_BODY_FIELD_NUMBER: _ClassVar[int]
    TRY_POSTAMBLE_FIELD_NUMBER: _ClassVar[int]
    HANDLERS_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    try_preamble: _containers.RepeatedCompositeFieldContainer[PythonBlock]
    try_body: _containers.RepeatedCompositeFieldContainer[ActionCall]
    try_postamble: _containers.RepeatedCompositeFieldContainer[PythonBlock]
    handlers: _containers.RepeatedCompositeFieldContainer[ExceptHandler]
    location: SourceLocation
    def __init__(
        self,
        try_preamble: _Optional[_Iterable[_Union[PythonBlock, _Mapping]]] = ...,
        try_body: _Optional[_Iterable[_Union[ActionCall, _Mapping]]] = ...,
        try_postamble: _Optional[_Iterable[_Union[PythonBlock, _Mapping]]] = ...,
        handlers: _Optional[_Iterable[_Union[ExceptHandler, _Mapping]]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Sleep(_message.Message):
    __slots__ = ("duration", "location")
    DURATION_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    duration: Expression
    location: SourceLocation
    def __init__(
        self,
        duration: _Optional[_Union[Expression, _Mapping]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Return(_message.Message):
    __slots__ = ("expression", "action", "gather", "location")
    EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    GATHER_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    expression: Expression
    action: ActionCall
    gather: Gather
    location: SourceLocation
    def __init__(
        self,
        expression: _Optional[_Union[Expression, _Mapping]] = ...,
        action: _Optional[_Union[ActionCall, _Mapping]] = ...,
        gather: _Optional[_Union[Gather, _Mapping]] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Spread(_message.Message):
    __slots__ = ("action", "loop_var", "iterable", "target", "location")
    ACTION_FIELD_NUMBER: _ClassVar[int]
    LOOP_VAR_FIELD_NUMBER: _ClassVar[int]
    ITERABLE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    action: ActionCall
    loop_var: str
    iterable: Expression
    target: str
    location: SourceLocation
    def __init__(
        self,
        action: _Optional[_Union[ActionCall, _Mapping]] = ...,
        loop_var: _Optional[str] = ...,
        iterable: _Optional[_Union[Expression, _Mapping]] = ...,
        target: _Optional[str] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...

class Statement(_message.Message):
    __slots__ = (
        "action_call",
        "gather",
        "python_block",
        "loop",
        "conditional",
        "try_except",
        "sleep",
        "return_stmt",
        "spread",
    )
    ACTION_CALL_FIELD_NUMBER: _ClassVar[int]
    GATHER_FIELD_NUMBER: _ClassVar[int]
    PYTHON_BLOCK_FIELD_NUMBER: _ClassVar[int]
    LOOP_FIELD_NUMBER: _ClassVar[int]
    CONDITIONAL_FIELD_NUMBER: _ClassVar[int]
    TRY_EXCEPT_FIELD_NUMBER: _ClassVar[int]
    SLEEP_FIELD_NUMBER: _ClassVar[int]
    RETURN_STMT_FIELD_NUMBER: _ClassVar[int]
    SPREAD_FIELD_NUMBER: _ClassVar[int]
    action_call: ActionCall
    gather: Gather
    python_block: PythonBlock
    loop: Loop
    conditional: Conditional
    try_except: TryExcept
    sleep: Sleep
    return_stmt: Return
    spread: Spread
    def __init__(
        self,
        action_call: _Optional[_Union[ActionCall, _Mapping]] = ...,
        gather: _Optional[_Union[Gather, _Mapping]] = ...,
        python_block: _Optional[_Union[PythonBlock, _Mapping]] = ...,
        loop: _Optional[_Union[Loop, _Mapping]] = ...,
        conditional: _Optional[_Union[Conditional, _Mapping]] = ...,
        try_except: _Optional[_Union[TryExcept, _Mapping]] = ...,
        sleep: _Optional[_Union[Sleep, _Mapping]] = ...,
        return_stmt: _Optional[_Union[Return, _Mapping]] = ...,
        spread: _Optional[_Union[Spread, _Mapping]] = ...,
    ) -> None: ...

class WorkflowParam(_message.Message):
    __slots__ = ("name", "type_annotation")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_ANNOTATION_FIELD_NUMBER: _ClassVar[int]
    name: str
    type_annotation: str
    def __init__(
        self, name: _Optional[str] = ..., type_annotation: _Optional[str] = ...
    ) -> None: ...

class Workflow(_message.Message):
    __slots__ = ("name", "params", "body", "return_type")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    RETURN_TYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    params: _containers.RepeatedCompositeFieldContainer[WorkflowParam]
    body: _containers.RepeatedCompositeFieldContainer[Statement]
    return_type: str
    def __init__(
        self,
        name: _Optional[str] = ...,
        params: _Optional[_Iterable[_Union[WorkflowParam, _Mapping]]] = ...,
        body: _Optional[_Iterable[_Union[Statement, _Mapping]]] = ...,
        return_type: _Optional[str] = ...,
    ) -> None: ...

class ActionDefinition(_message.Message):
    __slots__ = ("name", "module", "param_names")
    NAME_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    PARAM_NAMES_FIELD_NUMBER: _ClassVar[int]
    name: str
    module: str
    param_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self,
        name: _Optional[str] = ...,
        module: _Optional[str] = ...,
        param_names: _Optional[_Iterable[str]] = ...,
    ) -> None: ...

class ParseError(_message.Message):
    __slots__ = ("message", "location")
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    message: str
    location: SourceLocation
    def __init__(
        self,
        message: _Optional[str] = ...,
        location: _Optional[_Union[SourceLocation, _Mapping]] = ...,
    ) -> None: ...
