"""
Rappel IR (Intermediate Representation) - Immutable AST nodes.

All nodes are frozen dataclasses for immutability.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class SourceLocation:
    """Source code location for error reporting."""

    line: int
    column: int
    end_line: int | None = None
    end_column: int | None = None


# =============================================================================
# Value Types
# =============================================================================


@dataclass(frozen=True)
class RappelValue:
    """Base class for all Rappel values."""

    pass


@dataclass(frozen=True)
class RappelString(RappelValue):
    """String literal value."""

    value: str


@dataclass(frozen=True)
class RappelNumber(RappelValue):
    """Numeric literal value (int or float)."""

    value: int | float


@dataclass(frozen=True)
class RappelBoolean(RappelValue):
    """Boolean literal value."""

    value: bool


@dataclass(frozen=True)
class RappelList(RappelValue):
    """List value - immutable."""

    items: tuple[RappelExpr, ...]


@dataclass(frozen=True)
class RappelDict(RappelValue):
    """Dict value - immutable."""

    pairs: tuple[tuple[str, RappelExpr], ...]


# =============================================================================
# Expression Nodes
# =============================================================================


@dataclass(frozen=True)
class RappelLiteral:
    """A literal value (string, number, boolean)."""

    value: RappelValue
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelVariable:
    """A variable reference."""

    name: str
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelListExpr:
    """A list expression [a, b, c]."""

    items: tuple[RappelExpr, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelDictExpr:
    """A dict expression {"key": value}."""

    pairs: tuple[tuple[RappelExpr, RappelExpr], ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelBinaryOp:
    """A binary operation (a + b, a - b, etc.)."""

    op: str
    left: RappelExpr
    right: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelUnaryOp:
    """A unary operation (not x, -x)."""

    op: str
    operand: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelIndexAccess:
    """Index access: list[0] or dict["key"]."""

    target: RappelExpr
    index: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelDotAccess:
    """Dot access: obj.field."""

    target: RappelExpr
    field: str
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelSpread:
    """Spread operator: ...variable."""

    target: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelCall:
    """Function call: func(kwarg=value). All args must be kwargs."""

    target: str
    kwargs: tuple[tuple[str, RappelExpr], ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelRetryPolicy:
    """
    Retry policy for action calls.

    @action(...) [ValueError -> retry: 3, backoff: 2m]
    @action(...) [(ValueError, KeyError) -> retry: 3, backoff: 2s]
    @action(...) [retry: 3, backoff: 2m]  # catch all

    - exception_types: tuple of exception type names to catch (empty = catch all)
    - max_retries: maximum number of retry attempts
    - backoff_seconds: base backoff duration in seconds (exponential: backoff * 2^attempt)
    """

    exception_types: tuple[str, ...]  # Empty tuple means catch all exceptions
    max_retries: int = 3
    backoff_seconds: float = 60.0  # 1 minute default


@dataclass(frozen=True)
class RappelActionCall:
    """
    Action call: @action_name(kwarg=value). External action, kwargs only.

    Supports retry policies and timeout:
        @action(...) [ValueError -> retry: 3, backoff: 2m] [timeout: 2m]
    """

    action_name: str
    kwargs: tuple[tuple[str, RappelExpr], ...]
    retry_policies: tuple[RappelRetryPolicy, ...] = ()
    timeout_seconds: float | None = None  # Separate from retry policies
    location: SourceLocation | None = None


# Type alias for expressions
RappelExpr = (
    RappelLiteral
    | RappelVariable
    | RappelListExpr
    | RappelDictExpr
    | RappelBinaryOp
    | RappelUnaryOp
    | RappelIndexAccess
    | RappelDotAccess
    | RappelSpread
    | RappelCall
    | RappelActionCall
)


# =============================================================================
# Statement Nodes
# =============================================================================


@dataclass(frozen=True)
class RappelAssignment:
    """Variable assignment: x = expr"""

    target: str
    value: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelMultiAssignment:
    """Multiple assignment (unpacking): a, b, c = expr"""

    targets: tuple[str, ...]
    value: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelReturn:
    """Return statement: return expr or return [a, b, c]"""

    values: tuple[RappelExpr, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelExprStatement:
    """Expression as statement (e.g., action call)."""

    expr: RappelExpr
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelFunctionDef:
    """
    Function definition with explicit input/output.

    fn process(input: [x, y], output: [result]):
        result = x + y
        return result
    """

    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    body: tuple[RappelStatement, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelForLoop:
    """
    For loop - iterates over a collection with a single function call in body.

    for item in items:
        result = process_item(x=item)

    Supports unpacking multiple variables:
    for i, item in enumerate(items):
        result = process_item(idx=i, x=item)

    The body must contain exactly one statement: an assignment with a function call.
    """

    loop_vars: tuple[str, ...]
    iterable: RappelExpr
    body: tuple[RappelStatement, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelIfStatement:
    """
    If statement with explicit blocks.

    if condition:
        body
    else:
        else_body
    """

    condition: RappelExpr
    then_body: tuple[RappelStatement, ...]
    else_body: tuple[RappelStatement, ...] | None = None
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelSpreadAction:
    """
    Spread an action over a list, with results aggregated.

    spread items:item -> @fetch_details(id=item)
    results = spread items:item -> @fetch_details(id=item)

    - source_list: the list to spread over
    - item_var: the variable name for each item in the loop
    - action: the action call to execute for each item
    - target: optional variable to store aggregated results
    """

    source_list: RappelExpr
    item_var: str
    action: RappelActionCall
    target: str | None = None  # If assigned: results = spread ...
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelExceptHandler:
    """
    A single except handler in a try/except block.

    except ErrorType:
        result = @fallback_action(...)

    - exception_types: tuple of exception type names to catch (empty = catch all)
    - body: statements to execute (should contain action calls)
    """

    exception_types: tuple[str, ...]  # Empty tuple means catch all
    body: tuple[RappelStatement, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelTryExcept:
    """
    Try/except block for action error handling.

    try:
        result = @risky_action(...)
    except NetworkError:
        result = @fallback_action(...)
    except:
        result = @default_handler(...)

    The try block and each except block should be treated as isolated
    execution units - each containing action calls that are the unit
    of durable execution.
    """

    try_body: tuple[RappelStatement, ...]
    handlers: tuple[RappelExceptHandler, ...]
    location: SourceLocation | None = None


@dataclass(frozen=True)
class RappelParallelBlock:
    """
    Parallel execution block for concurrent action/function calls.

    results = parallel:
        @action_a()
        @action_b()
        func_c()

    Or without assignment:
    parallel:
        @action_a()
        @action_b()

    - calls: tuple of expressions (action calls or function calls)
    - target: optional variable to store results as a list
    """

    calls: tuple[RappelExpr, ...]
    target: str | None = None
    location: SourceLocation | None = None


# Type alias for statements
RappelStatement = (
    RappelAssignment
    | RappelMultiAssignment
    | RappelReturn
    | RappelExprStatement
    | RappelFunctionDef
    | RappelForLoop
    | RappelIfStatement
    | RappelSpreadAction
    | RappelTryExcept
    | RappelParallelBlock
)


@dataclass(frozen=True)
class RappelProgram:
    """A complete Rappel program."""

    statements: tuple[RappelStatement, ...]
    location: SourceLocation | None = None
