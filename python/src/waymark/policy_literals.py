"""Literal policy parsing shared by registered compiler handlers."""

import ast
import inspect
import math
from datetime import timedelta
from typing import Optional

from waymark.call_binding import bind_call
from waymark.compiler_ast import resolve_reference
from waymark.compiler_context import CompilerContext
from waymark.compiler_errors import UnsupportedPatternError
from waymark.compiler_recommendations import RECOMMENDATIONS
from waymark.proto import ast_pb2 as ir
from waymark.workflow import RetryPolicy

DEFAULT_RETRY_POLICY_MAX_RETRIES = 100
DEFAULT_RETRY_POLICY_EXCEPTION_TYPE = "Exception"
# `attempts` minus one is the IR's `max_retries`, a uint32.
MAX_RETRY_POLICY_ATTEMPTS = 2**32


def _is_none_literal(node: ast.AST) -> bool:
    """Whether `node` is the literal `None`."""
    return isinstance(node, ast.Constant) and node.value is None


def _number_literal(node: ast.AST) -> Optional[int | float]:
    """The number `node` is a literal of, if it is one; a bool is not, a negated one is."""
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        operand = _number_literal(node.operand)
        return None if operand is None else -operand
    if isinstance(node, ast.Constant) and not isinstance(node.value, bool):
        if isinstance(node.value, (int, float)):
            return node.value
    return None


# A sleep's wake time must fit the runtime's clock, which the sleep handlers
# add the duration to. A literal past that range would compile and then
# fail every run; the cap is a round figure far inside it.
_MAX_DURATION_YEARS = 100
_MAX_DURATION_SECONDS = _MAX_DURATION_YEARS * 365 * 24 * 60 * 60


class PolicyCompiler:
    def __init__(self, context: CompilerContext):
        self.context = context

    def apply(
        self, retry: ast.expr | None, timeout: ast.expr | None, action_call: ir.ActionCall
    ) -> None:
        """Attach already-bound policies without reinterpreting the wrapper signature."""
        for name, value in (("retry", retry), ("timeout", timeout)):
            if value is None:
                continue
            literal = self._policy_literal(value, name)
            if _is_none_literal(literal):
                # An explicit None, inline or through a self attribute, is the
                # parameter's default: no policy.
                continue
            policy_bracket = ir.PolicyBracket()
            if name == "retry":
                policy_bracket.retry.CopyFrom(self._parse_retry_policy(literal))
            else:
                policy_bracket.timeout.CopyFrom(self._parse_timeout_policy(literal))
            action_call.policies.append(policy_bracket)

    def _policy_error(
        self, message: str, recommendation: str, node: ast.expr
    ) -> UnsupportedPatternError:
        """An error for a policy the compiler cannot read, located at `node`."""
        return UnsupportedPatternError(
            message,
            RECOMMENDATIONS[recommendation],
            line=node.lineno,
            col=node.col_offset,
        )

    def _policy_literal(self, node: ast.expr, keyword: str) -> ast.expr:
        """The literal a policy keyword carries.

        A `self.<attribute>` resolves to the literal assigned to it once, at
        the top level of `__init__`; one without such an assignment, or one
        assigned more than once or under a branch, is rejected. Anything
        else is returned as is for the caller to read.
        """
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        ):
            if node.attr in self.context.ambiguous_instance_attrs:
                raise self._policy_error(
                    f"{keyword}=self.{node.attr} is assigned more than once or under a "
                    "branch in __init__",
                    "policy_literal",
                    node,
                )
            literal = self.context.instance_attrs.get(node.attr)
            if literal is None:
                raise self._policy_error(
                    f"{keyword}=self.{node.attr} is not assigned a literal in __init__",
                    "policy_literal",
                    node,
                )
            return literal
        return node

    def _seconds_literal(self, node: ast.expr, what: str, positive: bool) -> float:
        """A number literal read as seconds, fractions included.

        A negative is rejected, and so is a zero where `positive` says the
        value must be more than that. So is a value the runtime cannot sleep:
        one that is not finite, or a positive one that rounds to zero at
        nanosecond precision. So is a value above the 100-year cap, a round
        figure far inside what the runtime can sleep.
        """
        seconds = _number_literal(node)
        if seconds is None:
            raise self._policy_error(f"{what} must be a number literal", "policy_literal", node)
        if seconds < 0 or (positive and seconds == 0):
            bound = "more than zero seconds" if positive else "zero or more seconds"
            raise self._policy_error(f"{what} must be {bound}", "duration_range", node)
        return self._runnable_seconds(seconds, what, node)

    def _runnable_seconds(self, seconds: int | float, what: str, node: ast.expr) -> float:
        """`seconds` as the runtime can sleep it, or the error that says why not."""
        try:
            seconds = float(seconds)
        except OverflowError:
            # An integer literal too large for a float is too large for a sleep.
            seconds = math.inf
        if not math.isfinite(seconds) or seconds > _MAX_DURATION_SECONDS:
            raise self._policy_error(
                f"{what} must be at most {_MAX_DURATION_YEARS} years", "duration_range", node
            )
        if seconds > 0 and round(seconds * 1e9) == 0:
            raise self._policy_error(
                f"{what} must be at least one nanosecond", "duration_range", node
            )
        return seconds

    def _parse_retry_policy(self, node: ast.expr) -> ir.RetryPolicy:
        """Parse a RetryPolicy(...) literal into IR.

        Supports:
        - RetryPolicy(attempts=3)
        - RetryPolicy(attempts=3, exception_types=["ValueError"])
        - RetryPolicy(attempts=3, backoff_seconds=5)

        `node` is the literal itself: a `self.` attribute is resolved by the
        caller. Anything else is rejected: a field the compiler cannot read
        would otherwise silently take its default.
        """
        if not (
            isinstance(node, ast.Call)
            and resolve_reference(node.func, self.context.namespace) is RetryPolicy
        ):
            raise self._policy_error(
                "retry= must be a RetryPolicy(...) literal", "policy_literal", node
            )
        if node.args:
            raise self._policy_error(
                "RetryPolicy takes keyword arguments only", "policy_literal", node
            )

        bind_call(
            node,
            inspect.Signature(
                [
                    inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=None)
                    for name in ("attempts", "exception_types", "backoff_seconds")
                ]
            ),
        )

        # RetryPolicy() defaults to a high retry cap when attempts is omitted.
        policy = ir.RetryPolicy(max_retries=DEFAULT_RETRY_POLICY_MAX_RETRIES)
        exception_types_listed = False

        for kw in node.keywords:
            if kw.arg is None:
                raise self._policy_error(
                    "RetryPolicy does not take **kwargs", "policy_literal", kw.value
                )
            if kw.arg not in {"attempts", "exception_types", "backoff_seconds"}:
                raise self._policy_error(
                    f"RetryPolicy has no field {kw.arg!r}", "policy_literal", kw.value
                )
            if _is_none_literal(kw.value):
                # An explicit None is the field's default.
                continue
            if kw.arg == "attempts":
                attempts = _number_literal(kw.value)
                if not isinstance(attempts, int):
                    raise self._policy_error(
                        "RetryPolicy attempts must be an integer literal",
                        "policy_literal",
                        kw.value,
                    )
                if attempts < 1:
                    raise self._policy_error(
                        "RetryPolicy attempts must be at least 1", "policy_literal", kw.value
                    )
                if attempts > MAX_RETRY_POLICY_ATTEMPTS:
                    raise self._policy_error(
                        f"RetryPolicy attempts must be at most {MAX_RETRY_POLICY_ATTEMPTS}",
                        "policy_literal",
                        kw.value,
                    )
                # attempts means total executions, max_retries means retries after first attempt
                # So attempts=1 -> max_retries=0 (no retries), attempts=3 -> max_retries=2
                policy.max_retries = attempts - 1
            elif kw.arg == "exception_types":
                if not isinstance(kw.value, ast.List):
                    raise self._policy_error(
                        "RetryPolicy exception_types must be a list literal of class names",
                        "policy_literal",
                        kw.value,
                    )
                exception_types_listed = True
                for elt in kw.value.elts:
                    policy.exception_types.append(self._retry_exception_type_name(elt))
            elif kw.arg == "backoff_seconds":
                seconds = self._seconds_literal(
                    kw.value, "RetryPolicy backoff_seconds", positive=False
                )
                # A zero backoff is no backoff: the runtime has no zero sleep.
                if seconds > 0:
                    policy.backoff.seconds = seconds
            else:
                raise self._policy_error(
                    f"RetryPolicy has no field {kw.arg!r}", "policy_literal", kw.value
                )

        # An omitted filter retries on `Exception`, the policy's documented
        # default. A listed one is kept as written: empty retries nothing.
        if not exception_types_listed:
            policy.exception_types.append(DEFAULT_RETRY_POLICY_EXCEPTION_TYPE)

        return policy

    def _retry_exception_type_name(self, node: ast.expr) -> str:
        """One `exception_types` entry: a bare class name as a string literal.

        The VM matches exceptions by bare class name, so a dotted string,
        or anything else that is not an identifier, would never match; it
        is rejected rather than ignored, as is any entry that is not a
        string literal, such as the class itself.
        """
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value.isidentifier()
        ):
            return node.value
        raise UnsupportedPatternError(
            "RetryPolicy exception_types entries must be bare class names as strings",
            RECOMMENDATIONS["retry_exception_types"],
            line=node.lineno,
            col=node.col_offset,
        )

    def _parse_timeout_policy(self, node: ast.expr) -> ir.TimeoutPolicy:
        """Parse a timeout literal into IR.

        `node` is the literal itself: a `self.` attribute is resolved by the
        caller.

        Supports:
        - timeout=60 (seconds, fractions included)
        - timeout=timedelta(seconds=30)
        - timeout=timedelta(minutes=2)

        Anything else is rejected, as is a duration of zero or less.
        """
        policy = ir.TimeoutPolicy()

        if _number_literal(node) is not None:
            policy.timeout.seconds = self._seconds_literal(node, "timeout", positive=True)
            return policy

        if (
            isinstance(node, ast.Call)
            and resolve_reference(node.func, self.context.namespace) is timedelta
        ):
            if node.args:
                raise self._policy_error(
                    "timedelta(...) in timeout= takes keyword arguments only",
                    "policy_literal",
                    node,
                )
            bind_call(
                node,
                inspect.Signature(
                    [
                        inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=0)
                        for name in ("seconds", "minutes", "hours", "days")
                    ]
                ),
            )
            keywords: dict[str, int | float] = {}
            for kw in node.keywords:
                if kw.arg not in ("seconds", "minutes", "hours", "days"):
                    what = "**kwargs" if kw.arg is None else f"a {kw.arg!r} keyword"
                    raise self._policy_error(
                        f"timedelta(...) in timeout= does not take {what}: "
                        "use seconds, minutes, hours or days",
                        "policy_literal",
                        kw.value,
                    )
                value = _number_literal(kw.value)
                if value is None:
                    raise self._policy_error(
                        f"timedelta(...) in timeout= takes a number literal for {kw.arg}",
                        "policy_literal",
                        kw.value,
                    )
                keywords[kw.arg] = value
            # Python's own timedelta arithmetic over the accepted keywords:
            # minutes=4.1 is 246 seconds, as timedelta says.
            try:
                seconds = timedelta(**keywords).total_seconds()
            except OverflowError:
                raise self._policy_error(
                    "timeout is outside the range timedelta can hold", "duration_range", node
                ) from None
            if seconds <= 0:
                raise self._policy_error(
                    "timeout must be more than zero seconds", "duration_range", node
                )
            policy.timeout.seconds = self._runnable_seconds(seconds, "timeout", node)
            return policy

        raise self._policy_error(
            "timeout= must be a number literal or a timedelta(...) literal",
            "policy_literal",
            node,
        )
