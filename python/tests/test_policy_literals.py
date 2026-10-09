"""Policy fields the compiler cannot read are rejected, never silently defaulted.

Each fixture under ``fixtures_unsupported`` spells one form that used to be
ignored or truncated; the builder must raise ``UnsupportedPatternError`` with
a message naming the field.
"""

import importlib
from typing import cast

import pytest

from waymark.ir_builder import UnsupportedPatternError

REJECTED = [
    ("policy_retry_variable", "PolicyRetryVariableWorkflow", "RetryPolicy(...) literal"),
    (
        "policy_retry_self_unassigned",
        "PolicyRetrySelfUnassignedWorkflow",
        "not assigned a literal in __init__",
    ),
    ("policy_retry_positional", "PolicyRetryPositionalWorkflow", "keyword arguments only"),
    ("policy_retry_unknown_field", "PolicyRetryUnknownFieldWorkflow", "no field 'max_retries'"),
    (
        "policy_retry_attempts_variable",
        "PolicyRetryAttemptsVariableWorkflow",
        "attempts must be an integer literal",
    ),
    (
        "policy_retry_attempts_float",
        "PolicyRetryAttemptsFloatWorkflow",
        "attempts must be an integer literal",
    ),
    (
        "policy_retry_attempts_too_many",
        "PolicyRetryAttemptsTooManyWorkflow",
        "attempts must be at most 4294967296",
    ),
    (
        "policy_retry_attempts_zero",
        "PolicyRetryAttemptsZeroWorkflow",
        "attempts must be at least 1",
    ),
    (
        "policy_retry_backoff_fraction",
        "PolicyRetryBackoffFractionWorkflow",
        "backoff_seconds must be a whole number of seconds",
    ),
    (
        "policy_retry_exception_types_tuple",
        "PolicyRetryExceptionTypesTupleWorkflow",
        "exception_types must be a list literal",
    ),
    (
        "policy_timeout_variable",
        "PolicyTimeoutVariableWorkflow",
        "timeout= must be a number literal",
    ),
    ("policy_timeout_fraction", "PolicyTimeoutFractionWorkflow", "whole number of seconds"),
    ("policy_timeout_zero", "PolicyTimeoutZeroWorkflow", "at least one second"),
    (
        "policy_timeout_timedelta_milliseconds",
        "PolicyTimeoutTimedeltaMillisecondsWorkflow",
        "does not take a 'milliseconds' keyword",
    ),
    (
        "policy_timeout_timedelta_variable",
        "PolicyTimeoutTimedeltaVariableWorkflow",
        "number literal for seconds",
    ),
    (
        "policy_run_action_keyword",
        "PolicyRunActionKeywordWorkflow",
        "does not take a 'retries' keyword",
    ),
    (
        "policy_retry_positional_arg",
        "PolicyRetryPositionalArgWorkflow",
        "takes one positional argument",
    ),
    (
        "policy_retry_self_assigned_twice",
        "PolicyRetrySelfAssignedTwiceWorkflow",
        "assigned more than once or under a branch",
    ),
    (
        "policy_retry_self_in_branch",
        "PolicyRetrySelfInBranchWorkflow",
        "assigned more than once or under a branch",
    ),
]


@pytest.mark.parametrize(("module_name", "workflow_name", "fragment"), REJECTED)
def test_unreadable_policy_fields_are_rejected(
    module_name: str, workflow_name: str, fragment: str
) -> None:
    module = importlib.import_module(f"tests.fixtures_unsupported.{module_name}")
    workflow_cls = getattr(module, workflow_name)

    with pytest.raises(UnsupportedPatternError) as exc_info:
        workflow_cls.workflow_ir()

    error = cast(UnsupportedPatternError, exc_info.value)
    assert fragment in error.message, error.message
