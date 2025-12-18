"""
Workflow base class and registration decorator.

This module provides the foundation for defining workflows that can be
compiled to IR and executed by the Rappel runtime.
"""

import hashlib
import inspect
import os
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
from threading import RLock
from typing import Any, Awaitable, ClassVar, Optional, TypeVar

from proto import ast_pb2 as ir
from proto import messages_pb2 as pb2

from . import bridge
from .actions import deserialize_result_payload
from .ir_builder import build_workflow_ir
from .logger import configure as configure_logger
from .serialization import build_arguments_from_kwargs
from .workflow_runtime import WorkflowNodeResult

logger = configure_logger("rappel.workflow")

TWorkflow = TypeVar("TWorkflow", bound="Workflow")
TResult = TypeVar("TResult")


@dataclass(frozen=True)
class RetryPolicy:
    """Retry policy for action execution.

    Maps to IR RetryPolicy: [ExceptionType -> retry: N, backoff: Xs]

    Args:
        attempts: Maximum number of retry attempts.
        exception_types: List of exception type names to retry on. Empty = catch all.
        backoff_seconds: Constant backoff duration between retries in seconds.
    """

    attempts: Optional[int] = None
    exception_types: Optional[list[str]] = None
    backoff_seconds: Optional[float] = None


class Workflow:
    """Base class for workflow definitions."""

    name: ClassVar[Optional[str]] = None
    """Human-friendly identifier. Override to pin the registry key; defaults to lowercase class name."""

    concurrent: ClassVar[bool] = False
    """When True, downstream engines may respect DAG-parallel execution; False preserves sequential semantics."""

    _workflow_ir: ClassVar[Optional[ir.Program]] = None
    _ir_lock: ClassVar[RLock] = RLock()
    _workflow_version_id: ClassVar[Optional[str]] = None

    async def run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def run_action(
        self,
        awaitable: Awaitable[TResult],
        *,
        retry: Optional[RetryPolicy] = None,
        timeout: Optional[float | int | timedelta] = None,
    ) -> TResult:
        """Helper that simply awaits the provided action coroutine.

        The retry and timeout arguments are consumed by the workflow compiler
        (IR builder) rather than the runtime execution path.

        Args:
            awaitable: The action coroutine to execute.
            retry: Retry policy including max attempts, exception types, and backoff.
            timeout: Timeout duration in seconds (or timedelta).
        """
        # Parameters are intentionally unused at runtime; the workflow compiler
        # inspects the AST to record them.
        del retry, timeout
        return await awaitable

    @classmethod
    def short_name(cls) -> str:
        if cls.name:
            return cls.name
        return cls.__name__.lower()

    @classmethod
    def workflow_ir(cls) -> ir.Program:
        """Build and cache the IR program for this workflow."""
        if cls._workflow_ir is None:
            with cls._ir_lock:
                if cls._workflow_ir is None:
                    cls._workflow_ir = build_workflow_ir(cls)
        return cls._workflow_ir

    @classmethod
    def _build_registration_payload(
        cls, initial_context: Optional[pb2.WorkflowArguments] = None
    ) -> pb2.WorkflowRegistration:
        """Build a registration payload with the serialized IR."""
        program = cls.workflow_ir()

        # Serialize IR to bytes
        ir_bytes = program.SerializeToString()
        ir_hash = hashlib.sha256(ir_bytes).hexdigest()

        message = pb2.WorkflowRegistration(
            workflow_name=cls.short_name(),
            ir=ir_bytes,
            ir_hash=ir_hash,
            concurrent=cls.concurrent,
        )

        if initial_context:
            message.initial_context.CopyFrom(initial_context)

        return message


class WorkflowRegistry:
    """Registry of workflow definitions keyed by workflow name."""

    def __init__(self) -> None:
        self._workflows: dict[str, type[Workflow]] = {}
        self._lock = RLock()

    def register(self, name: str, workflow_cls: type[Workflow]) -> None:
        with self._lock:
            if name in self._workflows:
                raise ValueError(f"workflow '{name}' already registered")
            self._workflows[name] = workflow_cls

    def get(self, name: str) -> Optional[type[Workflow]]:
        with self._lock:
            return self._workflows.get(name)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._workflows.keys())

    def reset(self) -> None:
        with self._lock:
            self._workflows.clear()


workflow_registry = WorkflowRegistry()


def workflow(cls: type[TWorkflow]) -> type[TWorkflow]:
    """Decorator that registers workflow classes and caches their IR."""

    if not issubclass(cls, Workflow):
        raise TypeError("workflow decorator requires Workflow subclasses")
    run_impl = cls.run
    if not inspect.iscoroutinefunction(run_impl):
        raise TypeError("workflow run() must be defined with 'async def'")

    @wraps(run_impl)
    async def run_public(self: Workflow, *args: Any, **kwargs: Any) -> Any:
        if _running_under_pytest():
            cls.workflow_ir()
            return await run_impl(self, *args, **kwargs)

        # Get the signature of run() to map positional args to parameter names
        sig = inspect.signature(run_impl)
        params = list(sig.parameters.keys())[1:]  # Skip 'self'

        # Convert positional args to kwargs
        for i, arg in enumerate(args):
            if i < len(params):
                kwargs[params[i]] = arg

        # Serialize kwargs using common logic
        initial_context = build_arguments_from_kwargs(kwargs)

        payload = cls._build_registration_payload(initial_context)
        run_result = await bridge.run_instance(payload.SerializeToString())
        cls._workflow_version_id = run_result.workflow_version_id
        if _skip_wait_for_instance():
            logger.info(
                "Skipping wait_for_instance for workflow %s due to RAPPEL_SKIP_WAIT_FOR_INSTANCE",
                cls.short_name(),
            )
            return None
        result_bytes = await bridge.wait_for_instance(
            instance_id=run_result.workflow_instance_id,
            poll_interval_secs=1.0,
        )
        if result_bytes is None:
            raise TimeoutError(
                f"workflow instance {run_result.workflow_instance_id} did not complete"
            )
        arguments = pb2.WorkflowArguments()
        arguments.ParseFromString(result_bytes)
        result = deserialize_result_payload(arguments)
        if result.error:
            raise RuntimeError(f"workflow failed: {result.error}")

        # Unwrap WorkflowNodeResult if present (internal worker representation)
        if isinstance(result.result, WorkflowNodeResult):
            # Extract the actual result from the variables dict
            variables = result.result.variables
            program = cls.workflow_ir()
            # Get the return variable from the IR if available
            if program.functions:
                outputs = list(program.functions[0].io.outputs)
                if outputs:
                    return_var = outputs[0]
                    if return_var in variables:
                        return variables[return_var]
            return None

        return result.result

    cls.__workflow_run_impl__ = run_impl
    cls.run = run_public  # type: ignore[assignment]
    workflow_registry.register(cls.short_name(), cls)
    return cls


def _running_under_pytest() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _skip_wait_for_instance() -> bool:
    value = os.environ.get("RAPPEL_SKIP_WAIT_FOR_INSTANCE")
    if not value:
        return False
    return value.strip().lower() not in {"0", "false", "no"}
