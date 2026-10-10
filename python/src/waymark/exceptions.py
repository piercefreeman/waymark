"""Custom exception types raised by waymark workflows."""

from .serialization import ExceptionValue


class ExhaustedRetriesError(Exception):
    """Defined for an action exhausting its retry attempts; nothing raises it today.

    An exhausted ``RetryPolicy`` surfaces the action's own final exception
    through the workflow instead. ``ExhaustedRetries`` is the same class.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "action exhausted retries")


ExhaustedRetries = ExhaustedRetriesError


class ScheduleAlreadyExistsError(Exception):
    """Defined for a schedule name conflict; nothing raises it today.

    Registering a schedule name that already exists replaces that schedule.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "schedule already exists")


class WorkflowFailedError(Exception):
    """Raised when a workflow run ended with an exception.

    `exception_value` is the exception the workflow raised, as the VM
    models it.
    """

    def __init__(self, exception_value: ExceptionValue) -> None:
        self.exception_value = exception_value
        super().__init__(exception_value)

    def __str__(self) -> str:
        return f"workflow failed: {self.exception_value}"
