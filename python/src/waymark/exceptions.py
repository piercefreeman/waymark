"""Custom exception types raised by waymark workflows."""

from typing import Any


class ExhaustedRetriesError(Exception):
    """Raised when an action exhausts its allotted retry attempts."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "action exhausted retries")


ExhaustedRetries = ExhaustedRetriesError


class ScheduleAlreadyExistsError(Exception):
    """Raised when a schedule name is already registered."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "schedule already exists")


class WorkflowFailedError(Exception):
    """Raised when a workflow run ended with an exception.

    `type_id` names the exception type the workflow raised; `details`
    carries its serialized details, when the payload had any.
    """

    def __init__(self, error: dict[str, Any]) -> None:
        self.type_id: str = error["type_id"]
        self.details: dict[str, Any] | None = error.get("details")
        super().__init__(str(self))

    def _detail(self, key: str) -> Any:
        if self.details is None:
            return None
        return self.details.get(key)

    @property
    def message(self) -> str | None:
        """The message of the raised exception, when recorded."""
        message = self._detail("message")
        return message if isinstance(message, str) else None

    @property
    def module(self) -> str | None:
        """The module the exception type came from, when recorded."""
        module = self._detail("module")
        return module if isinstance(module, str) else None

    @property
    def traceback(self) -> str | None:
        """The formatted traceback, when recorded."""
        traceback = self._detail("traceback")
        return traceback if isinstance(traceback, str) else None

    @property
    def type_hierarchy(self) -> list[str] | None:
        """The exception type's class hierarchy, when recorded."""
        hierarchy = self._detail("type_hierarchy")
        return hierarchy if isinstance(hierarchy, list) else None

    @property
    def values(self) -> dict[str, Any] | None:
        """The exception's serialized attributes, when recorded."""
        values = self._detail("values")
        return values if isinstance(values, dict) else None

    def __str__(self) -> str:
        message = self.message
        if message:
            return f"workflow failed: {self.type_id}: {message}"
        return f"workflow failed: {self.type_id}"
