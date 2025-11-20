"""Custom exception types raised by rappel workflows."""


class ExhaustedRetriesError(Exception):
    """Raised when an action exhausts its allotted retry attempts."""

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "action exhausted retries")


ExhaustedRetries = ExhaustedRetriesError
