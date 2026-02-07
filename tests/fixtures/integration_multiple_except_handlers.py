"""Integration test: multiple except handlers execute correctly.

Issue 3: Multiple except blocks all execute
When a try block has multiple except handlers for different exception types,
only the first matching handler should execute, not all handlers.
"""

from datetime import timedelta

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


class SpecificError(Exception):
    """A specific error type that should be caught by its own handler."""

    pass


@action
async def raise_specific() -> str:
    """Raise a SpecificError."""
    raise SpecificError("specific error occurred")


@action
async def raise_generic() -> str:
    """Raise a generic ValueError."""
    raise ValueError("generic error occurred")


@action
async def log_handler(handler_name: str) -> str:
    """Log which handler was invoked."""
    return handler_name


@action
async def format_handlers(handlers: list[str]) -> dict:
    """Format the list of handlers that were invoked."""
    return {"handlers_called": handlers, "count": len(handlers)}


@workflow
class MultipleExceptHandlersWorkflow(Workflow):
    """Test that only one except handler executes when exception is raised."""

    async def run(self, raise_specific_error: bool = True) -> dict:
        """
        Expected: Only one handler executes (the most specific matching one).
        Bug: Both handlers may execute when SpecificError is raised.
        """
        handlers_called: list[str] = []

        try:
            if raise_specific_error:
                await self.run_action(
                    raise_specific(),
                    retry=RetryPolicy(attempts=1),
                    timeout=timedelta(seconds=5),
                )
            else:
                await self.run_action(
                    raise_generic(),
                    retry=RetryPolicy(attempts=1),
                    timeout=timedelta(seconds=5),
                )
        except SpecificError:
            handler = await log_handler(handler_name="SpecificError")
            handlers_called.append(handler)
        except Exception:
            handler = await log_handler(handler_name="Exception")
            handlers_called.append(handler)

        return await format_handlers(handlers=handlers_called)
