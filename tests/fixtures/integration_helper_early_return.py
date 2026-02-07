"""Integration test for helper functions with multiple early returns.

This tests a bug where a helper function with mutually exclusive early-return
branches causes the continuation to wait for ALL branches instead of just ONE.

Pattern that causes the bug:
    async def helper(value):
        if condition1:
            await error_action1()
            return None  # early return 1
        if condition2:
            await error_action2()
            return None  # early return 2
        result = await main_action()
        return result

    async def run():
        result = await helper(value)
        # BUG: This continuation incorrectly waits for error_action1 AND
        # error_action2 AND main_action, when it should only wait for ONE
        # (whichever branch was taken).
        final = await process_result(result)
        return final
"""

from waymark import action, workflow
from waymark.workflow import Workflow


class ValidationError(Exception):
    """Raised when validation fails."""

    pass


class AuthError(Exception):
    """Raised when authentication fails."""

    pass


@action
async def check_has_items(items: list) -> bool:
    """Check if the list has items."""
    return len(items) > 0


@action
async def check_has_auth(user_id: str) -> bool:
    """Check if user has authentication."""
    # Simulate: user_id "invalid" has no auth, others do
    return user_id != "invalid"


@action
async def raise_validation_error() -> None:
    """Raise a validation error (early return path 1)."""
    raise ValidationError("No items to process")


@action
async def raise_auth_error() -> None:
    """Raise an auth error (early return path 2)."""
    raise AuthError("User not authenticated")


@action
async def process_items(items: list) -> str:
    """Process the items (main path)."""
    return f"processed:{len(items)}"


@action
async def format_result(result: str) -> str:
    """Format the final result (continuation after helper)."""
    return f"final:{result}"


@workflow
class HelperEarlyReturnWorkflow(Workflow):
    """Workflow that tests helper functions with early returns.

    The helper function `do_processing` has three mutually exclusive paths:
    1. Early return if no items (raises ValidationError)
    2. Early return if no auth (raises AuthError)
    3. Normal path that processes items

    The continuation `format_result` should only wait for ONE of these paths,
    not all three.
    """

    async def do_processing(self, items: list, user_id: str) -> str:
        """Helper with multiple early return branches."""
        # Check 1: Validate items exist
        has_items = await self.run_action(check_has_items(items))
        if not has_items:
            await self.run_action(raise_validation_error())
            # Unreachable - action above raises
            return ""  # type: ignore

        # Check 2: Validate user has auth
        has_auth = await self.run_action(check_has_auth(user_id))
        if not has_auth:
            await self.run_action(raise_auth_error())
            # Unreachable - action above raises
            return ""  # type: ignore

        # Main path: Process the items
        result = await self.run_action(process_items(items))
        return result

    async def run(self, items: list, user_id: str = "valid") -> str:
        """Main workflow entry point."""
        # Call the helper function (will be inlined)
        result = await self.do_processing(items, user_id)

        # This continuation should NOT wait for raise_validation_error or
        # raise_auth_error when those guards are false. It should only wait
        # for process_items (or whichever single path was taken).
        final = await self.run_action(format_result(result))
        return final
