"""Test fixture: asyncio.gather with self.run_action in static pattern."""

import asyncio
from datetime import timedelta

from waymark import action, workflow
from waymark.workflow import RetryPolicy, Workflow


@action
async def upload_ci_logs(path: str) -> str:
    """Upload CI logs to storage."""
    return f"uploaded:{path}"


@action
async def cleanup_ci_sandbox(sandbox_id: str) -> str:
    """Clean up a CI sandbox environment."""
    return f"cleaned:{sandbox_id}"


@action
async def send_notification(message: str) -> str:
    """Send a notification."""
    return f"sent:{message}"


@workflow
class GatherRunActionStaticWorkflow(Workflow):
    """Workflow using self.run_action with retry/timeout in static gather pattern.

    Pattern: await asyncio.gather(
        self.run_action(action1(...), retry=..., timeout=...),
        self.run_action(action2(...), retry=..., timeout=...),
        return_exceptions=True,
    )
    """

    async def run(self, log_path: str, sandbox_id: str) -> tuple:
        results = await asyncio.gather(
            self.run_action(
                upload_ci_logs(path=log_path),
                timeout=timedelta(minutes=2),
                retry=RetryPolicy(attempts=3),
            ),
            self.run_action(
                cleanup_ci_sandbox(sandbox_id=sandbox_id),
                timeout=timedelta(minutes=1),
                retry=RetryPolicy(attempts=2),
            ),
            return_exceptions=True,
        )
        return results


@workflow
class GatherMixedRunActionWorkflow(Workflow):
    """Workflow mixing self.run_action() and direct action calls in gather.

    Pattern: await asyncio.gather(
        self.run_action(action1(...), retry=...),
        action2(...),  # direct call without wrapper
        return_exceptions=True,
    )
    """

    async def run(self, log_path: str, message: str) -> tuple:
        results = await asyncio.gather(
            self.run_action(
                upload_ci_logs(path=log_path),
                retry=RetryPolicy(attempts=3),
            ),
            send_notification(message=message),  # No run_action wrapper
            return_exceptions=True,
        )
        return results
