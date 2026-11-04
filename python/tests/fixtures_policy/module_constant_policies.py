"""Test fixture: policies read from module-level constants.

Regression: `RetryPolicy(attempts=AGENT_ATTEMPTS)` was rejected as "attempts
must be an integer literal", forcing every call site to repeat the literal.
Each global here compiles as the value it holds at compile time, however
that value was computed. `RETRY_ON` is a list, which a workflow cannot read
directly, but a `RetryPolicy` global built from it is a whole policy.
"""

from datetime import timedelta

from waymark import action, workflow
from waymark.workflow import RetryPolicy, Workflow

AGENT_ATTEMPTS = 1 + 2
BACKOFF_SECONDS = 2.5
RETRY_ON = ["ConnectionError", "TimeoutError"]
TIMEOUT_SECONDS = 60 * 2
SHARED_RETRY = RetryPolicy(attempts=AGENT_ATTEMPTS, exception_types=RETRY_ON)
SHARED_TIMEOUT = timedelta(minutes=5) * 2


@action
async def call_agent(value: str) -> str:
    """Action called under each constant-driven policy."""
    return f"done({value})"


@workflow
class ModuleConstantPoliciesWorkflow(Workflow):
    """Workflow whose policies name module constants instead of literals."""

    def __init__(self) -> None:
        super().__init__()
        self.init_retry = RetryPolicy(attempts=AGENT_ATTEMPTS, backoff_seconds=BACKOFF_SECONDS)
        self.init_timeout = SHARED_TIMEOUT

    async def run(self, value: str) -> str:
        a = await self.run_action(
            call_agent(value=value),
            retry=RetryPolicy(
                attempts=AGENT_ATTEMPTS,
                backoff_seconds=BACKOFF_SECONDS,
            ),
            timeout=TIMEOUT_SECONDS,
        )
        b = await self.run_action(call_agent(value=a), retry=SHARED_RETRY, timeout=SHARED_TIMEOUT)
        return await self.run_action(
            call_agent(value=b), retry=self.init_retry, timeout=self.init_timeout
        )
