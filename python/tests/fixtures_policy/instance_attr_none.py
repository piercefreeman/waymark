"""Test fixture: a `None` policy reached through a self attribute means no policy."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def action_with_none_retry(value: str) -> str:
    """Action whose retry policy is None through self."""
    return f"done({value})"


@action
async def action_with_none_timeout(value: str) -> str:
    """Action whose timeout is None through self."""
    return f"done({value})"


@workflow
class InstanceAttrNoneWorkflow(Workflow):
    """Workflow whose __init__ sets both policies to None."""

    def __init__(self) -> None:
        super().__init__()
        self.retry_policy = None
        self.timeout_value = None

    async def run(self, value: str) -> str:
        a = await self.run_action(action_with_none_retry(value=value), retry=self.retry_policy)
        return await self.run_action(action_with_none_timeout(value=a), timeout=self.timeout_value)
