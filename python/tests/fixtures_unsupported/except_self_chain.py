"""Fixture: an except clause dotted through a `self` attribute chain is rejected."""

from types import SimpleNamespace

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptSelfChainWorkflow(Workflow):
    """Workflow listing `except self.errors.Primary:` - rooted in `self`, fails validation."""

    def __init__(self) -> None:
        super().__init__()
        self.errors = SimpleNamespace(Primary=ValueError)

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except self.errors.Primary:
            result = "handled"
        return result
