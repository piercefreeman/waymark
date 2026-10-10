"""Fixture: an except clause typed through a `self` attribute is not supported."""

from waymark import Workflow, action, workflow


@action
async def flaky_action() -> str:
    return "ok"


@workflow
class ExceptSelfAttributeWorkflow(Workflow):
    """Workflow listing an except type through `self` - should fail validation."""

    def __init__(self) -> None:
        super().__init__()
        self.error_cls = ValueError

    async def run(self) -> str:
        try:
            result = await flaky_action()
        except self.error_cls:
            result = "handled"
        return result
