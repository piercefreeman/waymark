"""Test fixture: Child workflow calls async helper in base class."""

from tests.fixtures_workflow.workflow_helper_inheritance_base import BaseWorkflowWithHelper
from waymark import workflow


@workflow
class WorkflowWithHelperInheritance(BaseWorkflowWithHelper):
    """Workflow that calls a base-class helper method."""

    async def run(self, value: int) -> int:
        result = await self.run_internal(value=value)
        return result
