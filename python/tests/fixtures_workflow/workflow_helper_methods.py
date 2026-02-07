"""Test fixture: Workflow with helper methods called via self.method()."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_data(key: str) -> str:
    return f"data_{key}"


@action
async def process_data(data: str, multiplier: int) -> str:
    return f"{data}*{multiplier}"


@workflow
class WorkflowWithHelperMethods(Workflow):
    """Workflow that calls helper methods defined on the class.

    Helper methods are regular Python functions that can be called
    synchronously within the workflow. They don't need @action decoration
    since they don't represent durable operations.
    """

    def compute_multiplier(self, base: int, factor: int) -> int:
        """Pure Python helper - computes a value synchronously."""
        return base * factor

    def format_result(self, value: str) -> str:
        """Pure Python helper - formats the final result."""
        return "[RESULT: " + value + "]"

    async def run(self, key: str, base: int, factor: int) -> str:
        # Call an action
        data = await fetch_data(key=key)

        # Call a helper method (self.method())
        multiplier = self.compute_multiplier(base=base, factor=factor)

        # Call another action with the computed value
        processed = await process_data(data=data, multiplier=multiplier)

        # Call another helper to format result
        result = self.format_result(value=processed)

        return result
