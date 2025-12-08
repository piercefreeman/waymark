"""Fixture: list comprehension with ternary expression expands via loop."""

from rappel import Workflow, workflow


@workflow
class ListComprehensionIfExpWorkflow(Workflow):
    """Workflow that uses a ternary expression inside a list comprehension."""

    async def run(self, users: list) -> list:
        statuses = [user.name if user.active else "inactive" for user in users]
        return statuses
