"""Fixture: dict comprehension assignment expands to loop-based accumulator."""

from rappel import Workflow, workflow


@workflow
class DictComprehensionAssignmentWorkflow(Workflow):
    """Workflow that filters active users into a dict comprehension assignment."""

    async def run(self, users: list) -> dict:
        active_lookup = {user.id: user.name for user in users if user.active}
        return active_lookup
