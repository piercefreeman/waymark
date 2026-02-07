"""Fixture: list comprehension assignment expands to loop-based accumulator."""

from waymark import Workflow, workflow


@workflow
class ListComprehensionAssignmentWorkflow(Workflow):
    """Workflow that filters active users with a list comprehension assignment."""

    async def run(self, users: list) -> list:
        active_users = [user for user in users if user.active]
        return active_users
