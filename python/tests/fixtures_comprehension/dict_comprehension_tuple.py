"""Fixture: dict comprehension assignment with tuple unpacking."""

from rappel import Workflow, workflow


@workflow
class DictComprehensionTupleWorkflow(Workflow):
    """Workflow that builds a dict comprehension from tuple pairs."""

    async def run(self, pairs: list[tuple[str, int]]) -> dict:
        mapping = {key: value for key, value in pairs}
        return mapping
