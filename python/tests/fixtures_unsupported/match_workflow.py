"""Test fixture: match statement (unsupported pattern).

This uses match statement syntax (Python 3.10+) which is not supported
and should raise UnsupportedPatternError.
"""

from waymark import workflow
from waymark.workflow import Workflow


@workflow
class MatchWorkflow(Workflow):
    """Workflow with match statement that should raise an error."""

    async def run(self, x: int) -> str:
        match x:
            case 1:
                return "one"
            case _:
                return "other"
