"""Test fixture: Action with positional arguments.

The IR builder converts positional arguments to keyword arguments using
signature introspection. This ensures all arguments are named in the IR.
"""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def add_numbers(a: int, b: int) -> int:
    return a + b


@workflow
class ActionPositionalArgsWorkflow(Workflow):
    """Action called with positional arguments.

    The IR builder uses signature introspection to convert positional
    arguments to kwargs. Both syntaxes are fully supported.
    """

    async def run(self) -> int:
        # Positional args are converted to kwargs using signature introspection
        result = await add_numbers(10, 20)
        return result
