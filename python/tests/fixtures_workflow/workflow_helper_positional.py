"""Test fixture: Workflow helper methods called with positional args."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def get_value() -> int:
    return 42


@workflow
class WorkflowHelperPositionalArgs(Workflow):
    """Workflow that calls helper methods with positional arguments.

    Helper methods can be called with positional args, which should be
    preserved in the IR (not converted to kwargs like action calls).
    """

    def add(self, a: int, b: int) -> int:
        """Helper that adds two numbers."""
        return a + b

    def multiply(self, x: int, y: int, z: int) -> int:
        """Helper that multiplies three numbers."""
        return x * y * z

    async def run(self) -> int:
        value = await get_value()

        # Positional args to helper method
        sum_result = self.add(value, 10)

        # Mixed positional and keyword args
        product = self.multiply(sum_result, 2, z=3)

        return product
