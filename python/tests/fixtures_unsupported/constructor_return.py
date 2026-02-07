"""Fixture: returning a constructor call is not supported.

Note: Pydantic models and dataclasses ARE supported and will be converted
to dict expressions. This test uses a regular class which is NOT supported.
"""

from waymark import Workflow, action, workflow


class CustomResult:
    """Regular class (not Pydantic/dataclass) - constructor calls are not supported."""

    def __init__(self, value: int, message: str):
        self.value = value
        self.message = message


@action
async def get_value() -> int:
    return 42


@workflow
class ConstructorReturnWorkflow(Workflow):
    """Workflow that returns a constructor - should fail validation."""

    async def run(self) -> CustomResult:
        value = await get_value()
        # This should fail: returning a constructor call for non-model class
        return CustomResult(value=value, message="done")
