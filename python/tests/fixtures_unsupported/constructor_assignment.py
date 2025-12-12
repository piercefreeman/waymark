"""Fixture: assigning a constructor call is not supported.

Note: Pydantic models and dataclasses ARE supported and will be converted
to dict expressions. This test uses a regular class which is NOT supported.
"""

from rappel import Workflow, action, workflow


class CustomConfig:
    """Regular class (not Pydantic/dataclass) - constructor calls are not supported."""

    def __init__(self, timeout: int, retries: int):
        self.timeout = timeout
        self.retries = retries


@action
async def get_timeout() -> int:
    return 30


@workflow
class ConstructorAssignmentWorkflow(Workflow):
    """Workflow that assigns a constructor - should fail validation."""

    async def run(self) -> int:
        timeout = await get_timeout()
        # This should fail: assigning a constructor call for non-model class
        config = CustomConfig(timeout=timeout, retries=3)
        return config.timeout
