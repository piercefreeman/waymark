"""Test fixture: Workflow with helper methods that have default arguments."""

from enum import Enum

from waymark import action, workflow
from waymark.workflow import Workflow


class Priority(Enum):
    LOW = 1
    HIGH = 2


@action
async def do_work(value: int) -> int:
    return value * 2


@action
async def process_with_config(value: int, multiplier: int = 1) -> int:
    return value * multiplier


@workflow
class WorkflowWithDefaultArgs(Workflow):
    """Workflow that calls helper methods with optional parameters.

    This tests that when a caller omits an argument that has a default,
    the IR builder correctly fills in the default value.
    """

    async def helper_with_defaults(
        self,
        *,
        required_arg: int,
        optional_arg: int | None = None,
        optional_str: str = "default",
    ) -> int:
        """Helper method with some optional parameters."""
        result = await do_work(value=required_arg)
        return result

    async def helper_with_int_default(
        self,
        value: int,
        multiplier: int = 10,
    ) -> int:
        """Helper with int default."""
        result = await do_work(value=value * multiplier)
        return result

    async def run(self, value: int) -> int:
        # Call helper without optional_arg - should get default None
        # Call helper without optional_str - should get default "default"
        result = await self.helper_with_defaults(required_arg=value)

        # Call with int default omitted
        result2 = await self.helper_with_int_default(value=result)

        return result2


@workflow
class WorkflowWithComplexDefaults(Workflow):
    """Tests complex default values that can't be converted to literals."""

    async def helper_with_list_default(
        self,
        value: int,
        items: list[int] | None = None,  # Use None instead of [] as default
    ) -> int:
        result = await do_work(value=value)
        return result

    async def run(self, value: int) -> int:
        # This should work - None is a valid literal
        result = await self.helper_with_list_default(value=value)
        return result


@workflow
class WorkflowWithNestedCalls(Workflow):
    """Tests function calls nested in other expressions."""

    async def get_multiplier(self, base: int = 2) -> int:
        return base

    async def compute(self, value: int, mult: int) -> int:
        result = await do_work(value=value * mult)
        return result

    async def run(self, value: int) -> int:
        # Nested call - self.get_multiplier() is called without args
        # and its result is passed to compute
        mult = await self.get_multiplier()  # Should fill in base=2
        result = await self.compute(value=value, mult=mult)
        return result


@workflow
class WorkflowWithTrulyNestedCalls(Workflow):
    """Tests function calls as arguments to other function calls."""

    async def get_offset(self, delta: int = 5) -> int:
        return delta

    async def apply_offset(self, value: int, offset: int) -> int:
        result = await do_work(value=value + offset)
        return result

    async def run(self, value: int) -> int:
        # Truly nested - get_offset() result is directly passed to apply_offset
        # get_offset() should have delta=5 filled in
        result = await self.apply_offset(value=value, offset=await self.get_offset())
        return result
