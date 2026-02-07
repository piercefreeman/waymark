"""Test actions for integration tests."""

import builtins

from rappel.actions import action


@action
async def greet(name: str) -> str:
    """Simple greeting action for testing."""
    return f"Hello, {name}!"


@action
async def add(a: int, b: int) -> int:
    """Simple addition action for testing."""
    return a + b


@action
async def double(value: int) -> int:
    """Double a numeric value."""
    return value * 2


@action
async def sum(values: list[int]) -> int:
    """Sum a list of integers."""
    return builtins.sum(values)


@action
async def echo(message: str) -> str:
    """Echo action that returns the input."""
    return message
