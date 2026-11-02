"""Dictionary handlers preserve entries and reject unsupported dictionary forms."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@workflow
class DictionaryWorkflow(Workflow):
    async def run(self, value: int) -> dict[str, int]:
        """Mix literal and runtime values in one dictionary."""
        return {"input": value, "computed": value + 1}


def test_dictionary_entries_survive_execution() -> None:
    """All entries are retained through IR construction and VM execution."""
    assert asyncio.run(DictionaryWorkflow().run(4)) == {"input": 4, "computed": 5}


@pytest.mark.parametrize(
    "expression", ["{**{'key': 1}}", "{1: 'one'}", "{key: key for key in [1]}"]
)
def test_unsupported_dictionary_forms_are_rejected(expression: str) -> None:
    """Spreads, non-string keys, and comprehensions cannot silently lose their entries."""
    with pytest.raises(UnsupportedPatternError):
        compile_source(f"return {expression}")
