"""Sequences preserve ordering, empty values, and supported spreads."""

import asyncio

import pytest

from waymark import Workflow, workflow
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.errors import UnsupportedPatternError


@workflow
class SequenceWorkflow(Workflow):
    async def run(self, values: list[int]) -> list[int]:
        pair = (3, 4)
        return [0, *values, *[], 2, *pair, 5]


@pytest.mark.parametrize("values", [[], [1], [6, 7]])
def test_sequence_segments_keep_their_order(values: list[int]) -> None:
    assert asyncio.run(SequenceWorkflow().run(values)) == [0, *values, 2, 3, 4, 5]


@pytest.mark.parametrize("source", ["return [lambda: 1]", "return (value for value in [])"])
def test_unsupported_sequence_elements_are_rejected(source: str) -> None:
    with pytest.raises(UnsupportedPatternError):
        compile_source(source)
