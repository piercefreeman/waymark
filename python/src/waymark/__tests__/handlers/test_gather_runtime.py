"""Runtime regressions for gather's comprehension lowering."""

import asyncio

from waymark import Workflow, action, workflow


@action
async def gather_label(label: str) -> str:
    return label


@workflow
class SequentialGatherFiltersWorkflow(Workflow):
    async def run(self) -> list[str]:
        return await asyncio.gather(
            *[gather_label("present") for values in [[], [1]] if values if values[0]]
        )


def test_gather_filters_short_circuit_before_indexing_empty_values() -> None:
    """Regression: later filters must not index a value rejected by an earlier filter."""
    assert asyncio.run(SequentialGatherFiltersWorkflow().run()) == ["present"]


@workflow
class GatherFilterScopeWorkflow(Workflow):
    async def run(self) -> str:
        item = "outer"
        await asyncio.gather(*[gather_label(item) for item in ["first", "second"] if item])
        return item


def test_filtered_gather_preserves_outer_comprehension_bindings() -> None:
    """Regression: filtering a nonempty spread must not overwrite an outer loop variable."""
    assert asyncio.run(GatherFilterScopeWorkflow().run()) == "outer"
