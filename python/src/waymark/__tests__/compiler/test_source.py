"""Source discovery retains lexical bindings and source coordinates."""

import inspect

from waymark import Workflow
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.source import load_workflow_source, parse_function


def test_source_discovery_keeps_helper_defaults_and_closure_bindings() -> None:
    """Inherited helpers are parsed once, with their defining closure and signature."""
    offset = 4

    class Base(Workflow):
        async def helper(self, value: int = 3) -> int:
            return value + offset

    class Example(Base):
        async def run(self) -> int:
            return await self.helper()

    functions = load_workflow_source(Example)
    assert [function.name for function in functions] == ["main", "helper"]
    assert functions[1].namespace["offset"] == 4
    assert functions[1].signature.parameters["value"].default == 3
    assert "self" not in functions[1].signature.parameters


def test_parser_preserves_original_diagnostic_coordinates() -> None:
    """Dedenting source must not move diagnostics away from the offending expression."""

    def example(self):
        return object()

    parsed = parse_function(example)
    location = parsed.node.body[0]
    error = parsed.locate_error(
        UnsupportedPatternError(
            "unsupported", "rewrite", line=location.lineno, col=location.col_offset
        )
    )
    assert error.filename == inspect.getsourcefile(example)
    assert error.line == inspect.getsourcelines(example)[1] + 1
    assert error.col == 9
