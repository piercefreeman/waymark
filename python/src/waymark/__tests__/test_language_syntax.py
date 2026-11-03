"""Syntax registration routes validation and lowering through the same handler."""

import ast

import pytest

from waymark import ir_builder
from waymark.__tests__.compiler_test_helpers import compile_source
from waymark.compiler.analysis import SourceAnalysis
from waymark.compiler.context import CompilerContext
from waymark.language_syntax import StatementHandler, SyntaxRegistry
from waymark.proto import ast_pb2 as ir


class ExampleHandler(StatementHandler[ast.Pass]):
    node_types = (ast.Pass,)

    def __init__(self) -> None:
        self.visited: list[str] = []

    def validate(self, node: ast.Pass, context: CompilerContext) -> None:
        """Observe that the driver supplies the original node's analysis."""
        assert node in context.analysis.parents
        self.visited.append("validate")

    def lower(self, node: ast.Pass, context: CompilerContext) -> list[ir.Statement]:
        """Make handler dispatch observable in the compiled program."""
        self.visited.append("lower")
        return [
            ir.Statement(return_stmt=ir.ReturnStmt(value=ir.Expr(literal=ir.Literal(int_value=7))))
        ]


def test_registration_dispatches_validation_and_lowering(monkeypatch: pytest.MonkeyPatch) -> None:
    """Adding a handler requires no builder-specific method or name check."""
    handler = ExampleHandler()
    monkeypatch.setattr(ir_builder, "SYNTAX_HANDLERS", SyntaxRegistry([handler]))
    function = compile_source("pass")
    assert function.body.statements[0].return_stmt.value.literal.int_value == 7
    assert handler.visited == ["validate", "lower"]


def test_duplicate_syntax_registration_is_rejected() -> None:
    """Ambiguous ownership cannot silently replace an earlier construct handler."""
    with pytest.raises(ValueError, match="Duplicate syntax handler for Pass"):
        SyntaxRegistry([ExampleHandler(), ExampleHandler()])


def test_source_analysis_collects_facts_without_rejecting_syntax() -> None:
    """Analysis remains usable even when a construct handler will reject the program."""
    function = ast.parse(
        "def run():\n    values = []\n    alias = values\n    values.append(1)\n    try:\n        pass\n    finally:\n        pass"
    ).body[0]
    assert isinstance(function, ast.FunctionDef)
    analysis = SourceAnalysis()
    analysis.analyze(function)
    assert analysis.types["values"] == "list"
    assert analysis.types["alias"] == "list"
    assert "values" in analysis.aliased
