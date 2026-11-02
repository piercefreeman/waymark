"""Traverse source once, dispatching construct checks to their registered handlers."""

import ast

from waymark.compiler.context import CompilerContext
from waymark.language_syntax import SyntaxRegistry


class SourceValidator(ast.NodeVisitor):
    """Route source nodes to validation using the function's shared analysis."""

    def __init__(self, context: CompilerContext, registry: SyntaxRegistry):
        self.context = context
        self.registry = registry

    def validate(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Validate executable bodies; annotations are checked by the function compiler."""
        for statement in node.body:
            self.visit(statement)

    def visit(self, node: ast.AST) -> None:
        """Dispatch registered syntax handlers before walking their children."""
        handler = self.registry.resolve(node)
        if handler is not None:
            handler.validate(node, self.context)
            for child in handler.children(node):
                self.visit(child)
        else:
            super().visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        handler = self.context.resolve(node.func)
        if handler is not None:
            handler.validate_usage(
                node, self.context.analysis.parents.get(node), self.context.analysis.kind
            )
        self.generic_visit(node)
