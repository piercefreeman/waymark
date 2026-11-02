"""Typed syntax handlers registered by AST node type."""

import ast
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Generic, TypeVar, cast

from waymark.compiler.context import CompilerContext
from waymark.proto import ast_pb2 as ir

Node = TypeVar("Node", bound=ast.AST)


class SyntaxHandler(ABC, Generic[Node]):
    """Own a construct's support rules; statement and expression handlers also lower it."""

    node_types: tuple[type[Node], ...]

    def children(self, node: Node) -> Iterable[ast.AST]:
        """Expose executable children; handlers can exclude syntax used only as metadata."""
        return ast.iter_child_nodes(node)

    @abstractmethod
    def validate(self, node: Node, context: CompilerContext) -> None:
        """Check source syntax using shared facts, without evaluating runtime expressions."""


class StatementHandler(SyntaxHandler[Node]):
    """Lower a statement construct into its ordered IR statements."""

    @abstractmethod
    def lower(self, node: Node, context: CompilerContext) -> list[ir.Statement]:
        """Emit the validated statement and any required setup."""


class ExpressionHandler(SyntaxHandler[Node]):
    """Lower an expression construct to a VM value expression."""

    @abstractmethod
    def lower(self, node: Node, context: CompilerContext) -> ir.Expr:
        """Emit the validated expression."""


class SyntaxRegistry:
    """Resolve constructs by AST type without teaching the builder handler names."""

    def __init__(self, handlers: Iterable[SyntaxHandler[Any]]) -> None:
        self._handlers: dict[type[ast.AST], SyntaxHandler[Any]] = {}
        for handler in handlers:
            for node_type in handler.node_types:
                if node_type in self._handlers:
                    raise ValueError(f"Duplicate syntax handler for {node_type.__name__}")
                self._handlers[node_type] = handler

    def resolve(self, node: ast.AST) -> SyntaxHandler[Any] | None:
        """Find the handler registered for this exact syntax node type."""
        return self._handlers.get(type(node))

    def lower_statement(
        self, node: ast.stmt, context: CompilerContext
    ) -> list[ir.Statement] | None:
        """Lower a registered statement, returning None when ordinary dispatch should continue."""
        handler = self.resolve(node)
        if isinstance(handler, StatementHandler):
            return cast(StatementHandler[Any], handler).lower(node, context)
        return None

    def lower_expression(self, node: ast.expr, context: CompilerContext) -> ir.Expr | None:
        """Lower a registered expression using the same node-type registration as validation."""
        handler = self.resolve(node)
        if isinstance(handler, ExpressionHandler):
            return cast(ExpressionHandler[Any], handler).lower(node, context)
        return None
