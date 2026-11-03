"""Validation and lowering of supported try/except statements."""

import ast
from types import ModuleType

from waymark.compiler.ast import make_span, resolve_reference
from waymark.compiler.context import CompilerContext
from waymark.compiler.diagnostics import reject
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.recommendations import RECOMMENDATIONS
from waymark.language_syntax import StatementHandler
from waymark.proto import ast_pb2 as ir


class TryHandler(StatementHandler[ast.Try]):
    node_types = (ast.Try,)

    def validate(self, node: ast.Try, context: CompilerContext) -> None:
        """Check this construct against the shared source facts."""
        if node.finalbody or node.orelse:
            reject(node, "try/finally and try/else are unsupported")

    def lower(self, node: ast.Try, context: CompilerContext) -> list[ir.Statement]:
        """Preserve the try body, each catch body, exception names, and source positions."""
        try_body = context.block(node.body, node)
        handlers = []
        for handler in node.handlers:
            exception_types = []
            if isinstance(handler.type, ast.Tuple):
                if not handler.type.elts:
                    raise UnsupportedPatternError(
                        "An except clause with an empty tuple catches nothing",
                        RECOMMENDATIONS["except_empty_tuple"],
                        line=handler.lineno,
                        col=handler.col_offset,
                    )
                exception_types = [
                    self._exception_name(item, context) for item in handler.type.elts
                ]
            elif handler.type is not None:
                exception_types = [self._exception_name(handler.type, context)]
            handlers.append(
                ir.ExceptHandler(
                    exception_types=exception_types,
                    block_body=context.block(handler.body, handler),
                    span=make_span(handler),
                    exception_var=handler.name,
                )
            )
        return [
            ir.Statement(
                try_except=ir.TryExcept(try_block=try_body, handlers=handlers),
                span=make_span(node),
            )
        ]

    def _exception_name(self, node: ast.expr, context: CompilerContext) -> str:
        if isinstance(node, ast.Attribute):
            root = node.value
            while isinstance(root, ast.Attribute):
                root = root.value
            if not isinstance(root, ast.Name) or not isinstance(
                context.namespace.get(root.id), (ModuleType, type)
            ):
                raise UnsupportedPatternError(
                    "An except clause's dotted name must be rooted in a module or a class",
                    RECOMMENDATIONS["except_type"],
                    line=node.lineno,
                    col=node.col_offset,
                )
        elif not isinstance(node, ast.Name):
            raise UnsupportedPatternError(
                "An except clause must list exception classes by name",
                RECOMMENDATIONS["except_type"],
                line=node.lineno,
                col=node.col_offset,
            )
        resolved = resolve_reference(node, context.namespace)
        if isinstance(resolved, type) and issubclass(resolved, BaseException):
            return resolved.__name__
        raise UnsupportedPatternError(
            f"An except clause names {ast.unparse(node)}, which is not an exception class in scope",
            RECOMMENDATIONS["except_type"],
            line=node.lineno,
            col=node.col_offset,
        )
