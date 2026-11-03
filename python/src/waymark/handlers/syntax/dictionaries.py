"""Dictionary construction validates every key and preserves all entries."""

import ast

from waymark.compiler_ast import make_span
from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


class DictionaryHandler(ExpressionHandler[ast.Dict | ast.DictComp]):
    node_types = (ast.Dict, ast.DictComp)

    def validate(self, node: ast.Dict | ast.DictComp, context: CompilerContext) -> None:
        """Check this construct against the shared source facts."""
        if isinstance(node, ast.DictComp):
            return reject(node, "Dict comprehensions require unsupported subscript writes")
        for key in node.keys:
            if key is None:
                return reject(node, "Dictionary unpacking (**mapping) is unsupported")
            if context.analysis.kind(key) != "str":
                reject(key, "Dictionary keys must be known strings")

    def lower(self, node: ast.Dict | ast.DictComp, context: CompilerContext) -> ir.Expr:
        """Emit the supported dictionary entries in their source evaluation order."""
        if isinstance(node, ast.DictComp):
            return reject(node, "Dict comprehensions require unsupported subscript writes")
        entries = []
        for key, value in zip(node.keys, node.values, strict=True):
            if key is None:
                reject(node, "Dictionary unpacking (**mapping) is unsupported")
            assert key is not None
            key_value = context.expression(key)
            item_value = context.expression(value)
            assert key_value is not None and item_value is not None
            entries.append(ir.DictEntry(key=key_value, value=item_value))
        return ir.Expr(dict=ir.DictExpr(entries=entries), span=make_span(node))
