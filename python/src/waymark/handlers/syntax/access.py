"""Attribute and index access own their resolution, restrictions, and lowering."""

import ast
import copy
from collections.abc import Mapping
from enum import EnumMeta
from typing import Any

from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.compiler.diagnostics import reject
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.literals import constant_to_literal
from waymark.compiler.recommendations import RECOMMENDATIONS
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


class AccessHandler(ExpressionHandler[ast.Attribute | ast.Subscript]):
    node_types = (ast.Attribute, ast.Subscript)

    def validate(self, node: ast.Attribute | ast.Subscript, context: CompilerContext) -> None:
        """Reject inspection of caught exceptions while preserving ordinary value access."""
        if context.analysis.kind(node.value) == "exception":
            reject(node, "Caught exception inspection is unsupported")

    def lower(self, node: ast.Attribute | ast.Subscript, context: CompilerContext) -> ir.Expr:
        """Resolve static attributes or emit runtime dot/index access."""
        if isinstance(node, ast.Attribute):
            resolved = self._resolve_enum_attribute(node, context)
            if resolved is not None:
                return resolved
        value = context.expression(node.value)
        if isinstance(node, ast.Subscript):
            index = context.expression(node.slice)
            return ir.Expr(index=ir.IndexAccess(object=value, index=index), span=make_span(node))
        return ir.Expr(dot=ir.DotAccess(object=value, attribute=node.attr), span=make_span(node))

    def _resolve_enum_attribute(
        self, node: ast.Attribute, context: CompilerContext
    ) -> ir.Expr | None:
        instance = self._resolve_instance_attribute_chain(node, context)
        if instance is not None:
            return instance
        value = _resolve_enum_attribute_value(node, context.namespace)
        if value is None:
            return None
        literal = constant_to_literal(value)
        if literal is None:
            line = node.lineno
            col = node.col_offset
            raise UnsupportedPatternError(
                "Enum value must be a primitive literal",
                RECOMMENDATIONS["unsupported_literal"],
                line=line,
                col=col,
            )
        expr = ir.Expr(span=make_span(node))
        expr.literal.CopyFrom(literal)
        return expr

    def _resolve_instance_attribute_chain(
        self, node: ast.expr, context: CompilerContext
    ) -> ir.Expr | None:
        """Lower an initializer once, then select nested fields from its resulting value."""
        chain = self._extract_self_attribute_chain(node)
        if chain is None:
            return None
        top_attr = chain[0]
        if top_attr not in context.instance_attrs or top_attr in context.ambiguous_instance_attrs:
            raise UnsupportedPatternError(
                "Instance attribute is not assigned a unique literal in __init__",
                "Pass runtime values through run() arguments.",
                line=node.lineno,
                col=node.col_offset,
            )

        class InitializerEnums(ast.NodeTransformer):
            def visit_Attribute(self, attribute: ast.Attribute) -> ast.AST:
                """Keep initializer enum references in their defining scope."""
                value = _resolve_enum_attribute_value(attribute, context.instance_namespace)
                if value is not None:
                    return ast.copy_location(ast.Constant(value=value), attribute)
                return self.generic_visit(attribute)

        bound = InitializerEnums().visit(copy.deepcopy(context.instance_attrs[top_attr]))
        assert isinstance(bound, ast.expr)
        resolved = self._resolve_nested_attributes(context.expression(bound), chain[1:])
        if resolved is None:
            raise UnsupportedPatternError(
                "Instance attribute cannot be resolved at compile time",
                "Pass runtime values through run() arguments.",
                line=node.lineno,
                col=node.col_offset,
            )
        return resolved

    def _extract_self_attribute_chain(self, node: ast.expr) -> list[str] | None:
        """Extract attribute chain from self.a.b.c pattern.

        Returns ["a", "b", "c"] for self.a.b.c, or None if not a self chain.
        """
        chain: list[str] = []
        current = node

        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value

        # Check if the base is "self"
        if isinstance(current, ast.Name) and current.id == "self":
            chain.reverse()
            return chain

        return None

    def _resolve_nested_attributes(self, value: ir.Expr, attrs: list[str]) -> ir.Expr | None:
        """Read already-lowered dictionary fields without recompiling selected constructor arguments."""
        for name in attrs:
            if not value.HasField("dict"):
                return None
            # Dictionary construction keeps the last occurrence of a duplicate key.
            entry = next(
                (
                    entry
                    for entry in reversed(value.dict.entries)
                    if entry.key.HasField("literal")
                    and entry.key.literal.HasField("string_value")
                    and entry.key.literal.string_value == name
                ),
                None,
            )
            if entry is None:
                return None
            value = entry.value
        return value


def _attribute_chain(node: ast.Attribute) -> list[str] | None:
    parts: list[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return list(reversed(parts))
    return None


def _resolve_enum_attribute_value(
    node: ast.Attribute,
    module_globals: Mapping[str, Any],
) -> Any | None:
    chain = _attribute_chain(node)
    if not chain or len(chain) < 2:
        return None

    current = module_globals.get(chain[0])
    if current is None:
        return None

    for part in chain[1:-1]:
        try:
            current_dict = current.__dict__
        except AttributeError:
            return None
        current = current_dict.get(part)
        if current is None:
            return None

    member_name = chain[-1]
    if isinstance(current, EnumMeta):
        member = current.__members__.get(member_name)
        if member is None:
            return None
        return member.value

    return None
