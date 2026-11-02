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
        """Resolve self.attr.subattr chains at build time.

        For expressions like self.config.max_retries where self.config is
        defined in __init__, evaluate the chain at build time and return
        the value as an IR literal.
        """
        chain = self._extract_self_attribute_chain(node)
        if chain is None:
            return None

        # chain[0] is the top-level attribute (e.g., "config")
        # chain[1:] are nested attributes (e.g., ["max_retries"])
        top_attr = chain[0]
        if top_attr not in context.instance_attrs or top_attr in context.ambiguous_instance_attrs:
            raise UnsupportedPatternError(
                "Instance attribute is not assigned a unique literal in __init__",
                "Pass runtime values through run() arguments.",
                line=node.lineno,
                col=node.col_offset,
            )

        # Get the AST expression for the top-level attribute
        attr_ast = context.instance_attrs[top_attr]

        # Try to resolve nested attributes
        resolved_ast = self._resolve_nested_attributes(attr_ast, chain[1:], context)
        if resolved_ast is None:
            raise UnsupportedPatternError(
                "Instance attribute cannot be resolved at compile time",
                "Pass runtime values through run() arguments.",
                line=node.lineno,
                col=node.col_offset,
            )

        # Bind initializer enum references before entering the current method's local scope.
        class InitializerEnums(ast.NodeTransformer):
            def visit_Attribute(self, attribute: ast.Attribute) -> ast.AST:
                """Keep initializer enum references in their defining scope."""
                value = _resolve_enum_attribute_value(attribute, context.instance_namespace)
                if value is not None:
                    return ast.copy_location(ast.Constant(value=value), attribute)
                return self.generic_visit(attribute)

        bound = InitializerEnums().visit(copy.deepcopy(resolved_ast))
        assert isinstance(bound, ast.expr)
        return context.expression(bound)

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

    def _resolve_nested_attributes(
        self, ast_node: ast.expr, attrs: list[str], context: CompilerContext
    ) -> ast.expr | None:
        """Resolve nested attributes on an AST expression.

        For a Call like ClassName(a=1, b=2), resolving ["a"] returns the
        AST node for 1.
        """
        if not attrs:
            return ast_node

        attr_name = attrs[0]
        remaining = attrs[1:]

        # Handle Call expressions (constructor calls)
        if isinstance(ast_node, ast.Call):
            handler = context.resolve(ast_node.func)
            if handler is None or handler.requires_await:
                return None
            handler.compile(ast_node, context)
            # Look for keyword argument with matching name
            for kw in ast_node.keywords:
                if kw.arg == attr_name:
                    return self._resolve_nested_attributes(kw.value, remaining, context)
            return None

        # Handle Dict expressions
        if isinstance(ast_node, ast.Dict):
            for key, value in zip(ast_node.keys, ast_node.values, strict=True):
                if isinstance(key, ast.Constant) and key.value == attr_name:
                    return self._resolve_nested_attributes(value, remaining, context)
            return None

        return None


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
