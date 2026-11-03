"""AST substitutions used when lowering comprehension-local variables."""

import ast
import copy
from collections.abc import Mapping
from types import ModuleType
from typing import Any

from waymark.proto import ast_pb2 as ir


def rewrite_load_names(node: ast.AST, replacements: Mapping[str, ast.expr]) -> ast.AST:
    """Copy a subtree while substituting reads, preserving source locations and stores."""

    class LoadNameRewriter(ast.NodeTransformer):
        def visit_Name(self, current: ast.Name) -> ast.AST:
            if isinstance(current.ctx, ast.Load) and current.id in replacements:
                return ast.fix_missing_locations(
                    ast.copy_location(copy.deepcopy(replacements[current.id]), current)
                )
            return current

    return ast.fix_missing_locations(LoadNameRewriter().visit(copy.deepcopy(node)))


def resolve_reference(function: ast.expr, namespace: Mapping[str, Any]) -> Any:
    """Read module/class dictionaries without calling user descriptors."""
    if isinstance(function, ast.Name):
        return namespace.get(function.id)
    if isinstance(function, ast.Attribute):
        owner = resolve_reference(function.value, namespace)
        if type(owner) is ModuleType:
            return vars(owner).get(function.attr)
        if isinstance(owner, type):
            # Use type's builtin descriptors so custom metaclasses cannot intercept these reads.
            for base in type.__dict__["__mro__"].__get__(owner):
                attributes = type.__dict__["__dict__"].__get__(base)
                if function.attr in attributes:
                    return attributes[function.attr]
    return None


def make_span(node: ast.AST) -> ir.Span:
    """Preserve source positions, including generated nodes that have no location yet."""
    location = vars(node)
    return ir.Span(
        start_line=location.get("lineno", 0),
        start_col=location.get("col_offset", 0),
        end_line=location.get("end_lineno", 0) or 0,
        end_col=location.get("end_col_offset", 0) or 0,
    )
