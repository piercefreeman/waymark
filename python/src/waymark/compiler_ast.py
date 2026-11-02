"""AST substitutions used when lowering comprehension-local variables."""

import ast
import copy
from collections.abc import Mapping
from types import ModuleType
from typing import Any


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
        if type(owner) is type:
            return next(
                (
                    vars(base)[function.attr]
                    for base in owner.__mro__
                    if function.attr in vars(base)
                ),
                None,
            )
    return None
