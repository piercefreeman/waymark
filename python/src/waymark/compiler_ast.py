"""AST rewrites shared by compiler lowering passes."""

import ast
import copy
from collections.abc import Mapping


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
