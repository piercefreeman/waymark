"""Inline module-level values the compiler can treat as literals.

A name the function never binds reads its module global (or closure
binding). When that value is one the compiler can write as a literal, the
read is replaced by the literal itself, so `MAX = 5 * 6` and `MAX` used in a
workflow compile exactly as `30` would: in expressions, in policies, and in
every argument a handler requires to be a literal. The value is the one the
global holds when the workflow compiles; how it was computed does not matter.
Only immutable values are inlined: a list, dict or set is rejected, since
each inlined read is its own copy and would not share mutations.
"""

import ast
import copy
from collections.abc import Mapping, Set
from enum import Enum
from typing import Any, overload

from waymark.compiler.diagnostics import reject
from waymark.compiler.literals import (
    has_interior_mutability,
    is_literal_value,
    literal_expression,
)
from waymark.compiler.recommendations import RECOMMENDATIONS


def bound_names(function: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str]:
    """Every name the function binds anywhere, which Python then treats as local.

    Comprehension targets and `global`/`nonlocal` declarations are included
    too: neither reads a value fixed at compile time.
    """
    names: set[str] = set()
    for node in ast.walk(function):
        match node:
            case ast.Name(id=name, ctx=ast.Store() | ast.Del()):
                names.add(name)
            case ast.arg(arg=name):
                names.add(name)
            case ast.ExceptHandler(name=str(name)):
                names.add(name)
            case ast.alias(name=name, asname=asname):
                names.add(asname or name.split(".")[0])
            case ast.Global(names=declared) | ast.Nonlocal(names=declared):
                names.update(declared)
            case (
                ast.FunctionDef(name=name)
                | ast.AsyncFunctionDef(name=name)
                | ast.ClassDef(name=name)
            ) if node is not function:
                names.add(name)
    return names


def static_value(value: Any) -> Any:
    """The value a global compiles as: an enum member compiles to its value, as `Color.RED` does."""
    return value.value if isinstance(value, Enum) else value


FunctionNode = ast.FunctionDef | ast.AsyncFunctionDef


@overload
def inline_static_globals(
    node: FunctionNode, namespace: Mapping[str, Any], bound: Set[str]
) -> FunctionNode: ...


@overload
def inline_static_globals(
    node: ast.expr, namespace: Mapping[str, Any], bound: Set[str]
) -> ast.expr: ...


def inline_static_globals(
    node: FunctionNode | ast.expr, namespace: Mapping[str, Any], bound: Set[str]
) -> FunctionNode | ast.expr:
    """Copy `node`, replacing each read of a static global with its literal.

    Names in `bound` are the function's own and are left alone, as are
    globals with no literal form, such as functions, classes and modules,
    which handlers resolve by identity. A global holding a list, dict or set
    is rejected: each inlined read would be a separate copy, so a mutation
    through one would not show through the next.
    """

    class StaticGlobals(ast.NodeTransformer):
        def visit_Name(self, current: ast.Name) -> ast.AST:
            if (
                not isinstance(current.ctx, ast.Load)
                or current.id in bound
                or current.id not in namespace
            ):
                return current
            value = static_value(namespace[current.id])
            if has_interior_mutability(value):
                reject(
                    current,
                    f"Module-level '{current.id}' holds a mutable {type(value).__name__}",
                    RECOMMENDATIONS["mutable_global"],
                )
            if not is_literal_value(value):
                return current
            return literal_expression(value, current)

    rewritten = StaticGlobals().visit(copy.deepcopy(node))
    # A bare name is itself replaced, by a literal of another expression type.
    assert isinstance(rewritten, ast.expr if isinstance(node, ast.expr) else type(node))
    return rewritten
