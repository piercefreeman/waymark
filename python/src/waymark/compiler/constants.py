"""Inline module-level values the compiler can treat as literals.

A name the function never binds reads its module global (or closure
binding). When that value is one the compiler can write as a literal, the
read is replaced by the literal itself, so `MAX = 5 * 6` and `MAX` used in a
workflow compile exactly as `30` would: in expressions, in policies, and in
every argument a handler requires to be a literal. The value is the one the
global holds when the workflow compiles; how it was computed does not matter.
"""

import ast
import copy
from collections.abc import Mapping, Set
from enum import Enum
from typing import Any, TypeVar

from waymark.compiler.literals import is_literal_value, literal_expression

Node = TypeVar("Node", bound=ast.AST)


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
    """The literal a global's value compiles to, or `NotImplemented` when it has none.

    An enum member compiles to its value, as `Color.RED` does in workflow code.
    """
    if isinstance(value, Enum):
        value = value.value
    return value if is_literal_value(value) else NotImplemented


def inline_static_globals(node: Node, namespace: Mapping[str, Any], bound: Set[str]) -> Node:
    """Copy `node`, replacing each read of a static global with its literal.

    Names in `bound` are the function's own and are left alone, as are
    globals with no literal form, such as functions, classes and modules,
    which handlers resolve by identity.
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
            if value is NotImplemented:
                return current
            return literal_expression(value, current)

    rewritten = StaticGlobals().visit(copy.deepcopy(node))
    assert isinstance(rewritten, type(node))
    return rewritten
