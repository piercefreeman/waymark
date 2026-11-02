"""Bind source arguments without executing their expressions."""

import ast
import inspect
from collections.abc import Mapping
from typing import Any

from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.types import has_open_type_parameters


def bind_call(
    node: ast.Call, signature: inspect.Signature, namespace: Mapping[str, Any] | None = None
) -> inspect.BoundArguments:
    """Reject unsupported or ambiguous arguments before any lowering takes place."""

    def fail(message: str) -> UnsupportedPatternError:
        return UnsupportedPatternError(
            f"{ast.unparse(node.func)}(): {message}",
            "Pass each declared argument explicitly, without *args or **kwargs.",
            line=node.lineno,
            col=node.col_offset,
        )

    if any(
        has_open_type_parameters(annotation, namespace)
        for annotation in [
            signature.return_annotation,
            *(parameter.annotation for parameter in signature.parameters.values()),
        ]
    ):
        raise fail("open generic annotations are unsupported; specialize all type parameters")
    if any(isinstance(argument, ast.Starred) for argument in node.args):
        raise fail("positional unpacking (*args) is unsupported")
    keywords: dict[str, ast.expr] = {}
    for keyword in node.keywords:
        if keyword.arg is None:
            raise fail("keyword unpacking (**kwargs) is unsupported")
        if keyword.arg in keywords:
            raise fail(f"duplicate argument {keyword.arg!r}")
        keywords[keyword.arg] = keyword.value
    try:
        bound = signature.bind(*node.args, **keywords)
    except TypeError as error:
        raise fail(str(error)) from error
    for name in bound.arguments:
        if signature.parameters[name].kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        ):
            raise fail(f"parameter {name!r} cannot be represented as a named argument")
    positional_names = list(signature.parameters)[: len(node.args)]
    source_names = [*positional_names, *keywords]
    ordered = {name: bound.arguments[name] for name in source_names}
    bound.arguments.clear()
    bound.arguments.update(ordered)
    return bound
