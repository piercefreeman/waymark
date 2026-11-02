"""Annotation shapes that cannot survive the workflow value boundary."""

import ast
import dataclasses
from collections.abc import Mapping
from typing import Any, ForwardRef, Literal, TypeVar, get_args, get_origin

from pydantic import BaseModel

from waymark.compiler_ast import resolve_reference

ValueKind = Literal[
    "unknown", "mixed", "list", "tuple", "dict", "int", "float", "bool", "str", "exception", "none"
]
VALUE_KINDS: dict[str, ValueKind] = {
    "list": "list",
    "tuple": "tuple",
    "dict": "dict",
    "int": "int",
    "float": "float",
    "bool": "bool",
    "str": "str",
    "NoneType": "none",
    "List": "list",
    "Tuple": "tuple",
    "Dict": "dict",
}


def has_open_type_parameters(
    annotation: Any, namespace: Mapping[str, Any] | None = None, seen: frozenset[int] = frozenset()
) -> bool:
    """Find unresolved type parameters, including nested model fields, without evaluating annotations."""
    if id(annotation) in seen:
        return False
    seen = seen | {id(annotation)}
    if isinstance(annotation, TypeVar):
        return True
    if get_origin(annotation) is Literal:
        return False
    if isinstance(annotation, ForwardRef):
        return has_open_type_parameters(annotation.__forward_arg__, namespace, seen)
    if isinstance(annotation, str) and namespace is not None:

        def visit(node: ast.AST) -> bool:
            if (
                isinstance(node, ast.Subscript)
                and resolve_reference(node.value, namespace) is Literal
            ):
                return False
            if isinstance(node, (ast.Name, ast.Attribute)):
                return has_open_type_parameters(resolve_reference(node, namespace), namespace, seen)
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                return has_open_type_parameters(node.value, namespace, seen)
            return any(visit(child) for child in ast.iter_child_nodes(node))

        return visit(ast.parse(annotation, mode="eval").body)
    if isinstance(annotation, type):
        if annotation.__dict__.get("__parameters__"):
            return True
        metadata = annotation.__dict__.get("__pydantic_generic_metadata__", {})
        if metadata.get("parameters"):
            return True
        if issubclass(annotation, BaseModel):
            return any(
                has_open_type_parameters(field.annotation, namespace, seen)
                for field in annotation.model_fields.values()
            )
        if dataclasses.is_dataclass(annotation):
            return any(
                has_open_type_parameters(field.type, namespace, seen)
                for field in dataclasses.fields(annotation)
            )
    return any(
        has_open_type_parameters(argument, namespace, seen) for argument in get_args(annotation)
    )
