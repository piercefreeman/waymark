"""Annotation shapes that cannot survive the workflow value boundary."""

import ast
import dataclasses
import operator
from collections.abc import Mapping
from functools import reduce
from types import UnionType
from typing import Any, ForwardRef, Generic, Literal, TypeVar, Union, get_args, get_origin

from pydantic import BaseModel

from waymark.compiler.ast import resolve_reference

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


def type_parameter_bindings(cls: type) -> dict[TypeVar, Any]:
    """What each type parameter of `cls`'s generic bases is specialized to.

    `class Base(Generic[T])` and `class Concrete(Base[int])` bind `T` to `int`;
    a chain such as `class Middle(Base[list[U]], Generic[U])` and
    `class Concrete(Middle[int])` binds `U` to `int` and `T` to `list[int]`.
    A parameter no subclass specializes stays unbound, and so stays open.
    """
    bindings: dict[TypeVar, Any] = {}

    def visit(current: type, scope: Mapping[TypeVar, Any]) -> None:
        for base in current.__dict__.get("__orig_bases__", current.__bases__):
            origin = get_origin(base)
            if origin is None:
                if isinstance(base, type):
                    visit(base, {})
                continue
            if not isinstance(origin, type) or origin is Generic:
                continue
            parameters = origin.__dict__.get("__parameters__", ())
            arguments = get_args(base)
            if len(parameters) != len(arguments):
                continue
            resolved = {
                parameter: specialize(argument, scope)
                for parameter, argument in zip(parameters, arguments, strict=True)
                if isinstance(parameter, TypeVar)
            }
            for parameter, argument in resolved.items():
                bindings.setdefault(parameter, argument)
            visit(origin, resolved)

    visit(cls, {})
    return bindings


def specialize(annotation: Any, bindings: Mapping[TypeVar, Any]) -> Any:
    """`annotation` with each bound type parameter replaced, however deeply it is nested."""
    if not bindings:
        return annotation
    if isinstance(annotation, TypeVar):
        return bindings.get(annotation, annotation)
    parameters = _type_parameters(annotation)
    if not any(parameter in bindings for parameter in parameters):
        return annotation
    if get_origin(annotation) in (Union, UnionType):
        # `list[T] | None` is not subscriptable on every supported Python; rebuild it.
        return reduce(
            operator.or_, (specialize(member, bindings) for member in get_args(annotation))
        )
    return annotation[tuple(bindings.get(parameter, parameter) for parameter in parameters)]


def _type_parameters(annotation: Any) -> tuple[Any, ...]:
    """The open parameters of a subscriptable alias such as `list[T]`, `T | None` or `Model[T]`."""
    if isinstance(annotation, type):
        metadata = annotation.__dict__.get("__pydantic_generic_metadata__", {})
        return tuple(metadata.get("parameters", ()))
    if get_origin(annotation) is None:
        return ()
    try:
        return tuple(annotation.__parameters__)
    except AttributeError:
        return ()
