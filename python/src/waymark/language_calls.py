"""Argument contracts for calls implemented by the workflow compiler.

Handlers declare a dataclass signature: AST node types accept runtime
expressions, and Literal annotations accept only those compile-time values.
Undeclared arguments and keyword unpacking are rejected before lowering.
"""

import ast
import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar, Generic, Literal, TypeVar, get_args, get_origin, get_type_hints

from pydantic import ConfigDict, TypeAdapter, ValidationError

from waymark.compiler.ast import resolve_reference
from waymark.compiler.context import CompilerContext
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.types import ValueKind
from waymark.proto import ast_pb2 as ir

Arguments = TypeVar("Arguments")


@dataclass
class LoweredCall:
    """An expression and any statements that must execute before evaluating it."""

    value: ir.Expr
    statements: list[ir.Statement] = field(default_factory=list)


class LanguageCallRegistry:
    """Resolve source names to registered callables without evaluating user expressions."""

    def __init__(
        self,
        handlers: Iterable["LanguageCallHandler[Any]"],
        factories: Iterable[Callable[[Any], "LanguageCallHandler[Any] | None"]] = (),
    ) -> None:
        self._handlers = list(handlers)
        self._factories = list(factories)

    def resolve(
        self, function: ast.expr, namespace: Mapping[str, Any]
    ) -> "LanguageCallHandler[Any] | None":
        """Match callable identity, so aliases and module re-exports share one handler."""
        value = resolve_reference(function, namespace)
        registered = next(
            (handler for handler in self._handlers if value is type(handler).target), None
        )
        if registered is not None:
            return registered
        for factory in self._factories:
            handler = factory(value)
            if handler is not None:
                return handler
        return None


class LanguageCallHandler(ABC, Generic[Arguments]):
    """Declare the target callable and argument dataclass, then implement lower."""

    target: ClassVar[Callable[..., Any]]
    name: str
    arguments_type: type[Arguments]
    recommendation: str
    variadic_argument: str | None = None
    positional_only: tuple[str, ...] = ()
    requires_await: bool = True

    def __init__(self) -> None:
        signature = inspect.signature(self.arguments_type)
        if self.variadic_argument is not None:
            signature = signature.replace(
                parameters=[
                    parameter.replace(kind=inspect.Parameter.VAR_POSITIONAL)
                    if parameter.name == self.variadic_argument
                    else parameter
                    for parameter in signature.parameters.values()
                ]
            )
        signature = signature.replace(
            parameters=[
                parameter.replace(kind=inspect.Parameter.POSITIONAL_ONLY)
                if parameter.name in self.positional_only
                else parameter
                for parameter in signature.parameters.values()
            ]
        )
        self._signature = signature
        annotations = get_type_hints(self.arguments_type)
        self._annotations = {name: annotations[name] for name in signature.parameters}
        self._adapters = {
            name: TypeAdapter(
                annotation, config=ConfigDict(arbitrary_types_allowed=True, strict=True)
            )
            for name, annotation in self._annotations.items()
        }

    def compile(self, node: ast.Call, context: CompilerContext) -> LoweredCall:
        """Validate a source call before handing its bound arguments to lower."""
        return self.lower(self.parse_arguments(node), node, context)

    def validate_usage(
        self, node: ast.Call, parent: ast.AST | None, kind: Callable[[ast.expr | None], ValueKind]
    ) -> None:
        """Check placement and statically known operands before lowering."""

    @abstractmethod
    def lower(self, arguments: Arguments, node: ast.Call, context: CompilerContext) -> LoweredCall:
        """Lower a call after its declared argument contract has been checked."""

    def parse_arguments(self, node: ast.Call) -> Arguments:
        """Validate without lowering when a caller first needs to emit setup statements."""
        if self.variadic_argument is None:
            for argument in node.args:
                if isinstance(argument, ast.Starred):
                    raise self.error(argument, "positional unpacking (*args) is unsupported")
        keywords: dict[str, ast.expr] = {}
        for keyword in node.keywords:
            if keyword.arg is None:
                raise self.error(keyword.value, "keyword unpacking (**kwargs) is unsupported")
            if keyword.arg in keywords:
                raise self.error(keyword.value, f"duplicate argument {keyword.arg!r}")
            keywords[keyword.arg] = keyword.value

        try:
            bound = self._signature.bind(*node.args, **keywords)
        except TypeError as error:
            raise self.error(node, str(error)) from error
        bound.apply_defaults()

        values: dict[str, Any] = {}
        for name, value in bound.arguments.items():
            annotation = self._annotations[name]
            location = value if isinstance(value, ast.expr) else node
            if get_origin(annotation) is Literal:
                if isinstance(value, ast.Constant):
                    value = value.value
                allowed = get_args(annotation)
                # Literal[False] must not accept 0 merely because False == 0.
                if not any(type(value) is type(option) and value == option for option in allowed):
                    expected = ", ".join(repr(option) for option in allowed)
                    raise self.error(location, f"{name} must be a literal {expected}")
            try:
                values[name] = self._adapters[name].validate_python(value)
            except ValidationError as error:
                detail = error.errors(include_url=False)[0]["msg"]
                raise self.error(location, f"unsupported {name}: {detail}") from error

        return self.arguments_type(**values)

    def error(self, node: ast.expr, message: str) -> UnsupportedPatternError:
        """Attach the call's recommendation and source location to a validation failure."""
        return UnsupportedPatternError(
            f"{self.name}(): {message}",
            self.recommendation,
            line=node.lineno,
            col=node.col_offset,
        )
