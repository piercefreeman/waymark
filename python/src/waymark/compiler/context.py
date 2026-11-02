"""Shared compilation operations and per-function facts available to handlers."""

import ast
import inspect
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from waymark.call_binding import bind_call
from waymark.compiler.analysis import SourceAnalysis
from waymark.compiler.ast import make_span
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.literals import literal_expression
from waymark.compiler.recommendations import RECOMMENDATIONS
from waymark.proto import ast_pb2 as ir

if TYPE_CHECKING:
    from waymark.language_calls import LanguageCallHandler


@dataclass
class LoweredCall:
    """An expression and any statements that must execute before evaluating it."""

    value: ir.Expr
    statements: list[ir.Statement] = field(default_factory=list)


@dataclass
class CompilerContext:
    """Compile nested syntax using the current workflow's symbols and temporary names."""

    expression: Callable[[ast.expr], ir.Expr]
    statement: Callable[[ast.stmt], list[ir.Statement]]
    next_name: Callable[[str], str]
    function_signatures: Mapping[str, inspect.Signature]
    resolve: Callable[[ast.expr], "LanguageCallHandler[Any] | None"]
    namespace: Mapping[str, Any]
    validate_syntax: Callable[[ast.AST], None]
    analysis: SourceAnalysis = field(default_factory=SourceAnalysis)
    instance_attrs: Mapping[str, ast.expr] = field(default_factory=dict)
    instance_namespace: Mapping[str, Any] = field(default_factory=dict)
    ambiguous_instance_attrs: set[str] = field(default_factory=set)

    def block(self, nodes: Iterable[ast.stmt], location: ast.AST) -> ir.Block:
        """Compile a statement sequence while retaining the enclosing source span."""
        return ir.Block(
            statements=[statement for node in nodes for statement in self.statement(node)],
            span=make_span(location),
        )

    def lower_value(self, node: ast.expr) -> LoweredCall:
        """Keep setup attached to its value until the owning statement places both."""
        if isinstance(node, ast.Await):
            if not isinstance(node.value, ast.Call):
                raise UnsupportedPatternError(
                    "Await requires a direct action or workflow-helper call",
                    RECOMMENDATIONS["sync_function_call"],
                    line=node.lineno,
                    col=node.col_offset,
                )
            return self.compile_call(node.value, awaited=True)
        if isinstance(node, ast.Call):
            return self.compile_call(node, awaited=False)
        return LoweredCall(self.expression(node))

    def compile_call(self, node: ast.Call, *, awaited: bool) -> LoweredCall:
        """Resolve without emitting, then lower the selected call exactly once."""
        handler = self.resolve(node.func)
        if handler is not None:
            if handler.requires_await != awaited:
                message = (
                    "await this call in its own assignment, return, or expression statement"
                    if handler.requires_await
                    else "this call cannot be awaited"
                )
                raise handler.error(node, message)
            lowered = handler.compile(node, self)
            lowered.value.span.CopyFrom(make_span(node))
            return lowered
        function = node.func
        if (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and function.value.id == "self"
            and function.attr in self.function_signatures
        ):
            signature = self.function_signatures[function.attr]
            bound = bind_call(node, signature, self.namespace)
            call = ir.FunctionCall(name=function.attr)
            for argument in node.args:
                call.args.append(self.expression(argument))
            for keyword in node.keywords:
                assert keyword.arg is not None
                call.kwargs.append(ir.Kwarg(name=keyword.arg, value=self.expression(keyword.value)))
            for parameter in signature.parameters.values():
                if (
                    parameter.name not in bound.arguments
                    and parameter.default is not inspect.Parameter.empty
                ):
                    value = self.expression(literal_expression(parameter.default, node))
                    call.kwargs.append(ir.Kwarg(name=parameter.name, value=value))
            return LoweredCall(ir.Expr(function_call=call, span=make_span(node)))
        message = (
            f"unsupported awaitable '{ast.unparse(function)}'; use an action or workflow helper"
            if awaited
            else f"Calling '{ast.unparse(function)}()' is not supported in workflow code"
        )
        raise UnsupportedPatternError(
            message, RECOMMENDATIONS["sync_function_call"], line=node.lineno, col=node.col_offset
        )

    def call(self, node: ast.Call) -> ir.Call:
        """Compile one direct awaitable for an IR operation that accepts only calls."""
        lowered = self.compile_call(node, awaited=True)
        if not lowered.statements:
            if lowered.value.HasField("action_call"):
                return ir.Call(action=lowered.value.action_call)
            if lowered.value.HasField("function_call"):
                return ir.Call(function=lowered.value.function_call)
        raise UnsupportedPatternError(
            f"unsupported awaitable '{ast.unparse(node.func)}'; use an action or workflow helper",
            RECOMMENDATIONS["sync_function_call"],
            line=node.lineno,
            col=node.col_offset,
        )
