"""Shared compiler operations available to language-call handlers."""

import ast
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from waymark.language_calls import LanguageCallHandler

from waymark.compiler.analysis import SourceAnalysis
from waymark.compiler.ast import make_span
from waymark.proto import ast_pb2 as ir


@dataclass
class CompilerContext:
    """Compile nested syntax using the current workflow's symbols and temporary names."""

    expression: Callable[[ast.expr], ir.Expr | None]
    statement: Callable[[ast.stmt], list[ir.Statement]]
    action: Callable[[ast.expr], ir.ActionCall | None]
    function: Callable[[ast.Call], ir.FunctionCall | None]
    next_name: Callable[[str], str]
    function_names: Collection[str]
    resolve: Callable[[ast.expr], "LanguageCallHandler[Any] | None"]
    namespace: Mapping[str, Any]
    synchronous_call: Callable[[ast.Call], ir.Expr | None]
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

    def call(self, node: ast.Call) -> ir.Call | None:
        """Lower a registered action or workflow helper, including run_action policies."""
        handler = self.resolve(node.func)
        if handler is not None:
            if not handler.requires_await:
                raise handler.error(node, "expected an awaitable")
            lowered = handler.compile(node, self)
            if not lowered.statements:
                if lowered.value.HasField("action_call"):
                    return ir.Call(action=lowered.value.action_call)
                if lowered.value.HasField("function_call"):
                    return ir.Call(function=lowered.value.function_call)
            return None

        awaited = ast.copy_location(ast.Await(value=node), node)
        action = self.action(awaited)
        if action is not None:
            return ir.Call(action=action)

        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
            and node.func.attr in self.function_names
        ):
            function = self.function(node)
            if function is not None:
                return ir.Call(function=function)
        return None
