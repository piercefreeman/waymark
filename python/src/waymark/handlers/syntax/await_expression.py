"""Await placement and expression lowering share one syntax handler."""

import ast

from waymark.compiler.context import CompilerContext
from waymark.compiler.diagnostics import reject
from waymark.language_syntax import ExpressionHandler
from waymark.proto import ast_pb2 as ir


class AwaitHandler(ExpressionHandler[ast.Await]):
    node_types = (ast.Await,)

    def validate(self, node: ast.Await, context: CompilerContext) -> None:
        """Check this construct against the shared source facts."""
        if isinstance(node.value, ast.Call):
            handler = context.resolve(node.value.func)
            if handler is not None:
                handler.parse_arguments(node.value)
                if not handler.requires_await:
                    raise handler.error(node.value, "this call cannot be awaited")
        parent = context.analysis.parents.get(node)
        if not isinstance(parent, (ast.Assign, ast.AnnAssign, ast.Expr, ast.Return, ast.If)):
            reject(
                node, "Nested await expressions are unsupported; assign the awaited result first"
            )
        if not isinstance(node.value, ast.Call):
            return reject(node, "Await requires a direct action or workflow-helper call")
        call = node.value
        function = call.func
        name = (
            function.id
            if isinstance(function, ast.Name)
            else function.attr
            if isinstance(function, ast.Attribute)
            else "unknown"
        )
        helper = (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and function.value.id == "self"
            and name in context.function_names
        )
        if context.resolve(function) is None and not helper:
            reject(node, f"Awaiting non-action function '{name}()' is not supported")

    def lower(self, node: ast.Await, context: CompilerContext) -> ir.Expr:
        """Lower a directly awaited helper; statement-level durable calls use their call handlers."""
        if not isinstance(node.value, ast.Call):
            return reject(node, "Await requires a direct action or workflow-helper call")
        value = context.synchronous_call(node.value)
        if value is None:
            return reject(node, "Await requires a direct action or workflow-helper call")
        return value
