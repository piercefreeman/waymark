"""Reject runtime type checks until the VM implements their semantics."""

import ast
from dataclasses import dataclass

from waymark.compiler_context import CompilerContext
from waymark.language_calls import LanguageCallHandler, LoweredCall


@dataclass(frozen=True)
class IsinstanceArguments:
    obj: ast.expr
    class_or_tuple: ast.expr


class IsinstanceHandler(LanguageCallHandler[IsinstanceArguments]):
    target = isinstance
    name = "isinstance"
    arguments_type = IsinstanceArguments
    positional_only = ("obj", "class_or_tuple")
    requires_await = False
    recommendation = "Use an except clause for exceptions, or check types inside an action."

    def lower(
        self, arguments: IsinstanceArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Reject type checks until the VM can represent Python runtime class identity."""
        raise self.error(node, "runtime type checks are not supported")
