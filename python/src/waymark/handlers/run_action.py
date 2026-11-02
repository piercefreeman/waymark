"""The workflow action-policy call contract."""

import ast
from dataclasses import dataclass, field

from waymark.compiler.context import CompilerContext, LoweredCall
from waymark.language_calls import LanguageCallHandler
from waymark.policy_literals import PolicyCompiler
from waymark.proto import ast_pb2 as ir
from waymark.workflow import Workflow


@dataclass(frozen=True)
class RunActionArguments:
    awaitable: ast.Call
    retry: ast.expr | None = field(default=None, kw_only=True)
    timeout: ast.expr | None = field(default=None, kw_only=True)


class RunActionHandler(LanguageCallHandler[RunActionArguments]):
    target = Workflow.run_action
    name = "run_action"
    arguments_type = RunActionArguments
    recommendation = "Use self.run_action(action(...), retry=..., timeout=...)."

    def lower(
        self, arguments: RunActionArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Bind the wrapped action and attach the validated retry and timeout policies."""
        lowered = context.compile_call(arguments.awaitable, awaited=True)
        if lowered.statements or not lowered.value.HasField("action_call"):
            raise self.error(node, "requires an action call")
        action = lowered.value.action_call
        PolicyCompiler(context).apply(arguments.retry, arguments.timeout, action)
        return LoweredCall(ir.Expr(action_call=action))
