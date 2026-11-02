"""The workflow action-policy call contract."""

import ast
from dataclasses import dataclass, field

from waymark.compiler.context import CompilerContext
from waymark.language_calls import LanguageCallHandler, LoweredCall
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
        awaited = ast.copy_location(ast.Await(value=arguments.awaitable), node)
        action = context.action(awaited)
        if action is None:
            raise self.error(node, "requires an action call")
        PolicyCompiler(context).apply(arguments.retry, arguments.timeout, action)
        return LoweredCall(ir.Expr(action_call=action))
