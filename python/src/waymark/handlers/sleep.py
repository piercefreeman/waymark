"""Compile supported asyncio.sleep calls without losing their result."""

import ast
import asyncio
import math
from dataclasses import dataclass

from waymark.compiler.context import CompilerContext
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.policy_literals import PolicyCompiler
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class SleepArguments:
    delay: ast.expr
    result: ast.expr | None = None


class SleepHandler(LanguageCallHandler[SleepArguments]):
    target = asyncio.sleep
    name = "asyncio.sleep"
    arguments_type = SleepArguments
    recommendation = "Use a finite numeric delay within the runtime duration range."

    def lower(
        self, arguments: SleepArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Validate known delays and evaluate dynamic arguments in order before waiting."""
        delay: int | float | None = None
        try:
            literal = ast.literal_eval(arguments.delay)
        except (ValueError, TypeError):
            pass
        else:
            if type(literal) not in (int, float):
                raise self.error(arguments.delay, "delay must be a number")
            delay = literal

        duration: ir.Expr | None = None
        if delay is not None:
            try:
                finite = math.isfinite(delay)
            except OverflowError:
                finite = False
            if not finite:
                raise self.error(arguments.delay, "delay must be finite")
            # Known nonpositive delays need no VM wait, matching Python's result.
            if delay > 0:
                seconds = PolicyCompiler(context)._runnable_seconds(
                    delay, "sleep delay", arguments.delay
                )
                duration = ir.Expr(
                    literal=ir.Literal(int_value=delay)
                    if type(delay) is int
                    else ir.Literal(float_value=seconds)
                )

        value = ir.Expr(literal=ir.Literal(is_none=True))
        statements = []
        # Keyword order can differ from the signature; preserve source evaluation order.
        for argument in [*node.args, *(keyword.value for keyword in node.keywords)]:
            is_delay = argument is arguments.delay
            if is_delay and delay is not None:
                continue
            expression = context.expression(argument)
            assert expression is not None
            if not isinstance(argument, ast.Constant):
                temporary = context.next_name("sleep_delay" if is_delay else "sleep_result")
                statements.append(
                    ir.Statement(assignment=ir.Assignment(targets=[temporary], value=expression))
                )
                expression = ir.Expr(variable=ir.Variable(name=temporary))
            if is_delay:
                duration = expression
            else:
                value = expression

        if duration is not None:
            statements.append(ir.Statement(sleep_stmt=ir.SleepStmt(duration=duration)))
        return LoweredCall(value, statements)
