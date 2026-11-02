"""Compile supported asyncio.sleep calls without losing their result."""

import ast
import asyncio
import math
from dataclasses import dataclass

from waymark.compiler_context import CompilerContext
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.policy_literals import PolicyCompiler, _number_literal
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class SleepArguments:
    delay: ast.expr
    result: ast.expr | None = None


class SleepHandler(LanguageCallHandler[SleepArguments]):
    target = asyncio.sleep
    name = "asyncio.sleep"
    arguments_type = SleepArguments
    recommendation = "Use a finite literal delay; compute dynamic delays inside an action."

    def lower(
        self, arguments: SleepArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Evaluate the result before a durable wait; nonpositive delays return it immediately."""
        delay = _number_literal(arguments.delay)
        if delay is None:
            raise self.error(arguments.delay, "delay must be a finite number literal")
        try:
            finite = math.isfinite(delay)
        except OverflowError:
            finite = False
        if not finite:
            raise self.error(arguments.delay, "delay must be finite")
        value = (
            context.expression(arguments.result)
            if arguments.result is not None
            else ir.Expr(literal=ir.Literal(is_none=True))
        )
        assert value is not None
        statements = []
        if arguments.result is not None and not isinstance(arguments.result, ast.Constant):
            temporary = context.next_name("sleep_result")
            statements.append(
                ir.Statement(assignment=ir.Assignment(targets=[temporary], value=value))
            )
            value = ir.Expr(variable=ir.Variable(name=temporary))
        # Python treats nonpositive delays as a cooperative yield. There is no VM wait.
        if delay > 0:
            seconds = PolicyCompiler(context)._runnable_seconds(
                delay, "sleep delay", arguments.delay
            )
            statements.append(
                ir.Statement(
                    sleep_stmt=ir.SleepStmt(
                        duration=ir.Expr(
                            literal=ir.Literal(int_value=delay)
                            if type(delay) is int
                            else ir.Literal(float_value=seconds)
                        )
                    )
                )
            )
        return LoweredCall(value, statements)
