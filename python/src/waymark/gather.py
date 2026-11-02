"""The argument contract and lowering for workflow asyncio.gather calls."""

import ast
import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class GatherArguments:
    """Supported gather operands and the only implemented exception mode."""

    awaitables: tuple[ast.Call | ast.Starred, ...]
    return_exceptions: Literal[False] = field(default=False, kw_only=True)

    @property
    def comprehension(self) -> ast.ListComp | ast.GeneratorExp | None:
        """Expose a sole comprehension spread for statement-level setup and lowering."""
        if len(self.awaitables) != 1:
            return None
        argument: ast.expr = self.awaitables[0]
        if isinstance(argument, ast.Starred):
            argument = argument.value
        if isinstance(argument, (ast.ListComp, ast.GeneratorExp)):
            return argument
        return None


class GatherHandler(LanguageCallHandler[GatherArguments]):
    """Validate gather's contract and delegate expression lowering to the IR builder."""

    target = asyncio.gather
    name = "asyncio.gather"
    arguments_type = GatherArguments
    variadic_argument = "awaitables"
    recommendation = (
        "Pass action or workflow-helper calls directly, or use a single list comprehension "
        "spread: asyncio.gather(*[action(item) for item in items]). "
        "Omit return_exceptions or pass False; collecting exceptions with "
        "return_exceptions=True is not supported by the runtime."
    )

    def __init__(
        self,
        convert_call: Callable[[ast.expr], ir.Call],
        convert_comprehension: Callable[[ast.ListComp | ast.GeneratorExp], LoweredCall],
    ) -> None:
        super().__init__()
        self._convert_call = convert_call
        self._convert_comprehension = convert_comprehension

    def parse_arguments(self, node: ast.Call) -> GatherArguments:
        """Reject unsupported operand shapes before either gather lowering path runs."""
        arguments = super().parse_arguments(node)
        if not arguments.awaitables:
            raise self.error(node, "empty gathers are not supported")
        comprehension = arguments.comprehension
        if comprehension is not None:
            if len(comprehension.generators) != 1 or comprehension.generators[0].is_async:
                raise self.error(comprehension, "use a single synchronous comprehension loop")
            if not isinstance(comprehension.elt, ast.Call):
                raise self.error(comprehension.elt, "the comprehension element must be a call")
        else:
            for argument in arguments.awaitables:
                if not isinstance(argument, ast.Call):
                    raise self.error(
                        argument,
                        f"unsupported awaitable {ast.unparse(argument)!r}; "
                        "a comprehension spread must be the only positional argument",
                    )
        return arguments

    def lower(self, arguments: GatherArguments, node: ast.Call) -> LoweredCall:
        """Lower already-validated calls into static parallel calls or a comprehension spread."""
        comprehension = arguments.comprehension
        if comprehension is not None:
            return self._convert_comprehension(comprehension)
        return LoweredCall(
            value=ir.Expr(
                parallel_expr=ir.ParallelExpr(
                    calls=[self._convert_call(call) for call in arguments.awaitables]
                )
            )
        )
