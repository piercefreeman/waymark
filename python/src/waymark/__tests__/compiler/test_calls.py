"""Call recognition is pure and lowering happens once in each supported context."""

import ast
from dataclasses import dataclass

import pytest

from waymark.compiler.context import CompilerContext, LoweredCall
from waymark.ir_builder import IRBuilder, TransformContext
from waymark.language_calls import LanguageCallHandler, LanguageCallRegistry
from waymark.proto import ast_pb2 as ir


def probe(value: bool) -> bool:
    """Supply a unique callable identity for the test registry."""
    return value


@dataclass
class ProbeArguments:
    """Accept one expression so the probe exercises normal argument binding."""

    value: ast.expr


class ProbeHandler(LanguageCallHandler[ProbeArguments]):
    """Count lowering invocations and emit observable setup before its value."""

    target = probe
    name = "probe"
    arguments_type = ProbeArguments
    recommendation = "Await probe(value)."

    def __init__(self) -> None:
        super().__init__()
        self.lowerings = 0

    def lower(
        self, arguments: ProbeArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """A discarded compilation would consume a name and lose this assignment."""
        self.lowerings += 1
        temporary = context.next_name("probe")
        return LoweredCall(
            ir.Expr(variable=ir.Variable(name=temporary)),
            [
                ir.Statement(
                    assignment=ir.Assignment(
                        targets=[temporary], value=context.expression(arguments.value)
                    )
                )
            ],
        )


@pytest.mark.parametrize(
    "body",
    [
        "value = await probe(True)",
        "value: bool = await probe(True)",
        "return await probe(True)",
        "await probe(True)",
        "if await probe(True):\n        return 1",
        "if False:\n        return 1\n    elif await probe(True):\n        return 2",
    ],
)
def test_each_call_lowers_once_with_its_setup(body: str) -> None:
    """Regression: probing the result kind must not compile and discard a handler's setup."""
    handler = ProbeHandler()
    builder = IRBuilder(TransformContext(), module_globals={"probe": probe})
    builder._language_calls = LanguageCallRegistry([handler])
    context = builder._compiler_context
    function = ast.Name(id="probe")
    assert context.resolve(function) is handler
    assert context.resolve(function) is handler
    assert handler.lowerings == 0
    builder.visit(ast.parse("async def run():\n    " + body))
    assert handler.lowerings == 1
    assert builder.function_def is not None
