"""Small source compiler shared by language-boundary regression tests."""

import ast
import asyncio
import inspect
import textwrap
from datetime import timedelta
from typing import Any

from waymark import action
from waymark.ir_builder import ActionDefinition, IRBuilder, TransformContext
from waymark.proto import ast_pb2 as ir
from waymark.workflow import RetryPolicy, Workflow


@action
async def task(value: int = 1) -> int:
    return value


def compile_source(
    body: str, parameters: str = "", namespace: dict[str, Any] | None = None
) -> ir.FunctionDef:
    builder = IRBuilder(
        {"task": ActionDefinition("task", __name__, inspect.signature(task))},
        TransformContext(),
        module_globals={
            "asyncio": asyncio,
            "task": task,
            "RetryPolicy": RetryPolicy,
            "timedelta": timedelta,
            **(namespace or {}),
        },
        receiver_type=Workflow,
    )
    source = (
        "async def run(self"
        + (", " + parameters if parameters else "")
        + "):\n"
        + textwrap.indent(textwrap.dedent(body), "    ")
    )
    builder.visit(ast.parse(source))
    assert builder.function_def is not None
    return builder.function_def
