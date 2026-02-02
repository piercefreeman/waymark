"""CLI smoke check for core-python components."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from proto import ast_pb2 as ir

from .dag import convert_to_dag
from .dag_viz import render_dag_image
from .ir_executor import ExecutionError, StatementExecutor
from .ir_format import format_program
from .worker_pool import WorkerBridgeServer


def _literal_int(value: int) -> ir.Expr:
    return ir.Expr(literal=ir.Literal(int_value=value))


def _variable(name: str) -> ir.Expr:
    return ir.Expr(variable=ir.Variable(name=name))


def _binary(left: ir.Expr, op: ir.BinaryOperator, right: ir.Expr) -> ir.Expr:
    return ir.Expr(binary_op=ir.BinaryOp(left=left, op=op, right=right))


def _build_program() -> ir.Program:
    values_expr = ir.Expr(
        list=ir.ListExpr(
            elements=[
                _literal_int(1),
                _literal_int(2),
                _literal_int(3),
            ]
        )
    )
    doubles_expr = ir.Expr(
        spread_expr=ir.SpreadExpr(
            collection=_variable("values"),
            loop_var="item",
            action=ir.ActionCall(
                action_name="double",
                kwargs=[ir.Kwarg(name="value", value=_variable("item"))],
            ),
        )
    )
    parallel_expr = ir.Expr(
        parallel_expr=ir.ParallelExpr(
            calls=[
                ir.Call(
                    action=ir.ActionCall(
                        action_name="double",
                        kwargs=[ir.Kwarg(name="value", value=_variable("base"))],
                    )
                ),
                ir.Call(
                    action=ir.ActionCall(
                        action_name="double",
                        kwargs=[
                            ir.Kwarg(
                                name="value",
                                value=_binary(
                                    _variable("base"),
                                    ir.BinaryOperator.BINARY_OP_ADD,
                                    _literal_int(1),
                                ),
                            )
                        ],
                    )
                ),
            ]
        )
    )

    statements = [
        ir.Statement(assignment=ir.Assignment(targets=["values"], value=values_expr)),
        ir.Statement(assignment=ir.Assignment(targets=["doubles"], value=doubles_expr)),
        ir.Statement(assignment=ir.Assignment(targets=["a", "b"], value=parallel_expr)),
        ir.Statement(
            assignment=ir.Assignment(
                targets=["pair_sum"],
                value=_binary(
                    _variable("a"),
                    ir.BinaryOperator.BINARY_OP_ADD,
                    _variable("b"),
                ),
            )
        ),
        ir.Statement(
            assignment=ir.Assignment(
                targets=["total"],
                value=ir.Expr(
                    action_call=ir.ActionCall(
                        action_name="sum",
                        kwargs=[ir.Kwarg(name="values", value=_variable("doubles"))],
                    )
                ),
            )
        ),
        ir.Statement(
            assignment=ir.Assignment(
                targets=["final"],
                value=_binary(
                    _variable("pair_sum"),
                    ir.BinaryOperator.BINARY_OP_ADD,
                    _variable("total"),
                ),
            )
        ),
        ir.Statement(return_stmt=ir.ReturnStmt(value=_variable("final"))),
    ]

    main_block = ir.Block(statements=statements)
    main_fn = ir.FunctionDef(
        name="main",
        io=ir.IoDecl(inputs=["base"], outputs=["final"]),
        body=main_block,
    )
    return ir.Program(functions=[main_fn])


async def _action_handler(action: ir.ActionCall, kwargs: dict[str, Any]) -> Any:
    match action.action_name:
        case "double":
            return kwargs["value"] * 2
        case "sum":
            return sum(kwargs["values"])
        case _:
            raise ExecutionError(f"unknown action: {action.action_name}")


async def _run_smoke(base: int) -> int:
    program = _build_program()
    inputs = {"base": base}
    print("IR program")
    print(format_program(program))
    print("IR inputs: %s" % inputs)
    dag = convert_to_dag(program)
    output_path = render_dag_image(dag, Path.cwd() / "dag_smoke.png")
    print("DAG image written to %s" % output_path)

    executor = StatementExecutor(program, _action_handler)
    result = await executor.execute_program(inputs=inputs)
    print("Execution result: %s" % result)

    bridge = await WorkerBridgeServer.start(None)
    try:
        print("Worker bridge listening on %s" % bridge.addr)
    finally:
        await bridge.shutdown()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke check core-python components.")
    parser.add_argument("--base", type=int, default=5, help="Base input for the demo program.")
    args = parser.parse_args()

    raise SystemExit(asyncio.run(_run_smoke(args.base)))
