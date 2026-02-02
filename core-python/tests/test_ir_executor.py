import asyncio

from proto import ast_pb2 as ir
from rappel_core.ir_executor import StatementExecutor


def test_statement_executor_runs_basic_program() -> None:
    async def action_handler(_action: ir.ActionCall, _kwargs: dict[str, object]) -> object:
        raise AssertionError("action handler should not be called")

    program = ir.Program(
        functions=[
            ir.FunctionDef(
                name="main",
                io=ir.IoDecl(inputs=["x"], outputs=["result"]),
                body=ir.Block(
                    statements=[
                        ir.Statement(
                            assignment=ir.Assignment(
                                targets=["y"],
                                value=ir.Expr(
                                    binary_op=ir.BinaryOp(
                                        left=ir.Expr(variable=ir.Variable(name="x")),
                                        op=ir.BinaryOperator.BINARY_OP_ADD,
                                        right=ir.Expr(literal=ir.Literal(int_value=1)),
                                    )
                                ),
                            )
                        ),
                        ir.Statement(
                            return_stmt=ir.ReturnStmt(value=ir.Expr(variable=ir.Variable(name="y")))
                        ),
                    ]
                ),
            )
        ]
    )

    executor = StatementExecutor(program, action_handler)
    result = asyncio.run(executor.execute_program(inputs={"x": 2}))

    assert result == 3
