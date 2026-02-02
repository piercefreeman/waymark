from proto import ast_pb2 as ir
from rappel_core.dag import DAGConverter


def test_dag_converter_builds_basic_action_dag() -> None:
    program = ir.Program(
        functions=[
            ir.FunctionDef(
                name="main",
                io=ir.IoDecl(inputs=["value"], outputs=["result"]),
                body=ir.Block(
                    statements=[
                        ir.Statement(
                            assignment=ir.Assignment(
                                targets=["result"],
                                value=ir.Expr(
                                    action_call=ir.ActionCall(
                                        action_name="noop",
                                        module_name="tests.actions",
                                    )
                                ),
                            )
                        )
                    ]
                ),
            )
        ]
    )

    dag = DAGConverter().convert(program)

    assert dag.entry_node is not None
    assert dag.entry_node.startswith("main_input")

    action_nodes = [node for node in dag.nodes.values() if node.node_type == "action_call"]
    assert len(action_nodes) == 1
    action_node = action_nodes[0]
    assert action_node.action_name == "noop"
    assert action_node.targets == ["result"]
