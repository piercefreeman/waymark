from proto import ast_pb2 as ir
from rappel_core.dag import DAG, ActionCallNode, AssignmentNode, DAGEdge
from rappel_core.runner.executor import RunnerExecutor
from rappel_core.runner.state import RunnerState


def test_executor_unblocks_downstream_action() -> None:
    dag = DAG()

    action_start = ActionCallNode(
        id="action_start",
        action_name="fetch",
        kwargs={},
        kwarg_exprs={},
        targets=["x"],
    )
    assign_node = AssignmentNode(
        id="assign",
        targets=["y"],
        assign_expr=ir.Expr(
            binary_op=ir.BinaryOp(
                left=ir.Expr(variable=ir.Variable(name="x")),
                op=ir.BinaryOperator.BINARY_OP_ADD,
                right=ir.Expr(literal=ir.Literal(int_value=1)),
            )
        ),
    )
    action_next = ActionCallNode(
        id="action_next",
        action_name="work",
        kwargs={},
        kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="y"))},
        targets=["z"],
    )

    dag.add_node(action_start)
    dag.add_node(assign_node)
    dag.add_node(action_next)
    dag.add_edge(DAGEdge.state_machine(action_start.id, assign_node.id))
    dag.add_edge(DAGEdge.state_machine(assign_node.id, action_next.id))

    state = RunnerState(dag=dag, link_queued_nodes=False)
    start_exec = state.queue_template_node(action_start.id)

    executor = RunnerExecutor(dag, state=state, action_results={start_exec.node_id: 10})
    step = executor.increment(start_exec.node_id)

    assert len(step.actions) == 1
    assert step.actions[0].template_id == action_next.id
