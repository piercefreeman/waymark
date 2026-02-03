import asyncio

from proto import ast_pb2 as ir

from rappel_core.dag import DAG, ActionCallNode, DAGEdge, InputNode, OutputNode
from rappel_core.runloop import RunLoop
from rappel_core.runner.executor import RunnerExecutor
from rappel_core.runner.state import RunnerState
from rappel_core.workers import InlineWorkerPool


def test_runloop_executes_actions() -> None:
    dag = DAG()
    input_node = InputNode(id="input", io_vars=["x"])
    action_node = ActionCallNode(
        id="action",
        action_name="double",
        kwargs={"value": "x"},
        kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="x"))},
        targets=["y"],
    )
    output_node = OutputNode(id="output", io_vars=["y"])

    dag.add_node(input_node)
    dag.add_node(action_node)
    dag.add_node(output_node)
    dag.add_edge(DAGEdge.state_machine(input_node.id, action_node.id))
    dag.add_edge(DAGEdge.state_machine(action_node.id, output_node.id))

    state = RunnerState(dag=dag, link_queued_nodes=False)
    state.record_assignment(
        targets=["x"],
        expr=ir.Expr(literal=ir.Literal(int_value=4)),
        label="input x = 4",
    )
    entry_exec = state.queue_template_node(input_node.id)
    executor = RunnerExecutor(dag, state=state, action_results={})

    async def double(value: int) -> int:
        return value * 2

    worker_pool = InlineWorkerPool({"double": double})
    runloop = RunLoop(worker_pool)
    runloop.register_executor(executor, entry_exec.node_id)
    asyncio.run(runloop.run())

    results = list(executor.action_results.values())
    assert results == [8]
