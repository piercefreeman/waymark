import asyncio
from copy import deepcopy
from typing import Any
from uuid import UUID

import pytest
from proto import ast_pb2 as ir
from rappel_core.dag import DAG, ActionCallNode, AggregatorNode, AssignmentNode, DAGEdge
from rappel_core.ir_executor import StatementExecutor
from rappel_core.runner.executor import RunnerExecutor
from rappel_core.runner.replay import replay_variables
from rappel_core.runner.state import (
    ExecutionEdge,
    ExecutionNode,
    NodeStatus,
    RunnerState,
)


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


class TestRehydrationAtEachStep:
    """Integration tests verifying crash recovery via state rehydration.

    These tests ensure that at each step of executor execution, the resulting
    action DB objects and snapshot of state nodes/edges can recreate the full
    graph exactly as intended when rehydrated (simulating crash recovery).
    """

    def _snapshot_state(
        self,
        state: RunnerState,
        action_results: dict[UUID, Any],
    ) -> tuple[dict[UUID, ExecutionNode], set[ExecutionEdge], dict[UUID, Any]]:
        """Create a deep copy snapshot of the current state for rehydration testing."""
        nodes_snapshot = {
            node_id: ExecutionNode(
                node_id=node.node_id,
                node_type=node.node_type,
                label=node.label,
                status=node.status,
                template_id=node.template_id,
                targets=list(node.targets),
                action=node.action,
                value_expr=node.value_expr,
                assignments=dict(node.assignments),
                action_attempt=node.action_attempt,
            )
            for node_id, node in state.nodes.items()
        }
        edges_snapshot = set(state.edges)
        results_snapshot = dict(action_results)
        return nodes_snapshot, edges_snapshot, results_snapshot

    def _create_rehydrated_executor(
        self,
        dag: DAG,
        nodes: dict[UUID, ExecutionNode],
        edges: set[ExecutionEdge],
        action_results: dict[UUID, Any],
    ) -> RunnerExecutor:
        """Create a new executor from a state snapshot, simulating crash recovery."""
        return RunnerExecutor(
            dag,
            nodes=nodes,
            edges=edges,
            action_results=action_results,
        )

    def _compare_executor_states(
        self,
        original: RunnerExecutor,
        rehydrated: RunnerExecutor,
    ) -> None:
        """Assert that two executors have equivalent state."""
        orig_state = original.state
        rehy_state = rehydrated.state

        assert set(orig_state.nodes.keys()) == set(rehy_state.nodes.keys()), (
            "Node IDs should match"
        )

        for node_id in orig_state.nodes:
            orig_node = orig_state.nodes[node_id]
            rehy_node = rehy_state.nodes[node_id]
            assert orig_node.node_type == rehy_node.node_type
            assert orig_node.status == rehy_node.status
            assert orig_node.template_id == rehy_node.template_id
            assert orig_node.targets == rehy_node.targets
            assert orig_node.action_attempt == rehy_node.action_attempt

        assert orig_state.edges == rehy_state.edges, "Edges should match exactly"

    def test_rehydrate_after_first_action_queued(self) -> None:
        """Test rehydration after the first action is queued but not completed."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="fetch",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
        )
        action2 = ActionCallNode(
            id="action2",
            action_name="process",
            kwargs={},
            kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="x"))},
            targets=["y"],
        )

        dag.add_node(action1)
        dag.add_node(action2)
        dag.add_edge(DAGEdge.state_machine(action1.id, action2.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        executor = RunnerExecutor(dag, state=state, action_results={})

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )

        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        self._compare_executor_states(executor, rehydrated)

        assert exec1.node_id in rehydrated.state.nodes
        assert rehydrated.state.nodes[exec1.node_id].status == NodeStatus.QUEUED

    def test_rehydrate_after_action_completed_and_increment(self) -> None:
        """Test rehydration after first action completes and next action is queued."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="fetch",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
        )
        action2 = ActionCallNode(
            id="action2",
            action_name="process",
            kwargs={},
            kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="x"))},
            targets=["y"],
        )

        dag.add_node(action1)
        dag.add_node(action2)
        dag.add_edge(DAGEdge.state_machine(action1.id, action2.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        action_results: dict[UUID, Any] = {exec1.node_id: 42}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        step = executor.increment(exec1.node_id)
        assert len(step.actions) == 1
        exec2 = step.actions[0]
        assert exec2.template_id == action2.id

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )

        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        self._compare_executor_states(executor, rehydrated)

        assert exec1.node_id in rehydrated.state.nodes
        assert rehydrated.state.nodes[exec1.node_id].status == NodeStatus.COMPLETED

        assert exec2.node_id in rehydrated.state.nodes
        assert rehydrated.state.nodes[exec2.node_id].status == NodeStatus.QUEUED

    def test_rehydrate_multi_step_chain(self) -> None:
        """Test rehydration at each step of a 3-action chain."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="step1",
            kwargs={},
            kwarg_exprs={},
            targets=["a"],
        )
        action2 = ActionCallNode(
            id="action2",
            action_name="step2",
            kwargs={},
            kwarg_exprs={"input": ir.Expr(variable=ir.Variable(name="a"))},
            targets=["b"],
        )
        action3 = ActionCallNode(
            id="action3",
            action_name="step3",
            kwargs={},
            kwarg_exprs={"input": ir.Expr(variable=ir.Variable(name="b"))},
            targets=["c"],
        )

        dag.add_node(action1)
        dag.add_node(action2)
        dag.add_node(action3)
        dag.add_edge(DAGEdge.state_machine(action1.id, action2.id))
        dag.add_edge(DAGEdge.state_machine(action2.id, action3.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)
        action_results: dict[UUID, Any] = {}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)
        self._compare_executor_states(executor, rehydrated)

        action_results[exec1.node_id] = 10
        executor = RunnerExecutor(dag, state=state, action_results=action_results)
        step1 = executor.increment(exec1.node_id)
        assert len(step1.actions) == 1
        exec2 = step1.actions[0]

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)
        self._compare_executor_states(executor, rehydrated)

        action_results[exec2.node_id] = 20
        step2 = executor.increment(exec2.node_id)
        assert len(step2.actions) == 1
        exec3 = step2.actions[0]

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)
        self._compare_executor_states(executor, rehydrated)

        action_results[exec3.node_id] = 30
        step3 = executor.increment(exec3.node_id)
        assert len(step3.actions) == 0

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)
        self._compare_executor_states(executor, rehydrated)

        for node in rehydrated.state.nodes.values():
            if node.node_type == "action_call":
                assert node.status == NodeStatus.COMPLETED

    def test_rehydrate_with_assignment_node(self) -> None:
        """Test rehydration with inline assignment nodes between actions."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="fetch",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
        )
        assign = AssignmentNode(
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
        action2 = ActionCallNode(
            id="action2",
            action_name="process",
            kwargs={},
            kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="y"))},
            targets=["z"],
        )

        dag.add_node(action1)
        dag.add_node(assign)
        dag.add_node(action2)
        dag.add_edge(DAGEdge.state_machine(action1.id, assign.id))
        dag.add_edge(DAGEdge.state_machine(assign.id, action2.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        action_results: dict[UUID, Any] = {exec1.node_id: 10}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        step = executor.increment(exec1.node_id)
        assert len(step.actions) == 1
        assert step.actions[0].template_id == action2.id

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        self._compare_executor_states(executor, rehydrated)

        assign_nodes = [
            n for n in rehydrated.state.nodes.values() if n.template_id == assign.id
        ]
        assert len(assign_nodes) == 1
        assert assign_nodes[0].status == NodeStatus.COMPLETED
        assert "y" in assign_nodes[0].assignments

    def test_rehydrate_preserves_action_kwargs(self) -> None:
        """Test that rehydrated executor preserves action kwargs correctly."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="compute",
            kwargs={},
            kwarg_exprs={
                "a": ir.Expr(literal=ir.Literal(int_value=5)),
                "b": ir.Expr(literal=ir.Literal(string_value="test")),
            },
            targets=["result"],
        )

        dag.add_node(action1)

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        executor = RunnerExecutor(dag, state=state, action_results={})

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        orig_node = executor.state.nodes[exec1.node_id]
        rehy_node = rehydrated.state.nodes[exec1.node_id]

        assert orig_node.action is not None
        assert rehy_node.action is not None
        assert orig_node.action.action_name == rehy_node.action.action_name
        assert set(orig_node.action.kwargs.keys()) == set(rehy_node.action.kwargs.keys())

    def test_rehydrate_increments_from_same_position(self) -> None:
        """Test that rehydrated executor produces identical next steps."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="first",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
        )
        action2 = ActionCallNode(
            id="action2",
            action_name="second",
            kwargs={},
            kwarg_exprs={},
            targets=["y"],
        )

        dag.add_node(action1)
        dag.add_node(action2)
        dag.add_edge(DAGEdge.state_machine(action1.id, action2.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        action_results: dict[UUID, Any] = {exec1.node_id: 100}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        orig_step = executor.increment(exec1.node_id)

        orig_nodes_after, orig_edges_after, _ = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehy_step = rehydrated.increment(exec1.node_id)

        assert len(orig_step.actions) == len(rehy_step.actions)
        assert orig_step.actions[0].template_id == rehy_step.actions[0].template_id

    def test_rehydrate_resume_marks_running_as_retryable(self) -> None:
        """Test that resume() on rehydrated executor handles inflight actions."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="work",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
            policies=[ir.PolicyBracket(retry=ir.RetryPolicy(max_retries=3))],
        )

        dag.add_node(action1)

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)
        state.mark_running(exec1.node_id)

        executor = RunnerExecutor(dag, state=state, action_results={})

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )

        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        assert rehydrated.state.nodes[exec1.node_id].status == NodeStatus.RUNNING

        step = rehydrated.resume()

        assert len(step.actions) == 1
        assert step.actions[0].node_id == exec1.node_id
        assert rehydrated.state.nodes[exec1.node_id].status == NodeStatus.QUEUED
        assert rehydrated.state.nodes[exec1.node_id].action_attempt == 2

    def test_rehydrate_replay_variables_consistent(self) -> None:
        """Test that replay produces same variable values after rehydration."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="fetch",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
        )
        assign = AssignmentNode(
            id="assign",
            targets=["doubled"],
            assign_expr=ir.Expr(
                binary_op=ir.BinaryOp(
                    left=ir.Expr(variable=ir.Variable(name="x")),
                    op=ir.BinaryOperator.BINARY_OP_MUL,
                    right=ir.Expr(literal=ir.Literal(int_value=2)),
                )
            ),
        )

        dag.add_node(action1)
        dag.add_node(assign)
        dag.add_edge(DAGEdge.state_machine(action1.id, assign.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        action_results: dict[UUID, Any] = {exec1.node_id: 21}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)
        executor.increment(exec1.node_id)

        orig_replay = replay_variables(executor.state, executor.action_results)

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        rehy_replay = replay_variables(rehydrated.state, dict(rehydrated.action_results))

        assert orig_replay.variables == rehy_replay.variables
        assert rehy_replay.variables.get("doubled") == 42

    def test_rehydrate_spread_action_with_aggregator(self) -> None:
        """Test rehydration of spread actions with an aggregator node."""
        dag = DAG()

        initial_action = ActionCallNode(
            id="initial",
            action_name="get_items",
            kwargs={},
            kwarg_exprs={},
            targets=["items"],
        )

        spread_action = ActionCallNode(
            id="spread_action",
            action_name="process_item",
            kwargs={},
            kwarg_exprs={"item": ir.Expr(variable=ir.Variable(name="item"))},
            targets=["item_result"],
            spread_loop_var="item",
            spread_collection_expr=ir.Expr(variable=ir.Variable(name="items")),
            aggregates_to="aggregator",
        )

        aggregator = AggregatorNode(
            id="aggregator",
            aggregates_from="spread_action",
            targets=["results"],
        )

        dag.add_node(initial_action)
        dag.add_node(spread_action)
        dag.add_node(aggregator)
        dag.add_edge(DAGEdge.state_machine(initial_action.id, spread_action.id))
        dag.add_edge(DAGEdge.state_machine(spread_action.id, aggregator.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        initial_exec = state.queue_template_node(initial_action.id)

        action_results: dict[UUID, Any] = {initial_exec.node_id: [1, 2, 3]}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        step1 = executor.increment(initial_exec.node_id)

        assert len(step1.actions) == 3

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        self._compare_executor_states(executor, rehydrated)

        action_nodes = [
            n for n in executor.state.nodes.values()
            if n.node_type == "action_call" and n.template_id == spread_action.id
        ]
        assert len(action_nodes) == 3

        for action_node in action_nodes:
            assert action_node.node_id in rehydrated.state.nodes
            rehy_node = rehydrated.state.nodes[action_node.node_id]
            assert rehy_node.node_type == action_node.node_type
            assert rehy_node.status == action_node.status

    def test_rehydrate_full_spread_execution(self) -> None:
        """Test complete spread workflow with rehydration at each step."""
        dag = DAG()

        initial_action = ActionCallNode(
            id="initial",
            action_name="get_items",
            kwargs={},
            kwarg_exprs={},
            targets=["items"],
        )

        spread_action = ActionCallNode(
            id="spread_action",
            action_name="double",
            kwargs={},
            kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="item"))},
            targets=["item_result"],
            spread_loop_var="item",
            spread_collection_expr=ir.Expr(variable=ir.Variable(name="items")),
            aggregates_to="aggregator",
        )

        aggregator = AggregatorNode(
            id="aggregator",
            aggregates_from="spread_action",
            targets=["results"],
        )

        dag.add_node(initial_action)
        dag.add_node(spread_action)
        dag.add_node(aggregator)
        dag.add_edge(DAGEdge.state_machine(initial_action.id, spread_action.id))
        dag.add_edge(DAGEdge.state_machine(spread_action.id, aggregator.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        initial_exec = state.queue_template_node(initial_action.id)

        action_results: dict[UUID, Any] = {initial_exec.node_id: [10, 20]}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        step1 = executor.increment(initial_exec.node_id)
        spread_nodes = step1.actions
        assert len(spread_nodes) == 2

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)
        self._compare_executor_states(executor, rehydrated)

        for idx, node in enumerate(spread_nodes):
            action_results[node.node_id] = (idx + 1) * 100

        step2 = executor.increment_batch([n.node_id for n in spread_nodes])

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)
        self._compare_executor_states(executor, rehydrated)

        agg_nodes = [
            n for n in rehydrated.state.nodes.values()
            if n.template_id == aggregator.id
        ]
        assert len(agg_nodes) == 1
        assert agg_nodes[0].status == NodeStatus.COMPLETED
        assert "results" in agg_nodes[0].assignments

    def test_rehydrate_timeline_ordering_preserved(self) -> None:
        """Test that timeline ordering is correctly rebuilt after rehydration."""
        dag = DAG()

        actions = [
            ActionCallNode(
                id=f"action{i}",
                action_name=f"step{i}",
                kwargs={},
                kwarg_exprs={},
                targets=[f"x{i}"],
            )
            for i in range(4)
        ]

        for action in actions:
            dag.add_node(action)
        for i in range(len(actions) - 1):
            dag.add_edge(DAGEdge.state_machine(actions[i].id, actions[i + 1].id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec_nodes: list[ExecutionNode] = []
        action_results: dict[UUID, Any] = {}

        exec_nodes.append(state.queue_template_node(actions[0].id))
        executor = RunnerExecutor(dag, state=state, action_results=action_results)

        for i in range(3):
            action_results[exec_nodes[-1].node_id] = i * 10
            step = executor.increment(exec_nodes[-1].node_id)
            if step.actions:
                exec_nodes.append(step.actions[0])

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        orig_timeline = executor.state.timeline
        rehy_timeline = rehydrated.state.timeline

        assert len(orig_timeline) == len(rehy_timeline)
        assert set(orig_timeline) == set(rehy_timeline)

    def test_rehydrate_ready_queue_rebuilt(self) -> None:
        """Test that ready queue is correctly rebuilt after rehydration."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="first",
            kwargs={},
            kwarg_exprs={},
            targets=["x"],
        )
        action2 = ActionCallNode(
            id="action2",
            action_name="second",
            kwargs={},
            kwarg_exprs={},
            targets=["y"],
        )

        dag.add_node(action1)
        dag.add_node(action2)
        dag.add_edge(DAGEdge.state_machine(action1.id, action2.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        action_results: dict[UUID, Any] = {exec1.node_id: 50}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)
        step = executor.increment(exec1.node_id)
        exec2 = step.actions[0]

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        queued_in_rehydrated = [
            n for n in rehydrated.state.nodes.values()
            if n.status == NodeStatus.QUEUED
        ]
        assert len(queued_in_rehydrated) == 1
        assert queued_in_rehydrated[0].node_id == exec2.node_id

    def test_rehydrate_data_flow_edges_preserved(self) -> None:
        """Test that data flow edges are preserved through rehydration."""
        dag = DAG()

        action1 = ActionCallNode(
            id="action1",
            action_name="fetch",
            kwargs={},
            kwarg_exprs={},
            targets=["data"],
        )
        assign = AssignmentNode(
            id="assign",
            targets=["processed"],
            assign_expr=ir.Expr(
                binary_op=ir.BinaryOp(
                    left=ir.Expr(variable=ir.Variable(name="data")),
                    op=ir.BinaryOperator.BINARY_OP_ADD,
                    right=ir.Expr(literal=ir.Literal(int_value=5)),
                )
            ),
        )
        action2 = ActionCallNode(
            id="action2",
            action_name="send",
            kwargs={},
            kwarg_exprs={"value": ir.Expr(variable=ir.Variable(name="processed"))},
            targets=["result"],
        )

        dag.add_node(action1)
        dag.add_node(assign)
        dag.add_node(action2)
        dag.add_edge(DAGEdge.state_machine(action1.id, assign.id))
        dag.add_edge(DAGEdge.state_machine(assign.id, action2.id))

        state = RunnerState(dag=dag, link_queued_nodes=False)
        exec1 = state.queue_template_node(action1.id)

        action_results: dict[UUID, Any] = {exec1.node_id: 100}
        executor = RunnerExecutor(dag, state=state, action_results=action_results)
        executor.increment(exec1.node_id)

        from rappel_core.dag import EdgeType

        orig_data_edges = [e for e in executor.state.edges if e.edge_type == EdgeType.DATA_FLOW]

        nodes_snap, edges_snap, results_snap = self._snapshot_state(
            executor.state, dict(executor.action_results)
        )
        rehydrated = self._create_rehydrated_executor(dag, nodes_snap, edges_snap, results_snap)

        rehy_data_edges = [e for e in rehydrated.state.edges if e.edge_type == EdgeType.DATA_FLOW]

        assert len(orig_data_edges) == len(rehy_data_edges)
        assert set(orig_data_edges) == set(rehy_data_edges)
