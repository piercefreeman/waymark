from proto import ast_pb2 as ir
from rappel_core.runner.state import (
    ActionResultValue,
    BinaryOpValue,
    ListValue,
    LiteralValue,
    RunnerState,
)


def _action_plus_two_expr() -> ir.Expr:
    return ir.Expr(
        binary_op=ir.BinaryOp(
            left=ir.Expr(variable=ir.Variable(name="action_result")),
            op=ir.BinaryOperator.BINARY_OP_ADD,
            right=ir.Expr(literal=ir.Literal(int_value=2)),
        )
    )


def test_runner_state_unrolls_loop_assignments() -> None:
    state = RunnerState()

    state.queue_action("action", targets=["action_result"], iteration_index=0)
    first_list = ir.Expr(list=ir.ListExpr(elements=[_action_plus_two_expr()]))
    state.record_assignment(targets=["results"], expr=first_list)

    state.queue_action("action", targets=["action_result"], iteration_index=1)
    second_list = ir.Expr(list=ir.ListExpr(elements=[_action_plus_two_expr()]))
    concat_expr = ir.Expr(
        binary_op=ir.BinaryOp(
            left=ir.Expr(variable=ir.Variable(name="results")),
            op=ir.BinaryOperator.BINARY_OP_ADD,
            right=second_list,
        )
    )
    state.record_assignment(targets=["results"], expr=concat_expr)

    results = None
    for node_id in reversed(state.timeline):
        node = state.nodes[node_id]
        if "results" in node.assignments:
            results = node.assignments["results"]
            break

    assert results is not None
    assert isinstance(results, ListValue)
    assert len(results.elements) == 2

    first_item = results.elements[0]
    second_item = results.elements[1]

    assert isinstance(first_item, BinaryOpValue)
    assert isinstance(second_item, BinaryOpValue)

    assert isinstance(first_item.left, ActionResultValue)
    assert first_item.left.iteration_index == 0

    assert isinstance(second_item.left, ActionResultValue)
    assert second_item.left.iteration_index == 1

    assert isinstance(first_item.right, LiteralValue)
    assert first_item.right.value == 2
    assert isinstance(second_item.right, LiteralValue)
    assert second_item.right.value == 2


def test_runner_state_graph_dirty_for_action_updates() -> None:
    state = RunnerState()

    assert state.consume_graph_dirty_for_durable_execution() is False

    action_result = state.queue_action("action", targets=["action_result"])
    assert state.consume_graph_dirty_for_durable_execution() is True

    assert state.consume_graph_dirty_for_durable_execution() is False

    state.increment_action_attempt(action_result.node_id)
    assert state.consume_graph_dirty_for_durable_execution() is True


def test_runner_state_graph_dirty_not_set_for_assignments() -> None:
    state = RunnerState()

    state.record_assignment_value(targets=["value"], value_expr=LiteralValue(1))

    assert state.consume_graph_dirty_for_durable_execution() is False
