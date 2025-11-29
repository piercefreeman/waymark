import asyncio
from typing import Annotated

from proto import messages_pb2 as pb2
from rappel import registry as action_registry
from rappel.actions import action, serialize_error_payload, serialize_result_payload
from rappel.dependencies import Depend
from rappel.workflow_runtime import NodeExecutionResult, execute_node


@action
async def multiply(value: int) -> int:
    return value * 2


@action
async def exception_handler() -> str:
    return "handled"


async def provide_suffix() -> str:
    return "suffix"


@action
async def with_dependency(value: int, suffix: Annotated[str, Depend(provide_suffix)]) -> str:
    return f"{value}-{suffix}"


def _build_resolved_dispatch() -> pb2.WorkflowNodeDispatch:
    if action_registry.get("multiply") is None:
        action_registry.register("multiply", multiply)
    node = pb2.WorkflowDagNode(
        id="node_multiply",
        action="multiply",
        module=__name__,
    )
    node.produces.append("value")
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    resolved = pb2.WorkflowArguments()
    entry = resolved.arguments.add()
    entry.key = "value"
    entry.value.primitive.int_value = 10
    dispatch.resolved_kwargs.CopyFrom(resolved)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    return dispatch


def _build_exception_dispatch(include_error: bool) -> pb2.WorkflowNodeDispatch:
    if action_registry.get("exception_handler") is None:
        action_registry.register("exception_handler", exception_handler)
    node = pb2.WorkflowDagNode(
        id="node_handler",
        action="exception_handler",
        module=__name__,
    )
    node.produces.append("value")
    node.guard = "__workflow_exceptions.get('node_source') is not None"
    edge = node.exception_edges.add()
    edge.source_node_id = "node_source"
    edge.exception_type = "RuntimeError"
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    if include_error:
        payload = serialize_error_payload("source", RuntimeError("boom"))
    else:
        payload = serialize_result_payload("noop")
    entry = dispatch.context.add()
    entry.variable = ""
    entry.workflow_node_id = "node_source"
    entry.payload.CopyFrom(payload)
    return dispatch


def _build_dependency_dispatch() -> pb2.WorkflowNodeDispatch:
    if action_registry.get("with_dependency") is None:
        action_registry.register("with_dependency", with_dependency)
    node = pb2.WorkflowDagNode(
        id="node_dependency",
        action="with_dependency",
        module=__name__,
    )
    node.produces.append("value")
    dispatch = pb2.WorkflowNodeDispatch(node=node)
    resolved = pb2.WorkflowArguments()
    entry = resolved.arguments.add()
    entry.key = "value"
    entry.value.primitive.int_value = 3
    dispatch.resolved_kwargs.CopyFrom(resolved)
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())
    return dispatch


def test_execute_node_uses_resolved_kwargs() -> None:
    payload = _build_resolved_dispatch()
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, NodeExecutionResult)
    assert result.result == 20


def test_execute_node_handles_exception_when_edge_matches() -> None:
    payload = _build_exception_dispatch(include_error=True)
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, NodeExecutionResult)
    assert result.result == "handled"


def test_execute_node_resolves_dependencies() -> None:
    payload = _build_dependency_dispatch()
    result = asyncio.run(execute_node(payload))
    assert isinstance(result, NodeExecutionResult)
    assert result.result == "3-suffix"


def test_conditional_merge_node_extracts_temp_variable() -> None:
    """Test that merge nodes correctly extract values from temp branch variables.

    This is a regression test for a bug where conditional branches with elif
    would fail because the merge node received temp variables as dicts instead
    of their unwrapped values.
    """
    from rappel.workflow_runtime import _execute_python_block

    # Simulate the merge node code that's generated for conditionals
    merge_code = """
if "__branch_message_node_0" in locals():
    message = __branch_message_node_0
elif "__branch_message_node_1" in locals():
    message = __branch_message_node_1
else:
    raise RuntimeError("no conditional branch executed")
"""

    # Create a node that simulates the merge python_block
    node = pb2.WorkflowDagNode(
        id="node_merge",
        action="python_block",
    )
    node.kwargs["code"] = merge_code.strip()
    node.kwargs["imports"] = ""
    node.kwargs["definitions"] = ""
    node.produces.append("message")

    # Context should have the temp variable set to the actual string value
    # This is what would come from a guarded action node that was executed
    context = {
        "__branch_message_node_0": "High value detected: 80 is in the top tier!",
    }

    result = _execute_python_block(node, context)

    # The merge node should produce message with the unwrapped value
    assert "message" in result
    assert result["message"] == "High value detected: 80 is in the top tier!"
    assert isinstance(result["message"], str), (
        f"Expected string, got {type(result['message'])}: {result['message']}"
    )


def test_build_context_unwraps_workflow_node_result_for_temp_variables() -> None:
    """Test that _build_context correctly unwraps WorkflowNodeResult for temp variables.

    This is a regression test for a bug where guarded action nodes that produce
    temp variables (like __branch_message_node_0) would have their results
    incorrectly passed as dicts to the merge node.
    """
    from rappel.workflow_runtime import _build_context

    # Create a dispatch with context entries from a guarded action node
    dispatch = pb2.WorkflowNodeDispatch()
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())

    # Simulate what happens when a guarded action node produces a temp variable
    # The action returns a string, but it gets wrapped in a context entry
    temp_var_name = "__branch_message_node_0"
    action_result = "High value detected: 80 is in the top tier!"

    # The context entry has the temp variable name and the serialized result
    entry = dispatch.context.add()
    entry.variable = temp_var_name
    entry.workflow_node_id = "node_0"
    payload = serialize_result_payload(action_result)
    entry.payload.CopyFrom(payload)

    context, exceptions = _build_context(dispatch)

    # The temp variable should be set to the actual string value
    assert temp_var_name in context, f"Expected {temp_var_name} in context"
    assert context[temp_var_name] == action_result, (
        f"Expected {action_result!r}, got {context[temp_var_name]!r}"
    )
    assert isinstance(context[temp_var_name], str), (
        f"Expected string, got {type(context[temp_var_name])}"
    )


def test_build_context_unwraps_workflow_node_result_variables() -> None:
    """Test that _build_context handles WorkflowNodeResult with variables dict.

    When a python_block produces multiple variables, the result is wrapped
    in WorkflowNodeResult with a variables dict. The context should extract
    the specific variable from this dict.
    """
    from rappel.workflow_runtime import WorkflowNodeResult, _build_context

    dispatch = pb2.WorkflowNodeDispatch()
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())

    # Simulate a python_block that produces a temp variable
    temp_var_name = "__branch_message_node_0"
    actual_value = "High value detected: 80 is in the top tier!"

    # The python_block returns WorkflowNodeResult with variables dict
    node_result = WorkflowNodeResult(variables={temp_var_name: actual_value})

    entry = dispatch.context.add()
    entry.variable = temp_var_name
    entry.workflow_node_id = "node_0"
    payload = serialize_result_payload(node_result)
    entry.payload.CopyFrom(payload)

    context, exceptions = _build_context(dispatch)

    # The temp variable should be unwrapped from the WorkflowNodeResult
    assert temp_var_name in context, f"Expected {temp_var_name} in context"
    assert context[temp_var_name] == actual_value, (
        f"Expected {actual_value!r}, got {context[temp_var_name]!r}"
    )
    assert isinstance(context[temp_var_name], str), (
        f"Expected string, got {type(context[temp_var_name])}"
    )


def test_build_context_unwraps_dict_result_for_temp_variable() -> None:
    """Test that _build_context unwraps dict results that contain the variable name.

    This is a regression test for a bug where conditional branches fail because
    the Go scheduler sends results as dicts like {temp_var: value}, and
    _build_context was passing this dict directly instead of extracting the value.

    The fix is to check if the result is a dict containing the variable name
    as a key, and if so, extract that value.
    """
    from rappel.workflow_runtime import _build_context

    dispatch = pb2.WorkflowNodeDispatch()
    dispatch.workflow_input.CopyFrom(pb2.WorkflowArguments())

    temp_var_name = "__branch_message_node_0"
    actual_value = "High value detected: 80 is in the top tier!"

    # The Go scheduler sends results as dicts like {temp_var: value}
    dict_result = {temp_var_name: actual_value}

    entry = dispatch.context.add()
    entry.variable = temp_var_name
    entry.workflow_node_id = "node_0"
    payload = serialize_result_payload(dict_result)
    entry.payload.CopyFrom(payload)

    context, exceptions = _build_context(dispatch)

    # The context should have the unwrapped value, not the dict
    assert temp_var_name in context, f"Expected {temp_var_name} in context"
    assert context[temp_var_name] == actual_value, (
        f"Expected {actual_value!r}, got {context[temp_var_name]!r}. "
        f"The dict should have been unwrapped."
    )
    assert isinstance(context[temp_var_name], str), (
        f"Expected string, got {type(context[temp_var_name])}: {context[temp_var_name]}"
    )


def test_conditional_merge_node_skips_none_temp_variables() -> None:
    """Test that merge nodes correctly skip temp variables that are None.

    This is a regression test for a bug where conditional branches with elif
    would fail because the Go scheduler passes None for skipped guarded nodes,
    and the merge node's check '"__branch_x" in locals()' would return True
    for None values, causing the wrong branch to be selected.

    The fix is to use 'locals().get("__branch_x") is not None' instead.
    """
    from rappel.workflow_runtime import _execute_python_block

    # Simulate the merge node code with the fixed condition
    merge_code = """
if locals().get("__branch_message_node_0") is not None:
    message = __branch_message_node_0
    branch = "high"
elif locals().get("__branch_message_node_1") is not None:
    message = __branch_message_node_1
    branch = "medium"
elif locals().get("__branch_message_node_2") is not None:
    message = __branch_message_node_2
    branch = "low"
else:
    raise RuntimeError("conditional branch produced no value for message")
"""

    node = pb2.WorkflowDagNode(
        id="node_merge",
        action="python_block",
    )
    node.kwargs["code"] = merge_code.strip()
    node.kwargs["imports"] = ""
    node.kwargs["definitions"] = ""
    node.produces.append("message")
    node.produces.append("branch")

    # Context simulates: value=50, so the "medium" branch should execute
    # node_0 (high) guard was false -> None
    # node_1 (medium) guard was true -> actual value
    # node_2 (low) guard was false -> None
    context = {
        "value": 50,
        "__branch_message_node_0": None,  # Skipped (guard was false)
        "__branch_message_node_1": "Medium value: 50 is in the middle tier!",  # Executed
        "__branch_message_node_2": None,  # Skipped (guard was false)
    }

    result = _execute_python_block(node, context)

    assert result["message"] == "Medium value: 50 is in the middle tier!"
    assert result["branch"] == "medium"
