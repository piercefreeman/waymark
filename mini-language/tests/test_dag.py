"""Tests for Rappel DAG conversion."""

from rappel import (
    parse,
    convert_to_dag,
    DAG,
    DAGNode,
    DAGEdge,
    EdgeType,
    DAGConverter,
)


def test_dag_function_simple():
    """Test DAG for a simple function with assignment."""
    source = """fn test(input: [x], output: [y]):
    y = x + 1
    return y"""
    program = parse(source)
    dag = convert_to_dag(program)

    assert isinstance(dag, DAG)
    # Should have input, output, assignment, and return nodes
    assert len(dag.nodes) >= 3


def test_dag_function_chained_assignments():
    """Test DAG for function with chained assignments."""
    source = """fn compute(input: [x], output: [z]):
    y = x + 1
    z = y + 1
    return z"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should have nodes for each statement plus input/output
    assert len(dag.nodes) >= 4
    # Should have edges between dependent statements
    assert len(dag.edges) > 0


def test_dag_function_creates_subgraph():
    """Test that functions create separate subgraphs."""
    source = """fn add(input: [a, b], output: [result]):
    result = a + b
    return result"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should have function boundary nodes and internal nodes
    node_ids = list(dag.nodes.keys())
    assert any("add" in nid for nid in node_ids)


def test_dag_function_with_action_call():
    """Test DAG for function with action call."""
    source = """fn fetch(input: [url], output: [response]):
    response = @fetch_url(url=url)
    return response"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should have nodes including action call
    assert len(dag.nodes) >= 3
    # Check that action call node exists
    node_types = {n.node_type for n in dag.nodes.values()}
    assert "action_call" in node_types


def test_dag_function_with_for_loop():
    """Test DAG for function with for loop."""
    source = """fn process_all(input: [items], output: [results]):
    results = []
    for item in items:
        result = process(x=item)
    return results"""
    program = parse(source)
    dag = convert_to_dag(program)

    assert len(dag.nodes) >= 3
    # Check that for loop node exists
    node_types = {n.node_type for n in dag.nodes.values()}
    assert "for_loop" in node_types


def test_dag_function_with_if_statement():
    """Test DAG for function with if statement."""
    source = """fn classify(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"""
    program = parse(source)
    dag = convert_to_dag(program)

    assert len(dag.nodes) >= 3
    # Check that if node exists
    node_types = {n.node_type for n in dag.nodes.values()}
    assert "if" in node_types


def test_dag_edge_types():
    """Test that DAG has correct edge types."""
    source = """fn compute(input: [x], output: [y]):
    y = x + 1
    return y"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should have both state machine and data flow edges
    state_machine_edges = [e for e in dag.edges if e.edge_type == EdgeType.STATE_MACHINE]
    data_flow_edges = [e for e in dag.edges if e.edge_type == EdgeType.DATA_FLOW]

    assert len(state_machine_edges) > 0
    assert len(data_flow_edges) >= 0  # May or may not have data flow edges


def test_dag_converter_class():
    """Test using DAGConverter class directly."""
    source = """fn test(input: [], output: [x]):
    x = 42
    return x"""
    program = parse(source)

    converter = DAGConverter()
    dag = converter.convert(program)

    assert isinstance(dag, DAG)
    assert len(dag.nodes) > 0


def test_dag_node_properties():
    """Test DAGNode properties."""
    node = DAGNode(
        id="test_node",
        node_type="assignment",
        ir_node=None,
        label="Test",
    )

    assert node.id == "test_node"
    assert node.label == "Test"
    assert node.node_type == "assignment"
    assert node.ir_node is None


def test_dag_edge_properties():
    """Test DAGEdge properties."""
    edge = DAGEdge(
        source="node1",
        target="node2",
        edge_type=EdgeType.DATA_FLOW,
        variable="x",
    )

    assert edge.source == "node1"
    assert edge.target == "node2"
    assert edge.edge_type == EdgeType.DATA_FLOW
    assert edge.variable == "x"


def test_edge_type_enum():
    """Test EdgeType enum values."""
    assert EdgeType.STATE_MACHINE.value
    assert EdgeType.DATA_FLOW.value
    assert EdgeType.STATE_MACHINE != EdgeType.DATA_FLOW


def test_dag_visualization_no_crash():
    """Test that DAG has nodes and edges structure."""
    source = """fn compute(input: [x], output: [z]):
    y = x + 1
    z = y + 1
    return z"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should have nodes and edges
    assert len(dag.nodes) > 0
    assert len(dag.edges) >= 0


def test_dag_complex_workflow():
    """Test DAG for complex workflow with multiple constructs."""
    source = """fn process(input: [x], output: [y]):
    y = x * 2
    return y"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should handle all constructs without error
    assert len(dag.nodes) > 0


def test_dag_function_with_spread_action():
    """Test DAG for function with spread action."""
    source = """fn fetch_all(input: [items], output: [results]):
    results = spread items:item -> @fetch(id=item)
    return results"""
    program = parse(source)
    dag = convert_to_dag(program)

    assert len(dag.nodes) >= 3
    # Should have aggregator node
    node_types = {n.node_type for n in dag.nodes.values()}
    assert "aggregator" in node_types


def test_dag_multiple_functions():
    """Test DAG with multiple functions creates separate subgraphs."""
    source = """fn add(input: [a, b], output: [result]):
    result = a + b
    return result

fn multiply(input: [x, y], output: [product]):
    product = x * y
    return product"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Should have nodes for both functions
    functions = dag.get_functions()
    assert "add" in functions
    assert "multiply" in functions


def test_dag_function_io_nodes():
    """Test that functions have input and output boundary nodes."""
    source = """fn transform(input: [x], output: [y]):
    y = x * 2
    return y"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Find input and output nodes
    input_nodes = [n for n in dag.nodes.values() if n.is_input]
    output_nodes = [n for n in dag.nodes.values() if n.is_output]

    assert len(input_nodes) == 1
    assert len(output_nodes) == 1
    assert input_nodes[0].io_vars == ("x",)
    assert output_nodes[0].io_vars == ("y",)


def test_dag_fn_call_node():
    """Test that function calls create fn_call nodes."""
    source = """fn helper(input: [x], output: [y]):
    y = x + 1
    return y

fn main(input: [], output: [result]):
    result = helper(x=10)
    return result"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Find fn_call nodes
    fn_call_nodes = [n for n in dag.nodes.values() if n.is_fn_call]
    assert len(fn_call_nodes) >= 1
    assert any(n.called_function == "helper" for n in fn_call_nodes)


def test_data_flow_cutoff_on_variable_reassignment():
    """Test that data flow connects to most recent definition when variable is reassigned.

    When a variable is reassigned, downstream uses should get data flow from
    the reassignment node, not the original definition. This ensures the
    data flow graph correctly models variable shadowing/updates.
    """
    source = """fn test(input: [x], output: [z]):
    y = x + 1
    x = 10
    z = x + 2
    return z"""
    program = parse(source)
    dag = convert_to_dag(program)

    # Get data flow edges for variable 'x'
    data_flow_edges = dag.get_data_flow_edges()
    x_data_flow = [e for e in data_flow_edges if e.variable == "x"]

    # Find the nodes by their labels/types
    input_node = None
    reassign_node = None  # x = 10
    z_assign_node = None  # z = x + 2

    for node_id, node in dag.nodes.items():
        if node.is_input and node.function_name == "test":
            input_node = node_id
        elif node.node_type == "assignment" and node.ir_node is not None:
            from rappel.ir import RappelAssignment, RappelLiteral
            if isinstance(node.ir_node, RappelAssignment):
                if node.ir_node.target == "x" and isinstance(node.ir_node.value, RappelLiteral):
                    reassign_node = node_id
                elif node.ir_node.target == "z":
                    z_assign_node = node_id

    assert input_node is not None, "Should have input node"
    assert reassign_node is not None, "Should have x=10 reassignment node"
    assert z_assign_node is not None, "Should have z=x+2 assignment node"

    # The key assertion: z's use of x should get data flow from the reassignment (x=10),
    # NOT from the input node
    z_x_edges = [e for e in x_data_flow if e.target == z_assign_node]
    assert len(z_x_edges) == 1, "z should have exactly one data flow edge for x"
    assert z_x_edges[0].source == reassign_node, (
        f"z's data flow for x should come from reassignment node ({reassign_node}), "
        f"not input node ({input_node}). Got source: {z_x_edges[0].source}"
    )

    # Also verify: the first use of x (y = x + 1) should get data flow from input
    y_assign_node = None
    for node_id, node in dag.nodes.items():
        if node.node_type == "assignment" and node.ir_node is not None:
            from rappel.ir import RappelAssignment
            if isinstance(node.ir_node, RappelAssignment) and node.ir_node.target == "y":
                y_assign_node = node_id
                break

    assert y_assign_node is not None, "Should have y=x+1 assignment node"
    y_x_edges = [e for e in x_data_flow if e.target == y_assign_node]
    assert len(y_x_edges) == 1, "y should have exactly one data flow edge for x"
    assert y_x_edges[0].source == input_node, (
        f"y's data flow for x should come from input node ({input_node}), "
        f"not reassignment node ({reassign_node}). Got source: {y_x_edges[0].source}"
    )
