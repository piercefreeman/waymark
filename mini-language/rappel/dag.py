"""
Rappel DAG - Directed Acyclic Graph representation for program execution.

The DAG represents:
- Nodes: Individual execution steps (assignments, actions, loops, etc.)
- Edges: Execution order (state machine) and data flow relationships

Each function is converted into an isolated subgraph with input/output boundaries.
"""

from __future__ import annotations

import json
import tempfile
import uuid
import webbrowser
from dataclasses import dataclass, field
from enum import Enum, auto

from .ir import (
    RappelVariable,
    RappelBinaryOp,
    RappelUnaryOp,
    RappelIndexAccess,
    RappelDotAccess,
    RappelSpread,
    RappelCall,
    RappelActionCall,
    RappelListExpr,
    RappelDictExpr,
    RappelExpr,
    RappelAssignment,
    RappelMultiAssignment,
    RappelReturn,
    RappelExprStatement,
    RappelPythonBlock,
    RappelFunctionDef,
    RappelForLoop,
    RappelIfStatement,
    RappelSpreadAction,
    RappelStatement,
    RappelProgram,
)


class EdgeType(Enum):
    """Types of edges in the DAG."""

    STATE_MACHINE = auto()  # Execution order edge
    DATA_FLOW = auto()  # Variable data flow edge


def _generate_uuid() -> str:
    """Generate a new UUID for a node."""
    return str(uuid.uuid4())


@dataclass(frozen=True)
class DAGNode:
    """A node in the execution DAG."""

    id: str
    node_type: str  # e.g., "assignment", "action_call", "for_loop", "if", "aggregator", "fn_call", "input", "output"
    ir_node: RappelStatement | RappelExpr | None
    label: str  # Human-readable label
    # Unique identifier for this node instance (used for batch data operations)
    node_uuid: str = field(default_factory=_generate_uuid)
    function_name: str | None = None  # Which function this node belongs to (None = top-level)
    # For for loops
    is_loop_head: bool = False
    loop_var: str | None = None
    # For aggregators
    is_aggregator: bool = False
    aggregates_from: str | None = None  # Node ID this aggregates from
    # For function call nodes
    is_fn_call: bool = False
    called_function: str | None = None  # Name of function being called
    # For input/output boundary nodes
    is_input: bool = False
    is_output: bool = False
    io_vars: tuple[str, ...] | None = None  # Variables at this boundary


@dataclass(frozen=True)
class DAGEdge:
    """An edge in the execution DAG."""

    source: str  # Node ID
    target: str  # Node ID
    edge_type: EdgeType
    # For state machine edges
    condition: str | None = None  # e.g., "continue", "done", "then", "else"
    # For data flow edges
    variable: str | None = None  # Which variable is being passed


@dataclass
class DAG:
    """A directed acyclic graph representing program execution."""

    nodes: dict[str, DAGNode]
    edges: list[DAGEdge]
    entry_node: str | None = None

    def __init__(self):
        self.nodes = {}
        self.edges = []
        self.entry_node = None

    def add_node(self, node: DAGNode) -> None:
        """Add a node to the DAG."""
        self.nodes[node.id] = node
        if self.entry_node is None:
            self.entry_node = node.id

    def add_edge(self, edge: DAGEdge) -> None:
        """Add an edge to the DAG."""
        self.edges.append(edge)

    def get_incoming_edges(self, node_id: str) -> list[DAGEdge]:
        """Get all edges pointing to a node."""
        return [e for e in self.edges if e.target == node_id]

    def get_outgoing_edges(self, node_id: str) -> list[DAGEdge]:
        """Get all edges from a node."""
        return [e for e in self.edges if e.source == node_id]

    def get_state_machine_edges(self) -> list[DAGEdge]:
        """Get all state machine (execution order) edges."""
        return [e for e in self.edges if e.edge_type == EdgeType.STATE_MACHINE]

    def get_data_flow_edges(self) -> list[DAGEdge]:
        """Get all data flow edges."""
        return [e for e in self.edges if e.edge_type == EdgeType.DATA_FLOW]

    def get_functions(self) -> list[str]:
        """Get all function names that have nodes in this DAG."""
        functions = set()
        for node in self.nodes.values():
            if node.function_name:
                functions.add(node.function_name)
        return sorted(functions)

    def get_nodes_for_function(self, function_name: str) -> dict[str, DAGNode]:
        """Get all nodes belonging to a specific function."""
        return {
            nid: node for nid, node in self.nodes.items()
            if node.function_name == function_name
        }

    def get_edges_for_function(self, function_name: str) -> list[DAGEdge]:
        """Get all edges where both source and target belong to the function."""
        fn_nodes = set(self.get_nodes_for_function(function_name).keys())
        return [
            e for e in self.edges
            if e.source in fn_nodes and e.target in fn_nodes
        ]

    def visualize(self, title: str = "Rappel Program DAG") -> None:
        """
        Visualize the DAG using Cytoscape.js in a browser.

        - Solid lines: state machine (execution order) edges
        - Dotted lines: data flow edges
        - Functions are grouped as compound nodes
        """
        # Build Cytoscape elements
        elements = []

        # Function colors for compound nodes
        function_colors = ['#FFE0B2', '#E1BEE7', '#B2EBF2', '#C8E6C9', '#FFCDD2', '#D1C4E9']
        functions = self.get_functions()

        # Add compound nodes for each function
        for i, fn_name in enumerate(functions):
            elements.append({
                'data': {
                    'id': f'fn_{fn_name}',
                    'label': f'fn {fn_name}',
                    'isCompound': True,
                    'color': function_colors[i % len(function_colors)]
                }
            })

        # Add nodes
        for node_id, node in self.nodes.items():
            # Determine node color based on type
            if node.is_input:
                color = '#4CAF50'  # Green for inputs
                shape = 'ellipse'
            elif node.is_output:
                color = '#F44336'  # Red for outputs
                shape = 'ellipse'
            elif node.is_fn_call:
                color = '#3F51B5'  # Indigo for function calls
                shape = 'round-rectangle'
            elif node.is_loop_head:
                color = '#FF9800'  # Orange for loops
                shape = 'diamond'
            elif node.is_aggregator:
                color = '#9C27B0'  # Purple for aggregators
                shape = 'hexagon'
            elif node.node_type == 'action_call':
                color = '#E91E63'  # Pink for actions
                shape = 'round-rectangle'
            elif node.node_type == 'if':
                color = '#00BCD4'  # Cyan for conditionals
                shape = 'diamond'
            else:
                color = '#607D8B'  # Gray for others
                shape = 'round-rectangle'

            node_data = {
                'data': {
                    'id': node_id,
                    'label': node.label,
                    'color': color,
                    'shape': shape,
                    'nodeType': node.node_type
                }
            }

            # Add parent (compound node) if node belongs to a function
            if node.function_name:
                node_data['data']['parent'] = f'fn_{node.function_name}'

            elements.append(node_data)

        # Add edges
        for i, edge in enumerate(self.edges):
            if edge.source not in self.nodes or edge.target not in self.nodes:
                continue

            edge_data = {
                'data': {
                    'id': f'edge_{i}',
                    'source': edge.source,
                    'target': edge.target,
                    'label': edge.condition or edge.variable or '',
                    'edgeType': edge.edge_type.name
                }
            }
            elements.append(edge_data)

        # Generate HTML
        html_content = self._generate_cytoscape_html(title, elements)

        # Write to temp file and open in browser
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
            f.write(html_content)
            temp_path = f.name

        webbrowser.open(f'file://{temp_path}')
        print(f"DAG visualization opened in browser: {temp_path}")

    def _generate_cytoscape_html(self, title: str, elements: list) -> str:
        """Generate HTML with Cytoscape.js visualization."""
        elements_json = json.dumps(elements, indent=2)

        return f'''<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.28.1/cytoscape.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.5/dagre.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
        }}
        #header {{
            background: #333;
            color: white;
            padding: 15px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        #header h1 {{ font-size: 18px; font-weight: 500; }}
        #legend {{
            display: flex;
            gap: 15px;
            font-size: 12px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .legend-dot {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
            border: 1px solid #333;
        }}
        .legend-line {{
            width: 20px;
            height: 2px;
        }}
        #cy {{
            width: 100%;
            height: calc(100vh - 60px);
            background: white;
        }}
        #controls {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            display: flex;
            gap: 10px;
        }}
        #controls button {{
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            background: #333;
            color: white;
            cursor: pointer;
            font-size: 13px;
        }}
        #controls button:hover {{ background: #555; }}
    </style>
</head>
<body>
    <div id="header">
        <h1>{title}</h1>
        <div id="legend">
            <div class="legend-item"><div class="legend-dot" style="background:#4CAF50"></div>Input</div>
            <div class="legend-item"><div class="legend-dot" style="background:#F44336"></div>Output</div>
            <div class="legend-item"><div class="legend-dot" style="background:#3F51B5"></div>Fn Call</div>
            <div class="legend-item"><div class="legend-dot" style="background:#E91E63"></div>Action</div>
            <div class="legend-item"><div class="legend-dot" style="background:#FF9800"></div>Loop</div>
            <div class="legend-item"><div class="legend-dot" style="background:#9C27B0"></div>Aggregator</div>
            <div class="legend-item"><div class="legend-dot" style="background:#00BCD4"></div>Conditional</div>
            <div class="legend-item"><div class="legend-line" style="background:#2196F3"></div>Execution</div>
            <div class="legend-item"><div class="legend-line" style="background:#4CAF50;border-style:dashed"></div>Data Flow</div>
        </div>
    </div>
    <div id="cy"></div>
    <div id="controls">
        <button onclick="cy.fit()">Fit View</button>
        <button onclick="cy.zoom(cy.zoom() * 1.2)">Zoom In</button>
        <button onclick="cy.zoom(cy.zoom() / 1.2)">Zoom Out</button>
        <button onclick="runLayout()">Re-layout</button>
    </div>

    <script>
        const elements = {elements_json};

        const cy = cytoscape({{
            container: document.getElementById('cy'),
            elements: elements,
            style: [
                {{
                    selector: 'node',
                    style: {{
                        'label': 'data(label)',
                        'text-valign': 'bottom',
                        'text-halign': 'center',
                        'font-size': '11px',
                        'text-margin-y': 5,
                        'background-color': 'data(color)',
                        'border-width': 2,
                        'border-color': '#333',
                        'width': 35,
                        'height': 35,
                        'shape': 'data(shape)',
                        'text-wrap': 'wrap',
                        'text-max-width': '120px'
                    }}
                }},
                {{
                    selector: 'node[?isCompound]',
                    style: {{
                        'background-color': 'data(color)',
                        'background-opacity': 0.3,
                        'border-width': 2,
                        'border-color': '#666',
                        'border-style': 'solid',
                        'label': 'data(label)',
                        'text-valign': 'top',
                        'text-halign': 'center',
                        'font-size': '14px',
                        'font-weight': 'bold',
                        'padding': '20px',
                        'text-margin-y': -10
                    }}
                }},
                {{
                    selector: 'edge[edgeType="STATE_MACHINE"]',
                    style: {{
                        'width': 2,
                        'line-color': '#2196F3',
                        'target-arrow-color': '#2196F3',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier',
                        'label': 'data(label)',
                        'font-size': '10px',
                        'text-rotation': 'autorotate',
                        'text-margin-y': -10,
                        'color': '#2196F3'
                    }}
                }},
                {{
                    selector: 'edge[edgeType="DATA_FLOW"]',
                    style: {{
                        'width': 1.5,
                        'line-color': '#4CAF50',
                        'line-style': 'dashed',
                        'target-arrow-color': '#4CAF50',
                        'target-arrow-shape': 'triangle',
                        'curve-style': 'bezier',
                        'label': 'data(label)',
                        'font-size': '9px',
                        'text-rotation': 'autorotate',
                        'text-margin-y': -8,
                        'color': '#4CAF50'
                    }}
                }},
                {{
                    selector: ':selected',
                    style: {{
                        'border-width': 4,
                        'border-color': '#FF5722'
                    }}
                }}
            ],
            layout: {{ name: 'preset' }}
        }});

        function runLayout() {{
            cy.layout({{
                name: 'dagre',
                rankDir: 'TB',
                nodeSep: 50,
                rankSep: 80,
                padding: 30,
                animate: true,
                animationDuration: 500
            }}).run();
        }}

        // Run initial layout
        runLayout();

        // Enable node dragging
        cy.nodes().grabify();
    </script>
</body>
</html>
'''


class DAGConverter:
    """
    Converts Rappel IR into a DAG representation with function isolation.

    Each function is converted into an isolated subgraph with:
    - An "input" boundary node (receives inputs)
    - An "output" boundary node (produces outputs)
    - Internal nodes that only reference variables within the function

    Function calls create "fn_call" nodes that:
    - Connect to the calling function's data flow
    - Reference the called function (but don't merge subgraphs)

    Edge types:
    - State machine: Execution order edges (src, dst) means dst follows src
    - Data flow: Variable propagation edges (src, dst) with variable annotation

    Data flow rules:
    - Data flow edges only exist WITHIN a function
    - Cross-function data flow happens only through explicit inputs/outputs
    - Function call nodes receive inputs and produce outputs
    """

    def __init__(self):
        self.dag = DAG()
        self.node_counter = 0
        self.current_function: str | None = None  # Currently being converted
        self.function_defs: dict[str, RappelFunctionDef] = {}  # name -> def
        # Per-function variable tracking
        self.current_scope_vars: dict[str, str] = {}  # var_name -> defining node id
        self.var_modifications: dict[str, list[str]] = {}  # var_name -> list of modifying node ids

    def convert(self, program: RappelProgram) -> DAG:
        """Convert a Rappel program to a DAG with isolated function subgraphs."""
        self.dag = DAG()
        self.node_counter = 0
        self.function_defs = {}

        # First pass: collect all function definitions
        for stmt in program.statements:
            if isinstance(stmt, RappelFunctionDef):
                self.function_defs[stmt.name] = stmt

        # Second pass: convert each function into an isolated subgraph
        for fn_name, fn_def in self.function_defs.items():
            self._convert_function(fn_def)

        return self.dag

    def _convert_function(self, fn_def: RappelFunctionDef) -> None:
        """Convert a function definition into an isolated subgraph."""
        self.current_function = fn_def.name
        self.current_scope_vars = {}
        self.var_modifications = {}

        # Create input boundary node
        input_id = self._next_id(f"{fn_def.name}_input")
        input_label = f"input: [{', '.join(fn_def.inputs)}]" if fn_def.inputs else "input: []"
        input_node = DAGNode(
            id=input_id,
            node_type="input",
            ir_node=None,
            label=input_label,
            function_name=fn_def.name,
            is_input=True,
            io_vars=fn_def.inputs
        )
        self.dag.add_node(input_node)

        # Track input variables as defined at the input node
        for var in fn_def.inputs:
            self._track_var_definition(var, input_id)

        # Convert function body
        prev_node_id = input_id
        for stmt in fn_def.body:
            node_ids = self._convert_statement(stmt)

            if prev_node_id and node_ids:
                self.dag.add_edge(DAGEdge(
                    source=prev_node_id,
                    target=node_ids[0],
                    edge_type=EdgeType.STATE_MACHINE
                ))

            if node_ids:
                prev_node_id = node_ids[-1]

        # Create output boundary node
        output_id = self._next_id(f"{fn_def.name}_output")
        output_label = f"output: [{', '.join(fn_def.outputs)}]"
        output_node = DAGNode(
            id=output_id,
            node_type="output",
            ir_node=None,
            label=output_label,
            function_name=fn_def.name,
            is_output=True,
            io_vars=fn_def.outputs
        )
        self.dag.add_node(output_node)

        # Connect last body node to output
        if prev_node_id:
            self.dag.add_edge(DAGEdge(
                source=prev_node_id,
                target=output_id,
                edge_type=EdgeType.STATE_MACHINE
            ))

        # Add data flow edges within this function
        self._add_data_flow_edges_for_function(fn_def.name)

        self.current_function = None

    def _next_id(self, prefix: str = "node") -> str:
        """Generate the next unique node ID."""
        self.node_counter += 1
        return f"{prefix}_{self.node_counter}"

    def _convert_statement(self, stmt: RappelStatement) -> list[str]:
        """
        Convert a statement to DAG node(s).
        Returns list of node IDs created (in order).
        """
        if isinstance(stmt, RappelAssignment):
            return self._convert_assignment(stmt)
        elif isinstance(stmt, RappelMultiAssignment):
            return self._convert_multi_assignment(stmt)
        elif isinstance(stmt, RappelFunctionDef):
            # Function definitions are handled separately in _convert_function
            return []
        elif isinstance(stmt, RappelForLoop):
            return self._convert_for_loop(stmt)
        elif isinstance(stmt, RappelIfStatement):
            return self._convert_if_statement(stmt)
        elif isinstance(stmt, RappelPythonBlock):
            return self._convert_python_block(stmt)
        elif isinstance(stmt, RappelReturn):
            return self._convert_return(stmt)
        elif isinstance(stmt, RappelExprStatement):
            return self._convert_expr_statement(stmt)
        elif isinstance(stmt, RappelSpreadAction):
            return self._convert_spread_action(stmt)
        else:
            return []

    def _convert_assignment(self, stmt: RappelAssignment) -> list[str]:
        """Convert an assignment statement."""
        # Check if RHS contains a spread that needs aggregation
        if self._contains_spread_action(stmt.value):
            return self._convert_spread_assignment(stmt)

        # Check if RHS is an action call
        if isinstance(stmt.value, RappelActionCall):
            node_id = self._next_id("action")
            label = f"@{stmt.value.action_name}() -> {stmt.target}"
        else:
            node_id = self._next_id("assign")
            label = f"{stmt.target} = ..."

        # Check if RHS is a function call
        if isinstance(stmt.value, RappelCall):
            return self._convert_fn_call_assignment(stmt)

        node = DAGNode(
            id=node_id,
            node_type="action_call" if isinstance(stmt.value, RappelActionCall) else "assignment",
            ir_node=stmt,
            label=label,
            function_name=self.current_function
        )
        self.dag.add_node(node)

        # Track variable definition
        self._track_var_definition(stmt.target, node_id)

        return [node_id]

    def _convert_spread_assignment(self, stmt: RappelAssignment) -> list[str]:
        """
        Convert an assignment with spread action to action + aggregator nodes.

        Spread is implemented as:
        1. Action node that processes items in parallel
        2. Aggregator node that collects results
        """
        # Find the action call in the spread
        action_call = self._find_action_in_expr(stmt.value)

        # Create action node
        action_id = self._next_id("spread_action")
        action_label = f"@{action_call.action_name}() [spread]" if action_call else "spread_action"
        action_node = DAGNode(
            id=action_id,
            node_type="action_call",
            ir_node=stmt,
            label=action_label,
            function_name=self.current_function
        )
        self.dag.add_node(action_node)

        # Create aggregator node
        agg_id = self._next_id("aggregator")
        agg_node = DAGNode(
            id=agg_id,
            node_type="aggregator",
            ir_node=None,
            label=f"aggregate -> {stmt.target}",
            function_name=self.current_function,
            is_aggregator=True,
            aggregates_from=action_id
        )
        self.dag.add_node(agg_node)

        # Connect action to aggregator
        self.dag.add_edge(DAGEdge(
            source=action_id,
            target=agg_id,
            edge_type=EdgeType.STATE_MACHINE
        ))

        # Track variable definition at aggregator
        self._track_var_definition(stmt.target, agg_id)

        return [action_id, agg_id]

    def _convert_multi_assignment(self, stmt: RappelMultiAssignment) -> list[str]:
        """Convert a multi-assignment (unpacking)."""
        # Check if RHS is a function call
        if isinstance(stmt.value, RappelCall):
            return self._convert_fn_call_multi_assignment(stmt)

        node_id = self._next_id("multi_assign")
        targets_str = ", ".join(stmt.targets)
        node = DAGNode(
            id=node_id,
            node_type="assignment",
            ir_node=stmt,
            label=f"{targets_str} = ...",
            function_name=self.current_function
        )
        self.dag.add_node(node)

        # Track all variable definitions
        for target in stmt.targets:
            self._track_var_definition(target, node_id)

        return [node_id]

    def _convert_fn_call_assignment(self, stmt: RappelAssignment) -> list[str]:
        """Convert a function call assignment: x = fn_name(kwargs)"""
        call = stmt.value
        assert isinstance(call, RappelCall)

        node_id = self._next_id("fn_call")
        node = DAGNode(
            id=node_id,
            node_type="fn_call",
            ir_node=stmt,
            label=f"{call.target}() -> {stmt.target}",
            function_name=self.current_function,
            is_fn_call=True,
            called_function=call.target
        )
        self.dag.add_node(node)

        # Track variable definition
        self._track_var_definition(stmt.target, node_id)

        return [node_id]

    def _convert_fn_call_multi_assignment(self, stmt: RappelMultiAssignment) -> list[str]:
        """Convert a function call multi-assignment: x, y = fn_name(kwargs)"""
        call = stmt.value
        assert isinstance(call, RappelCall)

        node_id = self._next_id("fn_call")
        targets_str = ", ".join(stmt.targets)
        node = DAGNode(
            id=node_id,
            node_type="fn_call",
            ir_node=stmt,
            label=f"{call.target}() -> {targets_str}",
            function_name=self.current_function,
            is_fn_call=True,
            called_function=call.target
        )
        self.dag.add_node(node)

        # Track all variable definitions
        for target in stmt.targets:
            self._track_var_definition(target, node_id)

        return [node_id]

    def _convert_for_loop(self, stmt: RappelForLoop) -> list[str]:
        """
        Convert a for loop.

        For loops become a single node with:
        - "continue" edge to the first node in the body
        - "done" edge after all iterations complete

        Variables needed by the loop body are passed TO the for loop head.
        """
        # Create loop head node
        loop_id = self._next_id("for_loop")
        loop_node = DAGNode(
            id=loop_id,
            node_type="for_loop",
            ir_node=stmt,
            label=f"for {stmt.loop_var} in ...",
            function_name=self.current_function,
            is_loop_head=True,
            loop_var=stmt.loop_var
        )
        self.dag.add_node(loop_node)

        # Track loop variable
        self._track_var_definition(stmt.loop_var, loop_id)

        # Track output variables from the loop body assignment
        # Body contains exactly one assignment with a function call
        if stmt.body and isinstance(stmt.body[0], RappelAssignment):
            body_assignment = stmt.body[0]
            self._track_var_definition(body_assignment.target, loop_id)

        # The loop is a single node - body is handled at runtime
        # State machine edges for "continue" and "done" will be added
        # when we know what follows

        return [loop_id]

    def _convert_if_statement(self, stmt: RappelIfStatement) -> list[str]:
        """
        Convert an if statement.

        Creates:
        - Condition node
        - Then branch nodes
        - Else branch nodes (if present)
        - Join node
        """
        # Create condition node
        cond_id = self._next_id("if_cond")
        cond_node = DAGNode(
            id=cond_id,
            node_type="if",
            ir_node=stmt,
            label="if ...",
            function_name=self.current_function
        )
        self.dag.add_node(cond_node)

        result_nodes = [cond_id]
        then_last: str | None = None
        else_last: str | None = None

        # Process then branch
        prev_id = cond_id
        for body_stmt in stmt.then_body:
            node_ids = self._convert_statement(body_stmt)
            if node_ids:
                # Connect first node to previous
                self.dag.add_edge(DAGEdge(
                    source=prev_id,
                    target=node_ids[0],
                    edge_type=EdgeType.STATE_MACHINE,
                    condition="then" if prev_id == cond_id else None
                ))
                prev_id = node_ids[-1]
                result_nodes.extend(node_ids)
        then_last = prev_id if prev_id != cond_id else None

        # Process else branch
        if stmt.else_body:
            prev_id = cond_id
            for body_stmt in stmt.else_body:
                node_ids = self._convert_statement(body_stmt)
                if node_ids:
                    self.dag.add_edge(DAGEdge(
                        source=prev_id,
                        target=node_ids[0],
                        edge_type=EdgeType.STATE_MACHINE,
                        condition="else" if prev_id == cond_id else None
                    ))
                    prev_id = node_ids[-1]
                    result_nodes.extend(node_ids)
            else_last = prev_id if prev_id != cond_id else None

        # Create join node if we have branches
        if then_last or else_last:
            join_id = self._next_id("if_join")
            join_node = DAGNode(
                id=join_id,
                node_type="join",
                ir_node=None,
                label="join",
                function_name=self.current_function
            )
            self.dag.add_node(join_node)
            result_nodes.append(join_id)

            if then_last:
                self.dag.add_edge(DAGEdge(
                    source=then_last,
                    target=join_id,
                    edge_type=EdgeType.STATE_MACHINE
                ))
            else:
                # Empty then branch
                self.dag.add_edge(DAGEdge(
                    source=cond_id,
                    target=join_id,
                    edge_type=EdgeType.STATE_MACHINE,
                    condition="then"
                ))

            if else_last:
                self.dag.add_edge(DAGEdge(
                    source=else_last,
                    target=join_id,
                    edge_type=EdgeType.STATE_MACHINE
                ))
            elif stmt.else_body:
                # Empty else branch
                self.dag.add_edge(DAGEdge(
                    source=cond_id,
                    target=join_id,
                    edge_type=EdgeType.STATE_MACHINE,
                    condition="else"
                ))

        return result_nodes

    def _convert_python_block(self, stmt: RappelPythonBlock) -> list[str]:
        """Convert a python block."""
        node_id = self._next_id("python")
        outputs_str = ", ".join(stmt.outputs)
        node = DAGNode(
            id=node_id,
            node_type="python_block",
            ir_node=stmt,
            label=f"python -> {outputs_str}",
            function_name=self.current_function
        )
        self.dag.add_node(node)

        # Track output variables
        for output_var in stmt.outputs:
            self._track_var_definition(output_var, node_id)

        return [node_id]

    def _convert_return(self, stmt: RappelReturn) -> list[str]:
        """Convert a return statement."""
        node_id = self._next_id("return")
        node = DAGNode(
            id=node_id,
            node_type="return",
            ir_node=stmt,
            label="return",
            function_name=self.current_function
        )
        self.dag.add_node(node)

        return [node_id]

    def _convert_expr_statement(self, stmt: RappelExprStatement) -> list[str]:
        """Convert an expression statement (usually an action call)."""
        if isinstance(stmt.expr, RappelActionCall):
            node_id = self._next_id("action")
            node = DAGNode(
                id=node_id,
                node_type="action_call",
                ir_node=stmt,
                label=f"@{stmt.expr.action_name}()",
                function_name=self.current_function
            )
        else:
            node_id = self._next_id("expr")
            node = DAGNode(
                id=node_id,
                node_type="expression",
                ir_node=stmt,
                label="expr",
                function_name=self.current_function
            )
        self.dag.add_node(node)

        return [node_id]

    def _convert_spread_action(self, stmt: RappelSpreadAction) -> list[str]:
        """
        Convert a spread action to action + aggregator nodes.

        spread items:item -> @fetch_details(id=item)

        Creates:
        1. Spread action node (launches parallel actions)
        2. Aggregator node (collects results)
        """
        # Create spread action node
        action_id = self._next_id("spread_action")
        action_label = f"@{stmt.action.action_name}() [spread over {stmt.item_var}]"
        action_node = DAGNode(
            id=action_id,
            node_type="action_call",
            ir_node=stmt,
            label=action_label,
            function_name=self.current_function
        )
        self.dag.add_node(action_node)

        # Create aggregator node
        agg_id = self._next_id("aggregator")
        target_label = f" -> {stmt.target}" if stmt.target else ""
        agg_node = DAGNode(
            id=agg_id,
            node_type="aggregator",
            ir_node=None,
            label=f"aggregate{target_label}",
            function_name=self.current_function,
            is_aggregator=True,
            aggregates_from=action_id
        )
        self.dag.add_node(agg_node)

        # Connect action to aggregator
        self.dag.add_edge(DAGEdge(
            source=action_id,
            target=agg_id,
            edge_type=EdgeType.STATE_MACHINE
        ))

        # Track variable definition at aggregator (if target is specified)
        if stmt.target:
            self._track_var_definition(stmt.target, agg_id)

        return [action_id, agg_id]

    def _track_var_definition(self, var_name: str, node_id: str) -> None:
        """Track that a variable is defined/modified at a node."""
        self.current_scope_vars[var_name] = node_id
        if var_name not in self.var_modifications:
            self.var_modifications[var_name] = []
        self.var_modifications[var_name].append(node_id)

    def _add_data_flow_edges_for_function(self, function_name: str) -> None:
        """
        Add data flow edges for a specific function.

        Data flow edges only exist WITHIN a function - cross-function
        data flow happens through explicit inputs/outputs.

        Rule: Push data only to the most recent trailing node that
        DOESN'T modify the variable.
        """
        # Get only nodes for this function
        fn_nodes = self.dag.get_nodes_for_function(function_name)

        for node_id, node in fn_nodes.items():
            if node.ir_node is None and not node.is_input:
                continue

            # Get variables used by this node
            if node.ir_node:
                used_vars = self._get_used_variables(node.ir_node)
            else:
                used_vars = set()

            for var_name in used_vars:
                # Find the most recent definition of this variable
                # that comes BEFORE this node in execution order
                source_node = self._find_var_source(var_name, node_id, function_name)
                if source_node and source_node != node_id:
                    self.dag.add_edge(DAGEdge(
                        source=source_node,
                        target=node_id,
                        edge_type=EdgeType.DATA_FLOW,
                        variable=var_name
                    ))

    def _find_var_source(self, var_name: str, target_node_id: str, function_name: str | None = None) -> str | None:
        """
        Find the source node that should provide a variable's value.

        Returns the most recent node that defines the variable and
        comes before target_node_id in execution order.
        Only considers nodes within the same function.
        """
        if var_name not in self.var_modifications:
            return None

        modifications = self.var_modifications[var_name]

        # Get topological order for this function's nodes only
        if function_name:
            fn_nodes = set(self.dag.get_nodes_for_function(function_name).keys())
            order = self._get_execution_order_for_nodes(fn_nodes)
        else:
            order = self._get_execution_order_for_nodes(set(self.dag.nodes.keys()))

        if target_node_id not in order:
            return modifications[-1] if modifications else None

        target_idx = order.index(target_node_id)

        # Find the most recent modification before target
        best_source = None
        best_idx = -1

        for mod_node in modifications:
            if mod_node in order:
                mod_idx = order.index(mod_node)
                if mod_idx < target_idx and mod_idx > best_idx:
                    best_idx = mod_idx
                    best_source = mod_node

        return best_source

    def _get_execution_order_for_nodes(self, node_ids: set[str]) -> list[str]:
        """Get nodes in topological (execution) order for a subset of nodes."""
        # Simple topological sort using state machine edges
        in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
        adj: dict[str, list[str]] = {nid: [] for nid in node_ids}

        for edge in self.dag.get_state_machine_edges():
            if edge.source in node_ids and edge.target in node_ids:
                adj[edge.source].append(edge.target)
                in_degree[edge.target] += 1

        # Kahn's algorithm
        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        order = []

        while queue:
            node = queue.pop(0)
            order.append(node)
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order

    def _get_used_variables(self, node: RappelStatement | RappelExpr) -> set[str]:
        """Extract variable names used (read) by a node."""
        used = set()
        self._collect_used_vars(node, used)
        return used

    def _collect_used_vars(self, node, used: set[str]) -> None:
        """Recursively collect used variables."""
        if isinstance(node, RappelVariable):
            used.add(node.name)
        elif isinstance(node, RappelAssignment):
            self._collect_used_vars(node.value, used)
        elif isinstance(node, RappelMultiAssignment):
            self._collect_used_vars(node.value, used)
        elif isinstance(node, RappelBinaryOp):
            self._collect_used_vars(node.left, used)
            self._collect_used_vars(node.right, used)
        elif isinstance(node, RappelUnaryOp):
            self._collect_used_vars(node.operand, used)
        elif isinstance(node, RappelIndexAccess):
            self._collect_used_vars(node.target, used)
            self._collect_used_vars(node.index, used)
        elif isinstance(node, RappelDotAccess):
            self._collect_used_vars(node.target, used)
        elif isinstance(node, RappelCall):
            for _, arg in node.kwargs:
                self._collect_used_vars(arg, used)
        elif isinstance(node, RappelActionCall):
            for _, arg in node.kwargs:
                self._collect_used_vars(arg, used)
        elif isinstance(node, RappelListExpr):
            for item in node.items:
                self._collect_used_vars(item, used)
        elif isinstance(node, RappelDictExpr):
            for k, v in node.pairs:
                self._collect_used_vars(k, used)
                self._collect_used_vars(v, used)
        elif isinstance(node, RappelSpread):
            self._collect_used_vars(node.target, used)
        elif isinstance(node, RappelIfStatement):
            self._collect_used_vars(node.condition, used)
        elif isinstance(node, RappelForLoop):
            self._collect_used_vars(node.iterable, used)
        elif isinstance(node, RappelReturn):
            for val in node.values:
                self._collect_used_vars(val, used)
        elif isinstance(node, RappelExprStatement):
            self._collect_used_vars(node.expr, used)
        elif isinstance(node, RappelSpreadAction):
            self._collect_used_vars(node.source_list, used)
            # Also collect vars from the action kwargs (excluding the item_var)
            for _, arg in node.action.kwargs:
                self._collect_used_vars(arg, used)
            # Remove the item_var since it's defined by the spread, not used
            used.discard(node.item_var)

    def _contains_spread_action(self, expr: RappelExpr) -> bool:
        """Check if expression contains a spread with an action."""
        if isinstance(expr, RappelSpread):
            return True
        elif isinstance(expr, RappelActionCall):
            # Check if any arg contains spread
            for _, arg in expr.kwargs:
                if self._contains_spread_action(arg):
                    return True
        elif isinstance(expr, RappelBinaryOp):
            return self._contains_spread_action(expr.left) or self._contains_spread_action(expr.right)
        elif isinstance(expr, RappelListExpr):
            return any(self._contains_spread_action(item) for item in expr.items)
        return False

    def _find_action_in_expr(self, expr: RappelExpr) -> RappelActionCall | None:
        """Find an action call in an expression."""
        if isinstance(expr, RappelActionCall):
            return expr
        elif isinstance(expr, RappelBinaryOp):
            left = self._find_action_in_expr(expr.left)
            if left:
                return left
            return self._find_action_in_expr(expr.right)
        elif isinstance(expr, RappelListExpr):
            for item in expr.items:
                found = self._find_action_in_expr(item)
                if found:
                    return found
        return None


def convert_to_dag(program: RappelProgram) -> DAG:
    """Convert a Rappel program to a DAG."""
    converter = DAGConverter()
    return converter.convert(program)
