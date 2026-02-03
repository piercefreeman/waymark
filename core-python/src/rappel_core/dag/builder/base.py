"""IR -> DAG conversion and graph builder."""

from __future__ import annotations

import copy
from typing import Dict, List, Optional

from proto import ast_pb2 as ir

from ..models import DAG, ConvertedSubgraph, DAGEdge, assert_never
from ..nodes import InputNode, OutputNode, ReturnNode
from ..validate import validate_dag
from .assignments import AssignmentConversionMixin
from .conditionals import ConditionalConversionMixin
from .data_flow import DataFlowMixin
from .exceptions import ExceptionConversionMixin
from .expansion import ExpansionMixin
from .loops import LoopConversionMixin
from .spreads import SpreadConversionMixin
from .utils import BuilderUtilsMixin


class DAGConverter(
    AssignmentConversionMixin,
    SpreadConversionMixin,
    LoopConversionMixin,
    ConditionalConversionMixin,
    ExceptionConversionMixin,
    DataFlowMixin,
    ExpansionMixin,
    BuilderUtilsMixin,
):
    """Convert IR programs into a DAG with control + data-flow edges.

    Design overview:
    - Each IR statement becomes one or more DAG nodes (assignments, calls, joins, etc).
    - State-machine edges encode control flow, including branches, loops, and exceptions.
    - Data-flow edges link variable definitions to later uses so scheduling and replay
      can trace dependencies.
    - Function calls are expanded by cloning callee nodes with a stable prefix, then
      wiring caller -> callee arguments via synthetic assignments.

    Example IR:
    - results = parallel: @a() @b()
    Yields a parallel node, two call nodes, and a join/aggregator node connected by
    control edges, plus data-flow edges from each call's result into the aggregator.
    """

    def __init__(self) -> None:
        self.dag = DAG()
        self.node_counter = 0
        self.current_function: Optional[str] = None
        self.function_defs: Dict[str, ir.FunctionDef] = {}
        self.current_scope_vars: Dict[str, str] = {}
        self.var_modifications: Dict[str, List[str]] = {}
        self.loop_exit_stack: List[str] = []
        self.loop_incr_stack: List[str] = []
        self.try_depth = 0

    def convert(self, program: ir.Program) -> DAG:
        """Convert a full IR program into a flattened, executable DAG.

        This chooses an entry function, expands all reachable function calls,
        remaps exception edges to expanded call entries, adds global data-flow
        edges, and validates the resulting graph.

        Example entry selection:
        - If a function named "main" exists, it is used as the entry point.
        - Otherwise, the first non-dunder function becomes the entry.
        """
        unexpanded = self.convert_with_pointers(program)

        entry_fn = None
        for func in program.functions:
            if func.name == "main":
                entry_fn = func.name
                break
        if entry_fn is None:
            for func in program.functions:
                if not func.name.startswith("__"):
                    entry_fn = func.name
                    break
        if entry_fn is None and program.functions:
            entry_fn = program.functions[0].name
        if entry_fn is None:
            entry_fn = "main"

        dag = self.expand_functions(unexpanded, entry_fn)
        self.remap_exception_targets(dag)
        self.add_global_data_flow_edges(dag)
        validate_dag(dag)
        return dag

    def convert_with_pointers(self, program: ir.Program) -> DAG:
        """Convert each function into its own DAG fragment without inlining calls.

        The resulting graph preserves per-function node ids and keeps function
        calls as call nodes. This is primarily used as the input to
        expand_functions, which will inline and prefix the callee graphs.
        """
        self.dag = DAG()
        self.node_counter = 0
        self.function_defs.clear()

        for func in program.functions:
            self.function_defs[func.name] = copy.deepcopy(func)

        for func in program.functions:
            self.convert_function(func)

        dag = self.dag
        self.dag = DAG()
        return dag

    def convert_function(self, fn_def: ir.FunctionDef) -> None:
        """Convert a single function body into a DAG fragment.

        This creates input/output nodes, converts each statement in order, and
        adds per-function data-flow edges based on variable definitions.

        Example IR:
        - def main(x): y = x + 1; return y
        Produces input -> assignment -> return -> output with x/y data edges.
        """
        self.current_function = fn_def.name
        self.current_scope_vars.clear()
        self.var_modifications.clear()

        input_id = self.next_id(f"{fn_def.name}_input")
        input_node = InputNode(
            id=input_id,
            io_vars=list(fn_def.io.inputs),
            function_name=fn_def.name,
        )
        self.dag.add_node(input_node)

        for var in fn_def.io.inputs:
            self.track_var_definition(var, input_id)

        frontier = [input_id]
        for stmt in fn_def.body.statements:
            converted = self.convert_statement(stmt)
            if converted.is_noop:
                continue
            if converted.entry:
                for prev in frontier:
                    self.dag.add_edge(DAGEdge.state_machine(prev, converted.entry))
            frontier = converted.exits

        output_id = self.next_id(f"{fn_def.name}_output")
        output_node = OutputNode(
            id=output_id,
            io_vars=list(fn_def.io.outputs),
            function_name=fn_def.name,
        )
        self.dag.add_node(output_node)

        for prev in frontier:
            self.dag.add_edge(DAGEdge.state_machine(prev, output_id))

        return_nodes = [
            node_id
            for node_id, node in self.dag.nodes.items()
            if node.node_type == "return" and node.function_name == fn_def.name
        ]

        for return_id in return_nodes:
            already_connected = any(
                edge.source == return_id and edge.target == output_id for edge in self.dag.edges
            )
            if not already_connected:
                self.dag.add_edge(DAGEdge.state_machine(return_id, output_id))

        self.add_data_flow_edges_for_function(fn_def.name)
        self.current_function = None

    def next_id(self, prefix: str) -> str:
        """Generate a stable node id for the current conversion session."""
        self.node_counter += 1
        return f"{prefix}_{self.node_counter}"

    def convert_block(self, block: ir.Block) -> ConvertedSubgraph:
        """Convert a block into a connected subgraph.

        This stitches statement graphs together with state-machine edges and
        returns entry/exits for the caller to connect.
        """
        nodes: List[str] = []
        entry: Optional[str] = None
        frontier: Optional[List[str]] = None

        for stmt in block.statements:
            converted = self.convert_statement(stmt)
            nodes.extend(converted.nodes)

            if converted.is_noop:
                continue

            if entry is None:
                entry = converted.entry

            if frontier is not None and converted.entry is not None:
                for prev in frontier:
                    self.dag.add_edge(DAGEdge.state_machine(prev, converted.entry))

            frontier = converted.exits

        if entry is None:
            return ConvertedSubgraph.noop()

        return ConvertedSubgraph(
            entry=entry,
            exits=frontier or [],
            nodes=nodes,
            is_noop=False,
        )

    def convert_statement(self, stmt: ir.Statement) -> ConvertedSubgraph:
        """Convert a single statement into a subgraph with entry/exit nodes."""
        kind = stmt.WhichOneof("kind")
        match kind:
            case "assignment":
                node_ids = self.convert_assignment(stmt.assignment)
            case "action_call":
                node_ids = self.convert_action_call_with_targets(stmt.action_call, [])
            case "spread_action":
                node_ids = self.convert_spread_action(stmt.spread_action)
            case "parallel_block":
                node_ids = self.convert_parallel_block(stmt.parallel_block)
            case "for_loop":
                return self.convert_for_loop(stmt.for_loop)
            case "while_loop":
                return self.convert_while_loop(stmt.while_loop)
            case "conditional":
                return self.convert_conditional(stmt.conditional)
            case "try_except":
                return self.convert_try_except(stmt.try_except)
            case "return_stmt":
                node_ids = self.convert_return(stmt.return_stmt)
                return ConvertedSubgraph(
                    entry=node_ids[0] if node_ids else None,
                    exits=[],
                    nodes=node_ids,
                    is_noop=False,
                )
            case "break_stmt":
                return self.convert_break()
            case "continue_stmt":
                return self.convert_continue()
            case "expr_stmt":
                node_ids = self.convert_expr_statement(stmt.expr_stmt)
            case None:
                return ConvertedSubgraph.noop()
            case _:
                assert_never(kind)

        if not node_ids:
            return ConvertedSubgraph.noop()

        return ConvertedSubgraph(
            entry=node_ids[0],
            exits=[node_ids[-1]],
            nodes=node_ids,
            is_noop=False,
        )

    def convert_return(self, ret: ir.ReturnStmt) -> list[str]:
        """Convert a return statement into a ReturnNode."""
        node_id = self.next_id("return")
        node = ReturnNode(
            id=node_id,
            function_name=self.current_function,
        )

        if ret.HasField("value"):
            node.assign_expr = copy.deepcopy(ret.value)
            node.target = "result"

        has_target = node.target is not None
        self.dag.add_node(node)

        if has_target:
            self.track_var_definition("result", node_id)

        return [node_id]


def convert_to_dag(program: ir.Program) -> DAG:
    return DAGConverter().convert(program)
