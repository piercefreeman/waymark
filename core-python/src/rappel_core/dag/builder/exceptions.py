"""Exception flow conversion helpers."""

from __future__ import annotations

import copy
from typing import List, Optional

from proto import ast_pb2 as ir

from ..models import ConvertedSubgraph, EXCEPTION_SCOPE_VAR, DAGEdge
from ..nodes import AssignmentNode, JoinNode


class ExceptionConversionMixin:
    """Convert try/except blocks into exception-aware DAG edges."""

    def convert_try_except(self, try_except: ir.TryExcept) -> ConvertedSubgraph:
        """Convert try/except blocks into exception-aware edges and optional join."""
        self.try_depth += 1
        current_depth = self.try_depth
        try:
            try_graph = (
                self.convert_block(try_except.try_block)
                if try_except.HasField("try_block")
                else ConvertedSubgraph.noop()
            )
            if try_graph.is_noop:
                return ConvertedSubgraph.noop()

            nodes = list(try_graph.nodes)
            handler_graphs: List[tuple[List[str], ConvertedSubgraph]] = []

            for handler in try_except.handlers:
                graph = (
                    self.convert_block(handler.block_body)
                    if handler.HasField("block_body")
                    else ConvertedSubgraph.noop()
                )
                if handler.exception_var:
                    graph = self.prepend_exception_binding(handler.exception_var, graph)
                nodes.extend(graph.nodes)
                handler_graphs.append((list(handler.exception_types), graph))

            join_needed = bool(try_graph.exits) or any(
                graph.is_noop or graph.exits for _, graph in handler_graphs
            )

            join_id: Optional[str] = None
            if join_needed:
                join_id = self.next_id("join")
                join_node = JoinNode(
                    id=join_id,
                    description="join",
                    function_name=self.current_function,
                )
                self.dag.add_node(join_node)
                nodes.append(join_id)

            if join_id:
                for try_exit in try_graph.exits:
                    self.dag.add_edge(DAGEdge.state_machine_success(try_exit, join_id))

            try_exception_sources = list(try_graph.nodes)
            for exception_types, handler_graph in handler_graphs:
                if handler_graph.is_noop:
                    if join_id:
                        for source in try_exception_sources:
                            edge = DAGEdge.state_machine_with_exception(
                                source, join_id, list(exception_types)
                            )
                            edge.exception_depth = current_depth
                            self.dag.add_edge(edge)
                    continue

                if handler_graph.entry:
                    for source in try_exception_sources:
                        edge = DAGEdge.state_machine_with_exception(
                            source, handler_graph.entry, list(exception_types)
                        )
                        edge.exception_depth = current_depth
                        self.dag.add_edge(edge)

                if join_id:
                    for handler_exit in handler_graph.exits:
                        self.dag.add_edge(DAGEdge.state_machine(handler_exit, join_id))

            return ConvertedSubgraph(
                entry=try_graph.entry,
                exits=[join_id] if join_id else [],
                nodes=nodes,
                is_noop=False,
            )
        finally:
            self.try_depth = max(0, self.try_depth - 1)

    def prepend_exception_binding(
        self, exception_var: str, graph: ConvertedSubgraph
    ) -> ConvertedSubgraph:
        """Insert an exception binding node before a handler graph.

        Example:
        - except Exception as err: ...
        Inserts "err = __exception__" before the handler body.
        """
        binding_id = self.next_id("exc_bind")
        label = f"{exception_var} = {EXCEPTION_SCOPE_VAR}"
        assign_expr = ir.Expr(variable=ir.Variable(name=EXCEPTION_SCOPE_VAR))
        node = AssignmentNode(
            id=binding_id,
            targets=[exception_var],
            assign_expr=assign_expr,
            label_hint=label,
            function_name=self.current_function,
        )
        self.dag.add_node(node)
        self.track_var_definition(exception_var, binding_id)

        if graph.entry:
            self.dag.add_edge(DAGEdge.state_machine(binding_id, graph.entry))

        nodes = [binding_id, *graph.nodes]
        exits = [binding_id] if graph.is_noop else list(graph.exits)

        return ConvertedSubgraph(entry=binding_id, exits=exits, nodes=nodes, is_noop=False)
