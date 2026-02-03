"""Conditional conversion helpers."""

from __future__ import annotations

import copy
from typing import List, Optional, Sequence

from proto import ast_pb2 as ir

from ..models import ConvertedSubgraph, DagConversionError, DAGEdge
from ..nodes import BranchNode, JoinNode


class ConditionalConversionMixin:
    """Convert conditional blocks into branch/join nodes."""

    def convert_conditional(self, cond: ir.Conditional) -> ConvertedSubgraph:
        """Convert an if/elif/else tree into a branch + optional join graph."""
        nodes: List[str] = []

        branch_id = self.next_id("branch")
        branch_node = BranchNode(
            id=branch_id,
            description="branch",
            function_name=self.current_function,
        )
        self.dag.add_node(branch_node)
        nodes.append(branch_id)

        if not cond.HasField("if_branch"):
            raise DagConversionError("conditional missing if_branch")
        if_branch = cond.if_branch
        if not if_branch.HasField("condition"):
            raise DagConversionError("if branch missing guard expression")
        if_guard = copy.deepcopy(if_branch.condition)

        if_body_graph = (
            self.convert_block(if_branch.block_body)
            if if_branch.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        nodes.extend(if_body_graph.nodes)

        prior_guards = [if_guard]
        elif_graphs: List[tuple[ir.Expr, ConvertedSubgraph]] = []

        for elif_branch in cond.elif_branches:
            if not elif_branch.HasField("condition"):
                raise DagConversionError("elif branch missing guard expression")
            elif_cond = copy.deepcopy(elif_branch.condition)
            compound_guard = self.build_compound_guard(prior_guards, elif_cond)
            prior_guards.append(elif_cond)

            graph = (
                self.convert_block(elif_branch.block_body)
                if elif_branch.HasField("block_body")
                else ConvertedSubgraph.noop()
            )
            nodes.extend(graph.nodes)
            elif_graphs.append((compound_guard, graph))

        else_graph = (
            self.convert_block(cond.else_branch.block_body)
            if cond.HasField("else_branch") and cond.else_branch.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        nodes.extend(else_graph.nodes)

        join_needed = (
            if_body_graph.is_noop
            or bool(if_body_graph.exits)
            or any(graph.is_noop or graph.exits for _, graph in elif_graphs)
            or else_graph.is_noop
            or bool(else_graph.exits)
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

        def connect_guarded_branch(
            guard: ir.Expr, graph: ConvertedSubgraph, join_target: Optional[str]
        ) -> None:
            if graph.is_noop:
                if join_target:
                    self.dag.add_edge(
                        DAGEdge.state_machine_with_guard(branch_id, join_target, guard)
                    )
                return

            if graph.entry:
                self.dag.add_edge(DAGEdge.state_machine_with_guard(branch_id, graph.entry, guard))

            if join_target:
                for exit_node in graph.exits:
                    self.dag.add_edge(DAGEdge.state_machine(exit_node, join_target))

        def connect_else_branch(graph: ConvertedSubgraph, join_target: Optional[str]) -> None:
            if graph.is_noop:
                if join_target:
                    self.dag.add_edge(DAGEdge.state_machine_else(branch_id, join_target))
                return

            if graph.entry:
                self.dag.add_edge(DAGEdge.state_machine_else(branch_id, graph.entry))

            if join_target:
                for exit_node in graph.exits:
                    self.dag.add_edge(DAGEdge.state_machine(exit_node, join_target))

        connect_guarded_branch(if_guard, if_body_graph, join_id)
        for guard, graph in elif_graphs:
            connect_guarded_branch(guard, graph, join_id)
        connect_else_branch(else_graph, join_id)

        return ConvertedSubgraph(
            entry=branch_id,
            exits=[join_id] if join_id else [],
            nodes=nodes,
            is_noop=False,
        )

    def build_compound_guard(
        self, prior_guards: Sequence[ir.Expr], current_condition: Optional[ir.Expr]
    ) -> ir.Expr:
        """Build a guard for elif branches: not prior_guards and current_condition."""
        parts: List[ir.Expr] = []
        for guard in prior_guards:
            parts.append(
                ir.Expr(
                    unary_op=ir.UnaryOp(
                        op=ir.UnaryOperator.UNARY_OP_NOT,
                        operand=copy.deepcopy(guard),
                    )
                )
            )

        if current_condition is not None:
            parts.append(copy.deepcopy(current_condition))

        if not parts:
            raise DagConversionError(
                "build_compound_guard called with no prior conditions and no current condition"
            )
        if len(parts) == 1:
            return parts[0]

        result = parts.pop(0)
        for part in parts:
            result = ir.Expr(
                binary_op=ir.BinaryOp(
                    left=copy.deepcopy(result),
                    op=ir.BinaryOperator.BINARY_OP_AND,
                    right=copy.deepcopy(part),
                )
            )
        return result
