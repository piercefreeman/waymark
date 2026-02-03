"""Loop conversion helpers."""

from __future__ import annotations

import copy
from typing import List, Optional

from proto import ast_pb2 as ir

from ..models import ConvertedSubgraph, DagConversionError, DAGEdge
from ..nodes import AssignmentNode, BranchNode, BreakNode, ContinueNode, JoinNode


class LoopConversionMixin:
    """Convert loop constructs into explicit DAG nodes."""

    @staticmethod
    def build_loop_guard(loop_i_var: str, collection: Optional[ir.Expr]) -> Optional[ir.Expr]:
        """Build a guard expression for loop continuation based on collection length.

        Example:
        - loop_i_var="__loop_1_i", collection=items
        - guard becomes "__loop_1_i < len(items)"
        """
        if collection is None:
            return None
        return ir.Expr(
            binary_op=ir.BinaryOp(
                left=ir.Expr(variable=ir.Variable(name=loop_i_var)),
                op=ir.BinaryOperator.BINARY_OP_LT,
                right=ir.Expr(
                    function_call=ir.FunctionCall(
                        name="len",
                        args=[],
                        kwargs=[ir.Kwarg(name="items", value=copy.deepcopy(collection))],
                        global_function=ir.GlobalFunction.GLOBAL_FUNCTION_LEN,
                    )
                ),
            )
        )

    def convert_for_loop(self, for_loop: ir.ForLoop) -> "ConvertedSubgraph":
        """Convert a for-loop into explicit loop init/cond/body/incr/exit nodes.

        Example IR:
        - for item in items: @work(item)
        Expands into init (i = 0), cond, extract, body, incr, and loop join.
        """
        nodes: List[str] = []

        loop_id = self.next_id("loop")
        loop_vars_str = ", ".join(for_loop.loop_vars)
        collection_expr = (
            copy.deepcopy(for_loop.iterable) if for_loop.HasField("iterable") else None
        )
        collection_str = self.expr_to_string(collection_expr) if collection_expr else ""

        loop_i_var = f"__loop_{loop_id}_i"
        continue_guard = self.build_loop_guard(loop_i_var, collection_expr)
        break_guard = (
            ir.Expr(
                unary_op=ir.UnaryOp(
                    op=ir.UnaryOperator.UNARY_OP_NOT,
                    operand=copy.deepcopy(continue_guard),
                )
            )
            if continue_guard is not None
            else None
        )

        init_id = self.next_id("loop_init")
        init_label = f"{loop_i_var} = 0"
        init_expr = ir.Expr(literal=ir.Literal(int_value=0))
        init_node = AssignmentNode(
            id=init_id,
            targets=[loop_i_var],
            assign_expr=init_expr,
            label_hint=init_label,
            function_name=self.current_function,
        )
        self.dag.add_node(init_node)
        self.track_var_definition(loop_i_var, init_id)
        nodes.append(init_id)

        cond_id = self.next_id("loop_cond")
        cond_label = f"for {loop_vars_str} in {collection_str}"
        cond_node = BranchNode(
            id=cond_id,
            description=cond_label,
            function_name=self.current_function,
        )
        self.dag.add_node(cond_node)
        nodes.append(cond_id)

        self.dag.add_edge(DAGEdge.state_machine(init_id, cond_id))

        extract_id = self.next_id("loop_extract")
        if collection_expr is None:
            raise DagConversionError(
                f"for-loop collection expression is None for loop '{loop_vars_str}'"
            )
        index_expr = ir.Expr(
            index=ir.IndexAccess(
                object=copy.deepcopy(collection_expr),
                index=ir.Expr(variable=ir.Variable(name=loop_i_var)),
            )
        )
        extract_label = f"{loop_vars_str} = {collection_str}[{loop_i_var}]"
        extract_node = AssignmentNode(
            id=extract_id,
            targets=list(for_loop.loop_vars),
            assign_expr=index_expr,
            label_hint=extract_label,
            function_name=self.current_function,
        )
        self.dag.add_node(extract_node)
        nodes.append(extract_id)

        for loop_var in for_loop.loop_vars:
            self.track_var_definition(loop_var, extract_id)

        if continue_guard is not None:
            self.dag.add_edge(DAGEdge.state_machine_with_guard(cond_id, extract_id, continue_guard))
        else:
            self.dag.add_edge(DAGEdge.state_machine(cond_id, extract_id))

        exit_id = self.next_id("loop_exit")
        self.loop_exit_stack.append(exit_id)
        incr_id = self.next_id("loop_incr")
        self.loop_incr_stack.append(incr_id)

        body_targets: List[str] = []
        body_graph = (
            self.convert_block(for_loop.block_body)
            if for_loop.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        if for_loop.HasField("block_body"):
            self.collect_assigned_targets(for_loop.block_body.statements, body_targets)

        self.loop_incr_stack.pop()
        self.loop_exit_stack.pop()
        nodes.extend(body_graph.nodes)

        if body_graph.is_noop:
            pass
        elif body_graph.entry:
            self.dag.add_edge(DAGEdge.state_machine(extract_id, body_graph.entry))

        incr_label = f"{loop_i_var} = {loop_i_var} + 1"
        incr_expr = ir.Expr(
            binary_op=ir.BinaryOp(
                left=ir.Expr(variable=ir.Variable(name=loop_i_var)),
                op=ir.BinaryOperator.BINARY_OP_ADD,
                right=ir.Expr(literal=ir.Literal(int_value=1)),
            )
        )
        incr_node = AssignmentNode(
            id=incr_id,
            targets=[loop_i_var],
            assign_expr=incr_expr,
            label_hint=incr_label,
            function_name=self.current_function,
        )
        self.dag.add_node(incr_node)
        self.track_var_definition(loop_i_var, incr_id)
        nodes.append(incr_id)

        if body_graph.is_noop:
            self.dag.add_edge(DAGEdge.state_machine(extract_id, incr_id))
        else:
            for exit_node in body_graph.exits:
                self.dag.add_edge(DAGEdge.state_machine(exit_node, incr_id))

        self.dag.add_edge(DAGEdge.state_machine(incr_id, cond_id).with_loop_back(True))

        exit_label = f"end for {loop_vars_str}"
        exit_node = JoinNode(
            id=exit_id,
            description=exit_label,
            function_name=self.current_function,
        )
        if body_targets:
            exit_node.targets = list(body_targets)
            for target in body_targets:
                self.track_var_definition(target, exit_id)
        self.dag.add_node(exit_node)
        nodes.append(exit_id)

        if break_guard is not None:
            self.dag.add_edge(DAGEdge.state_machine_with_guard(cond_id, exit_id, break_guard))
        else:
            self.dag.add_edge(DAGEdge.state_machine(cond_id, exit_id))

        return ConvertedSubgraph(
            entry=init_id,
            exits=[exit_id],
            nodes=nodes,
            is_noop=False,
        )

    def convert_while_loop(self, while_loop: ir.WhileLoop) -> "ConvertedSubgraph":
        """Convert a while-loop into explicit condition/body/continue/exit nodes."""
        nodes: List[str] = []

        if not while_loop.HasField("condition"):
            raise DagConversionError("while loop missing condition")
        condition = copy.deepcopy(while_loop.condition)
        condition_str = self.expr_to_string(condition)

        cond_id = self.next_id("loop_cond")
        cond_label = f"while {condition_str}"
        cond_node = BranchNode(
            id=cond_id,
            description=cond_label,
            function_name=self.current_function,
        )
        self.dag.add_node(cond_node)
        nodes.append(cond_id)

        continue_guard = copy.deepcopy(condition)
        break_guard = ir.Expr(
            unary_op=ir.UnaryOp(
                op=ir.UnaryOperator.UNARY_OP_NOT,
                operand=copy.deepcopy(condition),
            )
        )

        exit_id = self.next_id("loop_exit")
        self.loop_exit_stack.append(exit_id)
        continue_id = self.next_id("loop_continue")
        self.loop_incr_stack.append(continue_id)

        body_targets: List[str] = []
        body_graph = (
            self.convert_block(while_loop.block_body)
            if while_loop.HasField("block_body")
            else ConvertedSubgraph.noop()
        )
        if while_loop.HasField("block_body"):
            self.collect_assigned_targets(while_loop.block_body.statements, body_targets)

        self.loop_incr_stack.pop()
        self.loop_exit_stack.pop()
        nodes.extend(body_graph.nodes)

        continue_node = AssignmentNode(
            id=continue_id,
            targets=[],
            label_hint="loop_continue",
            function_name=self.current_function,
        )
        self.dag.add_node(continue_node)
        nodes.append(continue_id)

        if body_graph.is_noop:
            self.dag.add_edge(
                DAGEdge.state_machine_with_guard(cond_id, continue_id, continue_guard)
            )
        elif body_graph.entry:
            self.dag.add_edge(
                DAGEdge.state_machine_with_guard(cond_id, body_graph.entry, continue_guard)
            )

        if not body_graph.is_noop:
            for exit_node in body_graph.exits:
                self.dag.add_edge(DAGEdge.state_machine(exit_node, continue_id))

        self.dag.add_edge(DAGEdge.state_machine(continue_id, cond_id).with_loop_back(True))

        exit_label = f"end while {condition_str}"
        exit_node = JoinNode(
            id=exit_id,
            description=exit_label,
            function_name=self.current_function,
        )
        if body_targets:
            exit_node.targets = list(body_targets)
            for target in body_targets:
                self.track_var_definition(target, exit_id)
        self.dag.add_node(exit_node)
        nodes.append(exit_id)

        self.dag.add_edge(DAGEdge.state_machine_with_guard(cond_id, exit_id, break_guard))

        return ConvertedSubgraph(
            entry=cond_id,
            exits=[exit_id],
            nodes=nodes,
            is_noop=False,
        )

    def convert_break(self) -> "ConvertedSubgraph":
        """Convert a break statement by wiring to the loop exit node."""
        if not self.loop_exit_stack:
            raise DagConversionError("break statement outside of loop")
        loop_exit = self.loop_exit_stack[-1]

        node_id = self.next_id("break")
        node = BreakNode(id=node_id, function_name=self.current_function)
        self.dag.add_node(node)

        self.dag.add_edge(DAGEdge.state_machine(node_id, loop_exit))

        return ConvertedSubgraph(entry=node_id, exits=[], nodes=[node_id], is_noop=False)

    def convert_continue(self) -> "ConvertedSubgraph":
        """Convert a continue statement by wiring to the loop increment node."""
        if not self.loop_incr_stack:
            raise DagConversionError("continue statement outside of loop")
        loop_incr = self.loop_incr_stack[-1]

        node_id = self.next_id("continue")
        node = ContinueNode(id=node_id, function_name=self.current_function)
        self.dag.add_node(node)

        self.dag.add_edge(DAGEdge.state_machine(node_id, loop_incr))

        return ConvertedSubgraph(entry=node_id, exits=[], nodes=[node_id], is_noop=False)
