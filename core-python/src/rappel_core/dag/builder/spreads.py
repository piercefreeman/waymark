"""Spread/parallel conversion helpers."""

from __future__ import annotations

import copy
from typing import Sequence

from proto import ast_pb2 as ir

from ..models import assert_never
from ..nodes import ActionCallNode, AggregatorNode, FnCallNode, ParallelNode
from ..models import DAGEdge


class SpreadConversionMixin:
    """Convert spread and parallel constructs into DAG nodes."""

    def convert_spread_action(self, spread: ir.SpreadAction) -> list[str]:
        """Convert a spread action without explicit targets."""
        return self.convert_spread_action_with_targets(spread, [])

    def convert_spread_expr(self, spread: ir.SpreadExpr, targets: Sequence[str]) -> list[str]:
        """Convert a spread expression into an action + aggregator pair.

        Example IR:
        - results = spread items: @do(item)
        Produces a spread action node (one per item at runtime) and an
        aggregator node that collects the spread results into results.
        """
        action = spread.action
        action_id = self.next_id("spread_action")
        kwargs = self.extract_kwargs(action.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
        collection_expr = (
            copy.deepcopy(spread.collection) if spread.HasField("collection") else ir.Expr()
        )
        spread_result_var = "_spread_result"

        agg_id = self.next_id("aggregator")
        module_name = action.module_name if action.HasField("module_name") else None
        action_node = ActionCallNode(
            id=action_id,
            action_name=action.action_name,
            module_name=module_name,
            kwargs=kwargs,
            kwarg_exprs=kwarg_exprs,
            target=spread_result_var,
            aggregates_to=agg_id,
            spread_loop_var=spread.loop_var,
            spread_collection_expr=collection_expr,
            function_name=self.current_function,
        )
        self.dag.add_node(action_node)
        agg_node = AggregatorNode(
            id=agg_id,
            aggregates_from=action_id,
            targets=list(targets) if targets else None,
            aggregator_kind="aggregate",
            function_name=self.current_function,
        )
        self.dag.add_node(agg_node)

        self.dag.add_edge(DAGEdge.state_machine(action_id, agg_id))
        self.dag.add_edge(DAGEdge.data_flow(action_id, agg_id, spread_result_var))

        for t in targets:
            self.track_var_definition(t, agg_id)

        return [action_id, agg_id]

    def convert_spread_action_with_targets(
        self, spread: ir.SpreadAction, targets: Sequence[str]
    ) -> list[str]:
        """Convert a spread action statement into action + aggregator nodes."""
        action = spread.action
        action_id = self.next_id("spread_action")
        kwargs = self.extract_kwargs(action.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
        collection_expr = (
            copy.deepcopy(spread.collection) if spread.HasField("collection") else ir.Expr()
        )
        spread_result_var = "_spread_result"

        agg_id = self.next_id("aggregator")
        module_name = action.module_name if action.HasField("module_name") else None
        action_node = ActionCallNode(
            id=action_id,
            action_name=action.action_name,
            module_name=module_name,
            kwargs=kwargs,
            kwarg_exprs=kwarg_exprs,
            target=spread_result_var,
            aggregates_to=agg_id,
            spread_loop_var=spread.loop_var,
            spread_collection_expr=collection_expr,
            function_name=self.current_function,
        )
        self.dag.add_node(action_node)
        agg_node = AggregatorNode(
            id=agg_id,
            aggregates_from=action_id,
            targets=list(targets) if targets else None,
            aggregator_kind="aggregate",
            function_name=self.current_function,
        )
        self.dag.add_node(agg_node)

        self.dag.add_edge(DAGEdge.state_machine(action_id, agg_id))
        self.dag.add_edge(DAGEdge.data_flow(action_id, agg_id, spread_result_var))

        for t in targets:
            self.track_var_definition(t, agg_id)

        return [action_id, agg_id]

    def convert_parallel_block(self, parallel: ir.ParallelBlock) -> list[str]:
        """Convert a parallel block statement without assignment targets."""
        return self.convert_parallel_block_with_targets(parallel.calls, [])

    def convert_parallel_expr(self, parallel: ir.ParallelExpr, targets: Sequence[str]) -> list[str]:
        """Convert a parallel expression with assignment targets."""
        return self.convert_parallel_block_with_targets(parallel.calls, targets)

    def convert_parallel_block_with_targets(
        self,
        calls: Sequence[ir.Call],
        targets: Sequence[str],
    ) -> list[str]:
        """Convert a parallel block/expression into a parallel node + calls + join.

        Example IR:
        - a, b = parallel: @x() @y()
        Produces a parallel node, two call nodes, and an aggregator/join node.
        """
        result_nodes: list[str] = []

        parallel_id = self.next_id("parallel")
        parallel_node = ParallelNode(id=parallel_id, function_name=self.current_function)
        self.dag.add_node(parallel_node)
        result_nodes.append(parallel_id)

        list_aggregate = len(targets) == 1
        agg_id = self.next_id("parallel_aggregator")

        has_fn_calls = any(call.WhichOneof("kind") == "function" for call in calls)

        call_node_ids: list[str] = []
        for idx, call in enumerate(calls):
            call_target = None if list_aggregate else (targets[idx] if idx < len(targets) else None)
            kind = call.WhichOneof("kind")
            if kind == "action":
                action = call.action
                call_id = self.next_id("parallel_action")
                kwargs = self.extract_kwargs(action.kwargs)
                kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
                module_name = action.module_name if action.HasField("module_name") else None
                node = ActionCallNode(
                    id=call_id,
                    action_name=action.action_name,
                    module_name=module_name,
                    kwargs=kwargs,
                    kwarg_exprs=kwarg_exprs,
                    parallel_index=idx,
                    target=call_target,
                    aggregates_to=agg_id if list_aggregate else None,
                    function_name=self.current_function,
                )
            elif kind == "function":
                func = call.function
                call_id = self.next_id("parallel_fn_call")
                kwargs, kwarg_exprs = self.extract_fn_call_args(func)
                node = FnCallNode(
                    id=call_id,
                    called_function=func.name,
                    kwargs=kwargs,
                    kwarg_exprs=kwarg_exprs,
                    parallel_index=idx,
                    target=call_target,
                    aggregates_to=agg_id if list_aggregate else None,
                    function_name=self.current_function,
                )
            elif kind is None:
                continue
            else:
                assert_never(kind)

            self.dag.add_node(node)

            if call_target:
                self.track_var_definition(call_target, call_id)

            call_node_ids.append(call_id)
            result_nodes.append(call_id)

            self.dag.add_edge(
                DAGEdge.state_machine_with_condition(parallel_id, call_id, f"parallel:{idx}")
            )

        aggregator_targets = [] if has_fn_calls else list(targets)
        agg_node = AggregatorNode(
            id=agg_id,
            aggregates_from=parallel_id,
            targets=aggregator_targets if aggregator_targets else None,
            aggregator_kind="parallel",
            function_name=self.current_function,
        )
        self.dag.add_node(agg_node)
        result_nodes.append(agg_id)

        for call_id in call_node_ids:
            self.dag.add_edge(DAGEdge.state_machine(call_id, agg_id))

        return result_nodes
