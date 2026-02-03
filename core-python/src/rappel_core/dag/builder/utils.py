"""Shared helpers for DAG conversion."""

from __future__ import annotations

from typing import Iterable, List

from proto import ast_pb2 as ir

from ..models import assert_never
from ..nodes import (
    ActionCallNode,
    AggregatorNode,
    AssignmentNode,
    DAGNode,
    FnCallNode,
    JoinNode,
    ReturnNode,
)


class BuilderUtilsMixin:
    """Utility helpers shared across conversion helpers."""

    def track_var_definition(self, var_name: str, node_id: str) -> None:
        """Record that a variable is defined at the given node."""
        self.current_scope_vars[var_name] = node_id
        self.var_modifications.setdefault(var_name, []).append(node_id)

    @staticmethod
    def push_unique_target(targets: List[str], target: str) -> None:
        """Append a target if it is not already present."""
        if target not in targets:
            targets.append(target)

    @staticmethod
    def _targets_for_node(node: DAGNode) -> List[str]:
        """Return assignment targets for nodes that bind variables."""
        if isinstance(
            node,
            (
                AssignmentNode,
                ActionCallNode,
                FnCallNode,
                AggregatorNode,
                ReturnNode,
                JoinNode,
            ),
        ):
            if node.targets:
                return list(node.targets)
            if node.target:
                return [node.target]
        return []

    @classmethod
    def collect_assigned_targets(
        cls, statements: Iterable[ir.Statement], targets: List[str]
    ) -> None:
        """Collect assignment targets from a statement list, recursively.

        This is used to determine loop join node targets so data-flow edges
        can carry loop-carried variables out of the loop body.
        """
        for stmt in statements:
            kind = stmt.WhichOneof("kind")
            match kind:
                case "assignment":
                    for target in stmt.assignment.targets:
                        cls.push_unique_target(targets, target)
                case "conditional":
                    if stmt.conditional.HasField(
                        "if_branch"
                    ) and stmt.conditional.if_branch.HasField("block_body"):
                        cls.collect_assigned_targets(
                            stmt.conditional.if_branch.block_body.statements, targets
                        )
                    for elif_branch in stmt.conditional.elif_branches:
                        if elif_branch.HasField("block_body"):
                            cls.collect_assigned_targets(elif_branch.block_body.statements, targets)
                    if stmt.conditional.HasField(
                        "else_branch"
                    ) and stmt.conditional.else_branch.HasField("block_body"):
                        cls.collect_assigned_targets(
                            stmt.conditional.else_branch.block_body.statements, targets
                        )
                case "for_loop":
                    if stmt.for_loop.HasField("block_body"):
                        cls.collect_assigned_targets(stmt.for_loop.block_body.statements, targets)
                case "while_loop":
                    if stmt.while_loop.HasField("block_body"):
                        cls.collect_assigned_targets(stmt.while_loop.block_body.statements, targets)
                case "try_except":
                    if stmt.try_except.HasField("try_block"):
                        cls.collect_assigned_targets(stmt.try_except.try_block.statements, targets)
                    for handler in stmt.try_except.handlers:
                        if handler.HasField("block_body"):
                            cls.collect_assigned_targets(handler.block_body.statements, targets)
                case (
                    "parallel_block"
                    | "spread_action"
                    | "action_call"
                    | "return_stmt"
                    | "break_stmt"
                    | "continue_stmt"
                    | "expr_stmt"
                    | None
                ):
                    continue
                case _:
                    assert_never(kind)
