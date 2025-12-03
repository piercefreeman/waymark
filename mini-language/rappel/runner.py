"""
Rappel DAG Runner - Executes DAG nodes with action queue management.

The runner handles two types of execution:
- Inline: AST evaluation within the runner (assignments, expressions, etc.)
- Delegated: External @actions that are pushed to workers

Uses an in-memory database to simulate a real backend, making read/write
patterns visible through stats tracking.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any, Callable

from .dag import DAG, DAGNode, EdgeType
from .db import InMemoryDB, Table
from .ir import (
    RappelLiteral,
    RappelVariable,
    RappelBinaryOp,
    RappelUnaryOp,
    RappelIndexAccess,
    RappelDotAccess,
    RappelListExpr,
    RappelDictExpr,
    RappelCall,
    RappelActionCall,
    RappelAssignment,
    RappelMultiAssignment,
    RappelReturn,
    RappelExprStatement,
    RappelForLoop,
    RappelIfStatement,
    RappelSpreadAction,
    RappelString,
    RappelNumber,
    RappelBoolean,
    RappelExpr,
)


class ActionStatus(Enum):
    """Status of a runnable action."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()


class ActionType(Enum):
    """Type of action execution."""
    INLINE = auto()      # Executed within the runner (AST evaluation)
    DELEGATED = auto()   # Pushed to external workers (@actions)


@dataclass
class RunnableAction:
    """
    Represents an action ready to be executed.

    This is what gets queued and picked up by the runner.
    Fully scopes the work needed so workers can pick it up.
    """
    id: str
    node_id: str
    function_name: str | None
    action_type: ActionType
    input_data: dict[str, Any] = field(default_factory=dict)
    status: ActionStatus = ActionStatus.PENDING
    # For spread actions - tracks which iteration this is
    spread_index: int | None = None
    spread_item: Any = None


@dataclass
class RunnableActionData:
    """
    Metadata storage for a node (simulates a DB row).

    Stores the values that have been pushed to this node
    from its dependencies.
    """
    node_id: str
    variable_values: dict[str, Any] = field(default_factory=dict)
    completed: bool = False
    # For nodes that expect multiple inputs (e.g., after spread)
    pending_inputs: int = 0
    collected_results: list[Any] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for DB storage."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunnableActionData":
        """Create from dict (DB retrieval)."""
        return cls(**data)


class ActionQueue:
    """
    Database-backed action queue.

    Uses the in-memory DB to store queued actions, making
    read/write patterns visible through stats. All ordering
    is maintained at the DB level (simulating SELECT ... FOR UPDATE SKIP LOCKED).
    """

    def __init__(self, db: InMemoryDB):
        self._db = db
        self._table: Table[dict[str, Any]] = db.create_table("action_queue")
        self._action_counter = 0

    def add(self, action: RunnableAction) -> None:
        """
        Add an action to the queue.

        Simulates: INSERT INTO action_queue (...) VALUES (...)
        """
        action_dict = {
            "id": action.id,
            "node_id": action.node_id,
            "function_name": action.function_name,
            "action_type": action.action_type.value,
            "input_data": action.input_data,
            "status": action.status.value,
            "spread_index": action.spread_index,
            "spread_item": action.spread_item,
        }
        self._table.insert(action.id, action_dict)

    def pop(self) -> RunnableAction | None:
        """
        Remove and return the next action.

        Simulates: SELECT * FROM action_queue ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
        followed by: DELETE FROM action_queue WHERE id = ?
        """
        result = self._table.pop_first()
        if result is None:
            return None

        action_id, action_dict = result
        self._table.delete(action_id)
        return self._dict_to_action(action_dict)

    def peek(self) -> RunnableAction | None:
        """
        Return the next action without removing.

        Simulates: SELECT * FROM action_queue ORDER BY created_at LIMIT 1
        """
        result = self._table.peek_first()
        if result is None:
            return None

        _, action_dict = result
        return self._dict_to_action(action_dict)

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self._table.count() == 0

    def size(self) -> int:
        """Return the number of actions in the queue."""
        return self._table.count()

    def next_id(self) -> str:
        """Generate a unique action ID."""
        self._action_counter += 1
        return f"action_{self._action_counter}"

    def _dict_to_action(self, d: dict[str, Any]) -> RunnableAction:
        """Convert dict back to RunnableAction."""
        return RunnableAction(
            id=d["id"],
            node_id=d["node_id"],
            function_name=d["function_name"],
            action_type=ActionType(d["action_type"]),
            input_data=d["input_data"],
            status=ActionStatus(d["status"]),
            spread_index=d["spread_index"],
            spread_item=d["spread_item"],
        )


class DAGRunner:
    """
    Executes a DAG by processing nodes through an action queue.

    The runner:
    1. Picks up actions from the queue
    2. Resolves the corresponding DAG node
    3. Executes it (inline or delegated)
    4. Handles results and pushes data to dependent nodes
    5. Queues the next action(s)

    Uses an in-memory DB for storage, making read/write patterns visible.
    """

    def __init__(
        self,
        dag: DAG,
        action_handlers: dict[str, Callable[..., Any]] | None = None,
        function_handlers: dict[str, Callable[..., Any]] | None = None,
        db: InMemoryDB | None = None,
    ):
        """
        Initialize the DAG runner.

        Args:
            dag: The DAG to execute
            action_handlers: Dict of action_name -> handler function for @actions
            function_handlers: Dict of function_name -> handler function for fn calls
            db: Optional database instance (creates new one if not provided)
        """
        self.dag = dag
        self.action_handlers = action_handlers or {}
        self.function_handlers = function_handlers or {}

        # Database for persistent storage (tracks read/write stats)
        self.db = db or InMemoryDB()
        self.queue = ActionQueue(self.db)

        # Node data - kept in memory as a "cache" during inline execution.
        # In a real implementation with a DB backend:
        # - Inline execution uses this cache (no DB writes)
        # - Delegated actions would persist to DB before queuing
        # The ActionQueue writes show the real difference.
        self.node_data: dict[str, RunnableActionData] = {}

        # Execution results per node
        self.results: dict[str, Any] = {}
        # Track which functions have completed
        self.completed_functions: set[str] = set()
        # Mapping from node UUID to node ID
        self._uuid_to_node_id: dict[str, str] = {}

        # Initialize node data for all nodes
        for node_id, node in self.dag.nodes.items():
            self.node_data[node_id] = RunnableActionData(node_id=node_id)
            self._uuid_to_node_id[node.node_uuid] = node_id

    def run(self, function_name: str, inputs: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Run a function in the DAG.

        Args:
            function_name: Name of the function to execute
            inputs: Input values for the function

        Returns:
            Dict of output variable names to values
        """
        inputs = inputs or {}

        # Reset state for this function's nodes
        self._reset_function_state(function_name)

        # Find the input node for this function
        input_node = self._find_input_node(function_name)
        if not input_node:
            raise ValueError(f"Function '{function_name}' not found in DAG")

        # Initialize input values
        self.node_data[input_node.id].variable_values = inputs.copy()
        self.node_data[input_node.id].completed = True

        # Push input values to successor nodes
        self._push_outputs(input_node.id, inputs)

        # Execute all inline nodes starting from input's successors
        # This will eagerly execute everything until hitting @actions
        self._execute_inline_batch(input_node.id)

        # Main execution loop - only processes delegated actions now
        while not self.queue.is_empty():
            action = self.queue.pop()
            if action is None:
                break

            # Execute the delegated action
            self._execute_action(action)

            # After delegated action completes, eagerly execute inline successors
            self._execute_inline_batch(action.node_id)

        # Find and return outputs
        output_node = self._find_output_node(function_name)
        if output_node:
            return self.node_data[output_node.id].variable_values
        return {}

    def run_main(self, inputs: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Run the 'main' function as the program entry point.

        Args:
            inputs: Input values for main (usually empty)

        Returns:
            Dict of output variable names to values
        """
        return self.run("main", inputs)

    def _execute_inline_batch(self, starting_node_id: str) -> None:
        """
        Eagerly execute all inline nodes reachable from the starting node.

        This executes inline nodes one at a time, respecting DAG ordering and
        dependencies, but does so in-memory without roundtripping to the "DB"
        to queue each action. Only delegated @actions get queued.

        The key optimization: we use the same handlers and data flow, but
        avoid writing to the action queue for inline nodes.

        Args:
            starting_node_id: The node whose successors we start from
        """
        # Get initial ready successors
        to_process = self._get_ready_successors(starting_node_id)

        while to_process:
            node_id = to_process.pop(0)  # FIFO order

            node = self.dag.nodes.get(node_id)
            if node is None:
                continue

            # Skip if already completed
            if self.node_data[node_id].completed:
                continue

            action_type = self._get_action_type(node)

            if action_type == ActionType.DELEGATED:
                # --------------------------------------------------------------
                # IN A REAL IMPLEMENTATION WITH A DATABASE BACKEND:
                # This is where we write to the action queue table, e.g.:
                #
                #   INSERT INTO action_queue (id, node_id, function_name, ...)
                #   VALUES (...)
                #
                # This is the "roundtrip" we're avoiding for inline nodes.
                # --------------------------------------------------------------
                action = RunnableAction(
                    id=self.queue.next_id(),
                    node_id=node_id,
                    function_name=node.function_name,
                    action_type=ActionType.DELEGATED,
                    input_data=self.node_data[node_id].variable_values.copy(),
                )
                self.queue.add(action)
                # Don't continue past delegated actions - they'll trigger
                # another inline batch when they complete
            else:
                # Execute inline node immediately (no queue roundtrip)
                action = RunnableAction(
                    id=self.queue.next_id(),
                    node_id=node_id,
                    function_name=node.function_name,
                    action_type=ActionType.INLINE,
                    input_data=self.node_data[node_id].variable_values.copy(),
                )
                self._execute_action(action)

                # After execution, get the next ready successors and add them
                # to our processing list (still in-memory, no queue)
                successors = self._get_ready_successors(node_id)
                for succ_id in successors:
                    if succ_id not in to_process:
                        to_process.append(succ_id)

    def _get_ready_successors(self, node_id: str) -> list[str]:
        """
        Get successor nodes that are ready to execute.

        A node is ready if:
        - It hasn't been completed yet
        - All its dependencies are satisfied

        Returns nodes in order suitable for processing.
        """
        node = self.dag.nodes.get(node_id)
        if node is None:
            return []

        ready = []

        # Handle conditional branching
        if node.ir_node and isinstance(node.ir_node, RappelIfStatement):
            condition_result = self.node_data[node_id].variable_values.get("_condition", True)

            for edge in self.dag.get_outgoing_edges(node_id):
                if edge.edge_type != EdgeType.STATE_MACHINE:
                    continue

                # Filter by condition
                if edge.condition == "then" and not condition_result:
                    continue
                if edge.condition == "else" and condition_result:
                    continue

                target_node = self.dag.nodes.get(edge.target)
                if target_node and not self.node_data[edge.target].completed:
                    # Check if aggregator is ready
                    if target_node.is_aggregator:
                        data = self.node_data[edge.target]
                        if data.pending_inputs > 0 and len(data.collected_results) < data.pending_inputs:
                            continue
                    ready.append(edge.target)
        else:
            # Normal node - follow all state machine edges
            for edge in self.dag.get_outgoing_edges(node_id):
                if edge.edge_type != EdgeType.STATE_MACHINE:
                    continue

                target_node = self.dag.nodes.get(edge.target)
                if target_node is None:
                    continue

                if self.node_data[edge.target].completed:
                    continue

                # Check if aggregator is ready
                if target_node.is_aggregator:
                    data = self.node_data[edge.target]
                    if data.pending_inputs > 0 and len(data.collected_results) < data.pending_inputs:
                        continue

                ready.append(edge.target)

        return ready

    def _reset_function_state(self, function_name: str) -> None:
        """Reset state for all nodes in a function."""
        fn_nodes = self.dag.get_nodes_for_function(function_name)
        for node_id in fn_nodes:
            self.node_data[node_id] = RunnableActionData(node_id=node_id)
            if node_id in self.results:
                del self.results[node_id]

    def _find_input_node(self, function_name: str) -> DAGNode | None:
        """Find the input boundary node for a function."""
        for node in self.dag.nodes.values():
            if node.function_name == function_name and node.is_input:
                return node
        return None

    def _find_output_node(self, function_name: str) -> DAGNode | None:
        """Find the output boundary node for a function."""
        for node in self.dag.nodes.values():
            if node.function_name == function_name and node.is_output:
                return node
        return None

    def _execute_action(self, action: RunnableAction) -> None:
        """Execute a single action."""
        action.status = ActionStatus.RUNNING
        node = self.dag.nodes.get(action.node_id)

        if node is None:
            action.status = ActionStatus.FAILED
            return

        try:
            if action.action_type == ActionType.DELEGATED:
                result = self._handle_delegated(node, action)
            else:
                result = self._handle_inline(node, action)

            action.status = ActionStatus.COMPLETED
            self.results[node.id] = result

            # Push outputs to dependent nodes
            self._push_outputs(node.id, result)

            # Mark node as completed
            self.node_data[node.id].completed = True

            # Note: Successor handling is done by _execute_inline_batch, not here

        except Exception as e:
            action.status = ActionStatus.FAILED
            raise RuntimeError(f"Action failed for node {node.id}: {e}") from e

    def _handle_inline(self, node: DAGNode, action: RunnableAction) -> Any:
        """
        Handle inline execution (AST evaluation).

        This handles assignments, expressions, control flow, etc.
        """
        # Get current scope values
        scope = self._get_scope_for_node(node.id)

        # Add spread item if this is a spread iteration
        if action.spread_item is not None and node.loop_var:
            scope[node.loop_var] = action.spread_item

        ir_node = node.ir_node

        if ir_node is None:
            # Boundary nodes (input/output) or joins just pass through
            if node.is_aggregator:
                # Aggregator collects results from spread
                return self._handle_aggregator(node)
            return scope

        if isinstance(ir_node, RappelAssignment):
            value = self._evaluate_expr(ir_node.value, scope)
            return {ir_node.target: value}

        elif isinstance(ir_node, RappelMultiAssignment):
            value = self._evaluate_expr(ir_node.value, scope)
            if isinstance(value, (list, tuple)):
                return {t: v for t, v in zip(ir_node.targets, value)}
            return {ir_node.targets[0]: value}

        elif isinstance(ir_node, RappelReturn):
            # Return just passes through the scope
            return scope

        elif isinstance(ir_node, RappelForLoop):
            return self._handle_for_loop(node, ir_node, scope)

        elif isinstance(ir_node, RappelIfStatement):
            return self._handle_if_statement(node, ir_node, scope)

        elif isinstance(ir_node, RappelExprStatement):
            self._evaluate_expr(ir_node.expr, scope)
            return scope

        elif isinstance(ir_node, RappelSpreadAction):
            return self._handle_spread_action(node, ir_node, scope, action)

        return scope

    def _handle_delegated(self, node: DAGNode, action: RunnableAction) -> Any:
        """
        Handle delegated execution (@actions).

        These are pushed to external workers.
        """
        scope = self._get_scope_for_node(node.id)
        ir_node = node.ir_node

        if ir_node is None:
            return scope

        # Extract the action call
        action_call = None
        target_var = None

        if isinstance(ir_node, RappelAssignment):
            if isinstance(ir_node.value, RappelActionCall):
                action_call = ir_node.value
                target_var = ir_node.target
        elif isinstance(ir_node, RappelExprStatement):
            if isinstance(ir_node.expr, RappelActionCall):
                action_call = ir_node.expr
        elif isinstance(ir_node, RappelSpreadAction):
            action_call = ir_node.action
            target_var = ir_node.target

        if action_call is None:
            return scope

        # Evaluate kwargs
        kwargs = {}
        for name, expr in action_call.kwargs:
            # For spread actions, the item var comes from the action's input_data
            if action.spread_item is not None and isinstance(expr, RappelVariable):
                if expr.name == action.input_data.get("_item_var"):
                    kwargs[name] = action.spread_item
                    continue
            kwargs[name] = self._evaluate_expr(expr, scope)

        # Call the action handler
        handler = self.action_handlers.get(action_call.action_name)
        if handler is None:
            raise ValueError(f"No handler registered for action '@{action_call.action_name}'")

        result = handler(**kwargs)

        if target_var:
            return {target_var: result}
        return scope

    def _handle_for_loop(self, node: DAGNode, ir_node: RappelForLoop, scope: dict[str, Any]) -> Any:
        """Handle for loop execution."""
        iterable = self._evaluate_expr(ir_node.iterable, scope)

        # For loops in this DSL have a single function call in the body
        # Execute the body for each item, accumulating changes to scope
        loop_scope = scope.copy()
        results = []

        for item in iterable:
            loop_scope[ir_node.loop_var] = item

            # Execute body statements
            for stmt in ir_node.body:
                if isinstance(stmt, RappelAssignment):
                    if isinstance(stmt.value, RappelCall):
                        # Call the function
                        result = self._execute_function_call(stmt.value, loop_scope)
                        loop_scope[stmt.target] = result
                        results.append(result)
                    else:
                        value = self._evaluate_expr(stmt.value, loop_scope)
                        loop_scope[stmt.target] = value

        # Return the updated scope (with accumulated changes)
        return loop_scope

    def _handle_if_statement(self, node: DAGNode, ir_node: RappelIfStatement, scope: dict[str, Any]) -> Any:
        """Handle if statement - just evaluate condition, branches handled by DAG edges."""
        condition = self._evaluate_expr(ir_node.condition, scope)
        # Store condition result for branch selection
        # Note: Put **scope first so our _condition overwrites any inherited one
        return {**scope, "_condition": condition}

    def _handle_spread_action(
        self,
        node: DAGNode,
        ir_node: RappelSpreadAction,
        scope: dict[str, Any],
        action: RunnableAction
    ) -> Any:
        """
        Handle spread action - queues multiple delegated actions.

        spread items:item -> @fetch_details(id=item)
        """
        if action.spread_index is not None:
            # This is an individual spread iteration - handle as delegated
            return self._handle_delegated(node, action)

        # This is the initial spread - queue actions for each item
        source_list = self._evaluate_expr(ir_node.source_list, scope)

        if not isinstance(source_list, (list, tuple)):
            raise ValueError(f"Spread source must be a list, got {type(source_list)}")

        # Find the aggregator node
        agg_node_id = None
        for edge in self.dag.get_outgoing_edges(node.id):
            target_node = self.dag.nodes.get(edge.target)
            if target_node and target_node.is_aggregator:
                agg_node_id = edge.target
                break

        if agg_node_id:
            # Set up aggregator to expect results
            self.node_data[agg_node_id].pending_inputs = len(source_list)
            self.node_data[agg_node_id].collected_results = []

        # Queue a delegated action for each item
        for i, item in enumerate(source_list):
            spread_action = RunnableAction(
                id=self.queue.next_id(),
                node_id=node.id,
                function_name=node.function_name,
                action_type=ActionType.DELEGATED,
                input_data={
                    **scope,
                    "_item_var": ir_node.item_var,
                    "_spread_index": i,
                },
                spread_index=i,
                spread_item=item,
            )
            self.queue.add(spread_action)

        # Return empty - results will be collected by aggregator
        return {}

    def _handle_aggregator(self, node: DAGNode) -> Any:
        """Handle aggregator node - collect spread results."""
        data = self.node_data[node.id]

        # Check if all spread results are in
        if len(data.collected_results) < data.pending_inputs:
            # Not ready yet
            return {}

        # Sort by spread index and extract values
        sorted_results = sorted(data.collected_results, key=lambda x: x[0])
        values = [r[1] for r in sorted_results]

        # Get the target variable from the aggregates_from node
        source_node = self.dag.nodes.get(node.aggregates_from) if node.aggregates_from else None
        target_var = None

        if source_node and source_node.ir_node:
            if isinstance(source_node.ir_node, RappelSpreadAction):
                target_var = source_node.ir_node.target

        if target_var:
            return {target_var: values}
        return {"_results": values}

    def _execute_function_call(self, call: RappelCall, scope: dict[str, Any]) -> Any:
        """Execute a function call."""
        # Evaluate kwargs
        kwargs = {}
        for name, expr in call.kwargs:
            kwargs[name] = self._evaluate_expr(expr, scope)

        # Check for registered handler
        handler = self.function_handlers.get(call.target)
        if handler:
            return handler(**kwargs)

        # Otherwise, run the function through the DAG
        # This would recursively run the function's subgraph
        result = self.run(call.target, kwargs)

        # Return the first output value (or all if multiple)
        if len(result) == 1:
            return list(result.values())[0]
        return tuple(result.values())

    def _evaluate_expr(self, expr: RappelExpr, scope: dict[str, Any]) -> Any:
        """Evaluate an expression in the given scope."""
        if isinstance(expr, RappelLiteral):
            return self._evaluate_literal(expr.value)

        elif isinstance(expr, RappelVariable):
            if expr.name not in scope:
                raise NameError(f"Variable '{expr.name}' not defined")
            return scope[expr.name]

        elif isinstance(expr, RappelBinaryOp):
            left = self._evaluate_expr(expr.left, scope)
            right = self._evaluate_expr(expr.right, scope)
            return self._apply_binary_op(expr.op, left, right)

        elif isinstance(expr, RappelUnaryOp):
            operand = self._evaluate_expr(expr.operand, scope)
            return self._apply_unary_op(expr.op, operand)

        elif isinstance(expr, RappelIndexAccess):
            target = self._evaluate_expr(expr.target, scope)
            index = self._evaluate_expr(expr.index, scope)
            return target[index]

        elif isinstance(expr, RappelDotAccess):
            target = self._evaluate_expr(expr.target, scope)
            return getattr(target, expr.field)

        elif isinstance(expr, RappelListExpr):
            items = []
            for item in expr.items:
                if hasattr(item, 'target'):  # RappelSpread
                    spread_val = self._evaluate_expr(item.target, scope)
                    items.extend(spread_val)
                else:
                    items.append(self._evaluate_expr(item, scope))
            return items

        elif isinstance(expr, RappelDictExpr):
            result = {}
            for key_expr, val_expr in expr.pairs:
                key = self._evaluate_expr(key_expr, scope)
                val = self._evaluate_expr(val_expr, scope)
                result[key] = val
            return result

        elif isinstance(expr, RappelCall):
            return self._execute_function_call(expr, scope)

        elif isinstance(expr, RappelActionCall):
            # This shouldn't be called directly - actions go through delegated handler
            raise ValueError("Action calls should be handled by delegated handler")

        else:
            raise ValueError(f"Unknown expression type: {type(expr)}")

    def _evaluate_literal(self, value) -> Any:
        """Evaluate a literal value."""
        if isinstance(value, RappelString):
            return value.value
        elif isinstance(value, RappelNumber):
            return value.value
        elif isinstance(value, RappelBoolean):
            return value.value
        else:
            return value

    def _apply_binary_op(self, op: str, left: Any, right: Any) -> Any:
        """Apply a binary operator."""
        ops = {
            "+": lambda a, b: a + b,
            "-": lambda a, b: a - b,
            "*": lambda a, b: a * b,
            "/": lambda a, b: a / b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            "<": lambda a, b: a < b,
            ">": lambda a, b: a > b,
            "<=": lambda a, b: a <= b,
            ">=": lambda a, b: a >= b,
            "and": lambda a, b: a and b,
            "or": lambda a, b: a or b,
        }
        if op not in ops:
            raise ValueError(f"Unknown operator: {op}")
        return ops[op](left, right)

    def _apply_unary_op(self, op: str, operand: Any) -> Any:
        """Apply a unary operator."""
        if op == "not":
            return not operand
        elif op == "-":
            return -operand
        else:
            raise ValueError(f"Unknown unary operator: {op}")

    def _get_scope_for_node(self, node_id: str) -> dict[str, Any]:
        """
        Get the variable scope for a node.

        Collects values from all data flow edges pointing to this node.
        """
        scope = {}

        # Get values from node data (pushed from predecessors)
        if node_id in self.node_data:
            scope.update(self.node_data[node_id].variable_values)

        return scope

    def _push_outputs(self, node_id: str, outputs: Any) -> None:
        """
        Push output values to dependent nodes.

        Updates the node_data for nodes that depend on this one.
        """
        if not isinstance(outputs, dict):
            return

        # Update this node's data
        self.node_data[node_id].variable_values.update(outputs)

        # Find data flow edges from this node
        for edge in self.dag.get_outgoing_edges(node_id):
            target_data = self.node_data.get(edge.target)
            if target_data is None:
                continue

            if edge.edge_type == EdgeType.DATA_FLOW:
                # Push specific variable
                if edge.variable and edge.variable in outputs:
                    target_data.variable_values[edge.variable] = outputs[edge.variable]
            elif edge.edge_type == EdgeType.STATE_MACHINE:
                # Push all outputs for execution order edges
                target_data.variable_values.update(outputs)

            # Handle aggregator collection
            target_node = self.dag.nodes.get(edge.target)
            if target_node and target_node.is_aggregator:
                # Check if this is from a spread action
                source_node = self.dag.nodes.get(node_id)
                if source_node and source_node.ir_node:
                    if isinstance(source_node.ir_node, RappelSpreadAction):
                        # Collect the result with its index
                        action = self.results.get(node_id)
                        if isinstance(action, dict):
                            for var, val in action.items():
                                if var.startswith("_"):
                                    continue
                                # Find the spread index from recent actions
                                # For now, just append
                                idx = len(target_data.collected_results)
                                target_data.collected_results.append((idx, val))

    def _queue_successors(self, node_id: str) -> None:
        """
        Queue actions for successor nodes.

        Follows state machine edges to find the next nodes to execute.
        """
        node = self.dag.nodes.get(node_id)
        if node is None:
            return

        # Check if this is a conditional node
        if node.ir_node and isinstance(node.ir_node, RappelIfStatement):
            self._queue_conditional_successors(node_id)
            return

        for edge in self.dag.get_outgoing_edges(node_id):
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue

            target_node = self.dag.nodes.get(edge.target)
            if target_node is None:
                continue

            # Skip if already completed
            if self.node_data[edge.target].completed:
                continue

            # Check if target is an aggregator
            if target_node.is_aggregator:
                # Only queue if all inputs are ready
                data = self.node_data[edge.target]
                if data.pending_inputs > 0 and len(data.collected_results) < data.pending_inputs:
                    continue

            # Determine action type
            action_type = self._get_action_type(target_node)

            action = RunnableAction(
                id=self.queue.next_id(),
                node_id=edge.target,
                function_name=target_node.function_name,
                action_type=action_type,
                input_data=self.node_data[edge.target].variable_values.copy(),
            )
            self.queue.add(action)

    def _queue_conditional_successors(self, node_id: str) -> None:
        """Queue the appropriate branch for a conditional."""
        condition_result = self.node_data[node_id].variable_values.get("_condition", True)

        for edge in self.dag.get_outgoing_edges(node_id):
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue

            # Check edge condition
            if edge.condition == "then" and not condition_result:
                continue
            if edge.condition == "else" and condition_result:
                continue

            target_node = self.dag.nodes.get(edge.target)
            if target_node is None:
                continue

            action_type = self._get_action_type(target_node)

            action = RunnableAction(
                id=self.queue.next_id(),
                node_id=edge.target,
                function_name=target_node.function_name,
                action_type=action_type,
                input_data=self.node_data[edge.target].variable_values.copy(),
            )
            self.queue.add(action)

    def _get_action_type(self, node: DAGNode) -> ActionType:
        """Determine whether a node should be handled inline or delegated."""
        if node.ir_node is None:
            return ActionType.INLINE

        # Action calls are always delegated
        if isinstance(node.ir_node, RappelExprStatement):
            if isinstance(node.ir_node.expr, RappelActionCall):
                return ActionType.DELEGATED

        if isinstance(node.ir_node, RappelAssignment):
            if isinstance(node.ir_node.value, RappelActionCall):
                return ActionType.DELEGATED

        if isinstance(node.ir_node, RappelSpreadAction):
            return ActionType.INLINE  # Spread starts inline, then queues delegated

        # Everything else is inline
        return ActionType.INLINE
