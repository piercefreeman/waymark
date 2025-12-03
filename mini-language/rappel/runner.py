"""
Rappel DAG Runner - Executes DAG nodes with action queue management.

The runner handles two types of execution:
- Inline: AST evaluation within the runner (assignments, expressions, etc.)
- Delegated: External @actions that are pushed to workers

Uses an in-memory database to simulate a real backend, making read/write
patterns visible through stats tracking.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any, Callable

from .dag import DAG, DAGNode, EdgeType
from .db import InMemoryDB, Table, get_db
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
    RappelRetryPolicy,
    RappelAssignment,
    RappelMultiAssignment,
    RappelReturn,
    RappelExprStatement,
    RappelForLoop,
    RappelIfStatement,
    RappelSpreadAction,
    RappelTryExcept,
    RappelExceptHandler,
    RappelParallelBlock,
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
    # For parallel blocks - tracks which call index this is
    parallel_index: int | None = None
    # For durable sleep - Unix timestamp when action should be executed
    # If None or in the past, action is ready immediately
    scheduled_at: float | None = None

    # Retry policy fields
    # lock_uuid: Worker that currently owns this action (for timeout detection)
    lock_uuid: str | None = None
    # timeout_at: Unix timestamp when action should be considered timed out (NOW + timeout)
    timeout_at: float | None = None
    # retries_remaining: How many retries are left for this action
    retries_remaining: int = 0
    # backoff_seconds: Base backoff duration for exponential retry
    backoff_seconds: float = 60.0
    # retry_attempt: Current retry attempt (0 = first attempt)
    retry_attempt: int = 0
    # original_action_id: If this is a retry, the original action ID
    original_action_id: str | None = None


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
        self._counter_lock = threading.Lock()

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
            "parallel_index": action.parallel_index,
            "scheduled_at": action.scheduled_at,
            # Retry policy fields
            "lock_uuid": action.lock_uuid,
            "timeout_at": action.timeout_at,
            "retries_remaining": action.retries_remaining,
            "backoff_seconds": action.backoff_seconds,
            "retry_attempt": action.retry_attempt,
            "original_action_id": action.original_action_id,
        }
        self._table.insert(action.id, action_dict)

    def pop(self) -> RunnableAction | None:
        """
        Remove and return the next ready action (thread-safe, atomic).

        Simulates: SELECT * FROM action_queue
                   WHERE scheduled_at IS NULL OR scheduled_at <= NOW()
                   ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED
        followed by: DELETE FROM action_queue WHERE id = ?

        Uses atomic pop_first_ready_and_delete to prevent race conditions between workers.
        Actions with scheduled_at in the future are skipped (durable sleep).
        """
        current_time = time.time()

        def is_ready(action_dict: dict) -> bool:
            scheduled_at = action_dict.get("scheduled_at")
            return scheduled_at is None or scheduled_at <= current_time

        result = self._table.pop_first_ready_and_delete(is_ready)
        if result is None:
            return None

        _, action_dict = result
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

    def has_scheduled_actions(self) -> bool:
        """
        Check if there are any actions scheduled for the future.

        Returns True if there are actions waiting for their scheduled time.
        This is used to determine if the runner should keep polling.
        """
        current_time = time.time()
        all_actions = self._table.all()
        for action_dict in all_actions.values():
            scheduled_at = action_dict.get("scheduled_at")
            if scheduled_at is not None and scheduled_at > current_time:
                return True
        return False

    def get_next_scheduled_time(self) -> float | None:
        """
        Get the earliest scheduled time of any pending action.

        Returns None if no actions are scheduled for the future.
        """
        current_time = time.time()
        earliest = None
        all_actions = self._table.all()
        for action_dict in all_actions.values():
            scheduled_at = action_dict.get("scheduled_at")
            if scheduled_at is not None and scheduled_at > current_time:
                if earliest is None or scheduled_at < earliest:
                    earliest = scheduled_at
        return earliest

    def size(self) -> int:
        """Return the number of actions in the queue."""
        return self._table.count()

    def next_id(self) -> str:
        """Generate a unique action ID (thread-safe)."""
        with self._counter_lock:
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
            parallel_index=d.get("parallel_index"),
            scheduled_at=d.get("scheduled_at"),
            # Retry policy fields
            lock_uuid=d.get("lock_uuid"),
            timeout_at=d.get("timeout_at"),
            retries_remaining=d.get("retries_remaining", 0),
            backoff_seconds=d.get("backoff_seconds", 60.0),
            retry_attempt=d.get("retry_attempt", 0),
            original_action_id=d.get("original_action_id"),
        )

    def claim(self, action_id: str, worker_uuid: str, timeout_seconds: float | None = None) -> bool:
        """
        Claim an action with a lock and optional timeout.

        Simulates: UPDATE action_queue SET lock_uuid = ?, timeout_at = NOW() + ?
                   WHERE id = ? AND lock_uuid IS NULL

        Returns True if successfully claimed, False if already claimed.
        """
        action_dict = self._table.get(action_id)
        if action_dict is None:
            return False

        if action_dict.get("lock_uuid") is not None:
            return False

        action_dict["lock_uuid"] = worker_uuid
        if timeout_seconds is not None:
            action_dict["timeout_at"] = time.time() + timeout_seconds

        self._table.update(action_id, action_dict)
        return True

    def release(self, action_id: str, worker_uuid: str) -> bool:
        """
        Release a lock on an action.

        Simulates: UPDATE action_queue SET lock_uuid = NULL, timeout_at = NULL
                   WHERE id = ? AND lock_uuid = ?

        Returns True if lock was released, False if not owned.
        """
        action_dict = self._table.get(action_id)
        if action_dict is None:
            return False

        if action_dict.get("lock_uuid") != worker_uuid:
            return False

        action_dict["lock_uuid"] = None
        action_dict["timeout_at"] = None
        self._table.update(action_id, action_dict)
        return True

    def get_timed_out_actions(self) -> list[RunnableAction]:
        """
        Get all actions that have timed out.

        Simulates: SELECT * FROM action_queue
                   WHERE timeout_at IS NOT NULL AND timeout_at < NOW()
                   FOR UPDATE SKIP LOCKED

        Returns list of timed out actions.
        """
        current_time = time.time()
        timed_out = []

        all_actions = self._table.all()
        for action_dict in all_actions.values():
            timeout_at = action_dict.get("timeout_at")
            if timeout_at is not None and timeout_at < current_time:
                timed_out.append(self._dict_to_action(action_dict))

        return timed_out

    def schedule_retry(self, action: RunnableAction) -> RunnableAction | None:
        """
        Schedule a retry for an action with exponential backoff.

        Returns the new retry action, or None if no retries remaining.

        The backoff is calculated as: backoff_seconds * 2^retry_attempt
        """
        if action.retries_remaining <= 0:
            return None

        # Calculate next scheduled time with exponential backoff
        backoff = action.backoff_seconds * (2 ** action.retry_attempt)
        scheduled_at = time.time() + backoff

        # Create new retry action
        retry_action = RunnableAction(
            id=self.next_id(),
            node_id=action.node_id,
            function_name=action.function_name,
            action_type=action.action_type,
            input_data=action.input_data,
            status=ActionStatus.PENDING,
            spread_index=action.spread_index,
            spread_item=action.spread_item,
            parallel_index=action.parallel_index,
            scheduled_at=scheduled_at,
            # Updated retry fields
            lock_uuid=None,
            timeout_at=None,
            retries_remaining=action.retries_remaining - 1,
            backoff_seconds=action.backoff_seconds,
            retry_attempt=action.retry_attempt + 1,
            original_action_id=action.original_action_id or action.id,
        )

        self.add(retry_action)
        return retry_action


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

        # Add built-in functions
        self._add_builtin_functions()

        # Database for persistent storage (tracks read/write stats)
        self.db = db or InMemoryDB()
        self.queue = ActionQueue(self.db)

        # Node data table - stores completion status and aggregator state
        # Simplified: variable_values now come from the inbox pattern
        self._node_data_table: Table[dict[str, Any]] = self.db.create_table("node_data")

        # Execution results per node
        self._results_table: Table[dict[str, Any]] = self.db.create_table("results")

        # INBOX PATTERN: Append-only ledger for passing values between nodes
        # When Node A completes, it INSERTs into this table for each target
        # When Node B runs, it SELECTs all rows WHERE target_node_id = B
        # This is O(1) writes (no locks!) and O(n) read when needed
        self._node_inputs_table: Table[dict[str, Any]] = self.db.create_table("node_inputs")

        # Track which functions have completed
        self.completed_functions: set[str] = set()
        # Mapping from node UUID to node ID
        self._uuid_to_node_id: dict[str, str] = {}

        # Initialize node data for all nodes
        for node_id, node in self.dag.nodes.items():
            data = RunnableActionData(node_id=node_id)
            self._node_data_table.insert(node_id, data.to_dict())
            self._uuid_to_node_id[node.node_uuid] = node_id

    def _add_builtin_functions(self) -> None:
        """Add built-in functions that aren't already overridden by user handlers."""
        builtins = {
            "range": self._builtin_range,
            "enumerate": self._builtin_enumerate,
            "len": self._builtin_len,
        }
        for name, func in builtins.items():
            if name not in self.function_handlers:
                self.function_handlers[name] = func

    def _builtin_range(self, start: int = 0, stop: int | None = None, step: int = 1) -> list[int]:
        """
        Built-in range function.

        Usage:
            range(stop=5) -> [0, 1, 2, 3, 4]
            range(start=1, stop=5) -> [1, 2, 3, 4]
            range(start=0, stop=10, step=2) -> [0, 2, 4, 6, 8]
        """
        if stop is None:
            # range(stop=5) means range(0, 5)
            stop = start
            start = 0
        return list(range(start, stop, step))

    def _builtin_enumerate(self, items) -> list[list]:
        """
        Built-in enumerate function.

        Returns a list of [index, item] pairs.
        For dicts, treats as list of [key, value] pairs.

        Usage:
            enumerate(items=["a", "b", "c"]) -> [[0, "a"], [1, "b"], [2, "c"]]
            enumerate(items={"a": 1, "b": 2}) -> [[0, ["a", 1]], [1, ["b", 2]]]
        """
        if isinstance(items, dict):
            return [[i, [k, v]] for i, (k, v) in enumerate(items.items())]
        return [[i, item] for i, item in enumerate(items)]

    def _builtin_len(self, items: list | dict | str) -> int:
        """
        Built-in len function.

        Returns the length of a list, dict, or string.

        Usage:
            len(items=[1, 2, 3]) -> 3
            len(items={"a": 1}) -> 1
            len(items="hello") -> 5
        """
        return len(items)

    def _get_retry_info(self, node: DAGNode) -> tuple[int, float, float | None]:
        """
        Extract retry policy info from a node's IR.

        Returns (max_retries, backoff_seconds, timeout_seconds) tuple.
        Uses the first matching policy's settings for retry/backoff,
        and the action call's timeout_seconds field for timeout.
        """
        ir_node = node.ir_node

        # Extract the action call from various node types
        action_call: RappelActionCall | None = None
        if isinstance(ir_node, RappelActionCall):
            action_call = ir_node
        elif isinstance(ir_node, RappelAssignment) and isinstance(ir_node.value, RappelActionCall):
            action_call = ir_node.value
        elif isinstance(ir_node, RappelExprStatement) and isinstance(ir_node.expr, RappelActionCall):
            action_call = ir_node.expr

        if action_call is None:
            return (0, 60.0, None)

        # Get timeout from action call (separate from retry policies)
        timeout_seconds = action_call.timeout_seconds

        # Get retry/backoff from first policy (if any)
        if action_call.retry_policies:
            policy = action_call.retry_policies[0]
            return (policy.max_retries, policy.backoff_seconds, timeout_seconds)

        # Default: no retries, but keep any timeout
        return (0, 60.0, timeout_seconds)

    def _find_matching_retry_policy(
        self, node: DAGNode, exception: Exception
    ) -> RappelRetryPolicy | None:
        """
        Find a retry policy that matches the given exception.

        Returns the matching policy, or None if no match.
        """
        ir_node = node.ir_node

        # Get retry policies from the action call
        policies = []
        if isinstance(ir_node, RappelActionCall):
            policies = ir_node.retry_policies
        elif isinstance(ir_node, RappelAssignment) and isinstance(ir_node.value, RappelActionCall):
            policies = ir_node.value.retry_policies
        elif isinstance(ir_node, RappelExprStatement) and isinstance(ir_node.expr, RappelActionCall):
            policies = ir_node.expr.retry_policies

        if not policies:
            return None

        exception_type_name = type(exception).__name__

        # First, look for a specific match
        for policy in policies:
            if policy.exception_types:  # Non-empty means specific exception types
                if exception_type_name in policy.exception_types:
                    return policy
            else:
                # Empty tuple = catch all, but prioritize specific matches first
                pass

        # Then, look for a catch-all policy
        for policy in policies:
            if not policy.exception_types:  # Empty = catch all
                return policy

        return None

    def _get_node_data(self, node_id: str) -> RunnableActionData:
        """
        Read node data from DB.

        Simulates: SELECT * FROM node_data WHERE node_id = ?
        """
        data_dict = self._node_data_table.get(node_id)
        if data_dict is None:
            raise KeyError(f"Node data not found for {node_id}")
        return RunnableActionData.from_dict(data_dict)

    def _set_node_data(self, node_id: str, data: RunnableActionData) -> None:
        """
        Write node data to DB.

        Simulates: UPDATE node_data SET ... WHERE node_id = ?
        """
        self._node_data_table.upsert(node_id, data.to_dict())

    def _get_result(self, node_id: str) -> Any:
        """Read execution result from DB."""
        result = self._results_table.get(node_id)
        return result.get("value") if result else None

    def _set_result(self, node_id: str, value: Any) -> None:
        """Write execution result to DB."""
        self._results_table.upsert(node_id, {"node_id": node_id, "value": value})

    # =========================================================================
    # INBOX PATTERN: Append-only ledger for node inputs
    # =========================================================================

    def _append_to_inbox(
        self,
        target_node_id: str,
        variable: str,
        value: Any,
        source_node_id: str,
        spread_index: int | None = None,
    ) -> None:
        """
        Append a value to a node's inbox (O(1) write, no locks).

        Simulates: INSERT INTO node_inputs (target, var, val, source, spread_idx) VALUES (...)
        """
        # Generate unique ID for this inbox entry (include target for fan-out cases)
        entry_id = f"{target_node_id}:{source_node_id}:{variable}:{spread_index or 0}"
        self._node_inputs_table.upsert(entry_id, {
            "target_node_id": target_node_id,
            "variable": variable,
            "value": value,
            "source_node_id": source_node_id,
            "spread_index": spread_index,
        })

    def _read_inbox(self, node_id: str) -> dict[str, Any]:
        """
        Read all pending inputs for a node (single query).

        Simulates: SELECT * FROM node_inputs WHERE target_node_id = ?
        Returns dict of variable_name -> value
        """
        # Get all entries and filter by target (simulates WHERE clause)
        all_entries = self._node_inputs_table.all()
        scope = {}
        for entry in all_entries.values():
            if entry["target_node_id"] == node_id:
                scope[entry["variable"]] = entry["value"]
        return scope

    def _read_inbox_for_aggregator(self, node_id: str) -> list[tuple[int, Any]]:
        """
        Read spread results for an aggregator node.

        Returns list of (spread_index, value) tuples for ordering.
        """
        all_entries = self._node_inputs_table.all()
        results = []
        for entry in all_entries.values():
            if entry["target_node_id"] == node_id and entry["spread_index"] is not None:
                results.append((entry["spread_index"], entry["value"]))
        return results

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

        # Initialize input values (DB write)
        input_data = self._get_node_data(input_node.id)
        input_data.variable_values = inputs.copy()
        input_data.completed = True
        self._set_node_data(input_node.id, input_data)

        # Push input values to successor nodes
        self._push_outputs(input_node.id, inputs)

        # Execute all inline nodes starting from input's successors
        # This will eagerly execute everything until hitting @actions
        self._execute_inline_batch(input_node.id)

        # Main execution loop - only processes delegated actions now
        # Handles both immediate and scheduled (durable sleep) actions
        while True:
            action = self.queue.pop()

            if action is None:
                # No ready actions - check if there are scheduled ones
                next_time = self.queue.get_next_scheduled_time()
                if next_time is None:
                    # No more actions at all
                    break
                # Wait until the next scheduled action is ready
                wait_time = next_time - time.time()
                if wait_time > 0:
                    time.sleep(wait_time)
                continue

            # Execute the delegated action
            self._execute_action(action)

            # After delegated action completes, eagerly execute inline successors
            self._execute_inline_batch(action.node_id)

        # Find and return outputs (read from INBOX)
        output_node = self._find_output_node(function_name)
        if output_node:
            return self._read_inbox(output_node.id)
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

            # Skip if already completed (DB read)
            node_data = self._get_node_data(node_id)
            if node_data.completed:
                continue

            action_type = self._get_action_type(node)

            if action_type == ActionType.DELEGATED:
                # --------------------------------------------------------------
                # DELEGATED ACTION: Write to action queue (the DB roundtrip)
                #
                # In a distributed system, another worker will:
                #   1. SELECT ... FOR UPDATE SKIP LOCKED from action_queue
                #   2. Read node_data for this node
                #   3. Execute the @action
                #   4. Write results back to node_data
                #   5. DELETE from action_queue
                # --------------------------------------------------------------

                # Check for @sleep - set scheduled_at for durable sleep
                # Read from inbox to get the full scope (including inputs)
                inbox_scope = self._read_inbox(node_id)
                scheduled_at = self._get_scheduled_at(node, inbox_scope)

                # Get retry policy info
                max_retries, backoff_seconds, timeout_seconds = self._get_retry_info(node)

                action = RunnableAction(
                    id=self.queue.next_id(),
                    node_id=node_id,
                    function_name=node.function_name,
                    action_type=ActionType.DELEGATED,
                    input_data=node_data.variable_values.copy(),
                    scheduled_at=scheduled_at,
                    retries_remaining=max_retries,
                    backoff_seconds=backoff_seconds,
                    # timeout_at will be set when action is claimed
                )
                self.queue.add(action)
                # Don't continue past delegated actions - they'll trigger
                # another inline batch when they complete
            else:
                # --------------------------------------------------------------
                # INLINE ACTION: Execute immediately (NO queue roundtrip)
                #
                # We still read/write node_data to DB, but we skip the
                # action_queue entirely. This is the optimization.
                # --------------------------------------------------------------
                action = RunnableAction(
                    id=self.queue.next_id(),
                    node_id=node_id,
                    function_name=node.function_name,
                    action_type=ActionType.INLINE,
                    input_data=node_data.variable_values.copy(),
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
        - All its dependencies are satisfied (check inbox for aggregators)

        Returns nodes in order suitable for processing.
        """
        node = self.dag.nodes.get(node_id)
        if node is None:
            return []

        ready = []
        node_data = self._get_node_data(node_id)

        # If the source node is not completed AND it's an action call with retry policies,
        # don't trigger successors (a retry was scheduled)
        # Skip this check for parallel/spread items which are never marked complete individually
        if not node_data.completed:
            # Check if this is an action call with retry policies
            has_retry_policy = False
            ir = node.ir_node
            if isinstance(ir, RappelAssignment) and isinstance(ir.value, RappelActionCall):
                if ir.value.retry_policies:
                    has_retry_policy = True
            elif isinstance(ir, RappelExprStatement) and isinstance(ir.expr, RappelActionCall):
                if ir.expr.retry_policies:
                    has_retry_policy = True
            elif isinstance(ir, RappelActionCall):
                if ir.retry_policies:
                    has_retry_policy = True

            if has_retry_policy:
                # Retry was scheduled, don't trigger successors yet
                return []

        # Check if this node has an exception (is in a try block and failed)
        if node_data.variable_values.get("_exception"):
            # Find the parent try node by looking for incoming "try" edge
            return self._route_to_except_handler(node_id, node_data)

        # Handle try/except - only follow "try" edge, not "except" edges
        # Except edges are only followed when an exception occurs (handled above)
        if node.ir_node and isinstance(node.ir_node, RappelTryExcept):
            for edge in self.dag.get_outgoing_edges(node_id):
                if edge.edge_type != EdgeType.STATE_MACHINE:
                    continue
                # Only follow the "try" edge, not "except:*" edges
                if edge.condition and edge.condition.startswith("except:"):
                    continue
                target_node = self.dag.nodes.get(edge.target)
                if target_node:
                    target_data = self._get_node_data(edge.target)
                    if not target_data.completed:
                        ready.append(edge.target)
            return ready

        # Handle parallel blocks - the call nodes are already queued by _handle_parallel_block
        # Don't return them as successors to avoid double-queuing
        if node.ir_node and isinstance(node.ir_node, RappelParallelBlock):
            # Parallel blocks queue their own actions, no successors to return here
            return []

        # Handle conditional branching (read condition from node_data)
        if node.ir_node and isinstance(node.ir_node, RappelIfStatement):
            # Read condition result from node's variable_values (stored there after execution)
            condition_result = node_data.variable_values.get("_condition", True)

            for edge in self.dag.get_outgoing_edges(node_id):
                if edge.edge_type != EdgeType.STATE_MACHINE:
                    continue

                # Filter by condition
                if edge.condition == "then" and not condition_result:
                    continue
                if edge.condition == "else" and condition_result:
                    continue

                target_node = self.dag.nodes.get(edge.target)
                if target_node:
                    target_data = self._get_node_data(edge.target)
                    if not target_data.completed:
                        # Check if aggregator is ready (from inbox)
                        if target_node.is_aggregator:
                            inbox_results = self._read_inbox_for_aggregator(edge.target)
                            if target_data.pending_inputs > 0 and len(inbox_results) < target_data.pending_inputs:
                                continue
                        ready.append(edge.target)
            return ready

        # Normal node - follow all state machine edges
        for edge in self.dag.get_outgoing_edges(node_id):
            if edge.edge_type != EdgeType.STATE_MACHINE:
                continue

            target_node = self.dag.nodes.get(edge.target)
            if target_node is None:
                continue

            target_data = self._get_node_data(edge.target)
            if target_data.completed:
                continue

            # Check if aggregator is ready (from inbox)
            if target_node.is_aggregator:
                inbox_results = self._read_inbox_for_aggregator(edge.target)
                if target_data.pending_inputs > 0 and len(inbox_results) < target_data.pending_inputs:
                    continue

            ready.append(edge.target)

        return ready

    def _route_to_except_handler(self, node_id: str, node_data: RunnableActionData) -> list[str]:
        """
        Route from a failed try-block node to the matching except handler.

        Finds the parent try node and returns the appropriate except handler
        based on the exception type.
        """
        exception_type = node_data.variable_values.get("_exception_type", "Exception")

        # Find the parent try node by looking for incoming edge with "try" condition
        try_node_id = None
        for edge in self.dag.edges:
            if edge.target == node_id and edge.condition == "try":
                try_node_id = edge.source
                break

        if try_node_id is None:
            # No try node found - this shouldn't happen
            return []

        # Get all except handlers from the try node
        except_handlers = []
        catch_all_handler = None

        for edge in self.dag.get_outgoing_edges(try_node_id):
            if edge.condition and edge.condition.startswith("except:"):
                handler_types = edge.condition[7:]  # Remove "except:" prefix
                if handler_types == "*":
                    catch_all_handler = edge.target
                else:
                    except_handlers.append((handler_types.split(", "), edge.target))

        # Find matching handler
        for types, handler_id in except_handlers:
            if exception_type in types:
                handler_data = self._get_node_data(handler_id)
                if not handler_data.completed:
                    return [handler_id]

        # Fall back to catch-all handler
        if catch_all_handler:
            handler_data = self._get_node_data(catch_all_handler)
            if not handler_data.completed:
                return [catch_all_handler]

        return []

    def _reset_function_state(self, function_name: str) -> None:
        """Reset state for all nodes in a function (DB writes)."""
        fn_nodes = self.dag.get_nodes_for_function(function_name)
        for node_id in fn_nodes:
            # Reset node data
            self._set_node_data(node_id, RunnableActionData(node_id=node_id))
            # Clear any cached results
            self._results_table.delete(node_id)
        # Clear the inbox table (for re-runs)
        self._node_inputs_table.clear()

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
            # Read scope from INBOX (single query for all inputs)
            scope = self._read_inbox(node.id)

            # Execute the action with the scope from inbox
            if action.action_type == ActionType.DELEGATED:
                result = self._handle_delegated(node, action, scope)
            else:
                result = self._handle_inline(node, action, scope)

            action.status = ActionStatus.COMPLETED

            # Store result in DB (use spread_index/parallel_index for parallel items to avoid overwrite)
            # Use whichever index is set (spread or parallel)
            item_index = action.spread_index if action.spread_index is not None else action.parallel_index
            if item_index is not None:
                result_key = f"{node.id}::{item_index}"
            else:
                result_key = node.id
            self._set_result(result_key, result)

            # Mark node as completed (single write)
            # Skip for spread/parallel items - they don't update the parent node's state
            if item_index is None:
                node_data = self._get_node_data(node.id)
                node_data.completed = True
                # Store _condition for if statements so _get_ready_successors can read it
                if isinstance(result, dict) and "_condition" in result:
                    node_data.variable_values["_condition"] = result["_condition"]
                self._set_node_data(node.id, node_data)

            # Push outputs to dependent nodes (DB writes to targets)
            self._push_outputs_to_targets(node.id, result, action)

            # Note: Successor handling is done by _execute_inline_batch, not here

        except Exception as e:
            # First, check if this action has a retry policy that matches the exception
            matching_policy = self._find_matching_retry_policy(node, e)

            if matching_policy and action.retries_remaining > 0:
                # Schedule a retry with exponential backoff
                retry_action = self.queue.schedule_retry(action)
                if retry_action:
                    action.status = ActionStatus.COMPLETED  # Original action is done
                    # Don't mark node as completed - the retry will handle it
                    return

            # Check if this node is inside a try block (has a "success" outgoing edge)
            in_try_block = any(
                edge.condition == "success"
                for edge in self.dag.get_outgoing_edges(node.id)
            )

            if in_try_block:
                # Store exception info and mark as completed (with exception)
                # This allows _get_ready_successors to route to except handler
                action.status = ActionStatus.COMPLETED
                node_data = self._get_node_data(node.id)
                node_data.completed = True
                node_data.variable_values["_exception"] = True
                node_data.variable_values["_exception_type"] = type(e).__name__
                node_data.variable_values["_exception_message"] = str(e)
                self._set_node_data(node.id, node_data)
                # Don't push outputs - exception handler will provide them
            else:
                action.status = ActionStatus.FAILED
                raise RuntimeError(f"Action failed for node {node.id}: {e}") from e

    def _handle_inline(self, node: DAGNode, action: RunnableAction, scope: dict[str, Any]) -> Any:
        """
        Handle inline execution (AST evaluation).

        This handles assignments, expressions, control flow, etc.
        Scope is passed in to avoid redundant DB reads.
        """
        # Add spread item if this is a spread iteration
        if action.spread_item is not None and node.loop_vars:
            # For a single loop var, just assign the item directly
            # For multiple loop vars, unpack the item (which should be a list)
            if len(node.loop_vars) == 1:
                scope = {**scope, node.loop_vars[0]: action.spread_item}
            else:
                # Unpack the item into multiple variables
                item = action.spread_item
                for i, var in enumerate(node.loop_vars):
                    scope = {**scope, var: item[i]}

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

        elif isinstance(ir_node, RappelTryExcept):
            # Try node just passes through - actual exception handling is done
            # by modifying how we route after action execution
            return scope

        elif isinstance(ir_node, RappelExceptHandler):
            # Except handler node just passes through - body is in child nodes
            return scope

        elif isinstance(ir_node, RappelExprStatement):
            self._evaluate_expr(ir_node.expr, scope)
            return scope

        elif isinstance(ir_node, RappelSpreadAction):
            return self._handle_spread_action(node, ir_node, scope, action)

        elif isinstance(ir_node, RappelParallelBlock):
            return self._handle_parallel_block(node, ir_node, scope, action)

        return scope

    def _handle_delegated(self, node: DAGNode, action: RunnableAction, scope: dict[str, Any]) -> Any:
        """
        Handle delegated execution (@actions and function calls).

        These are pushed to external workers.
        Scope is passed in to avoid redundant DB reads.
        """
        ir_node = node.ir_node

        if ir_node is None:
            return scope

        # Handle function calls (from parallel blocks)
        if isinstance(ir_node, RappelCall):
            result = self._execute_function_call(ir_node, scope)
            return {"_result": result}

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
        elif isinstance(ir_node, RappelActionCall):
            # Direct action call (from parallel blocks)
            action_call = ir_node

        if action_call is None:
            return scope

        # Handle built-in @sleep action - it's a no-op since the delay
        # was handled by scheduled_at when the action was queued
        if action_call.action_name == "sleep":
            return scope

        # Evaluate kwargs
        kwargs = {}

        # For spread actions, inject the spread item as the loop variable
        if action.spread_item is not None:
            item_var = action.input_data.get("_item_var")
            if item_var:
                scope = {**scope, item_var: action.spread_item}

        for name, expr in action_call.kwargs:
            kwargs[name] = self._evaluate_expr(expr, scope)

        # Call the action handler
        handler = self.action_handlers.get(action_call.action_name)
        if handler is None:
            raise ValueError(f"No handler registered for action '@{action_call.action_name}'")

        result = handler(**kwargs)

        if target_var:
            return {target_var: result}
        # For parallel actions without assignment, return the result for aggregation
        return {"_result": result}

    def _handle_for_loop(self, node: DAGNode, ir_node: RappelForLoop, scope: dict[str, Any]) -> Any:
        """Handle for loop execution."""
        iterable = self._evaluate_expr(ir_node.iterable, scope)

        # For loops in this DSL have a single function call in the body
        # Execute the body for each item, accumulating changes to scope
        loop_scope = scope.copy()
        results = []

        for item in iterable:
            # Unpack item into loop variables
            if len(ir_node.loop_vars) == 1:
                loop_scope[ir_node.loop_vars[0]] = item
            else:
                # Multiple loop vars - item should be a list/tuple to unpack
                for i, var in enumerate(ir_node.loop_vars):
                    loop_scope[var] = item[i]

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
            # Set up aggregator to expect results (DB read + write)
            agg_data = self._get_node_data(agg_node_id)
            agg_data.pending_inputs = len(source_list)
            agg_data.collected_results = []
            self._set_node_data(agg_node_id, agg_data)

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

    def _handle_parallel_block(
        self,
        node: DAGNode,
        ir_node: RappelParallelBlock,
        scope: dict[str, Any],
        action: RunnableAction
    ) -> Any:
        """
        Handle parallel block - queues multiple actions/calls concurrently.

        parallel:
            @action_a()
            @action_b()
            func_c()
        """
        # Find the aggregator node - it's connected to the call nodes, not the parallel node
        # Look at outgoing edges of call nodes to find the aggregator
        agg_node_id = None
        for edge in self.dag.get_outgoing_edges(node.id):
            if edge.condition and edge.condition.startswith("parallel:"):
                call_node_id = edge.target
                # Look for aggregator in call node's successors
                for call_edge in self.dag.get_outgoing_edges(call_node_id):
                    target_node = self.dag.nodes.get(call_edge.target)
                    if target_node and target_node.is_aggregator:
                        agg_node_id = call_edge.target
                        break
                if agg_node_id:
                    break

        if agg_node_id:
            # Set up aggregator to expect results from all calls (DB read + write)
            agg_data = self._get_node_data(agg_node_id)
            agg_data.pending_inputs = len(ir_node.calls)
            agg_data.collected_results = []
            self._set_node_data(agg_node_id, agg_data)

        # Queue each call as a separate action
        # The DAG already has nodes for each call - find them via edges
        for edge in self.dag.get_outgoing_edges(node.id):
            if edge.condition and edge.condition.startswith("parallel:"):
                # Get the parallel index from the condition
                parallel_idx = int(edge.condition.split(":")[1])
                call_node_id = edge.target
                call_node = self.dag.nodes.get(call_node_id)

                if call_node is None:
                    continue

                # Determine action type based on call type
                call_ir = call_node.ir_node
                if isinstance(call_ir, RappelActionCall):
                    action_type = ActionType.DELEGATED
                elif isinstance(call_ir, RappelCall):
                    # Function calls are inline but still need to go through queue
                    # for proper ordering
                    action_type = ActionType.DELEGATED
                else:
                    continue

                parallel_action = RunnableAction(
                    id=self.queue.next_id(),
                    node_id=call_node_id,
                    function_name=call_node.function_name,
                    action_type=action_type,
                    input_data={
                        **scope,
                        "_parallel_index": parallel_idx,
                    },
                    parallel_index=parallel_idx,
                )
                self.queue.add(parallel_action)

        # Return empty - results will be collected by aggregator
        return {}

    def _handle_aggregator(self, node: DAGNode) -> Any:
        """Handle aggregator node - collect spread/parallel results from INBOX."""
        data = self._get_node_data(node.id)

        # Read results from inbox (single query)
        collected_results = self._read_inbox_for_aggregator(node.id)

        # Check if all results are in
        if len(collected_results) < data.pending_inputs:
            # Not ready yet
            return {}

        # Sort by index and extract values
        sorted_results = sorted(collected_results, key=lambda x: x[0])
        values = [r[1] for r in sorted_results]

        # Get the target variable from the aggregates_from node
        source_node = self.dag.nodes.get(node.aggregates_from) if node.aggregates_from else None
        target_var = None

        if source_node and source_node.ir_node:
            if isinstance(source_node.ir_node, RappelSpreadAction):
                target_var = source_node.ir_node.target
            elif isinstance(source_node.ir_node, RappelParallelBlock):
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
        Get the variable scope for a node (DB read).

        Collects values from all data flow edges pointing to this node.
        """
        scope = {}

        # Get values from node data (pushed from predecessors)
        try:
            node_data = self._get_node_data(node_id)
            scope.update(node_data.variable_values)
        except KeyError:
            pass

        return scope

    def _push_outputs(self, node_id: str, outputs: Any) -> None:
        """
        Push output values to dependent nodes using INBOX PATTERN.

        Simple version for non-action contexts (e.g., input node initialization).
        Uses append-only writes - no reads required!
        """
        if not isinstance(outputs, dict):
            return

        # Push to successors via inbox (append-only, no locks!)
        for edge in self.dag.get_outgoing_edges(node_id):
            if edge.edge_type == EdgeType.DATA_FLOW:
                if edge.variable and edge.variable in outputs:
                    self._append_to_inbox(
                        target_node_id=edge.target,
                        variable=edge.variable,
                        value=outputs[edge.variable],
                        source_node_id=node_id,
                    )
            elif edge.edge_type == EdgeType.STATE_MACHINE:
                for var, val in outputs.items():
                    self._append_to_inbox(
                        target_node_id=edge.target,
                        variable=var,
                        value=val,
                        source_node_id=node_id,
                    )

    def _push_outputs_to_targets(self, node_id: str, outputs: Any, action: RunnableAction) -> None:
        """
        Push output values to dependent nodes using INBOX PATTERN.

        Instead of read-modify-write on each target, we just append to inbox.
        O(1) writes per target, no locks needed!
        """
        if not isinstance(outputs, dict):
            return

        # Find data flow edges from this node
        for edge in self.dag.get_outgoing_edges(node_id):
            target_node = self.dag.nodes.get(edge.target)
            if target_node is None:
                continue

            # Handle aggregator collection for spread/parallel actions
            item_index = action.spread_index if action.spread_index is not None else action.parallel_index
            if target_node.is_aggregator and item_index is not None:
                # This is a spread/parallel item result - append with index for ordering
                for var, val in outputs.items():
                    # Skip internal variables EXCEPT _result which holds action results
                    if var.startswith("_") and var != "_result":
                        continue
                    # Append to aggregator's inbox with index
                    self._append_to_inbox(
                        target_node_id=edge.target,
                        variable=f"_aggregate_{var}",
                        value=val,
                        source_node_id=node_id,
                        spread_index=item_index,  # Reuse spread_index field for ordering
                    )
            else:
                # Normal data flow - append to target's inbox
                if edge.edge_type == EdgeType.DATA_FLOW:
                    if edge.variable and edge.variable in outputs:
                        self._append_to_inbox(
                            target_node_id=edge.target,
                            variable=edge.variable,
                            value=outputs[edge.variable],
                            source_node_id=node_id,
                        )
                elif edge.edge_type == EdgeType.STATE_MACHINE:
                    # Push all outputs for execution order edges
                    # Include _condition for conditional branching
                    for var, val in outputs.items():
                        if not var.startswith("_") or var == "_condition":
                            self._append_to_inbox(
                                target_node_id=edge.target,
                                variable=var,
                                value=val,
                                source_node_id=node_id,
                            )

    def _queue_successors(self, node_id: str) -> None:
        """
        Queue actions for successor nodes (DB reads).

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

            # Skip if already completed (DB read)
            target_data = self._get_node_data(edge.target)
            if target_data.completed:
                continue

            # Check if target is an aggregator
            if target_node.is_aggregator:
                # Only queue if all inputs are ready
                if target_data.pending_inputs > 0 and len(target_data.collected_results) < target_data.pending_inputs:
                    continue

            # Determine action type
            action_type = self._get_action_type(target_node)

            action = RunnableAction(
                id=self.queue.next_id(),
                node_id=edge.target,
                function_name=target_node.function_name,
                action_type=action_type,
                input_data=target_data.variable_values.copy(),
            )
            self.queue.add(action)

    def _queue_conditional_successors(self, node_id: str) -> None:
        """Queue the appropriate branch for a conditional (DB reads)."""
        node_data = self._get_node_data(node_id)
        condition_result = node_data.variable_values.get("_condition", True)

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

            target_data = self._get_node_data(edge.target)
            action = RunnableAction(
                id=self.queue.next_id(),
                node_id=edge.target,
                function_name=target_node.function_name,
                action_type=action_type,
                input_data=target_data.variable_values.copy(),
            )
            self.queue.add(action)

    def _get_action_type(self, node: DAGNode) -> ActionType:
        """Determine whether a node should be handled inline or delegated."""
        if node.ir_node is None:
            return ActionType.INLINE

        # Direct action calls (from parallel blocks) are delegated
        if isinstance(node.ir_node, RappelActionCall):
            return ActionType.DELEGATED

        # Direct function calls (from parallel blocks) are delegated
        if isinstance(node.ir_node, RappelCall):
            return ActionType.DELEGATED

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

    def _get_scheduled_at(self, node: DAGNode, scope: dict[str, Any]) -> float | None:
        """
        Get the scheduled_at time for an action, if applicable.

        Handles @sleep(duration) by returning time.time() + duration.
        Returns None for all other actions (execute immediately).
        """
        if node.ir_node is None:
            return None

        # Extract action call from various node types
        action_call = None

        if isinstance(node.ir_node, RappelExprStatement):
            if isinstance(node.ir_node.expr, RappelActionCall):
                action_call = node.ir_node.expr
        elif isinstance(node.ir_node, RappelAssignment):
            if isinstance(node.ir_node.value, RappelActionCall):
                action_call = node.ir_node.value

        if action_call is None:
            return None

        # Check if this is a @sleep action
        if action_call.action_name == "sleep":
            # Evaluate the duration argument
            for name, expr in action_call.kwargs:
                if name == "duration":
                    duration = self._evaluate_expr(expr, scope)
                    return time.time() + duration
            # If no duration kwarg, check for positional (first arg)
            if action_call.kwargs:
                _, first_expr = action_call.kwargs[0]
                duration = self._evaluate_expr(first_expr, scope)
                return time.time() + duration

        return None

    def _is_sleep_action(self, node: DAGNode) -> bool:
        """Check if a node is a @sleep action."""
        if node.ir_node is None:
            return False

        action_call = None

        if isinstance(node.ir_node, RappelExprStatement):
            if isinstance(node.ir_node.expr, RappelActionCall):
                action_call = node.ir_node.expr
        elif isinstance(node.ir_node, RappelAssignment):
            if isinstance(node.ir_node.value, RappelActionCall):
                action_call = node.ir_node.value

        return action_call is not None and action_call.action_name == "sleep"


class ThreadedDAGRunner(DAGRunner):
    """
    Multi-threaded DAG runner that simulates distributed execution.

    Uses a pool of worker threads that compete for actions from a shared
    queue, simulating multiple machines connected to the same database.

    Each worker:
    1. Polls the action queue (SELECT ... FOR UPDATE SKIP LOCKED)
    2. Executes the action
    3. Pushes results and queues successors
    4. Repeats until shutdown

    Thread safety is handled at the DB level, making this a realistic
    simulation of a distributed task broker.
    """

    def __init__(
        self,
        dag: DAG,
        action_handlers: dict[str, Callable[..., Any]] | None = None,
        function_handlers: dict[str, Callable[..., Any]] | None = None,
        db: InMemoryDB | None = None,
        num_workers: int = 4,
    ):
        """
        Initialize the threaded DAG runner.

        Args:
            dag: The DAG to execute
            action_handlers: Dict of action_name -> handler function for @actions
            function_handlers: Dict of function_name -> handler function for fn calls
            db: Optional database instance (uses global singleton if not provided)
            num_workers: Number of worker threads (simulates machines in a cluster)
        """
        # Use global DB singleton if not provided - simulates shared database
        if db is None:
            db = get_db()

        super().__init__(dag, action_handlers, function_handlers, db)

        self.num_workers = num_workers
        self._workers: list[threading.Thread] = []
        self._shutdown_event = threading.Event()
        self._active_workers = 0
        self._active_lock = threading.Lock()
        self._completion_event = threading.Event()
        self._error: Exception | None = None
        self._error_lock = threading.Lock()

        # Execution log for debugging (thread-safe)
        self._execution_log: list[tuple[str, str, float]] = []
        self._log_lock = threading.Lock()

    def _log_execution(self, worker_id: str, message: str) -> None:
        """Thread-safe logging of execution events."""
        with self._log_lock:
            self._execution_log.append((worker_id, message, time.time()))

    def get_execution_log(self) -> list[tuple[str, str, float]]:
        """Get a copy of the execution log."""
        with self._log_lock:
            return list(self._execution_log)

    def run(self, function_name: str, inputs: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Run a function using the worker pool.

        Starts workers, seeds initial work, waits for completion.
        """
        inputs = inputs or {}

        # Clear previous state
        self._execution_log.clear()
        self._shutdown_event.clear()
        self._completion_event.clear()
        self._error = None

        # Reset state for this function's nodes
        self._reset_function_state(function_name)

        # Find the input node for this function
        input_node = self._find_input_node(function_name)
        if not input_node:
            raise ValueError(f"Function '{function_name}' not found in DAG")

        # Initialize input values (DB write)
        input_data = self._get_node_data(input_node.id)
        input_data.variable_values = inputs.copy()
        input_data.completed = True
        self._set_node_data(input_node.id, input_data)

        # Push input values to successor nodes
        self._push_outputs(input_node.id, inputs)

        # Execute initial inline nodes (seeds the delegated action queue)
        self._execute_inline_batch(input_node.id)

        # Start worker threads
        self._start_workers()

        # Wait for completion or error
        self._wait_for_completion()

        # Check for errors
        if self._error:
            raise self._error

        # Find and return outputs (read from INBOX)
        output_node = self._find_output_node(function_name)
        if output_node:
            return self._read_inbox(output_node.id)
        return {}

    def _start_workers(self) -> None:
        """Start the worker threads."""
        self._workers.clear()
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                args=(f"worker-{i}",),
                daemon=True,
            )
            self._workers.append(worker)
            worker.start()

    def _worker_loop(self, worker_id: str) -> None:
        """
        Main loop for a worker thread.

        Continuously polls for work until shutdown signal.
        """
        self._log_execution(worker_id, "started")

        while not self._shutdown_event.is_set():
            # Try to get an action from the queue
            action = self.queue.pop()

            if action is None:
                # No work available - check if we should exit
                if self._check_completion():
                    break

                # Check if there are scheduled actions waiting
                next_time = self.queue.get_next_scheduled_time()
                if next_time is not None:
                    # Wait until the next scheduled action is ready
                    wait_time = min(next_time - time.time(), 0.1)  # Cap at 100ms
                    if wait_time > 0:
                        time.sleep(wait_time)
                else:
                    # Brief sleep to avoid busy-waiting
                    time.sleep(0.001)
                continue

            # Mark ourselves as active
            with self._active_lock:
                self._active_workers += 1

            try:
                self._log_execution(worker_id, f"executing {action.node_id}")

                # Execute the delegated action
                self._execute_action(action)

                # After delegated action completes, eagerly execute inline successors
                # This may add more delegated actions to the queue
                self._execute_inline_batch(action.node_id)

                self._log_execution(worker_id, f"completed {action.node_id}")

            except Exception as e:
                with self._error_lock:
                    if self._error is None:
                        self._error = e
                self._shutdown_event.set()
                self._log_execution(worker_id, f"error: {e}")

            finally:
                with self._active_lock:
                    self._active_workers -= 1

        self._log_execution(worker_id, "stopped")

    def _check_completion(self) -> bool:
        """
        Check if all work is complete.

        Work is complete when:
        1. The action queue is empty (no ready actions AND no scheduled actions)
        2. No workers are currently processing actions
        """
        with self._active_lock:
            # Check if there are any actions at all (ready or scheduled)
            queue_empty = self.queue.is_empty()
            has_scheduled = self.queue.has_scheduled_actions() if not queue_empty else False

            if queue_empty and self._active_workers == 0:
                self._completion_event.set()
                self._shutdown_event.set()
                return True

            # If there are only scheduled actions, don't exit yet
            if not queue_empty and has_scheduled and self._active_workers == 0:
                return False
        return False

    def _wait_for_completion(self, timeout: float = 30.0) -> None:
        """Wait for all workers to complete."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self._completion_event.wait(timeout=0.1):
                break

            # Check for errors
            with self._error_lock:
                if self._error:
                    self._shutdown_event.set()
                    break

        # Signal shutdown and wait for workers
        self._shutdown_event.set()
        for worker in self._workers:
            worker.join(timeout=1.0)

    def print_execution_log(self) -> None:
        """Print the execution log for debugging."""
        log = self.get_execution_log()
        if not log:
            print("No execution log entries")
            return

        start_time = log[0][2]
        for worker_id, message, timestamp in log:
            elapsed = timestamp - start_time
            print(f"[{elapsed:.4f}s] {worker_id}: {message}")
