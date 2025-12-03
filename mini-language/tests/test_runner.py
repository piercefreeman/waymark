"""Tests for Rappel DAG Runner."""

import pytest

import time

from rappel import (
    parse,
    convert_to_dag,
    ActionStatus,
    ActionType,
    RunnableAction,
    RunnableActionData,
    ActionQueue,
    DAGRunner,
    ThreadedDAGRunner,
    InMemoryDB,
)


class TestActionQueue:
    """Tests for ActionQueue."""

    def test_queue_add_and_pop(self):
        """Test adding and popping actions from queue."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.INLINE,
        )
        queue.add(action)

        assert queue.size() == 1
        assert not queue.is_empty()

        popped = queue.pop()
        assert popped.id == action.id
        assert queue.is_empty()

    def test_queue_fifo_order(self):
        """Test that queue maintains FIFO order."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        for i in range(3):
            queue.add(RunnableAction(
                id=f"action_{i}",
                node_id=f"node_{i}",
                function_name="test",
                action_type=ActionType.INLINE,
            ))

        assert queue.pop().id == "action_0"
        assert queue.pop().id == "action_1"
        assert queue.pop().id == "action_2"

    def test_queue_peek(self):
        """Test peeking at next action without removing."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.INLINE,
        )
        queue.add(action)

        peeked = queue.peek()
        assert peeked.id == action.id
        assert queue.size() == 1  # Still in queue

    def test_queue_pop_empty(self):
        """Test popping from empty queue returns None."""
        db = InMemoryDB()
        queue = ActionQueue(db)
        assert queue.pop() is None

    def test_queue_next_id(self):
        """Test generating unique action IDs."""
        db = InMemoryDB()
        queue = ActionQueue(db)
        id1 = queue.next_id()
        id2 = queue.next_id()
        assert id1 != id2

    def test_queue_db_stats(self):
        """Test that queue operations track DB stats."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        # Add an action (should be a write)
        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.INLINE,
        )
        queue.add(action)
        assert db.stats.writes == 1

        # Pop the action (simulates SELECT ... FOR UPDATE SKIP LOCKED + DELETE)
        queue.pop()
        assert db.stats.queries == 1  # The SELECT query
        assert db.stats.deletes == 1  # The DELETE


class TestRunnableAction:
    """Tests for RunnableAction dataclass."""

    def test_action_default_status(self):
        """Test that actions default to PENDING status."""
        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.INLINE,
        )
        assert action.status == ActionStatus.PENDING

    def test_action_with_input_data(self):
        """Test action with input data."""
        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.DELEGATED,
            input_data={"x": 10, "y": 20},
        )
        assert action.input_data == {"x": 10, "y": 20}

    def test_action_spread_fields(self):
        """Test spread-related fields."""
        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.DELEGATED,
            spread_index=2,
            spread_item={"id": 123},
        )
        assert action.spread_index == 2
        assert action.spread_item == {"id": 123}


class TestRunnableActionData:
    """Tests for RunnableActionData dataclass."""

    def test_action_data_defaults(self):
        """Test default values."""
        data = RunnableActionData(node_id="node_1")
        assert data.variable_values == {}
        assert data.completed is False
        assert data.pending_inputs == 0
        assert data.collected_results == []

    def test_action_data_with_values(self):
        """Test with variable values."""
        data = RunnableActionData(
            node_id="node_1",
            variable_values={"x": 10},
            completed=True,
        )
        assert data.variable_values == {"x": 10}
        assert data.completed is True


class TestDAGRunnerSimple:
    """Tests for simple DAG execution."""

    def test_run_simple_function(self):
        """Test running a simple function with assignment."""
        source = """fn double(input: [x], output: [result]):
    result = x * 2
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("double", {"x": 5})
        assert outputs.get("result") == 10

    def test_run_function_with_addition(self):
        """Test function with binary addition."""
        source = """fn add(input: [a, b], output: [sum]):
    sum = a + b
    return sum"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("add", {"a": 3, "b": 7})
        assert outputs.get("sum") == 10

    def test_run_function_with_chained_ops(self):
        """Test function with chained operations."""
        source = """fn compute(input: [x], output: [result]):
    y = x + 1
    result = y * 2
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("compute", {"x": 4})
        assert outputs.get("result") == 10  # (4 + 1) * 2

    def test_run_function_not_found(self):
        """Test error when function not found."""
        source = """fn test(input: [], output: [x]):
    x = 1
    return x"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        with pytest.raises(ValueError, match="not found"):
            runner.run("nonexistent", {})


class TestDAGRunnerActions:
    """Tests for @action execution."""

    def test_run_with_action_handler(self):
        """Test running a function that calls an @action."""
        source = """fn fetch(input: [url], output: [data]):
    data = @get_data(url=url)
    return data"""

        program = parse(source)
        dag = convert_to_dag(program)

        # Mock action handler
        def mock_get_data(url):
            return {"fetched": url}

        runner = DAGRunner(dag, action_handlers={"get_data": mock_get_data})

        outputs = runner.run("fetch", {"url": "http://example.com"})
        assert outputs.get("data") == {"fetched": "http://example.com"}

    def test_action_without_handler_raises(self):
        """Test that unregistered action raises error."""
        source = """fn fetch(input: [url], output: [data]):
    data = @unknown_action(url=url)
    return data"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        with pytest.raises(RuntimeError):
            runner.run("fetch", {"url": "test"})


class TestDAGRunnerConditionals:
    """Tests for conditional execution."""

    def test_run_if_then_branch(self):
        """Test if statement taking then branch."""
        source = """fn classify(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("classify", {"x": 5})
        assert outputs.get("result") == "positive"

    def test_run_if_else_branch(self):
        """Test if statement taking else branch."""
        source = """fn classify(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("classify", {"x": -3})
        assert outputs.get("result") == "negative"


class TestDAGRunnerExpressions:
    """Tests for expression evaluation."""

    def test_evaluate_list_expression(self):
        """Test list creation."""
        source = """fn make_list(input: [], output: [items]):
    items = [1, 2, 3]
    return items"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("make_list", {})
        assert outputs.get("items") == [1, 2, 3]

    def test_evaluate_dict_expression(self):
        """Test dict creation."""
        source = """fn make_dict(input: [], output: [config]):
    config = {"a": 1, "b": 2}
    return config"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("make_dict", {})
        assert outputs.get("config") == {"a": 1, "b": 2}

    def test_evaluate_index_access(self):
        """Test list/dict index access."""
        source = """fn get_first(input: [items], output: [first]):
    first = items[0]
    return first"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("get_first", {"items": [10, 20, 30]})
        assert outputs.get("first") == 10

    def test_evaluate_comparison_operators(self):
        """Test comparison operators."""
        source = """fn compare(input: [a, b], output: [result]):
    result = a == b
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        assert runner.run("compare", {"a": 5, "b": 5}).get("result") is True
        assert runner.run("compare", {"a": 5, "b": 3}).get("result") is False

    def test_evaluate_unary_not(self):
        """Test unary not operator."""
        source = """fn negate(input: [x], output: [result]):
    result = not x
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        assert runner.run("negate", {"x": True}).get("result") is False
        assert runner.run("negate", {"x": False}).get("result") is True

    def test_evaluate_unary_minus(self):
        """Test unary minus operator."""
        source = """fn negative(input: [x], output: [result]):
    result = -x
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        assert runner.run("negative", {"x": 5}).get("result") == -5


class TestDAGRunnerFunctionCalls:
    """Tests for function-to-function calls."""

    def test_function_calls_another_with_handler(self):
        """Test function calling another function via handler."""
        source = """fn outer(input: [x], output: [result]):
    result = inner(val=x)
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        # Mock function handler
        def mock_inner(val):
            return val * 3

        runner = DAGRunner(dag, function_handlers={"inner": mock_inner})

        outputs = runner.run("outer", {"x": 4})
        assert outputs.get("result") == 12


class TestActionTypes:
    """Tests for action type enums."""

    def test_action_status_values(self):
        """Test ActionStatus enum values."""
        assert ActionStatus.PENDING.value
        assert ActionStatus.RUNNING.value
        assert ActionStatus.COMPLETED.value
        assert ActionStatus.FAILED.value

    def test_action_type_values(self):
        """Test ActionType enum values."""
        assert ActionType.INLINE.value
        assert ActionType.DELEGATED.value


class TestThreadedDAGRunner:
    """Tests for multi-threaded DAG execution."""

    def test_threaded_simple_function(self):
        """Test running a simple function with threads."""
        source = """fn double(input: [x], output: [result]):
    result = x * 2
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()
        runner = ThreadedDAGRunner(dag, db=db, num_workers=2)

        outputs = runner.run("double", {"x": 5})
        assert outputs.get("result") == 10

    def test_threaded_with_action_handler(self):
        """Test threaded runner with @action."""
        source = """fn fetch(input: [url], output: [data]):
    data = @get_data(url=url)
    return data"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()

        def mock_get_data(url):
            return {"fetched": url}

        runner = ThreadedDAGRunner(
            dag,
            action_handlers={"get_data": mock_get_data},
            db=db,
            num_workers=2,
        )

        outputs = runner.run("fetch", {"url": "http://example.com"})
        assert outputs.get("data") == {"fetched": "http://example.com"}

    def test_threaded_parallel_actions(self):
        """Test that multiple actions run in parallel with threads."""
        source = """fn process(input: [], output: [results]):
    items = @get_items()
    processed = spread items:item -> @process_item(id=item)
    results = processed
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()

        execution_times = []

        def mock_get_items():
            return [1, 2, 3, 4]

        def mock_process_item(id):
            start = time.time()
            time.sleep(0.02)  # Simulate work
            execution_times.append((id, time.time() - start))
            return {"id": id, "processed": True}

        runner = ThreadedDAGRunner(
            dag,
            action_handlers={
                "get_items": mock_get_items,
                "process_item": mock_process_item,
            },
            db=db,
            num_workers=4,
        )

        start = time.time()
        outputs = runner.run("process", {})
        elapsed = time.time() - start

        # Verify results
        results = outputs.get("results")
        assert len(results) == 4
        assert all(r["processed"] for r in results)

        # With 4 workers and 4 items taking 0.02s each,
        # parallel execution should be faster than sequential (0.08s)
        # Allow some overhead, but should be under 0.06s
        assert elapsed < 0.08, f"Expected parallel execution, got {elapsed:.3f}s"

    def test_threaded_execution_log(self):
        """Test that execution log tracks worker activity."""
        source = """fn test(input: [], output: [x]):
    x = @action1()
    return x"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()

        def mock_action1():
            return 42

        runner = ThreadedDAGRunner(
            dag,
            action_handlers={"action1": mock_action1},
            db=db,
            num_workers=2,
        )

        runner.run("test", {})

        log = runner.get_execution_log()
        assert len(log) > 0

        # Check that log contains worker started/stopped messages
        messages = [entry[1] for entry in log]
        assert any("started" in msg for msg in messages)
        assert any("stopped" in msg for msg in messages)

    def test_threaded_correct_result_ordering(self):
        """Test that spread results maintain correct order."""
        source = """fn ordered(input: [], output: [results]):
    items = @get_sequence()
    doubled = spread items:x -> @double(n=x)
    results = doubled
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()

        def mock_get_sequence():
            return [10, 20, 30, 40, 50]

        def mock_double(n):
            # Add varying delays to mix up completion order
            time.sleep(0.01 * (5 - n // 10))
            return n * 2

        runner = ThreadedDAGRunner(
            dag,
            action_handlers={
                "get_sequence": mock_get_sequence,
                "double": mock_double,
            },
            db=db,
            num_workers=4,
        )

        outputs = runner.run("ordered", {})
        results = outputs.get("results")

        # Results should be in original order despite varying completion times
        assert results == [20, 40, 60, 80, 100]
