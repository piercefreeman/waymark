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


class TestDAGRunnerTryExcept:
    """Tests for try/except exception handling."""

    def test_try_succeeds(self):
        """Test that successful try block skips except handlers."""
        source = """fn safe_action(input: [], output: [result]):
    try:
        result = @might_fail()
    except:
        result = "caught error"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_might_fail():
            return "success"

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        outputs = runner.run("safe_action", {})
        assert outputs.get("result") == "success"

    def test_try_fails_catch_all(self):
        """Test catch-all except handler catches any exception."""
        source = """fn safe_action(input: [], output: [result]):
    try:
        result = @might_fail()
    except:
        result = "caught error"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_might_fail():
            raise RuntimeError("Something went wrong")

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        outputs = runner.run("safe_action", {})
        assert outputs.get("result") == "caught error"

    def test_try_specific_exception_match(self):
        """Test that specific exception type matches correct handler."""
        source = """fn typed_handler(input: [], output: [result]):
    try:
        result = @might_fail()
    except ValueError:
        result = "value error"
    except:
        result = "other error"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_might_fail():
            raise ValueError("bad value")

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        outputs = runner.run("typed_handler", {})
        assert outputs.get("result") == "value error"

    def test_try_exception_falls_through_to_catch_all(self):
        """Test that unmatched exception falls through to catch-all."""
        source = """fn typed_handler(input: [], output: [result]):
    try:
        result = @might_fail()
    except ValueError:
        result = "value error"
    except:
        result = "other error"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_might_fail():
            raise TypeError("wrong type")

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        outputs = runner.run("typed_handler", {})
        assert outputs.get("result") == "other error"

    def test_try_multiple_specific_handlers(self):
        """Test multiple specific exception handlers."""
        source = """fn multi_handler(input: [error_type], output: [result]):
    try:
        result = @might_fail(type=error_type)
    except ValueError:
        result = "value error"
    except TypeError:
        result = "type error"
    except KeyError:
        result = "key error"
    except:
        result = "other error"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_might_fail(type):
            if type == "value":
                raise ValueError("bad value")
            elif type == "type":
                raise TypeError("wrong type")
            elif type == "key":
                raise KeyError("missing key")
            else:
                raise RuntimeError("unknown")

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        assert runner.run("multi_handler", {"error_type": "value"}).get("result") == "value error"
        assert runner.run("multi_handler", {"error_type": "type"}).get("result") == "type error"
        assert runner.run("multi_handler", {"error_type": "key"}).get("result") == "key error"
        assert runner.run("multi_handler", {"error_type": "other"}).get("result") == "other error"

    def test_try_with_multiple_statements_in_body(self):
        """Test try block with multiple statements."""
        source = """fn multi_stmt(input: [], output: [result]):
    try:
        x = @get_value()
        y = x * 2
        result = y + 1
    except:
        result = 0
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_get_value():
            return 5

        runner = DAGRunner(dag, action_handlers={"get_value": mock_get_value})

        outputs = runner.run("multi_stmt", {})
        assert outputs.get("result") == 11  # 5 * 2 + 1

    def test_try_except_tuple_types(self):
        """Test except handler with multiple exception types."""
        source = """fn tuple_handler(input: [error_type], output: [result]):
    try:
        result = @might_fail(type=error_type)
    except (ValueError, TypeError):
        result = "value or type error"
    except:
        result = "other error"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_might_fail(type):
            if type == "value":
                raise ValueError("bad value")
            elif type == "type":
                raise TypeError("wrong type")
            else:
                raise KeyError("missing key")

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        assert runner.run("tuple_handler", {"error_type": "value"}).get("result") == "value or type error"
        assert runner.run("tuple_handler", {"error_type": "type"}).get("result") == "value or type error"
        assert runner.run("tuple_handler", {"error_type": "other"}).get("result") == "other error"


class TestDAGRunnerDurableSleep:
    """Tests for durable sleep (@sleep) functionality."""

    def test_sleep_delays_execution(self):
        """Test that @sleep delays subsequent actions."""
        source = """fn delayed(input: [], output: [result]):
    @sleep(duration=0.05)
    result = "done"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        start = time.time()
        outputs = runner.run("delayed", {})
        elapsed = time.time() - start

        assert outputs.get("result") == "done"
        # Should take at least 50ms due to sleep
        assert elapsed >= 0.05, f"Expected at least 50ms delay, got {elapsed:.3f}s"

    def test_sleep_with_variable_duration(self):
        """Test @sleep with duration from variable."""
        source = """fn delayed(input: [wait_time], output: [result]):
    @sleep(duration=wait_time)
    result = "done"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        start = time.time()
        outputs = runner.run("delayed", {"wait_time": 0.03})
        elapsed = time.time() - start

        assert outputs.get("result") == "done"
        assert elapsed >= 0.03, f"Expected at least 30ms delay, got {elapsed:.3f}s"

    def test_sleep_with_action_before_and_after(self):
        """Test @sleep between two actions."""
        source = """fn delayed_action(input: [], output: [result]):
    before = @get_time()
    @sleep(duration=0.05)
    after = @get_time()
    result = after - before
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        runner = DAGRunner(dag, action_handlers={"get_time": lambda: time.time()})

        outputs = runner.run("delayed_action", {})
        elapsed = outputs.get("result")

        # The time difference should be at least 50ms
        assert elapsed >= 0.05, f"Expected at least 50ms between actions, got {elapsed:.3f}s"

    def test_sleep_in_sequence(self):
        """Test multiple sleeps in sequence."""
        source = """fn multi_sleep(input: [], output: [result]):
    @sleep(duration=0.02)
    @sleep(duration=0.02)
    result = "done"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        start = time.time()
        outputs = runner.run("multi_sleep", {})
        elapsed = time.time() - start

        assert outputs.get("result") == "done"
        # Should take at least 40ms (two 20ms sleeps)
        assert elapsed >= 0.04, f"Expected at least 40ms delay, got {elapsed:.3f}s"

    def test_threaded_sleep(self):
        """Test @sleep with threaded runner."""
        source = """fn delayed(input: [], output: [result]):
    @sleep(duration=0.05)
    result = "done"
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()
        runner = ThreadedDAGRunner(dag, db=db, num_workers=2)

        start = time.time()
        outputs = runner.run("delayed", {})
        elapsed = time.time() - start

        assert outputs.get("result") == "done"
        assert elapsed >= 0.05, f"Expected at least 50ms delay, got {elapsed:.3f}s"

    def test_sleep_preserves_scope(self):
        """Test that @sleep preserves variable scope."""
        source = """fn preserve_scope(input: [x], output: [result]):
    y = x * 2
    @sleep(duration=0.02)
    result = y + 1
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("preserve_scope", {"x": 5})
        # y = 5 * 2 = 10, result = 10 + 1 = 11
        assert outputs.get("result") == 11


class TestDAGRunnerParallel:
    """Tests for parallel block execution."""

    def test_parallel_actions(self):
        """Test parallel execution of multiple actions."""
        source = """fn multi_action(input: [], output: [results]):
    results = parallel:
        @action_a()
        @action_b()
        @action_c()
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_a():
            return "result_a"

        def mock_b():
            return "result_b"

        def mock_c():
            return "result_c"

        runner = DAGRunner(
            dag,
            action_handlers={
                "action_a": mock_a,
                "action_b": mock_b,
                "action_c": mock_c,
            },
        )

        outputs = runner.run("multi_action", {})
        results = outputs.get("results")
        # Results should be in order of calls
        assert results == ["result_a", "result_b", "result_c"]

    def test_parallel_with_kwargs(self):
        """Test parallel actions with different kwargs."""
        source = """fn parallel_fetch(input: [], output: [results]):
    results = parallel:
        @fetch(id=1)
        @fetch(id=2)
        @fetch(id=3)
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_fetch(id):
            return f"data_{id}"

        runner = DAGRunner(dag, action_handlers={"fetch": mock_fetch})

        outputs = runner.run("parallel_fetch", {})
        results = outputs.get("results")
        assert results == ["data_1", "data_2", "data_3"]

    def test_parallel_without_assignment(self):
        """Test parallel block without capturing results."""
        source = """fn side_effects(input: [], output: [done]):
    parallel:
        @log(msg="a")
        @log(msg="b")
    done = true
    return done"""

        program = parse(source)
        dag = convert_to_dag(program)

        logged = []

        def mock_log(msg):
            logged.append(msg)
            return None

        runner = DAGRunner(dag, action_handlers={"log": mock_log})

        outputs = runner.run("side_effects", {})
        assert outputs.get("done") is True
        assert set(logged) == {"a", "b"}

    def test_parallel_preserves_order(self):
        """Test that parallel results are ordered correctly."""
        source = """fn ordered(input: [], output: [results]):
    results = parallel:
        @slow()
        @fast()
        @medium()
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)

        execution_order = []

        def slow():
            time.sleep(0.03)
            execution_order.append("slow")
            return "slow_result"

        def fast():
            execution_order.append("fast")
            return "fast_result"

        def medium():
            time.sleep(0.01)
            execution_order.append("medium")
            return "medium_result"

        runner = DAGRunner(
            dag,
            action_handlers={"slow": slow, "fast": fast, "medium": medium},
        )

        outputs = runner.run("ordered", {})
        results = outputs.get("results")

        # Results should be in declaration order, not execution order
        assert results == ["slow_result", "fast_result", "medium_result"]

    def test_parallel_with_scope_variables(self):
        """Test parallel actions can access scope variables."""
        source = """fn use_scope(input: [multiplier], output: [results]):
    results = parallel:
        @compute(x=1, m=multiplier)
        @compute(x=2, m=multiplier)
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_compute(x, m):
            return x * m

        runner = DAGRunner(dag, action_handlers={"compute": mock_compute})

        outputs = runner.run("use_scope", {"multiplier": 10})
        results = outputs.get("results")
        assert results == [10, 20]

    def test_parallel_with_function_calls(self):
        """Test parallel block with function calls."""
        source = """fn helper(input: [x], output: [result]):
    result = x * 2
    return result

fn use_parallel_funcs(input: [], output: [results]):
    results = parallel:
        @action_a()
        @action_b()
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)

        runner = DAGRunner(
            dag,
            action_handlers={
                "action_a": lambda: "a",
                "action_b": lambda: "b",
            },
        )

        outputs = runner.run("use_parallel_funcs", {})
        results = outputs.get("results")
        assert results == ["a", "b"]

    def test_threaded_parallel(self):
        """Test parallel block with threaded runner."""
        source = """fn threaded_parallel(input: [], output: [results]):
    results = parallel:
        @slow_action()
        @fast_action()
    return results"""

        program = parse(source)
        dag = convert_to_dag(program)
        db = InMemoryDB()

        def slow_action():
            time.sleep(0.02)
            return "slow"

        def fast_action():
            return "fast"

        runner = ThreadedDAGRunner(
            dag,
            action_handlers={
                "slow_action": slow_action,
                "fast_action": fast_action,
            },
            db=db,
            num_workers=2,
        )

        outputs = runner.run("threaded_parallel", {})
        results = outputs.get("results")
        assert results == ["slow", "fast"]


class TestDAGRunnerBuiltins:
    """Tests for built-in functions: range, enumerate, len."""

    def test_range_single_arg(self):
        """Test range(stop=n) produces [0, 1, ..., n-1]."""
        source = """fn use_range(input: [], output: [items]):
    items = range(stop=5)
    return items"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_range", {})
        assert outputs.get("items") == [0, 1, 2, 3, 4]

    def test_range_start_stop(self):
        """Test range(start=a, stop=b) produces [a, a+1, ..., b-1]."""
        source = """fn use_range(input: [], output: [items]):
    items = range(start=2, stop=6)
    return items"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_range", {})
        assert outputs.get("items") == [2, 3, 4, 5]

    def test_range_with_step(self):
        """Test range(start=a, stop=b, step=s)."""
        source = """fn use_range(input: [], output: [items]):
    items = range(start=0, stop=10, step=2)
    return items"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_range", {})
        assert outputs.get("items") == [0, 2, 4, 6, 8]

    def test_enumerate_basic(self):
        """Test enumerate returns [[0, item0], [1, item1], ...]."""
        source = """fn use_enumerate(input: [data], output: [pairs]):
    pairs = enumerate(items=data)
    return pairs"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_enumerate", {"data": ["a", "b", "c"]})
        assert outputs.get("pairs") == [[0, "a"], [1, "b"], [2, "c"]]

    def test_len_list(self):
        """Test len returns length of a list."""
        source = """fn use_len(input: [data], output: [size]):
    size = len(items=data)
    return size"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_len", {"data": [1, 2, 3, 4, 5]})
        assert outputs.get("size") == 5

    def test_len_dict(self):
        """Test len returns length of a dict."""
        source = """fn use_len(input: [data], output: [size]):
    size = len(items=data)
    return size"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_len", {"data": {"a": 1, "b": 2}})
        assert outputs.get("size") == 2

    def test_len_string(self):
        """Test len returns length of a string."""
        source = """fn use_len(input: [data], output: [size]):
    size = len(items=data)
    return size"""

        program = parse(source)
        dag = convert_to_dag(program)
        runner = DAGRunner(dag)

        outputs = runner.run("use_len", {"data": "hello"})
        assert outputs.get("size") == 5


class TestDAGRunnerForLoopUnpacking:
    """Tests for multi-variable for loop unpacking."""

    def test_for_loop_single_variable(self):
        """Test basic for loop with single variable still works."""
        source = """fn process_items(input: [items], output: [result]):
    for item in items:
        result = double(x=item)
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def double(x):
            return x * 2

        runner = DAGRunner(dag, function_handlers={"double": double})

        outputs = runner.run("process_items", {"items": [1, 2, 3]})
        # Last result from loop
        assert outputs.get("result") == 6

    def test_for_loop_two_variables(self):
        """Test for loop unpacking with two variables (e.g., for i, val in enumerate(x))."""
        source = """fn process_enumerated(input: [items], output: [result]):
    indexed = enumerate(items=items)
    for i, val in indexed:
        result = format_pair(idx=i, value=val)
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def format_pair(idx, value):
            return f"{idx}: {value}"

        runner = DAGRunner(dag, function_handlers={"format_pair": format_pair})

        outputs = runner.run("process_enumerated", {"items": ["a", "b", "c"]})
        # Last result from loop
        assert outputs.get("result") == "2: c"

    def test_for_loop_enumerate_with_action(self):
        """Test for loop with enumerate and action call."""
        source = """fn process_with_index(input: [data], output: [result]):
    pairs = enumerate(items=data)
    for idx, item in pairs:
        result = process(i=idx, x=item)
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        results_log = []

        def process(i, x):
            result = f"item[{i}]={x}"
            results_log.append(result)
            return result

        runner = DAGRunner(dag, function_handlers={"process": process})

        outputs = runner.run("process_with_index", {"data": ["x", "y", "z"]})
        assert outputs.get("result") == "item[2]=z"
        assert results_log == ["item[0]=x", "item[1]=y", "item[2]=z"]

    def test_for_loop_with_list_of_lists(self):
        """Test unpacking a list of lists directly."""
        source = """fn process_pairs(input: [pairs], output: [result]):
    for a, b in pairs:
        result = add(x=a, y=b)
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def add(x, y):
            return x + y

        runner = DAGRunner(dag, function_handlers={"add": add})

        outputs = runner.run("process_pairs", {"pairs": [[1, 2], [3, 4], [5, 6]]})
        # Last result: 5 + 6 = 11
        assert outputs.get("result") == 11

    def test_parser_for_loop_multiple_vars(self):
        """Test that parser correctly handles multiple loop variables."""
        source = """for i, item in items:
    result = process(x=i, y=item)"""

        from rappel import RappelForLoop

        program = parse(source)
        stmt = program.statements[0]
        assert isinstance(stmt, RappelForLoop)
        assert stmt.loop_vars == ("i", "item")

    def test_for_loop_three_variables(self):
        """Test for loop unpacking with three variables."""
        source = """fn process_triples(input: [triples], output: [result]):
    for a, b, c in triples:
        result = combine(x=a, y=b, z=c)
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def combine(x, y, z):
            return x + y + z

        runner = DAGRunner(dag, function_handlers={"combine": combine})

        outputs = runner.run("process_triples", {"triples": [[1, 2, 3], [4, 5, 6]]})
        # Last result: 4 + 5 + 6 = 15
        assert outputs.get("result") == 15


class TestDAGRunnerRetryPolicies:
    """Tests for retry policies on action calls."""

    def test_parser_retry_policy_basic(self):
        """Test parsing a basic retry policy with separate timeout bracket."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [retry: 3, backoff: 60] [timeout: 30]
    return result"""

        from rappel import RappelActionCall

        program = parse(source)
        fn = program.statements[0]
        assignment = fn.body[0]
        action_call = assignment.value

        assert isinstance(action_call, RappelActionCall)
        assert len(action_call.retry_policies) == 1
        policy = action_call.retry_policies[0]
        assert policy.max_retries == 3
        assert policy.backoff_seconds == 60.0
        assert policy.exception_types == ()  # catch all
        # Timeout is now on the action call, not the policy
        assert action_call.timeout_seconds == 30.0

    def test_parser_retry_policy_with_exception_type(self):
        """Test parsing retry policy with specific exception type using -> syntax."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [ValueError -> retry: 3, backoff: 120]
    return result"""

        from rappel import RappelActionCall

        program = parse(source)
        fn = program.statements[0]
        assignment = fn.body[0]
        action_call = assignment.value

        assert isinstance(action_call, RappelActionCall)
        assert len(action_call.retry_policies) == 1
        policy = action_call.retry_policies[0]
        assert policy.exception_types == ("ValueError",)
        assert policy.max_retries == 3
        assert policy.backoff_seconds == 120.0

    def test_parser_retry_policy_duration_strings(self):
        """Test parsing retry policy with duration strings."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [retry: 5, backoff: "2m"] [timeout: "30s"]
    return result"""

        from rappel import RappelActionCall

        program = parse(source)
        fn = program.statements[0]
        assignment = fn.body[0]
        action_call = assignment.value

        policy = action_call.retry_policies[0]
        assert policy.max_retries == 5
        assert policy.backoff_seconds == 120.0  # 2 minutes
        # Timeout is now on the action call
        assert action_call.timeout_seconds == 30.0

    def test_retry_on_failure_schedules_retry(self):
        """Test that a failed action with retry policy schedules a retry."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [retry: 3, backoff: 1]
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        fail_count = [0]

        def mock_might_fail():
            fail_count[0] += 1
            if fail_count[0] < 3:
                raise ValueError("Failed!")
            return "success"

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_might_fail})

        # Run until all scheduled actions complete
        # Since backoff is 1s, retries should complete quickly
        outputs = runner.run("test_retry", {})

        # After 3 attempts (2 failures + 1 success), should succeed
        assert outputs.get("result") == "success"
        assert fail_count[0] == 3

    def test_retry_exhausted_raises_error(self):
        """Test that exhausting retries raises an error."""
        source = """fn test_retry(input: [], output: [result]):
    result = @always_fails() [retry: 2, backoff: 0]
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_always_fails():
            raise ValueError("Always fails!")

        runner = DAGRunner(dag, action_handlers={"always_fails": mock_always_fails})

        with pytest.raises(RuntimeError):
            runner.run("test_retry", {})

    def test_retry_policy_specific_exception_match(self):
        """Test that retry only happens for matching exception types."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [ValueError -> retry: 3, backoff: 0]
    return result"""

        program = parse(source)
        dag = convert_to_dag(program)

        def mock_raises_type_error():
            raise TypeError("Wrong type!")

        runner = DAGRunner(dag, action_handlers={"might_fail": mock_raises_type_error})

        # TypeError doesn't match ValueError policy, so should fail immediately
        with pytest.raises(RuntimeError):
            runner.run("test_retry", {})

    def test_parser_tuple_exception_types(self):
        """Test parsing retry policy with tuple of exception types."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [(ValueError, KeyError) -> retry: 3, backoff: 30]
    return result"""

        from rappel import RappelActionCall

        program = parse(source)
        fn = program.statements[0]
        assignment = fn.body[0]
        action_call = assignment.value

        assert isinstance(action_call, RappelActionCall)
        assert len(action_call.retry_policies) == 1
        policy = action_call.retry_policies[0]
        assert policy.exception_types == ("ValueError", "KeyError")
        assert policy.max_retries == 3
        assert policy.backoff_seconds == 30.0

    def test_parser_multiple_policies(self):
        """Test parsing multiple retry policies for different exception types."""
        source = """fn test_retry(input: [], output: [result]):
    result = @might_fail() [ValueError -> retry: 3, backoff: 10] [KeyError -> retry: 5, backoff: 20] [timeout: 60]
    return result"""

        from rappel import RappelActionCall

        program = parse(source)
        fn = program.statements[0]
        assignment = fn.body[0]
        action_call = assignment.value

        assert isinstance(action_call, RappelActionCall)
        assert len(action_call.retry_policies) == 2

        # First policy: ValueError
        policy1 = action_call.retry_policies[0]
        assert policy1.exception_types == ("ValueError",)
        assert policy1.max_retries == 3
        assert policy1.backoff_seconds == 10.0

        # Second policy: KeyError
        policy2 = action_call.retry_policies[1]
        assert policy2.exception_types == ("KeyError",)
        assert policy2.max_retries == 5
        assert policy2.backoff_seconds == 20.0

        # Timeout on the action call
        assert action_call.timeout_seconds == 60.0

    def test_action_queue_claim_and_release(self):
        """Test claiming and releasing actions in the queue."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.DELEGATED,
        )
        queue.add(action)

        # Claim with a worker UUID
        worker_uuid = "worker_123"
        assert queue.claim("action_1", worker_uuid, timeout_seconds=30) is True

        # Can't claim again
        assert queue.claim("action_1", "other_worker", timeout_seconds=30) is False

        # Release
        assert queue.release("action_1", worker_uuid) is True

        # Can claim again after release
        assert queue.claim("action_1", "other_worker", timeout_seconds=30) is True

    def test_action_queue_schedule_retry(self):
        """Test scheduling a retry action."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        # Use queue.next_id() to get a proper unique ID
        action_id = queue.next_id()
        action = RunnableAction(
            id=action_id,
            node_id="node_1",
            function_name="test",
            action_type=ActionType.DELEGATED,
            retries_remaining=3,
            backoff_seconds=60.0,
            retry_attempt=0,
        )
        queue.add(action)

        # Schedule retry
        retry_action = queue.schedule_retry(action)

        assert retry_action is not None
        assert retry_action.retries_remaining == 2
        assert retry_action.retry_attempt == 1
        assert retry_action.original_action_id == action_id
        assert retry_action.scheduled_at is not None
        # Backoff should be 60 * 2^0 = 60 seconds from now
        assert retry_action.scheduled_at > time.time()

    def test_action_queue_no_retry_when_exhausted(self):
        """Test that no retry is scheduled when retries are exhausted."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.DELEGATED,
            retries_remaining=0,  # No retries left
            backoff_seconds=60.0,
        )
        queue.add(action)

        # Should not schedule retry
        retry_action = queue.schedule_retry(action)
        assert retry_action is None

    def test_get_timed_out_actions(self):
        """Test getting timed out actions from the queue."""
        db = InMemoryDB()
        queue = ActionQueue(db)

        # Add an action with timeout in the past
        action = RunnableAction(
            id="action_1",
            node_id="node_1",
            function_name="test",
            action_type=ActionType.DELEGATED,
            lock_uuid="worker_1",
            timeout_at=time.time() - 10,  # 10 seconds ago
        )
        queue.add(action)

        # Add an action with timeout in the future
        action2 = RunnableAction(
            id="action_2",
            node_id="node_2",
            function_name="test",
            action_type=ActionType.DELEGATED,
            lock_uuid="worker_2",
            timeout_at=time.time() + 3600,  # 1 hour from now
        )
        queue.add(action2)

        timed_out = queue.get_timed_out_actions()
        assert len(timed_out) == 1
        assert timed_out[0].id == "action_1"
