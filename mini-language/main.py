"""
Entry point for trying out the Rappel language runner.

Run with: python main.py
"""

import time
from rappel import (
    parse,
    convert_to_dag,
    RappelPrettyPrinter,
    DAGRunner,
    ThreadedDAGRunner,
    InMemoryDB,
    reset_db,
)


# Example program with a main() entry point
EXAMPLE_PROGRAM = """
# Main entry point - orchestrates the workflow with spread
fn main(input: [], output: [results]):
    # Step 1: Fetch pending orders (external @action)
    orders = @get_pending_orders(status="pending", limit=10)

    # Step 2: Process payments for ALL orders in parallel (spread!)
    # This fans out to N workers, each processing one payment
    payments = spread orders:order -> @process_payment(order_id=order["id"], amount=order["total"])

    # Step 3: Fetch shipping quotes for all orders in parallel (another spread!)
    shipping = spread orders:order -> @get_shipping_quote(order_id=order["id"], destination=order["destination"])

    # Step 4: Combine results
    results = {"payments": payments, "shipping": shipping}
    return results
"""


def main():
    print("=" * 60)
    print("Rappel Language - DAG Runner Demo")
    print("=" * 60)

    # Parse the program
    print("\n1. Parsing program...")
    program = parse(EXAMPLE_PROGRAM)
    print(f"   Parsed {len(program.statements)} function definitions")

    # Pretty print
    print("\n2. Program source:")
    print("-" * 60)
    printer = RappelPrettyPrinter()
    print(printer.print(program))
    print("-" * 60)

    # Convert to DAG
    print("\n3. Converting to DAG...")
    dag = convert_to_dag(program)
    print(f"   Created {len(dag.nodes)} nodes and {len(dag.edges)} edges")
    print(f"   Functions: {dag.get_functions()}")

    # Create mock action handlers
    # Thread-safe execution log with timestamps
    import threading
    execution_log = []
    log_lock = threading.Lock()

    def log_action(message):
        with log_lock:
            execution_log.append((time.time(), threading.current_thread().name, message))

    def mock_get_pending_orders(status, limit):
        """Mock: Simulates fetching orders from a database."""
        log_action(f"@get_pending_orders(status={status}, limit={limit})")
        time.sleep(0.05)  # Simulate network latency
        return [
            {"id": 1001, "total": 99.99, "customer": "Alice", "destination": "NYC"},
            {"id": 1002, "total": 149.50, "customer": "Bob", "destination": "LA"},
            {"id": 1003, "total": 0, "customer": "Charlie", "destination": "Chicago"},
        ]

    def mock_process_payment(order_id, amount):
        """Mock: Simulates processing a payment (slow!)."""
        log_action(f"@process_payment(order_id={order_id}, amount={amount})")
        time.sleep(0.5)  # Simulate slow payment gateway
        return {"transaction_id": f"TXN-{order_id}", "success": True}

    def mock_get_shipping_quote(order_id, destination):
        """Mock: Simulates getting a shipping quote."""
        log_action(f"@get_shipping_quote(order_id={order_id}, destination={destination})")
        time.sleep(0.02)  # Simulate API call
        rates = {"NYC": 5.99, "LA": 8.99, "Chicago": 6.99}
        return {"order_id": order_id, "rate": rates.get(destination, 10.00)}

    action_handlers = {
        "get_pending_orders": mock_get_pending_orders,
        "process_payment": mock_process_payment,
        "get_shipping_quote": mock_get_shipping_quote,
    }

    # =========================================================
    # SINGLE-THREADED EXECUTION
    # =========================================================
    print("\n4. Single-threaded execution (baseline)...")
    print("-" * 60)

    db = InMemoryDB()
    execution_log.clear()

    runner = DAGRunner(dag, action_handlers=action_handlers, db=db)
    db.reset_stats()

    start_time = time.time()
    output = runner.run_main()
    single_thread_time = time.time() - start_time

    print(f"   Time: {single_thread_time:.4f}s")
    print(f"   {db.stats}")
    print(f"   Result: {output.get('results')}")

    # =========================================================
    # MULTI-THREADED EXECUTION (4 workers)
    # =========================================================
    print("\n5. Multi-threaded execution (4 workers simulating distributed cluster)...")
    print("-" * 60)

    # Reset global DB for threaded runner
    db = reset_db()
    execution_log.clear()

    # Re-convert DAG since we need fresh state
    dag = convert_to_dag(program)

    threaded_runner = ThreadedDAGRunner(
        dag,
        action_handlers=action_handlers,
        db=db,
        num_workers=4,
    )
    db.reset_stats()

    start_time = time.time()
    output = threaded_runner.run_main()
    multi_thread_time = time.time() - start_time

    print(f"   Time: {multi_thread_time:.4f}s")
    print(f"   {db.stats}")
    print(f"   Result: {output.get('results')}")

    # Show speedup
    if multi_thread_time > 0:
        speedup = single_thread_time / multi_thread_time
        print(f"\n   Speedup: {speedup:.2f}x faster with parallel workers!")

    # Show worker execution log
    print("\n6. Worker execution timeline:")
    print("-" * 60)
    threaded_runner.print_execution_log()

    # Show action distribution across workers
    print("\n7. @action distribution across workers:")
    print("-" * 60)
    if execution_log:
        base_time = execution_log[0][0]
        for ts, thread_name, action in execution_log:
            elapsed = ts - base_time
            # Shorten thread name
            short_name = thread_name.replace("Thread-", "W")
            print(f"   [{elapsed:.4f}s] {short_name}: {action}")

    # Show final stats
    print("\n8. Final statistics:")
    print("-" * 60)
    print(f"   {db.stats}")
    print("   ")
    print("   This demonstrates:")
    print("   - Workers competed for actions using SELECT ... FOR UPDATE SKIP LOCKED")
    print("   - 7 @actions were distributed across 4 worker threads")
    print("   - spread operations (process_payment, get_shipping_quote) ran in parallel")
    print("   - DB was the central broker, just like a real distributed system")
    print("-" * 60)

    print("\n9. Visualizing DAG (opens in browser)...")
    try:
        dag.visualize("Rappel Program DAG - main() workflow")
    except Exception as e:
        print(f"   Visualization skipped: {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
