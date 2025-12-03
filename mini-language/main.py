"""
Entry point for trying out the Rappel language runner.

Run with: python main.py
"""

from rappel import (
    parse,
    convert_to_dag,
    RappelPrettyPrinter,
    DAGRunner,
    InMemoryDB,
)


# Example program with a main() entry point
EXAMPLE_PROGRAM = """
# Helper function to validate an order
fn validate_order(input: [order], output: [is_valid]):
    if order["total"] > 0:
        is_valid = true
    else:
        is_valid = false
    return is_valid

# Helper function to format order result
fn format_result(input: [order_id, status], output: [result]):
    result = {"order_id": order_id, "status": status}
    return result

# Main entry point - orchestrates the workflow
fn main(input: [], output: [final_result]):
    # Step 1: Fetch pending orders (external @action)
    orders = @get_pending_orders(status="pending", limit=10)

    # Step 2: Process the first order
    order = orders[0]

    # Step 3: Validate inline
    valid = validate_order(order=order)

    # Step 4: Based on validation, process payment (external @action)
    if valid:
        payment = @process_payment(order_id=order["id"], amount=order["total"])
        status = "completed"
    else:
        payment = {}
        status = "invalid"

    # Step 5: Format and return result
    final_result = format_result(order_id=order["id"], status=status)
    return final_result
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

    # Create runner with mock action handlers and DB for stats tracking
    print("\n4. Creating DAG Runner with mock @action handlers...")

    db = InMemoryDB()
    execution_log = []

    def mock_get_pending_orders(status, limit):
        """Mock: Simulates fetching orders from a database."""
        execution_log.append(f"@get_pending_orders(status={status}, limit={limit})")
        return [
            {"id": 1001, "total": 99.99, "customer": "Alice"},
            {"id": 1002, "total": 149.50, "customer": "Bob"},
            {"id": 1003, "total": 0, "customer": "Charlie"},  # Invalid order
        ]

    def mock_process_payment(order_id, amount):
        """Mock: Simulates processing a payment."""
        execution_log.append(f"@process_payment(order_id={order_id}, amount={amount})")
        return {"transaction_id": f"TXN-{order_id}", "success": True}

    runner = DAGRunner(
        dag,
        action_handlers={
            "get_pending_orders": mock_get_pending_orders,
            "process_payment": mock_process_payment,
        },
        db=db,
    )

    # Run main() and get the output
    print("\n5. Running main()...")
    print("-" * 60)

    # Reset stats before running to get clean measurements
    db.reset_stats()

    output = runner.run_main()

    print("\n   @action execution log:")
    for log_entry in execution_log:
        print(f"     - {log_entry}")

    print("\n6. Program output:")
    print("-" * 60)
    print(f"   final_result = {output.get('final_result')}")
    print("-" * 60)

    # Show DB stats to demonstrate the optimization
    print("\n7. Database stats (showing inline vs delegated optimization):")
    print("-" * 60)
    print(f"   {db.stats}")
    print("   ")
    print("   Key insight: Only 2 queue writes (for the 2 @actions).")
    print("   All other inline nodes executed in-memory without queue roundtrips!")
    print("-" * 60)

    # Also demonstrate running individual functions
    print("\n8. Running individual functions:")
    print("-" * 60)

    # Test validate_order with valid order
    result = runner.run("validate_order", {"order": {"total": 50}})
    print(f"   validate_order(order={{total: 50}}) => {result}")

    # Test validate_order with invalid order
    result = runner.run("validate_order", {"order": {"total": 0}})
    print(f"   validate_order(order={{total: 0}}) => {result}")

    # Test format_result
    result = runner.run("format_result", {"order_id": 123, "status": "shipped"})
    print(f"   format_result(order_id=123, status='shipped') => {result}")

    print("-" * 60)

    print("\n9. Visualizing DAG (opens in browser)...")
    try:
        dag.visualize("Rappel Program DAG - main() workflow")
    except Exception as e:
        print(f"   Visualization skipped: {e}")

    print("\nDone!")


if __name__ == "__main__":
    main()
