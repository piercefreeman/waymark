"""Integration test for data pipeline workflow patterns.

This tests workflows with:
- Multiple sequential transformations
- Accumulating results in lists
- Conditional filtering
- Aggregation patterns
"""
from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def fetch_records(source: str) -> list[dict]:
    """Fetch records from a source."""
    data = {
        "sales": [
            {"id": 1, "amount": 100, "region": "north"},
            {"id": 2, "amount": 250, "region": "south"},
            {"id": 3, "amount": 75, "region": "north"},
            {"id": 4, "amount": 500, "region": "east"},
        ],
        "inventory": [
            {"id": 1, "quantity": 50, "status": "ok"},
            {"id": 2, "quantity": 5, "status": "low"},
            {"id": 3, "quantity": 100, "status": "ok"},
        ],
    }
    return data.get(source, [])


@action
async def filter_by_threshold(records: list[dict], field: str, threshold: int) -> list[dict]:
    """Filter records where field value >= threshold."""
    return [r for r in records if r.get(field, 0) >= threshold]


@action
async def compute_total(records: list[dict], field: str) -> int:
    """Compute total of a numeric field across records."""
    return sum(r.get(field, 0) for r in records)


@action
async def count_records(records: list[dict]) -> int:
    """Count the number of records."""
    return len(records)


@action
async def format_summary(total: int, count: int, filtered_count: int) -> str:
    """Format a summary string."""
    avg = total // count if count > 0 else 0
    return f"total:{total},count:{count},filtered:{filtered_count},avg:{avg}"


@workflow
class DataPipelineWorkflow(Workflow):
    """Workflow simulating a data processing pipeline."""

    async def run(self, source: str, threshold: int) -> str:
        # Fetch raw data
        records = await fetch_records(source)

        # Get initial count
        total_count = await count_records(records)

        # Filter based on threshold
        filtered = await filter_by_threshold(records, "amount", threshold)

        # Count filtered records
        filtered_count = await count_records(filtered)

        # Compute total of filtered records
        total_amount = await compute_total(filtered, "amount")

        # Format and return summary
        summary = await format_summary(total_amount, total_count, filtered_count)
        return summary
