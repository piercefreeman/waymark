import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def load_items() -> list[dict]:
    """Load items to process."""
    return [
        {"id": "A", "value": 10},
        {"id": "B", "value": 20},
        {"id": "C", "value": 30},
    ]


@action
async def process_item(item: dict) -> dict:
    """Process an item and return both a result string and a metric."""
    return {
        "result": f"PROCESSED_{item['id']}_{item['value']}",
        "metric": item["value"] * 2,
    }


@action
async def summarize_results(results: list[str], metrics: list[int]) -> str:
    """Summarize the results and metrics."""
    results_str = ",".join(results)
    metrics_str = ",".join(str(m) for m in metrics)
    return f"RESULTS:{results_str}|METRICS:{metrics_str}"


@workflow
class MultiAccumulatorWorkflow(Workflow):
    """Test workflow with two separate accumulators in a loop.

    Each iteration processes an item and appends to both accumulators.
    """

    async def run(self) -> str:
        items = await load_items()

        results = []
        metrics = []
        for item in items:
            processed = await process_item(item=item)
            results.append(processed["result"])
            metrics.append(processed["metric"])

        summary = await summarize_results(results=results, metrics=metrics)
        return summary
