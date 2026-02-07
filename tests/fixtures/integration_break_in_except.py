"""Integration test: break inside except block control flow.

Issue 2: break inside except block causes workflow to raise exception
When using break inside an except block within a for loop, the workflow should
exit the loop and continue with code after the loop, not raise an exception.
"""

from datetime import timedelta

from waymark import RetryPolicy, action, workflow
from waymark.workflow import Workflow


class CrawlFetchError(Exception):
    """Custom exception for crawl failures."""

    pass


@action
async def handle_crawl(page: int, fail_on: int | None) -> str:
    """Simulate a crawl operation that may fail."""
    if fail_on is not None and page == fail_on:
        raise CrawlFetchError(f"Failed on page {page}")
    return f"page_{page}_data"


@action
async def log_crawl_error(error_type: str, page: int) -> str:
    """Log a crawl error and return the message."""
    return f"{error_type} on page {page}"


@action
async def parse_item(item: str) -> str:
    """Parse an item."""
    return f"parsed_{item}"


@action
async def format_results(items: list[str], log_messages: list[str]) -> dict:
    """Format the final results."""
    return {"items": items, "logs": log_messages}


@workflow
class BreakInExceptWorkflow(Workflow):
    """Test that break inside except properly exits loop and continues execution."""

    async def run(
        self, max_pages: int = 3, fail_on: int | None = 1
    ) -> dict:
        """
        Expected: After break in except, code after the loop executes normally.
        Bug: Exception propagates to outer handler instead of continuing.
        """
        results: list[str] = []
        log_messages: list[str] = []

        for page in range(max_pages):
            try:
                result = await self.run_action(
                    handle_crawl(page=page, fail_on=fail_on),
                    retry=RetryPolicy(attempts=1),
                    timeout=timedelta(seconds=5),
                )
                results.append(result)
            except CrawlFetchError:
                msg = await log_crawl_error(error_type="CrawlFetchError", page=page)
                log_messages.append(msg)
                break
            except Exception:
                msg = await log_crawl_error(error_type="Exception", page=page)
                log_messages.append(msg)
                break

        # This code should be reached after break
        parsed_items: list[str] = []
        for item in results:
            parsed = await parse_item(item=item)
            parsed_items.append(parsed)

        return await format_results(items=parsed_items, log_messages=log_messages)
