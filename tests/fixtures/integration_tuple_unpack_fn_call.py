"""
Integration test for tuple unpacking from helper method returns.

This tests the scenario where:
1. A helper method returns a tuple
2. The tuple is unpacked into variables: a, b = await self.helper_method()
3. Those unpacked variables are used in subsequent action kwargs

The bug was that the Rust scheduler stored the entire tuple for each target
instead of unpacking individual items.
"""

from pydantic import BaseModel

from waymark import action, workflow
from waymark.workflow import Workflow


class ProfileMetadata(BaseModel):
    profile_id: str


class CrawlResult(BaseModel):
    crawl_id: str


class ParseRequest(BaseModel):
    profile_id: str
    crawl_id: str


class ParseResult(BaseModel):
    profile_id: str
    crawl_id: str
    status: str


@action
async def get_profile(user_id: str) -> ProfileMetadata:
    """Fetch profile metadata."""
    return ProfileMetadata(profile_id=f"profile_{user_id}")


@action
async def get_subscribed_users(profile_id: str) -> list[str]:
    """Get subscribed users for a profile."""
    return [f"user_1_{profile_id}", f"user_2_{profile_id}"]


@action
async def perform_crawl(profile_id: str) -> CrawlResult:
    """Perform a crawl operation."""
    return CrawlResult(crawl_id=f"crawl_{profile_id}")


@action
async def parse_results(request: ParseRequest) -> ParseResult:
    """Parse results using profile_id and crawl_id from tuple-unpacked variables."""
    return ParseResult(
        profile_id=request.profile_id,
        crawl_id=request.crawl_id,
        status="success",
    )


@workflow
class TupleUnpackFnCallWorkflow(Workflow):
    """
    Workflow that reproduces the tuple unpacking bug.

    The key pattern is:
    1. get_profile() returns ProfileMetadata
    2. run_crawl() helper returns a tuple (subscribed_users, crawl_result)
    3. crawl_result (from tuple index 1) is used in parse_results() kwargs
    """

    async def run(self, user_id: str) -> ParseResult:
        # Get profile metadata
        profile_metadata = await self.run_action(get_profile(user_id))

        # Call helper that returns tuple - THIS IS THE KEY PATTERN
        subscribed_users, crawl_result = await self.run_crawl(profile_metadata)

        # Use variables from tuple unpacking in action kwargs
        # This is where the bug manifested - kwargs were empty
        result = await self.run_action(
            parse_results(
                ParseRequest(
                    profile_id=profile_metadata.profile_id,  # From method param
                    crawl_id=crawl_result.crawl_id,  # From tuple unpacking
                )
            )
        )

        return result

    async def run_crawl(
        self, profile_metadata: ProfileMetadata
    ) -> tuple[list[str], CrawlResult]:
        """Helper method that returns a tuple."""
        subscribed_users = await self.run_action(
            get_subscribed_users(profile_metadata.profile_id)
        )
        crawl_result = await self.run_action(
            perform_crawl(profile_metadata.profile_id)
        )
        return subscribed_users, crawl_result
