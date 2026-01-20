from rappel import action, workflow
from rappel.workflow import RetryPolicy, Workflow


class PostContentError(Exception):
    pass


@action
async def post_content(attempt: int, succeed_on: int) -> str:
    if attempt < succeed_on:
        raise PostContentError(f"failed attempt {attempt}")
    return "posted"


@action
async def recheck_conflict() -> bool:
    return False


@action
async def finalize(
    post_succeeded: bool,
    attempts: int,
    inner_failures: int,
    outer_handled: bool,
) -> dict:
    return {
        "post_succeeded": post_succeeded,
        "attempts": attempts,
        "inner_failures": inner_failures,
        "outer_handled": outer_handled,
    }


@workflow
class NestedTryLoopWorkflow(Workflow):
    async def run(self, post_max_retries: int = 3, succeed_on: int = 2) -> dict:
        post_succeeded = False
        inner_failures = 0
        outer_handled = False
        attempts = 0

        try:
            for current_retry in range(post_max_retries):
                attempts = current_retry + 1
                try:
                    await self.run_action(
                        post_content(attempt=current_retry, succeed_on=succeed_on),
                        retry=RetryPolicy(attempts=1),
                    )
                    post_succeeded = True
                    break
                except Exception:
                    inner_failures = inner_failures + 1
                    conflict = await self.run_action(
                        recheck_conflict(),
                        retry=RetryPolicy(attempts=1),
                    )
                    if conflict:
                        break
        except Exception:
            outer_handled = True

        return await finalize(
            post_succeeded=post_succeeded,
            attempts=attempts,
            inner_failures=inner_failures,
            outer_handled=outer_handled,
        )
