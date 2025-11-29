"""Integration test for conditional logic with compound guards.

This tests workflows with:
- Multiple sequential conditionals
- Intermediate variable computations
- Combined condition evaluation
"""
from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def get_user_score(user_id: str) -> int:
    """Get a user's score."""
    scores = {"user_a": 85, "user_b": 45, "user_c": 95, "user_d": 15}
    return scores.get(user_id, 0)


@action
async def get_user_level(user_id: str) -> int:
    """Get a user's level."""
    levels = {"user_a": 3, "user_b": 1, "user_c": 5, "user_d": 2}
    return levels.get(user_id, 0)


@action
async def determine_badge(score: int, level: int) -> str:
    """Determine badge based on score and level thresholds."""
    if score >= 90:
        if level >= 5:
            return "elite"
        else:
            return "rising_star"
    elif score >= 50:
        if level >= 3:
            return "veteran"
        else:
            return "regular"
    else:
        if level >= 2:
            return "needs_work"
        else:
            return "newcomer"


@action
async def determine_notification(score: int) -> str:
    """Determine notification message based on score."""
    if score >= 90:
        return "high_achiever"
    elif score >= 50:
        return "keep_going"
    else:
        return "welcome"


@action
async def award_badge(badge_type: str, user_id: str) -> str:
    """Award a badge to a user."""
    return f"{user_id}:{badge_type}"


@action
async def send_notification(message: str) -> str:
    """Send a notification."""
    return f"notified:{message}"


@workflow
class NestedConditionalsWorkflow(Workflow):
    """Workflow with conditional logic using computed intermediate values."""

    async def run(self, user_id: str) -> str:
        # Get user data
        score = await get_user_score(user_id)
        level = await get_user_level(user_id)

        # Determine what to award based on conditions
        badge_type = await determine_badge(score, level)
        notification_msg = await determine_notification(score)

        # Execute the awarding actions
        badge = await award_badge(badge_type, user_id)
        notification = await send_notification(notification_msg)

        return f"{badge}|{notification}"
