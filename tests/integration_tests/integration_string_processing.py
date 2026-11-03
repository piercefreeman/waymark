"""Integration test for string processing workflow patterns.

This tests workflows with:
- String manipulation and formatting
- Sequential transformations
- Multiple string operations chained together

Regression: compiled workflows reject dynamic string lengths, so count characters
inside the Python action. The Unicode case uses 'éléphant' (8 characters, 10 UTF-8
bytes) to guard against byte-count semantics.
"""
from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def validate_and_process(text: str) -> str:
    """Validate input and return processing status.

    Returns the text if valid, or 'INVALID' if not.
    """
    if len(text) >= 3 and text.isalnum():
        return text
    return "INVALID"


@action
async def normalize_text(text: str) -> str:
    """Normalize text to lowercase and strip whitespace."""
    if text == "INVALID":
        return "INVALID"
    return text.lower().strip()


@action
async def extract_prefix(text: str, length: int) -> str:
    """Extract a prefix of specified length."""
    if text == "INVALID":
        return "INVALID"
    return text[:length] if len(text) >= length else text


@action
async def generate_code(prefix: str, text: str) -> str:
    """Generate a code from prefix and computed suffix."""
    if prefix == "INVALID":
        return "ERROR:invalid_input"
    suffix = len(text) * 7  # Simple hash-like computation
    return f"{prefix.upper()}-{suffix}"


@workflow
class StringProcessingWorkflow(Workflow):
    """Workflow demonstrating string processing with sequential transformations."""

    async def run(self, text: str) -> str:
        # Validate and get processing result
        validated = await validate_and_process(text)

        # Normalize the text
        normalized = await normalize_text(validated)

        # Extract prefix
        prefix = await extract_prefix(normalized, 3)

        # Generate final code (handles INVALID case internally)
        code = await generate_code(prefix, text)
        return code
