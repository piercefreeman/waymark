"""Source-located errors raised while compiling a workflow."""


class UnsupportedPatternError(Exception):
    """Raised when the IR builder encounters an unsupported Python pattern.

    This error includes a recommendation for how to rewrite the code to use
    supported patterns.
    """

    def __init__(
        self,
        message: str,
        recommendation: str,
        line: int | None = None,
        col: int | None = None,
        filename: str | None = None,
    ):
        self.message = message
        self.recommendation = recommendation
        self.line = line
        self.col = col
        self.filename = filename

        location_parts: list[str] = []
        if filename:
            location_parts.append(filename)
        if line:
            location_parts.append(f"line {line}")
        if col is not None:
            location_parts.append(f"col {col}")
        location = f" ({', '.join(location_parts)})" if location_parts else ""
        full_message = f"{message}{location}\n\nRecommendation: {recommendation}"
        super().__init__(full_message)
