"""Register syntax handlers independently of the IR builder."""

from waymark.handlers.syntax.try_statement import TryHandler
from waymark.language_syntax import SyntaxRegistry

SYNTAX_HANDLERS = SyntaxRegistry(
    [
        TryHandler(),
    ]
)
