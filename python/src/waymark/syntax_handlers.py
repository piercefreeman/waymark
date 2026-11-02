"""Register syntax handlers independently of the IR builder."""

from waymark.handlers.syntax.access import AccessHandler
from waymark.handlers.syntax.arithmetic import ArithmeticHandler
from waymark.handlers.syntax.assignments import AssignmentHandler
from waymark.handlers.syntax.comparisons import ComparisonHandler
from waymark.handlers.syntax.dictionaries import DictionaryHandler
from waymark.handlers.syntax.literals import LiteralHandler
from waymark.handlers.syntax.loops import LoopHandler
from waymark.handlers.syntax.try_statement import TryHandler
from waymark.language_syntax import SyntaxRegistry

SYNTAX_HANDLERS = SyntaxRegistry(
    [
        LiteralHandler(),
        AccessHandler(),
        DictionaryHandler(),
        AssignmentHandler(),
        ComparisonHandler(),
        ArithmeticHandler(),
        LoopHandler(),
        TryHandler(),
    ]
)
