"""Register syntax handlers independently of the IR builder."""

from waymark.handlers.syntax.arithmetic import ArithmeticHandler
from waymark.handlers.syntax.assignments import AssignmentHandler
from waymark.handlers.syntax.comparisons import ComparisonHandler
from waymark.handlers.syntax.loops import LoopHandler
from waymark.handlers.syntax.try_statement import TryHandler
from waymark.language_syntax import SyntaxRegistry

SYNTAX_HANDLERS = SyntaxRegistry(
    [AssignmentHandler(), ComparisonHandler(), ArithmeticHandler(), LoopHandler(), TryHandler()]
)
