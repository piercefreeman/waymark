"""Register syntax handlers independently of the IR builder."""

from waymark.handlers.syntax.expressions.access import AccessHandler
from waymark.handlers.syntax.expressions.arithmetic import ArithmeticHandler
from waymark.handlers.syntax.expressions.await_expression import AwaitHandler
from waymark.handlers.syntax.expressions.comparisons import ComparisonHandler
from waymark.handlers.syntax.expressions.dictionaries import DictionaryHandler
from waymark.handlers.syntax.expressions.literals import LiteralHandler
from waymark.handlers.syntax.expressions.sequences import SequenceHandler
from waymark.handlers.syntax.statements.assignments import AssignmentHandler
from waymark.handlers.syntax.statements.conditionals import ConditionalHandler
from waymark.handlers.syntax.statements.loops import LoopHandler
from waymark.handlers.syntax.statements.returns import ReturnHandler
from waymark.handlers.syntax.statements.try_statement import TryHandler
from waymark.language_syntax import SyntaxRegistry

SYNTAX_HANDLERS = SyntaxRegistry(
    [
        SequenceHandler(),
        ReturnHandler(),
        ConditionalHandler(),
        AwaitHandler(),
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
