"""Register the language-call handlers available during workflow compilation."""

from waymark.handlers.enumerate import EnumerateHandler
from waymark.handlers.gather import GatherHandler
from waymark.handlers.isinstance import IsinstanceHandler
from waymark.handlers.length import LengthHandler
from waymark.handlers.range import RangeHandler
from waymark.handlers.run_action import RunActionHandler
from waymark.handlers.sleep import SleepHandler
from waymark.language_calls import LanguageCallRegistry

GLOBAL_HANDLERS = LanguageCallRegistry(
    [
        GatherHandler(),
        RunActionHandler(),
        SleepHandler(),
        LengthHandler(),
        RangeHandler(),
        EnumerateHandler(),
        IsinstanceHandler(),
    ]
)
