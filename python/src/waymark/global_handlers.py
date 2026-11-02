"""Register the language-call handlers available during workflow compilation."""

from waymark.handlers.action import action_handler
from waymark.handlers.append import AppendHandler
from waymark.handlers.enumerate import EnumerateHandler
from waymark.handlers.gather import GatherHandler
from waymark.handlers.isinstance import IsinstanceHandler
from waymark.handlers.length import LengthHandler
from waymark.handlers.model import model_handler
from waymark.handlers.range import RangeHandler
from waymark.handlers.run_action import RunActionHandler
from waymark.handlers.sleep import SleepHandler
from waymark.language_calls import LanguageCallRegistry

GLOBAL_HANDLERS = LanguageCallRegistry(
    [
        AppendHandler(),
        GatherHandler(),
        SleepHandler(),
        RunActionHandler(),
        LengthHandler(),
        RangeHandler(),
        EnumerateHandler(),
        IsinstanceHandler(),
    ],
    factories=[action_handler, model_handler],
)
