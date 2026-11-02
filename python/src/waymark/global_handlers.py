"""Register the language-call handlers available during workflow compilation."""

from waymark.gather import GatherHandler
from waymark.language_calls import LanguageCallRegistry

GLOBAL_HANDLERS = LanguageCallRegistry([GatherHandler()])
