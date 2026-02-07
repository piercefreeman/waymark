import os
import re
import sys
from typing import List, Sequence, TextIO

RESET = "\033[0m"

STYLE_CODES = {
    "bold": "\033[1m",
    "dim": "\033[2m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
    "white": "\033[37m",
}

_TAG_PATTERN = re.compile(r"\[(/?)([a-zA-Z]+)?\]")


def supports_color(stream: TextIO) -> bool:
    """Return True if the provided stream likely supports ANSI colors."""

    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    if stream.isatty():
        if sys.platform != "win32":
            return True
        return bool(
            os.environ.get("ANSICON")
            or os.environ.get("WT_SESSION")
            or os.environ.get("TERM_PROGRAM")
        )
    return False


class Formatter:
    """
    Very small markup formatter inspired by Rich's tag syntax. We want to minimize our python client
    dependencies to just grpc+standard library.

    """

    def __init__(self, enable_colors: bool) -> None:
        self._enable_colors = enable_colors

    @property
    def enable_colors(self) -> bool:
        return self._enable_colors

    def format(self, text: str) -> str:
        if not text:
            return text
        return _apply_markup(text, self._enable_colors)

    def apply_styles(self, text: str, styles: Sequence[str]) -> str:
        """Wrap text with markup tags for the provided style sequence."""

        if not styles:
            return text
        opening = "".join(f"[{style}]" for style in styles)
        closing = "".join(f"[/{style}]" for style in reversed(styles))
        return f"{opening}{text}{closing}"


def _apply_markup(text: str, enable_colors: bool) -> str:
    if not enable_colors:
        return _TAG_PATTERN.sub("", text)
    result: List[str] = []
    stack: List[str] = []
    index = 0
    for match in _TAG_PATTERN.finditer(text):
        result.append(text[index : match.start()])
        is_closing = match.group(1) == "/"
        tag_name = match.group(2)
        if tag_name is None:
            if is_closing:
                if stack:
                    stack.clear()
                    result.append(RESET)
            else:
                result.append(match.group(0))
            index = match.end()
            continue
        if is_closing:
            if tag_name in stack:
                while stack:
                    name = stack.pop()
                    result.append(RESET)
                    if name == tag_name:
                        break
                if stack:
                    result.append("".join(STYLE_CODES[name] for name in stack))
            index = match.end()
            continue
        code = STYLE_CODES.get(tag_name)
        if code is None:
            result.append(match.group(0))
        else:
            stack.append(tag_name)
            result.append(code)
        index = match.end()
    result.append(text[index:])
    if stack:
        result.append(RESET)
    return "".join(result)
