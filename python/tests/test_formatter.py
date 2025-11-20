from __future__ import annotations

import io

import pytest

from rappel.formatter import Formatter, supports_color


class DummyStream(io.StringIO):
    def __init__(self, *, is_tty: bool) -> None:
        super().__init__()
        self._is_tty = is_tty

    def isatty(self) -> bool:  # type: ignore[override]
        return self._is_tty


def test_formatter_strips_tags_when_disabled() -> None:
    formatter = Formatter(enable_colors=False)
    assert formatter.format("hello [bold]world[/bold]") == "hello world"


def test_formatter_applies_basic_styles() -> None:
    formatter = Formatter(enable_colors=True)
    styled = formatter.format("[bold]hello[/bold][/]")
    assert "\033[1m" in styled
    assert styled.endswith("\033[0m")


def test_supports_color_honors_env(monkeypatch: pytest.MonkeyPatch) -> None:
    stream = DummyStream(is_tty=False)
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert supports_color(stream) is True
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setenv("NO_COLOR", "1")
    assert supports_color(DummyStream(is_tty=True)) is False
