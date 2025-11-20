"""Simple logging helpers for configurable rappel loggers."""

from __future__ import annotations

import logging
import os
from typing import Optional

DEFAULT_LEVEL = logging.INFO
ENV_VAR = "RAPPEL_LOG_LEVEL"


def _resolve_level(value: Optional[str]) -> int:
    if not value:
        return DEFAULT_LEVEL
    normalized = value.strip().upper()
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.FATAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    return mapping.get(normalized, DEFAULT_LEVEL)


def configure(name: str) -> logging.Logger:
    """Return a logger configured from RAPPEL_LOG_LEVEL."""

    logger = logging.getLogger(name)
    level = _resolve_level(os.environ.get(ENV_VAR))
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(name)s] %(levelname)s: %(message)s"))
        handler.setLevel(level)
        logger.addHandler(handler)
    return logger
