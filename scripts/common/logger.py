#!/usr/bin/env python3
"""Common logging setup for scripts."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def get_logger(script_name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger that logs to stdout and logs/<script_name>.log."""
    logger = logging.getLogger(script_name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    logs_dir = Path(__file__).resolve().parents[2] / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"{script_name}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(fmt)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger
