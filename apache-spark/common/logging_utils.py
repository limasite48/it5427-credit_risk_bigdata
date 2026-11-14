"""Uniform logging (console + rotating file under reports/logs)."""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from common.config import PROJECT_ROOT

_LOG_DIR = PROJECT_ROOT / "reports" / "logs"
_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(_FMT))
    logger.addHandler(console)
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(_LOG_DIR / "pipeline.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(logging.Formatter(_FMT))
        logger.addHandler(fh)
    except OSError:  # read-only FS etc. -> console only
        pass
    logger.propagate = False
    return logger
