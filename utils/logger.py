"""
Logging setup for LeadHarvest.
Writes logs to output/logs/ and also prints INFO+ to the terminal.
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

LOG_PATH = os.getenv("LOG_PATH", "output/logs/")


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger that writes to both a timestamped log file
    and the terminal (stdout).

    Args:
        name: Logger name, typically __name__ of the calling module.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if the logger is requested multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Ensure the log directory exists
    log_dir = Path(LOG_PATH)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Timestamped log file — one file per run session
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"leadharvest_{timestamp}.log"

    # File handler — captures everything (DEBUG and above)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    # Console handler — shows INFO and above in the terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(levelname)-8s | %(message)s")
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
