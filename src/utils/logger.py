"""Module for configuring Loguru logging library."""

import sys

from loguru import logger


def log_configure():
    logger.remove()

    console_handler = {
        "sink": sys.stdout,
        "format": "{time:YYYY-MM-DD HH:mm:ss} ({elapsed}) | "
        + "<level>{level}</level> | "
        + "{name}: "
        + "<level>{message}</level>",
    }

    file_handler = {
        "sink": "logs/calendar.log",
        "format": "{time:YYYY-MM-DD HH:mm:ss} ({elapsed}) | "
        + "<level>{level}</level> | "
        + "{name}: "
        + "{function}: "
        + "{line} | "
        + "<level>{message}</level>",
        "rotation": "10 MB",
        "compression": "tar.gz",
        "retention": 5,
    }

    logger.configure(handlers=[console_handler, file_handler])
