"""Shared logger configuration for the PureFlow project."""

import logging
import sys


def setup_logger():
    """Configures the global logger for the application."""
    new_logger = logging.getLogger("pureflow")
    new_logger.setLevel(logging.INFO)

    # Console Handler
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    if not new_logger.handlers:
        new_logger.addHandler(handler)

    return new_logger


# Initialize global logger
logger = setup_logger()
