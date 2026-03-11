"""One-call logging configuration for the MCP server process."""

from __future__ import annotations

import logging
import sys

from file_profiler.config.env import LOG_LEVEL, LOG_FORMAT


def configure_logging() -> None:
    """Configure root logger.  Call once at process startup.

    Logs go to stderr — stdout is reserved for the MCP stdio JSON-RPC
    transport.  Writing log lines to stdout would corrupt the protocol.
    """
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format=LOG_FORMAT,
        stream=sys.stderr,
    )
    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
