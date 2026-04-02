"""One-call logging configuration for the MCP server process."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

from file_profiler.config.env import LOG_LEVEL, LOG_FORMAT, OUTPUT_DIR


def configure_logging() -> None:
    """Configure root logger with rotation.  Call once at process startup.

    Logs go to stderr — stdout is reserved for the MCP stdio JSON-RPC
    transport.  Writing log lines to stdout would corrupt the protocol.
    
    Also writes to a rotating log file to prevent disk space exhaustion.
    """
    # Create logs directory
    log_dir = Path(OUTPUT_DIR) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "profiler.log"
    
    # Console handler (stderr)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    
    # Rotating file handler (prevents disk full)
    # 10 MB per file, keep 5 backup files (50 MB total)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)  # File logs are more verbose
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
