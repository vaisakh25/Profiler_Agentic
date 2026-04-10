"""One-call logging configuration for the MCP server process."""

from __future__ import annotations

import logging
import logging.handlers
import sys
import tempfile
from pathlib import Path

from file_profiler.config.env import LOG_LEVEL, LOG_FORMAT, OUTPUT_DIR


def _resolve_log_dir() -> Path:
    """Resolve a writable log directory.

    Falls back to workspace and tmp locations when OUTPUT_DIR is not writable.
    """
    candidates = (
        Path(OUTPUT_DIR) / "logs",
        Path.cwd() / ".profiler_logs",
        Path(tempfile.gettempdir()) / "file_profiler_logs",
    )

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            if candidate != Path(OUTPUT_DIR) / "logs":
                sys.stderr.write(
                    f"[file_profiler] log dir fallback in use: {candidate}\n"
                )
            return candidate
        except OSError:
            continue

    raise OSError("No writable log directory available")


def configure_logging() -> None:
    """Configure root logger with rotation.  Call once at process startup.

    Logs go to stderr — stdout is reserved for the MCP stdio JSON-RPC
    transport.  Writing log lines to stdout would corrupt the protocol.
    
    Also writes to a rotating log file to prevent disk space exhaustion.
    """
    # Create logs directory
    log_dir = _resolve_log_dir()
    log_file = log_dir / "profiler.log"
    
    # Reset existing handlers first. Some frameworks install debug handlers
    # before we run, which causes duplicate/noisy output.
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

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
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("openai._base_client").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("mcp").setLevel(logging.WARNING)
    logging.getLogger("mcp.server").setLevel(logging.WARNING)
    logging.getLogger("mcp.server.sse").setLevel(logging.WARNING)
    logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)
    logging.getLogger("sse_starlette").setLevel(logging.WARNING)
