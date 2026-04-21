"""Structured logging configuration for the MCP server process.

Supports two modes controlled by the ``LOG_JSON`` env var:
- **JSON mode** (``LOG_JSON=true``): machine-parseable structured JSON to stderr.
  Suitable for production log aggregation (ELK, CloudWatch, Datadog).
- **Console mode** (default): human-readable colored output to stderr.
  Suitable for local development.

Both modes integrate with stdlib ``logging`` so existing
``log = logging.getLogger(__name__)`` calls work unchanged.
"""

from __future__ import annotations

import logging
import sys

from file_profiler.config.env import LOG_JSON, LOG_LEVEL, LOG_FORMAT


def configure_logging() -> None:
    """Configure root logger with structlog.  Call once at process startup.

    Logs go to stderr — stdout is reserved for the MCP stdio JSON-RPC
    transport.  Writing log lines to stdout would corrupt the protocol.
    """
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

    try:
        import structlog

        shared_processors = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
        ]

        if LOG_JSON:
            renderer = structlog.processors.JSONRenderer()
        else:
            renderer = structlog.dev.ConsoleRenderer()

        structlog.configure(
            processors=[
                *shared_processors,
                structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
            ],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        formatter = structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                renderer,
            ],
        )

        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(formatter)

        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(handler)
        root.setLevel(level)

    except ImportError:
        # structlog not installed — fall back to plain logging
        logging.basicConfig(
            level=level,
            format=LOG_FORMAT,
            stream=sys.stderr,
        )

    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("chromadb").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
