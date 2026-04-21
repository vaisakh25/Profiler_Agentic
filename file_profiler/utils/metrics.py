"""Prometheus metrics for the data profiler.

All metric operations no-op if prometheus_client is not installed,
so this module is always safe to import.

Usage:
    from file_profiler.utils.metrics import TABLES_PROFILED, LLM_CALLS
    TABLES_PROFILED.inc()
    with LLM_CALL_DURATION.time():
        await llm.ainvoke(...)
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Histogram, Gauge, REGISTRY

    TABLES_PROFILED = Counter(
        "profiler_tables_profiled_total",
        "Total tables profiled",
    )
    TABLES_ENRICHED = Counter(
        "profiler_tables_enriched_total",
        "Total tables enriched via MAP phase",
    )
    TABLES_FAILED = Counter(
        "profiler_tables_failed_total",
        "Total tables that failed enrichment (dead letters)",
    )

    LLM_CALLS = Counter(
        "profiler_llm_calls_total",
        "Total LLM API calls",
        ["provider", "phase"],
    )
    LLM_ERRORS = Counter(
        "profiler_llm_errors_total",
        "Total LLM API errors",
        ["provider", "error_type"],
    )
    LLM_CALL_DURATION = Histogram(
        "profiler_llm_call_duration_seconds",
        "LLM call duration in seconds",
        ["provider", "phase"],
        buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120),
    )
    LLM_RATE_LIMITS = Counter(
        "profiler_llm_rate_limits_total",
        "Total 429 rate limit responses from LLM providers",
        ["provider"],
    )

    ENRICHMENT_DURATION = Histogram(
        "profiler_enrichment_duration_seconds",
        "Total enrichment pipeline duration",
        buckets=(10, 30, 60, 120, 300, 600, 1200, 3600),
    )
    ENRICHMENT_PHASE_DURATION = Histogram(
        "profiler_enrichment_phase_duration_seconds",
        "Duration of each enrichment phase",
        ["phase"],
        buckets=(1, 5, 10, 30, 60, 120, 300, 600),
    )

    ACTIVE_WEBSOCKET_SESSIONS = Gauge(
        "profiler_active_websocket_sessions",
        "Number of active WebSocket sessions",
    )

    MCP_TOOL_CALLS = Counter(
        "profiler_mcp_tool_calls_total",
        "Total MCP tool invocations",
        ["tool_name"],
    )

    METRICS_AVAILABLE = True
    log.debug("Prometheus metrics initialized")

except ImportError:
    # prometheus_client not installed — create no-op stubs

    class _NoOp:
        """No-op metric that silently ignores all operations."""
        def inc(self, *a, **kw): pass
        def dec(self, *a, **kw): pass
        def set(self, *a, **kw): pass
        def observe(self, *a, **kw): pass
        def time(self):
            import contextlib
            return contextlib.nullcontext()
        def labels(self, *a, **kw): return self

    _noop = _NoOp()

    TABLES_PROFILED = _noop
    TABLES_ENRICHED = _noop
    TABLES_FAILED = _noop
    LLM_CALLS = _noop
    LLM_ERRORS = _noop
    LLM_CALL_DURATION = _noop
    LLM_RATE_LIMITS = _noop
    ENRICHMENT_DURATION = _noop
    ENRICHMENT_PHASE_DURATION = _noop
    ACTIVE_WEBSOCKET_SESSIONS = _noop
    MCP_TOOL_CALLS = _noop

    METRICS_AVAILABLE = False
