"""Terminal progress display for the chatbot.

Provides an animated spinner with elapsed time during tool execution,
a cumulative progress bar with percentage, and smart result summaries.

Design goals:
  - No external dependencies (no tqdm, no rich) — pure ANSI escape codes
  - Async-friendly — spinner runs as a background task
  - Windows Terminal compatible
  - Weighted progress — tools have different costs
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from typing import Optional


# ---------------------------------------------------------------------------
# Tool weights — relative cost of each tool in a typical workflow
# ---------------------------------------------------------------------------

# These weights reflect real-world wall-clock time.  They don't need to
# sum to 100 — the progress bar normalises against the running total.
TOOL_WEIGHTS: dict[str, float] = {
    "list_supported_files":     5,
    "profile_file":            15,
    "profile_directory":       35,
    "detect_relationships":    25,
    "enrich_relationships":    60,  # heaviest — profile + detect + embed + LLM
    "get_quality_summary":     10,
    "upload_file":              5,
    "query_knowledge_base":     5,
    "get_table_relationships":  5,
    "compare_profiles":        20,
    "check_enrichment_status":  3,
    "visualize_profile":        5,
}

REMOTE_TOOL_ALIASES: dict[str, str] = {
    "profile_remote_source": "profile_directory",
    "remote_detect_relationships": "detect_relationships",
    "remote_enrich_relationships": "enrich_relationships",
    "remote_check_enrichment_status": "check_enrichment_status",
    "remote_reset_vector_store": "reset_vector_store",
    "remote_get_quality_summary": "get_quality_summary",
    "remote_query_knowledge_base": "query_knowledge_base",
    "remote_get_table_relationships": "get_table_relationships",
    "remote_compare_profiles": "compare_profiles",
    "remote_visualize_profile": "visualize_profile",
}

# Fallback for unknown tools
DEFAULT_TOOL_WEIGHT: float = 10

# Spinner frames
_SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

# Progress bar characters
_BAR_FILL = "█"
_BAR_EMPTY = "░"
_BAR_WIDTH = 30


# ---------------------------------------------------------------------------
# ProgressTracker — manages state across an entire agent turn
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Tracks cumulative progress across multiple tool calls in one turn.

    Usage::

        tracker = ProgressTracker()
        tracker.start_tool("profile_directory", {"dir_path": "./data"})
        # ... tool executes ...
        tracker.finish_tool("profile_directory", result_content)
        tracker.print_summary()
    """

    def __init__(self) -> None:
        self._completed_weight: float = 0.0
        self._total_weight: float = 0.0
        self._tool_count: int = 0
        self._spinner_task: Optional[asyncio.Task] = None
        self._current_tool: Optional[str] = None
        self._start_time: float = 0.0
        self._turn_start: float = time.time()

    @property
    def elapsed_total(self) -> float:
        """Total elapsed time for the current turn."""
        return time.time() - self._turn_start

    # --- Spinner control ---------------------------------------------------

    async def start_tool(self, tool_name: str, args: dict) -> None:
        """Begin tracking a tool call — starts the animated spinner."""
        self._tool_count += 1
        self._current_tool = tool_name
        self._start_time = time.time()

        weight = TOOL_WEIGHTS.get(canonicalize_tool_name(tool_name), DEFAULT_TOOL_WEIGHT)
        self._total_weight += weight

        # Print tool call header
        args_short = ", ".join(
            f"{k}={_truncate(str(v), 50)}" for k, v in args.items()
        )
        _clear_line()
        sys.stdout.write(
            f"\r  [{self._tool_count}] {tool_name}({args_short})\n"
        )
        sys.stdout.flush()

        # Start spinner
        self._spinner_task = asyncio.create_task(
            self._spin(tool_name)
        )

    async def finish_tool(self, tool_name: str, content: str) -> None:
        """Tool completed — stop spinner, show result + progress bar."""
        # Stop spinner
        if self._spinner_task and not self._spinner_task.done():
            self._spinner_task.cancel()
            try:
                await self._spinner_task
            except asyncio.CancelledError:
                pass

        elapsed = time.time() - self._start_time
        weight = TOOL_WEIGHTS.get(canonicalize_tool_name(tool_name), DEFAULT_TOOL_WEIGHT)
        self._completed_weight += weight

        # Clear spinner line and print completion
        _clear_line()

        # Smart result summary
        summary = _extract_summary(tool_name, content)
        check = "✓" if "Error" not in content[:100] else "✗"

        sys.stdout.write(
            f"\r      {check} Done in {_fmt_time(elapsed)} — {summary}\n"
        )

        # Progress bar
        pct = (self._completed_weight / self._total_weight * 100) if self._total_weight > 0 else 0
        pct = min(pct, 100)
        bar = _render_bar(pct)
        sys.stdout.write(f"      {bar}\n")
        sys.stdout.flush()

    async def finish_thinking(self) -> None:
        """Agent is done thinking — stop any active spinner."""
        if self._spinner_task and not self._spinner_task.done():
            self._spinner_task.cancel()
            try:
                await self._spinner_task
            except asyncio.CancelledError:
                pass
            _clear_line()

    def print_summary(self) -> None:
        """Print final turn summary with total time."""
        if self._tool_count > 0:
            total = time.time() - self._turn_start
            sys.stdout.write(
                f"\n      Pipeline complete: {self._tool_count} "
                f"step{'s' if self._tool_count != 1 else ''} "
                f"in {_fmt_time(total)}\n"
            )
            sys.stdout.flush()

    async def start_thinking(self) -> None:
        """Show a thinking indicator while the LLM reasons."""
        if self._spinner_task and not self._spinner_task.done():
            self._spinner_task.cancel()
            try:
                await self._spinner_task
            except asyncio.CancelledError:
                pass

        self._spinner_task = asyncio.create_task(
            self._spin_thinking()
        )

    # --- Internal ----------------------------------------------------------

    async def _spin(self, tool_name: str) -> None:
        """Animated spinner with elapsed time — runs until cancelled."""
        i = 0
        stage_hints = _get_stage_hints(tool_name)
        hint_idx = 0
        last_hint_change = time.time()

        try:
            while True:
                elapsed = time.time() - self._start_time
                frame = _SPINNER[i % len(_SPINNER)]

                # Rotate hints every few seconds for long-running tools
                if stage_hints and (time.time() - last_hint_change) > 8:
                    hint_idx = min(hint_idx + 1, len(stage_hints) - 1)
                    last_hint_change = time.time()

                hint = stage_hints[hint_idx] if stage_hints else "working"

                _clear_line()
                sys.stdout.write(
                    f"\r      {frame} {hint}... ({_fmt_time(elapsed)})"
                )
                sys.stdout.flush()
                i += 1
                await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            pass

    async def _spin_thinking(self) -> None:
        """Thinking spinner while LLM processes."""
        i = 0
        try:
            while True:
                frame = _SPINNER[i % len(_SPINNER)]
                _clear_line()
                sys.stdout.write(f"\r      {frame} Thinking...")
                sys.stdout.flush()
                i += 1
                await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            _clear_line()


# ---------------------------------------------------------------------------
# Stage hints — tool-specific status messages that rotate during execution
# ---------------------------------------------------------------------------

def _get_stage_hints(tool_name: str) -> list[str]:
    """Return stage hints for a tool — rotated during long executions."""
    hints = {
        "list_supported_files": [
            "Scanning directory",
            "Detecting file formats",
        ],
        "profile_file": [
            "Reading file",
            "Inferring column types",
            "Computing statistics",
            "Checking quality",
        ],
        "profile_directory": [
            "Scanning files",
            "Profiling tables",
            "Inferring column types",
            "Computing statistics",
            "Checking quality",
        ],
        "detect_relationships": [
            "Profiling tables",
            "Matching column names",
            "Checking type compatibility",
            "Scoring FK candidates",
            "Saving intermediate results",
        ],
        "enrich_relationships": [
            "Profiling tables",
            "Detecting relationships",
            "MAP: Summarising tables & columns",
            "APPLY: Writing descriptions to profiles",
            "EMBED: Storing in vector DB",
            "COLUMN CLUSTER: DBSCAN grouping",
            "DERIVE: PK/FK from clusters",
            "TABLE CLUSTER: Affinity grouping",
            "REDUCE: LLM synthesis",
            "Generating enriched ER diagram",
        ],
        "get_quality_summary": [
            "Profiling file",
            "Analysing quality flags",
        ],
        "upload_file": [
            "Decoding upload",
            "Saving file",
        ],
        "query_knowledge_base": [
            "Searching vector store",
            "Ranking results",
        ],
        "get_table_relationships": [
            "Loading relationships",
            "Matching table",
        ],
        "compare_profiles": [
            "Profiling current state",
            "Loading previous fingerprints",
            "Comparing schemas",
        ],
        "check_enrichment_status": [
            "Computing fingerprints",
            "Checking completion manifest",
        ],
        "visualize_profile": [
            "Loading profile data",
            "Generating charts",
            "Saving chart images",
        ],
    }
    return hints.get(canonicalize_tool_name(tool_name), ["Processing"])


# ---------------------------------------------------------------------------
# Smart result summaries — extract meaningful info from tool results
# ---------------------------------------------------------------------------

def _extract_summary(tool_name: str, content: str) -> str:
    """Parse tool result content and return a human-readable summary."""
    try:
        data = json.loads(content) if content.startswith(("{", "[")) else None
    except (json.JSONDecodeError, TypeError):
        data = None

    if data is None:
        # Check for error messages
        if "Error" in content[:200]:
            return _truncate(content, 80)
        if "erDiagram" in content:
            return "ER diagram generated"
        return f"{len(content):,} chars of results"

    tool_name = canonicalize_tool_name(tool_name)

    if tool_name == "list_supported_files" and isinstance(data, list):
        formats = {}
        db_table_count = 0
        for f in data:
            fmt = f.get("detected_format", "unknown")
            formats[fmt] = formats.get(fmt, 0) + 1
            db_table_count += f.get("table_count", 0)
        parts = [f"{c} {fmt}" for fmt, c in sorted(formats.items(), key=lambda x: -x[1])]
        summary = f"{len(data)} files found ({', '.join(parts)})"
        if db_table_count:
            summary += f", {db_table_count} database tables"
        return summary

    if tool_name == "profile_file" and isinstance(data, list):
        # Database file — returned a list of table profiles
        total_rows = sum(p.get("row_count", 0) for p in data)
        return f"{len(data)} tables profiled ({total_rows:,} total rows)"

    if tool_name == "profile_file" and isinstance(data, dict):
        name = data.get("table_name", "?")
        rows = data.get("row_count", 0)
        cols = len(data.get("columns", []))
        return f"{name}: {rows:,} rows, {cols} columns"

    if tool_name == "profile_directory" and isinstance(data, list):
        total_rows = sum(p.get("row_count", 0) for p in data)
        return f"{len(data)} tables profiled ({total_rows:,} total rows)"

    if tool_name == "detect_relationships" and isinstance(data, dict):
        n = len(data.get("candidates", []))
        has_er = "erDiagram" in str(data.get("er_diagram", ""))
        suffix = " + ER diagram" if has_er else ""
        return f"{n} FK candidates detected{suffix}"

    if tool_name == "enrich_relationships" and isinstance(data, dict):
        tables = data.get("tables_analyzed", 0)
        rels = data.get("relationships_analyzed", 0)
        derived = data.get("cluster_derived_relationships", 0)
        col_clusters = data.get("column_clusters_formed", 0)
        enrichment_len = len(data.get("enrichment", ""))
        parts = [f"{tables} tables, {rels} deterministic rels"]
        if derived:
            parts.append(f"{derived} cluster-derived rels ({col_clusters} clusters)")
        parts.append(f"{enrichment_len:,} chars of LLM analysis")
        return ", ".join(parts)

    if tool_name == "get_quality_summary" and isinstance(data, dict):
        name = data.get("table_name", "?")
        qs = data.get("quality_summary", {})
        issues = qs.get("columns_with_issues", 0)
        total = qs.get("columns_profiled", 0)
        return f"{name}: {issues}/{total} columns with issues"

    if tool_name == "upload_file" and isinstance(data, dict):
        size = data.get("size_bytes", 0)
        return f"Uploaded ({size:,} bytes)"

    if tool_name == "query_knowledge_base" and isinstance(data, dict):
        tables = data.get("total_table_matches", 0)
        cols = data.get("total_column_matches", 0)
        return f"{tables} table matches, {cols} column matches"

    if tool_name == "get_table_relationships" and isinstance(data, dict):
        det = data.get("total_deterministic", 0)
        vec = data.get("total_vector_discovered", 0)
        related = len(data.get("related_tables", []))
        return f"{det} deterministic, {vec} vector-discovered, {related} related tables"

    if tool_name == "compare_profiles" and isinstance(data, dict):
        return data.get("summary", "comparison complete")

    if tool_name == "check_enrichment_status" and isinstance(data, dict):
        status = data.get("status", "unknown")
        return f"Status: {status} — {data.get('reason', '')}"[:80]

    if tool_name == "visualize_profile" and isinstance(data, dict):
        n = data.get("chart_count", len(data.get("charts", [])))
        table = data.get("table_name", "")
        return f"{n} chart(s) generated for {table}"

    return f"{len(content):,} chars of results"


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _render_bar(pct: float) -> str:
    """Render a progress bar string: ████████░░░░ 67%"""
    filled = int(_BAR_WIDTH * pct / 100)
    empty = _BAR_WIDTH - filled
    bar = _BAR_FILL * filled + _BAR_EMPTY * empty
    return f"{bar} {pct:.0f}%"


def _fmt_time(seconds: float) -> str:
    """Format seconds to a human-friendly string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.0f}s"


def _truncate(text: str, max_len: int) -> str:
    """Truncate text with ellipsis."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _clear_line() -> None:
    """Clear the current terminal line."""
    sys.stdout.write("\r\033[K")
    sys.stdout.flush()


def canonicalize_tool_name(tool_name: str) -> str:
    """Map remote pipeline tool names onto their local equivalents."""
    return REMOTE_TOOL_ALIASES.get(tool_name, tool_name)
