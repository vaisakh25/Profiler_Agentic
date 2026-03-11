"""
MCP Server for the Agentic Data Profiler.

Exposes the file_profiler pipeline as MCP tools, resources, and prompts.
Tools are thin wrappers — all business logic lives in file_profiler.main.

Transports:
  stdio — for local use (Claude Desktop, Claude Code, LangGraph local)
  sse   — for containerised / remote deployment

Usage:
  python -m file_profiler --transport stdio
  python -m file_profiler --transport sse --host 0.0.0.0 --port 8081
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import OrderedDict
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP, Context

from file_profiler.config.env import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_TRANSPORT,
    OUTPUT_DIR,
)
from file_profiler.main import (
    profile_file as _pipeline_profile_file,
    profile_directory as _pipeline_profile_directory,
    analyze_relationships as _pipeline_analyze,
    _SCANNABLE_EXTENSIONS,
)
from file_profiler.intake.validator import validate
from file_profiler.classification.classifier import classify
from file_profiler.output.er_diagram_writer import generate as _generate_er_diagram
from file_profiler.output.profile_writer import serialise, compute_quality_summary
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport
from file_profiler.utils.file_resolver import resolve_path, save_upload, cleanup_expired_uploads
from file_profiler.utils.logging_setup import configure_logging

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Server instance
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="file-profiler",
    instructions=(
        "Agentic Data Profiler — profile CSV, Parquet, and other tabular "
        "data files.  Detects schemas, types, quality issues, and cross-table "
        "foreign key relationships."
    ),
)

# ---------------------------------------------------------------------------
# In-memory caches (bounded LRU)
# ---------------------------------------------------------------------------

_PROFILE_CACHE_MAX_SIZE: int = 200


class _LRUCache(OrderedDict):
    """OrderedDict-based LRU cache with a max size."""

    def __init__(self, max_size: int) -> None:
        super().__init__()
        self._max_size = max_size

    def __setitem__(self, key: str, value: dict) -> None:
        if key in self:
            self.move_to_end(key)
        super().__setitem__(key, value)
        if len(self) > self._max_size:
            oldest = next(iter(self))
            del self[oldest]
            log.debug("Cache evicted: %s (max %d)", oldest, self._max_size)

    def __getitem__(self, key: str) -> dict:
        self.move_to_end(key)
        return super().__getitem__(key)


_profile_cache: _LRUCache = _LRUCache(_PROFILE_CACHE_MAX_SIZE)
_relationship_cache: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Health endpoint (replaces /sse-based healthcheck)
# ---------------------------------------------------------------------------

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request) -> "JSONResponse":
    from starlette.responses import JSONResponse
    return JSONResponse({
        "status": "ok",
        "server": "file-profiler",
        "cached_profiles": len(_profile_cache),
    })


# ---------------------------------------------------------------------------
# Serialisation helpers  (reuse pipeline serialiser — no duplication)
# ---------------------------------------------------------------------------

def _to_dict(profile: FileProfile) -> dict:
    """Serialise a FileProfile to a JSON-compatible dict."""
    profile.quality_summary = compute_quality_summary(profile)
    data = serialise(profile)
    data["low_cardinality_columns"] = [
        {
            "name": col.name,
            "distinct_count": col.distinct_count,
            "top_values": serialise(col.top_values),
        }
        for col in profile.columns
        if col.is_low_cardinality
    ]
    return data


def _report_to_dict(
    report: RelationshipReport,
    min_confidence: float = 0.0,
) -> dict:
    """Serialise a RelationshipReport, optionally filtering by confidence."""
    data = serialise(report)
    if min_confidence > 0:
        data["candidates"] = [
            c for c in data.get("candidates", [])
            if c.get("confidence", 0) >= min_confidence
        ]
    return data


# ═══════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def profile_file(file_path: str, ctx: Context) -> dict:
    """
    Profile a single data file through the full 11-layer pipeline.

    Runs: intake → classification → size strategy → format engine →
    standardization → column profiling → type inference → quality checks.

    Args:
        file_path: Path to the file (inside mounted volume or upload dir).

    Returns:
        Complete FileProfile with columns, types, quality flags, and statistics.
    """
    resolved = resolve_path(file_path)
    await ctx.report_progress(0, 3, "Starting profiling pipeline")

    # Bridge sync pipeline progress → async MCP progress
    async def _report(step: int, total: int, msg: str) -> None:
        await ctx.report_progress(step, total, msg)

    def _sync_progress(step: int, total: int, msg: str) -> None:
        import asyncio
        try:
            loop = asyncio.get_running_loop()
            asyncio.run_coroutine_threadsafe(_report(step, total, msg), loop)
        except RuntimeError:
            pass  # No running loop — skip progress (e.g. in tests)

    result = _pipeline_profile_file(
        resolved,
        output_dir=OUTPUT_DIR,
        progress_callback=_sync_progress,
    )

    await ctx.report_progress(2, 3, "Serialising results")
    profile_dict = _to_dict(result)
    _profile_cache[result.table_name] = profile_dict

    await ctx.report_progress(3, 3, "Complete")
    log.info("Profiled: %s (%d columns, %d rows)",
             result.table_name, len(result.columns), result.row_count)
    return profile_dict


@mcp.tool()
async def profile_directory(
    dir_path: str,
    parallel: bool = True,
    ctx: Context = None,
) -> list[dict]:
    """
    Profile all supported files in a directory.

    Scans for CSV, Parquet, and other supported formats.  Profiles each
    through the full pipeline.  Failed files are logged but do not block others.

    Args:
        dir_path: Path to directory containing data files.
        parallel: Whether to profile files in parallel (default True).

    Returns:
        List of FileProfile dicts, one per successfully profiled file.
    """
    resolved = resolve_path(dir_path)

    if ctx:
        await ctx.report_progress(0, 2, "Scanning directory")

    results = _pipeline_profile_directory(
        resolved, output_dir=OUTPUT_DIR, parallel=parallel,
    )

    if ctx:
        await ctx.report_progress(1, 2, "Serialising results")

    profiles = []
    for r in results:
        d = _to_dict(r)
        _profile_cache[r.table_name] = d
        profiles.append(d)

    if ctx:
        await ctx.report_progress(2, 2, f"Complete — {len(profiles)} files profiled")

    log.info("Directory profiled: %s (%d files)", dir_path, len(profiles))
    return profiles


@mcp.tool()
async def detect_relationships(
    dir_path: str,
    confidence_threshold: float = 0.50,
    ctx: Context = None,
) -> dict:
    """
    Detect foreign key relationships across tables in a directory.

    Profiles all files first (if not cached), then runs the relationship
    detector across all table pairs.  Scores based on naming conventions,
    type compatibility, cardinality, and value overlap.

    Args:
        dir_path: Path to directory with data files.
        confidence_threshold: Minimum confidence to include (default 0.50).

    Returns:
        RelationshipReport with FK candidates sorted by confidence.
    """
    global _relationship_cache

    resolved = resolve_path(dir_path)

    if ctx:
        await ctx.report_progress(0, 3, "Profiling directory")

    results = _pipeline_profile_directory(
        resolved, output_dir=OUTPUT_DIR, parallel=True,
    )

    # Cache individual profiles as a side effect
    for r in results:
        _profile_cache[r.table_name] = _to_dict(r)

    if ctx:
        await ctx.report_progress(1, 3, "Detecting relationships")

    report = _pipeline_analyze(
        results,
        output_path=OUTPUT_DIR / "relationships.json",
        er_diagram_path=OUTPUT_DIR / "er_diagram.md",
    )

    if ctx:
        await ctx.report_progress(2, 3, "Serialising report")

    result = _report_to_dict(report, min_confidence=confidence_threshold)
    er_lines = _generate_er_diagram(results, report, min_confidence=confidence_threshold)
    result["er_diagram"] = "\n".join(er_lines)
    _relationship_cache = result

    if ctx:
        await ctx.report_progress(3, 3, "Complete")

    log.info("Relationships detected: %d candidates",
             len(result.get("candidates", [])))
    return result


@mcp.tool()
async def enrich_relationships(
    dir_path: str,
    provider: str = "google",
    model: str | None = None,
    incremental: bool = True,
    ctx: Context = None,
) -> dict:
    """
    Enrich detected relationships using a scalable map-reduce LLM pipeline.

    Automatically selects between two execution paths based on table count:

    **Small datasets** (≤ CLUSTER_TARGET_SIZE tables, default 15):
      1. MAP:    Summarize each table via LLM (parallel)
      2. EMBED:  Store summaries in persistent ChromaDB vector store
      3. REDUCE: Single focused LLM prompt → full analysis + ER diagram

    **Large datasets** (> CLUSTER_TARGET_SIZE tables):
      1. MAP:    Summarize each table via LLM (parallel)
      2. EMBED:  Store summaries in persistent ChromaDB
      3. CLUSTER: Group semantically similar tables using embedding similarity
      4. REDUCE per cluster: Focused LLM analysis per cluster (parallel)
      5. META-REDUCE: Synthesise cross-cluster insights → final report + ER diagram

    When incremental=True (default), unchanged tables reuse cached summaries.

    Args:
        dir_path:     Path to directory with data files.
        provider:     LLM provider — "google" (default), "groq", "openai", or "anthropic".
        model:        Model name override (default: provider's default model).
        incremental:  If True, reuse cached summaries for unchanged tables.

    Returns:
        Dict with enrichment analysis, metadata (tables_summarized, clusters_formed, etc.).
    """
    from file_profiler.agent.enrichment_mapreduce import enrich

    resolved = resolve_path(dir_path)

    if ctx:
        await ctx.report_progress(0, 5, "Profiling directory")

    results = _pipeline_profile_directory(
        resolved, output_dir=OUTPUT_DIR, parallel=True,
    )

    for r in results:
        _profile_cache[r.table_name] = _to_dict(r)

    if ctx:
        await ctx.report_progress(1, 5, "Detecting relationships")

    report = _pipeline_analyze(
        results,
        output_path=OUTPUT_DIR / "relationships.json",
        er_diagram_path=OUTPUT_DIR / "er_diagram.md",
    )

    if ctx:
        await ctx.report_progress(2, 5, "MAP: Summarizing tables")

    enrichment_result = await enrich(
        profiles=results,
        report=report,
        dir_path=dir_path,
        provider=provider,
        model=model,
        incremental=incremental,
    )

    if ctx:
        clusters = enrichment_result.get("clusters_formed", 1)
        if clusters > 1:
            await ctx.report_progress(5, 5,
                f"Enrichment complete ({clusters} clusters)")
        else:
            await ctx.report_progress(5, 5, "Enrichment complete")

    log.info(
        "Enrichment complete: %d tables (%d summarized, %d cached), "
        "%d relationships, %d clusters, %d docs embedded",
        enrichment_result["tables_analyzed"],
        enrichment_result["tables_summarized"],
        enrichment_result["tables_cached"],
        enrichment_result["relationships_analyzed"],
        enrichment_result.get("clusters_formed", 1),
        enrichment_result["documents_embedded"],
    )
    return enrichment_result


@mcp.tool()
async def list_supported_files(dir_path: str, ctx: Context = None) -> list[dict]:
    """
    List files in a directory that the profiler can handle.

    Runs intake validation and classification only — no full profiling.
    Useful for reconnaissance before deciding which files to profile.

    Args:
        dir_path: Path to directory to scan.

    Returns:
        List of dicts with file_name, file_path, size_bytes, detected_format.
    """
    resolved = resolve_path(dir_path)

    if not resolved.is_dir():
        raise ValueError(f"Not a directory: {dir_path}")

    candidates = sorted(
        f for f in resolved.iterdir()
        if f.is_file() and f.suffix.lower() in _SCANNABLE_EXTENSIONS
    )

    if ctx:
        await ctx.report_progress(0, len(candidates), "Scanning files")

    file_infos: list[dict] = []
    for i, fpath in enumerate(candidates):
        try:
            intake = validate(fpath)
            fmt = classify(intake)
            file_infos.append({
                "file_name": fpath.name,
                "file_path": str(fpath),
                "size_bytes": intake.size_bytes,
                "detected_format": fmt.value,
                "encoding": intake.encoding,
                "compression": intake.compression,
            })
        except Exception as exc:
            file_infos.append({
                "file_name": fpath.name,
                "file_path": str(fpath),
                "error": str(exc),
            })

        if ctx:
            await ctx.report_progress(i + 1, len(candidates))

    return file_infos


@mcp.tool()
async def upload_file(
    file_name: str,
    file_content_base64: str,
    ctx: Context = None,
) -> dict:
    """
    Upload a file for profiling (base64-encoded).

    Decodes and saves the file to the server's upload directory.
    Returns the server-side path that can be passed to profile_file.

    Args:
        file_name: Original file name (e.g. 'customers.csv').
        file_content_base64: Base64-encoded file content.

    Returns:
        Dict with server_path, size_bytes, and a usage hint.
    """
    if ctx:
        await ctx.report_progress(0, 2, "Decoding upload")

    # Opportunistic cleanup of expired uploads
    cleanup_expired_uploads()

    dest = save_upload(file_name, file_content_base64)

    if ctx:
        await ctx.report_progress(2, 2, "Upload complete")

    return {
        "server_path": str(dest),
        "size_bytes": dest.stat().st_size,
        "message": f"File saved.  Use profile_file('{dest}') to profile it.",
    }


@mcp.tool()
async def get_quality_summary(file_path: str, ctx: Context = None) -> dict:
    """
    Get a quality summary for a single file.

    Runs the full profile pipeline and extracts the quality_summary section.
    If the file was already profiled, returns cached quality data.

    Args:
        file_path: Path to the file.

    Returns:
        Quality dict with columns_profiled, columns_with_issues,
        null_heavy_columns, type_conflict_columns, structural_issues.
    """
    resolved = resolve_path(file_path)
    table_name = resolved.stem

    # Return from cache if available
    if table_name in _profile_cache:
        cached = _profile_cache[table_name]
        return {
            "table_name": table_name,
            "quality_summary": cached.get("quality_summary", {}),
            "structural_issues": cached.get("structural_issues", []),
            "source": "cache",
        }

    if ctx:
        await ctx.report_progress(0, 2, "Profiling file for quality check")

    result = _pipeline_profile_file(resolved, output_dir=OUTPUT_DIR)
    profile_dict = _to_dict(result)
    _profile_cache[result.table_name] = profile_dict

    if ctx:
        await ctx.report_progress(2, 2, "Complete")

    return {
        "table_name": result.table_name,
        "quality_summary": profile_dict.get("quality_summary", {}),
        "structural_issues": profile_dict.get("structural_issues", []),
        "source": "fresh",
    }


# ═══════════════════════════════════════════════════════════════════════════
# RESOURCES
# ═══════════════════════════════════════════════════════════════════════════

@mcp.resource("profiles://{table_name}")
async def get_cached_profile(table_name: str) -> str:
    """Return a previously generated profile by table name."""
    if table_name not in _profile_cache:
        return json.dumps({
            "error": f"No cached profile for '{table_name}'.  Run profile_file first.",
        })
    return json.dumps(_profile_cache[table_name], indent=2)


@mcp.resource("relationships://latest")
async def get_cached_relationships() -> str:
    """Return the most recent relationship report."""
    if _relationship_cache is None:
        return json.dumps({
            "error": "No relationship report cached.  Run detect_relationships first.",
        })
    return json.dumps(_relationship_cache, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.prompt()
async def summarize_profile(table_name: str) -> str:
    """Generate a natural-language summary prompt for a profiled table."""
    profile = _profile_cache.get(table_name)
    if profile is None:
        content = f"No profile found for table '{table_name}'.  Please run profile_file first."
    else:
        content = json.dumps(profile, indent=2)

    return (
        f"Analyse the following data profile for table '{table_name}'.\n"
        f"Provide a concise summary covering:\n"
        f"1. Row count and column count\n"
        f"2. Column types breakdown (how many integer, string, date, etc.)\n"
        f"3. Key candidates (columns likely to be primary keys)\n"
        f"4. Quality issues (null-heavy columns, type conflicts, structural problems)\n"
        f"5. List low cardinality columns and their distinct value counts\n"
        f"6. Notable patterns (constant columns, high cardinality, sparse columns)\n\n"
        f"Profile data:\n{content}"
    )


@mcp.prompt()
async def migration_readiness(dir_path: str) -> str:
    """Assess migration readiness for a set of data files."""
    profiles_summary = {
        name: p.get("quality_summary", {})
        for name, p in _profile_cache.items()
    }
    rels = _relationship_cache or {}

    return (
        f"Assess migration readiness for data files in '{dir_path}'.\n\n"
        f"Evaluate based on:\n"
        f"1. Type consistency across columns\n"
        f"2. Null ratios and data completeness\n"
        f"3. Key candidate coverage (do all tables have identifiable PKs?)\n"
        f"4. Relationship coverage (are FK relationships detected?)\n"
        f"5. Encoding and structural issues\n\n"
        f"Provide a readiness score (High / Medium / Low) with justification.\n\n"
        f"Quality summaries:\n{json.dumps(profiles_summary, indent=2)}\n\n"
        f"Relationships:\n{json.dumps(rels, indent=2)}"
    )


@mcp.prompt()
async def quality_report(table_name: str) -> str:
    """Generate a detailed quality report for a table."""
    profile = _profile_cache.get(table_name, {})

    column_flags = []
    for col in profile.get("columns", []):
        if col.get("quality_flags"):
            column_flags.append({
                "column": col["name"],
                "flags": col["quality_flags"],
                "null_count": col.get("null_count", 0),
                "inferred_type": col.get("inferred_type"),
            })

    return (
        f"Generate a detailed quality report for table '{table_name}'.\n\n"
        f"For each quality issue found:\n"
        f"1. Describe the issue\n"
        f"2. Identify affected columns\n"
        f"3. Assess severity (Critical / Warning / Info)\n"
        f"4. Suggest remediation steps\n\n"
        f"Quality summary:\n{json.dumps(profile.get('quality_summary', {}), indent=2)}\n\n"
        f"Structural issues:\n{json.dumps(profile.get('structural_issues', []), indent=2)}\n\n"
        f"Column-level flags:\n{json.dumps(column_flags, indent=2)}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    """CLI entry point for the MCP server."""
    configure_logging()

    parser = argparse.ArgumentParser(description="File Profiler MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default=DEFAULT_TRANSPORT,
        help=f"Transport protocol (default: {DEFAULT_TRANSPORT})",
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    # Ensure output directories exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Clean up expired uploads from previous runs
    cleanup_expired_uploads()

    # Host and port are set on the FastMCP instance (used by sse/http transports)
    mcp.settings.host = args.host
    mcp.settings.port = args.port

    log.info(
        "Starting File Profiler MCP server (transport=%s, host=%s, port=%d)",
        args.transport, args.host, args.port,
    )

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
