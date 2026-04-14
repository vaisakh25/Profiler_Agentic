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

import asyncio
import argparse
import json
import logging
import math
import os
import sys
import tempfile
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import Any

# Set event loop policy early before any other imports create a loop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from mcp.server.fastmcp import Context

from file_profiler.config.env import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_TRANSPORT,
    OUTPUT_DIR,
)
from file_profiler.main import (
    profile_file as _pipeline_profile_file,
    profile_database as _pipeline_profile_database,
    profile_directory as _pipeline_profile_directory,
    analyze_relationships as _pipeline_analyze,
    _SCANNABLE_EXTENSIONS,
    _DB_EXTENSIONS,
)
from file_profiler.intake.validator import validate
from file_profiler.classification.classifier import classify
from file_profiler.output.profile_writer import serialise, compute_quality_summary
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport
from file_profiler.observability.langsmith import (
    compact_text_output,
    safe_name,
    trace_context,
    traceable,
)
from file_profiler.utils.file_resolver import resolve_path, save_upload, cleanup_expired_uploads
from file_profiler.utils.logging_setup import configure_logging
from file_profiler.utils.mcp_compat import (
    configure_fastmcp_network,
    create_fastmcp_with_fallback,
    patch_host_validation_permissive,
)

log = logging.getLogger(__name__)


def _trace_local_enrich_inputs(inputs: dict) -> dict:
    return {
        "dir_path": safe_name(inputs.get("dir_path"), kind="path"),
        "provider": inputs.get("provider"),
        "model": inputs.get("model"),
        "incremental": inputs.get("incremental"),
    }


def _resolve_writable_output_dir() -> Path:
    """Resolve a writable output directory with safe fallbacks."""
    candidates = (
        Path(OUTPUT_DIR),
        Path.cwd() / ".profiler_output",
        Path(tempfile.gettempdir()) / "file_profiler_output",
    )
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue
    raise OSError("No writable output directory available")

# ---------------------------------------------------------------------------
# Server instance
# ---------------------------------------------------------------------------

_INSTRUCTIONS = (
    "Agentic Data Profiler — profile CSV, Parquet, and other tabular "
    "data files.  Detects schemas, types, quality issues, and cross-table "
    "foreign key relationships."
)

# Patch host validation before FastMCP instantiation so constructor-time
# references capture permissive validators in container deployments.
patch_host_validation_permissive(logger=log)

mcp = create_fastmcp_with_fallback(
    name="file-profiler",
    instructions=_INSTRUCTIONS,
    host=DEFAULT_HOST,
    port=DEFAULT_PORT,
    logger=log,
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


def _cache_profile(profile: FileProfile) -> dict:
    """Serialise a FileProfile, store in cache, and return the dict.

    Consolidates the repeated _to_dict → _profile_cache → append pattern
    that was duplicated across profile_file, profile_directory, and
    _get_or_profile_directory.
    """
    d = _to_dict(profile)
    _profile_cache[profile.table_name] = d
    return d


def _compute_fingerprints(profiles: list) -> dict[str, str]:
    """Build a table_name → fingerprint mapping from a list of FileProfiles.

    Consolidates the identical comprehension that was duplicated in
    enrich_relationships and compare_profiles.
    """
    from file_profiler.agent.vector_store import _table_fingerprint
    return {
        p.table_name: _table_fingerprint(p.table_name, p.row_count, len(p.columns))
        for p in profiles
    }


def _compute_file_fingerprints(directory: Path) -> dict[str, str]:
    """Build lightweight file-level fingerprints using file stat() — no profiling.

    Returns a mapping of file_stem → hash(size, mtime).
    Used by check_enrichment_status to detect changes without profiling.
    """
    import hashlib
    fps: dict[str, str] = {}
    for f in sorted(directory.iterdir()):
        if f.is_file() and f.suffix.lower() in (_SCANNABLE_EXTENSIONS | _DB_EXTENSIONS):
            st = f.stat()
            fp = hashlib.md5(f"{f.stem}:{st.st_size}:{st.st_mtime}".encode()).hexdigest()[:12]
            fps[f.stem] = fp
    return fps


def _load_relationship_data() -> dict | None:
    """Load relationship data from cache or disk.

    Consolidates the identical fallback pattern used in
    visualize_profile and get_table_relationships.
    """
    if _relationship_cache:
        return _relationship_cache
    rel_path = OUTPUT_DIR / "relationships.json"
    if rel_path.exists():
        try:
            return json.loads(rel_path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


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


# ---------------------------------------------------------------------------
# Helper: profile directory with cache reuse
# ---------------------------------------------------------------------------

_dir_profile_cache: dict[str, list] = {}  # resolved_dir_path → [FileProfile, ...]


def _resolve_dir(path: Path) -> Path:
    """Ensure path is a directory; if it's a file, return its parent.

    Tools like enrich_relationships and detect_relationships require a
    directory path, but users (and the agent) often pass a file path
    after profiling a single file.  This helper normalises gracefully.
    """
    if path.is_file():
        log.debug("Received file path %s — using parent directory %s", path, path.parent)
        return path.parent
    return path


def _seed_dir_cache(resolved_dir: Path, profiles: list) -> None:
    """Seed the directory cache so subsequent _get_or_profile_directory calls
    for the same directory skip re-profiling.

    Called by profile_file after successfully profiling a single file, so
    that enrich_relationships (which needs a directory) can pick up the
    already-profiled results without re-running the pipeline.

    If the directory cache already exists for this path, the new profiles
    are merged in (replacing any with the same table_name).
    """
    cache_key = str(resolved_dir)
    existing = _dir_profile_cache.get(cache_key, [])
    if existing:
        # Merge: replace existing profiles by table_name, add new ones
        by_name = {r.table_name: r for r in existing}
        for p in profiles:
            by_name[p.table_name] = p
        _dir_profile_cache[cache_key] = list(by_name.values())
    else:
        _dir_profile_cache[cache_key] = list(profiles)
    log.debug("Directory cache seeded: %s (%d profiles)",
              cache_key, len(_dir_profile_cache[cache_key]))


def _get_or_profile_directory(resolved: Path) -> list:
    """Profile directory via pipeline, reusing cached results if available.

    Caches the full list of FileProfile objects keyed by directory path.
    Subsequent calls for the same directory skip the profiling pipeline entirely.

    When the cache was seeded by profile_file (partial), checks whether all
    scannable files in the directory are covered.  If not, runs the full
    directory scan but reuses already-profiled FileProfile objects from
    the cache to avoid redundant work.

    Returns list of FileProfile objects.
    """
    cache_key = str(resolved)

    if cache_key in _dir_profile_cache:
        cached = _dir_profile_cache[cache_key]

        # Verify cache covers all files in the directory.  profile_file
        # seeds the cache with only the single file it profiled — if the
        # directory contains more files, we need to profile the rest.
        all_files = sorted(
            f for f in resolved.iterdir()
            if f.is_file() and f.suffix.lower() in (_SCANNABLE_EXTENSIONS | _DB_EXTENSIONS)
        )

        # Track which source files have been profiled.  For regular files
        # the table_name equals the file stem.  For database files (.duckdb,
        # .db) file_path points to the source DB file — multiple tables
        # share the same file_path.
        cached_source_stems: set[str] = set()
        for r in cached:
            cached_source_stems.add(r.table_name)
            if r.file_path:
                cached_source_stems.add(Path(r.file_path).stem)

        uncovered = [f for f in all_files if f.stem not in cached_source_stems]

        if not uncovered:
            log.debug("Directory cache hit (complete): %s (%d profiles)",
                      cache_key, len(cached))
            return cached

        # Partial cache — profile only the uncovered files
        log.debug("Directory cache partial: %s — %d cached, %d uncovered",
                  cache_key, len(cached), len(uncovered))

        from file_profiler.main import profile_file as _pf, profile_database as _pd
        new_results: list = []
        for f in uncovered:
            try:
                if f.suffix.lower() in _DB_EXTENSIONS:
                    new_results.extend(_pd(f, output_dir=OUTPUT_DIR))
                else:
                    new_results.append(_pf(f, output_dir=OUTPUT_DIR))
            except Exception as exc:
                log.error("Failed to profile %s: %s", f.name, exc)

        for r in new_results:
            _cache_profile(r)

        all_results = cached + new_results
        _dir_profile_cache[cache_key] = all_results
        return all_results

    results = _pipeline_profile_directory(
        resolved, output_dir=OUTPUT_DIR, parallel=True,
    )
    for r in results:
        _cache_profile(r)

    _dir_profile_cache[cache_key] = results
    return results


# ═══════════════════════════════════════════════════════════════════════════
# TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def profile_file(file_path: str, ctx: Context) -> "dict | list[dict]":
    """
    Profile a single data file through the full 11-layer pipeline.

    For regular files (CSV, Parquet, JSON, Excel): returns a single FileProfile.
    For database files (.duckdb, .db, .sqlite): returns a list of FileProfile
    dicts, one per table inside the database.

    Runs: intake → classification → size strategy → format engine →
    standardization → column profiling → type inference → quality checks.

    Args:
        file_path: Path to the file (inside mounted volume or upload dir).

    Returns:
        Complete FileProfile (or list of FileProfiles for database files)
        with columns, types, quality flags, and statistics.
    """
    # Auto-detect remote URIs — tell the user to use the connector server
    from file_profiler.connectors.uri_parser import is_remote_uri
    if is_remote_uri(file_path):
        return {
            "error": (
                "Remote URIs should be profiled using the profile_remote_source "
                "tool on the data-connector server.  Use that tool instead."
            ),
            "uri": file_path,
        }

    resolved = resolve_path(file_path)

    # Database files contain multiple tables — use profile_database
    if resolved.suffix.lower() in _DB_EXTENSIONS:
        await ctx.report_progress(0, 3, "Detecting database tables")

        db_results = await asyncio.to_thread(
            _pipeline_profile_database, resolved, output_dir=OUTPUT_DIR,
        )

        await ctx.report_progress(2, 3, "Serialising results")
        profiles = [_cache_profile(r) for r in db_results]

        # Seed directory cache so enrich_relationships can skip re-profiling
        _seed_dir_cache(resolved.parent, db_results)

        await ctx.report_progress(3, 3, f"Complete — {len(profiles)} tables profiled")
        log.info("Database profiled: %s (%d tables)", resolved.name, len(profiles))
        return profiles

    await ctx.report_progress(0, 3, "Starting profiling pipeline")

    # Bridge sync pipeline progress → async MCP progress
    async def _report(step: int, total: int, msg: str) -> None:
        await ctx.report_progress(step, total, msg)

    def _sync_progress(step: int, total: int, msg: str) -> None:
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
    profile_dict = _cache_profile(result)

    # Seed directory cache so enrich_relationships can skip re-profiling
    _seed_dir_cache(resolved.parent, [result])

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
    resolved = _resolve_dir(resolve_path(dir_path))

    if ctx:
        await ctx.report_progress(0, 2, "Scanning directory")

    results = _get_or_profile_directory(resolved)

    if ctx:
        await ctx.report_progress(1, 2, "Serialising results")

    # _get_or_profile_directory already populated _profile_cache via
    # _cache_profile — just pull the serialised dicts from the cache.
    profiles = [_profile_cache[r.table_name] for r in results]

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

    Produces intermediate relationship signals saved as structured JSON.
    This is NOT the final output — run enrich_relationships to produce
    the final ER diagram and join recommendations via LLM analysis.

    Args:
        dir_path: Path to directory with data files.
        confidence_threshold: Minimum confidence to include (default 0.50).

    Returns:
        Intermediate RelationshipReport with FK candidates sorted by confidence.
        Use enrich_relationships for the final LLM-powered analysis.
    """
    global _relationship_cache

    resolved = _resolve_dir(resolve_path(dir_path))

    if ctx:
        await ctx.report_progress(0, 3, "Profiling directory")

    results = _get_or_profile_directory(resolved)

    if ctx:
        await ctx.report_progress(1, 3, "Detecting relationships")

    # Run synchronous relationship detection off the event loop
    report = await asyncio.to_thread(
        _pipeline_analyze,
        results,
        output_path=OUTPUT_DIR / "relationships.json",
    )

    if ctx:
        await ctx.report_progress(2, 3, "Serialising report")

    result = _report_to_dict(report, min_confidence=confidence_threshold)
    result["status"] = "intermediate"
    result["message"] = (
        "Deterministic relationship signals saved. "
        "Run enrich_relationships to produce the final ER diagram, "
        "join recommendations, and key mapping via LLM analysis."
    )
    _relationship_cache = result

    if ctx:
        await ctx.report_progress(3, 3, "Complete")

    log.info("Relationships detected: %d candidates (intermediate)",
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
    Enrich detected relationships using a scalable map-reduce LLM pipeline
    with unified column-affinity-based clustering and relationship discovery.

    Processes files in internal batches to keep the SSE connection alive
    via progress updates.  The agent sees a single tool call.

    Pipeline phases:

    1. **Profile** — Profile all files (reuses cache).
    2. **Detect** — Deterministic FK detection.
    3. **MAP + EMBED** (batched) — For each batch of tables:
       summarise via LLM, write descriptions back to JSONs, embed in
       ChromaDB with enriched column signals (sample values, cardinality).
       Progress is reported per table.
    4. **DISCOVER + CLUSTER** — Build a table-to-table affinity matrix from
       column embedding similarities.  Tables sharing many similar columns
       cluster together.  FK candidates fall out of the same computation.
    5. **REDUCE** — Cross-table LLM synthesis (uses a stronger model
       when REDUCE_LLM_PROVIDER/MODEL are configured).  For large datasets,
       runs per-cluster REDUCE + META-REDUCE across clusters.

    Args:
        dir_path:     Path to directory with data files.
        provider:     LLM provider — "google" (default), "groq", "openai", or "anthropic".
        model:        Model name override (default: provider's default model).
        incremental:  If True, reuse cached summaries for unchanged tables.

    Returns:
        Dict with enrichment analysis, column_relationships_discovered count,
        enriched_profiles_path, enriched_er_diagram_path, and metadata.
        On failure: Dict with error=True, error_type, error_message, traceback, and hint.
    """
    try:
        return await _enrich_relationships_impl(
            dir_path, provider, model, incremental, ctx,
        )
    except Exception as exc:
        tb = traceback.format_exc()
        log.error("enrich_relationships failed:\n%s", tb)
        # Clean up progress file so UI doesn't get stuck
        try:
            from file_profiler.agent.enrichment_progress import clear_progress as _cp
            _cp(OUTPUT_DIR)
        except Exception:
            pass
        return {
            "error": True,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": tb,
            "hint": (
                "If the error is related to stale vector store data from a "
                "previous run, try calling reset_vector_store() first, then "
                "retry enrich_relationships."
            ),
        }


@traceable(
    name="mcp.enrich_relationships",
    run_type="chain",
    process_inputs=_trace_local_enrich_inputs,
    process_outputs=compact_text_output,
)
async def _enrich_relationships_impl(
    dir_path: str,
    provider: str = "google",
    model: str | None = None,
    incremental: bool = True,
    ctx: Context = None,
) -> dict:
    """Inner implementation of enrich_relationships — see the tool docstring."""
    from file_profiler.agent.enrichment_mapreduce import (
        batch_enrich,
        discover_and_reduce_pipeline,
    )
    from file_profiler.config.env import BATCH_SIZE

    from file_profiler.agent.enrichment_progress import (
        check_enrichment_complete,
        clear_progress,
        write_manifest,
        write_progress,
    )

    with trace_context(
        surface="mcp",
        flow="enrichment",
        metadata={
            "dir_path": safe_name(dir_path, kind="path"),
            "provider": provider,
            "model": model,
            "incremental": incremental,
        },
        tags=("dataset:local", f"provider:{provider}"),
    ):
        return await _enrich_relationships_impl_traced(
            dir_path, provider, model, incremental, ctx
        )


async def _enrich_relationships_impl_traced(
    dir_path: str,
    provider: str = "google",
    model: str | None = None,
    incremental: bool = True,
    ctx: Context = None,
) -> dict:
    """Implementation body for traced local enrichment."""
    from file_profiler.agent.enrichment_mapreduce import (
        batch_enrich,
        discover_and_reduce_pipeline,
    )
    from file_profiler.config.env import BATCH_SIZE

    from file_profiler.agent.enrichment_progress import (
        check_enrichment_complete,
        clear_progress,
        write_manifest,
        write_progress,
    )

    resolved = _resolve_dir(resolve_path(dir_path))

    # --- Phase 0: Profile (single call, reused for fingerprint check) -------
    results = _get_or_profile_directory(resolved)
    n_tables = len(results)
    current_fingerprints = _compute_fingerprints(results)

    # --- Early return: check if previous enrichment is still valid ----------
    completion = check_enrichment_complete(OUTPUT_DIR, dir_path, current_fingerprints)

    if completion["status"] == "complete":
        log.info("Enrichment already complete for %s — returning cached results", dir_path)
        cached = completion["cached_result"]
        cached["from_cache"] = True
        cached["message"] = (
            "Enrichment was already completed and data has not changed. "
            "Returning cached results. Use compare_profiles to see details, "
            "or query_knowledge_base for follow-up questions."
        )
        if ctx:
            await ctx.report_progress(100, 100, "Already complete — cached")
        return cached

    # Progress helper: write to file (for web UI) + MCP ctx (for SSE keepalive).
    # Step indices match PIPELINE_STEPS["enrich_relationships"] in web_server.py.
    # Step-to-percent mapping: each step covers a proportional range of 0-100.
    _STEP_PCT = {
        0: 0,    # Profiling tables
        1: 8,    # Detecting relationships
        2: 12,   # MAP: Summarizing tables & columns
        3: 60,   # APPLY: Writing descriptions
        4: 65,   # EMBED: Storing in vector DB
        5: 72,   # COLUMN CLUSTER: DBSCAN grouping
        6: 75,   # DERIVE: PK/FK from clusters
        7: 78,   # TABLE CLUSTER: Affinity grouping
        8: 95,   # REDUCE: LLM synthesis
        9: 99,   # Generating enriched ER diagram
    }

    # Pre-compute per-table row/column counts for live stats reporting
    _table_rows: dict[str, int] = {}
    _table_cols: dict[str, int] = {}

    async def _report(step: int, name: str, detail: str = "",
                      stats: dict | None = None):
        pct = _STEP_PCT.get(step, 0)
        write_progress(OUTPUT_DIR, step, name, detail, stats=stats)
        if ctx:
            await ctx.report_progress(pct, 100, f"{name}: {detail}" if detail else name)

    # Build row/column lookup for live stats
    total_rows = 0
    total_columns = 0
    for r in results:
        _table_rows[r.table_name] = r.row_count
        _table_cols[r.table_name] = len(r.columns)
        total_rows += r.row_count
        total_columns += len(r.columns)

    # Build compact preview data for each table so the web UI can render
    # table cards during the profiling phase of enrichment.
    _table_previews = []
    for r in results:
        cols = []
        for c in r.columns[:12]:
            cols.append({
                "name": c.name,
                "type": c.inferred_type.value if hasattr(c.inferred_type, 'value') else str(c.inferred_type),
                "null_pct": round((c.null_count / r.row_count * 100) if r.row_count > 0 else 0, 1),
                "distinct": c.distinct_count,
            })
        qs = r.quality_summary
        _table_previews.append({
            "table_name": r.table_name,
            "row_count": r.row_count,
            "col_count": len(r.columns),
            "format": r.file_format.value if hasattr(r.file_format, 'value') else str(r.file_format),
            "columns": cols,
            "quality": {
                "issues": qs.columns_with_issues if qs else 0,
                "total": qs.columns_profiled if qs else 0,
            },
        })

    await _report(0, "Profiling tables", f"{n_tables} tables profiled",
                  stats={
                      "tables_done": n_tables,
                      "total_tables": n_tables,
                      "rows": total_rows,
                      "columns": total_columns,
                      "profiles_preview": _table_previews,
                  })

    # --- Phase 1: Detect relationships --------------------------------------
    await _report(1, "Detecting relationships")

    report = await asyncio.to_thread(
        _pipeline_analyze,
        results,
        output_path=OUTPUT_DIR / "relationships.json",
    )

    await _report(1, "Detecting relationships",
                  f"{len(report.candidates)} FK candidates",
                  stats={
                      "tables_done": n_tables,
                      "total_tables": n_tables,
                      "rows": total_rows,
                      "columns": total_columns,
                      "fk": len(report.candidates),
                  })

    # --- Phase 2+3+4: Batched MAP + APPLY + EMBED --------------------------
    await _report(2, "MAP: Summarizing tables & columns")

    total_batches = math.ceil(n_tables / BATCH_SIZE) if n_tables else 1
    # Reserve progress 12-60 for batched MAP (step 2)
    batch_progress_start = 12
    batch_progress_end = 60
    batch_progress_range = batch_progress_end - batch_progress_start

    # Track cumulative tables done across all batches for per-table progress
    cumulative_tables_done = 0
    cumulative_rows_done = 0
    cumulative_cols_done = 0

    total_summarized = 0
    total_cached = 0
    all_column_descriptions: dict = {}

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = start + BATCH_SIZE
        batch_profiles = results[start:end]

        if not batch_profiles:
            break

        # Per-table progress callback — fires after each LLM call completes,
        # keeping the SSE connection alive during long MAP phases.
        async def _on_table_done(done_in_batch, total_in_batch, table_name):
            nonlocal cumulative_tables_done, cumulative_rows_done, cumulative_cols_done
            cumulative_tables_done += 1
            cumulative_rows_done += _table_rows.get(table_name, 0)
            cumulative_cols_done += _table_cols.get(table_name, 0)
            table_pct = batch_progress_start + int(
                cumulative_tables_done / n_tables * batch_progress_range
            )
            detail = f"{table_name} ({cumulative_tables_done}/{n_tables})"
            write_progress(
                OUTPUT_DIR, 2, "MAP: Summarizing tables & columns", detail,
                stats={
                    "tables_done": cumulative_tables_done,
                    "total_tables": n_tables,
                    "rows": cumulative_rows_done,
                    "columns": cumulative_cols_done,
                    "fk": len(report.candidates),
                },
            )
            if ctx:
                await ctx.report_progress(
                    min(table_pct, batch_progress_end - 1), 100,
                    f"MAP: {detail}"
                )

        batch_result = await batch_enrich(
            profiles=batch_profiles,
            report=report,
            dir_path=dir_path,
            provider=provider,
            model=model,
            incremental=incremental,
            on_table_done=_on_table_done,
        )

        total_summarized += batch_result.get("tables_summarized", 0)
        total_cached += batch_result.get("tables_cached", 0)
        all_column_descriptions.update(batch_result.get("column_descriptions", {}))

    await _report(2, "MAP: Summarizing tables & columns",
                  f"{total_summarized} summarized, {total_cached} cached")

    # Deferred APPLY — re-write profile JSONs once after all batches
    await _report(3, "APPLY: Writing descriptions to profiles")

    if all_column_descriptions:
        from file_profiler.output.profile_writer import write as _write_profile
        for r in results:
            output_path = OUTPUT_DIR / f"{r.table_name}_profile.json"
            try:
                _write_profile(r, output_path)
            except Exception as exc:
                log.warning("Failed to write profile for %s: %s", r.table_name, exc)

        col_desc_path = OUTPUT_DIR / "column_descriptions.json"
        col_desc_path.write_text(
            json.dumps(all_column_descriptions, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    await _report(3, "APPLY: Writing descriptions to profiles",
                  f"{len(all_column_descriptions)} tables updated")
    await _report(4, "EMBED: Storing in vector DB", "embedded")

    # --- Phase 5-9: DISCOVER + CLUSTER + REDUCE -----------------------------
    # The on_phase_done callback bridges discover_and_reduce_pipeline()
    # progress into the progress file + MCP ctx.
    async def _on_phase(step: int, name: str, detail: str = ""):
        await _report(step, name, detail)

    # Skip REDUCE if no new tables were summarized (all fingerprint-cached)
    all_cached = total_summarized == 0 and total_cached > 0

    enrichment_result = await discover_and_reduce_pipeline(
        profiles=results,
        report=report,
        dir_path=dir_path,
        provider=provider,
        model=model,
        output_dir=OUTPUT_DIR,
        on_phase_done=_on_phase,
        skip_reduce=all_cached,
    )

    enrichment_result["tables_summarized"] = total_summarized
    enrichment_result["tables_cached"] = total_cached

    if ctx:
        col_clusters = enrichment_result.get("column_clusters_formed", 0)
        tbl_clusters = enrichment_result.get("table_clusters_formed", 1)
        derived = enrichment_result.get("cluster_derived_relationships", 0)
        discovered = enrichment_result.get("column_relationships_discovered", 0)
        msg = f"Enrichment complete — {discovered} column pairs discovered"
        if derived:
            msg += f", {derived} PK/FK derived from {col_clusters} column clusters"
        if tbl_clusters > 1:
            msg += f" ({tbl_clusters} table clusters)"
        await ctx.report_progress(100, 100, msg)

    # Clean up progress file — run is complete
    clear_progress(OUTPUT_DIR)

    # Write completion manifest for future cache hits.
    # Include file-level fingerprints so check_enrichment_status can detect
    # changes without profiling.
    file_fps = _compute_file_fingerprints(resolved)
    write_manifest(OUTPUT_DIR, dir_path, current_fingerprints, enrichment_result,
                   file_fingerprints=file_fps)

    log.info(
        "Enrichment complete: %d tables (%d summarized, %d cached), "
        "%d deterministic rels, %d vector-discovered column rels, "
        "%d cluster-derived rels, %d column clusters, %d table clusters, "
        "%d docs embedded",
        enrichment_result["tables_analyzed"],
        total_summarized,
        total_cached,
        enrichment_result["relationships_analyzed"],
        enrichment_result.get("column_relationships_discovered", 0),
        enrichment_result.get("cluster_derived_relationships", 0),
        enrichment_result.get("column_clusters_formed", 0),
        enrichment_result.get("table_clusters_formed", 1),
        enrichment_result["documents_embedded"],
    )
    return enrichment_result


@mcp.tool()
async def check_enrichment_status(dir_path: str, ctx: Context = None) -> dict:
    """
    Check whether enrichment has already been completed for a directory.

    This is a **fast, lightweight check** — it only reads the manifest file
    and compares file modification times.  **No profiling or LLM calls are
    made.**  It never triggers automatic profiling.

    **Call this BEFORE calling enrich_relationships** to avoid redundant work.

    Args:
        dir_path: Path to directory with data files.

    Returns:
        Dict with:
        - status: "complete" (cached & up-to-date), "stale" (data changed),
          or "none" (never enriched).
        - reason: Human-readable explanation.
        - changes: What changed (only if stale).
        - tables: Number of tables (only if complete).
        - enriched_at: Timestamp of last enrichment (only if complete).
    """
    from file_profiler.agent.enrichment_progress import check_enrichment_complete

    resolved = _resolve_dir(resolve_path(dir_path))

    if ctx:
        await ctx.report_progress(0, 2, "Scanning directory files")

    # Build lightweight file-level fingerprints from stat() — NO profiling.
    # Uses file stem → hash(size, mtime) so we can detect changes without
    # needing to actually parse the files.
    current_fps = _compute_file_fingerprints(resolved)

    if not current_fps:
        return {
            "status": "none",
            "reason": f"No supported data files found in {dir_path}.",
            "files_found": 0,
        }

    if ctx:
        await ctx.report_progress(1, 2, "Checking manifest")

    status = check_enrichment_complete(OUTPUT_DIR, dir_path, current_fps)

    # Don't return the full cached_result in the status check — just metadata
    if "cached_result" in status:
        cached = status.pop("cached_result")
        status["tables_analyzed"] = cached.get("tables_analyzed", 0)
        status["relationships_analyzed"] = cached.get("relationships_analyzed", 0)
        status["column_relationships_discovered"] = cached.get("column_relationships_discovered", 0)
        if cached.get("enriched_er_diagram_path"):
            status["enriched_er_diagram_path"] = cached["enriched_er_diagram_path"]
        if cached.get("enriched_profiles_path"):
            status["enriched_profiles_path"] = cached["enriched_profiles_path"]

    status["files_found"] = len(current_fps)

    if ctx:
        await ctx.report_progress(2, 2, f"Status: {status['status']}")

    return status


@mcp.tool()
async def reset_vector_store(ctx: Context = None) -> dict:
    """
    Clear the ChromaDB vector store and enrichment manifest/cache.

    Use this when enrichment fails due to stale data from a previous run
    (e.g., you previously enriched 194 tables but now want to enrich only 10).

    This resets:
    - ChromaDB collections (table summaries, column descriptions, cluster summaries)
    - Enrichment manifest (.enrichment_manifest.json)
    - Enrichment progress file (.enrichment_progress.json)
    - In-memory profile caches

    After calling this, retry enrich_relationships.
    """
    from file_profiler.config.env import VECTOR_STORE_DIR
    from file_profiler.agent.vector_store import clear_store

    cleaned = []

    # Clear ChromaDB collections using the API (avoids Windows file-lock issues)
    if VECTOR_STORE_DIR.exists():
        try:
            clear_store(VECTOR_STORE_DIR)
            cleaned.append(f"ChromaDB store: {VECTOR_STORE_DIR}")
        except Exception as exc:
            log.warning("Could not clear vector store: %s", exc)

    # Clear enrichment manifest and progress files
    from file_profiler.agent.enrichment_progress import (
        clear_progress,
        manifest_path,
    )
    clear_progress(OUTPUT_DIR)
    cleaned.append("Enrichment progress file")

    mp = manifest_path(OUTPUT_DIR)
    if mp.exists():
        try:
            mp.unlink()
            cleaned.append("Enrichment manifest")
        except Exception as exc:
            log.warning("Could not remove manifest: %s", exc)

    # Clear in-memory caches
    _profile_cache.clear()
    _dir_profile_cache.clear()
    cleaned.append("In-memory profile caches")

    log.info("Vector store reset: %s", ", ".join(cleaned))

    return {
        "status": "reset",
        "cleaned": cleaned,
        "message": (
            "Vector store and caches cleared. "
            "You can now re-run enrich_relationships."
        ),
    }


@mcp.tool()
async def visualize_profile(
    chart_type: str = "overview",
    table_name: str | None = None,
    column_name: str | None = None,
    theme: str = "dark",
    ctx: Context = None,
) -> dict:
    """
    Generate professional data visualization charts from profiled data.

    Creates publication-quality charts (matplotlib/seaborn) and returns
    image URLs that are rendered in the chat UI.  Charts are designed to
    provide data-scientist-grade insights with statistical annotations.

    **Available chart types:**

    Single-table charts (require table_name):
    - ``overview`` — comprehensive dashboard: quality scorecard + null distribution +
      type pie + cardinality + completeness + numeric summary + skewness +
      outlier analysis + correlation matrix (recommended first choice)
    - ``data_quality_scorecard`` — radar chart scoring 6 dimensions (completeness,
      consistency, type confidence, uniqueness, schema stability, outlier health)
    - ``null_distribution`` — bar chart of null percentage per column (color-coded)
    - ``type_distribution`` — donut chart of column type breakdown
    - ``cardinality`` — bar chart of distinct count / unique ratio per column
      (highlights primary key candidates in green)
    - ``completeness`` — stacked bar of filled vs null per column
    - ``numeric_summary`` — grouped bar comparing mean/median/std_dev across
      all numeric columns
    - ``skewness`` — bar chart of skewness for numeric columns
    - ``outlier_summary`` — bar chart of outlier counts/percentages using
      Tukey's IQR method (color-coded by severity)
    - ``correlation_matrix`` — Pearson correlation heatmap between numeric
      columns (diverging colormap, annotated)

    Column-level charts (require table_name AND column_name):
    - ``top_values`` — horizontal bar of most frequent values
    - ``string_lengths`` — bar chart of string length percentiles (P10/P50/P90)
    - ``distribution`` — percentile waterfall (P5/Q1/Median/Q3/P95) with mean
      line + statistics table (std, skewness, kurtosis, IQR, CV, outliers)
    - ``column_detail`` — multi-panel deep-dive: statistics card + top values
      bar + percentile view in one chart

    Multi-table charts (use table_name="*"):
    - ``overview_directory`` — row count comparison + quality heatmap
    - ``row_counts`` — bar chart comparing row counts across all tables
    - ``quality_heatmap`` — heatmap of quality flags across tables

    Relationship charts (use table_name="*"):
    - ``relationship_confidence`` — horizontal bar of FK confidence scores

    Args:
        chart_type:  Chart type (see above). Default "overview".
        table_name:  Table name from profiled data, or "*" for multi-table charts.
        column_name: Column name (required for distribution, column_detail,
                     top_values, string_lengths).
        theme:       "dark" (default) or "light".

    Returns:
        Dict with list of chart URLs, or error message if data not found.
    """
    try:
        from file_profiler.output.chart_generator import generate_chart, AVAILABLE_CHART_TYPES
    except ModuleNotFoundError as exc:
        log.warning("Visualization unavailable: %s", exc)
        return {
            "status": "unavailable",
            "error": "visualization_unavailable",
            "message": (
                "Visualization dependencies are unavailable in this runtime. "
                "Profiling and relationship analysis remain available."
            ),
        }

    if chart_type not in AVAILABLE_CHART_TYPES:
        return {
            "error": f"Unknown chart type: '{chart_type}'",
            "available_types": AVAILABLE_CHART_TYPES,
        }

    if ctx:
        await ctx.report_progress(0, 3, "Loading profile data")

    # Multi-table charts
    if table_name == "*" or chart_type in ("overview_directory", "row_counts",
                                            "quality_heatmap", "relationship_confidence"):
        profile_dicts = list(_profile_cache.values())
        if not profile_dicts:
            return {"error": "No profiled tables in cache. Run profile_directory first."}

        relationship_data = _load_relationship_data() if chart_type == "relationship_confidence" else _relationship_cache

        if ctx:
            await ctx.report_progress(1, 3, "Generating charts")

        charts = generate_chart(
            chart_type=chart_type,
            output_dir=OUTPUT_DIR,
            theme=theme,
            profile_dicts=profile_dicts,
            relationship_data=relationship_data,
        )

    else:
        # Single-table or column-level chart
        if not table_name:
            # Default to first table in cache
            if _profile_cache:
                table_name = next(iter(_profile_cache))
            else:
                return {"error": "No table_name specified and no profiled tables in cache."}

        profile_dict = _profile_cache.get(table_name)
        if profile_dict is None:
            available = list(_profile_cache.keys())
            return {
                "error": f"Table '{table_name}' not found in cache.",
                "available_tables": available[:20],
                "hint": "Run profile_directory or profile_file first.",
            }

        if ctx:
            await ctx.report_progress(1, 3, "Generating charts")

        charts = generate_chart(
            chart_type=chart_type,
            output_dir=OUTPUT_DIR,
            theme=theme,
            profile_dict=profile_dict,
            column_name=column_name,
        )

    if ctx:
        await ctx.report_progress(2, 3, f"Generated {len(charts)} chart(s)")

    if not charts:
        return {
            "message": f"No charts generated for type '{chart_type}'. "
                       "The data may not have the required fields.",
            "chart_type": chart_type,
        }

    result = {
        "charts": charts,
        "chart_count": len(charts),
        "table_name": table_name or "*",
        "message": f"Generated {len(charts)} chart(s). "
                   "Charts are displayed as images in the chat.",
    }

    if ctx:
        await ctx.report_progress(3, 3, "Complete")

    log.info("Generated %d chart(s): %s for %s",
             len(charts), chart_type, table_name or "*")
    return result


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
    resolved = _resolve_dir(resolve_path(dir_path))

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
            entry = {
                "file_name": fpath.name,
                "file_path": str(fpath),
                "size_bytes": intake.size_bytes,
                "detected_format": fmt.value,
                "encoding": intake.encoding,
                "compression": intake.compression,
            }

            # For database files, enumerate the tables inside
            if fmt.value in ("duckdb", "sqlite"):
                try:
                    from file_profiler.engines.db_engine import list_tables
                    tables = list_tables(fpath, fmt)
                    entry["tables"] = tables
                    entry["table_count"] = len(tables)
                except Exception as exc:
                    entry["tables_error"] = str(exc)

            file_infos.append(entry)
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

    # Check cache — try both the stem and any matching table_name key
    # (table_name set by the pipeline may differ from the file stem).
    table_name = resolved.stem
    cached = _profile_cache.get(table_name)
    if cached:
        return {
            "table_name": table_name,
            "quality_summary": cached.get("quality_summary", {}),
            "structural_issues": cached.get("structural_issues", []),
            "source": "cache",
        }

    if ctx:
        await ctx.report_progress(0, 2, "Profiling file for quality check")

    result = _pipeline_profile_file(resolved, output_dir=OUTPUT_DIR)
    profile_dict = _cache_profile(result)

    if ctx:
        await ctx.report_progress(2, 2, "Complete")

    return {
        "table_name": result.table_name,
        "quality_summary": profile_dict.get("quality_summary", {}),
        "structural_issues": profile_dict.get("structural_issues", []),
        "source": "fresh",
    }


@mcp.tool()
async def query_knowledge_base(
    question: str,
    top_k: int = 10,
    ctx: Context = None,
) -> dict:
    """
    Semantic search over the vector store of profiled tables and columns.

    Queries the ChromaDB vector store (populated by enrich_relationships)
    to find tables and columns relevant to a natural-language question.
    Useful for follow-up questions like "which tables have customer-related
    columns?" or "what columns look like timestamps?"

    Args:
        question: Natural-language query to search for.
        top_k: Number of results to return (default 10).

    Returns:
        Dict with matching table summaries and column descriptions.
    """
    from file_profiler.agent.vector_store import (
        get_or_create_column_store,
        get_or_create_store,
    )
    from file_profiler.config.env import VECTOR_STORE_DIR

    if ctx:
        await ctx.report_progress(0, 2, "Searching vector store")

    results: dict = {"question": question, "table_matches": [], "column_matches": []}

    # Search table summaries
    try:
        store = get_or_create_store(VECTOR_STORE_DIR)
        table_docs = store.similarity_search(question, k=min(top_k, 20))
        for doc in table_docs:
            meta = doc.metadata
            if meta.get("doc_type") == "table_summary":
                results["table_matches"].append({
                    "table_name": meta.get("table_name", ""),
                    "summary": doc.page_content[:500],
                    "row_count": meta.get("row_count"),
                    "column_count": meta.get("column_count"),
                })
    except Exception as exc:
        log.warning("query_knowledge_base: table search failed: %s", exc)

    # Search column descriptions
    try:
        col_store = get_or_create_column_store(VECTOR_STORE_DIR)
        col_docs = col_store.similarity_search(question, k=min(top_k, 30))
        for doc in col_docs:
            meta = doc.metadata
            if meta.get("doc_type") == "column_description":
                results["column_matches"].append({
                    "table_name": meta.get("table_name", ""),
                    "column_name": meta.get("column_name", ""),
                    "column_type": meta.get("column_type", ""),
                    "role": meta.get("role", ""),
                    "description": doc.page_content[:300],
                })
    except Exception as exc:
        log.warning("query_knowledge_base: column search failed: %s", exc)

    if ctx:
        await ctx.report_progress(2, 2, "Search complete")

    results["total_table_matches"] = len(results["table_matches"])
    results["total_column_matches"] = len(results["column_matches"])

    if not results["table_matches"] and not results["column_matches"]:
        results["message"] = (
            "No results found. Run enrich_relationships first to populate "
            "the vector store."
        )

    return results


@mcp.tool()
async def get_table_relationships(
    table_name: str,
    ctx: Context = None,
) -> dict:
    """
    Get all known relationships for a specific table.

    Returns both deterministic FK candidates and vector-discovered column
    similarities involving the given table.  Uses cached data — does not
    re-run profiling.

    Args:
        table_name: Name of the table to query (e.g. 'customers', 'orders').

    Returns:
        Dict with deterministic_relationships, vector_discovered_relationships,
        and related_tables list.
    """
    if ctx:
        await ctx.report_progress(0, 3, "Loading relationships")

    result: dict = {
        "table_name": table_name,
        "deterministic_relationships": [],
        "vector_discovered_relationships": [],
        "related_tables": [],
    }

    related: set[str] = set()

    det_rels = _load_relationship_data()

    if det_rels:
        for c in det_rels.get("candidates", []):
            fk_table = c.get("fk", {}).get("table_name", "")
            pk_table = c.get("pk", {}).get("table_name", "")
            if fk_table == table_name or pk_table == table_name:
                result["deterministic_relationships"].append(c)
                other = pk_table if fk_table == table_name else fk_table
                related.add(other)

    if ctx:
        await ctx.report_progress(1, 3, "Loading vector-discovered relationships")

    # Load vector-discovered relationships from disk
    discovered_path = OUTPUT_DIR / "discovered_column_relationships.json"
    if discovered_path.exists():
        try:
            discovered = json.loads(discovered_path.read_text(encoding="utf-8"))
            for d in discovered:
                src = d.get("source_table", "")
                tgt = d.get("target_table", "")
                if src == table_name or tgt == table_name:
                    result["vector_discovered_relationships"].append(d)
                    other = tgt if src == table_name else src
                    related.add(other)
        except Exception:
            pass

    if ctx:
        await ctx.report_progress(2, 3, "Building summary")

    result["related_tables"] = sorted(related)
    result["total_deterministic"] = len(result["deterministic_relationships"])
    result["total_vector_discovered"] = len(result["vector_discovered_relationships"])

    # Include table profile summary if cached
    if table_name in _profile_cache:
        cached = _profile_cache[table_name]
        result["table_summary"] = {
            "row_count": cached.get("row_count"),
            "column_count": len(cached.get("columns", [])),
            "format": cached.get("file_format"),
        }

    if ctx:
        await ctx.report_progress(3, 3, "Complete")

    return result


@mcp.tool()
async def compare_profiles(
    dir_path: str,
    ctx: Context = None,
) -> dict:
    """
    Detect schema drift by comparing current data against previously profiled state.

    Re-profiles the directory and compares against the stored fingerprints
    in the vector store.  Reports new tables, removed tables, and tables
    whose shape (row count or column count) has changed.

    Args:
        dir_path: Path to directory with data files.

    Returns:
        Dict with new_tables, removed_tables, changed_tables, and unchanged_tables.
    """
    from file_profiler.agent.vector_store import (
        get_or_create_store,
        get_stored_fingerprints,
    )
    from file_profiler.config.env import VECTOR_STORE_DIR

    resolved = _resolve_dir(resolve_path(dir_path))

    if ctx:
        await ctx.report_progress(0, 3, "Profiling current state")

    # Profile current state
    current_profiles = _get_or_profile_directory(resolved)
    current_fps = _compute_fingerprints(current_profiles)

    if ctx:
        await ctx.report_progress(1, 3, "Loading previous fingerprints")

    # Load previous fingerprints from vector store
    previous_fingerprints: dict[str, str] = {}
    try:
        store = get_or_create_store(VECTOR_STORE_DIR)
        previous_fingerprints = get_stored_fingerprints(store)
    except Exception:
        pass

    if ctx:
        await ctx.report_progress(2, 3, "Comparing states")

    # Compare
    current_tables = set(current_fps.keys())
    previous_tables = set(previous_fingerprints.keys())

    new_tables = sorted(current_tables - previous_tables)
    removed_tables = sorted(previous_tables - current_tables)
    changed_tables = []
    unchanged_tables = []

    for p in current_profiles:
        if p.table_name not in previous_fingerprints:
            continue  # new table
        if current_fps[p.table_name] != previous_fingerprints[p.table_name]:
            changed_tables.append({
                "table_name": p.table_name,
                "current_rows": p.row_count,
                "current_columns": len(p.columns),
                "fingerprint_changed": True,
            })
        else:
            unchanged_tables.append(p.table_name)

    if ctx:
        await ctx.report_progress(3, 3, "Comparison complete")

    has_previous = bool(previous_fingerprints)

    result = {
        "has_previous_state": has_previous,
        "current_tables": len(current_tables),
        "previous_tables": len(previous_tables),
        "new_tables": new_tables,
        "removed_tables": removed_tables,
        "changed_tables": changed_tables,
        "unchanged_tables": unchanged_tables,
        "summary": (
            f"{len(new_tables)} new, {len(removed_tables)} removed, "
            f"{len(changed_tables)} changed, {len(unchanged_tables)} unchanged"
        ),
    }

    if not has_previous:
        result["message"] = (
            "No previous profiling state found. Run enrich_relationships first "
            "to establish a baseline, then run compare_profiles to detect drift."
        )

    return result


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

def _graceful_shutdown(signum, frame) -> None:
    """Handle SIGTERM/SIGINT for clean shutdown."""
    import signal
    sig_name = signal.Signals(signum).name
    log.info("Received %s — shutting down gracefully", sig_name)

    # Clear caches to release memory
    _profile_cache.clear()
    _dir_profile_cache.clear()

    # Clean up expired uploads
    try:
        cleanup_expired_uploads()
    except Exception:
        pass

    log.info("Shutdown complete")
    raise SystemExit(0)


def main() -> None:
    """CLI entry point for the MCP server."""
    import signal

    # Ensure output directories exist before logging setup to avoid
    # startup crashes when the log path parent is missing.
    resolved_output_dir = _resolve_writable_output_dir()

    # Keep module-level and env-module OUTPUT_DIR aligned for code paths
    # that reference constants imported at module import time.
    global OUTPUT_DIR
    OUTPUT_DIR = resolved_output_dir
    from file_profiler.config import env as _env
    _env.OUTPUT_DIR = resolved_output_dir
    os.environ["PROFILER_OUTPUT_DIR"] = str(resolved_output_dir)

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

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)

    # Clean up expired uploads from previous runs
    cleanup_expired_uploads()

    # Pre-warm embedding model in a background thread to avoid cold-start
    # latency on the first enrich_relationships call
    import threading
    def _prewarm():
        try:
            from file_profiler.agent.vector_store import warm_embeddings
            warm_embeddings()
        except Exception as exc:
            log.warning("Embedding pre-warm failed: %s", exc)
    threading.Thread(target=_prewarm, daemon=True).start()

    # Keep FastMCP network settings aligned with the CLI host/port and ensure
    # non-loopback deployments do not inherit localhost-only host validation.
    configure_fastmcp_network(mcp, host=args.host, port=args.port, logger=log)

    # Re-apply legacy hook patches for MCP versions that still expose them.
    patch_host_validation_permissive(logger=log)

    log.info(
        "Starting File Profiler MCP server (transport=%s, host=%s, port=%d)",
        args.transport, args.host, args.port,
    )

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
