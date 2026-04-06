"""
MCP Server for Remote Data Connectors.

Exposes the full profiling pipeline for remote data sources
(PostgreSQL, Snowflake, S3, ADLS Gen2, GCS).

The connector server materialises remote profiles to a staging directory
(OUTPUT_DIR/connectors/{connection_id}/) and reuses the same pipeline
functions as the file-profiler server.  This avoids code duplication
while keeping the two servers independent.

Transports:
  stdio -- for local use
  sse   -- for containerised / remote deployment

Usage:
  python -m file_profiler.connectors --transport sse --port 8081
"""

from __future__ import annotations

import asyncio
import argparse
import json
import logging
import math
import sys
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import Any

# Set event loop policy early before any other imports create a loop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from mcp.server.fastmcp import FastMCP, Context

from file_profiler.config.env import (
    CONNECTOR_MCP_PORT,
    DEFAULT_HOST,
    OUTPUT_DIR,
)
from file_profiler.output.profile_writer import serialise, compute_quality_summary
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport
from file_profiler.utils.logging_setup import configure_logging

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Server instance
# ---------------------------------------------------------------------------

try:
    mcp = FastMCP(
        name="data-connector",
        instructions=(
            "Remote Data Connector -- manage connections and run the full profiling "
            "pipeline (profile, detect relationships, LLM enrichment, visualisation, "
            "knowledge-base queries) on PostgreSQL, Snowflake, S3, ADLS Gen2, and GCS."
        ),
        # Allow Docker container hostnames for internal communication
        allowed_origins=["*"],
    )
except TypeError:
    # Backward compatibility for FastMCP versions that do not support allowed_origins.
    mcp = FastMCP(
        name="data-connector",
        instructions=(
            "Remote Data Connector -- manage connections and run the full profiling "
            "pipeline (profile, detect relationships, LLM enrichment, visualisation, "
            "knowledge-base queries) on PostgreSQL, Snowflake, S3, ADLS Gen2, and GCS."
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

# Staging directory cache: connection_id -> [FileProfile, ...]
_staging_cache: dict[str, list] = {}


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request) -> "JSONResponse":
    from starlette.responses import JSONResponse
    return JSONResponse({
        "status": "ok",
        "server": "data-connector",
        "cached_profiles": len(_profile_cache),
        "staged_connections": len(_staging_cache),
    })


# ---------------------------------------------------------------------------
# Serialisation helpers
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
    """Serialise a FileProfile, store in cache, and return the dict."""
    d = _to_dict(profile)
    _profile_cache[profile.table_name] = d
    return d


def _compute_fingerprints(profiles: list) -> dict[str, str]:
    """Build a table_name -> fingerprint mapping from a list of FileProfiles."""
    from file_profiler.agent.vector_store import _table_fingerprint
    return {
        p.table_name: _table_fingerprint(p.table_name, p.row_count, len(p.columns))
        for p in profiles
    }


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


def _load_relationship_data(staging_dir: Path) -> dict | None:
    """Load relationship data from cache or disk."""
    if _relationship_cache:
        return _relationship_cache
    rel_path = staging_dir / "relationships.json"
    if rel_path.exists():
        try:
            return json.loads(rel_path.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# Staging directory helpers
# ---------------------------------------------------------------------------

def _staging_dir(connection_id: str) -> Path:
    """Return the staging directory for a connection, creating it if needed."""
    d = OUTPUT_DIR / "connectors" / connection_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _materialize_profiles(connection_id: str, profiles: list[FileProfile]) -> Path:
    """Write FileProfile JSON files to the staging directory.

    This allows the existing pipeline functions (detect_relationships,
    enrich_relationships) to work on remote data the same way they work
    on local files.
    """
    from file_profiler.output.profile_writer import write as _write_profile

    staging = _staging_dir(connection_id)

    for fp in profiles:
        out_path = staging / f"{fp.table_name}_profile.json"
        try:
            _write_profile(fp, out_path)
        except Exception as exc:
            log.warning("Failed to materialise profile for %s: %s", fp.table_name, exc)

    # Cache for later pipeline steps
    _staging_cache[connection_id] = profiles
    return staging


def _get_staged_profiles(connection_id: str) -> list[FileProfile]:
    """Return staged FileProfile objects for a connection.

    Checks in-memory cache first.  If the cache is cold (e.g. after a
    server restart), the caller must re-run profile_remote_source —
    we don't attempt to reconstruct FileProfile objects from JSON since
    the pipeline needs the full in-memory objects.
    """
    if connection_id in _staging_cache:
        return _staging_cache[connection_id]

    # Check if staging dir has profile JSONs (indicates prior run)
    staging = _staging_dir(connection_id)
    has_profiles = any(staging.glob("*_profile.json"))

    if has_profiles:
        log.info(
            "Staged profiles exist on disk for '%s' but not in memory. "
            "Re-run profile_remote_source to reload them.",
            connection_id,
        )

    return []


def _resolve_connection_id(connection_id: str) -> str:
    """Validate and return a connection ID, defaulting if needed."""
    cid = connection_id.strip()
    if not cid:
        # Try to find the most recently used connection
        if _staging_cache:
            cid = next(iter(_staging_cache))
            log.debug("No connection_id given, defaulting to: %s", cid)
        else:
            raise ValueError(
                "No connection_id specified and no staged profiles found. "
                "Run profile_remote_source first."
            )
    return cid


# ═══════════════════════════════════════════════════════════════════════════
# CONNECTION MANAGEMENT TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def connect_source(
    connection_id: str,
    scheme: str,
    credentials: dict,
    display_name: str = "",
    test: bool = True,
    ctx: Context = None,
) -> dict:
    """
    Register credentials for a remote data source.

    Stores credentials in memory (encrypted on disk if PROFILER_SECRET_KEY
    is set).  Credentials never pass through the LLM.

    Args:
        connection_id: Unique name for this connection (e.g. "prod-s3", "analytics-pg").
        scheme: Source type -- one of: s3, abfss, gs, snowflake, postgresql.
        credentials: Auth credentials (scheme-specific).
            S3: {aws_access_key_id, aws_secret_access_key, region}
                or {profile_name} for AWS CLI profile.
            ADLS: {connection_string} or {tenant_id, client_id, client_secret}.
            GCS: {service_account_json} (path or inline JSON) or {} for ADC.
            Snowflake: {account, user, password, warehouse, role}.
            PostgreSQL: {connection_string} or {host, port, user, password, dbname}.
        display_name: Human-readable label for UI display.
        test: Whether to test connectivity immediately (default True).

    Returns:
        Dict with connection_id, scheme, and optional test result.
    """
    from file_profiler.connectors.connection_manager import get_connection_manager

    mgr = get_connection_manager()
    info = mgr.register(connection_id, scheme, credentials, display_name)

    result = {
        "connection_id": info.connection_id,
        "scheme": info.scheme,
        "display_name": info.display_name,
        "registered": True,
    }

    if test:
        test_result = mgr.test(connection_id)
        result["test"] = {
            "success": test_result.success,
            "message": test_result.message,
            "latency_ms": round(test_result.latency_ms, 1),
        }

    return result


@mcp.tool()
async def list_connections(ctx: Context = None) -> list:
    """
    List all registered remote connections with their status.

    Returns a list of connection summaries.  Credentials are never
    included in the response.

    Returns:
        List of dicts with connection_id, scheme, display_name,
        last_tested, and is_healthy.
    """
    from file_profiler.connectors.connection_manager import get_connection_manager

    mgr = get_connection_manager()
    connections = mgr.list_connections()

    return [
        {
            "connection_id": c.connection_id,
            "scheme": c.scheme,
            "display_name": c.display_name,
            "last_tested": c.last_tested,
            "is_healthy": c.is_healthy,
        }
        for c in connections
    ]


@mcp.tool()
async def test_connection(
    connection_id: str,
    ctx: Context = None,
) -> dict:
    """
    Test connectivity for a registered connection.

    Args:
        connection_id: Name of the connection to test.

    Returns:
        Dict with success, message, and latency_ms.
    """
    from file_profiler.connectors.connection_manager import get_connection_manager

    mgr = get_connection_manager()
    result = mgr.test(connection_id)
    return {
        "connection_id": connection_id,
        "success": result.success,
        "message": result.message,
        "latency_ms": round(result.latency_ms, 1),
    }


@mcp.tool()
async def remove_connection(
    connection_id: str,
    ctx: Context = None,
) -> dict:
    """
    Remove a registered connection and its stored credentials.

    Args:
        connection_id: Name of the connection to remove.

    Returns:
        Dict with connection_id and whether it was removed.
    """
    from file_profiler.connectors.connection_manager import get_connection_manager

    mgr = get_connection_manager()
    removed = mgr.remove(connection_id)
    return {
        "connection_id": connection_id,
        "removed": removed,
    }


# ═══════════════════════════════════════════════════════════════════════════
# DISCOVERY TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def list_tables(
    uri: str,
    connection_id: str = "",
    ctx: Context = None,
) -> list[dict]:
    """
    List tables or files at a remote data source without profiling.

    Connects to the source and enumerates available objects.  For databases,
    lists tables in the specified schema.  For object storage, lists files
    under the prefix.

    Args:
        uri: Remote URI (e.g. postgresql://host:5432/dbname/schema,
             s3://bucket/prefix/, snowflake://account/db/schema).
        connection_id: Name of a registered connection for credentials.

    Returns:
        List of dicts with name, uri, size_bytes (if available), file_format.
    """
    from file_profiler.connectors.uri_parser import parse_uri
    from file_profiler.connectors.registry import registry
    from file_profiler.connectors.connection_manager import get_connection_manager

    conn_id = connection_id.strip() or None
    descriptor = parse_uri(uri, connection_id=conn_id)
    connector = registry.get(descriptor.scheme)

    mgr = get_connection_manager()
    credentials = mgr.resolve_credentials(descriptor)

    objects = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: connector.list_objects(descriptor, credentials),
    )

    return [
        {
            "name": obj.name,
            "uri": obj.uri,
            "size_bytes": obj.size_bytes,
            "file_format": obj.file_format,
        }
        for obj in objects
    ]


@mcp.tool()
async def list_schemas(
    uri: str,
    connection_id: str = "",
    ctx: Context = None,
) -> list[str]:
    """
    List schemas in a remote database.

    Only supported for database connectors (PostgreSQL, Snowflake).
    System schemas (pg_catalog, information_schema, pg_toast) are excluded.

    Args:
        uri: Database URI (e.g. postgresql://host:5432/dbname,
             snowflake://account/database).
        connection_id: Name of a registered connection for credentials.

    Returns:
        List of schema names.
    """
    from file_profiler.connectors.uri_parser import parse_uri
    from file_profiler.connectors.registry import registry
    from file_profiler.connectors.connection_manager import get_connection_manager

    conn_id = connection_id.strip() or None
    descriptor = parse_uri(uri, connection_id=conn_id)
    connector = registry.get(descriptor.scheme)

    mgr = get_connection_manager()
    credentials = mgr.resolve_credentials(descriptor)

    schemas = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: connector.list_schemas(descriptor, credentials),
    )

    return schemas


# ═══════════════════════════════════════════════════════════════════════════
# PROFILING TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def profile_remote_source(
    uri: str,
    connection_id: str = "",
    table_filter: str = "",
    ctx: Context = None,
) -> "dict | list[dict]":
    """
    Profile a remote data source -- cloud storage files or database tables.

    Profiles are materialised to a staging directory so that the full
    pipeline (detect_relationships, enrich_relationships, visualize, etc.)
    can operate on the results.

    Supports:
        S3:         s3://bucket/path/file.parquet  or  s3://bucket/prefix/
        ADLS:       abfss://container@account.dfs.core.windows.net/path/
        GCS:        gs://bucket/prefix/
        PostgreSQL: postgresql://host:5432/dbname/schema
        Snowflake:  snowflake://account/database/schema

    Args:
        uri: Remote URI to profile.
        connection_id: Name of a registered connection (from connect_source).
                       Leave empty to use env vars or SDK defaults.
        table_filter: Comma-separated table names to profile (databases only).
                      Leave empty to profile all tables.

    Returns:
        Dict (single file/table) or list of dicts (directory/schema).
    """
    from file_profiler.main import profile_remote

    conn_id = connection_id.strip() or None
    tbl_filter = [t.strip() for t in table_filter.split(",") if t.strip()] or None

    if ctx:
        await ctx.report_progress(0, 3, "Profiling remote source")

    result = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: profile_remote(
            uri=uri,
            connection_id=conn_id,
            table_filter=tbl_filter,
            output_dir=str(OUTPUT_DIR),
        ),
    )

    if ctx:
        await ctx.report_progress(1, 3, "Materialising profiles to staging")

    # Normalise to list
    profiles = result if isinstance(result, list) else [result]

    # Determine a connection_id for staging — use provided or derive from URI
    staging_id = conn_id or uri.split("://")[0] + "-" + uri.split("/")[-1]
    staging_dir = _materialize_profiles(staging_id, profiles)

    if ctx:
        await ctx.report_progress(2, 3, "Caching results")

    serialised = [_cache_profile(fp) for fp in profiles]

    if ctx:
        n = len(profiles)
        await ctx.report_progress(3, 3, f"Complete -- {n} table(s) profiled")

    log.info("Remote source profiled: %s (%d tables) -> %s",
             uri, len(profiles), staging_dir)

    # Return single dict for single-table, list for multi-table
    if len(serialised) == 1 and not isinstance(result, list):
        return serialised[0]
    return serialised


# ═══════════════════════════════════════════════════════════════════════════
# RELATIONSHIP DETECTION TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def remote_detect_relationships(
    connection_id: str,
    confidence_threshold: float = 0.50,
    ctx: Context = None,
) -> dict:
    """
    Detect foreign key relationships across profiled remote tables.

    Uses staged profiles from a previous profile_remote_source call.
    Produces intermediate relationship signals.  Run enrich_relationships
    for the final LLM-powered analysis.

    Args:
        connection_id: Connection whose staged profiles to analyse.
        confidence_threshold: Minimum confidence to include (default 0.50).

    Returns:
        Intermediate RelationshipReport with FK candidates.
    """
    from file_profiler.main import analyze_relationships as _pipeline_analyze

    global _relationship_cache

    cid = _resolve_connection_id(connection_id)
    staging = _staging_dir(cid)
    profiles = _get_staged_profiles(cid)

    if not profiles:
        return {"error": f"No staged profiles for connection '{cid}'. Run profile_remote_source first."}

    if ctx:
        await ctx.report_progress(0, 3, f"Loading {len(profiles)} profiles")

    if ctx:
        await ctx.report_progress(1, 3, "Detecting relationships")

    report = await asyncio.to_thread(
        _pipeline_analyze,
        profiles,
        output_path=staging / "relationships.json",
    )

    if ctx:
        await ctx.report_progress(2, 3, "Serialising report")

    result = _report_to_dict(report, min_confidence=confidence_threshold)
    result["status"] = "intermediate"
    result["connection_id"] = cid
    result["message"] = (
        "Deterministic relationship signals saved. "
        "Run enrich_relationships to produce the final ER diagram, "
        "join recommendations, and key mapping via LLM analysis."
    )
    _relationship_cache = result

    if ctx:
        await ctx.report_progress(3, 3, "Complete")

    log.info("Relationships detected for %s: %d candidates (intermediate)",
             cid, len(result.get("candidates", [])))
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ENRICHMENT TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def remote_enrich_relationships(
    connection_id: str,
    provider: str = "google",
    model: str | None = None,
    incremental: bool = True,
    ctx: Context = None,
) -> dict:
    """
    Enrich detected relationships using a scalable map-reduce LLM pipeline
    with unified column-affinity-based clustering and relationship discovery.

    Uses staged profiles from profile_remote_source.  Runs the same
    pipeline as the file-profiler server.

    Pipeline phases:
    1. Profile (from staging cache)
    2. Detect -- Deterministic FK detection
    3. MAP + EMBED (batched) -- LLM summarise, embed in ChromaDB
    4. DISCOVER + CLUSTER -- Column affinity matrix, DBSCAN clustering
    5. REDUCE -- Cross-table LLM synthesis

    Args:
        connection_id: Connection whose staged profiles to enrich.
        provider:      LLM provider (default "google").
        model:         Model name override.
        incremental:   Reuse cached summaries for unchanged tables.

    Returns:
        Dict with enrichment analysis, column_relationships_discovered,
        enriched ER diagram path, and metadata.
    """
    try:
        return await _enrich_relationships_impl(
            connection_id, provider, model, incremental, ctx,
        )
    except Exception as exc:
        tb = traceback.format_exc()
        log.error("enrich_relationships failed:\n%s", tb)
        try:
            from file_profiler.agent.enrichment_progress import clear_progress as _cp
            cid = connection_id.strip() or next(iter(_staging_cache), "unknown")
            _cp(_staging_dir(cid))
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


async def _enrich_relationships_impl(
    connection_id: str,
    provider: str = "google",
    model: str | None = None,
    incremental: bool = True,
    ctx: Context = None,
) -> dict:
    """Inner implementation of enrich_relationships."""
    from file_profiler.agent.enrichment_mapreduce import (
        batch_enrich,
        discover_and_reduce_pipeline,
    )
    from file_profiler.config.env import BATCH_SIZE
    from file_profiler.main import analyze_relationships as _pipeline_analyze
    from file_profiler.agent.enrichment_progress import (
        check_enrichment_complete,
        clear_progress,
        write_manifest,
        write_progress,
    )

    cid = _resolve_connection_id(connection_id)
    staging = _staging_dir(cid)
    results = _get_staged_profiles(cid)

    if not results:
        return {"error": f"No staged profiles for connection '{cid}'. Run profile_remote_source first."}

    n_tables = len(results)
    current_fingerprints = _compute_fingerprints(results)
    dir_path = str(staging)

    # --- Early return: check if previous enrichment is still valid ----------
    completion = check_enrichment_complete(staging, dir_path, current_fingerprints)

    if completion["status"] == "complete":
        log.info("Enrichment already complete for %s -- returning cached", cid)
        cached = completion["cached_result"]
        cached["from_cache"] = True
        cached["connection_id"] = cid
        cached["message"] = (
            "Enrichment was already completed and data has not changed. "
            "Returning cached results. Use compare_profiles or "
            "query_knowledge_base for follow-up."
        )
        if ctx:
            await ctx.report_progress(100, 100, "Already complete -- cached")
        return cached

    # Progress helper
    _STEP_PCT = {
        0: 0, 1: 8, 2: 12, 3: 60, 4: 65, 5: 72, 6: 75, 7: 78, 8: 95, 9: 99,
    }

    _table_rows: dict[str, int] = {}
    _table_cols: dict[str, int] = {}

    async def _report(step: int, name: str, detail: str = "",
                      stats: dict | None = None):
        pct = _STEP_PCT.get(step, 0)
        write_progress(staging, step, name, detail, stats=stats)
        if ctx:
            await ctx.report_progress(pct, 100, f"{name}: {detail}" if detail else name)

    # Build row/column lookup
    total_rows = 0
    total_columns = 0
    for r in results:
        _table_rows[r.table_name] = r.row_count
        _table_cols[r.table_name] = len(r.columns)
        total_rows += r.row_count
        total_columns += len(r.columns)

    # Table previews for web UI
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

    # --- Phase 1: Detect relationships ----------------------------------------
    await _report(1, "Detecting relationships")

    report = await asyncio.to_thread(
        _pipeline_analyze,
        results,
        output_path=staging / "relationships.json",
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

    # --- Phase 2+3+4: Batched MAP + APPLY + EMBED ----------------------------
    await _report(2, "MAP: Summarizing tables & columns")

    total_batches = math.ceil(n_tables / BATCH_SIZE) if n_tables else 1
    batch_progress_start = 12
    batch_progress_end = 60
    batch_progress_range = batch_progress_end - batch_progress_start

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
                staging, 2, "MAP: Summarizing tables & columns", detail,
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

    # Deferred APPLY -- re-write profile JSONs once after all batches
    await _report(3, "APPLY: Writing descriptions to profiles")

    if all_column_descriptions:
        from file_profiler.output.profile_writer import write as _write_profile
        for r in results:
            output_path = staging / f"{r.table_name}_profile.json"
            try:
                _write_profile(r, output_path)
            except Exception as exc:
                log.warning("Failed to write profile for %s: %s", r.table_name, exc)

        col_desc_path = staging / "column_descriptions.json"
        col_desc_path.write_text(
            json.dumps(all_column_descriptions, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    await _report(3, "APPLY: Writing descriptions to profiles",
                  f"{len(all_column_descriptions)} tables updated")
    await _report(4, "EMBED: Storing in vector DB", "embedded")

    # --- Phase 5-9: DISCOVER + CLUSTER + REDUCE --------------------------------
    async def _on_phase(step: int, name: str, detail: str = ""):
        await _report(step, name, detail)

    all_cached = total_summarized == 0 and total_cached > 0

    enrichment_result = await discover_and_reduce_pipeline(
        profiles=results,
        report=report,
        dir_path=dir_path,
        provider=provider,
        model=model,
        on_phase_done=_on_phase,
        skip_reduce=all_cached,
    )

    enrichment_result["tables_summarized"] = total_summarized
    enrichment_result["tables_cached"] = total_cached
    enrichment_result["connection_id"] = cid

    if ctx:
        col_clusters = enrichment_result.get("column_clusters_formed", 0)
        tbl_clusters = enrichment_result.get("table_clusters_formed", 1)
        derived = enrichment_result.get("cluster_derived_relationships", 0)
        discovered = enrichment_result.get("column_relationships_discovered", 0)
        msg = f"Enrichment complete -- {discovered} column pairs discovered"
        if derived:
            msg += f", {derived} PK/FK derived from {col_clusters} column clusters"
        if tbl_clusters > 1:
            msg += f" ({tbl_clusters} table clusters)"
        await ctx.report_progress(100, 100, msg)

    # Clean up progress
    clear_progress(staging)

    # Write completion manifest
    write_manifest(staging, dir_path, current_fingerprints, enrichment_result)

    log.info(
        "Enrichment complete for %s: %d tables (%d summarized, %d cached), "
        "%d deterministic rels, %d vector-discovered, %d cluster-derived",
        cid,
        enrichment_result["tables_analyzed"],
        total_summarized,
        total_cached,
        enrichment_result["relationships_analyzed"],
        enrichment_result.get("column_relationships_discovered", 0),
        enrichment_result.get("cluster_derived_relationships", 0),
    )
    return enrichment_result


@mcp.tool()
async def remote_check_enrichment_status(
    connection_id: str,
    ctx: Context = None,
) -> dict:
    """
    Check whether enrichment has already been completed for a connection.

    Lightweight check -- reads the manifest and compares fingerprints.
    No profiling or LLM calls.  Call BEFORE enrich_relationships.

    Args:
        connection_id: Connection to check.

    Returns:
        Dict with status ("complete", "stale", or "none"), reason,
        and metadata.
    """
    from file_profiler.agent.enrichment_progress import check_enrichment_complete

    cid = _resolve_connection_id(connection_id)
    staging = _staging_dir(cid)
    profiles = _get_staged_profiles(cid)

    if not profiles:
        return {
            "status": "none",
            "reason": f"No staged profiles for connection '{cid}'. Run profile_remote_source first.",
            "connection_id": cid,
        }

    current_fps = _compute_fingerprints(profiles)

    if ctx:
        await ctx.report_progress(0, 2, "Checking manifest")

    status = check_enrichment_complete(staging, str(staging), current_fps)

    if "cached_result" in status:
        cached = status.pop("cached_result")
        status["tables_analyzed"] = cached.get("tables_analyzed", 0)
        status["relationships_analyzed"] = cached.get("relationships_analyzed", 0)
        status["column_relationships_discovered"] = cached.get("column_relationships_discovered", 0)

    status["connection_id"] = cid
    status["tables_staged"] = len(profiles)

    if ctx:
        await ctx.report_progress(2, 2, f"Status: {status['status']}")

    return status


@mcp.tool()
async def remote_reset_vector_store(
    connection_id: str = "",
    ctx: Context = None,
) -> dict:
    """
    Clear the ChromaDB vector store and enrichment manifest/cache for remote data.

    Use when enrichment fails due to stale data from a previous run.

    Args:
        connection_id: Connection to reset (empty = reset all).

    Returns:
        Dict with status and list of cleaned items.
    """
    from file_profiler.config.env import VECTOR_STORE_DIR
    from file_profiler.agent.vector_store import clear_store

    cleaned = []

    # Clear ChromaDB
    if VECTOR_STORE_DIR.exists():
        try:
            clear_store(VECTOR_STORE_DIR)
            cleaned.append(f"ChromaDB store: {VECTOR_STORE_DIR}")
        except Exception as exc:
            log.warning("Could not clear vector store: %s", exc)

    # Clear enrichment manifest/progress for specific or all connections
    from file_profiler.agent.enrichment_progress import (
        clear_progress,
        manifest_path,
    )

    if connection_id.strip():
        staging = _staging_dir(connection_id.strip())
        clear_progress(staging)
        mp = manifest_path(staging)
        if mp.exists():
            mp.unlink()
            cleaned.append(f"Manifest for {connection_id}")
        if connection_id.strip() in _staging_cache:
            del _staging_cache[connection_id.strip()]
    else:
        # Reset all connections
        for cid in list(_staging_cache.keys()):
            staging = _staging_dir(cid)
            clear_progress(staging)
            mp = manifest_path(staging)
            if mp.exists():
                mp.unlink()
        _staging_cache.clear()
        cleaned.append("All staging caches")

    # Clear in-memory caches
    _profile_cache.clear()
    cleaned.append("In-memory profile caches")

    log.info("Vector store reset: %s", ", ".join(cleaned))

    return {
        "status": "reset",
        "cleaned": cleaned,
        "message": "Vector store and caches cleared. Re-run enrich_relationships.",
    }


# ═══════════════════════════════════════════════════════════════════════════
# VISUALISATION TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def remote_visualize_profile(
    chart_type: str = "overview",
    table_name: str | None = None,
    column_name: str | None = None,
    connection_id: str = "",
    theme: str = "dark",
    ctx: Context = None,
) -> dict:
    """
    Generate professional data visualisation charts from profiled remote data.

    Same chart types as the file-profiler server.

    Single-table: overview, data_quality_scorecard, null_distribution,
    type_distribution, cardinality, completeness, numeric_summary,
    skewness, outlier_summary, correlation_matrix.

    Column-level: top_values, string_lengths, distribution, column_detail.

    Multi-table (table_name="*"): overview_directory, row_counts,
    quality_heatmap, relationship_confidence.

    Args:
        chart_type:    Chart type (default "overview").
        table_name:    Table name or "*" for multi-table charts.
        column_name:   Column name (for column-level charts).
        connection_id: Connection ID (for loading relationship data).
        theme:         "dark" (default) or "light".

    Returns:
        Dict with chart URLs.
    """
    from file_profiler.output.chart_generator import generate_chart, AVAILABLE_CHART_TYPES

    if chart_type not in AVAILABLE_CHART_TYPES:
        return {
            "error": f"Unknown chart type: '{chart_type}'",
            "available_types": AVAILABLE_CHART_TYPES,
        }

    # Determine output dir -- use staging if connection_id given
    cid = connection_id.strip() or None
    if cid:
        out_dir = _staging_dir(cid)
    else:
        out_dir = OUTPUT_DIR

    if ctx:
        await ctx.report_progress(0, 3, "Loading profile data")

    # Multi-table charts
    if table_name == "*" or chart_type in ("overview_directory", "row_counts",
                                            "quality_heatmap", "relationship_confidence"):
        profile_dicts = list(_profile_cache.values())
        if not profile_dicts:
            return {"error": "No profiled tables in cache. Run profile_remote_source first."}

        staging = _staging_dir(cid) if cid else OUTPUT_DIR
        relationship_data = _load_relationship_data(staging) if chart_type == "relationship_confidence" else _relationship_cache

        if ctx:
            await ctx.report_progress(1, 3, "Generating charts")

        charts = generate_chart(
            chart_type=chart_type,
            output_dir=out_dir,
            theme=theme,
            profile_dicts=profile_dicts,
            relationship_data=relationship_data,
        )

    else:
        # Single-table or column-level chart
        if not table_name:
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
                "hint": "Run profile_remote_source first.",
            }

        if ctx:
            await ctx.report_progress(1, 3, "Generating charts")

        charts = generate_chart(
            chart_type=chart_type,
            output_dir=out_dir,
            theme=theme,
            profile_dict=profile_dict,
            column_name=column_name,
        )

    if ctx:
        await ctx.report_progress(2, 3, f"Generated {len(charts)} chart(s)")

    if not charts:
        return {
            "message": f"No charts generated for type '{chart_type}'.",
            "chart_type": chart_type,
        }

    result = {
        "charts": charts,
        "chart_count": len(charts),
        "table_name": table_name or "*",
        "message": f"Generated {len(charts)} chart(s). Charts are displayed as images in the chat.",
    }

    if ctx:
        await ctx.report_progress(3, 3, "Complete")

    log.info("Generated %d chart(s): %s for %s", len(charts), chart_type, table_name or "*")
    return result


# ═══════════════════════════════════════════════════════════════════════════
# QUALITY & KNOWLEDGE BASE TOOLS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool()
async def remote_get_quality_summary(
    table_name: str,
    ctx: Context = None,
) -> dict:
    """
    Get the quality summary for a profiled remote table.

    Uses cached profile data -- does not re-profile.

    Args:
        table_name: Name of the table.

    Returns:
        Quality dict with columns_profiled, columns_with_issues, etc.
    """
    cached = _profile_cache.get(table_name)
    if not cached:
        return {
            "error": f"No cached profile for '{table_name}'. Run profile_remote_source first.",
            "available_tables": list(_profile_cache.keys())[:20],
        }

    return {
        "table_name": table_name,
        "quality_summary": cached.get("quality_summary", {}),
        "structural_issues": cached.get("structural_issues", []),
        "source": "cache",
    }


@mcp.tool()
async def remote_query_knowledge_base(
    question: str,
    top_k: int = 10,
    ctx: Context = None,
) -> dict:
    """
    Semantic search over the vector store of profiled remote tables.

    Queries the ChromaDB vector store (populated by enrich_relationships)
    to find tables and columns relevant to a natural-language question.

    Args:
        question: Natural-language query.
        top_k: Number of results (default 10).

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
async def remote_get_table_relationships(
    table_name: str,
    connection_id: str = "",
    ctx: Context = None,
) -> dict:
    """
    Get all known relationships for a specific remote table.

    Returns both deterministic FK candidates and vector-discovered column
    similarities involving the given table.

    Args:
        table_name: Table to query (e.g. 'customers', 'orders').
        connection_id: Connection ID for loading relationship data.

    Returns:
        Dict with deterministic_relationships, vector_discovered_relationships,
        and related_tables list.
    """
    cid = connection_id.strip() or None
    staging = _staging_dir(cid) if cid else OUTPUT_DIR

    if ctx:
        await ctx.report_progress(0, 3, "Loading relationships")

    result: dict = {
        "table_name": table_name,
        "deterministic_relationships": [],
        "vector_discovered_relationships": [],
        "related_tables": [],
    }

    related: set[str] = set()

    det_rels = _load_relationship_data(staging)

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

    discovered_path = staging / "discovered_column_relationships.json"
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
async def remote_compare_profiles(
    connection_id: str,
    ctx: Context = None,
) -> dict:
    """
    Detect schema drift by comparing current remote data against previously
    profiled state.

    Re-reads staged profiles and compares against stored fingerprints
    in the vector store.

    Args:
        connection_id: Connection to compare.

    Returns:
        Dict with new_tables, removed_tables, changed_tables, unchanged_tables.
    """
    from file_profiler.agent.vector_store import (
        get_or_create_store,
        get_stored_fingerprints,
    )
    from file_profiler.config.env import VECTOR_STORE_DIR

    cid = _resolve_connection_id(connection_id)
    profiles = _get_staged_profiles(cid)

    if not profiles:
        return {"error": f"No staged profiles for connection '{cid}'."}

    if ctx:
        await ctx.report_progress(0, 3, "Computing current fingerprints")

    current_fps = _compute_fingerprints(profiles)

    if ctx:
        await ctx.report_progress(1, 3, "Loading previous fingerprints")

    previous_fingerprints: dict[str, str] = {}
    try:
        store = get_or_create_store(VECTOR_STORE_DIR)
        previous_fingerprints = get_stored_fingerprints(store)
    except Exception:
        pass

    if ctx:
        await ctx.report_progress(2, 3, "Comparing states")

    current_tables = set(current_fps.keys())
    previous_tables = set(previous_fingerprints.keys())

    new_tables = sorted(current_tables - previous_tables)
    removed_tables = sorted(previous_tables - current_tables)
    changed_tables = []
    unchanged_tables = []

    for p in profiles:
        if p.table_name not in previous_fingerprints:
            continue
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
        "connection_id": cid,
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

@mcp.resource("connector-profiles://{table_name}")
async def get_cached_profile(table_name: str) -> str:
    """Return a previously generated remote profile by table name."""
    if table_name not in _profile_cache:
        return json.dumps({
            "error": f"No cached profile for '{table_name}'. Run profile_remote_source first.",
        })
    return json.dumps(_profile_cache[table_name], indent=2)


@mcp.resource("connector-relationships://latest")
async def get_cached_relationships() -> str:
    """Return the most recent relationship report for remote data."""
    if _relationship_cache is None:
        return json.dumps({
            "error": "No relationship report cached. Run detect_relationships first.",
        })
    return json.dumps(_relationship_cache, indent=2)


# ═══════════════════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════════════════

@mcp.prompt()
async def summarize_profile(table_name: str) -> str:
    """Generate a natural-language summary prompt for a profiled remote table."""
    profile = _profile_cache.get(table_name)
    if profile is None:
        content = f"No profile found for table '{table_name}'. Run profile_remote_source first."
    else:
        content = json.dumps(profile, indent=2)

    return (
        f"Analyse the following data profile for remote table '{table_name}'.\n"
        f"Provide a concise summary covering:\n"
        f"1. Row count and column count\n"
        f"2. Column types breakdown\n"
        f"3. Key candidates (likely primary keys)\n"
        f"4. Quality issues (null-heavy columns, type conflicts)\n"
        f"5. Low cardinality columns and distinct value counts\n"
        f"6. Notable patterns\n\n"
        f"Profile data:\n{content}"
    )


@mcp.prompt()
async def migration_readiness(connection_id: str) -> str:
    """Assess migration readiness for a remote data source."""
    profiles_summary = {
        name: p.get("quality_summary", {})
        for name, p in _profile_cache.items()
    }
    rels = _relationship_cache or {}

    return (
        f"Assess migration readiness for remote data source '{connection_id}'.\n\n"
        f"Evaluate based on:\n"
        f"1. Type consistency across columns\n"
        f"2. Null ratios and data completeness\n"
        f"3. Key candidate coverage\n"
        f"4. Relationship coverage\n"
        f"5. Encoding and structural issues\n\n"
        f"Provide a readiness score (High / Medium / Low) with justification.\n\n"
        f"Quality summaries:\n{json.dumps(profiles_summary, indent=2)}\n\n"
        f"Relationships:\n{json.dumps(rels, indent=2)}"
    )


@mcp.prompt()
async def quality_report(table_name: str) -> str:
    """Generate a detailed quality report for a remote table."""
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
        f"Generate a detailed quality report for remote table '{table_name}'.\n\n"
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
    log.info("Received %s -- shutting down gracefully", sig_name)
    _profile_cache.clear()
    _staging_cache.clear()
    log.info("Shutdown complete")
    raise SystemExit(0)


def main() -> None:
    """CLI entry point for the Connector MCP server."""
    import signal

    configure_logging()

    parser = argparse.ArgumentParser(description="Data Connector MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="sse",
        help="Transport protocol (default: sse)",
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=CONNECTOR_MCP_PORT)
    args = parser.parse_args()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _graceful_shutdown)
    signal.signal(signal.SIGINT, _graceful_shutdown)

    # Ensure output directories exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Host and port are set on the FastMCP instance
    mcp.settings.host = args.host
    mcp.settings.port = args.port
    
    log.info(
        "Starting Data Connector MCP server (transport=%s, host=%s, port=%d)",
        args.transport, args.host, args.port,
    )

    # Disable strict host validation for Docker container communication.
    # Older/newer MCP versions may not expose this hook.
    try:
        from mcp.server import transport_security

        validate_origin = getattr(transport_security, "validate_request_origin", None)
        if callable(validate_origin):
            def patched_validate(*args, **kwargs):
                return True  # Allow all hosts in Docker context

            transport_security.validate_request_origin = patched_validate
            log.info("Disabled strict host validation for Docker deployment")
        else:
            log.debug("MCP host-validation hook not available; skipping patch")
    except Exception as e:
        log.warning("Could not patch host validation: %s", e)

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
