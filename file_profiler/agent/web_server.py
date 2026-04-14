"""FastAPI + WebSocket backend for the Data Profiler chat UI.

Usage:
  # Terminal 1 — MCP server:
  python -m file_profiler --transport sse --port 8080

  # Terminal 2 — Web UI:
  python -m file_profiler.agent --web
  # Opens http://localhost:8501
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import logging
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional, cast

# Set event loop policy early before any other imports create a loop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, File, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import StateGraph

from file_profiler.agent.erd_wait import (
    configure_erd_wait_graph,
    extract_erd_turn_status,
    get_last_visible_ai_text,
)
from file_profiler.config.env import DATA_DIR, MAX_UPLOAD_SIZE_MB, OUTPUT_DIR, UPLOAD_DIR
from file_profiler.config.database import get_checkpointer, get_pool, close_pool
from file_profiler.agent.chatbot import (
    CHATBOT_SYSTEM_PROMPT,
    _compact_messages_preserving_tool_pairs,
    _get_int_config,
    _is_timeout_error,
    _trim_messages,
    _validate_and_recover_tool_chain,
)
from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.mcp_endpoints import (
    DEFAULT_FILE_MCP_URL,
    derive_connector_url,
    resolve_mcp_endpoints,
)
from file_profiler.agent.progress import (
    TOOL_WEIGHTS,
    DEFAULT_TOOL_WEIGHT,
    _extract_summary,
    _get_stage_hints,
    canonicalize_tool_name,
)
from file_profiler.agent.state import AgentState
from file_profiler.observability.langsmith import (
    compact_text_output,
    resolve_prompt,
    trace_context,
    traceable,
)

log = logging.getLogger(__name__)


def _trace_web_state_inputs(inputs: dict) -> dict:
    state = inputs.get("state") or {}
    messages = state.get("messages", []) if isinstance(state, dict) else []
    return {
        "message_count": len(messages),
        "mode": state.get("mode") if isinstance(state, dict) else "",
    }


def _trace_web_turn_inputs(inputs: dict) -> dict:
    config = inputs.get("config") or {}
    configurable = config.get("configurable", {}) if isinstance(config, dict) else {}
    turn_inputs = inputs.get("inputs") or {}
    messages = turn_inputs.get("messages", []) if isinstance(turn_inputs, dict) else []
    return {
        "thread_id": configurable.get("thread_id", ""),
        "message_count": len(messages),
    }

# Max chars for payloads sent over WebSocket to prevent browser memory issues
_MAX_WS_CONTENT_CHARS = 50_000
_MAX_WS_SUMMARY_CHARS = 500

FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"


def _to_int_or_none(value: Any) -> int | None:
    """Best-effort numeric conversion for token usage fields."""
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_ai_token_usage(msg: AIMessage) -> dict[str, int] | None:
    """Extract token usage from an AIMessage across provider-specific schemas."""
    usage = getattr(msg, "usage_metadata", None)
    if not isinstance(usage, dict):
        usage = None

    if usage is None:
        response_meta = getattr(msg, "response_metadata", None)
        if isinstance(response_meta, dict):
            token_usage = response_meta.get("token_usage")
            if isinstance(token_usage, dict):
                usage = token_usage
            elif isinstance(response_meta.get("usage"), dict):
                usage = cast(dict[str, Any], response_meta.get("usage"))

    if not usage:
        return None

    input_tokens = (
        _to_int_or_none(usage.get("input_tokens"))
        or _to_int_or_none(usage.get("prompt_tokens"))
        or _to_int_or_none(usage.get("inputTokenCount"))
        or 0
    )
    output_tokens = (
        _to_int_or_none(usage.get("output_tokens"))
        or _to_int_or_none(usage.get("completion_tokens"))
        or _to_int_or_none(usage.get("candidatesTokenCount"))
        or 0
    )
    total_tokens = (
        _to_int_or_none(usage.get("total_tokens"))
        or _to_int_or_none(usage.get("totalTokenCount"))
        or (input_tokens + output_tokens)
    )

    if total_tokens <= 0 and input_tokens <= 0 and output_tokens <= 0:
        return None

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }


# ---------------------------------------------------------------------------
# Preview extractor — turns tool results into compact data for table cards
# ---------------------------------------------------------------------------

def _extract_preview(tool_name: str, content: str) -> dict | None:
    """Extract lightweight preview data from tool result for inline cards."""
    try:
        data = json.loads(content) if content.startswith(("{", "[")) else None
    except (json.JSONDecodeError, TypeError):
        return None

    if data is None:
        return None

    preview_tool_name = _preview_tool_name(tool_name)

    if preview_tool_name == "profile_file" and isinstance(data, list):
        # Database file — returned multiple table profiles
        tables = []
        for p in data[:30]:
            columns = p.get("columns", [])
            col_previews = []
            for c in columns[:12]:
                col_previews.append({
                    "name": c.get("column_name", "?"),
                    "type": c.get("inferred_type", c.get("declared_type", "?")),
                    "null_pct": round(c.get("null_percentage", 0), 1),
                    "distinct": c.get("distinct_count", 0),
                    "flags": [f.get("flag", "") for f in c.get("quality_flags", [])],
                })
            qs = p.get("quality_summary", {})
            tables.append({
                "table_name": p.get("table_name", "?"),
                "row_count": p.get("row_count", 0),
                "col_count": len(columns),
                "format": p.get("format", "?"),
                "columns": col_previews,
                "quality": {
                    "issues": qs.get("columns_with_issues", 0),
                    "total": qs.get("columns_profiled", 0),
                },
            })
        return {
            "kind": "directory",
            "tables": tables,
            "total_rows": sum(t["row_count"] for t in tables),
        }

    if preview_tool_name == "profile_file" and isinstance(data, dict):
        columns = data.get("columns", [])
        col_previews = []
        for c in columns[:12]:  # cap at 12 columns for UI
            col_previews.append({
                "name": c.get("column_name", "?"),
                "type": c.get("inferred_type", c.get("declared_type", "?")),
                "null_pct": round(c.get("null_percentage", 0), 1),
                "distinct": c.get("distinct_count", 0),
                "flags": [f.get("flag", "") for f in c.get("quality_flags", [])],
            })
        qs = data.get("quality_summary", {})
        return {
            "kind": "table",
            "table_name": data.get("table_name", "?"),
            "row_count": data.get("row_count", 0),
            "col_count": len(columns),
            "format": data.get("format", "?"),
            "columns": col_previews,
            "quality": {
                "issues": qs.get("columns_with_issues", 0),
                "total": qs.get("columns_profiled", 0),
            },
        }

    if preview_tool_name == "profile_directory" and isinstance(data, list):
        tables = []
        for p in data[:30]:  # cap at 30 tables
            columns = p.get("columns", [])
            col_previews = []
            for c in columns[:12]:
                col_previews.append({
                    "name": c.get("column_name", "?"),
                    "type": c.get("inferred_type", c.get("declared_type", "?")),
                    "null_pct": round(c.get("null_percentage", 0), 1),
                    "distinct": c.get("distinct_count", 0),
                    "flags": [f.get("flag", "") for f in c.get("quality_flags", [])],
                })
            qs = p.get("quality_summary", {})
            tables.append({
                "table_name": p.get("table_name", "?"),
                "row_count": p.get("row_count", 0),
                "col_count": len(columns),
                "format": p.get("format", "?"),
                "columns": col_previews,
                "quality": {
                    "issues": qs.get("columns_with_issues", 0),
                    "total": qs.get("columns_profiled", 0),
                },
            })
        return {
            "kind": "directory",
            "tables": tables,
            "total_rows": sum(t["row_count"] for t in tables),
        }

    if preview_tool_name == "visualize_profile" and isinstance(data, dict):
        charts = data.get("charts", [])
        if charts:
            return {
                "kind": "charts",
                "charts": charts,
                "table_name": data.get("table_name", ""),
            }

    if preview_tool_name == "detect_relationships" and isinstance(data, dict):
        candidates = data.get("candidates", [])
        fk_previews = []
        for c in candidates[:15]:
            fk = c.get("fk", {})
            pk = c.get("pk", {})
            fk_previews.append({
                "fk": f"{fk.get('table_name', '?')}.{fk.get('column_name', '?')}",
                "pk": f"{pk.get('table_name', '?')}.{pk.get('column_name', '?')}",
                "confidence": round(c.get("confidence", 0), 2),
            })
        return {
            "kind": "relationships",
            "candidates": fk_previews,
        }

    return None


def _preview_tool_name(tool_name: str) -> str:
    """Normalize tool names for preview rendering."""
    if tool_name == "profile_remote_source":
        return "profile_directory"
    return canonicalize_tool_name(tool_name)


def _pipeline_tool_name(tool_name: str) -> str:
    """Normalize tool names for pipeline step lookup."""
    if tool_name == "profile_remote_source":
        return "profile_directory"
    return canonicalize_tool_name(tool_name)


def _tool_output_dir(tool_name: str, args: dict[str, Any] | None = None) -> Path:
    """Resolve the output directory used by a tool's progress/artifacts."""
    args = args or {}
    if tool_name.startswith("remote_"):
        connection_id = str(args.get("connection_id", "")).strip()
        if connection_id:
            return OUTPUT_DIR / "connectors" / connection_id
    return OUTPUT_DIR


# ---------------------------------------------------------------------------
# Pipeline sub-steps per MCP tool — drives the step tracker in the UI
# ---------------------------------------------------------------------------

PIPELINE_STEPS: dict[str, list[dict]] = {
    "list_supported_files": [
        {"name": "Scanning directory"},
        {"name": "Detecting file formats"},
    ],
    "profile_file": [
        {"name": "Intake validation"},
        {"name": "Classifying file format"},
        {"name": "Selecting size strategy"},
        {"name": "Running format engine"},
        {"name": "Standardizing columns"},
        {"name": "Profiling columns"},
        {"name": "Running quality checks"},
        {"name": "Writing output"},
    ],
    "profile_directory": [
        {"name": "Scanning files"},
        {"name": "Profiling tables"},
        {"name": "Inferring column types"},
        {"name": "Computing statistics"},
        {"name": "Running quality checks"},
    ],
    "detect_relationships": [
        {"name": "Profiling tables"},
        {"name": "Matching column names"},
        {"name": "Checking type compatibility"},
        {"name": "Scoring FK candidates"},
        {"name": "Saving intermediate results"},
    ],
    "enrich_relationships": [
        {"name": "Profiling tables"},
        {"name": "Detecting relationships"},
        {"name": "MAP: Summarizing tables & columns"},
        {"name": "APPLY: Writing descriptions to profiles"},
        {"name": "EMBED: Storing in vector DB"},
        {"name": "COLUMN CLUSTER: DBSCAN grouping"},
        {"name": "DERIVE: PK/FK from clusters"},
        {"name": "TABLE CLUSTER: Affinity grouping"},
        {"name": "REDUCE: LLM synthesis"},
        {"name": "Generating enriched ER diagram"},
    ],
    "get_quality_summary": [
        {"name": "Profiling file"},
        {"name": "Analysing quality flags"},
    ],
    "upload_file": [
        {"name": "Decoding upload"},
        {"name": "Saving file"},
    ],
    "query_knowledge_base": [
        {"name": "Searching vector store"},
        {"name": "Ranking results"},
    ],
    "get_table_relationships": [
        {"name": "Loading relationships"},
        {"name": "Matching table"},
    ],
    "compare_profiles": [
        {"name": "Profiling current state"},
        {"name": "Loading previous fingerprints"},
        {"name": "Comparing schemas"},
    ],
    "check_enrichment_status": [
        {"name": "Computing fingerprints"},
        {"name": "Checking completion manifest"},
    ],
    "visualize_profile": [
        {"name": "Loading profile data"},
        {"name": "Generating charts"},
        {"name": "Saving images"},
    ],
}

# Singleton checkpointer — created via lifespan startup.
_checkpointer = None


@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    """Initialize and tear down shared resources for the web server."""
    global _checkpointer
    _checkpointer = await get_checkpointer()
    log.info("Checkpointer initialized: %s", type(_checkpointer).__name__)

    try:
        yield
    finally:
        # Close PostgreSQL pool
        await close_pool()

        # Close cached MCP clients
        n = len(_mcp_client_cache)
        for url, (client, _ts) in list(_mcp_client_cache.items()):
            try:
                if hasattr(client, "close"):
                    await client.close()
            except Exception as exc:
                log.debug("Error closing MCP client for %s: %s", url, exc)
        _mcp_client_cache.clear()
        log.info("Web server shutdown: cleaned up %d cached MCP client(s)", n)


app = FastAPI(title="Data Profiler UI", lifespan=_app_lifespan)


@app.middleware("http")
async def _disable_frontend_cache(request: Request, call_next):
    """Prevent stale HTML/JS/CSS from hiding recent frontend changes."""
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# ── Connection management REST endpoints (credentials bypass the LLM) ────

@app.get("/api/connections")
async def api_list_connections():
    """List all registered connections (no secrets in response)."""
    from file_profiler.connectors.connection_manager import get_connection_manager
    mgr = get_connection_manager()
    summaries = mgr.list_connections()
    return JSONResponse([
        {
            "connection_id": s.connection_id,
            "scheme": s.scheme,
            "display_name": s.display_name,
            "created_at": s.created_at,
            "last_tested": s.last_tested,
            "is_healthy": s.is_healthy,
        }
        for s in summaries
    ])


@app.post("/api/connections")
async def api_create_connection(request: Request):
    """Register a new connection.  Credentials flow here directly from the
    UI — they never pass through the LLM or chat history."""
    from file_profiler.connectors.connection_manager import get_connection_manager
    body = await request.json()

    connection_id = body.get("connection_id", "").strip()
    scheme = body.get("scheme", "").strip()
    credentials = body.get("credentials", {})
    display_name = body.get("display_name", "")

    if not connection_id or not scheme:
        return JSONResponse(
            {"error": "connection_id and scheme are required"},
            status_code=400,
        )

    try:
        mgr = get_connection_manager()
        info = mgr.register(
            connection_id=connection_id,
            scheme=scheme,
            credentials=credentials,
            display_name=display_name,
        )
        return JSONResponse({
            "connection_id": info.connection_id,
            "scheme": info.scheme,
            "display_name": info.display_name,
            "created_at": info.created_at,
        })
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


@app.delete("/api/connections/{connection_id}")
async def api_delete_connection(connection_id: str):
    """Remove a stored connection."""
    from file_profiler.connectors.connection_manager import get_connection_manager
    mgr = get_connection_manager()
    removed = mgr.remove(connection_id)
    return JSONResponse({"deleted": removed})


@app.post("/api/connections/{connection_id}/test")
async def api_test_connection(connection_id: str):
    """Test a stored connection.  Returns success/failure + latency."""
    from file_profiler.connectors.connection_manager import get_connection_manager
    mgr = get_connection_manager()
    try:
        result = mgr.test(connection_id)
        return JSONResponse({
            "success": result.success,
            "message": result.message,
            "latency_ms": round(result.latency_ms, 1),
        })
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)


# Serve CSS/JS as static files
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# Serve generated chart images
def _resolve_charts_dir() -> Path:
    """Resolve a writable charts directory without failing at import time."""
    candidates = (
        OUTPUT_DIR / "charts",
        Path.cwd() / ".profiler_charts",
        Path(tempfile.gettempdir()) / "file_profiler_charts",
    )

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            if candidate != OUTPUT_DIR / "charts":
                log.warning(
                    "Using fallback charts directory %s (default %s unavailable)",
                    candidate,
                    OUTPUT_DIR / "charts",
                )
            return candidate
        except OSError as exc:
            log.debug("Charts directory unavailable at %s: %s", candidate, exc)

    # FRONTEND_DIR always exists; this keeps import/startup from crashing.
    log.warning("Using frontend directory as final charts fallback: %s", FRONTEND_DIR)
    return FRONTEND_DIR


_CHARTS_DIR = _resolve_charts_dir()
app.mount("/charts", StaticFiles(directory=str(_CHARTS_DIR)), name="charts")


@app.get("/")
async def index():
    """Serve the main HTML page."""
    return FileResponse(str(FRONTEND_DIR / "index.html"))


# ── Session API endpoints ─────────────────────────────────

@app.get("/api/sessions")
async def api_list_sessions():
    """Return recent chat sessions for the sidebar."""
    from file_profiler.agent.session_manager import list_sessions
    sessions = await list_sessions(limit=30)
    return JSONResponse(sessions)


@app.post("/api/sessions")
async def api_upsert_session(request: Request):
    """Create or update a session (label, message_count)."""
    from file_profiler.agent.session_manager import touch_session, update_session
    body = await request.json()
    sid = body.get("session_id", "")
    if not sid:
        return JSONResponse({"error": "session_id required"}, status_code=400)

    label = body.get("label", "")
    message_count = body.get("message_count")

    if message_count is not None or label:
        result = await update_session(sid, label=label, message_count=message_count)
        if result is None:
            result = await touch_session(sid, label=label)
    else:
        result = await touch_session(sid, label=label)

    return JSONResponse(result)


@app.delete("/api/sessions/{session_id}")
async def api_delete_session(session_id: str):
    """Delete a session."""
    from file_profiler.agent.session_manager import delete_session
    deleted = await delete_session(session_id)
    return JSONResponse({"deleted": deleted})


# ── File upload endpoint (drag-and-drop) ──────────────────

_SUPPORTED_EXTENSIONS = {
    ".csv", ".tsv", ".dat", ".psv",
    ".parquet", ".pq", ".parq",
    ".json", ".jsonl", ".ndjson",
    ".xlsx", ".xls", ".gz", ".zip",
    ".duckdb", ".db", ".sqlite", ".sqlite3",
}

_UPLOAD_TARGET_ALIASES = {
    "temporary": "temporary",
    "temp": "temporary",
    "upload": "temporary",
    "uploads": "temporary",
    "persistent": "persistent",
    "mounted": "persistent",
    "data": "persistent",
}


def _normalize_upload_target(target: str | None) -> str:
    """Map user-facing storage names to the internal upload target."""
    normalized = _UPLOAD_TARGET_ALIASES.get(str(target or "temporary").strip().lower())
    if normalized is None:
        raise ValueError("Invalid upload target. Use 'temporary' or 'persistent'.")
    return normalized


def _upload_root_for_target(target: str) -> Path:
    """Resolve the filesystem root for the requested upload target."""
    return DATA_DIR if target == "persistent" else UPLOAD_DIR


def _sanitize_batch_id(batch_id: str | None) -> str:
    """Limit batch ids to safe path-segment characters."""
    if not batch_id:
        return uuid.uuid4().hex[:12]

    safe_batch = re.sub(r"[^A-Za-z0-9_-]", "", batch_id)[:64]
    if not safe_batch:
        raise ValueError("Invalid batch_id")
    return safe_batch


def _sanitize_upload_name(file_name: str) -> str:
    """Strip any path segments and fall back to a safe default name."""
    safe_name = Path(file_name or "upload").name.strip().replace("\x00", "")
    if not safe_name or safe_name in {".", ".."}:
        return "upload"
    return safe_name


def _unique_upload_path(dest_dir: Path, file_name: str) -> Path:
    """Avoid overwriting earlier files in the same upload batch."""
    candidate = dest_dir / file_name
    if not candidate.exists():
        return candidate

    stem = candidate.stem or "upload"
    suffix = candidate.suffix
    index = 2
    while True:
        candidate = dest_dir / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


@app.post("/api/upload")
async def upload_file(
    file: UploadFile | None = File(default=None),
    files: list[UploadFile] | None = File(default=None),
    batch_id: str | None = None,
    target: str = "temporary",
):
    """Accept one or more multipart uploads and save them to the requested storage."""
    try:
        storage_target = _normalize_upload_target(target)
        safe_batch = _sanitize_batch_id(batch_id)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    uploads: list[UploadFile] = []
    if file is not None:
        uploads.append(file)
    if files:
        uploads.extend(files)

    if not uploads:
        return JSONResponse(status_code=400, content={"error": "No files provided"})

    storage_root = _upload_root_for_target(storage_target)
    dest_dir = storage_root / safe_batch

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Could not create upload directory under {storage_root}: {exc}",
                "storage_target": storage_target,
                "storage_root": str(storage_root),
            },
        )

    saved: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for upload in uploads:
        original_name = _sanitize_upload_name(upload.filename or "upload")
        ext = Path(original_name).suffix.lower()

        if ext not in _SUPPORTED_EXTENSIONS:
            errors.append({
                "file_name": original_name,
                "error": f"Unsupported file type: {ext or '[none]'}",
            })
            continue

        content = await upload.read()
        size_mb = len(content) / (1024 * 1024)
        if size_mb > MAX_UPLOAD_SIZE_MB:
            errors.append({
                "file_name": original_name,
                "error": (
                    f"File too large ({size_mb:.1f} MB, max {MAX_UPLOAD_SIZE_MB} MB)"
                ),
            })
            continue

        dest = _unique_upload_path(dest_dir, original_name)
        try:
            dest.write_bytes(content)
        except OSError as exc:
            errors.append({
                "file_name": original_name,
                "error": f"Could not save file: {exc}",
            })
            continue

        saved.append({
            "server_path": str(dest),
            "upload_dir": str(dest_dir),
            "file_name": dest.name,
            "original_file_name": original_name,
            "size_bytes": len(content),
            "storage_target": storage_target,
        })

    if not saved:
        first_error = errors[0]["error"] if errors else "Upload failed"
        return JSONResponse(
            status_code=400,
            content={
                "error": first_error,
                "errors": errors,
                "storage_target": storage_target,
                "storage_root": str(storage_root),
            },
        )

    total_size = sum(item["size_bytes"] for item in saved)
    if len(saved) == 1 and len(uploads) == 1 and not errors:
        return {
            **saved[0],
            "storage_root": str(storage_root),
        }

    response: dict[str, Any] = {
        "files": saved,
        "upload_dir": str(dest_dir),
        "file_count": len(saved),
        "size_bytes": total_size,
        "storage_target": storage_target,
        "storage_root": str(storage_root),
    }
    if errors:
        response["errors"] = errors
    return response


# ── MCP client cache ──────────────────────────────────────
# Reuse the MCP client across WebSocket sessions to avoid reconnection overhead.
# Entries are (client, last_used_timestamp) tuples with TTL-based eviction.
import time as _time

_MCP_CLIENT_TTL_SECONDS = 3600  # 1 hour

_mcp_client_cache: dict[str, tuple] = {}  # mcp_url → (client, last_used_ts)
_WEB_MCP_URL_OVERRIDE: Optional[str] = None
_WEB_CONNECTOR_MCP_URL_OVERRIDE: Optional[str] = None


def _derive_connector_url(base_url: str) -> str:
    """Backward-compatible wrapper for connector MCP URL derivation."""
    return derive_connector_url(base_url)


def _default_web_mcp_url() -> str:
    """Resolve the default file-profiler MCP endpoint for web mode."""
    for candidate in (_WEB_MCP_URL_OVERRIDE, os.getenv("WEB_MCP_URL"), os.getenv("MCP_URL")):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return DEFAULT_FILE_MCP_URL


def _default_web_connector_url(mcp_url: str) -> str:
    """Resolve the default connector MCP endpoint for web mode."""
    for candidate in (
        _WEB_CONNECTOR_MCP_URL_OVERRIDE,
        os.getenv("WEB_CONNECTOR_MCP_URL"),
        os.getenv("CONNECTOR_MCP_URL"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return _derive_connector_url(mcp_url)


def _load_langgraph_prebuilt():
    try:
        from langgraph.prebuilt import ToolNode, tools_condition
        return ToolNode, tools_condition
    except ImportError as exc:
        raise RuntimeError(
            "LangGraph prebuilt components are unavailable. "
            "Install compatible versions of langgraph and langgraph-prebuilt."
        ) from exc


# ── Graph builder ─────────────────────────────────────────

async def _build_graph(
    mcp_url: str = DEFAULT_FILE_MCP_URL,
    connector_mcp_url: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
):
    """Build the LangGraph chat graph connected to both MCP servers.

    Returns (compiled_graph, tool_count) or raises on failure.
    Reuses the MCP client if the URL hasn't changed.
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient

    mcp_url, connector_url, transport = resolve_mcp_endpoints(
        mcp_url=mcp_url,
        connector_mcp_url=connector_mcp_url,
    )

    ToolNode, _ = _load_langgraph_prebuilt()

    mcp_client_timeout = _get_int_config("MCP_CLIENT_TIMEOUT", 120)
    chat_llm_timeout = _get_int_config(
        "CHATBOT_LLM_TIMEOUT",
        _get_int_config("LLM_TIMEOUT", 120),
    )

    file_profiler_cfg = {
        "url": mcp_url,
        "transport": transport,
        "timeout": mcp_client_timeout,
        "sse_read_timeout": 3600,
    }
    connector_cfg = {
        "url": connector_url,
        "transport": transport,
        "timeout": mcp_client_timeout,
        "sse_read_timeout": 3600,
    }

    def _make_client(include_connector: bool = True):
        servers = {"file-profiler": file_profiler_cfg}
        if include_connector:
            servers["data-connector"] = connector_cfg
        return MultiServerMCPClient(cast(Any, servers))

    # Evict expired entries
    now = _time.time()
    for url in list(_mcp_client_cache):
        _, ts = _mcp_client_cache[url]
        if now - ts > _MCP_CLIENT_TTL_SECONDS:
            _mcp_client_cache.pop(url, None)
            log.debug("Evicted stale MCP client for %s (TTL expired)", url)

    # Reuse existing client for the same URL, or create a new one
    if mcp_url in _mcp_client_cache:
        client, _ = _mcp_client_cache[mcp_url]
        _mcp_client_cache[mcp_url] = (client, now)  # refresh TTL
        log.debug("Reusing cached MCP client for %s", mcp_url)
    else:
        # Try both servers; fall back to file-profiler only if connector is down
        try:
            client = _make_client(include_connector=True)
            _mcp_client_cache[mcp_url] = (client, now)
        except Exception:
            log.warning(
                "Could not connect to connector server at %s. "
                "Continuing with file-profiler only.",
                connector_url,
            )
            client = _make_client(include_connector=False)
            _mcp_client_cache[mcp_url] = (client, now)

    try:
        tools = await client.get_tools()
    except Exception:
        # Client may be stale -- discard and retry
        _mcp_client_cache.pop(mcp_url, None)
        try:
            client = _make_client(include_connector=True)
            tools = await client.get_tools()
        except Exception:
            log.warning("Connector server unavailable, falling back to file-profiler only")
            client = _make_client(include_connector=False)
            tools = await client.get_tools()
        _mcp_client_cache[mcp_url] = (client, _time.time())

    if not tools:
        raise RuntimeError("MCP server returned no tools")

    llm = get_llm_with_fallback(
        provider=provider,
        model=model,
        timeout=chat_llm_timeout,
    )
    llm_with_tools = llm.bind_tools(tools)

    @traceable(
        name="agent.web_node",
        run_type="chain",
        process_inputs=_trace_web_state_inputs,
        process_outputs=compact_text_output,
    )
    async def agent_node(state: AgentState):
        messages = state["messages"]
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [
                SystemMessage(
                    content=resolve_prompt(
                        "file-profiler/chatbot_system",
                        CHATBOT_SYSTEM_PROMPT,
                    )
                )
            ] + list(messages)

        messages, recovered = _validate_and_recover_tool_chain(messages)
        if recovered:
            log.warning("Recovered inconsistent tool-call chain before web LLM invoke")

        messages = _trim_messages(messages)
        try:
            response = await llm_with_tools.ainvoke(messages)
            return {"messages": [response]}
        except BaseException as exc:
            if not _is_timeout_error(exc):
                raise

            log.warning(
                "Web LLM request timed out (timeout=%ss); retrying with compact context",
                chat_llm_timeout,
            )

            compact_messages = _compact_messages_preserving_tool_pairs(
                messages,
                max_non_system_messages=8,
            )

            try:
                response = await llm_with_tools.ainvoke(compact_messages)
                return {"messages": [response]}
            except BaseException as retry_exc:
                if not _is_timeout_error(retry_exc):
                    raise

                log.warning(
                    "Web LLM request timed out after retry (timeout=%ss)",
                    chat_llm_timeout,
                )
                return {
                    "messages": [
                        AIMessage(
                            content=(
                                "The data work finished, but the final explanation step "
                                "timed out at the LLM provider. Increase "
                                "CHATBOT_LLM_TIMEOUT in config.yml, or retry and the "
                                "cached results should be reused."
                            )
                        )
                    ]
                }

    tool_node = ToolNode(tools, handle_tool_errors=True)

    async def tools_node(state: AgentState):
        messages = list(state.get("messages", []))
        checked, recovered = _validate_and_recover_tool_chain(
            messages,
            allow_pending_tail_tool_calls=True,
        )
        if recovered:
            log.warning("Recovered inconsistent tool-call chain before web tool execution")

        if not checked or not isinstance(checked[-1], AIMessage) or not checked[-1].tool_calls:
            log.warning("Skipped web tool execution due to non-executable tail tool state")
            return {"messages": []}

        safe_state = dict(state)
        safe_state["messages"] = checked
        return await tool_node.ainvoke(safe_state)

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tools_node)
    configure_erd_wait_graph(builder)

    graph = builder.compile(checkpointer=_checkpointer)

    return graph, len(tools)


# ── WebSocket chat endpoint ───────────────────────────────

# ── Rate limiting ─────────────────────────────────────────
_MAX_CONCURRENT_SESSIONS = 10
_active_sessions: int = 0
_MIN_MESSAGE_INTERVAL_SECONDS = 1.0  # min gap between user messages


def _resolve_web_provider(requested_provider: Optional[str]) -> str:
    """Resolve a provider that is likely to work in local web mode.

    Priority:
    1) Explicit provider from UI, if its key is configured.
    2) LLM_PROVIDER from env, if its key is configured.
    3) First configured provider in a stable order.
    4) "openai" as final default (will still show a clear error if key missing).
    """

    def _has_key(provider_name: str) -> bool:
        if provider_name == "openai":
            return bool(
                os.getenv("OPENAI_API_KEY", "").strip()
                or os.getenv("NVIDIA_API_KEY", "").strip()
            )
        key_env = {
            "groq": "GROQ_API_KEY",
            "google": "GOOGLE_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
        }.get(provider_name)
        if not key_env:
            return False
        return bool(os.getenv(key_env, "").strip())

    candidate = (requested_provider or "").strip().lower()
    if candidate and _has_key(candidate):
        return candidate

    env_provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    if env_provider and _has_key(env_provider):
        return env_provider

    for provider_name in ("openai", "groq", "google", "anthropic"):
        if _has_key(provider_name):
            return provider_name

    return "openai"


@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """Handle a single chat session over WebSocket."""
    global _active_sessions

    if _active_sessions >= _MAX_CONCURRENT_SESSIONS:
        await websocket.close(code=1013, reason="Server at capacity — try again later")
        return

    await websocket.accept()
    _active_sessions += 1

    graph = None
    session_id = "web-session-1"  # updated from client's config message
    last_message_time = 0.0
    message_count = 0

    async def _safe_send(msg: dict) -> bool:
        """Send JSON over WebSocket, returning False if the connection is gone."""
        try:
            await websocket.send_json(msg)
            return True
        except (WebSocketDisconnect, ConnectionResetError, RuntimeError, OSError):
            return False

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)

            if data["type"] == "config":
                # Capture session ID from client (UUID generated in browser)
                candidate_session_id = data.get("session_id")
                if isinstance(candidate_session_id, str) and candidate_session_id.strip():
                    session_id = candidate_session_id

                # (Re-)connect to MCP and build graph
                requested_mcp_url = data.get("mcp_url")
                if not isinstance(requested_mcp_url, str) or not requested_mcp_url.strip():
                    requested_mcp_url = _default_web_mcp_url()

                requested_connector_url = data.get("connector_mcp_url")
                if (
                    not isinstance(requested_connector_url, str)
                    or not requested_connector_url.strip()
                ):
                    requested_connector_url = _derive_connector_url(requested_mcp_url)

                provider = _resolve_web_provider(data.get("provider"))
                try:
                    graph, tool_count = await _build_graph(
                        mcp_url=requested_mcp_url,
                        connector_mcp_url=requested_connector_url,
                        provider=provider,
                    )
                except Exception as exc:
                    if not await _safe_send({
                        "type": "error",
                        "content": f"Could not connect to MCP server: {exc}",
                    }):
                        break
                    continue

                # Persist session in database (non-critical — don't block MCP)
                try:
                    from file_profiler.agent.session_manager import touch_session
                    await touch_session(session_id)
                except Exception as exc:
                    log.warning("Could not persist session %s: %s", session_id, exc)

                has_history = False
                if _checkpointer and hasattr(_checkpointer, 'aget'):
                    try:
                        cp = await _checkpointer.aget(
                            {"configurable": {"thread_id": session_id}}
                        )
                        has_history = cp is not None
                    except Exception:
                        pass

                resp = {
                    "type": "connected",
                    "tools": tool_count,
                    "session_id": session_id,
                    "has_history": has_history,
                }
                if not await _safe_send(resp):
                    break

                # Send conversation history if resuming a session
                if has_history and graph is not None:
                    try:
                        history_msgs = await _get_history_messages(
                            graph, session_id
                        )
                        if history_msgs:
                            await _safe_send({
                                "type": "history",
                                "messages": history_msgs,
                            })
                    except Exception as exc:
                        log.warning(
                            "Could not load history for %s: %s",
                            session_id, exc,
                        )

                continue

            if data["type"] == "message":
                if graph is None:
                    if not await _safe_send({
                        "type": "error",
                        "content": "Not connected to MCP server. Refresh the page.",
                    }):
                        break
                    continue

                # Rate limit: enforce minimum interval between messages
                now = _time.time()
                if now - last_message_time < _MIN_MESSAGE_INTERVAL_SECONDS:
                    if not await _safe_send({
                        "type": "error",
                        "content": "Please wait a moment before sending another message.",
                    }):
                        break
                    continue
                last_message_time = now

                user_text = data.get("content", "").strip()
                if not user_text:
                    continue

                config = {"configurable": {"thread_id": session_id}}
                inputs = {
                    "messages": [HumanMessage(content=user_text)],
                    "mode": "autonomous",
                    "erd_retry_count": 0,
                    "erd_guard_action": "",
                }

                with trace_context(
                    surface="web",
                    flow="agent",
                    metadata={
                        "thread_id": session_id,
                        "user_input_chars": len(user_text),
                    },
                    tags=("mode:websocket",),
                ):
                    await _stream_turn(websocket, graph, inputs, config)

                # Track message count (user + assistant = 2 per turn)
                message_count += 2
                try:
                    from file_profiler.agent.session_manager import update_session
                    await update_session(session_id, message_count=message_count)
                except Exception:
                    pass

    except WebSocketDisconnect:
        log.info("WebSocket client disconnected")
    except (ConnectionResetError, RuntimeError, OSError) as exc:
        log.warning("WebSocket connection lost: %s", exc)
    except Exception as exc:
        log.exception("WebSocket error: %s", exc)
        await _safe_send({
            "type": "error",
            "content": "An internal error occurred. Please try again.",
        })
    finally:
        _active_sessions = max(0, _active_sessions - 1)


async def _get_history_messages(graph, session_id: str) -> list[dict]:
    """Retrieve conversation history from the checkpointer.

    Returns a list of ``{"role": "user"|"assistant"|"tool", "content": ...}``
    dicts suitable for rendering in the frontend.  Tool messages are collapsed
    into a single-line summary to keep the payload small.
    """
    config = {"configurable": {"thread_id": session_id}}
    state = await graph.aget_state(config)
    if not state or not state.values:
        return []

    messages = state.values.get("messages", [])
    history: list[dict] = []

    for msg in messages:
        if isinstance(msg, HumanMessage):
            content = msg.content if isinstance(msg.content, str) else str(msg.content)
            history.append({"role": "user", "content": content})
        elif isinstance(msg, AIMessage):
            # Skip pure tool-call messages (no visible text)
            if msg.tool_calls and not msg.content:
                continue
            content = msg.content
            if isinstance(content, list):
                content = " ".join(
                    item.get("text", str(item)) if isinstance(item, dict) else str(item)
                    for item in content
                )
            if content:
                # Truncate very long assistant messages
                if len(content) > _MAX_WS_CONTENT_CHARS:
                    content = content[:_MAX_WS_CONTENT_CHARS] + "\n\n... (truncated)"
                history.append({"role": "assistant", "content": content})
        elif isinstance(msg, ToolMessage):
            # Compact tool summary — don't send full payloads
            tool_name = getattr(msg, "name", "") or ""
            raw = msg.content if isinstance(msg.content, str) else str(msg.content)
            summary = _extract_summary(tool_name, raw)
            if len(summary) > _MAX_WS_SUMMARY_CHARS:
                summary = summary[:_MAX_WS_SUMMARY_CHARS - 3] + "..."
            history.append({
                "role": "tool",
                "tool": tool_name,
                "content": summary,
            })

    return history


async def _stream_turn(
    websocket: WebSocket,
    graph,
    inputs: dict,
    config: dict,
) -> None:
    """Stream one conversational turn, sending events over WebSocket."""
    pending_tools: dict[str, str] = {}  # tool_call_id → tool_name
    completed_tools: set[str] = set()   # tool names that finished this turn
    final_text = ""

    # Progress tracking state
    completed_weight = 0.0
    total_weight = 0.0
    tool_index = 0
    stage_hint_tasks: dict[str, asyncio.Task] = {}  # tool_id → hint sender task

    async def _send_stage_hints(tool_id: str, tool_name: str):
        """Poll the enrichment progress file for real phase completion.

        For 'enrich_relationships', reads the progress file written by the
        MCP server at each actual pipeline phase completion, updating the
        step tracker synchronously with real work.  Also forwards live
        stats (tables_done, rows, columns, fk) for real-time counters.

        For other tools, falls back to time-based hint rotation.
        """
        from file_profiler.agent.enrichment_progress import read_progress

        hints = _get_stage_hints(tool_name)
        steps = PIPELINE_STEPS.get(tool_name, [])
        idx = 0
        last_progress_detail = ""
        preview_sent = False

        try:
            while True:
                if tool_name == "enrich_relationships":
                    # Poll progress file for real phase/detail changes
                    progress = read_progress(OUTPUT_DIR)
                    if progress:
                        detail = progress.get("detail", "")
                        progress_key = f"{progress['step']}:{detail}"

                        # Send update if step OR detail changed (per-table)
                        if progress_key != last_progress_detail:
                            last_progress_detail = progress_key
                            step_idx = progress["step"]
                            stage_name = progress.get("name", "Processing")
                            display = f"{stage_name}: {detail}" if detail else stage_name

                            # Step-to-percent mapping from MCP server
                            step_pcts = {0: 0, 1: 8, 2: 12, 3: 60, 4: 65,
                                         5: 72, 6: 75, 7: 78, 8: 95, 9: 99}
                            pct = step_pcts.get(step_idx, 0)

                            # If we have per-table stats during MAP, use
                            # them for more granular percent
                            stats = progress.get("stats")
                            if stats and step_idx == 2:
                                done = stats.get("tables_done", 0)
                                total = stats.get("total_tables", 1)
                                pct = 12 + int(done / total * 48)

                            msg = {
                                "type": "progress",
                                "percent": round(min(pct, 99), 1),
                                "stage": display,
                                "tool": tool_name,
                                "tool_index": tool_index,
                            }

                            # Forward live stats if available
                            if stats:
                                msg["stats"] = stats

                            await websocket.send_json(msg)

                            # Emit per-table preview cards once
                            # when profiling phase completes
                            if (stats
                                    and stats.get("profiles_preview")
                                    and not preview_sent):
                                preview_sent = True
                                preview = {
                                    "kind": "directory",
                                    "tables": stats["profiles_preview"],
                                    "total_rows": sum(
                                        t.get("row_count", 0)
                                        for t in stats["profiles_preview"]
                                    ),
                                }
                                await websocket.send_json({
                                    "type": "tool_result",
                                    "tool": "enrich_relationships",
                                    "tool_index": tool_index,
                                    "percent": round(min(pct, 99), 1),
                                    "summary": f"{len(stats['profiles_preview'])} tables profiled",
                                    "success": True,
                                    "preview": preview,
                                })

                            if steps and step_idx < len(steps):
                                await websocket.send_json({
                                    "type": "step_update",
                                    "tool": tool_name,
                                    "active_step": step_idx,
                                    "total_steps": len(steps),
                                })

                else:
                    # Fallback: time-based rotation for non-enrichment tools
                    hint = hints[idx] if hints else "Processing"
                    pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0
                    await websocket.send_json({
                        "type": "progress",
                        "percent": round(min(pct, 99), 1),
                        "stage": hint,
                        "tool": tool_name,
                        "tool_index": tool_index,
                    })

                    if steps:
                        step_idx = min(
                            int(idx / max(len(hints) - 1, 1) * len(steps)),
                            len(steps) - 1,
                        ) if hints else 0
                        await websocket.send_json({
                            "type": "step_update",
                            "tool": tool_name,
                            "active_step": step_idx,
                            "total_steps": len(steps),
                        })

                    if idx < len(hints) - 1:
                        idx += 1

                # Poll faster for enrichment (per-table updates) vs other tools
                await asyncio.sleep(1 if tool_name == "enrich_relationships" else 2)
        except asyncio.CancelledError:
            pass
        except (ConnectionResetError, OSError):
            # WebSocket closed while sending hints — silently stop
            pass

    try:
        async for event in graph.astream(inputs, config=config, stream_mode="updates"):
            for node_name, node_output in event.items():

                if node_name == "agent":
                    msg = node_output["messages"][-1]
                    if isinstance(msg, AIMessage):
                        if msg.tool_calls:
                            for tc in msg.tool_calls:
                                raw_tool_id = tc.get("id", tc["name"])
                                tool_id = str(raw_tool_id)
                                tool_name = tc["name"]
                                tool_index += 1
                                pending_tools[tool_id] = tool_name

                                weight = TOOL_WEIGHTS.get(tool_name, DEFAULT_TOOL_WEIGHT)
                                total_weight += weight

                                hints = _get_stage_hints(tool_name)
                                pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0

                                await websocket.send_json({
                                    "type": "tool_start",
                                    "tool": tool_name,
                                    "tool_index": tool_index,
                                    "percent": round(min(pct, 99), 1),
                                    "stage": hints[0] if hints else "Processing",
                                })

                                # Send pipeline sub-steps for step tracker
                                steps = PIPELINE_STEPS.get(tool_name, [])
                                if steps:
                                    await websocket.send_json({
                                        "type": "pipeline_steps",
                                        "tool": tool_name,
                                        "steps": steps,
                                    })


                                # Start rotating stage hints
                                stage_hint_tasks[tool_id] = asyncio.create_task(
                                    _send_stage_hints(tool_id, tool_name)
                                )

                        elif msg.content:
                            final_text = msg.content
                        else:
                            await websocket.send_json({
                                "type": "thinking",
                                "percent": round(
                                    (completed_weight / total_weight * 100)
                                    if total_weight > 0 else 0, 1
                                ),
                            })

                elif node_name == "tools":
                    for msg in node_output["messages"]:
                        if isinstance(msg, ToolMessage):
                            tool_id = msg.tool_call_id or "unknown-tool-call"
                            tool_name = pending_tools.pop(tool_id, "unknown")
                            completed_tools.add(tool_name)

                            # Cancel stage hint sender
                            hint_task = stage_hint_tasks.pop(tool_id, None)
                            if hint_task and not hint_task.done():
                                hint_task.cancel()

                            weight = TOOL_WEIGHTS.get(tool_name, DEFAULT_TOOL_WEIGHT)
                            completed_weight += weight
                            pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0

                            content = msg.content if isinstance(msg.content, str) else str(msg.content)
                            summary = _extract_summary(tool_name, content)
                            has_error = "Error" in content[:100]

                            # Truncate summary to avoid oversized WS frames
                            if len(summary) > _MAX_WS_SUMMARY_CHARS:
                                summary = summary[:_MAX_WS_SUMMARY_CHARS - 3] + "..."

                            # Extract preview data for table cards
                            preview = _extract_preview(tool_name, content)

                            result_msg = {
                                "type": "tool_result",
                                "tool": tool_name,
                                "tool_index": tool_index,
                                "percent": round(min(pct, 100), 1),
                                "summary": summary,
                                "success": not has_error,
                            }
                            if preview:
                                result_msg["preview"] = preview
                            await websocket.send_json(result_msg)

                            # Mark all steps complete for this tool
                            steps = PIPELINE_STEPS.get(tool_name, [])
                            if steps:
                                await websocket.send_json({
                                    "type": "step_complete",
                                    "tool": tool_name,
                                    "success": not has_error,
                                })


    except (ConnectionResetError, OSError) as exc:
        # Windows ProactorBasePipeTransport can drop SSE connections during
        # long-running MCP tool calls (e.g. enrichment of 100+ tables).
        # Cancel hint tasks and report gracefully.
        for task in stage_hint_tasks.values():
            if not task.done():
                task.cancel()
        log.warning("SSE/MCP connection reset during tool execution: %s", exc)
        try:
            await websocket.send_json({
                "type": "error",
                "content": (
                    "The connection to the MCP server was interrupted during a "
                    "long-running operation. The work may have completed on the "
                    "server side. Try refreshing and re-running the command — "
                    "cached results will be reused automatically."
                ),
            })
        except Exception:
            pass
        return
    except Exception as exc:
        # Cancel any running hint tasks
        for task in stage_hint_tasks.values():
            if not task.done():
                task.cancel()
        try:
            await websocket.send_json({
                "type": "error",
                "content": f"Error during processing: {exc}",
            })
        except (ConnectionResetError, OSError):
            log.warning("Could not send error to client — connection already closed")
        return

    # Cancel any remaining hint tasks
    for task in stage_hint_tasks.values():
        if not task.done():
            task.cancel()

    # Send 100% completion
    if tool_index > 0:
        await websocket.send_json({
            "type": "progress",
            "percent": 100,
            "stage": f"Complete — {tool_index} step{'s' if tool_index != 1 else ''}",
            "tool": "",
            "tool_index": tool_index,
        })

    # Normalise content (some providers return list of dicts)
    if isinstance(final_text, list):
        final_text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in final_text
        )

    if final_text:
        # Truncate very large assistant responses to prevent browser OOM
        content_to_send = final_text
        truncated = False
        if isinstance(content_to_send, str) and len(content_to_send) > _MAX_WS_CONTENT_CHARS:
            content_to_send = content_to_send[:_MAX_WS_CONTENT_CHARS] + "\n\n... (truncated — full output saved to disk)"
            truncated = True
        await websocket.send_json({
            "type": "assistant",
            "content": content_to_send,
            **({"truncated": True} if truncated else {}),
        })
    else:
        await websocket.send_json({
            "type": "assistant",
            "content": "I didn't get a response. Please try again.",
        })

    # Only send the ER diagram if this turn actually ran a relationship/enrichment tool
    er_tools = {"detect_relationships", "enrich_relationships"}
    ran_er_tool = bool(completed_tools & er_tools)

    if ran_er_tool:
        # ER diagram is only produced by the LLM enrichment REDUCE phase.
        # detect_relationships saves intermediate JSON only (no ER diagram).
        enriched_er_path = OUTPUT_DIR / "enriched_er_diagram.md"

        if enriched_er_path.exists():
            try:
                er_content = enriched_er_path.read_text(encoding="utf-8").strip()
                if er_content:
                    await websocket.send_json({
                        "type": "er_diagram",
                        "content": er_content,
                    })
            except Exception as exc:
                log.warning("Could not read ER diagram: %s", exc)


# ── Runner ────────────────────────────────────────────────

@traceable(
    name="agent.web_turn",
    run_type="chain",
    process_inputs=_trace_web_turn_inputs,
    process_outputs=compact_text_output,
)
async def _stream_turn_guarded(
    websocket: WebSocket,
    graph,
    inputs: dict,
    config: dict,
) -> None:
    """Guarded websocket streamer that waits for ER diagrams to exist."""
    pending_tools: dict[str, dict[str, Any]] = {}
    final_text = ""
    completed_weight = 0.0
    total_weight = 0.0
    tool_count = 0
    stage_hint_tasks: dict[str, asyncio.Task] = {}

    async def _send_stage_hints(
        tool_id: str,
        tool_name: str,
        tool_args: dict[str, Any],
        tool_index: int,
    ) -> None:
        from file_profiler.agent.enrichment_progress import read_progress

        hints = _get_stage_hints(tool_name)
        steps = PIPELINE_STEPS.get(_pipeline_tool_name(tool_name), [])
        output_dir = _tool_output_dir(tool_name, tool_args)
        is_enrichment = tool_name in {
            "enrich_relationships",
            "remote_enrich_relationships",
        }
        idx = 0
        last_progress_detail = ""
        preview_sent = False

        try:
            while True:
                if is_enrichment:
                    progress = read_progress(output_dir)
                    if progress:
                        detail = progress.get("detail", "")
                        progress_key = f"{progress['step']}:{detail}"
                        if progress_key != last_progress_detail:
                            last_progress_detail = progress_key
                            step_idx = progress["step"]
                            stage_name = progress.get("name", "Processing")
                            display = f"{stage_name}: {detail}" if detail else stage_name
                            step_pcts = {
                                0: 0,
                                1: 8,
                                2: 12,
                                3: 60,
                                4: 65,
                                5: 72,
                                6: 75,
                                7: 78,
                                8: 95,
                                9: 99,
                            }
                            pct = step_pcts.get(step_idx, 0)
                            stats = progress.get("stats")
                            if stats and step_idx == 2:
                                done = stats.get("tables_done", 0)
                                total = stats.get("total_tables", 1)
                                pct = 12 + int(done / total * 48)

                            progress_msg = {
                                "type": "progress",
                                "percent": round(min(pct, 99), 1),
                                "stage": display,
                                "tool": tool_name,
                                "tool_index": tool_index,
                            }
                            if stats:
                                progress_msg["stats"] = stats
                            await websocket.send_json(progress_msg)

                            if stats and stats.get("profiles_preview") and not preview_sent:
                                preview_sent = True
                                preview = {
                                    "kind": "directory",
                                    "tables": stats["profiles_preview"],
                                    "total_rows": sum(
                                        table.get("row_count", 0)
                                        for table in stats["profiles_preview"]
                                    ),
                                }
                                await websocket.send_json({
                                    "type": "tool_result",
                                    "tool": tool_name,
                                    "tool_index": tool_index,
                                    "percent": round(min(pct, 99), 1),
                                    "summary": f"{len(stats['profiles_preview'])} tables profiled",
                                    "success": True,
                                    "preview": preview,
                                })

                            if steps and step_idx < len(steps):
                                await websocket.send_json({
                                    "type": "step_update",
                                    "tool": tool_name,
                                    "active_step": step_idx,
                                    "total_steps": len(steps),
                                })
                else:
                    hint = hints[idx] if hints else "Processing"
                    pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0
                    await websocket.send_json({
                        "type": "progress",
                        "percent": round(min(pct, 99), 1),
                        "stage": hint,
                        "tool": tool_name,
                        "tool_index": tool_index,
                    })

                    if steps:
                        step_idx = min(
                            int(idx / max(len(hints) - 1, 1) * len(steps)),
                            len(steps) - 1,
                        ) if hints else 0
                        await websocket.send_json({
                            "type": "step_update",
                            "tool": tool_name,
                            "active_step": step_idx,
                            "total_steps": len(steps),
                        })

                    if idx < len(hints) - 1:
                        idx += 1

                await asyncio.sleep(1 if is_enrichment else 2)
        except asyncio.CancelledError:
            pass
        except (ConnectionResetError, OSError):
            pass

    try:
        async for event in graph.astream(inputs, config=config, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "agent":
                    msg = node_output["messages"][-1]
                    if isinstance(msg, AIMessage):
                        if msg.tool_calls:
                            llm_usage = _extract_ai_token_usage(msg)
                            for tc in msg.tool_calls:
                                tool_name = str(tc["name"])
                                tool_args = tc.get("args", {}) or {}
                                tool_id = str(tc.get("id", tool_name))
                                tool_count += 1
                                tool_index = tool_count
                                pending_tools[tool_id] = {
                                    "name": tool_name,
                                    "args": tool_args,
                                    "index": tool_index,
                                    "llm_usage": llm_usage,
                                }

                                weight = TOOL_WEIGHTS.get(
                                    _pipeline_tool_name(tool_name),
                                    DEFAULT_TOOL_WEIGHT,
                                )
                                total_weight += weight
                                hints = _get_stage_hints(tool_name)
                                pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0

                                start_msg = {
                                    "type": "tool_start",
                                    "tool": tool_name,
                                    "tool_index": tool_index,
                                    "percent": round(min(pct, 99), 1),
                                    "stage": hints[0] if hints else "Processing",
                                }
                                if llm_usage:
                                    start_msg["llm_usage"] = llm_usage
                                await websocket.send_json(start_msg)

                                steps = PIPELINE_STEPS.get(_pipeline_tool_name(tool_name), [])
                                if steps:
                                    await websocket.send_json({
                                        "type": "pipeline_steps",
                                        "tool": tool_name,
                                        "steps": steps,
                                    })

                                stage_hint_tasks[tool_id] = asyncio.create_task(
                                    _send_stage_hints(tool_id, tool_name, tool_args, tool_index)
                                )
                        elif msg.content:
                            final_text = msg.content
                        else:
                            await websocket.send_json({
                                "type": "thinking",
                                "percent": round(
                                    (completed_weight / total_weight * 100)
                                    if total_weight > 0 else 0,
                                    1,
                                ),
                            })
                elif node_name == "tools":
                    for msg in node_output["messages"]:
                        if isinstance(msg, ToolMessage):
                            tool_id = msg.tool_call_id or "unknown-tool-call"
                            tool_info = pending_tools.pop(
                                tool_id,
                                {"name": "unknown", "args": {}, "index": tool_count},
                            )
                            tool_name = str(tool_info["name"])
                            tool_index = int(tool_info["index"])
                            llm_usage = tool_info.get("llm_usage")

                            hint_task = stage_hint_tasks.pop(tool_id, None)
                            if hint_task and not hint_task.done():
                                hint_task.cancel()

                            weight = TOOL_WEIGHTS.get(
                                _pipeline_tool_name(tool_name),
                                DEFAULT_TOOL_WEIGHT,
                            )
                            completed_weight += weight
                            pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0

                            content = msg.content if isinstance(msg.content, str) else str(msg.content)
                            summary = _extract_summary(tool_name, content)
                            has_error = "Error" in content[:100]
                            if len(summary) > _MAX_WS_SUMMARY_CHARS:
                                summary = summary[:_MAX_WS_SUMMARY_CHARS - 3] + "..."

                            result_msg = {
                                "type": "tool_result",
                                "tool": tool_name,
                                "tool_index": tool_index,
                                "percent": round(min(pct, 100), 1),
                                "summary": summary,
                                "success": not has_error,
                            }
                            if isinstance(llm_usage, dict):
                                result_msg["llm_usage"] = llm_usage
                            preview = _extract_preview(tool_name, content)
                            if preview:
                                result_msg["preview"] = preview
                            await websocket.send_json(result_msg)

                            steps = PIPELINE_STEPS.get(_pipeline_tool_name(tool_name), [])
                            if steps:
                                await websocket.send_json({
                                    "type": "step_complete",
                                    "tool": tool_name,
                                    "success": not has_error,
                                })
    except (ConnectionResetError, OSError) as exc:
        for task in stage_hint_tasks.values():
            if not task.done():
                task.cancel()
        log.warning("SSE/MCP connection reset during tool execution: %s", exc)
        try:
            await websocket.send_json({
                "type": "error",
                "content": (
                    "The connection to the MCP server was interrupted during a "
                    "long-running operation. The work may have completed on the "
                    "server side. Try refreshing and re-running the command â€” "
                    "cached results will be reused automatically."
                ),
            })
        except Exception:
            pass
        return
    except Exception as exc:
        for task in stage_hint_tasks.values():
            if not task.done():
                task.cancel()
        try:
            await websocket.send_json({
                "type": "error",
                "content": f"Error during processing: {exc}",
            })
        except (ConnectionResetError, OSError):
            log.warning("Could not send error to client â€” connection already closed")
        return

    for task in stage_hint_tasks.values():
        if not task.done():
            task.cancel()

    if tool_count > 0:
        await websocket.send_json({
            "type": "progress",
            "percent": 100,
            "stage": f"Complete â€” {tool_count} step{'s' if tool_count != 1 else ''}",
            "tool": "",
            "tool_index": tool_count,
        })

    state_messages: list = []
    try:
        state = await graph.aget_state(config)
        if state and state.values:
            state_messages = state.values.get("messages", [])
            final_text = get_last_visible_ai_text(state_messages)
    except Exception as exc:
        log.debug("Could not reload final web turn state: %s", exc)

    erd_turn_status = extract_erd_turn_status(state_messages) if state_messages else None

    if final_text:
        content_to_send = final_text
        truncated = False
        if isinstance(content_to_send, str) and len(content_to_send) > _MAX_WS_CONTENT_CHARS:
            content_to_send = (
                content_to_send[:_MAX_WS_CONTENT_CHARS]
                + "\n\n... (truncated â€” full output saved to disk)"
            )
            truncated = True
        await websocket.send_json({
            "type": "assistant",
            "content": content_to_send,
            **({"truncated": True} if truncated else {}),
        })
    else:
        await websocket.send_json({
            "type": "assistant",
            "content": "I didn't get a response. Please try again.",
        })

    if erd_turn_status and erd_turn_status.complete and erd_turn_status.enriched_er_diagram_path:
        try:
            er_content = Path(erd_turn_status.enriched_er_diagram_path).read_text(
                encoding="utf-8"
            ).strip()
            if er_content:
                await websocket.send_json({
                    "type": "er_diagram",
                    "content": er_content,
                })
        except Exception as exc:
            log.warning("Could not read ER diagram: %s", exc)


_stream_turn = _stream_turn_guarded


def run(
    host: str = "0.0.0.0",
    port: int = 8501,
    mcp_url: Optional[str] = None,
    connector_mcp_url: Optional[str] = None,
) -> None:
    """Start the web UI server."""
    import uvicorn

    global _WEB_MCP_URL_OVERRIDE, _WEB_CONNECTOR_MCP_URL_OVERRIDE

    # Load .env
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
    except ImportError:
        pass

    _WEB_MCP_URL_OVERRIDE = mcp_url.strip() if isinstance(mcp_url, str) and mcp_url.strip() else None
    _WEB_CONNECTOR_MCP_URL_OVERRIDE = (
        connector_mcp_url.strip()
        if isinstance(connector_mcp_url, str) and connector_mcp_url.strip()
        else None
    )

    default_mcp_url = _default_web_mcp_url()
    default_connector_url = _default_web_connector_url(default_mcp_url)

    log.info(
        "Web UI MCP defaults: file=%s connector=%s",
        default_mcp_url,
        default_connector_url,
    )

    print(f"\n  Data Profiler UI -> http://localhost:{port}\n")

    # Force SelectorEventLoop on Windows — uvicorn may override the policy
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Let uvicorn choose the event loop implementation for broad compatibility.
    uvicorn.run(app, host=host, port=port, log_level="info")
