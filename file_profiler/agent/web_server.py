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
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Set event loop policy early before any other imports create a loop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from file_profiler.config.env import MAX_UPLOAD_SIZE_MB, OUTPUT_DIR, UPLOAD_DIR
from file_profiler.config.database import get_checkpointer, get_pool, close_pool
from file_profiler.agent.chatbot import CHATBOT_SYSTEM_PROMPT, _trim_messages
from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.progress import (
    TOOL_WEIGHTS,
    DEFAULT_TOOL_WEIGHT,
    _extract_summary,
    _get_stage_hints,
)
from file_profiler.agent.state import AgentState

log = logging.getLogger(__name__)

# Max chars for payloads sent over WebSocket to prevent browser memory issues
_MAX_WS_CONTENT_CHARS = 50_000
_MAX_WS_SUMMARY_CHARS = 500

FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"


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

    if tool_name == "profile_file" and isinstance(data, list):
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

    if tool_name == "profile_file" and isinstance(data, dict):
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

    if tool_name == "profile_directory" and isinstance(data, list):
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

    if tool_name == "visualize_profile" and isinstance(data, dict):
        charts = data.get("charts", [])
        if charts:
            return {
                "kind": "charts",
                "charts": charts,
                "table_name": data.get("table_name", ""),
            }

    if tool_name == "detect_relationships" and isinstance(data, dict):
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

app = FastAPI(title="Data Profiler UI")

# API key auth middleware — no-op when PROFILER_API_KEY is not set
from file_profiler.auth.api_key import APIKeyMiddleware
app.add_middleware(APIKeyMiddleware)


@app.get("/health")
async def health():
    """Health check endpoint."""
    return JSONResponse({"status": "ok"})


@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint.

    Returns metrics in Prometheus text exposition format.
    Returns 404 if prometheus_client is not installed.
    """
    from file_profiler.utils.metrics import METRICS_AVAILABLE
    if not METRICS_AVAILABLE:
        return JSONResponse(
            {"error": "prometheus_client not installed"},
            status_code=404,
        )
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from starlette.responses import Response
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


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


# Singleton checkpointer — created on first use via startup event
_checkpointer = None


@app.on_event("startup")
async def _startup_event():
    """Initialize persistent checkpointer on server start."""
    global _checkpointer
    _checkpointer = await get_checkpointer()
    log.info("Checkpointer initialized: %s", type(_checkpointer).__name__)


@app.on_event("shutdown")
async def _shutdown_event():
    """Clean up resources on server shutdown."""
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

# Serve CSS/JS as static files
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# Serve generated chart images
_CHARTS_DIR = OUTPUT_DIR / "charts"
_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
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
    ".csv", ".tsv", ".parquet", ".json", ".jsonl", ".ndjson",
    ".xlsx", ".xls", ".gz", ".zip",
    ".duckdb", ".db", ".sqlite", ".sqlite3",
}


@app.post("/api/upload")
async def upload_file(file: UploadFile):
    """Accept a file via multipart upload and save to the upload directory."""
    import uuid

    name = file.filename or "upload"
    ext = Path(name).suffix.lower()

    if ext not in _SUPPORTED_EXTENSIONS:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unsupported file type: {ext}"},
        )

    content = await file.read()
    size_mb = len(content) / (1024 * 1024)
    if size_mb > MAX_UPLOAD_SIZE_MB:
        return JSONResponse(
            status_code=413,
            content={"error": f"File too large ({size_mb:.1f} MB, max {MAX_UPLOAD_SIZE_MB} MB)"},
        )

    dest_dir = UPLOAD_DIR / uuid.uuid4().hex[:12]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / name
    dest.write_bytes(content)

    return {
        "server_path": str(dest),
        "file_name": name,
        "size_bytes": len(content),
    }


# ── MCP client cache ──────────────────────────────────────
# Reuse the MCP client across WebSocket sessions to avoid reconnection overhead.
# Entries are (client, last_used_timestamp) tuples with TTL-based eviction.
import time as _time

_MCP_CLIENT_TTL_SECONDS = 3600  # 1 hour

_mcp_client_cache: dict[str, tuple] = {}  # mcp_url → (client, last_used_ts)


# ── Graph builder ─────────────────────────────────────────

async def _build_graph(
    mcp_url: str = "http://localhost:8080/sse",
    provider: Optional[str] = None,
    model: Optional[str] = None,
):
    """Build the LangGraph chat graph connected to both MCP servers.

    Returns (compiled_graph, tool_count) or raises on failure.
    Reuses the MCP client if the URL hasn't changed.
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from file_profiler.agent.graph import _derive_connector_url

    connector_url = _derive_connector_url(mcp_url)

    transport = "sse"
    if "/mcp" in mcp_url or mcp_url.endswith("/mcp"):
        transport = "streamable_http"

    file_profiler_cfg = {
        "url": mcp_url,
        "transport": transport,
        "timeout": 60,
        "sse_read_timeout": 3600,
    }
    connector_cfg = {
        "url": connector_url,
        "transport": transport,
        "timeout": 60,
        "sse_read_timeout": 3600,
    }

    def _make_client(include_connector: bool = True):
        servers = {"file-profiler": file_profiler_cfg}
        if include_connector:
            servers["data-connector"] = connector_cfg
        return MultiServerMCPClient(servers)

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

    llm = get_llm_with_fallback(provider=provider, model=model)
    llm_with_tools = llm.bind_tools(tools)

    async def agent_node(state: AgentState):
        messages = state["messages"]
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [SystemMessage(content=CHATBOT_SYSTEM_PROMPT)] + list(messages)
        messages = _trim_messages(messages)
        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", ToolNode(tools, handle_tool_errors=True))
    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", tools_condition)
    builder.add_edge("tools", "agent")

    graph = builder.compile(checkpointer=_checkpointer)

    return graph, len(tools)


# ── WebSocket chat endpoint ───────────────────────────────

# ── Rate limiting ─────────────────────────────────────────
_MAX_CONCURRENT_SESSIONS = 10
_active_sessions: int = 0
_MIN_MESSAGE_INTERVAL_SECONDS = 1.0  # min gap between user messages


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
    session_id = None  # set from client's config message
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
                session_id = data.get("session_id") or session_id or "web-session-1"

                # (Re-)connect to MCP and build graph
                mcp_url = data.get("mcp_url", "http://localhost:8080/sse")
                provider = data.get("provider") or None
                try:
                    graph, tool_count = await _build_graph(
                        mcp_url=mcp_url, provider=provider,
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
                }

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
                                tool_id = tc.get("id", tc["name"])
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
                            tool_id = msg.tool_call_id
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

def run(host: str = "0.0.0.0", port: int = 8501) -> None:
    """Start the web UI server."""
    import uvicorn

    # Load .env
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
    except ImportError:
        pass

    print(f"\n  Data Profiler UI -> http://localhost:{port}\n")

    # Force SelectorEventLoop on Windows — uvicorn may override the policy
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    uvicorn.run(app, host=host, port=port, log_level="info", loop="none")
