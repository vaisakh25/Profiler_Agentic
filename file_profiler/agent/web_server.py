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
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

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

FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"

app = FastAPI(title="Data Profiler UI")

# Serve CSS/JS as static files
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/")
async def index():
    """Serve the main HTML page."""
    return FileResponse(str(FRONTEND_DIR / "index.html"))


# ── Graph builder ─────────────────────────────────────────

async def _build_graph(
    mcp_url: str = "http://localhost:8080/sse",
    provider: Optional[str] = None,
    model: Optional[str] = None,
):
    """Build the LangGraph chat graph connected to the MCP server.

    Returns (compiled_graph, tool_count) or raises on failure.
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient

    transport = "sse"
    if "/mcp" in mcp_url or mcp_url.endswith("/mcp"):
        transport = "streamable_http"

    client = MultiServerMCPClient(
        {
            "file-profiler": {
                "url": mcp_url,
                "transport": transport,
                "timeout": 30,
                "sse_read_timeout": 600,
            }
        }
    )

    tools = await client.get_tools()
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

    checkpointer = MemorySaver()
    graph = builder.compile(checkpointer=checkpointer)

    return graph, len(tools)


# ── WebSocket chat endpoint ───────────────────────────────

@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """Handle a single chat session over WebSocket."""
    await websocket.accept()

    graph = None
    session_id = "web-session-1"

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)

            if data["type"] == "config":
                # (Re-)connect to MCP and build graph
                mcp_url = data.get("mcp_url", "http://localhost:8080/sse")
                provider = data.get("provider") or None
                try:
                    graph, tool_count = await _build_graph(
                        mcp_url=mcp_url, provider=provider,
                    )
                    await websocket.send_json({
                        "type": "connected",
                        "tools": tool_count,
                    })
                except Exception as exc:
                    await websocket.send_json({
                        "type": "error",
                        "content": f"Could not connect to MCP server: {exc}",
                    })
                continue

            if data["type"] == "message":
                if graph is None:
                    await websocket.send_json({
                        "type": "error",
                        "content": "Not connected to MCP server. Refresh the page.",
                    })
                    continue

                user_text = data.get("content", "").strip()
                if not user_text:
                    continue

                config = {"configurable": {"thread_id": session_id}}
                inputs = {
                    "messages": [HumanMessage(content=user_text)],
                    "mode": "autonomous",
                }

                await _stream_turn(websocket, graph, inputs, config)

    except WebSocketDisconnect:
        log.info("WebSocket client disconnected")
    except Exception as exc:
        log.exception("WebSocket error: %s", exc)
        try:
            await websocket.send_json({
                "type": "error",
                "content": f"Server error: {exc}",
            })
        except Exception:
            pass


_MILESTONES = [10, 20, 40, 67, 75, 80, 90, 95, 100]

_MILESTONE_MESSAGES = {
    10:  "Getting started...",
    20:  "Making progress...",
    40:  "Almost halfway there...",
    67:  "Two-thirds done...",
    75:  "Three-quarters complete...",
    80:  "Home stretch...",
    90:  "Nearly there...",
    95:  "Finishing up...",
    100: "All done!",
}


async def _stream_turn(
    websocket: WebSocket,
    graph,
    inputs: dict,
    config: dict,
) -> None:
    """Stream one conversational turn, sending events over WebSocket."""
    pending_tools: dict[str, str] = {}  # tool_call_id → tool_name
    final_text = ""

    # Progress tracking state
    completed_weight = 0.0
    total_weight = 0.0
    tool_index = 0
    stage_hint_tasks: dict[str, asyncio.Task] = {}  # tool_id → hint sender task
    milestones_sent: set[int] = set()

    async def _check_milestones(pct: float):
        """Send milestone status updates when thresholds are crossed."""
        for m in _MILESTONES:
            if m not in milestones_sent and pct >= m:
                milestones_sent.add(m)
                await websocket.send_json({
                    "type": "milestone",
                    "percent": m,
                    "message": _MILESTONE_MESSAGES[m],
                })

    async def _send_stage_hints(tool_id: str, tool_name: str):
        """Periodically send rotating stage hints for a running tool."""
        hints = _get_stage_hints(tool_name)
        idx = 0
        try:
            while True:
                hint = hints[idx] if hints else "Processing"
                pct = (completed_weight / total_weight * 100) if total_weight > 0 else 0
                await websocket.send_json({
                    "type": "progress",
                    "percent": round(min(pct, 99), 1),
                    "stage": hint,
                    "tool": tool_name,
                    "tool_index": tool_index,
                })
                await asyncio.sleep(5)
                if idx < len(hints) - 1:
                    idx += 1
        except asyncio.CancelledError:
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
                                await _check_milestones(pct)

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

                            await websocket.send_json({
                                "type": "tool_result",
                                "tool": tool_name,
                                "tool_index": tool_index,
                                "percent": round(min(pct, 100), 1),
                                "summary": summary,
                                "success": not has_error,
                            })
                            await _check_milestones(pct)

    except Exception as exc:
        # Cancel any running hint tasks
        for task in stage_hint_tasks.values():
            if not task.done():
                task.cancel()
        await websocket.send_json({
            "type": "error",
            "content": f"Error during processing: {exc}",
        })
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
        await _check_milestones(100)

    # Normalise content (some providers return list of dicts)
    if isinstance(final_text, list):
        final_text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in final_text
        )

    if final_text:
        await websocket.send_json({
            "type": "assistant",
            "content": final_text,
        })
    else:
        await websocket.send_json({
            "type": "assistant",
            "content": "I didn't get a response. Please try again.",
        })


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

    print(f"\n  Data Profiler UI → http://localhost:{port}\n")
    uvicorn.run(app, host=host, port=port, log_level="info")
