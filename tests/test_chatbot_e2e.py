"""Deterministic end-to-end tests for the chatbot agent with a live MCP server."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MCP_PORT = int(os.getenv("TEST_MCP_PORT", "8098"))
MCP_URL = os.getenv("TEST_MCP_URL", f"http://localhost:{MCP_PORT}/sse")
GET_TOOLS_TIMEOUT_SECONDS = 45
TURN_TIMEOUT_SECONDS = 90


def _wait_for_mcp_health(port: int, timeout_seconds: int = 20) -> bool:
    import urllib.request

    deadline = time.time() + timeout_seconds
    url = f"http://localhost:{port}/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.5) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.5)
    return False


def _start_mcp_server() -> subprocess.Popen:
    env = os.environ.copy()
    env.setdefault("PROFILER_DATA_DIR", str(PROJECT_ROOT / "data" / "files"))
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "file_profiler",
            "--transport",
            "sse",
            "--host",
            "0.0.0.0",
            "--port",
            str(MCP_PORT),
        ],
        cwd=str(PROJECT_ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not _wait_for_mcp_health(MCP_PORT):
        proc.terminate()
        raise RuntimeError(f"MCP server failed to become healthy on port {MCP_PORT}")
    return proc


class _DeterministicBoundLLM:
    def __init__(self, dir_path: str) -> None:
        self._dir_path = dir_path
        self._issued_tool = False

    async def ainvoke(self, messages):
        from langchain_core.messages import AIMessage, ToolMessage

        saw_tool_message = any(isinstance(m, ToolMessage) for m in messages)
        if not saw_tool_message and not self._issued_tool:
            self._issued_tool = True
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "list_supported_files",
                        "args": {"dir_path": self._dir_path},
                        "id": "tool-call-1",
                        "type": "tool_call",
                    }
                ],
            )

        return AIMessage(content="Completed one profiling reconnaissance turn.")


class _DeterministicLLM:
    def __init__(self, dir_path: str) -> None:
        self._dir_path = dir_path

    def bind_tools(self, tools):
        tool_names = {t.name for t in tools}
        assert "list_supported_files" in tool_names
        return _DeterministicBoundLLM(self._dir_path)


@pytest.fixture(scope="module")
def mcp_server():
    proc = _start_mcp_server()
    try:
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.mark.asyncio
async def test_connection_and_tools(mcp_server):
    from langchain_mcp_adapters.client import MultiServerMCPClient

    client = MultiServerMCPClient(
        {
            "file-profiler": {
                "url": MCP_URL,
                "transport": "sse",
            }
        }
    )
    tools = await asyncio.wait_for(
        client.get_tools(),
        timeout=GET_TOOLS_TIMEOUT_SECONDS,
    )
    assert len(tools) >= 4, f"Expected at least 4 tools, got {len(tools)}"


@pytest.mark.asyncio
async def test_single_turn(mcp_server):
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import START, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition

    from file_profiler.agent.chatbot import CHATBOT_SYSTEM_PROMPT
    from file_profiler.agent.state import AgentState

    client = MultiServerMCPClient(
        {
            "file-profiler": {
                "url": MCP_URL,
                "transport": "sse",
                "timeout": 30,
                "sse_read_timeout": 600,
            }
        }
    )
    tools = await asyncio.wait_for(
        client.get_tools(),
        timeout=GET_TOOLS_TIMEOUT_SECONDS,
    )

    target_dir = str(PROJECT_ROOT / "data" / "files")
    llm_with_tools = _DeterministicLLM(target_dir).bind_tools(tools)

    async def agent_node(state: AgentState):
        messages = state["messages"]
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [SystemMessage(content=CHATBOT_SYSTEM_PROMPT)] + list(messages)
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
    config = {
        "configurable": {"thread_id": "test-session"},
        "recursion_limit": 6,
    }

    tool_calls_seen = 0
    final_text = ""

    stream_input = cast(
        Any,
        {
            "messages": [HumanMessage(content="List files available for profiling")],
            "mode": "autonomous",
        },
    )
    try:
        async with asyncio.timeout(TURN_TIMEOUT_SECONDS):
            async for event in graph.astream(
                stream_input,
                config=cast(Any, config),
                stream_mode="updates",
            ):
                for node_name, node_output in event.items():
                    if node_name != "agent":
                        continue

                    msg = node_output["messages"][-1]
                    if isinstance(msg, AIMessage):
                        if msg.tool_calls:
                            tool_calls_seen += len(msg.tool_calls)
                        elif msg.content:
                            final_text = msg.content

                if final_text:
                    break
    except TimeoutError:
        pytest.fail(f"Timed out after {TURN_TIMEOUT_SECONDS}s waiting for chatbot turn")

    if isinstance(final_text, list):
        final_text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in final_text
        )

    assert tool_calls_seen > 0, "Agent should have made at least one tool call"
    assert len(final_text) > 0, "Expected non-empty assistant response"
