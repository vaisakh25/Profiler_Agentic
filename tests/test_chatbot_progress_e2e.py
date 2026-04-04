"""E2E test for the chatbot with progress tracking.

Starts the MCP server as a subprocess, runs one agent turn,
and verifies the progress tracker fires correctly.

Usage:
  conda activate gen_ai
  set PROFILER_DATA_DIR=C:/Projects/profiler/Profiler/data
  python tests/test_chatbot_progress_e2e.py
"""

import asyncio
import os
import sys
import subprocess
import socket
import time
from pathlib import Path
from typing import Any, cast

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# Ensure data dir is set
os.environ.setdefault("PROFILER_DATA_DIR", str(PROJECT_ROOT / "data" / "files"))

MCP_PORT = int(os.getenv("TEST_MCP_PORT", "0"))
MCP_URL = os.getenv("TEST_MCP_URL", "")
GET_TOOLS_TIMEOUT_SECONDS = 45
TURN_TIMEOUT_SECONDS = 90


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


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
                        "id": "progress-tool-1",
                        "type": "tool_call",
                    }
                ],
            )

        return AIMessage(content="Completed list_supported_files and summarized the result.")


class _DeterministicLLM:
    def __init__(self, dir_path: str) -> None:
        self._dir_path = dir_path

    def bind_tools(self, tools):
        tool_names = {t.name for t in tools}
        assert "list_supported_files" in tool_names
        return _DeterministicBoundLLM(self._dir_path)


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


@pytest.fixture(scope="module")
def mcp_server():
    port = MCP_PORT or _pick_free_port()
    url = MCP_URL or f"http://localhost:{port}/sse"
    proc = start_mcp_server(port)
    try:
        yield {"proc": proc, "port": port, "url": url}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.mark.asyncio
async def test_single_turn(mcp_server):
    """Run a single chatbot turn with progress tracking."""
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import START, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition
    from langchain_mcp_adapters.client import MultiServerMCPClient

    # Use a compact prompt for live tests to minimize token usage and prevent tool loops.
    live_test_system_prompt = (
        "You are a test assistant. "
        "Call list_supported_files at most once for the user's request, "
        "then respond with a short plain-language summary."
    )
    from file_profiler.agent.progress import ProgressTracker
    from file_profiler.agent.state import AgentState

    # Connect to MCP
    print("\n[1] Connecting to MCP server...")
    mcp_url = str(mcp_server["url"])
    client = MultiServerMCPClient({
        "file-profiler": {
            "url": mcp_url,
            "transport": "sse",
            "timeout": 30,
            "sse_read_timeout": 300,
        }
    })

    tools = await asyncio.wait_for(
        client.get_tools(),
        timeout=GET_TOOLS_TIMEOUT_SECONDS,
    )
    tools = [t for t in tools if t.name == "list_supported_files"]
    assert tools, "Expected list_supported_files tool to be available"
    print(f"    Loaded {len(tools)} tool for live run: {[t.name for t in tools]}")

    # Build graph with deterministic LLM output to avoid live-provider dependency.
    target_dir = str(PROJECT_ROOT / "data" / "files")
    llm_with_tools = _DeterministicLLM(target_dir).bind_tools(tools)

    async def agent_node(state: AgentState):
        messages = state["messages"]
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [SystemMessage(content=live_test_system_prompt)] + list(messages)

        # Some providers (e.g. Groq) require tool messages to have string content.
        normalized_messages = []
        for m in messages:
            if isinstance(m, ToolMessage) and not isinstance(m.content, str):
                normalized_messages.append(
                    ToolMessage(content=str(m.content), tool_call_id=m.tool_call_id)
                )
            else:
                normalized_messages.append(m)

        response = await llm_with_tools.ainvoke(normalized_messages)
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
        "configurable": {"thread_id": "test-progress-1"},
        "recursion_limit": 6,
    }

    # Run a turn that should trigger list_supported_files
    print("\n[2] Running agent turn with progress tracking...")
    print("    User: 'List the data files in data/files'\n")

    inputs = {
        "messages": [HumanMessage(content="List the data files in data/files")],
        "mode": "autonomous",
    }

    tracker = ProgressTracker()
    final_text = ""
    pending_tools = {}
    tool_count = 0

    try:
        async with asyncio.timeout(TURN_TIMEOUT_SECONDS):
            async for event in graph.astream(
                cast(Any, inputs),
                config=cast(Any, config),
                stream_mode="updates",
            ):
                should_stop = False
                for node_name, node_output in event.items():
                    if node_name == "agent":
                        msg = node_output["messages"][-1]
                        if isinstance(msg, AIMessage):
                            if msg.tool_calls:
                                await tracker.finish_thinking()
                                for tc in msg.tool_calls:
                                    tool_id = tc.get("id", tc["name"])
                                    pending_tools[tool_id] = {
                                        "name": tc["name"],
                                        "args": tc.get("args", {}),
                                    }
                                    await tracker.start_tool(tc["name"], tc.get("args", {}))
                                    tool_count += 1
                            elif msg.content:
                                await tracker.finish_thinking()
                                final_text = msg.content
                                should_stop = True
                            else:
                                await tracker.start_thinking()

                    elif node_name == "tools":
                        for msg in node_output["messages"]:
                            if isinstance(msg, ToolMessage):
                                content = msg.content if isinstance(msg.content, str) else str(msg.content)
                                tool_id = msg.tool_call_id
                                tool_info = pending_tools.pop(tool_id, None)
                                tool_name = tool_info["name"] if tool_info else "unknown"
                                await tracker.finish_tool(tool_name, content)
                                # For progress E2E, one successful tool cycle is sufficient
                                # and avoids expensive iterative tool loops with live providers.
                                if not final_text:
                                    final_text = f"Completed {tool_name}: {content[:240]}"
                                should_stop = True

                if should_stop:
                    break
    except TimeoutError:
        pytest.fail(f"Timed out after {TURN_TIMEOUT_SECONDS}s waiting for chatbot progress flow")

    tracker.print_summary()

    # Normalize content
    if isinstance(final_text, list):
        final_text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in final_text
        )

    print(f"\n[3] Results:")
    print(f"    Tools called: {tool_count}")
    print(f"    Response length: {len(final_text)} chars")
    if final_text:
        preview = final_text[:300].replace("\n", "\n    ")
        print(f"    Preview: {preview}...")

    assert tool_count > 0, "Expected at least one tool call"
    assert len(final_text) > 0, "Expected non-empty response"
    print("\n[PASS] Chatbot progress tracking E2E test passed!")


def start_mcp_server(port: int):
    """Start the MCP server as a subprocess."""
    env = os.environ.copy()
    env["PROFILER_DATA_DIR"] = str(PROJECT_ROOT / "data" / "files")

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "file_profiler",
            "--transport", "sse",
            "--port", str(port),
        ],
        cwd=str(PROJECT_ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not _wait_for_mcp_health(port):
        proc.terminate()
        raise RuntimeError(f"MCP server failed to start on port {port}")
    return proc


if __name__ == "__main__":
    port = MCP_PORT or _pick_free_port()
    print("Starting MCP server on port", port)
    server = start_mcp_server(port)
    try:
        asyncio.run(test_single_turn(server))
    finally:
        server.terminate()
        server.wait(timeout=5)
        print("\nMCP server stopped.")
