"""Simulate what the web server does: init checkpointer, build graph, send a message."""
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import traceback
import logging
import time

import pytest

logging.basicConfig(level=logging.DEBUG)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MCP_PORT = 8097
MCP_URL = f"http://localhost:{MCP_PORT}/sse"
CHECKPOINTER_TIMEOUT_SECONDS = 25
BUILD_GRAPH_TIMEOUT_SECONDS = 90


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
    env["PROFILER_DATA_DIR"] = str(PROJECT_ROOT / "data" / "files")

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "file_profiler",
            "--transport",
            "sse",
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
        raise RuntimeError("MCP server failed to start within timeout")
    return proc


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


async def _run_manual_smoke() -> None:
    print("=== Step 1: Init checkpointer ===")
    from file_profiler.config.database import get_checkpointer
    try:
        cp = await get_checkpointer()
        print(f"Checkpointer: {type(cp).__name__}")
    except Exception as e:
        print(f"Checkpointer error: {e}")
        traceback.print_exc()
        return

    print("\n=== Step 2: Build graph (MCP connect) ===")
    from file_profiler.agent.web_server import _build_graph
    try:
        graph, tool_count = await _build_graph(mcp_url="http://localhost:8080/sse")
        print(f"Graph built with {tool_count} tools")
    except Exception as e:
        print(f"Build graph error: {e}")
        traceback.print_exc()
        return

    print("\n=== Step 3: Send test message ===")
    from langchain_core.messages import HumanMessage
    config = {"configurable": {"thread_id": "test-direct"}}
    inputs = {
        "messages": [HumanMessage(content="List the supported files in C:/Projects/profiler/Profiler_Agentic_LLM/Profiler_Agentic/data/files/wwi_files")],
        "mode": "autonomous",
    }

    async for event in graph.astream(inputs, config=config, stream_mode="updates"):
        for node_name, node_output in event.items():
            msgs = node_output.get("messages", [])
            for m in msgs:
                print(f"  [{node_name}] {type(m).__name__}: {str(m.content)[:200]}")

    print("\nDone!")


@pytest.mark.asyncio
async def test_build_graph_smoke(mcp_server, monkeypatch) -> None:
    """Smoke test: checkpointer + graph initialization for web-server flow."""
    from file_profiler.config.database import get_checkpointer
    from file_profiler.agent import web_server

    class _DummyBoundLLM:
        async def ainvoke(self, messages):
            from langchain_core.messages import AIMessage

            return AIMessage(content="ok")

    class _DummyLLM:
        def bind_tools(self, tools):
            return _DummyBoundLLM()

    monkeypatch.setattr(
        web_server,
        "get_llm_with_fallback",
        lambda provider=None, model=None: _DummyLLM(),
    )

    cp = await asyncio.wait_for(
        get_checkpointer(),
        timeout=CHECKPOINTER_TIMEOUT_SECONDS,
    )
    assert cp is not None

    graph, tool_count = await asyncio.wait_for(
        web_server._build_graph(mcp_url=MCP_URL),
        timeout=BUILD_GRAPH_TIMEOUT_SECONDS,
    )
    assert graph is not None
    assert tool_count > 0


if __name__ == "__main__":
    asyncio.run(_run_manual_smoke())
