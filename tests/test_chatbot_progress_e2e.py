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
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# Ensure data dir is set
os.environ.setdefault("PROFILER_DATA_DIR", "C:/Projects/profiler/Profiler/data")

MCP_PORT = 8099  # use a non-default port to avoid conflicts
MCP_URL = f"http://localhost:{MCP_PORT}/sse"


async def test_single_turn():
    """Run a single chatbot turn with progress tracking."""
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import END, START, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition
    from langchain_mcp_adapters.client import MultiServerMCPClient

    from file_profiler.agent.chatbot import CHATBOT_SYSTEM_PROMPT
    from file_profiler.agent.llm_factory import get_llm_with_fallback
    from file_profiler.agent.progress import ProgressTracker
    from file_profiler.agent.state import AgentState

    # Connect to MCP
    print("\n[1] Connecting to MCP server...")
    client = MultiServerMCPClient({
        "file-profiler": {
            "url": MCP_URL,
            "transport": "sse",
            "timeout": 30,
            "sse_read_timeout": 300,
        }
    })

    tools = await client.get_tools()
    print(f"    Loaded {len(tools)} tools: {[t.name for t in tools]}")

    # Build graph
    llm = get_llm_with_fallback()
    llm_with_tools = llm.bind_tools(tools)

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
    config = {"configurable": {"thread_id": "test-progress-1"}}

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

    async for event in graph.astream(inputs, config=config, stream_mode="updates"):
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


def start_mcp_server():
    """Start the MCP server as a subprocess."""
    env = os.environ.copy()
    env["PROFILER_DATA_DIR"] = "C:/Projects/profiler/Profiler/data"

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "file_profiler",
            "--transport", "sse",
            "--port", str(MCP_PORT),
        ],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Give it time to start
    time.sleep(3)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        raise RuntimeError(f"MCP server failed to start: {stderr}")
    return proc


if __name__ == "__main__":
    print("Starting MCP server on port", MCP_PORT)
    server = start_mcp_server()
    try:
        asyncio.run(test_single_turn())
    finally:
        server.terminate()
        server.wait(timeout=5)
        print("\nMCP server stopped.")
