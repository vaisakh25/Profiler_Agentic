"""Simulate what the web server does: init checkpointer, build graph, send a message."""
import asyncio
import sys
import traceback
import logging

logging.basicConfig(level=logging.DEBUG)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def test():
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


asyncio.run(test())
