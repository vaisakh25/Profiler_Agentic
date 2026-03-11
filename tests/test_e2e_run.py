"""Full E2E test: MCP server (already running) → Groq LLM → progress tracking.

Usage:
  conda activate gen_ai
  python tests/test_e2e_run.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

os.environ.setdefault("PROFILER_DATA_DIR", "C:/Projects/profiler/Profiler/data")

MCP_URL = "http://localhost:8098/sse"


async def run_e2e():
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import START, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition
    from langchain_mcp_adapters.client import MultiServerMCPClient

    from file_profiler.agent.chatbot import CHATBOT_SYSTEM_PROMPT
    from file_profiler.agent.llm_factory import get_llm_with_fallback
    from file_profiler.agent.progress import ProgressTracker
    from file_profiler.agent.state import AgentState

    # Step 1: Connect to MCP
    print("\n[1] Connecting to MCP server...")
    client = MultiServerMCPClient({
        "file-profiler": {
            "url": MCP_URL,
            "transport": "sse",
            "timeout": 30,
            "sse_read_timeout": 600,
        }
    })
    tools = await client.get_tools()
    print(f"    {len(tools)} tools: {[t.name for t in tools]}")

    # Step 2: Build LLM (picks up LLM_PROVIDER from .env)
    print("\n[2] Initializing LLM...")
    llm = get_llm_with_fallback()
    provider = os.getenv("LLM_PROVIDER", "?")
    print(f"    Provider: {provider}")
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
    config = {"configurable": {"thread_id": "e2e-test-1"}}

    # Step 3: Run agent turn
    print("\n[3] Running agent turn...")
    print("    User: 'List the data files in data/test_enrich and enrich the relationships'\n")

    inputs = {
        "messages": [HumanMessage(content="List the data files in data/test_enrich and then enrich the relationships for that folder")],
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
                            pending_tools[tool_id] = {"name": tc["name"], "args": tc.get("args", {})}
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

    print(f"\n[4] Results:")
    print(f"    Tools called: {tool_count}")
    print(f"    Response length: {len(final_text)} chars")
    if final_text:
        preview = final_text[:500].replace("\n", "\n    ")
        print(f"\n    Assistant:\n    {preview}")
        if len(final_text) > 500:
            print(f"\n    ... ({len(final_text) - 500} more chars)")

    assert tool_count > 0, "Expected at least one tool call"
    assert len(final_text) > 0, "Expected non-empty response"
    print("\n[PASS] Full E2E test passed!")


if __name__ == "__main__":
    asyncio.run(run_e2e())
