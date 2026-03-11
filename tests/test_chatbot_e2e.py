"""End-to-end test for the chatbot agent — runs a single turn against a live MCP server.

Prerequisites:
  - MCP server running: python -m file_profiler --transport sse --port 8080
  - ANTHROPIC_API_KEY set in environment

Usage:
  python tests/test_chatbot_e2e.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env from project root
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))


async def test_connection_and_tools():
    """Test 1: Verify MCP client connects and loads tools."""
    from langchain_mcp_adapters.client import MultiServerMCPClient

    client = MultiServerMCPClient({
        "file-profiler": {
            "url": "http://localhost:8080/sse",
            "transport": "sse",
        }
    })
    tools = await client.get_tools()
    print(f"[PASS] Connected to MCP server. Tools loaded: {[t.name for t in tools]}")
    assert len(tools) >= 4, f"Expected at least 4 tools, got {len(tools)}"


async def test_single_turn():
    """Test 2: Run a single agent turn with a real LLM."""
    from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
    from langgraph.checkpoint.memory import MemorySaver
    from langgraph.graph import START, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition
    from langchain_mcp_adapters.client import MultiServerMCPClient

    from file_profiler.agent.chatbot import CHATBOT_SYSTEM_PROMPT
    from file_profiler.agent.llm_factory import get_llm
    from file_profiler.agent.state import AgentState

    client = MultiServerMCPClient({
        "file-profiler": {
            "url": "http://localhost:8080/sse",
            "transport": "sse",
            "timeout": 30,
            "sse_read_timeout": 600,
        }
    })
    tools = await client.get_tools()

    llm = get_llm(provider="google", model="gemini-2.5-flash")
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
    config = {"configurable": {"thread_id": "test-session"}}

    data_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "test_small",
    )
    user_msg = f"My data sits in {data_path}. Profile it and show me the ER diagram."

    print(f"\n[TEST] Sending: {user_msg}")
    print("[TEST] Agent working (this may take a minute)...\n")

    tool_calls_seen = 0
    final_text = ""

    async for event in graph.astream(
        {"messages": [HumanMessage(content=user_msg)], "mode": "autonomous"},
        config=config,
        stream_mode="updates",
    ):
        for node_name, node_output in event.items():
            if node_name == "agent":
                msg = node_output["messages"][-1]
                if isinstance(msg, AIMessage):
                    if msg.tool_calls:
                        for tc in msg.tool_calls:
                            tool_calls_seen += 1
                            args_str = ", ".join(
                                f"{k}={v}" for k, v in tc["args"].items()
                            )
                            print(f"  [{tool_calls_seen}] Tool call: {tc['name']}({args_str})")
                    elif msg.content:
                        final_text = msg.content
            elif node_name == "tools":
                for msg in node_output["messages"]:
                    content = msg.content if isinstance(msg.content, str) else str(msg.content)
                    if "erDiagram" in content:
                        print(f"       -> Done (includes ER diagram)")
                    else:
                        print(f"       -> Done ({len(content)} chars)")

    print(f"\n[PASS] Agent made {tool_calls_seen} tool calls.")
    assert tool_calls_seen > 0, "Agent should have made at least one tool call"

    # Gemini may return content as a list of dicts — normalise to string
    if isinstance(final_text, list):
        final_text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in final_text
        )

    if final_text:
        print(f"\n{'='*60}")
        print("AGENT RESPONSE:")
        print(f"{'='*60}")
        print(final_text[:3000])
        if len(final_text) > 3000:
            print(f"\n... ({len(final_text) - 3000} more chars)")
        print(f"{'='*60}")

    has_er = "erDiagram" in final_text or "mermaid" in final_text.lower()
    if has_er:
        print("\n[PASS] Response contains ER diagram.")
    else:
        print("\n[WARN] Response may not contain ER diagram — check output above.")

    print("[PASS] Test complete.\n")


async def main():
    print("=" * 60)
    print("Chatbot E2E Test")
    print("=" * 60)

    print("\n--- Test 1: Connection & Tool Loading ---")
    await test_connection_and_tools()

    print("\n--- Test 2: Single Turn (LLM + Tools) ---")
    await test_single_turn()


if __name__ == "__main__":
    asyncio.run(main())
