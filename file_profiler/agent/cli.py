"""CLI entry point for the LangGraph profiling agent.

Usage:
  # Start MCP server first:
  python -m file_profiler --transport sse --port 8080

  # Then run the agent:
  python -m file_profiler.agent --data-path ./data/files
  python -m file_profiler.agent --data-path ./data/files --mode interactive
  python -m file_profiler.agent --data-path ./data/files --provider openai
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from file_profiler.agent.graph import create_agent

log = logging.getLogger(__name__)


async def run_agent(
    data_path: str,
    mode: str = "autonomous",
    mcp_url: str = "http://localhost:8080/sse",
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Run the profiling agent and return the final report.

    Args:
        data_path: Path to a data directory or file to profile.
        mode:      ``"autonomous"`` or ``"interactive"``.
        mcp_url:   URL of the running MCP server.
        provider:  LLM provider override.
        model:     Model name override.

    Returns:
        The final AI message content (the profiling report).
    """
    graph, client = await create_agent(
        mcp_server_url=mcp_url,
        provider=provider,
        model=model,
        mode=mode,
    )

    try:
        initial_message = (
            f"Profile the data at: {data_path}\n\n"
            f"Follow the standard workflow: discover files, profile them, "
            f"detect relationships, check quality, and produce a comprehensive report."
        )

        config = {"configurable": {"thread_id": "profiler-session-1"}}

        if mode == "autonomous":
            return await _run_autonomous(graph, initial_message, config)
        else:
            return await _run_interactive(graph, initial_message, config)
    finally:
        pass  # client sessions are auto-managed per tool call


async def _run_autonomous(graph, initial_message: str, config: dict) -> str:
    """Run the agent without interruptions."""
    print("\n--- Data Profiling Agent (Autonomous Mode) ---\n")
    print(f"Task: {initial_message}\n")

    result = await graph.ainvoke(
        {"messages": [HumanMessage(content=initial_message)], "mode": "autonomous"},
        config=config,
    )

    # Extract the final AI message
    final_message = ""
    for msg in reversed(result["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            final_message = msg.content
            break

    print("\n--- Profiling Report ---\n")
    print(final_message)
    return final_message


async def _run_interactive(graph, initial_message: str, config: dict) -> str:
    """Run the agent with human-in-the-loop approval for tool calls."""
    from langgraph.checkpoint.memory import MemorySaver

    # Rebuild graph with checkpointer for interrupt/resume
    graph_with_cp, client = await create_agent(
        mcp_server_url=config.get("mcp_url", "http://localhost:8080/sse"),
        mode="interactive",
    )

    # Use in-memory checkpointer
    checkpointer = MemorySaver()

    # Re-import and rebuild with checkpointer
    from file_profiler.agent.graph import create_agent as _create
    from file_profiler.agent.state import AgentState
    from langgraph.graph import END, START, StateGraph
    from langgraph.prebuilt import ToolNode, tools_condition
    from file_profiler.agent.llm_factory import get_llm_with_fallback
    from langchain_core.messages import SystemMessage
    from file_profiler.agent.graph import SYSTEM_PROMPT

    tools = client.get_tools()
    llm = get_llm_with_fallback()
    llm_with_tools = llm.bind_tools(tools)

    async def agent_node(state: AgentState):
        messages = state["messages"]
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(messages)
        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", ToolNode(tools))
    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", tools_condition)
    builder.add_edge("tools", "agent")

    compiled = builder.compile(
        checkpointer=checkpointer,
        interrupt_before=["tools"],
    )

    print("\n--- Data Profiling Agent (Interactive Mode) ---\n")
    print(f"Task: {initial_message}\n")
    print("You will be asked to approve each tool call.\n")

    state = {"messages": [HumanMessage(content=initial_message)], "mode": "interactive"}

    while True:
        result = await compiled.ainvoke(state, config=config)

        # Check if we hit an interrupt (pending tool calls)
        snapshot = await compiled.aget_state(config)

        if snapshot.next:
            # There are pending tool calls — show them to user
            last_msg = result["messages"][-1]
            if isinstance(last_msg, AIMessage) and last_msg.tool_calls:
                print("\n--- Pending Tool Calls ---")
                for tc in last_msg.tool_calls:
                    print(f"  Tool: {tc['name']}")
                    print(f"  Args: {tc['args']}")
                    print()

                approval = input("Approve these tool calls? (y/n/q): ").strip().lower()

                if approval == "q":
                    print("Agent stopped by user.")
                    break
                elif approval == "y":
                    # Resume execution
                    result = await compiled.ainvoke(None, config=config)
                    state = result
                    continue
                else:
                    # Skip — inject a message saying tools were rejected
                    from langchain_core.messages import HumanMessage as HM
                    state = {
                        "messages": [
                            HM(content="The user rejected the tool calls. "
                               "Please adjust your approach or provide the "
                               "report with the information available so far.")
                        ]
                    }
                    continue
        else:
            # No more tool calls — agent is done
            break

    # Extract final message
    final_message = ""
    for msg in reversed(result["messages"]):
        if isinstance(msg, AIMessage) and msg.content:
            final_message = msg.content
            break

    print("\n--- Profiling Report ---\n")
    print(final_message)

    await client.__aexit__(None, None, None)
    return final_message


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="LangGraph Data Profiling Agent",
        epilog=(
            "Start the MCP server first:\n"
            "  python -m file_profiler --transport sse --port 8080\n\n"
            "Then run the agent:\n"
            "  python -m file_profiler.agent --data-path ./data/files"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--data-path",
        required=True,
        help="Path to a data directory or file to profile.",
    )
    parser.add_argument(
        "--mode",
        choices=["autonomous", "interactive"],
        default="autonomous",
        help="Execution mode (default: autonomous).",
    )
    parser.add_argument(
        "--mcp-url",
        default="http://localhost:8080/sse",
        help="URL of the MCP server SSE endpoint (default: http://localhost:8080/sse).",
    )
    parser.add_argument(
        "--provider",
        choices=["anthropic", "openai", "google"],
        default=None,
        help="LLM provider (default: from LLM_PROVIDER env var or anthropic).",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="LLM model name override (default: from LLM_MODEL env var).",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )

    try:
        asyncio.run(
            run_agent(
                data_path=args.data_path,
                mode=args.mode,
                mcp_url=args.mcp_url,
                provider=args.provider,
                model=args.model,
            )
        )
    except KeyboardInterrupt:
        print("\nAgent interrupted.")
        sys.exit(0)
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
