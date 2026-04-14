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

from langchain_core.messages import AIMessage, HumanMessage

from file_profiler.agent.erd_wait import get_last_visible_ai_text
from file_profiler.agent.graph import create_agent
from file_profiler.observability.langsmith import compact_text_output, trace_context, traceable

log = logging.getLogger(__name__)


async def run_agent(
    data_path: str,
    mode: str = "autonomous",
    mcp_url: str = "http://localhost:8080/sse",
    connector_mcp_url: str | None = None,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Run the profiling agent and return the final report.

    Args:
        data_path:         Path to a data directory or file to profile.
        mode:              ``"autonomous"`` or ``"interactive"``.
        mcp_url:           URL of the file-profiler MCP server.
        connector_mcp_url: URL of the connector MCP server (derived if None).
        provider:          LLM provider override.
        model:             Model name override.

    Returns:
        The final AI message content (the profiling report).
    """
    graph, client = await create_agent(
        mcp_server_url=mcp_url,
        connector_mcp_url=connector_mcp_url,
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


def _trace_cli_turn_inputs(inputs: dict) -> dict:
    config = inputs.get("config") or {}
    configurable = config.get("configurable", {}) if isinstance(config, dict) else {}
    message = inputs.get("initial_message") or ""
    return {
        "thread_id": configurable.get("thread_id", ""),
        "initial_message_chars": len(message),
    }


@traceable(
    name="agent.cli_turn",
    run_type="chain",
    process_inputs=_trace_cli_turn_inputs,
    process_outputs=compact_text_output,
)
async def _run_autonomous(graph, initial_message: str, config: dict) -> str:
    """Run the agent without interruptions."""
    print("\n--- Data Profiling Agent (Autonomous Mode) ---\n")
    print(f"Task: {initial_message}\n")

    with trace_context(
        surface="cli",
        flow="agent",
        metadata={
            "thread_id": config.get("configurable", {}).get("thread_id", ""),
            "mode": "autonomous",
        },
        tags=("mode:autonomous",),
    ):
        result = await graph.ainvoke(
            {
                "messages": [HumanMessage(content=initial_message)],
                "mode": "autonomous",
                "erd_retry_count": 0,
                "erd_guard_action": "",
            },
            config=config,
        )

    # Extract the final AI message
    final_message = get_last_visible_ai_text(result["messages"], current_turn_only=False)

    print("\n--- Profiling Report ---\n")
    print(final_message)
    return final_message


async def _run_interactive(graph, initial_message: str, config: dict) -> str:
    """Run the agent with human-in-the-loop approval for tool calls."""
    # The graph passed in was compiled without a checkpointer; we need to
    # rebuild it with one so interrupt/resume works.  create_agent() already
    # handles the MCP client and graph construction — just pass mode.
    # Note: mcp_url is passed via run_agent → config is only for thread_id.
    print("\n--- Data Profiling Agent (Interactive Mode) ---\n")
    print(f"Task: {initial_message}\n")
    print("You will be asked to approve each tool call.\n")

    state = {
        "messages": [HumanMessage(content=initial_message)],
        "mode": "interactive",
        "erd_retry_count": 0,
        "erd_guard_action": "",
    }

    while True:
        result = await graph.ainvoke(state, config=config)

        # Check if we hit an interrupt (pending tool calls)
        snapshot = await graph.aget_state(config)

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
                    result = await graph.ainvoke(None, config=config)
                    state = result
                    continue
                else:
                    state = {
                        "messages": [
                            HumanMessage(
                                content="The user rejected the tool calls. "
                                "Please adjust your approach or provide the "
                                "report with the information available so far."
                            )
                        ],
                        "erd_retry_count": 0,
                        "erd_guard_action": "",
                    }
                    continue
        else:
            # No more tool calls — agent is done
            break

    # Extract final message
    final_message = get_last_visible_ai_text(result["messages"], current_turn_only=False)

    print("\n--- Profiling Report ---\n")
    print(final_message)
    return final_message


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="LangGraph Data Profiling Agent",
        epilog=(
            "Start both MCP servers first:\n"
            "  python -m file_profiler --transport sse --port 8080\n"
            "  python -m file_profiler.connectors --transport sse --port 8081\n\n"
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
        help="URL of the file-profiler MCP server (default: http://localhost:8080/sse).",
    )
    parser.add_argument(
        "--connector-mcp-url",
        default=None,
        help="URL of the connector MCP server (default: derived from --mcp-url).",
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
                connector_mcp_url=args.connector_mcp_url,
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
