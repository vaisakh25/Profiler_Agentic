"""Interactive chatbot for the data profiling agent.

Multi-turn conversational interface that connects to the MCP server
and lets users profile data, detect relationships, and view ER diagrams
through natural language.

Usage:
  # Terminal 1 — start MCP server:
  python -m file_profiler --transport sse --port 8080

  # Terminal 2 — start chatbot:
  python -m file_profiler.agent --chat
  python -m file_profiler.agent --chat --provider openai
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

# Max chars kept per tool result — higher limit to avoid truncating file lists
# and profile summaries. Groq (8k context) may still need trimming, but Google
# Gemini and other providers handle much larger payloads.
_MAX_TOOL_CHARS = 12000


def _trim_messages(messages: list) -> list:
    """Truncate oversized ToolMessage content to avoid context overflow."""
    trimmed = []
    for msg in messages:
        if isinstance(msg, ToolMessage):
            content = msg.content if isinstance(msg.content, str) else str(msg.content)
            if len(content) > _MAX_TOOL_CHARS:
                content = content[:_MAX_TOOL_CHARS] + "\n...[truncated]"
                msg = ToolMessage(content=content, tool_call_id=msg.tool_call_id)
        trimmed.append(msg)
    return trimmed
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.progress import ProgressTracker
from file_profiler.agent.state import AgentState

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt — conversational style
# ---------------------------------------------------------------------------

CHATBOT_SYSTEM_PROMPT = """\
You are a friendly data profiling assistant.  You help users explore and \
understand their data files (CSV, Parquet, JSON, Excel).

You have access to MCP tools that can:
- **list_supported_files** — scan a directory for data files
- **profile_file** / **profile_directory** — run the full profiling pipeline
- **detect_relationships** — find foreign key relationships and generate ER diagrams
- **enrich_relationships** — LLM-powered deep analysis: embeds profiles + sample \
  rows into a vector store, then produces semantic descriptions, PK/FK \
  reassessment, join recommendations, and an enriched ER diagram
- **get_quality_summary** — check data quality for a specific file

## How to help

When a user tells you where their data is (a folder path or file path):
1. First call `list_supported_files` to show them what's there.
2. Then call `enrich_relationships` — this runs the full pipeline in one shot: \
   profiles all files, detects relationships, builds a vector store from \
   profiles + sample rows + low-cardinality values, and uses an LLM to produce \
   semantic descriptions, PK/FK reassessment, join recommendations, and an \
   enriched ER diagram.
3. Present the enriched ER diagram and the LLM's analysis to the user.

If the user only wants basic profiling without LLM enrichment, use \
`detect_relationships` instead of `enrich_relationships`.

## Presentation guidelines

- When showing the ER diagram, display the raw Mermaid markdown so the user \
  can copy-paste it into any Mermaid renderer.
- Summarise key findings: table counts, row counts, detected relationships, \
  and quality issues.
- Include the LLM's semantic descriptions and join recommendations.
- Be concise but thorough.  Use bullet points and tables where helpful.
- If a tool fails, explain the error and suggest next steps.
- For follow-up questions, use cached results when possible.

## Conversation style

- Be conversational and helpful, not robotic.
- Ask clarifying questions if the user's intent is unclear.
- Offer suggestions for next steps (e.g. "Want me to check data quality?" \
  or "I can enrich the analysis with LLM descriptions?").
"""


async def run_chatbot(
    mcp_url: str = "http://localhost:8080/sse",
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> None:
    """Run the interactive chatbot loop."""
    from langchain_mcp_adapters.client import MultiServerMCPClient

    # Determine transport from URL
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

    print("\n  Connecting to MCP server...", end="", flush=True)
    try:
        tools = await client.get_tools()
    except Exception as exc:
        print(f" FAILED\n\n  Could not connect to MCP server at {mcp_url}")
        print(f"  Error: {exc}")
        print("\n  Make sure the server is running:")
        print("    python -m file_profiler --transport sse --port 8080\n")
        return

    if not tools:
        print(" FAILED — no tools loaded.\n")
        return

    print(f" OK ({len(tools)} tools loaded)")

    # Build the graph with a checkpointer for multi-turn memory
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

    config = {"configurable": {"thread_id": "chatbot-session-1"}}

    _print_banner()

    try:
        while True:
            try:
                user_input = input("\n You: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n\n  Goodbye!\n")
                break

            if not user_input:
                continue

            if user_input.lower() in ("exit", "quit", "q", "bye"):
                print("\n  Goodbye!\n")
                break

            if user_input.lower() in ("help", "?"):
                _print_help()
                continue

            print()
            await _run_turn(graph, user_input, config)

    except Exception:
        pass  # clean exit


async def _run_turn(graph, user_input: str, config: dict) -> None:
    """Execute one conversational turn with progress tracking."""
    inputs = {"messages": [HumanMessage(content=user_input)], "mode": "autonomous"}

    tracker = ProgressTracker()
    final_text = ""

    # Pending tool calls — we start the spinner when the agent emits them,
    # and stop it when the tools node returns results.
    pending_tools: dict[str, dict] = {}  # tool_call_id → {name, args}

    try:
        async for event in graph.astream(inputs, config=config, stream_mode="updates"):
            for node_name, node_output in event.items():

                if node_name == "agent":
                    msg = node_output["messages"][-1]
                    if isinstance(msg, AIMessage):
                        if msg.tool_calls:
                            # Agent decided to call tools — start progress
                            await tracker.finish_thinking()
                            for tc in msg.tool_calls:
                                tool_id = tc.get("id", tc["name"])
                                pending_tools[tool_id] = {
                                    "name": tc["name"],
                                    "args": tc.get("args", {}),
                                }
                                await tracker.start_tool(
                                    tc["name"], tc.get("args", {}),
                                )
                        elif msg.content:
                            # Final response — stop any spinner
                            await tracker.finish_thinking()
                            final_text = msg.content
                        else:
                            # Agent is thinking (no tool calls, no content yet)
                            await tracker.start_thinking()

                elif node_name == "tools":
                    # Tool results arrived — match to pending calls
                    for msg in node_output["messages"]:
                        if isinstance(msg, ToolMessage):
                            content = msg.content if isinstance(msg.content, str) else str(msg.content)
                            tool_id = msg.tool_call_id

                            # Find the matching tool call
                            tool_info = pending_tools.pop(tool_id, None)
                            tool_name = tool_info["name"] if tool_info else "unknown"

                            await tracker.finish_tool(tool_name, content)

    except Exception as exc:
        await tracker.finish_thinking()
        print(f"\n  Error: {exc}")
        log.exception("Agent turn failed")
        return

    # Print pipeline summary
    tracker.print_summary()

    # Normalise content — some providers return list of dicts
    if isinstance(final_text, list):
        final_text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in final_text
        )

    # Print final response
    if final_text:
        print(f"\n Assistant:\n")
        for line in final_text.split("\n"):
            print(f"  {line}")


# ---------------------------------------------------------------------------
# Banner and help
# ---------------------------------------------------------------------------

def _print_banner() -> None:
    """Print the chatbot welcome banner."""
    print("\n" + "=" * 60)
    print("  Data Profiler Chatbot")
    print("=" * 60)
    print()
    print("  Tell me where your data is and I'll profile it for you.")
    print("  I can detect schemas, relationships, and generate")
    print("  ER diagrams from your data files.")
    print()
    print("  Commands: 'help' for tips, 'quit' to exit")
    print("=" * 60)


def _print_help() -> None:
    """Print help text."""
    print()
    print("  Examples:")
    print("    'My data is in C:/data/files'")
    print("    'Profile the file at ./customers.csv'")
    print("    'Show me the ER diagram for ./data/files'")
    print("    'Check data quality for person.parquet'")
    print("    'What relationships exist between my tables?'")
    print()
    print("  Tips:")
    print("    - Point me to a directory and I'll find all data files")
    print("    - I support CSV, Parquet, JSON, and Excel files")
    print("    - After profiling, ask follow-up questions about your data")
    print()
    print("  Commands:")
    print("    help, ?    — show this help")
    print("    quit, exit — exit the chatbot")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _load_dotenv() -> None:
    """Load .env file from the project root if available."""
    try:
        from dotenv import load_dotenv
        from pathlib import Path

        env_path = Path(__file__).resolve().parent.parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=False)
            log.debug("Loaded .env from %s", env_path)
    except ImportError:
        pass


def main(
    mcp_url: str = "http://localhost:8080/sse",
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> None:
    """Entry point for the chatbot."""
    _load_dotenv()
    try:
        asyncio.run(run_chatbot(mcp_url=mcp_url, provider=provider, model=model))
    except KeyboardInterrupt:
        print("\n\n  Goodbye!\n")
        sys.exit(0)
