"""LangGraph StateGraph for the data profiling agent.

Builds a ReAct-style agent loop:

    ┌─────────┐     tool_calls?     ┌─────────┐
    │  agent  │ ──── yes ──────────►│  tools  │
    │  (LLM)  │◄────────────────────│(ToolNode)│
    └────┬────┘                     └─────────┘
         │ no tool_calls
         ▼
        END

The agent node sends the conversation (with a system prompt) to the LLM.
If the LLM decides to call tools, ToolNode executes them and loops back.
"""

from __future__ import annotations

import logging
from typing import Optional

from langchain_core.messages import SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from langchain_core.messages import ToolMessage

from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.state import AgentState

# Max chars kept per tool result — higher limit to avoid truncating file lists
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

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a data profiling agent.  You have access to MCP tools that can profile \
data files (CSV, Parquet, JSON, Excel), detect foreign-key relationships, and \
assess data quality.

## Workflow

When given a data directory or file path, follow this workflow:

1. **Discover** — Call ``list_supported_files`` to see what files are available \
and their detected formats.
2. **Profile** — Call ``profile_directory`` (for a directory) or ``profile_file`` \
(for a single file) to run the full profiling pipeline.
3. **Relationships** — Call ``detect_relationships`` to find foreign-key \
candidates across tables.
4. **Quality** — Review quality flags from the profiles.  Call \
``get_quality_summary`` for a focused quality check on specific files if needed.
5. **Report** — Produce a structured summary covering:
   - Files profiled (name, format, row count, column count)
   - Column type breakdown per table
   - Key candidates (likely primary keys)
   - Detected relationships (FK → PK with confidence)
   - Quality issues (null-heavy columns, type conflicts, structural problems)
   - Recommendations and next steps

## Rules

- Always start with reconnaissance (``list_supported_files``) before profiling.
- Present numeric facts (row counts, null ratios, confidence scores) precisely.
- Flag critical quality issues clearly and suggest remediations.
- If a tool call fails, report the error and continue with remaining files.
- Keep the final report concise but comprehensive.
"""


async def create_agent(
    mcp_server_url: str = "http://localhost:8080/sse",
    provider: Optional[str] = None,
    model: Optional[str] = None,
    mode: str = "autonomous",
):
    """Create and return a compiled LangGraph profiling agent.

    Args:
        mcp_server_url: URL of the running MCP server (SSE endpoint).
        provider:       LLM provider name (anthropic/openai/google).
        model:          Model name override.
        mode:           ``"autonomous"`` or ``"interactive"``.

    Returns:
        A tuple of ``(compiled_graph, mcp_client)`` — caller must manage
        the client lifecycle (``async with client``).
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient

    # Determine transport from URL
    transport = "sse"
    if "/mcp" in mcp_server_url or mcp_server_url.endswith("/mcp"):
        transport = "streamable_http"

    client = MultiServerMCPClient(
        {
            "file-profiler": {
                "url": mcp_server_url,
                "transport": transport,
            }
        }
    )

    # Connect and load tools
    tools = await client.get_tools()

    if not tools:
        raise RuntimeError(
            f"No tools loaded from MCP server at {mcp_server_url}. "
            f"Is the server running?"
        )

    log.info("Loaded %d MCP tools: %s", len(tools), [t.name for t in tools])

    # Create LLM and bind tools
    llm = get_llm_with_fallback(provider=provider, model=model)
    llm_with_tools = llm.bind_tools(tools)

    # Define agent node
    async def agent_node(state: AgentState):
        messages = state["messages"]
        # Prepend system prompt if not already present
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [SystemMessage(content=SYSTEM_PROMPT)] + list(messages)
        messages = _trim_messages(messages)
        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}

    # Build graph
    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", ToolNode(tools))

    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", tools_condition)
    builder.add_edge("tools", "agent")

    # Compile — interactive mode adds interrupt before tool execution
    if mode == "interactive":
        graph = builder.compile(interrupt_before=["tools"])
    else:
        graph = builder.compile()

    return graph, client
