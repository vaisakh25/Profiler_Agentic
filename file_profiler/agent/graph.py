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
from langgraph.graph import START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from file_profiler.agent.chatbot import _trim_messages
from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.state import AgentState

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a data profiling agent.  You have access to MCP tools that can profile \
data files (CSV, Parquet, JSON, Excel), detect foreign-key relationships, \
enrich with LLM analysis, and assess data quality.

## Workflow

When given a data directory or file path, follow this workflow:

1. **Discover** — Call ``list_supported_files`` to see what files are available \
and their detected formats.
2. **Check existing state** — Call ``check_enrichment_status`` to see if the \
directory was already profiled and enriched.  If the status is ``"complete"``, \
skip to step 5 (Report) — do NOT re-run profiling or enrichment.  If the \
status is ``"stale"`` or ``"none"``, proceed to step 3.
3. **Profile & Enrich** — Call ``enrich_relationships`` to run the full pipeline \
in one shot: profiles all files, detects deterministic relationships, generates \
per-column semantic descriptions, embeds everything into a vector store, clusters \
tables by column affinity (tables sharing similar columns are grouped together), \
discovers cross-table column relationships via vector similarity, and produces a \
comprehensive LLM analysis with an enriched ER diagram.  Alternatively, call \
``profile_directory`` + ``detect_relationships`` for basic profiling without LLM enrichment.
4. **Quality** — Review quality flags from the profiles.  Call \
``get_quality_summary`` for a focused quality check on specific files if needed.
5. **Visualize** — When the user asks to see, visualize, or chart their data, \
call ``visualize_profile`` to generate professional charts.  Use \
``chart_type="overview"`` with a specific ``table_name`` for a quick visual \
summary, or ``chart_type="overview_directory"`` with ``table_name="*"`` for \
multi-table charts.  Available types: null_distribution, type_distribution, \
cardinality, completeness, skewness, top_values (needs column_name), \
string_lengths (needs column_name), row_counts, quality_heatmap, \
relationship_confidence, overview, overview_directory.  Include the returned \
chart URLs in your response using markdown image syntax: ``![title](url)``.
6. **Follow-up** — Use ``query_knowledge_base`` to answer questions about the \
profiled data via semantic search, ``get_table_relationships`` for a specific \
table's connections, or ``compare_profiles`` to detect schema changes since the \
last run.
7. **Report** — Produce a structured summary covering:
   - Files profiled (name, format, row count, column count)
   - Column type breakdown per table
   - Key candidates (likely primary keys)
   - Detected relationships (FK → PK with confidence)
   - Vector-discovered column similarities
   - Table clusters (which tables share similar columns)
   - Quality issues (null-heavy columns, type conflicts, structural problems)
   - Recommendations and next steps

## Rules

- Always start with reconnaissance (``list_supported_files``) before profiling.
- **ALWAYS check ``check_enrichment_status`` before calling ``enrich_relationships``** \
to avoid redundant work.  Only run enrichment if status is not ``"complete"``.
- Present numeric facts (row counts, null ratios, confidence scores) precisely.
- Flag critical quality issues clearly and suggest remediations.
- If a tool call fails, report the error and continue with remaining files.
- Keep the final report concise but comprehensive.
"""


def _derive_connector_url(base_url: str) -> str:
    """Derive the connector MCP server URL from the file-profiler URL.

    Replaces the port in the URL with CONNECTOR_MCP_PORT (default 8081).
    """
    import re
    from file_profiler.config.env import CONNECTOR_MCP_PORT
    # Replace port number in URL like http://localhost:8080/sse -> http://localhost:8081/sse
    return re.sub(r":(\d+)/", f":{CONNECTOR_MCP_PORT}/", base_url)


async def create_agent(
    mcp_server_url: str = "http://localhost:8080/sse",
    connector_mcp_url: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    mode: str = "autonomous",
):
    """Create and return a compiled LangGraph profiling agent.

    Args:
        mcp_server_url:   URL of the file-profiler MCP server (SSE endpoint).
        connector_mcp_url: URL of the connector MCP server.  If None,
                           derived from mcp_server_url by changing the port.
        provider:          LLM provider name (anthropic/openai/google).
        model:             Model name override.
        mode:              ``"autonomous"`` or ``"interactive"``.

    Returns:
        A tuple of ``(compiled_graph, mcp_client)`` — caller must manage
        the client lifecycle (``async with client``).
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient

    if connector_mcp_url is None:
        connector_mcp_url = _derive_connector_url(mcp_server_url)

    # Determine transport from URL
    transport = "sse"
    if "/mcp" in mcp_server_url or mcp_server_url.endswith("/mcp"):
        transport = "streamable_http"

    file_profiler_server = {
        "url": mcp_server_url,
        "transport": transport,
        "timeout": 30,
        "sse_read_timeout": 1800,
    }
    connector_server = {
        "url": connector_mcp_url,
        "transport": transport,
        "timeout": 30,
        "sse_read_timeout": 1800,
    }

    # Try both servers first; fall back to file-profiler only if connector is down
    try:
        client = MultiServerMCPClient({
            "file-profiler": file_profiler_server,
            "data-connector": connector_server,
        })
        tools = await client.get_tools()
        log.info("Connected to both MCP servers (file-profiler + data-connector)")
    except Exception as exc:
        log.warning(
            "Could not connect to connector server at %s: %s. "
            "Continuing with file-profiler only.",
            connector_mcp_url, exc,
        )
        client = MultiServerMCPClient({"file-profiler": file_profiler_server})
        tools = await client.get_tools()

    if not tools:
        raise RuntimeError(
            f"No tools loaded from MCP servers. "
            f"Is the file-profiler server running at {mcp_server_url}?"
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
