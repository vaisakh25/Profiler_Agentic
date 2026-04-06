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

# Set event loop policy early before any other imports create a loop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langgraph.graph import START, StateGraph

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
            # Always create a new ToolMessage with string content (not rich TextChunk objects)
            msg = ToolMessage(content=content, tool_call_id=msg.tool_call_id)
        trimmed.append(msg)
    return trimmed


def _load_langgraph_prebuilt():
    try:
        from langgraph.prebuilt import ToolNode, tools_condition
        return ToolNode, tools_condition
    except ImportError as exc:
        raise RuntimeError(
            "LangGraph prebuilt components are unavailable. "
            "Install compatible versions of langgraph and langgraph-prebuilt."
        ) from exc

from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.progress import ProgressTracker
from file_profiler.agent.state import AgentState
from file_profiler.config.runtime_config import get_config

log = logging.getLogger(__name__)


def _get_int_config(name: str, default: int) -> int:
    """Read an integer runtime setting and fallback safely on bad values."""
    raw_value = get_config(name, str(default))
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        log.warning("Invalid %s=%r; using default=%d", name, raw_value, default)
        return default


def _is_timeout_error(exc: BaseException) -> bool:
    """Return True when an exception chain indicates a request timeout."""
    seen: set[int] = set()
    current: BaseException | None = exc

    while current is not None and id(current) not in seen:
        seen.add(id(current))

        if isinstance(current, TimeoutError):
            return True

        try:
            import httpx

            if isinstance(current, httpx.TimeoutException):
                return True
        except ImportError:
            pass

        try:
            from openai import APITimeoutError

            if isinstance(current, APITimeoutError):
                return True
        except ImportError:
            pass

        error_text = f"{type(current).__name__}: {current}".lower()
        if "timeout" in error_text or "readtimeout" in error_text:
            return True

        current = current.__cause__ or current.__context__

    return False

# ---------------------------------------------------------------------------
# System prompt — conversational style
# ---------------------------------------------------------------------------

CHATBOT_SYSTEM_PROMPT = """\
You are a friendly data profiling assistant.  You help users explore and \
understand their data files (CSV, Parquet, JSON, Excel).

You have access to MCP tools from two servers:

### Local File Profiling (file-profiler server)
- **list_supported_files** -- scan a directory for data files
- **profile_file** / **profile_directory** -- run the full profiling pipeline
- **upload_file** -- upload a file for profiling (base64-encoded)

### Remote Data Connectors (data-connector server)
- **connect_source** -- register credentials for a remote data source \
  (PostgreSQL, Snowflake, S3, ADLS Gen2, GCS).  Credentials are stored \
  securely and never pass through the LLM.
- **list_connections** -- list all registered remote connections
- **test_connection** -- test connectivity for a registered connection
- **remove_connection** -- remove a connection and its credentials
- **list_schemas** -- list schemas in a remote database
- **list_tables** -- list tables/files at a remote source without profiling
- **profile_remote_source** -- profile a remote data source (database tables \
  or cloud storage files).  Materialises profiles to a staging directory so \
  the full pipeline can operate on them.

### Local Pipeline Tools (for local file data)
- **detect_relationships** -- find FK relationships and generate ER diagrams
- **enrich_relationships** -- LLM-powered deep analysis with unified \
  column-affinity clustering and relationship discovery.  Runs a pipeline: \
  (1) MAP -- summarize each table + generate per-column descriptions, \
  (2) APPLY -- write descriptions back into profile JSONs, \
  (3) EMBED -- store summaries + column descriptions in ChromaDB with \
  enriched signals (sample values, cardinality, top values), \
  (4) DISCOVER + CLUSTER -- build a table-to-table affinity matrix from \
  column embedding similarities; tables sharing many similar columns \
  cluster together, and FK candidates emerge from the same computation, \
  (5) REDUCE -- synthesize all findings with vector-discovered relationships \
  prioritised over deterministic FK candidates.  Produces semantic \
  descriptions, PK/FK reassessment, join recommendations, and an \
  enriched ER diagram.
- **check_enrichment_status** -- lightweight check if enrichment is already done
- **reset_vector_store** -- clear ChromaDB and caches when enrichment fails
- **get_quality_summary** -- check data quality for a specific file
- **query_knowledge_base** -- semantic search over the vector store
- **get_table_relationships** -- get all relationships for a specific table
- **compare_profiles** -- detect schema drift
- **visualize_profile** -- generate professional data-scientist-grade charts \
  with statistical annotations from profiled data.  Chart types include: \
  overview (comprehensive dashboard), data_quality_scorecard (radar chart), \
  null_distribution, type_distribution, cardinality, completeness, \
  numeric_summary (mean/median/std comparison), skewness, outlier_summary \
  (Tukey IQR method), correlation_matrix (Pearson heatmap), distribution \
  (percentile waterfall + stats table for a column), column_detail \
  (multi-panel deep-dive), top_values, string_lengths, row_counts, \
  quality_heatmap, relationship_confidence, overview_directory.  \
  Always call this when the user asks to "show", "visualize", "chart", or \
  "plot" their data, or when visual output would enhance understanding

### Remote Pipeline Tools (for remote/connector data -- prefixed with remote_)
These are the same pipeline tools but operate on remote data staged by \
`profile_remote_source`.  They take a `connection_id` parameter:
- **remote_detect_relationships** -- FK detection on remote tables
- **remote_enrich_relationships** -- full LLM enrichment pipeline on remote data
- **remote_check_enrichment_status** -- check if remote enrichment is done
- **remote_reset_vector_store** -- clear caches for remote data
- **remote_get_quality_summary** -- quality summary for a remote table
- **remote_query_knowledge_base** -- semantic search over remote profiled data
- **remote_get_table_relationships** -- relationships for a remote table
- **remote_compare_profiles** -- schema drift detection for remote data
- **remote_visualize_profile** -- charts for remote profiled data

## How to help

### Local files
When a user tells you where their data is (a folder path or file path):
1. First call `list_supported_files` to show them what's there.
2. **Check first**: Call `check_enrichment_status` to see if the directory was \
   already profiled and enriched.  This is a **lightweight check** -- it only \
   reads a manifest file and compares file timestamps.  It does NOT profile \
   any files.  If the status is `"complete"`, tell the user that enrichment \
   is already done and skip to presenting results.  Do NOT re-run \
   `enrich_relationships` if data hasn't changed.
3. If status is `"stale"` or `"none"`, **ask the user for confirmation** before \
   proceeding.  Tell them how many files were found and that running \
   `enrich_relationships` will profile all files and run LLM analysis.  \
   Only call `enrich_relationships` after the user confirms.
4. Present the enriched ER diagram and the LLM's analysis to the user.

**After profiling completes** (whether via `profile_file` or `profile_directory`), \
always suggest the natural next step: running `enrich_relationships` on the \
directory containing the profiled data.  Use the **parent directory** of the \
profiled file (not the file path itself) when calling `enrich_relationships`.

### Remote data sources (databases, cloud storage)
When a user wants to profile data from PostgreSQL, Snowflake, S3, ADLS, or GCS:
1. Help them register a connection via `connect_source` with their credentials.  \
   **Never ask the user to paste credentials into chat** -- use the connect_source \
   tool which stores them securely.
2. Use `list_schemas` and `list_tables` to explore the remote source.
3. Use `profile_remote_source` with the connection_id to profile tables.  \
   This materialises the profiles to a staging directory.
4. After profiling, use the `remote_` prefixed pipeline tools: \
   `remote_detect_relationships` -> `remote_enrich_relationships` -> \
   `remote_visualize_profile`.  Pass the **connection_id** to these tools.
5. Present results the same way as local files.

If the user only wants basic profiling without LLM enrichment, use \
`detect_relationships` instead of `enrich_relationships`.

For follow-up questions after enrichment, use `query_knowledge_base` to \
search the vector store, `get_table_relationships` for a specific table's \
connections, or `compare_profiles` to detect changes since the last run.

**Troubleshooting enrichment failures:** If `enrich_relationships` fails \
(especially with ValueError or stale data errors), call `reset_vector_store` \
to clear the ChromaDB collections and cached data, then retry. This is \
especially needed when the user changes which tables to enrich (e.g. went \
from 194 tables to 10) — stale vector data from the previous run can \
cause conflicts.

## Presentation guidelines — think like a senior data scientist

You are not just a data profiler — you are a **senior data scientist** who \
interprets results with depth and nuance.  When presenting findings:

**Statistical interpretation:**
- Interpret skewness: values > 1 or < -1 indicate heavy skew; near 0 is \
  symmetric.  Explain what this means for the data (e.g. "revenue is \
  right-skewed — most transactions are small with a long tail of large ones").
- Interpret kurtosis: positive excess kurtosis = heavy tails (more extreme \
  outliers than normal); negative = light tails.  Flag leptokurtic columns.
- When outliers are detected (via Tukey IQR), quantify their impact: how \
  many, what percentage, and whether they might indicate data entry errors \
  vs genuine extreme values.
- Coefficient of variation (CV) > 1.0 means high relative variability — \
  flag this.  CV < 0.1 means the column is nearly constant.
- Compare mean vs median: large divergence indicates skew or outlier \
  influence.  Call this out explicitly.

**Data quality assessment:**
- Use ``data_quality_scorecard`` to give users a quick quality grade (0-100).
- When completeness is low, explain which columns are worst offenders and \
  suggest whether missing data is random (MAR) or systematic (MNAR).
- Flag columns with mixed types or low confidence scores as data integrity \
  risks.
- When multiple columns have the same cardinality pattern, suggest they may \
  be derived from each other.

**Proactive chart generation:**
- When the user asks about their data, proactively generate the overview \
  dashboard using ``visualize_profile`` (chart_type="overview").
- For numeric columns of interest, offer ``distribution`` or ``column_detail`` \
  charts to provide the deep statistical view.
- When comparing tables, use ``overview_directory`` and ``correlation_matrix``.
- When showing charts, include the returned URLs in your response using \
  markdown image syntax: ``![Chart Title](/charts/filename.png)``.  Briefly \
  describe what each chart reveals with statistical context.
- For a quick overview of a single table, use ``chart_type="overview"`` with \
  the table name.  For comparing across tables, use ``overview_directory``.

**Relationship and schema insights:**
- When showing the ER diagram, display the raw Mermaid markdown so the user \
  can copy-paste it into any Mermaid renderer.
- Summarise key findings: table counts, row counts, detected relationships \
  (both deterministic and vector-discovered), and quality issues.
- Include the LLM's semantic descriptions and join recommendations.
- Highlight vector-discovered column similarities — these are often the most \
  valuable insights for understanding cross-table connections.

**Communication style:**
- Lead with the most actionable insight, then support with data.
- Use bullet points and tables where helpful.  Be concise but thorough.
- When presenting numeric stats, round appropriately and use commas for \
  readability.
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
    connector_mcp_url: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> None:
    """Run the interactive chatbot loop."""
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from file_profiler.agent.graph import _derive_connector_url

    if connector_mcp_url is None:
        connector_mcp_url = _derive_connector_url(mcp_url)

    # Determine transport from URL
    transport = "sse"
    if "/mcp" in mcp_url or mcp_url.endswith("/mcp"):
        transport = "streamable_http"

    mcp_client_timeout = _get_int_config("MCP_CLIENT_TIMEOUT", 120)
    chat_llm_timeout = _get_int_config(
        "CHATBOT_LLM_TIMEOUT",
        _get_int_config("LLM_TIMEOUT", 120),
    )

    client = MultiServerMCPClient(
        {
            "file-profiler": {
                "url": mcp_url,
                "transport": transport,
                "timeout": mcp_client_timeout,
                "sse_read_timeout": 3600,
            },
            "data-connector": {
                "url": connector_mcp_url,
                "transport": transport,
                "timeout": mcp_client_timeout,
                "sse_read_timeout": 3600,
            },
        }
    )

    print("\n  Connecting to MCP servers...", end="", flush=True)
    try:
        tools = await client.get_tools()
    except Exception as exc:
        print(f" FAILED\n\n  Could not connect to MCP servers")
        print(f"  Error: {exc}")
        print("\n  Make sure both servers are running:")
        print("    python -m file_profiler --transport sse --port 8080")
        print("    python -m file_profiler.connectors --transport sse --port 8081\n")
        return

    if not tools:
        print(" FAILED -- no tools loaded.\n")
        return

    print(f" OK ({len(tools)} tools loaded)")

    # Build the graph with a checkpointer for multi-turn memory
    llm = get_llm_with_fallback(
        provider=provider,
        model=model,
        timeout=chat_llm_timeout,
    )
    llm_with_tools = llm.bind_tools(tools)

    async def agent_node(state: AgentState):
        messages = state["messages"]
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [SystemMessage(content=CHATBOT_SYSTEM_PROMPT)] + list(messages)
        messages = _trim_messages(messages)

        try:
            response = await llm_with_tools.ainvoke(messages)
            return {"messages": [response]}
        except BaseException as exc:
            if not _is_timeout_error(exc):
                raise

            log.warning(
                "LLM request timed out (timeout=%ss); retrying with compact context",
                chat_llm_timeout,
            )

            system_messages = [m for m in messages if isinstance(m, SystemMessage)]
            non_system_messages = [m for m in messages if not isinstance(m, SystemMessage)]
            compact_messages = system_messages + non_system_messages[-8:]

            try:
                response = await llm_with_tools.ainvoke(compact_messages)
                return {"messages": [response]}
            except BaseException as retry_exc:
                if not _is_timeout_error(retry_exc):
                    raise

                log.warning(
                    "LLM request timed out after retry (timeout=%ss)",
                    chat_llm_timeout,
                )
                guidance = (
                    "I hit a provider timeout while processing your request. "
                    "Please retry. If this keeps happening, increase "
                    "CHATBOT_LLM_TIMEOUT or LLM_TIMEOUT in config.yml, or try "
                    "a shorter request."
                )
                return {"messages": [AIMessage(content=guidance)]}

    ToolNode, tools_condition = _load_langgraph_prebuilt()

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", ToolNode(tools, handle_tool_errors=True))
    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", tools_condition)
    builder.add_edge("tools", "agent")

    from file_profiler.config.database import get_checkpointer
    checkpointer = await get_checkpointer()
    graph = builder.compile(checkpointer=checkpointer)

    import uuid
    session_id = f"cli-{uuid.uuid4().hex[:12]}"
    config = {"configurable": {"thread_id": session_id}}

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

    except (ConnectionResetError, OSError) as exc:
        await tracker.finish_thinking()
        print(f"\n  Connection to MCP server was interrupted: {exc}")
        print("  The operation may have completed on the server side.")
        print("  Try re-running — cached results will be reused automatically.")
        log.warning("SSE/MCP connection reset: %s", exc)
        return
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
    connector_mcp_url: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> None:
    """Entry point for the chatbot."""
    _load_dotenv()

    # Windows: use SelectorEventLoop to avoid ProactorBasePipeTransport
    # ConnectionResetError during long-running SSE/MCP connections.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run_chatbot(
            mcp_url=mcp_url,
            connector_mcp_url=connector_mcp_url,
            provider=provider,
            model=model,
        ))
    except KeyboardInterrupt:
        print("\n\n  Goodbye!\n")
        sys.exit(0)
