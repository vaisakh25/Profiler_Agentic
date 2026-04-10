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
from collections import defaultdict
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
from langgraph.graph import StateGraph

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


def _validate_and_recover_tool_chain(
    messages: list[BaseMessage],
    *,
    allow_pending_tail_tool_calls: bool = False,
) -> tuple[list[BaseMessage], bool]:
    """Return a tool-call-consistent message list and whether recovery was applied.

    Guarantees that any included assistant tool_call has a matching ToolMessage.
    If inconsistencies are detected, trims back to the last consistent point and
    preserves the latest human intent so execution can continue safely.

    When ``allow_pending_tail_tool_calls=True``, a trailing unresolved AI
    tool_call block is allowed so the tool node can execute it.
    """
    pending_tool_calls: set[str] = set()
    pending_origins: dict[str, int] = {}
    last_consistent_idx = -1
    orphan_tool_message = False

    for idx, msg in enumerate(messages):
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                call_id = str(tc.get("id", tc.get("name", "")))
                if call_id:
                    pending_tool_calls.add(call_id)
                    pending_origins[call_id] = idx
        elif isinstance(msg, ToolMessage):
            call_id = str(msg.tool_call_id or "")
            if not call_id or call_id not in pending_tool_calls:
                orphan_tool_message = True
                break
            pending_tool_calls.remove(call_id)
            pending_origins.pop(call_id, None)

        if not pending_tool_calls:
            last_consistent_idx = idx

    if not orphan_tool_message and not pending_tool_calls:
        return list(messages), False

    if allow_pending_tail_tool_calls and not orphan_tool_message and pending_tool_calls:
        first_pending_idx = min(pending_origins.values()) if pending_origins else len(messages)
        tail_is_executable = True
        for msg in messages[first_pending_idx:]:
            if not (isinstance(msg, AIMessage) and msg.tool_calls):
                tail_is_executable = False
                break
        if tail_is_executable and first_pending_idx > last_consistent_idx:
            return list(messages), False

    recovered = list(messages[: last_consistent_idx + 1]) if last_consistent_idx >= 0 else []

    # Preserve the latest user intent to allow safe continuation.
    last_human: BaseMessage | None = None
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            last_human = msg
            break

    if last_human is not None and (not recovered or recovered[-1] is not last_human):
        recovered.append(last_human)

    return recovered, True


def _compact_messages_preserving_tool_pairs(
    messages: list[BaseMessage],
    max_non_system_messages: int = 8,
) -> list[BaseMessage]:
    """Compact context while preserving assistant tool_call → ToolMessage integrity.

    A naive tail slice can keep a ToolMessage while dropping its originating
    AI tool_call message, which causes provider/API validation failures.
    This helper keeps a bounded suffix and then closes over required parents
    and sibling tool messages for any included tool call IDs.
    """
    system_messages = [m for m in messages if isinstance(m, SystemMessage)]
    non_system = [m for m in messages if not isinstance(m, SystemMessage)]

    if len(non_system) <= max_non_system_messages:
        return system_messages + non_system

    start = max(0, len(non_system) - max_non_system_messages)
    included: set[int] = set(range(start, len(non_system)))

    tool_call_to_ai_idx: dict[str, int] = {}
    ai_tool_ids_by_idx: dict[int, list[str]] = {}
    tool_msg_indices_by_id: dict[str, list[int]] = defaultdict(list)

    for idx, msg in enumerate(non_system):
        if isinstance(msg, AIMessage) and msg.tool_calls:
            call_ids: list[str] = []
            for tc in msg.tool_calls:
                tcid = str(tc.get("id", tc.get("name", "")))
                if tcid:
                    tool_call_to_ai_idx[tcid] = idx
                    call_ids.append(tcid)
            if call_ids:
                ai_tool_ids_by_idx[idx] = call_ids
        elif isinstance(msg, ToolMessage):
            tcid = str(msg.tool_call_id or "")
            if tcid:
                tool_msg_indices_by_id[tcid].append(idx)

    changed = True
    while changed:
        changed = False

        # If a ToolMessage is included, include its parent AI tool_call message.
        for idx in sorted(included):
            msg = non_system[idx]
            if not isinstance(msg, ToolMessage):
                continue
            tcid = str(msg.tool_call_id or "")
            parent_idx = tool_call_to_ai_idx.get(tcid)
            if parent_idx is not None and parent_idx not in included:
                included.add(parent_idx)
                changed = True

        # If an AI tool_call message is included, include all matching tool responses.
        for ai_idx in [i for i in sorted(included) if i in ai_tool_ids_by_idx]:
            for tcid in ai_tool_ids_by_idx[ai_idx]:
                for tool_idx in tool_msg_indices_by_id.get(tcid, []):
                    if tool_idx not in included:
                        included.add(tool_idx)
                        changed = True

    compact_non_system = [non_system[i] for i in sorted(included)]
    return system_messages + compact_non_system


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
from file_profiler.agent.erd_wait import configure_erd_wait_graph, get_last_visible_ai_text
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
You are a fault-tolerant Data Profiling Agent operating in debug + recovery mode. \
You profile datasets, detect relationships (FKs), enrich with AI insights, and answer \
cached-data questions quickly and concisely.

All actions are fast, minimal, and deterministic-first.

You have access to MCP tools from two servers:

### Local File Profiling (file-profiler server)
- **list_supported_files** -- scan a directory for data files
- **profile_file** / **profile_directory** -- run the full profiling pipeline
- **upload_file** -- upload a file for profiling (base64-encoded)

### Remote Data Connectors (data-connector server)
- **connect_source** -- register credentials for a remote data source \
  (PostgreSQL, Snowflake, S3, MinIO, ADLS Gen2, GCS).  Credentials are stored \
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

Debug + recovery rules:
- If source is remote (s3://, minio://, abfss://, gs://, snowflake://, postgresql://),
    use `profile_remote_source` and do not loop `profile_file`.
- Keep tool-call chains complete: no unresolved tool calls and no partial tool-response chains.
- If visualization dependencies are unavailable, skip visualization tools and continue
    with profiling + relationships + enrichment.

Runtime validation layer:
- Before execution, validate tool-call consistency and profile-state consistency.
- If enrichment is requested, ensure staged/profile state exists and source strategy is consistent.
- If state is missing/corrupt, self-heal by rebuilding state with the correct method:
    remote -> `profile_remote_source`; local -> `profile_directory`.
- Do not trust cached status blindly; if staged/profile fingerprints mismatch, run fresh enrichment.

Pre-execution guard (mandatory):
1. Tool-chain invariant:
    - If any prior assistant tool call is missing matching tool responses, trim to last valid state.
    - Never continue with a broken chain.
2. Source-state invariant:
    - Require same source_id, same profiling method, and matching fingerprint when available.
    - If violated, discard staged state and reprofile with the locked strategy.
3. Profiling strategy lock:
    - Remote source -> `profile_remote_source` only.
    - Local source -> `profile_directory` only.
    - Never mix local and remote strategies in one pipeline.
4. Enrichment readiness:
    - Require staged profiles to exist and pass consistency checks before enrichment.
    - If not ready, run one self-healing reprofile before enrichment.

Execution priority stack:
1. Correctness (state + tool integrity)
2. Completeness (relationships + ER)
3. Performance (batching)

Speed rules:
1. Batch tool calls where possible.
2. Skip non-destructive confirmations.
3. No preamble before action/results.
4. Cache first: always call `check_enrichment_status` before `enrich_relationships`.
5. Keep tool args terse (required/default-only).
6. Call `detect_relationships` once per directory.
7. Provide progressive updates for long workflows.
8. Do not dump raw JSON unless explicitly requested.

Tool selection logic (strict routing):

1. User says `profile [path]` or `analyse [path]`:
     - URI (`s3://`, `abfss://`, `gs://`, `snowflake://`, `postgresql://`) -> `profile_remote_source`.
     - Local single file -> `profile_file`.
     - Local directory -> `profile_directory`.

2. User says `find relationships` or `detect FKs`:
     - Already profiled -> `detect_relationships`.
     - Not profiled -> `profile_directory` then `detect_relationships`.

3. User says `enrich`, `AI descriptions`, or `ER diagram`:
     - First `check_enrichment_status` (or `remote_check_enrichment_status`).
     - `complete` -> return cached ER outputs immediately.
     - `stale` or `none` -> run `enrich_relationships` (or `remote_enrich_relationships`).

4. User asks `what tables/files are here?`:
     - Local -> `list_supported_files`.
     - Remote -> `list_tables` (and `list_schemas` when useful).

5. User asks a question about already-profiled data:
     - Use `query_knowledge_base` (or `remote_query_knowledge_base`).
     - Do not re-profile unless explicitly requested.

6. User says `connect [source]` or `add [cloud source]`:
     - Use `connect_source`.

7. User asks about quality:
     - Use `get_quality_summary` (or `remote_get_quality_summary`).

Batching patterns:
- Pattern A (new local directory): `list_supported_files` -> `profile_directory` -> `detect_relationships`.
- Pattern B (enrichment): `check_enrichment_status` then `enrich_relationships` only if stale/none.
- Pattern C (remote source): `list_tables` -> `profile_remote_source` -> `remote_detect_relationships`.
- Never loop `profile_file` for directory-wide profiling.

Response format rules:
- Profile summaries: first line includes row count and column count.
- If more than 20 columns, summarize in a compact table instead of raw JSON.
- Use "table" terminology for profiled entities.
- Relationship lists must include confidence scores.
- Always render ER diagrams as inline Mermaid when available.
- For pipeline summaries, always use sections in this order:
    Summary
    Relationships
    ER Diagram
- Bold only critical quality flags: `FULLY_NULL` and `STRUCTURAL_CORRUPTION`.
- Use comma separators for large numbers and percentages with one decimal place.

Error handling:
- If enrichment fails (timeout/rate-limit), provide deterministic results first and suggest retrying enrichment.
- If connector server is unavailable, continue with file-profiler tools and state that remote tools are unavailable.
- If path is missing/unsupported, run `list_supported_files` to show alternatives.
- If directory is empty, respond in one sentence with supported formats.

Style constraints:
- Profile summary max 300 words.
- Enrichment summary max 500 words.
- Mention FK confidence scores explicitly.
- Use present tense.
- Never use the phrase "I found that".
- Do not ask users for credentials or discuss credentials in responses.
- Ask for confirmation only for destructive reset tools (`reset_vector_store`, `remote_reset_vector_store`).

## Conversation style

- Be direct, concise, and action-first.
- Ask clarifying questions only when intent or path is ambiguous.
- Offer next steps in one short line.
"""


async def run_chatbot(
    mcp_url: str = "http://localhost:8080/sse",
    connector_mcp_url: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> None:
    """Run the interactive chatbot loop."""
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from file_profiler.agent.mcp_endpoints import resolve_mcp_endpoints

    mcp_url, connector_mcp_url, transport = resolve_mcp_endpoints(
        mcp_url=mcp_url,
        connector_mcp_url=connector_mcp_url,
    )

    log.info(
        "Chatbot endpoints resolved: file=%s connector=%s transport=%s",
        mcp_url,
        connector_mcp_url,
        transport,
    )

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

        messages, recovered = _validate_and_recover_tool_chain(messages)
        if recovered:
            log.warning("Recovered inconsistent tool-call chain before LLM invoke")
            messages = messages + [
                SystemMessage(
                    content=(
                        "Internal recovery: repaired an inconsistent tool-call chain. "
                        "Continue execution safely. Include a brief recovery note only "
                        "if the user-visible output is a pipeline summary."
                    )
                )
            ]

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

            compact_messages = _compact_messages_preserving_tool_pairs(
                messages,
                max_non_system_messages=8,
            )

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

    ToolNode, _ = _load_langgraph_prebuilt()
    tool_node = ToolNode(tools, handle_tool_errors=True)

    async def tools_node(state: AgentState):
        messages = list(state.get("messages", []))
        checked, recovered = _validate_and_recover_tool_chain(
            messages,
            allow_pending_tail_tool_calls=True,
        )
        if recovered:
            log.warning("Recovered inconsistent tool-call chain before tool execution")

        if not checked or not isinstance(checked[-1], AIMessage) or not checked[-1].tool_calls:
            log.warning("Skipped tool execution because no executable tail tool_call remained")
            return {"messages": []}

        safe_state = dict(state)
        safe_state["messages"] = checked
        return await tool_node.ainvoke(safe_state)

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tools_node)
    configure_erd_wait_graph(builder)

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
    inputs = {
        "messages": [HumanMessage(content=user_input)],
        "mode": "autonomous",
        "erd_retry_count": 0,
        "erd_guard_action": "",
    }

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
    try:
        state = await graph.aget_state(config)
        if state and state.values:
            final_text = get_last_visible_ai_text(state.values.get("messages", []))
    except Exception as exc:
        log.debug("Could not reload final chatbot state: %s", exc)

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
