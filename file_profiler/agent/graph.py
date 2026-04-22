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
from typing import Any, Optional, cast

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import StateGraph

from file_profiler.agent.chatbot import (
    _normalize_system_messages,
    _trim_messages,
    _validate_and_recover_tool_chain,
)
from file_profiler.agent.erd_wait import configure_erd_wait_graph
from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.mcp_endpoints import derive_connector_url, resolve_mcp_endpoints
from file_profiler.agent.state import AgentState
from file_profiler.agent.system_prompt import UNIFIED_SYSTEM_PROMPT, OPTIMIZED_PROMPT
from file_profiler.observability.langsmith import compact_text_output, extract_llm_usage, resolve_prompt, traceable
from file_profiler.config.runtime_config import get_config

log = logging.getLogger(__name__)

SYSTEM_PROMPT = UNIFIED_SYSTEM_PROMPT


def _trace_agent_state_inputs(inputs: dict) -> dict:
    state = inputs.get("state") or {}
    messages = state.get("messages", []) if isinstance(state, dict) else []
    return {
        "message_count": len(messages),
        "mode": state.get("mode") if isinstance(state, dict) else "",
    }


def _load_langgraph_prebuilt():
    try:
        from langgraph.prebuilt import ToolNode, tools_condition
        return ToolNode, tools_condition
    except ImportError as exc:
        raise RuntimeError(
            "LangGraph prebuilt components are unavailable. "
            "Install compatible versions of langgraph and langgraph-prebuilt."
        ) from exc


def _derive_connector_url(base_url: str) -> str:
    """Backward-compatible wrapper for connector MCP URL derivation."""
    return derive_connector_url(base_url)


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

    mcp_server_url, connector_mcp_url, transport = resolve_mcp_endpoints(
        mcp_url=mcp_server_url,
        connector_mcp_url=connector_mcp_url,
    )

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
        client = MultiServerMCPClient(cast(Any, {
            "file-profiler": file_profiler_server,
            "data-connector": connector_server,
        }))
        tools = await client.get_tools()
        log.info("Connected to both MCP servers (file-profiler + data-connector)")
    except Exception as exc:
        log.warning(
            "Could not connect to connector server at %s: %s. "
            "Continuing with file-profiler only.",
            connector_mcp_url, exc,
        )
        client = MultiServerMCPClient(cast(Any, {"file-profiler": file_profiler_server}))
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
    @traceable(
        name="agent.autonomous_node",
        run_type="chain",
        process_inputs=_trace_agent_state_inputs,
        process_outputs=extract_llm_usage,
    )
    async def agent_node(state: AgentState):
        messages = state["messages"]
        
        # Choose prompt based on config
        use_optimized = get_config().get("USE_OPTIMIZED_PROMPT", True)
        prompt = OPTIMIZED_PROMPT if use_optimized else UNIFIED_SYSTEM_PROMPT
        
        # Prepend system prompt if not already present
        if not messages or not isinstance(messages[0], SystemMessage):
            messages = [
                SystemMessage(
                    content=resolve_prompt("file-profiler/chatbot_system", prompt)
                )
            ] + list(messages)

        messages, recovered = _validate_and_recover_tool_chain(messages)
        if recovered:
            log.warning("Recovered inconsistent tool-call chain before autonomous LLM invoke")
            # Add recovery note as HumanMessage, not SystemMessage
            # (SystemMessage can only appear at position 0)
            messages = messages + [
                HumanMessage(
                    content=(
                        "Internal recovery: repaired an inconsistent tool-call chain. "
                        "Continue execution safely. Include a brief recovery note only "
                        "if the user-visible output is a pipeline summary."
                    )
                )
            ]

        messages = _trim_messages(messages)
        
        # Ensure system messages are always at position 0 before LLM call
        messages = _normalize_system_messages(messages)
        
        response = await llm_with_tools.ainvoke(messages)
        return {"messages": [response]}

    # Build graph
    ToolNode, _ = _load_langgraph_prebuilt()

    tool_node = ToolNode(tools)

    async def tools_node(state: AgentState):
        messages = list(state.get("messages", []))
        checked, recovered = _validate_and_recover_tool_chain(
            messages,
            allow_pending_tail_tool_calls=True,
        )
        if recovered:
            log.warning("Recovered inconsistent tool-call chain before autonomous tool execution")

        if not checked or not isinstance(checked[-1], AIMessage) or not checked[-1].tool_calls:
            log.warning("Skipped autonomous tool execution due to non-executable tail tool state")
            return {"messages": []}

        safe_state = dict(state)
        safe_state["messages"] = checked
        return await tool_node.ainvoke(safe_state)

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tools_node)
    configure_erd_wait_graph(builder)

    # Compile — interactive mode adds interrupt before tool execution
    if mode == "interactive":
        graph = builder.compile(interrupt_before=["tools"])
    else:
        graph = builder.compile()

    return graph, client
