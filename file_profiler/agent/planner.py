"""Two-tier planning agent for large-scale profiling (50+ files).

The planner decomposes complex profiling tasks into steps, dispatches
them to specialist sub-agents with appropriate tool subsets, and
aggregates the results into a final report.

Architecture:
    START → planner → dispatch → specialist_subgraph → aggregate → planner|END

The planner uses a stronger LLM to reason about the decomposition,
while specialists use the standard model for tool execution.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from file_profiler.agent.chatbot import _trim_messages
from file_profiler.agent.llm_factory import get_llm_with_fallback
from file_profiler.agent.state import PlannerState

log = logging.getLogger(__name__)

PLANNER_SYSTEM_PROMPT = """\
You are a planning agent for large-scale data profiling tasks.  You decompose \
work into discrete steps that specialist agents can execute independently.

Given a user request involving data files, produce a JSON plan with steps.
Each step has:
- "id": unique step identifier (e.g., "discover", "profile", "enrich", "report")
- "action": what the specialist should do
- "tools": list of tool names the specialist needs
- "depends_on": list of step IDs this step depends on (empty = can run immediately)

For a typical profiling task on a large directory:
1. "discover" — list_supported_files to see what's there
2. "check" — check_enrichment_status to avoid redundant work
3. "enrich" — enrich_relationships (async_mode=True for 50+ tables)
4. "monitor" — get_job_status to track async job progress
5. "visualize" — generate overview charts
6. "report" — produce final summary

Output your plan as a JSON array inside ```json ... ``` fences.
After the plan executes, you'll receive specialist results and produce the \
final report for the user.
"""


def _parse_plan(content: str) -> list[dict]:
    """Extract a JSON plan from LLM output."""
    import re
    # Try to find JSON in fences
    match = re.search(r"```json\s*\n?(.*?)```", content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    # Try raw JSON
    try:
        result = json.loads(content)
        if isinstance(result, list):
            return result
    except (json.JSONDecodeError, TypeError):
        pass
    return []


async def create_planning_agent(
    mcp_server_url: str = "http://localhost:8080/sse",
    connector_mcp_url: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    mode: str = "autonomous",
):
    """Create a two-tier planning agent for large-scale profiling.

    The planner decomposes work, dispatches to specialist sub-agents,
    and aggregates results.

    Args:
        mcp_server_url:    URL of the file-profiler MCP server.
        connector_mcp_url: URL of the connector MCP server.
        provider:          LLM provider name.
        model:             Model name override.
        mode:              "autonomous" or "interactive".

    Returns:
        Tuple of (compiled_graph, mcp_client).
    """
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from file_profiler.agent.graph import _derive_connector_url

    if connector_mcp_url is None:
        connector_mcp_url = _derive_connector_url(mcp_server_url)

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

    try:
        client = MultiServerMCPClient({
            "file-profiler": file_profiler_server,
            "data-connector": connector_server,
        })
        tools = await client.get_tools()
    except Exception:
        client = MultiServerMCPClient({"file-profiler": file_profiler_server})
        tools = await client.get_tools()

    if not tools:
        raise RuntimeError("No tools loaded from MCP servers.")

    # Deduplicate
    seen: set[str] = set()
    unique_tools = []
    for t in tools:
        if t.name not in seen:
            seen.add(t.name)
            unique_tools.append(t)
    tools = unique_tools

    # Build tool name → tool lookup for specialist subsetting
    tool_map = {t.name: t for t in tools}

    # Planner LLM (no tools — it reasons, doesn't execute)
    planner_llm = get_llm_with_fallback(provider=provider, model=model)

    # Specialist LLM (with tools)
    specialist_llm = get_llm_with_fallback(provider=provider, model=model)
    specialist_llm_with_tools = specialist_llm.bind_tools(tools)

    # --- Planner node: decompose or aggregate ---
    async def planner_node(state: PlannerState):
        messages = state["messages"]
        plan = state.get("plan", [])
        specialist_results = state.get("specialist_results", [])

        if not plan:
            # First call — decompose the task
            sys_msg = SystemMessage(content=PLANNER_SYSTEM_PROMPT)
            response = await planner_llm.ainvoke([sys_msg] + list(messages))
            parsed = _parse_plan(response.content)

            if parsed:
                return {
                    "plan": parsed,
                    "current_step": 0,
                    "completed_steps": [],
                    "specialist_results": [],
                    "messages": [AIMessage(content=f"Plan created with {len(parsed)} steps. Executing...")],
                }
            else:
                # Couldn't parse a plan — fall back to direct execution
                return {
                    "messages": [response],
                    "plan": [{"id": "direct", "action": "execute directly", "tools": [], "depends_on": []}],
                    "current_step": 0,
                }

        elif specialist_results:
            # All steps done — aggregate results into a report
            results_text = "\n\n".join(
                f"Step '{r.get('step_id', '?')}': {r.get('result', 'no result')}"
                for r in specialist_results
            )
            agg_prompt = (
                f"All specialist steps are complete. Produce a final report "
                f"for the user based on these results:\n\n{results_text}"
            )
            sys_msg = SystemMessage(content=PLANNER_SYSTEM_PROMPT)
            response = await planner_llm.ainvoke(
                [sys_msg] + list(messages) + [HumanMessage(content=agg_prompt)]
            )
            return {"messages": [response], "plan": []}  # Clear plan to signal END

        return {"messages": messages}

    # --- Dispatch node: pick next step and run specialist ---
    async def dispatch_node(state: PlannerState):
        plan = state.get("plan", [])
        current_step = state.get("current_step", 0)
        completed = set(state.get("completed_steps", []))

        if current_step >= len(plan):
            return state

        step = plan[current_step]
        step_id = step.get("id", f"step_{current_step}")
        action = step.get("action", "")

        # Build specialist prompt
        specialist_prompt = (
            f"Execute this step: {action}\n"
            f"Use the available tools to complete this task. "
            f"Report your findings concisely."
        )

        # Run specialist as a simple tool-calling loop
        specialist_messages = [
            SystemMessage(content="You are a specialist data profiling agent. Execute the given task using available tools."),
            HumanMessage(content=specialist_prompt),
        ]

        # Simple ReAct loop (max 10 iterations to prevent infinite loops)
        for _ in range(10):
            response = await specialist_llm_with_tools.ainvoke(specialist_messages)
            specialist_messages.append(response)

            if not response.tool_calls:
                break

            # Execute tools
            tool_node = ToolNode(tools)
            tool_results = await tool_node.ainvoke({"messages": specialist_messages})
            specialist_messages.extend(tool_results.get("messages", []))

        # Collect result
        final_content = response.content if hasattr(response, "content") else str(response)
        results = list(state.get("specialist_results", []))
        results.append({"step_id": step_id, "result": final_content[:2000]})

        completed_steps = list(state.get("completed_steps", []))
        completed_steps.append(step_id)

        return {
            "specialist_results": results,
            "completed_steps": completed_steps,
            "current_step": current_step + 1,
            "messages": [AIMessage(content=f"Step '{step_id}' complete.")],
        }

    # --- Routing ---
    def should_continue(state: PlannerState) -> str:
        plan = state.get("plan", [])
        current_step = state.get("current_step", 0)

        if not plan:
            return END

        if current_step < len(plan):
            return "dispatch"

        # All steps done — go back to planner for aggregation
        if state.get("specialist_results"):
            return "planner"

        return END

    # Build graph
    builder = StateGraph(PlannerState)
    builder.add_node("planner", planner_node)
    builder.add_node("dispatch", dispatch_node)

    builder.add_edge(START, "planner")
    builder.add_conditional_edges("planner", should_continue)
    builder.add_conditional_edges("dispatch", should_continue)

    if mode == "interactive":
        graph = builder.compile(interrupt_before=["dispatch"])
    else:
        graph = builder.compile()

    log.info("Planning agent created with %d tools", len(tools))
    return graph, client
