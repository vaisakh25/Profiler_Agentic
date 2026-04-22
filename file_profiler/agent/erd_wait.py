"""Shared ER-diagram wait helpers for chatbot, CLI, and web flows."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langgraph.graph import END, START, StateGraph

ERD_WAIT_RETRY_LIMIT = 2

_ERD_REMINDER = (
    "Internal instruction: the user asked for an ER diagram. Do not send a "
    "final answer yet. Keep using the enrichment/status tools until one of "
    "these is true: (1) enrich_relationships or remote_enrich_relationships "
    "returns a readable enriched_er_diagram_path; (2) check_enrichment_status "
    "or remote_check_enrichment_status returns status='complete' with a "
    "readable enriched_er_diagram_path; or (3) an explicit enrichment failure "
    "occurs. If status is stale/none, continue the workflow instead of "
    "replying early."
)

_ERD_PATTERNS = (
    re.compile(r"\ber\s*diagram\b", re.IGNORECASE),
    re.compile(r"\berd\b", re.IGNORECASE),
    re.compile(r"\bmermaid(?:\s+diagram)?\b", re.IGNORECASE),
    re.compile(r"\benrich\s*(?:&|and)\s*analy[sz]e\b", re.IGNORECASE),
)

_ENRICH_TOOLS = {"enrich_relationships", "remote_enrich_relationships"}
_STATUS_TOOLS = {"check_enrichment_status", "remote_check_enrichment_status"}


@dataclass(slots=True)
class ERDTurnStatus:
    """Current-turn ERD artifact status."""

    complete: bool = False
    failed: bool = False
    failure_message: str | None = None
    enriched_er_diagram_path: str | None = None
    enriched_profiles_path: str | None = None
    source_tool: str | None = None


def _content_text(content: Any) -> str:
    """Normalize message content to plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in content
        )
    return str(content)


def latest_user_requests_erd(messages: list[BaseMessage]) -> bool:
    """Return True when the latest user turn explicitly asks for an ERD."""
    for message in reversed(messages):
        if isinstance(message, HumanMessage):
            text = _content_text(message.content)
            return any(pattern.search(text) for pattern in _ERD_PATTERNS)
    return False


def current_turn_messages(messages: list[BaseMessage]) -> list[BaseMessage]:
    """Return only messages created after the latest user message."""
    for index in range(len(messages) - 1, -1, -1):
        if isinstance(messages[index], HumanMessage):
            return list(messages[index + 1 :])
    return list(messages)


def get_last_visible_ai_message(
    messages: list[BaseMessage],
    *,
    current_turn_only: bool = True,
) -> AIMessage | None:
    """Return the latest AI message with visible content."""
    candidates = current_turn_messages(messages) if current_turn_only else messages
    for message in reversed(candidates):
        if isinstance(message, AIMessage) and _content_text(message.content).strip():
            return message
    return None


def get_last_visible_ai_text(
    messages: list[BaseMessage],
    *,
    current_turn_only: bool = True,
) -> str:
    """Return normalized text for the latest visible AI message."""
    message = get_last_visible_ai_message(messages, current_turn_only=current_turn_only)
    if message is None:
        return ""
    return _content_text(message.content).strip()


def extract_erd_turn_status(messages: list[BaseMessage]) -> ERDTurnStatus:
    """Inspect current-turn tool results for ERD completion/failure."""
    tool_names: dict[str, str] = {}
    status = ERDTurnStatus()

    for message in current_turn_messages(messages):
        if isinstance(message, AIMessage) and message.tool_calls:
            for tool_call in message.tool_calls:
                tool_names[str(tool_call.get("id", tool_call.get("name", "")))] = str(
                    tool_call.get("name", "")
                )
            continue

        if not isinstance(message, ToolMessage):
            continue

        tool_name = getattr(message, "name", "") or tool_names.get(message.tool_call_id, "")
        if tool_name not in _ENRICH_TOOLS | _STATUS_TOOLS:
            continue

        payload = _parse_tool_payload(message.content)
        parsed = payload if isinstance(payload, dict) else {}

        if tool_name in _ENRICH_TOOLS:
            failure = _tool_failure_message(tool_name, parsed)
            if failure:
                status.failed = True
                status.failure_message = failure
                status.source_tool = tool_name
                continue

            er_path = _readable_path(parsed.get("enriched_er_diagram_path"))
            profiles_path = _readable_path(parsed.get("enriched_profiles_path"))
            if er_path:
                status.complete = True
                status.failed = False
                status.failure_message = None
                status.enriched_er_diagram_path = er_path
                status.enriched_profiles_path = profiles_path
                status.source_tool = tool_name
            else:
                status.failed = True
                status.failure_message = (
                    "The enrichment finished, but no readable ER diagram artifact "
                    "was produced."
                )
                status.source_tool = tool_name
            continue

        completion_state = str(parsed.get("status", "")).lower()
        if completion_state == "complete":
            er_path = _readable_path(parsed.get("enriched_er_diagram_path"))
            profiles_path = _readable_path(parsed.get("enriched_profiles_path"))
            if er_path:
                status.complete = True
                status.failed = False
                status.failure_message = None
                status.enriched_er_diagram_path = er_path
                status.enriched_profiles_path = profiles_path
                status.source_tool = tool_name
            else:
                status.failed = True
                status.failure_message = (
                    "Cached enrichment was marked complete, but the ER diagram "
                    "artifact could not be read."
                )
                status.source_tool = tool_name
            continue

        failure = _tool_failure_message(tool_name, parsed)
        if failure:
            status.failed = True
            status.failure_message = failure
            status.source_tool = tool_name

    return status


def configure_erd_wait_graph(
    builder: StateGraph,
    *,
    agent_node_name: str = "agent",
    tools_node_name: str = "tools",
    guard_node_name: str = "erd_guard",
) -> None:
    """Wire the shared ERD gate into an agent graph."""
    builder.add_node(guard_node_name, _erd_guard_node)
    builder.add_edge(START, agent_node_name)
    builder.add_conditional_edges(
        agent_node_name,
        _route_after_agent,
        {
            "tools": tools_node_name,
            "erd_guard": guard_node_name,
        },
    )
    builder.add_edge(tools_node_name, agent_node_name)
    builder.add_conditional_edges(
        guard_node_name,
        _route_after_erd_guard,
        {
            "agent": agent_node_name,
            "end": END,
        },
    )


def _parse_tool_payload(content: Any) -> Any:
    """Parse JSON tool payloads when possible."""
    if isinstance(content, (dict, list)):
        return content
    text = _content_text(content).strip()
    if not text.startswith(("{", "[")):
        return text
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text


def _readable_path(value: Any) -> str | None:
    """Return a readable filesystem path string when it exists."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        path = Path(value.strip())
    except (TypeError, ValueError):
        return None
    return str(path) if path.exists() else None


def _tool_failure_message(tool_name: str, payload: dict[str, Any]) -> str | None:
    """Extract an explicit failure message from an enrichment payload."""
    if not isinstance(payload, dict):
        return None
    error_value = payload.get("error")
    error_message = str(payload.get("error_message", "")).strip()
    if isinstance(error_value, str) and error_value.strip():
        return error_value.strip()
    if error_value is True and error_message:
        return error_message
    if error_value is True:
        return f"{tool_name} failed."
    if error_message:
        return error_message
    return None


def _looks_like_failure_response(message: AIMessage | None) -> bool:
    """Best-effort check for an assistant message already surfacing a failure."""
    if message is None:
        return False
    text = _content_text(message.content).lower()
    return any(
        token in text
        for token in ("error", "failed", "could not", "couldn't", "unable", "didn't complete")
    )


def _route_after_agent(state: dict[str, Any]) -> str:
    """Route tool calls to ToolNode and everything else to the ERD guard."""
    messages = state.get("messages", [])
    if messages:
        last = messages[-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "tools"
    return "erd_guard"


async def _erd_guard_node(state: dict[str, Any]) -> dict[str, Any]:
    """Hold ERD turns open until the diagram is ready or has clearly failed."""
    messages = state.get("messages", [])
    retry_count = int(state.get("erd_retry_count", 0) or 0)

    if not latest_user_requests_erd(messages):
        return {"erd_guard_action": "end", "erd_retry_count": 0}

    turn_status = extract_erd_turn_status(messages)
    last_ai = get_last_visible_ai_message(messages)

    if turn_status.complete:
        return {"erd_guard_action": "end", "erd_retry_count": 0}

    if turn_status.failed:
        if _looks_like_failure_response(last_ai):
            return {"erd_guard_action": "end", "erd_retry_count": 0}
        return {
            "messages": [
                AIMessage(
                    content=turn_status.failure_message
                    or "The ER diagram workflow failed before the artifact was ready."
                )
            ],
            "erd_guard_action": "end",
            "erd_retry_count": 0,
        }

    if retry_count >= ERD_WAIT_RETRY_LIMIT:
        return {
            "messages": [
                AIMessage(
                    content=(
                        "I couldn't confirm that the ER diagram artifact was actually "
                        "built yet, so I'm stopping instead of replying with a partial "
                        "answer. Please retry or check the enrichment status again."
                    )
                )
            ],
            "erd_guard_action": "end",
            "erd_retry_count": 0,
        }

    return {
        "messages": [HumanMessage(content=_ERD_REMINDER)],
        "erd_guard_action": "retry",
        "erd_retry_count": retry_count + 1,
    }


def _route_after_erd_guard(state: dict[str, Any]) -> str:
    """Either continue the loop or end the turn after the ERD guard runs."""
    if state.get("erd_guard_action") == "retry":
        return "agent"
    return "end"
