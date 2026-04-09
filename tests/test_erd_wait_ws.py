"""Tests for the ER-diagram wait gate in websocket streaming."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolNode

from file_profiler.agent.erd_wait import configure_erd_wait_graph, extract_erd_turn_status
from file_profiler.agent.state import AgentState
from file_profiler.agent.web_server import _stream_turn_guarded


class _RecorderWebSocket:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.messages.append(payload)


class _EarlyAnswerLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def ainvoke(self, messages):
        self.calls += 1
        has_tool_result = any(type(message).__name__ == "ToolMessage" for message in messages)
        if has_tool_result:
            return AIMessage(content="The ER diagram is ready now.")
        if self.calls == 1:
            return AIMessage(content="I will notify you when the ER diagram is ready.")
        return AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "check_enrichment_status",
                    "args": {"dir_path": "dummy"},
                    "id": "erd-status-1",
                    "type": "tool_call",
                }
            ],
        )


@pytest.mark.asyncio
async def test_websocket_waits_for_erd_before_assistant_reply(tmp_path: Path) -> None:
    er_path = tmp_path / "enriched_er_diagram.md"
    er_path.write_text("erDiagram\n  ORDERS {\n    int id\n  }\n", encoding="utf-8")
    profiles_path = tmp_path / "enriched_profiles.json"
    profiles_path.write_text("[]\n", encoding="utf-8")

    @tool
    def check_enrichment_status(dir_path: str) -> str:
        """Return a cached-complete enrichment status for the ERD test."""
        return json.dumps(
            {
                "status": "complete",
                "reason": "Cached enrichment is ready.",
                "enriched_er_diagram_path": str(er_path),
                "enriched_profiles_path": str(profiles_path),
            }
        )

    llm = _EarlyAnswerLLM()

    async def agent_node(state: AgentState):
        return {"messages": [await llm.ainvoke(state["messages"])]}

    builder = StateGraph(AgentState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", ToolNode([check_enrichment_status], handle_tool_errors=True))
    configure_erd_wait_graph(builder)
    graph = builder.compile(checkpointer=MemorySaver())

    websocket = _RecorderWebSocket()
    config = {"configurable": {"thread_id": "erd-ws-test"}}
    inputs = {
        "messages": [HumanMessage(content="Please build the ER diagram and wait until it is done.")],
        "mode": "autonomous",
        "erd_retry_count": 0,
        "erd_guard_action": "",
    }

    await _stream_turn_guarded(websocket, graph, inputs, config)

    assistant_indexes = [
        index for index, message in enumerate(websocket.messages)
        if message.get("type") == "assistant"
    ]
    erd_indexes = [
        index for index, message in enumerate(websocket.messages)
        if message.get("type") == "er_diagram"
    ]

    assert llm.calls >= 3
    assert len(assistant_indexes) == 1
    assert len(erd_indexes) == 1
    assert assistant_indexes[0] < erd_indexes[0]
    assert websocket.messages[assistant_indexes[0]]["content"] == "The ER diagram is ready now."
    assert "erDiagram" in websocket.messages[erd_indexes[0]]["content"]


def test_extract_erd_turn_status_flags_missing_diagram_artifact() -> None:
    from langchain_core.messages import ToolMessage

    messages = [
        HumanMessage(content="Generate the ERD."),
        AIMessage(
            content="",
            tool_calls=[
                {
                    "name": "enrich_relationships",
                    "args": {"dir_path": "dummy"},
                    "id": "enrich-1",
                    "type": "tool_call",
                }
            ],
        ),
        ToolMessage(
            content=json.dumps({"tables_analyzed": 1, "enriched_profiles_path": "profiles.json"}),
            tool_call_id="enrich-1",
        ),
    ]

    status = extract_erd_turn_status(messages)

    assert status.failed is True
    assert status.complete is False
    assert "no readable er diagram artifact" in (status.failure_message or "").lower()
